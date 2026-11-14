package matrix

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/yunhu"
)

// MediaStore keeps Matrix uploads on local disk and proxies Yunhu media through
// the homeserver, so clients only ever talk to one origin and no Yunhu
// credentials leak into media URLs.
type MediaStore struct {
	cfg      *config.Config
	dir      string
	cacheDir string
	log      *slog.Logger
	http     *http.Client

	mu sync.Mutex
}

const remoteMediaPrefix = "yh_"

// NewMediaStore prepares the media directories.
func NewMediaStore(cfg *config.Config, log *slog.Logger) (*MediaStore, error) {
	dir := filepath.Join(cfg.Storage.DataDir, "media")
	cache := filepath.Join(cfg.Storage.DataDir, "cache")
	for _, d := range []string{dir, cache} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create media dir: %w", err)
		}
	}
	return &MediaStore{
		cfg:      cfg,
		dir:      dir,
		cacheDir: cache,
		log:      log,
		http:     &http.Client{Timeout: cfg.Timeout()},
	}, nil
}

func (m *MediaStore) localPath(id string) string {
	if len(id) < 2 {
		return filepath.Join(m.dir, id)
	}
	return filepath.Join(m.dir, id[:2], id)
}

// Put stores bytes and returns the media ID (a content hash). The content type
// is kept in a sidecar file so downloads do not have to re-sniff every time.
func (m *MediaStore) Put(data []byte, contentType string) (string, error) {
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])
	path := m.localPath(id)
	if _, err := os.Stat(path); err == nil {
		return id, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	if contentType == "" || contentType == "application/octet-stream" {
		contentType = http.DetectContentType(data)
	}
	_ = os.WriteFile(path+".meta", []byte(contentType), 0o600)
	return id, nil
}

// ContentType returns the stored content type for a media ID.
func (m *MediaStore) ContentType(id string) string {
	raw, err := os.ReadFile(m.localPath(id) + ".meta")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// Open returns a local media file.
func (m *MediaStore) Open(id string) (io.ReadSeekCloser, int64, error) {
	path := m.localPath(id)
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

// MXC wraps a local media ID into an mxc URI.
func (m *MediaStore) MXC(id string) string {
	return "mxc://" + m.cfg.Server.Name + "/" + id
}

// RemoteMXC wraps a Yunhu media reference (absolute URL or Qiniu key) into an
// mxc URI served by this homeserver.
func (m *MediaStore) RemoteMXC(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "mxc://") {
		return raw
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(raw))
	return "mxc://" + m.cfg.Server.Name + "/" + remoteMediaPrefix + encoded
}

// CachedSize returns the byte size of a proxied reference if we have already
// fetched it, and 0 otherwise.
//
// Yunhu never reports a size for images, voice notes or videos — only for files
// — and clients render `"size": 0` as "0 B". So the only useful answers are a
// real number or no key at all, and the disk cache is the one place we can get a
// real number without another round trip.
func (m *MediaStore) CachedSize(ref string) int64 {
	if ref == "" {
		return 0
	}
	sum := sha256.Sum256([]byte(m.remoteURL(ref)))
	st, err := os.Stat(filepath.Join(m.cacheDir, hex.EncodeToString(sum[:])))
	if err != nil {
		return 0
	}
	return st.Size()
}

// decodeRemote unwraps a proxied media ID.
func (m *MediaStore) decodeRemote(mediaID string) (string, bool) {
	if !strings.HasPrefix(mediaID, remoteMediaPrefix) {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(mediaID, remoteMediaPrefix))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// remoteURL turns a Yunhu media reference into a fetchable URL.
//
// Bare keys are resolved per kind by yunhu.MediaURL, because Yunhu spreads its
// objects over several CDN hosts (images and stickers on chat-img, audio on
// chat-audio1, video on chat-video1, documents on chat-file). yunhu.media_base
// is only an override, e.g. for the in-process mock upstream.
func (m *MediaStore) remoteURL(ref string) string {
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref
	}
	if base := strings.TrimRight(m.cfg.Yunhu.MediaBase, "/"); base != "" {
		return base + "/" + strings.TrimLeft(ref, "/")
	}
	return yunhu.MediaURL(ref)
}

// FetchRemote downloads (and caches) a proxied media reference.
func (m *MediaStore) FetchRemote(ctx context.Context, ref string) ([]byte, string, error) {
	url := m.remoteURL(ref)
	sum := sha256.Sum256([]byte(url))
	cachePath := filepath.Join(m.cacheDir, hex.EncodeToString(sum[:]))

	m.mu.Lock()
	data, err := os.ReadFile(cachePath)
	m.mu.Unlock()
	if err == nil {
		return data, http.DetectContentType(data), nil
	}

	client := yunhu.NewClient(m.cfg.Yunhu.BaseURL, m.cfg.Timeout())
	body, ctype, err := client.Fetch(ctx, url, 128<<20)
	if err != nil {
		return nil, "", err
	}
	m.mu.Lock()
	_ = os.WriteFile(cachePath, body, 0o600)
	m.mu.Unlock()
	if ctype == "" {
		ctype = http.DetectContentType(body)
	}
	return body, ctype, nil
}

// ResolveForYunhu converts a Matrix media reference into something Yunhu can
// store in a message: an absolute URL, or a Qiniu object key.
//
// Preference order for locally stored uploads:
//  1. upload to Yunhu's Qiniu bucket and return the object key;
//  2. fall back to a public URL when server.public_url is configured;
//  3. otherwise fail, because Yunhu cannot dereference mxc://.
func (m *MediaStore) ResolveForYunhu(ctx context.Context, yc *yunhu.Client, ref, kind string) (string, error) {
	if ref == "" {
		return "", errors.New("empty media reference")
	}
	if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
		return ref, nil
	}
	if strings.HasPrefix(ref, "mxc://") {
		rest := strings.TrimPrefix(ref, "mxc://")
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			return "", fmt.Errorf("malformed mxc uri %q", ref)
		}
		serverName := rest[:slash]
		mediaID := rest[slash+1:]
		if serverName != m.cfg.Server.Name {
			return "", fmt.Errorf("cannot resolve remote mxc uri %q", ref)
		}
		if remote, ok := m.decodeRemote(mediaID); ok {
			// Already a Yunhu object: hand the original reference back.
			return remote, nil
		}
		data, ctype, err := m.readLocal(mediaID)
		if err != nil {
			return "", err
		}
		// Upload to Yunhu's Qiniu bucket. yunhu.upload_host is only an override
		// (the in-process mock); the real upload domain is discovered per bucket,
		// so an empty value must still upload instead of falling through to the
		// public-url path - which fails outright when public_url is unset.
		var uploadErr error
		if yc != nil {
			key, err := yc.UploadToQiniu(ctx, m.cfg.Yunhu.UploadHost, kind, mediaID, ctype, data)
			if err == nil {
				return key, nil
			}
			uploadErr = err
			m.log.Warn("qiniu upload failed, falling back to public url", "err", err)
		}
		if m.cfg.Server.PublicURL != "" {
			return m.cfg.Server.PublicURL + "/_matrix/media/v3/download/" + serverName + "/" + mediaID, nil
		}
		if uploadErr != nil {
			return "", fmt.Errorf("cannot hand the upload to Yunhu (no public_url configured): %w", uploadErr)
		}
		return "", errors.New("no way to expose uploaded media to Yunhu: set server.public_url")
	}
	// Bare reference: assume it is already a Yunhu object key.
	return ref, nil
}

func (m *MediaStore) readLocal(id string) ([]byte, string, error) {
	f, size, err := m.Open(id)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	if size > 128<<20 {
		return nil, "", errors.New("media too large")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, "", err
	}
	return data, http.DetectContentType(data), nil
}
