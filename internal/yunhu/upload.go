package yunhu

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

// Yunhu keeps uploads in four Qiniu buckets, and a bucket is only reachable
// through the upload domain of the region it lives in. The API reference points
// at the discovery call rather than a fixed host:
//
//	GET https://api.qiniu.com/v4/query?ak=<ak>&bucket=<bucket>
//	响应中 up 下的域名就是上传 Host
//
// Posting to the generic https://upload.qiniup.com instead — which is what this
// code used to do — gets a bare "EOF" from Qiniu's edge when the bucket lives in
// another region:
//
//	msg="qiniu upload failed, falling back to public url" err="Post \"https://upload.qiniup.com\": EOF"
const (
	qiniuBucketImage = "chat68"
	qiniuBucketFile  = "chat68-file"
	qiniuBucketVideo = "chat68-video"
	qiniuBucketAudio = "chat68-audio"

	// qiniuDefaultUploadHost is the documented fallback. It is only used when
	// discovery fails, because a bucket outside its region answers EOF here.
	qiniuDefaultUploadHost = "upload-z2.qiniup.com"
)

// qiniuQueryBase is a variable so tests can point discovery at a fake.
var qiniuQueryBase = "https://api.qiniu.com"

// QiniuBucket maps a media kind onto the bucket Yunhu stores it in.
func QiniuBucket(kind string) string {
	switch kind {
	case "file":
		return qiniuBucketFile
	case "video":
		return qiniuBucketVideo
	case "audio":
		return qiniuBucketAudio
	}
	return qiniuBucketImage
}

// qiniuHostCache remembers the discovered upload domain per bucket. Region
// mapping changes roughly never, and every upload would otherwise pay for an
// extra round trip.
type qiniuHostCacheEntry struct {
	host string
	at   time.Time
}

const qiniuHostTTL = time.Hour

var (
	qiniuHostMu    sync.Mutex
	qiniuHostCache = map[string]qiniuHostCacheEntry{}
)

func cachedQiniuHost(bucket string) string {
	qiniuHostMu.Lock()
	defer qiniuHostMu.Unlock()
	entry, ok := qiniuHostCache[bucket]
	if !ok || time.Since(entry.at) > qiniuHostTTL {
		return ""
	}
	return entry.host
}

func rememberQiniuHost(bucket, host string) {
	qiniuHostMu.Lock()
	qiniuHostCache[bucket] = qiniuHostCacheEntry{host: host, at: time.Now()}
	qiniuHostMu.Unlock()
}

// qiniuUploadHost asks Qiniu which domain serves the bucket. The access key is
// the first segment of the upload token ("<ak>:<signature>:<token>").
func (c *Client) qiniuUploadHost(ctx context.Context, token, bucket string) (string, error) {
	ak := strings.SplitN(token, ":", 2)[0]
	if ak == "" {
		return "", errors.New("malformed qiniu upload token")
	}
	if host := cachedQiniuHost(bucket); host != "" {
		return host, nil
	}

	endpoint := qiniuQueryBase + "/v4/query?ak=" + url.QueryEscape(ak) + "&bucket=" + url.QueryEscape(bucket)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "yhmatrix/1.0")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return "", &APIError{Path: endpoint, Status: resp.StatusCode, Body: string(raw)}
	}
	var out struct {
		Hosts []struct {
			Up struct {
				Domains []string `json:"domains"`
			} `json:"up"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", &APIError{Path: endpoint, Body: string(raw), Err: err}
	}
	for _, h := range out.Hosts {
		for _, d := range h.Up.Domains {
			if d = strings.TrimSpace(d); d != "" {
				rememberQiniuHost(bucket, d)
				return d, nil
			}
		}
	}
	return "", &APIError{Path: endpoint, Body: string(raw), Err: errors.New("no upload domain in query response")}
}

// qiniuUploadURL turns an upload domain into a URL. A bare host gets https; a
// full URL (used by the mock upstream and by tests) is kept as-is.
func qiniuUploadURL(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if strings.Contains(host, "://") {
		return strings.TrimRight(host, "/") + "/"
	}
	return "https://" + strings.TrimRight(host, "/") + "/"
}

// qiniuKey builds the object key Yunhu expects. The Yunhu client names objects
// after the MD5 of the payload plus the real extension ("<md5>.webp"), and files
// go under disk/ ("disk/<md5>.pdf"). The extension matters: the receiving
// clients pick a renderer from it, and an extension-less key shows up as a
// nameless blob.
func qiniuKey(kind, filename, contentType string, data []byte) string {
	sum := md5.Sum(data)
	base := hex.EncodeToString(sum[:])

	ext := strings.ToLower(path.Ext(strings.TrimSpace(filename)))
	if ext == "" {
		ext = extForMime(contentType)
	}
	if ext != "" && !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	if kind == "file" {
		return "disk/" + base + ext
	}
	return base + ext
}

// extForMime maps the stored content type onto a file extension.
func extForMime(contentType string) string {
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/bmp":
		return ".bmp"
	case "image/heic":
		return ".heic"
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "video/webm":
		return ".webm"
	case "audio/mp4", "audio/m4a", "audio/x-m4a":
		return ".m4a"
	case "audio/mpeg":
		return ".mp3"
	case "audio/ogg", "audio/opus":
		return ".ogg"
	case "audio/wav", "audio/x-wav":
		return ".wav"
	case "audio/amr":
		return ".amr"
	case "application/pdf":
		return ".pdf"
	case "application/zip":
		return ".zip"
	case "text/plain":
		return ".txt"
	}
	return ""
}

// UploadToQiniu uploads bytes and returns the object key Yunhu expects.
//
// uploadHost is an override (the mock upstream uses it); when it is empty the
// upload domain is discovered for the bucket. Discovery is tried first in every
// case: a hard-coded host that happens to be in the wrong region is what broke
// image messages, and the failure mode ("EOF") does not look like a network
// error at all.
func (c *Client) UploadToQiniu(ctx context.Context, uploadHost, kind, filename, contentType string, data []byte) (string, error) {
	token, err := c.QiniuToken(ctx, kind)
	if err != nil {
		return "", err
	}
	bucket := QiniuBucket(kind)
	key := qiniuKey(kind, filename, contentType, data)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Candidate hosts in order: the configured override, the discovered domain,
	// then the documented default. The override goes first so the mock upstream
	// (and tests) never depend on Qiniu's discovery service.
	candidates := make([]string, 0, 3)
	if h := strings.TrimSpace(uploadHost); h != "" {
		candidates = append(candidates, h)
	}
	if h, err := c.qiniuUploadHost(ctx, token, bucket); err == nil {
		candidates = append(candidates, h)
	}
	candidates = append(candidates, qiniuDefaultUploadHost)

	seen := map[string]struct{}{}
	var failures []string
	for _, host := range candidates {
		target := qiniuUploadURL(host)
		if target == "" {
			continue
		}
		if _, dup := seen[target]; dup {
			continue
		}
		seen[target] = struct{}{}

		key2, err := c.postQiniu(ctx, target, token, key, filename, contentType, data)
		if err == nil {
			return key2, nil
		}
		// Keep every attempt in the message: "EOF" alone says nothing about
		// which host was wrong, and that ambiguity is what made this bug so
		// hard to see in the log.
		failures = append(failures, target+": "+err.Error())
	}
	if len(failures) == 0 {
		return "", errors.New("no qiniu upload host available")
	}
	return "", fmt.Errorf("qiniu upload failed on every host (bucket %s): %s", bucket, strings.Join(failures, "; "))
}

// postQiniu performs one form upload. Qiniu wants token, key and file, in that
// order, and answers {"key":...,"hash":...,"fsize":...}.
func (c *Client) postQiniu(ctx context.Context, target, token, key, filename, contentType string, data []byte) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.WriteField("token", token); err != nil {
		return "", err
	}
	if err := w.WriteField("key", key); err != nil {
		return "", err
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, escapeQuotes(key)))
	h.Set("Content-Type", contentType)
	part, err := w.CreatePart(h)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(data); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("User-Agent", "yhmatrix/1.0")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 400 {
		return "", &APIError{Path: target, Status: resp.StatusCode, Body: string(raw)}
	}

	var out struct {
		Key  string `json:"key"`
		Hash string `json:"hash"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", &APIError{Path: target, Body: string(raw), Err: err}
	}
	if out.Key == "" {
		out.Key = out.Hash
	}
	if out.Key == "" {
		return "", &APIError{Path: target, Body: string(raw), Err: errors.New("no key in upload response")}
	}
	return out.Key, nil
}
