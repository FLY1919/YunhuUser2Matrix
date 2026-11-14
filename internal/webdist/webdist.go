// Package webdist downloads and unpacks a static Element Web build so the
// homeserver can serve the client from its own origin.
//
// This exists because of a browser restriction, not a server bug: a page loaded
// over https:// (app.element.io) is not allowed to call a plain http://
// homeserver on a private LAN address. Chrome treats it as mixed content plus a
// Private Network Access violation, so the request never leaves the browser and
// Element reports "Invalid /versions response". Serving the client from
// http://<host>:<port>/ makes everything same-origin and sidesteps the issue
// without certificates.
package webdist

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	repoOwner = "element-hq"
	repoName  = "element-web"
	// apiLatest is only a fallback: unauthenticated GitHub API calls are capped
	// at 60/hour per IP, which shared or proxied networks exhaust quickly.
	apiLatest = "https://api.github.com/repos/" + repoOwner + "/" + repoName + "/releases/latest"
	// htmlLatest redirects to the release page; it is not rate limited, so it
	// is the primary way to discover the current version.
	htmlLatest = "https://github.com/" + repoOwner + "/" + repoName + "/releases/latest"
	// maxExtractBytes caps total unpacked size as a guard against a hostile or
	// corrupt archive. The real build is a few hundred megabytes.
	maxExtractBytes = 2 << 30
	// progressEvery is how often the download logs a progress line.
	progressEvery = 10 << 20
)

var httpClient = &http.Client{Timeout: 20 * time.Minute}

// Release identifies a downloadable Element Web build.
type Release struct {
	Version string
	URL     string
	Size    int64
}

// Options controls Fetch.
type Options struct {
	// Dir is the destination directory for the unpacked build.
	Dir string
	// Version pins a release such as "v1.12.35". Empty means "latest".
	Version string
	// Force allows replacing a directory that already holds a build.
	Force bool
	// Mirror is an optional download mirror. Either a ghproxy-style prefix
	// ("https://ghproxy.net") or a base that already contains github.com.
	Mirror string
	Log    *slog.Logger
}

// Fetch downloads an Element Web release into opts.Dir. The archive's single
// top-level directory is stripped, so opts.Dir ends up holding index.html
// directly.
func Fetch(ctx context.Context, opts Options) (*Release, error) {
	if strings.TrimSpace(opts.Dir) == "" {
		return nil, errors.New("webdist: empty destination directory")
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	index := filepath.Join(opts.Dir, "index.html")
	if st, err := os.Stat(index); err == nil && st.Size() > 0 && !opts.Force {
		return nil, fmt.Errorf("webdist: %s already contains a client (pass -force to replace it)", opts.Dir)
	}

	tag, err := resolveVersion(ctx, opts.Version)
	if err != nil {
		return nil, err
	}
	rel := &Release{
		Version: tag,
		URL:     assetURL(opts.Mirror, tag),
		Size:    headSize(ctx, assetURL(opts.Mirror, tag)),
	}
	log.Info("downloading Element Web",
		"version", rel.Version, "size_mb", rel.Size>>20, "url", rel.URL)

	if err := downloadAndExtract(ctx, rel.URL, opts.Dir, log); err != nil {
		return nil, err
	}
	if st, err := os.Stat(index); err != nil || st.Size() == 0 {
		return nil, fmt.Errorf("webdist: unpacked archive but %s is missing", index)
	}
	return rel, nil
}

// resolveVersion finds the release tag to install. An explicit version wins;
// otherwise the HTML redirect is tried first because it has no rate limit, and
// the JSON API is only used if that fails.
func resolveVersion(ctx context.Context, version string) (string, error) {
	if v := strings.TrimSpace(version); v != "" {
		if !strings.HasPrefix(v, "v") {
			v = "v" + v
		}
		return v, nil
	}

	tag, redirectErr := tagFromRedirect(ctx)
	if redirectErr == nil {
		return tag, nil
	}
	tag, apiErr := tagFromAPI(ctx)
	if apiErr == nil {
		return tag, nil
	}
	return "", fmt.Errorf("webdist: cannot determine the latest version (%v; %v) - "+
		"pin one with -web-version v1.12.35", redirectErr, apiErr)
}

// tagFromRedirect reads the Location header of /releases/latest, which points
// at .../releases/tag/<tag>.
func tagFromRedirect(ctx context.Context) (string, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, htmlLatest, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "yhmatrix-webdist")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	loc := resp.Header.Get("Location")
	i := strings.LastIndex(loc, "/tag/")
	if i < 0 {
		return "", fmt.Errorf("no release tag in redirect (status %s)", resp.Status)
	}
	tag := strings.Trim(strings.TrimSpace(loc[i+len("/tag/"):]), "/")
	if tag == "" {
		return "", errors.New("empty release tag in redirect")
	}
	return tag, nil
}

// tagFromAPI asks the GitHub JSON API, optionally authenticated via
// GITHUB_TOKEN to lift the 60/hour anonymous limit.
func tagFromAPI(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiLatest, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "yhmatrix-webdist")
	req.Header.Set("Accept", "application/vnd.github+json")
	if tok := strings.TrimSpace(os.Getenv("GITHUB_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusForbidden {
			return "", errors.New("GitHub API rate limit exceeded, set GITHUB_TOKEN or pin -web-version")
		}
		return "", fmt.Errorf("GitHub API returned %s", resp.Status)
	}

	var payload struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&payload); err != nil {
		return "", err
	}
	if payload.TagName == "" {
		return "", errors.New("GitHub API returned an empty tag_name")
	}
	return payload.TagName, nil
}

// assetURL builds the download URL for a tag. Element names its tarball
// "element-<tag>.tar.gz".
func assetURL(mirror, tag string) string {
	full := fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/element-%s.tar.gz",
		repoOwner, repoName, tag, tag)
	m := strings.TrimRight(strings.TrimSpace(mirror), "/")
	switch {
	case m == "":
		return full
	case strings.Contains(m, "github.com"):
		// Host-replacement mirror, e.g. https://mirror.example.com/github.com
		return strings.Replace(full, "https://github.com", m, 1)
	default:
		// ghproxy-style prefix, e.g. https://ghproxy.net/https://github.com/...
		return m + "/" + full
	}
}

// headSize reports the download size, or 0 if it cannot be determined. It is
// only used for the progress display, so failures are not fatal.
func headSize(ctx context.Context, url string) int64 {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("User-Agent", "yhmatrix-webdist")
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	return resp.ContentLength
}

// downloadAndExtract streams the archive straight from the network into dest,
// never holding the whole build in memory.
func downloadAndExtract(ctx context.Context, url, dest string, log *slog.Logger) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "yhmatrix-webdist")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webdist: download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webdist: download returned %s", resp.Status)
	}

	gz, err := gzip.NewReader(&progressReader{r: resp.Body, total: resp.ContentLength, log: log})
	if err != nil {
		return fmt.Errorf("webdist: open gzip: %w", err)
	}
	defer gz.Close()

	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	written, skipped, err := extractTar(tar.NewReader(gz), dest)
	if err != nil {
		return err
	}
	log.Info("Element Web unpacked", "dir", dest, "files", written, "skipped", skipped)
	return nil
}

// extractTar writes regular files and directories into dest, dropping the
// archive's single leading path component. Entries that would escape dest are
// rejected rather than written.
func extractTar(tr *tar.Reader, dest string) (files, skipped int, err error) {
	root := filepath.Clean(dest)
	for {
		hdr, rerr := tr.Next()
		if rerr == io.EOF {
			return files, skipped, nil
		}
		if rerr != nil {
			return files, skipped, fmt.Errorf("webdist: read archive: %w", rerr)
		}

		name := strings.TrimPrefix(filepath.ToSlash(hdr.Name), "./")
		if name == "" || name == "." {
			continue
		}
		// Strip "element-v1.12.35/" so index.html lands at the root.
		slash := strings.IndexByte(name, '/')
		if slash < 0 {
			continue
		}
		name = name[slash+1:]
		if name == "" {
			continue
		}

		target := filepath.Join(root, filepath.FromSlash(name))
		if !within(root, target) {
			return files, skipped, fmt.Errorf("webdist: archive entry escapes destination: %q", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return files, skipped, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return files, skipped, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return files, skipped, err
			}
			// LimitReader also bounds the total unpacked size.
			_, cerr := io.Copy(f, io.LimitReader(tr, maxExtractBytes))
			if err := f.Close(); err != nil {
				return files, skipped, err
			}
			if cerr != nil {
				return files, skipped, fmt.Errorf("webdist: write %s: %w", target, cerr)
			}
			files++
		default:
			// Element ships plain files; symlinks and devices are not expected
			// and are unsafe to recreate (Windows needs privileges anyway).
			skipped++
		}
	}
}

// within reports whether target is root itself or sits underneath it.
func within(root, target string) bool {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	if filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// progressReader logs throughput every progressEvery bytes.
type progressReader struct {
	r     io.Reader
	read  int64
	total int64
	log   *slog.Logger
	next  int64
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.read += int64(n)
	if p.next == 0 {
		p.next = progressEvery
	}
	if p.read >= p.next {
		if p.total > 0 {
			p.log.Info("download progress",
				"percent", p.read*100/p.total, "mb", p.read>>20, "of_mb", p.total>>20)
		} else {
			p.log.Info("download progress", "mb", p.read>>20)
		}
		p.next += progressEvery
	}
	return n, err
}
