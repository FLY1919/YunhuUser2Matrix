package yunhu

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// resetQiniuCache drops the discovered-host cache between tests, since it is
// package level (the real thing is shared by every upload in the process).
func resetQiniuCache() {
	qiniuHostMu.Lock()
	qiniuHostCache = map[string]qiniuHostCacheEntry{}
	qiniuHostMu.Unlock()
}

// tokenServer answers the qiniu-token endpoint with a token whose first segment
// is the access key Qiniu's discovery API wants.
func tokenServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/misc/qiniu-token" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 1,
			"data": map[string]any{"token": token},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestUploadUsesDiscoveredHost is the regression test for the "EOF" upload
// failure: the request must go to the upload domain Qiniu reports for the
// bucket, carrying both token and key, instead of to the generic
// upload.qiniup.com which answers EOF for a bucket in another region.
func TestUploadUsesDiscoveredHost(t *testing.T) {
	resetQiniuCache()

	var gotToken, gotKey, gotFilename, gotContentType string
	upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			t.Errorf("parse multipart: %v", err)
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		gotToken = r.FormValue("token")
		gotKey = r.FormValue("key")
		f, hdr, err := r.FormFile("file")
		if err != nil {
			t.Errorf("no file part: %v", err)
			http.Error(w, "no file", http.StatusBadRequest)
			return
		}
		defer f.Close()
		gotFilename = hdr.Filename
		gotContentType = hdr.Header.Get("Content-Type")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"key": gotKey, "hash": "h", "fsize": 3,
		})
	}))
	defer upload.Close()

	var queries int32
	discovery := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&queries, 1)
		if got := r.URL.Query().Get("ak"); got != "ak-1" {
			t.Errorf("ak = %q, want the token's first segment", got)
		}
		if got := r.URL.Query().Get("bucket"); got != qiniuBucketImage {
			t.Errorf("bucket = %q, want %q", got, qiniuBucketImage)
		}
		// A full URL so the test can stay on plain HTTP; production gets a bare
		// host and qiniuUploadURL prefixes https.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hosts": []any{map[string]any{"up": map[string]any{"domains": []string{upload.URL}}}},
		})
	}))
	defer discovery.Close()

	oldBase := qiniuQueryBase
	qiniuQueryBase = discovery.URL
	t.Cleanup(func() { qiniuQueryBase = oldBase })

	c := NewClient(tokenServer(t, "ak-1:sig:tok").URL, 0)
	data := []byte("png")

	key, err := c.UploadToQiniu(context.Background(), "", "image", "", "image/png", data)
	if err != nil {
		t.Fatalf("UploadToQiniu: %v", err)
	}

	sum := md5.Sum(data)
	wantKey := hex.EncodeToString(sum[:]) + ".png"
	if key != wantKey {
		t.Fatalf("key = %q, want %q", key, wantKey)
	}
	if gotKey != wantKey {
		t.Fatalf("form key = %q, want %q", gotKey, wantKey)
	}
	if gotToken != "ak-1:sig:tok" {
		t.Fatalf("form token = %q", gotToken)
	}
	if gotFilename != wantKey {
		t.Fatalf("file part filename = %q, want %q", gotFilename, wantKey)
	}
	if gotContentType != "image/png" {
		t.Fatalf("file part content type = %q", gotContentType)
	}

	// The second upload must reuse the discovered host.
	if _, err := c.UploadToQiniu(context.Background(), "", "image", "", "image/png", data); err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if got := atomic.LoadInt32(&queries); got != 1 {
		t.Fatalf("discovery was called %d times, want 1 (host should be cached)", got)
	}
}

// TestUploadFallsBackWhenHostClosesConnection covers the exact failure mode the
// user hit: Qiniu's edge drops the connection (EOF) instead of answering. The
// upload has to move on to the next candidate host instead of surfacing "EOF".
func TestUploadFallsBackWhenHostClosesConnection(t *testing.T) {
	resetQiniuCache()

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer broken.Close()

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseMultipartForm(8 << 20)
		_ = json.NewEncoder(w).Encode(map[string]any{"key": r.FormValue("key"), "hash": "h"})
	}))
	defer good.Close()

	discovery := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"hosts": []any{map[string]any{"up": map[string]any{"domains": []string{broken.URL}}}},
		})
	}))
	defer discovery.Close()

	oldBase := qiniuQueryBase
	qiniuQueryBase = discovery.URL
	t.Cleanup(func() { qiniuQueryBase = oldBase })

	c := NewClient(tokenServer(t, "ak-2:sig:tok").URL, 0)
	c.HTTP.Timeout = 5 * time.Second

	// The broken host is the discovered one; the working one is the configured
	// override, so a successful upload proves the loop moved past the EOF.
	key, err := c.UploadToQiniu(context.Background(), good.URL, "image", "", "image/png", []byte("x"))
	if err != nil {
		t.Fatalf("UploadToQiniu: %v", err)
	}
	if key == "" {
		t.Fatal("empty key")
	}
}

// TestQiniuKey pins the object naming, which decides whether clients can render
// the result: an extension-less key shows up as a nameless blob.
func TestQiniuKey(t *testing.T) {
	data := []byte("payload")
	sum := md5.Sum(data)
	base := hex.EncodeToString(sum[:])

	cases := []struct {
		kind        string
		filename    string
		contentType string
		want        string
	}{
		{"image", "", "image/webp", base + ".webp"},
		{"image", "", "image/jpeg", base + ".jpg"},
		{"sticker", "", "image/gif", base + ".gif"},
		{"video", "", "video/mp4", base + ".mp4"},
		{"audio", "", "audio/mp4", base + ".m4a"},
		{"file", "", "application/pdf", "disk/" + base + ".pdf"},
		// A filename with an extension wins over the content type.
		{"image", "photo.PNG", "application/octet-stream", base + ".png"},
		// Unknown content type: still a key, just without an extension.
		{"image", "", "application/octet-stream", base},
	}
	for _, c := range cases {
		if got := qiniuKey(c.kind, c.filename, c.contentType, data); got != c.want {
			t.Errorf("qiniuKey(%q,%q,%q) = %q, want %q", c.kind, c.filename, c.contentType, got, c.want)
		}
	}
}

// TestQiniuBucket pins the bucket per media kind, taken from the Yunhu client's
// own upload utilities.
func TestQiniuBucket(t *testing.T) {
	cases := map[string]string{
		"image":   "chat68",
		"sticker": "chat68",
		"file":    "chat68-file",
		"video":   "chat68-video",
		"audio":   "chat68-audio",
		"":        "chat68",
	}
	for kind, want := range cases {
		if got := QiniuBucket(kind); got != want {
			t.Errorf("QiniuBucket(%q) = %q, want %q", kind, got, want)
		}
	}
}

// TestQiniuUploadURL: a discovered domain is a bare host, a mock upstream is a
// full URL, and both have to end up as something http.NewRequest accepts.
func TestQiniuUploadURL(t *testing.T) {
	cases := map[string]string{
		"upload-z2.qiniup.com":       "https://upload-z2.qiniup.com/",
		"https://upload.qiniup.com":  "https://upload.qiniup.com/",
		"http://127.0.0.1:9000":      "http://127.0.0.1:9000/",
		"https://host.example/path/": "https://host.example/path/",
		"":                           "",
	}
	for in, want := range cases {
		if got := qiniuUploadURL(in); got != want {
			t.Errorf("qiniuUploadURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestQiniuKeyHasNoSlashes guards the multipart filename header: a key with a
// quote or newline would break the form encoding.
func TestQiniuKeyHasNoSlashes(t *testing.T) {
	key := qiniuKey("image", "", "image/png", []byte("x"))
	if strings.ContainsAny(key, `"`+"\r\n") {
		t.Fatalf("key %q contains characters that would break the form header", key)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}\.png$`).MatchString(key) {
		t.Fatalf("unexpected key shape %q", key)
	}
}
