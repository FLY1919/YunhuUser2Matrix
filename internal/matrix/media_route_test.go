package matrix

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// authenticatedMediaPaths is what Element X actually requests. It never uses
// the legacy /_matrix/media/v3 family for avatars or message images, so if any
// of these routes is missing the router answers 404 M_UNRECOGNIZED and the user
// sees broken images everywhere while the logs show `status=404 dur_ms=0`.
func authenticatedMediaPaths(serverName, mediaID string) []string {
	return []string{
		"/_matrix/client/v1/media/download/" + serverName + "/" + mediaID,
		"/_matrix/client/v1/media/thumbnail/" + serverName + "/" + mediaID + "?width=128&height=128&method=crop",
		"/_matrix/media/v3/download/" + serverName + "/" + mediaID,
		"/_matrix/media/v3/thumbnail/" + serverName + "/" + mediaID + "?width=128&height=128",
	}
}

// TestAuthenticatedMediaRoutesServeBytes is the regression test for the 404
// storm: every media path a modern client uses must return the real bytes.
func TestAuthenticatedMediaRoutesServeBytes(t *testing.T) {
	s, _ := newTestServer(t)
	serverName := s.Cfg.Server.Name

	// A tiny PNG header is enough; the store never validates the payload.
	payload := []byte("\x89PNG\r\n\x1a\nfake-image-body")
	id, err := s.Media.Put(payload, "image/png")
	if err != nil {
		t.Fatalf("Media.Put: %v", err)
	}

	for _, path := range authenticatedMediaPaths(serverName, id) {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200 (body: %s)", path, rec.Code, rec.Body.String())
			continue
		}
		if got := rec.Body.String(); got != string(payload) {
			t.Errorf("%s: body %q, want %q", path, got, payload)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "image/png" {
			t.Errorf("%s: Content-Type %q, want image/png", path, ct)
		}
	}
}

// TestAuthenticatedMediaRouteIsRouted guards the failure mode that caused the
// 404s: a missing route returns M_UNRECOGNIZED from the catch-all, while a
// missing *file* on an existing route returns M_NOT_FOUND from the handler.
// Both are 404, so the errcode is the only way to tell them apart.
func TestAuthenticatedMediaRouteIsRouted(t *testing.T) {
	s, _ := newTestServer(t)
	serverName := s.Cfg.Server.Name

	for _, path := range authenticatedMediaPaths(serverName, "no-such-media-id") {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", path, rec.Code)
			continue
		}
		if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
			t.Errorf("%s: route is not registered, got %s", path, body)
		}
	}
}

// TestMediaRouteRejectsForeignServerName keeps the existing guard intact on the
// new authenticated routes: media IDs are only meaningful for this homeserver.
func TestMediaRouteRejectsForeignServerName(t *testing.T) {
	s, _ := newTestServer(t)
	id, err := s.Media.Put([]byte("x"), "text/plain")
	if err != nil {
		t.Fatalf("Media.Put: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v1/media/download/other.example/"+id, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 for a foreign server name", rec.Code)
	}
}

// TestProxiedMediaServedUnderAnyServerName covers the case that bites after a
// server.name change: old mxc URIs still say the previous name, and the proxied
// reference is self-describing, so it must still be served. The cache file is
// seeded by hand so the test never touches the network.
func TestProxiedMediaServedUnderAnyServerName(t *testing.T) {
	s, _ := newTestServer(t)
	const upstream = "https://cdn.example.com/old-avatar.png"
	payload := []byte("\x89PNG\r\n\x1a\ncached")

	sum := sha256.Sum256([]byte(upstream))
	if err := os.WriteFile(filepath.Join(s.Media.cacheDir, hex.EncodeToString(sum[:])), payload, 0o600); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	mxc := s.Media.RemoteMXC(upstream)
	path := "/_matrix/client/v1/media/thumbnail/old.example/" + mediaIDOf(mxc)

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != string(payload) {
		t.Fatalf("body %q, want %q", got, payload)
	}
}

// TestAuthenticatedMediaRouteSurvivesBase64Padding pins the media-ID shape:
// proxied IDs are base64url without padding, but the router must not mangle a
// padded one either, because the ID is a single path segment that must reach
// the handler byte for byte.
func TestAuthenticatedMediaRouteSurvivesBase64Padding(t *testing.T) {
	s, _ := newTestServer(t)
	serverName := s.Cfg.Server.Name

	const padded = "yh_YWJjZA=="
	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v1/media/download/"+serverName+"/"+padded, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	// Not a real reference, so the handler answers 502 (fetch failed) or 404 —
	// what matters is that the route matched instead of falling through.
	if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
		t.Fatalf("padded media ID did not reach the handler: %s", body)
	}
}

// TestMediaServesRangeRequests is the "media shows 0 B" guard for players: a
// client that asks for `bytes=N-` must get 206 with that slice, not a 200 from
// offset 0. Without it audio/video never start, and the UI reports an empty
// file.
func TestMediaServesRangeRequests(t *testing.T) {
	s, _ := newTestServer(t)
	serverName := s.Cfg.Server.Name
	payload := []byte("0123456789abcdefghij")
	id, err := s.Media.Put(payload, "audio/mp4")
	if err != nil {
		t.Fatalf("Media.Put: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v1/media/download/"+serverName+"/"+id, nil)
	req.Header.Set("Range", "bytes=10-15")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status %d, want 206 for a Range request", rec.Code)
	}
	if got := rec.Body.String(); got != "abcdef" {
		t.Fatalf("body %q, want %q", got, "abcdef")
	}
	if cr := rec.Header().Get("Content-Range"); cr != "bytes 10-15/20" {
		t.Fatalf("Content-Range = %q, want bytes 10-15/20", cr)
	}
	if ar := rec.Header().Get("Accept-Ranges"); ar != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", ar)
	}
}

// TestMediaInfoNeverReportsZeroSize covers the payload side of "0 B": Yunhu sends
// no size for images or voice notes, and emitting `"size": 0` makes clients show
// an empty file. The key must be absent instead, and the MIME type must be there
// so the client knows what it is dealing with.
func TestMediaInfoNeverReportsZeroSize(t *testing.T) {
	s, _ := newTestServer(t)

	img := s.buildContent(pb.ContentType_image, &pb.MsgContent{
		ImageUrl: "https://chat-img.jwznb.com/abc.gif",
		Width:    100, Height: 50,
	})
	info, _ := img["info"].(map[string]any)
	if _, has := info["size"]; has {
		t.Fatalf("image info must not carry a zero size: %v", info)
	}
	if info["mimetype"] != "image/gif" {
		t.Fatalf("image mimetype = %v, want image/gif", info["mimetype"])
	}
	if body := img["body"]; body != "abc.gif" {
		t.Fatalf("image body = %v, want the real file name", body)
	}

	// A file with no reported size must also stay silent about it.
	f := s.buildContent(pb.ContentType_file, &pb.MsgContent{
		FileName: "report.pdf", FileUrl: "https://chat-file.jwznb.com/report.pdf",
	})
	finfo, _ := f["info"].(map[string]any)
	if _, has := finfo["size"]; has {
		t.Fatalf("file info must not carry a zero size: %v", finfo)
	}
	if finfo["mimetype"] != "application/pdf" {
		t.Fatalf("file mimetype = %v, want application/pdf", finfo["mimetype"])
	}

	// When the size is known it must be reported.
	known := s.buildContent(pb.ContentType_file, &pb.MsgContent{
		FileName: "a.bin", FileUrl: "https://chat-file.jwznb.com/a.bin", FileSize: 1234,
	})
	if got := known["info"].(map[string]any)["size"]; got != int64(1234) {
		t.Fatalf("file size = %v, want 1234", got)
	}
}

// TestMediaInfoNeverReportsZeroDimensions is the layout half of the same rule:
// `w`/`h` of 0 make clients compute a 0/0 aspect ratio and collapse the image,
// which is how a sticker with no reported size ends up invisible.
func TestMediaInfoNeverReportsZeroDimensions(t *testing.T) {
	s, _ := newTestServer(t)

	stick := s.buildContent(pb.ContentType_sticker, &pb.MsgContent{
		Image: "sticker/85ade3342b00c98361d3322347758104.gif",
	})
	info, _ := stick["info"].(map[string]any)
	if _, has := info["w"]; has {
		t.Fatalf("sticker info must not carry a zero width: %v", info)
	}
	if _, has := info["h"]; has {
		t.Fatalf("sticker info must not carry a zero height: %v", info)
	}
	if info["mimetype"] != "image/gif" {
		t.Fatalf("sticker mimetype = %v, want image/gif", info["mimetype"])
	}

	// Real dimensions are still reported.
	img := s.buildContent(pb.ContentType_image, &pb.MsgContent{
		ImageUrl: "https://chat-img.jwznb.com/abc.png", Width: 640, Height: 480,
	})
	iinfo, _ := img["info"].(map[string]any)
	if iinfo["w"] != int32(640) || iinfo["h"] != int32(480) {
		t.Fatalf("image dimensions = %v/%v, want 640/480", iinfo["w"], iinfo["h"])
	}
}

// TestRemoteStickerKeyServedFromImageCDN is the end-to-end guard for the sticker
// 502s: a bare key must resolve to the documented image host, the download route
// must serve the bytes, and the two must agree on the cache key (they both go
// through remoteURL). The cache file is seeded by hand so nothing dials out.
func TestRemoteStickerKeyServedFromImageCDN(t *testing.T) {
	s, _ := newTestServer(t)
	serverName := s.Cfg.Server.Name

	const ref = "sticker/85ade3342b00c98361d3322347758104.gif"
	const want = "https://chat-img.jwznb.com/sticker/85ade3342b00c98361d3322347758104.gif"

	if got := s.Media.remoteURL(ref); got != want {
		t.Fatalf("remoteURL(%q) = %q, want %q", ref, got, want)
	}

	payload := []byte("GIF89a-sticker")
	sum := sha256.Sum256([]byte(want))
	if err := os.WriteFile(filepath.Join(s.Media.cacheDir, hex.EncodeToString(sum[:])), payload, 0o600); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	// The size is reported from the same cache entry, which is how a sticker
	// avoids the "0 B" label.
	if size := s.Media.CachedSize(ref); size != int64(len(payload)) {
		t.Fatalf("CachedSize(%q) = %d, want %d", ref, size, len(payload))
	}

	mediaID := mediaIDOf(s.Media.RemoteMXC(ref))
	path := "/_matrix/client/v1/media/download/" + serverName + "/" + mediaID
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != string(payload) {
		t.Fatalf("body %q, want %q", got, payload)
	}
}

// TestMediaBaseOverrideStillWins keeps the escape hatch honest: the in-process
// mock upstream sets yunhu.media_base and every bare key must go there instead
// of to a real CDN.
func TestMediaBaseOverrideStillWins(t *testing.T) {
	s, _ := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.MediaBase = "http://127.0.0.1:9/media/"
	})
	if got := s.Media.remoteURL("sticker/x.gif"); got != "http://127.0.0.1:9/media/sticker/x.gif" {
		t.Fatalf("remoteURL with override = %q", got)
	}
	// Absolute URLs are never rewritten by the override.
	if got := s.Media.remoteURL("https://chat-img.jwznb.com/a.gif"); got != "https://chat-img.jwznb.com/a.gif" {
		t.Fatalf("remoteURL of an absolute URL = %q", got)
	}
}

// TestDefaultMediaBaseIsNotTheDeadQiniuHost pins the default that broke every
// sticker: `qiniu.jwzhd.com` stopped resolving, so a bare key must not be
// prefixed with it any more.
func TestDefaultMediaBaseIsNotTheDeadQiniuHost(t *testing.T) {
	cfg := config.Default()
	if strings.Contains(cfg.Yunhu.MediaBase, "qiniu.jwzhd.com") {
		t.Fatalf("default media_base still points at the dead host: %q", cfg.Yunhu.MediaBase)
	}
}
