package yunhu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMediaURLRoutesBareKeys is the regression test for the sticker 502s: Yunhu
// sends stickers and expressions as bare keys, and the API reference says they
// live on chat-img.jwznb.com. The previous single-base default pointed at a
// Qiniu origin host that no longer resolves, so every sticker died with
// `dial tcp: lookup qiniu.jwzhd.com: no such host`.
func TestMediaURLRoutesBareKeys(t *testing.T) {
	cases := map[string]string{
		// The exact key from the bug report.
		"sticker/85ade3342b00c98361d3322347758104.gif": "https://chat-img.jwznb.com/sticker/85ade3342b00c98361d3322347758104.gif",
		"sticker/abe73b4671e44aa984ab27ba3dda5951.png": "https://chat-img.jwznb.com/sticker/abe73b4671e44aa984ab27ba3dda5951.png",
		// Expressions are documented as "前面加上 https://chat-img.jwznb.com/".
		"expression/abcdef123456789abcdef123456789ab.jpg": "https://chat-img.jwznb.com/expression/abcdef123456789abcdef123456789ab.jpg",
		// Yunhu's own spelling for the bundled default avatars.
		"defalut-avatars/Nellie Bly.png": "https://chat-img.jwznb.com/defalut-avatars/Nellie Bly.png",
		// A key with no recognisable prefix (a background image) still belongs
		// to the image bucket rather than to a dead host.
		"background9.webp": "https://chat-img.jwznb.com/background9.webp",
		// A leading slash must not survive into the URL.
		"/sticker/x.gif": "https://chat-img.jwznb.com/sticker/x.gif",
		// Per-kind routes from the API reference.
		"audio/voice.m4a":           "https://chat-audio1.jwznb.com/audio/voice.m4a",
		"voice/voice.amr":           "https://chat-audio1.jwznb.com/voice/voice.amr",
		"video/clip.mp4":            "https://chat-video1.jwznb.com/video/clip.mp4",
		"file/report.pdf":           "https://chat-file.jwznb.com/file/report.pdf",
		"knowledge/835a1ab0d63.txt": "https://chat-file.jwznb.com/knowledge/835a1ab0d63.txt",
		// Absolute URLs are already resolvable and must pass through.
		"https://chat-img.jwznb.com/abc.gif": "https://chat-img.jwznb.com/abc.gif",
		"http://cdn.example.com/a.png":       "http://cdn.example.com/a.png",
		// Nothing in, nothing out.
		"": "",
	}
	for ref, want := range cases {
		if got := MediaURL(ref); got != want {
			t.Errorf("MediaURL(%q) = %q, want %q", ref, got, want)
		}
	}
}

// TestMediaURLStickerFetchSendsReferer ties the two halves together: the URL
// produced for a sticker key must be a jwznb.com URL, so Fetch has to attach
// the CDN's required Referer. Without it the request comes back 403 RHIE and
// the proxy turns that into a 502 for the client.
func TestMediaURLStickerFetchSendsReferer(t *testing.T) {
	var gotPath, gotReferer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotReferer = r.Header.Get("Referer")
		w.Header().Set("Content-Type", "image/gif")
		_, _ = w.Write([]byte("GIF89a"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	c.HTTP.Transport = &rewriteTransport{target: srv.URL}

	const key = "sticker/85ade3342b00c98361d3322347758104.gif"
	body, ctype, err := c.Fetch(context.Background(), MediaURL(key), 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != "GIF89a" || !strings.HasPrefix(ctype, "image/gif") {
		t.Fatalf("body %q ctype %q", body, ctype)
	}
	if gotPath != "/"+key {
		t.Fatalf("path = %q, want %q", gotPath, "/"+key)
	}
	if gotReferer != YunhuMediaReferer {
		t.Fatalf("Referer = %q, want %q", gotReferer, YunhuMediaReferer)
	}
}

// TestMediaURLIsIdempotent guards the double-prefix trap: callers hand both
// bare keys and already-resolved URLs to the same helper.
func TestMediaURLIsIdempotent(t *testing.T) {
	once := MediaURL("sticker/x.gif")
	if twice := MediaURL(once); twice != once {
		t.Fatalf("MediaURL(MediaURL(k)) = %q, want %q", twice, once)
	}
}
