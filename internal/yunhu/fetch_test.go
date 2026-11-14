package yunhu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFetchSendsRefererForYunhuStorage is the regression test for the 403 storm
// on avatars and message images. Yunhu's CDN rejects any request to a jwznb.com
// host without `Referer: http://myapp.jwznb.com` (X-Error-Detail: RHIE), and
// clients cannot be changed to send it — the proxy has to.
func TestFetchSendsRefererForYunhuStorage(t *testing.T) {
	var gotReferer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReferer = r.Header.Get("Referer")
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png"))
	}))
	defer srv.Close()

	// httptest gives us 127.0.0.1, so point the check at a fake storage host
	// through the same code path: isYunhuStorageHost decides, and the URL's
	// host is what it inspects. Swap the host while keeping the test server's
	// address reachable by using a client whose transport rewrites the Host.
	c := NewClient(srv.URL, 0)
	c.HTTP.Transport = &rewriteTransport{target: srv.URL}

	if _, _, err := c.Fetch(context.Background(), "https://chat-img.jwznb.com/a.png", 0); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if gotReferer != YunhuMediaReferer {
		t.Fatalf("Referer = %q, want %q", gotReferer, YunhuMediaReferer)
	}
}

// TestFetchOmitsRefererForOtherHosts makes sure the Yunhu-specific header is not
// sprayed at arbitrary third parties.
func TestFetchOmitsRefererForOtherHosts(t *testing.T) {
	var gotReferer string
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		gotReferer = r.Header.Get("Referer")
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	if _, _, err := c.Fetch(context.Background(), srv.URL+"/a.png", 0); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !called {
		t.Fatal("test server was never reached")
	}
	if gotReferer != "" {
		t.Fatalf("Referer = %q, want it empty for a non-Yunhu host", gotReferer)
	}
}

// TestIsYunhuStorageHost pins the host matching: every CDN family documented by
// the API reference must be covered, and lookalike domains must not be.
func TestIsYunhuStorageHost(t *testing.T) {
	yes := []string{
		"chat-img.jwznb.com", "chat-img2.jwznb.com", "chat-img3.jwznb.com",
		"chat-video1.jwznb.com", "chat-audio1.jwznb.com", "chat-file.jwznb.com",
		"chat-storage1.jwznb.com", "chat-file-oss.jwznb.com", "app-cdn1.jwznb.com",
		"JWZNB.COM",
	}
	for _, h := range yes {
		if !isYunhuStorageHost(h) {
			t.Errorf("isYunhuStorageHost(%q) = false, want true", h)
		}
	}
	no := []string{
		"jwznb.com.evil.example", "notjwznb.com", "example.com", "", "chat-img.jwznb.co",
	}
	for _, h := range no {
		if isYunhuStorageHost(h) {
			t.Errorf("isYunhuStorageHost(%q) = true, want false", h)
		}
	}
}

// TestFetchReportsCDNErrorDetail keeps the CDN's own explanation in the error,
// so "403" can be told apart from "file was deleted".
func TestFetchReportsCDNErrorDetail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Error-Detail", "RHIE")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, 0)
	_, _, err := c.Fetch(context.Background(), srv.URL+"/a.png", 0)
	if err == nil {
		t.Fatal("expected an error for HTTP 403")
	}
	if !strings.Contains(err.Error(), "RHIE") {
		t.Fatalf("error %q does not mention the CDN detail", err)
	}
}

// rewriteTransport sends every request to target while leaving the URL (and
// therefore the Host used for header decisions) untouched.
type rewriteTransport struct{ target string }

func (t *rewriteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	clone := r.Clone(r.Context())
	clone.URL.Scheme = "http"
	clone.URL.Host = strings.TrimPrefix(t.target, "http://")
	return http.DefaultTransport.RoundTrip(clone)
}
