package matrix

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yunhu/yhmatrix/internal/config"
)

// getWellKnown fetches /.well-known/matrix/support through the real router.
func getWellKnown(t *testing.T, s *Server) (int, map[string]any, http.Header) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/matrix/support", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body, rec.Header()
}

// TestWellKnownSupportDerivesAdminContact covers the reported 404: with no
// configuration at all the endpoint must still answer, using the local account
// as the administrator - on a single-user bridge the account owner is the admin.
func TestWellKnownSupportDerivesAdminContact(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	code, body, header := getWellKnown(t, s)
	if code != http.StatusOK {
		t.Fatalf("status %d, want 200: %v", code, body)
	}
	if got := header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want * (discovery is fetched cross-origin)", got)
	}

	contacts, ok := body["contacts"].([]any)
	if !ok || len(contacts) != 1 {
		t.Fatalf("contacts = %v, want one derived admin contact", body["contacts"])
	}
	entry := contacts[0].(map[string]any)
	if got := entry["role"]; got != "m.role.admin" {
		t.Fatalf("role = %v, want m.role.admin", got)
	}
	if got := entry["matrix_id"]; got != acc.UserID {
		t.Fatalf("matrix_id = %v, want %s", got, acc.UserID)
	}
	if _, has := body["support_page"]; has {
		t.Fatalf("support_page must be omitted when not configured: %v", body)
	}
}

// TestWellKnownSupportWithoutAccounts: a fresh server has nothing to publish,
// and 404 is the spec's "no support information available".
func TestWellKnownSupportWithoutAccounts(t *testing.T) {
	s, _ := newTestServer(t)

	code, body, _ := getWellKnown(t, s)
	if code != http.StatusNotFound {
		t.Fatalf("status %d, want 404: %v", code, body)
	}
	// A missing route would also 404, so make sure the router actually matched:
	// M_NOT_FOUND means the handler ran, M_UNRECOGNIZED means it never existed.
	if got := body["errcode"]; got != ErrNotFound {
		t.Fatalf("errcode = %v, want %s (route must exist)", got, ErrNotFound)
	}
}

// TestWellKnownSupportUsesConfiguredContacts pins the configured shape,
// including the role default and dropping unreachable contacts.
func TestWellKnownSupportUsesConfiguredContacts(t *testing.T) {
	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Server.Support = config.Support{
			SupportPage: "https://example.org/support.html",
			Contacts: []config.SupportContact{
				{Role: "m.role.admin", MatrixID: "@admin:example.org", EmailAddress: "admin@example.org"},
				{Role: "m.role.security", EmailAddress: "security@example.org"},
				{MatrixID: "@fallback:example.org"}, // role defaults to m.role.admin
				{EmailAddress: "   ", MatrixID: ""}, // unreachable, must be dropped
			},
		}
	})
	seedAccount(t, st)

	code, body, _ := getWellKnown(t, s)
	if code != http.StatusOK {
		t.Fatalf("status %d, want 200: %v", code, body)
	}
	if got := body["support_page"]; got != "https://example.org/support.html" {
		t.Fatalf("support_page = %v", got)
	}

	contacts := body["contacts"].([]any)
	if len(contacts) != 3 {
		t.Fatalf("got %d contacts, want 3 (one was unreachable): %v", len(contacts), contacts)
	}
	first := contacts[0].(map[string]any)
	if first["role"] != "m.role.admin" || first["matrix_id"] != "@admin:example.org" ||
		first["email_address"] != "admin@example.org" {
		t.Fatalf("first contact = %v", first)
	}
	if second := contacts[1].(map[string]any); second["role"] != "m.role.security" {
		t.Fatalf("second contact = %v", second)
	}
	third := contacts[2].(map[string]any)
	if third["role"] != "m.role.admin" {
		t.Fatalf("a contact without a role must default to m.role.admin, got %v", third["role"])
	}
	if _, has := third["email_address"]; has {
		t.Fatalf("absent fields must be omitted, got %v", third)
	}
}

// TestWellKnownSupportExplicitEmptyList: `contacts: []` is the documented way to
// opt out of the derived admin contact.
func TestWellKnownSupportExplicitEmptyList(t *testing.T) {
	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Server.Support = config.Support{Contacts: []config.SupportContact{}}
	})
	seedAccount(t, st)

	code, body, _ := getWellKnown(t, s)
	if code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 when contacts is explicitly empty: %v", code, body)
	}
}

// TestWellKnownSupportPageOnly: support_page on its own is a valid document.
func TestWellKnownSupportPageOnly(t *testing.T) {
	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Server.Support = config.Support{
			SupportPage: "https://example.org/help",
			Contacts:    []config.SupportContact{},
		}
	})
	seedAccount(t, st)

	code, body, _ := getWellKnown(t, s)
	if code != http.StatusOK {
		t.Fatalf("status %d, want 200: %v", code, body)
	}
	if got := body["support_page"]; got != "https://example.org/help" {
		t.Fatalf("support_page = %v", got)
	}
	if _, has := body["contacts"]; has {
		t.Fatalf("contacts must be omitted when empty: %v", body)
	}
}
