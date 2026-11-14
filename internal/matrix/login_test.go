package matrix

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/config"
)

// loginBody builds an m.login.password request the way clients send it.
func loginBody(identifier, password string) string {
	return `{"type":"m.login.password","identifier":{"type":"m.id.user","user":"` +
		identifier + `"},"password":"` + password + `","initial_device_display_name":"test"}`
}

// TestLoginWithMatrixIDReusesExistingAccount is the reported bug: entering the
// Matrix ID of an account that already exists here used to be treated as a
// numeric Yunhu id, classified as a phone number, and pushed into the SMS flow.
// The login must be answered locally instead — no upstream round trip, and the
// Yunhu id "10001" is deliberately all digits.
func TestLoginWithMatrixIDReusesExistingAccount(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	for _, identifier := range []string{
		"@yh_10001:test.local", // full Matrix ID
		"yh_10001",             // localpart
		"10001",                // bare Yunhu id, all digits
	} {
		code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
			loginBody(identifier, "whatever"))

		if code != http.StatusOK {
			t.Fatalf("%q: status %d, want 200 (body: %v)", identifier, code, body)
		}
		if got, _ := body["user_id"].(string); got != acc.UserID {
			t.Fatalf("%q: user_id = %v, want %s", identifier, got, acc.UserID)
		}
		if tok, _ := body["access_token"].(string); tok == "" {
			t.Fatalf("%q: no access token in the response", identifier)
		}
		if got, _ := body["device_id"].(string); got == "" {
			t.Fatalf("%q: no device id in the response", identifier)
		}
	}
}

// thirdPartyLoginBody is what FluffyChat actually sends when the user picks the
// email login: the address goes into identifier.address, not identifier.user.
func thirdPartyLoginBody(medium, address, password string) string {
	return `{"type":"m.login.password","identifier":{"type":"m.id.thirdparty","medium":"` +
		medium + `","address":"` + address + `"},"password":"` + password +
		`","initial_device_display_name":"FluffyChat android","refresh_token":false}`
}

// TestLoginWithThirdPartyEmailIdentifier is the reported bug:
//
//	{"identifier":{"type":"m.id.thirdparty","medium":"email",
//	               "address":"@241638640@qq.com:localhost"}, ...}
//
// Only identifier.user was read, so the username came out empty and the whole
// request failed. The address may also be wrapped as if it were a Matrix user ID
// (leading "@" and a trailing ":server"), which is a client-side confusion that
// still has to resolve to the Yunhu account.
func TestLoginWithThirdPartyEmailIdentifier(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	acc.Email = "241638640@qq.com"
	st.PutAccount(acc)
	seedDevice(t, st, acc)

	for _, address := range []string{
		"241638640@qq.com",              // plain address
		"@241638640@qq.com:localhost",   // FluffyChat wraps it like a user ID
		"@241638640@qq.com:127.0.0.1:8008", // …with the LAN host as the domain
	} {
		code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
			thirdPartyLoginBody("email", address, "ASdf114514"))

		if code != http.StatusOK {
			t.Fatalf("%q: status %d, want 200 (body: %v)", address, code, body)
		}
		if got, _ := body["user_id"].(string); got != acc.UserID {
			t.Fatalf("%q: user_id = %v, want %s", address, got, acc.UserID)
		}
	}
}

// TestLoginThirdPartyMsisdn covers the phone shape of the same identifier type.
func TestLoginThirdPartyMsisdn(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	acc.Phone = "13800138000"
	st.PutAccount(acc)
	seedDevice(t, st, acc)

	for _, address := range []string{"13800138000", "+13800138000"} {
		code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
			thirdPartyLoginBody("msisdn", address, "x"))
		if code != http.StatusOK {
			t.Fatalf("%q: status %d, want 200 (body: %v)", address, code, body)
		}
	}
}

// TestIdentifierStringShapes pins the extraction rules for every shape a client
// has been seen to send.
func TestIdentifierStringShapes(t *testing.T) {
	s, _ := newTestServer(t)

	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "m.id.user",
			body: `{"identifier":{"type":"m.id.user","user":"@yh_10001:test.local"}}`,
			want: "@yh_10001:test.local",
		},
		{
			name: "legacy user field",
			body: `{"user":"someone@example.com"}`,
			want: "someone@example.com",
		},
		{
			name: "third-party email",
			body: `{"identifier":{"type":"m.id.thirdparty","medium":"email","address":"someone@example.com"}}`,
			want: "someone@example.com",
		},
		{
			name: "third-party email wrapped as a user id",
			body: `{"identifier":{"type":"m.id.thirdparty","medium":"email","address":"@someone@example.com:test.local"}}`,
			want: "someone@example.com",
		},
		{
			name: "third-party msisdn keeps the digits",
			body: `{"identifier":{"type":"m.id.thirdparty","medium":"msisdn","address":"+13800138000"}}`,
			want: "13800138000",
		},
		{
			name: "email field",
			body: `{"type":"com.yunhu.email","email":"someone@example.com"}`,
			want: "someone@example.com",
		},
	}

	for _, tc := range cases {
		var req loginRequest
		if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
			t.Fatalf("%s: decode: %v", tc.name, err)
		}
		if got := s.identifierString(&req); got != tc.want {
			t.Errorf("%s: identifierString = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestLoginWithMatrixIDMintsDistinctDevices makes sure a second device gets its
// own session instead of hijacking the first one.
func TestLoginWithMatrixIDMintsDistinctDevices(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	_, first := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		loginBody("@yh_10001:test.local", "x"))
	_, second := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		loginBody("@yh_10001:test.local", "x"))

	if first["access_token"] == second["access_token"] {
		t.Fatal("the second login reused the first access token")
	}
	if first["device_id"] == second["device_id"] {
		t.Fatal("the second login reused the first device id")
	}

	s.Store.Mu.RLock()
	devices := len(acc.Devices)
	s.Store.Mu.RUnlock()
	if devices != 3 { // the seeded D1 plus the two new ones
		t.Fatalf("account has %d devices, want 3", devices)
	}
}

// TestLoginReloginCanRequireToken covers the hardened mode: with allow_relogin
// off, the password has to be the account's stored Yunhu token.
func TestLoginReloginCanRequireToken(t *testing.T) {
	s, st := newTestServerWith(t, func(cfg *config.Config) { cfg.Server.AllowRelogin = false })
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	code, _ := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		loginBody("@yh_10001:test.local", "wrong"))
	if code != http.StatusForbidden {
		t.Fatalf("status %d with a wrong password, want 403", code)
	}

	code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		loginBody("@yh_10001:test.local", acc.Token))
	if code != http.StatusOK {
		t.Fatalf("status %d with the stored token as password, want 200 (%v)", code, body)
	}
}

// TestLoginUnknownIdentifierStillGoesUpstream guards the other direction: an
// identifier that is not a local account must not silently create one.
func TestLoginUnknownIdentifierStillGoesUpstream(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	// The test config points Yunhu at a closed port, so this fails fast.
	code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		loginBody("@yh_99999:test.local", "x"))
	if code != http.StatusForbidden {
		t.Fatalf("status %d for an unknown user, want 403 (body: %v)", code, body)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, "云湖") && !strings.Contains(msg, "connect") {
		t.Fatalf("error should explain the upstream failure, got %q", msg)
	}
}

// TestLocalAccountForMatchesStoredIdentities pins the resolution rules.
func TestLocalAccountForMatchesStoredIdentities(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	acc.Phone = "13800138000"
	acc.Email = "me@example.com"
	st.PutAccount(acc)

	cases := []string{
		"@yh_10001:test.local",
		"yh_10001",
		"10001",
		"13800138000",
		"me@example.com",
		// A domain that is not ours still means the local account: there is no
		// federation here.
		"@yh_10001:192.168.1.12:8008",
		// Garbage a client produces when the full ID is pasted into a field that
		// already appends "@" and ":server".
		"@//yh_10001/:test.local",
	}
	for _, in := range cases {
		got := s.localAccountFor(in)
		if got == nil || got.UserID != acc.UserID {
			t.Errorf("localAccountFor(%q) = %v, want %s", in, got, acc.UserID)
		}
	}

	for _, in := range []string{"", "@yh_12345:test.local", "someone@example.com"} {
		if got := s.localAccountFor(in); got != nil {
			t.Errorf("localAccountFor(%q) = %v, want nil", in, got.UserID)
		}
	}
}

// TestLoginFlowsAdvertisePassword keeps clients from hiding the username form:
// the local relogin rides on m.login.password.
func TestLoginFlowsAdvertisePassword(t *testing.T) {
	s, _ := newTestServer(t)
	code, body := doJSON(t, s, http.MethodGet, "/_matrix/client/v3/login", "")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	flows, _ := body["flows"].([]any)
	var hasPassword bool
	for _, f := range flows {
		if m, ok := f.(map[string]any); ok && m["type"] == "m.login.password" {
			hasPassword = true
		}
	}
	if !hasPassword {
		t.Fatalf("m.login.password is missing from %v", flows)
	}
}
