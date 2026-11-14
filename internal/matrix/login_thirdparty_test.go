package matrix

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestLoginExactFluffyChatRequest replays the request from the bug report
// verbatim, including the trailing ":localhost" domain that FluffyChat appends
// to a third-party address. This is the whole point of the fix: the identifier
// lives in identifier.address and the caller's homeserver name is not what the
// client wrote.
func TestLoginExactFluffyChatRequest(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	acc.Email = "241638640@qq.com"
	st.PutAccount(acc)
	seedDevice(t, st, acc)

	const body = `{"identifier":{"type":"m.id.thirdparty","medium":"email","address":"@241638640@qq.com:localhost"},"initial_device_display_name":"FluffyChat android","password":"ASdf114514","refresh_token":false,"type":"m.login.password"}`

	req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Dart/3.12 (dart:io)")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got, _ := out["user_id"].(string); got != acc.UserID {
		t.Fatalf("user_id = %v, want %s", got, acc.UserID)
	}
	if got, _ := out["access_token"].(string); got == "" {
		t.Fatalf("no access token: %v", out)
	}
	if got, _ := out["device_id"].(string); got == "" {
		t.Fatalf("no device id: %v", out)
	}
}

// TestLoginThirdPartyUnknownEmailFails: wrapping must not make an unknown
// address resolve. A typo has to reach Yunhu and fail there, not log the user
// into somebody else's account.
func TestLoginThirdPartyUnknownEmailFails(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	acc.Email = "241638640@qq.com"
	st.PutAccount(acc)
	seedDevice(t, st, acc)

	code, body := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/login",
		thirdPartyLoginBody("email", "@other@example.com:localhost", "x"))
	if code == http.StatusOK {
		t.Fatalf("an unknown email logged in: %v", body)
	}
	if got, _ := body["user_id"].(string); got != "" {
		t.Fatalf("a failed login returned a user_id: %v", body)
	}
}
