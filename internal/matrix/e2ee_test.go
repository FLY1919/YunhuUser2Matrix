package matrix

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/hub"
	"github.com/yunhu/yhmatrix/internal/store"
)

// doJSON calls an authenticated endpoint through the real router.
func doJSON(t *testing.T, s *Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
	}
	return rec.Code, out
}

const testDeviceKeys = `{
  "user_id": "@yh_10001:test.local",
  "device_id": "D1",
  "algorithms": ["m.olm.v1.curve25519-aes-sha2", "m.megolm.v1.aes-sha2"],
  "keys": {
    "curve25519:D1": "curve25519pub",
    "ed25519:D1": "ed25519pub"
  },
  "signatures": {"@yh_10001:test.local": {"ed25519:D1": "selfsig"}}
}`

// TestKeysUploadThenQuery covers the round trip Element X needs to see its own
// device. The old stub returned an empty device_keys map, so the client believed
// it had no device and never finished the identity step.
func TestKeysUploadThenQuery(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	code, resp := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload",
		`{"device_keys":`+testDeviceKeys+`,"one_time_keys":{"signed_curve25519:AAAAAQ":{"key":"k1"}}}`)
	if code != http.StatusOK {
		t.Fatalf("upload status %d: %v", code, resp)
	}
	counts, _ := resp["one_time_key_counts"].(map[string]any)
	if counts["signed_curve25519"] != float64(1) {
		t.Fatalf("one_time_key_counts = %v, want one signed_curve25519", resp["one_time_key_counts"])
	}

	code, resp = doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/query",
		`{"device_keys":{"@yh_10001:test.local":[]}}`)
	if code != http.StatusOK {
		t.Fatalf("query status %d: %v", code, resp)
	}
	byUser, _ := resp["device_keys"].(map[string]any)
	devices, _ := byUser[acc.UserID].(map[string]any)
	if _, ok := devices["D1"]; !ok {
		t.Fatalf("own device missing from /keys/query: %v", resp["device_keys"])
	}
}

// TestKeysQueryHonoursDeviceFilter makes sure a client asking for a subset gets
// only that subset, which is what Synapse does and what clients rely on when
// they refresh a single session.
func TestKeysQueryHonoursDeviceFilter(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", `{"device_keys":`+testDeviceKeys+`}`)
	_, resp := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/query",
		`{"device_keys":{"@yh_10001:test.local":["OTHER"]}}`)

	byUser, _ := resp["device_keys"].(map[string]any)
	if devices, ok := byUser[acc.UserID]; ok {
		t.Fatalf("a non-matching device filter must return nothing, got %v", devices)
	}
}

// TestCrossSigningUploadAndSignDevice is the path that unblocks the client:
// bootstrap cross-signing keys, then attach the self-signing signature to the
// device, and check /keys/query hands both back.
func TestCrossSigningUploadAndSignDevice(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", `{"device_keys":`+testDeviceKeys+`}`)

	code, resp := doJSON(t, s, http.MethodPost,
		"/_matrix/client/v3/keys/device_signing/upload",
		`{"master_key":{"user_id":"@yh_10001:test.local","usage":["master"],`+
			`"keys":{"ed25519:master1":"masterpub"},"signatures":{}},`+
			`"self_signing_key":{"user_id":"@yh_10001:test.local","usage":["self_signing"],`+
			`"keys":{"ed25519:self1":"selfpub"},"signatures":{}}}`)
	if code != http.StatusOK {
		t.Fatalf("device_signing upload status %d: %v", code, resp)
	}

	// The client then signs its own device with the self-signing key.
	signed := `{"@yh_10001:test.local":{"D1":` + testDeviceKeys + `}}`
	code, resp = doJSON(t, s, http.MethodPost,
		"/_matrix/client/v3/keys/signatures/upload", signed)
	if code != http.StatusOK {
		t.Fatalf("signature upload status %d: %v", code, resp)
	}
	if failures, _ := resp["failures"].(map[string]any); len(failures) != 0 {
		t.Fatalf("unexpected failures: %v", failures)
	}

	_, resp = doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/query",
		`{"device_keys":{"@yh_10001:test.local":[]}}`)

	for _, key := range []string{"master_keys", "self_signing_keys"} {
		m, _ := resp[key].(map[string]any)
		if _, ok := m[acc.UserID]; !ok {
			t.Fatalf("%s missing after upload: %v", key, resp)
		}
	}
	byUser, _ := resp["device_keys"].(map[string]any)
	devices, _ := byUser[acc.UserID].(map[string]any)
	dev, _ := devices["D1"].(map[string]any)
	if _, ok := dev["signatures"]; !ok {
		t.Fatalf("device lost its signatures: %v", dev)
	}
}

// TestSignatureUploadMergesIntoExistingDevice checks that a second signature
// (another session signing this device) does not wipe the first one.
func TestSignatureUploadMergesIntoExistingDevice(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", `{"device_keys":`+testDeviceKeys+`}`)

	// A user-signing signature arrives for the same device.
	extra := `{"@yh_10001:test.local":{"D1":{"device_id":"D1","signatures":` +
		`{"@yh_10001:test.local":{"ed25519:self1":"crosssig"}}}}}`
	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/signatures/upload", extra)

	s.Store.Mu.RLock()
	raw := acc.E2EE.Devices["D1"]
	s.Store.Mu.RUnlock()

	var dev struct {
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(raw, &dev); err != nil {
		t.Fatalf("decode stored device: %v", err)
	}
	sigs := dev.Signatures[acc.UserID]
	if sigs["ed25519:D1"] != "selfsig" {
		t.Fatalf("original signature lost: %v", sigs)
	}
	if sigs["ed25519:self1"] != "crosssig" {
		t.Fatalf("new signature not merged: %v", sigs)
	}
}

// TestKeysClaimConsumesOneTimeKey covers the claim/consume contract.
func TestKeysClaimConsumesOneTimeKey(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload",
		`{"device_keys":`+testDeviceKeys+`,"one_time_keys":{"signed_curve25519:AAAAAQ":{"key":"k1"}}}`)

	_, resp := doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/claim",
		`{"one_time_keys":{"@yh_10001:test.local":{"D1":"signed_curve25519"}}}`)
	byUser, _ := resp["one_time_keys"].(map[string]any)
	byDevice, _ := byUser[acc.UserID].(map[string]any)
	keys, _ := byDevice["D1"].(map[string]any)
	if _, ok := keys["signed_curve25519:AAAAAQ"]; !ok {
		t.Fatalf("claim did not return the key: %v", resp["one_time_keys"])
	}

	// A second claim must come back empty: one-time keys are single use.
	_, resp = doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/claim",
		`{"one_time_keys":{"@yh_10001:test.local":{"D1":"signed_curve25519"}}}`)
	if byUser, _ := resp["one_time_keys"].(map[string]any); len(byUser) != 0 {
		t.Fatalf("one-time key was handed out twice: %v", byUser)
	}
}

// TestAccountDataPersists covers secret storage: the client writes
// m.secret_storage.default_key and must read it back, including through sync.
func TestAccountDataPersists(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	code, _ := doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/user/@yh_10001:test.local/account_data/m.secret_storage.default_key",
		`{"key":"abc123"}`)
	if code != http.StatusOK {
		t.Fatalf("put account data status %d", code)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/user/@yh_10001:test.local/account_data/m.secret_storage.default_key", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get account data status %d: %s", rec.Code, rec.Body.String())
	}
	var stored map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &stored)
	if stored["key"] != "abc123" {
		t.Fatalf("account data round trip lost content: %v", stored)
	}

	// And it must appear in sync, or the client never learns about it.
	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 10)
	s.Store.Mu.RUnlock()
	ad, _ := resp["account_data"].(map[string]any)
	events, _ := ad["events"].([]map[string]any)
	if len(events) != 1 || events[0]["type"] != "m.secret_storage.default_key" {
		t.Fatalf("account data missing from sync: %v", ad["events"])
	}
}

// TestSyncReportsDeviceListChange is what makes a client re-query /keys/query
// after it uploads keys. Without it, a freshly uploaded device stays invisible
// to its own session.
func TestSyncReportsDeviceListChange(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 10)
	s.Store.Mu.RUnlock()
	if changed, _ := resp["device_lists"].(map[string]any)["changed"].([]string); len(changed) != 0 {
		t.Fatalf("no upload yet, so nothing should have changed: %v", changed)
	}

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", `{"device_keys":`+testDeviceKeys+`}`)

	s.Store.Mu.RLock()
	resp, _ = s.buildSync(acc, nil, 0, true, 10)
	s.Store.Mu.RUnlock()
	changed, _ := resp["device_lists"].(map[string]any)["changed"].([]string)
	if len(changed) != 1 || changed[0] != acc.UserID {
		t.Fatalf("device list change not reported: %v", resp["device_lists"])
	}
}

// TestKeysChangesHonoursFrom checks the /keys/changes cursor.
func TestKeysChangesHonoursFrom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", `{"device_keys":`+testDeviceKeys+`}`)

	s.Store.Mu.RLock()
	pos := acc.E2EE.DeviceListPos
	s.Store.Mu.RUnlock()

	_, resp := doJSON(t, s, http.MethodGet,
		"/_matrix/client/v3/keys/changes?from=s"+strconv.FormatInt(pos, 10)+"&to=s"+strconv.FormatInt(pos+10, 10), "")
	if changed, _ := resp["changed"].([]any); len(changed) != 0 {
		t.Fatalf("nothing changed after the cursor: %v", changed)
	}

	_, resp = doJSON(t, s, http.MethodGet, "/_matrix/client/v3/keys/changes?from=s0&to=s99", "")
	changed, _ := resp["changed"].([]any)
	if len(changed) != 1 || changed[0] != acc.UserID {
		t.Fatalf("change before the cursor not reported: %v", resp["changed"])
	}
}

// TestAccountDataSurvivesReload makes sure secret storage is persisted, not
// just held in memory: a restart used to drop it, so the client re-entered the
// identity flow on every launch.
func TestAccountDataSurvivesReload(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()
	cfg.Storage.DataDir = dir
	log := discardLogger()

	st, err := store.Open(dir, time.Hour, 1000, log)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	s, err := NewServer(cfg, st, hub.New(), log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/user/@yh_10001:test.local/account_data/m.secret_storage.default_key",
		`{"key":"persist-me"}`)

	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	st2, err := store.Open(dir, time.Hour, 1000, log)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	acc2 := st2.GetByYunhuID(acc.YunhuID)
	if acc2 == nil {
		t.Fatal("account missing after reload")
	}
	entry := acc2.EnsureE2EE().AccountData["m.secret_storage.default_key"]
	if entry == nil {
		t.Fatal("secret storage was not persisted")
	}
	var content map[string]any
	if err := json.Unmarshal(entry.Content, &content); err != nil {
		t.Fatalf("decode persisted content: %v", err)
	}
	if content["key"] != "persist-me" {
		t.Fatalf("persisted content lost: %v", content)
	}
}

// discardLogger silences logs in tests.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
