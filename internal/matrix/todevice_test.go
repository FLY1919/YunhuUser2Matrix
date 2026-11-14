package matrix

import (
	"net/http"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
)

// addDevice registers a second session so to-device addressing can be tested.
func addDevice(t *testing.T, st *store.Store, acc *model.Account, id string) *model.Device {
	t.Helper()
	dev := &model.Device{ID: id, Token: "tok-" + id, LastSeen: time.Now().UnixMilli()}
	st.AddDevice(acc, dev)
	return dev
}

// TestSendToDeviceDeliversToAddressedSession covers the channel session
// verification runs on: the message must reach the target device exactly once.
func TestSendToDeviceDeliversToAddressedSession(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	other := addDevice(t, st, acc, "D2")

	code, _ := doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/sendToDevice/m.key.verification.request/txn1",
		`{"messages":{"@yh_10001:test.local":{"D2":{"from_device":"D1","transaction_id":"txn1"}}}}`)
	if code != http.StatusOK {
		t.Fatalf("sendToDevice status %d", code)
	}

	// The addressed device receives it.
	s.Store.Mu.RLock()
	events := pendingToDevice(acc, other)
	s.Store.Mu.RUnlock()
	if len(events) != 1 {
		t.Fatalf("D2 got %d events, want 1", len(events))
	}
	if events[0]["type"] != "m.key.verification.request" {
		t.Fatalf("event type = %v", events[0]["type"])
	}
	if events[0]["sender"] != acc.UserID {
		t.Fatalf("sender = %v, want %v", events[0]["sender"], acc.UserID)
	}

	// And only once: the cursor advanced.
	s.Store.Mu.RLock()
	again := pendingToDevice(acc, other)
	s.Store.Mu.RUnlock()
	if len(again) != 0 {
		t.Fatalf("event delivered twice: %v", again)
	}

	// The sending device must not see a message addressed to another session.
	s.Store.Mu.RLock()
	self := pendingToDevice(acc, acc.Devices["D1"])
	s.Store.Mu.RUnlock()
	if len(self) != 0 {
		t.Fatalf("D1 received D2's message: %v", self)
	}
}

// TestSendToDeviceWildcardReachesEverySession covers the "*" addressing used for
// requests that should fan out to all of a user's sessions.
func TestSendToDeviceWildcardReachesEverySession(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	d1 := seedDeviceAndReturn(t, st, acc)
	d2 := addDevice(t, st, acc, "D2")

	doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/sendToDevice/m.key.verification.done/txn2",
		`{"messages":{"@yh_10001:test.local":{"*":{"transaction_id":"txn2"}}}}`)

	s.Store.Mu.RLock()
	got1 := pendingToDevice(acc, d1)
	got2 := pendingToDevice(acc, d2)
	s.Store.Mu.RUnlock()
	if len(got1) != 1 || len(got2) != 1 {
		t.Fatalf("wildcard delivery: D1=%d D2=%d, want 1 each", len(got1), len(got2))
	}
}

// TestSyncCarriesToDevice makes sure the queue is actually surfaced through
// /sync, which is how a client learns the other device answered.
func TestSyncCarriesToDevice(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	dev := seedDeviceAndReturn(t, st, acc)

	doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/sendToDevice/m.key.verification.accept/txn3",
		`{"messages":{"@yh_10001:test.local":{"D1":{"transaction_id":"txn3"}}}}`)

	s.Store.Mu.RLock()
	resp, has := s.buildSync(acc, dev, 0, true, 30)
	s.Store.Mu.RUnlock()
	if !has {
		t.Fatal("a pending to-device event must count as content")
	}
	events, _ := resp["to_device"].(map[string]any)["events"].([]map[string]any)
	if len(events) != 1 || events[0]["type"] != "m.key.verification.accept" {
		t.Fatalf("to_device = %v", resp["to_device"])
	}

	// Second sync must be empty again.
	s.Store.Mu.RLock()
	resp, _ = s.buildSync(acc, dev, 0, true, 30)
	s.Store.Mu.RUnlock()
	if events, _ := resp["to_device"].(map[string]any)["events"].([]map[string]any); len(events) != 0 {
		t.Fatalf("to_device re-delivered: %v", events)
	}
}

// TestSlidingCarriesToDevice covers the same queue over sliding sync, which is
// the only transport Element X uses.
func TestSlidingCarriesToDevice(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	dev := seedDeviceAndReturn(t, st, acc)

	doJSON(t, s, http.MethodPut,
		"/_matrix/client/v3/sendToDevice/m.key.verification.start/txn4",
		`{"messages":{"@yh_10001:test.local":{"D1":{"transaction_id":"txn4"}}}}`)

	req := mustSliding(t, `{"extensions":{"to_device":{"enabled":true}}}`)
	s.Store.Mu.RLock()
	resp, has := s.buildSliding(acc, dev, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	// A to-device event alone must count as content, otherwise the long poll
	// keeps waiting, the cursor has already advanced, and the verification
	// message is silently dropped.
	if !has {
		t.Fatal("a pending to-device event must count as sliding sync content")
	}
	ext, _ := resp["extensions"].(map[string]any)["to_device"].(map[string]any)
	events, _ := ext["events"].([]map[string]any)
	if len(events) != 1 || events[0]["type"] != "m.key.verification.start" {
		t.Fatalf("sliding to_device = %v", ext)
	}

	// Once delivered, an otherwise unchanged account has nothing to report.
	s.Store.Mu.RLock()
	_, has = s.buildSliding(acc, dev, req, 0, false, false, nil)
	s.Store.Mu.RUnlock()
	if has {
		t.Fatal("nothing left to deliver, so this must be an empty delta")
	}
}

// TestRepeatedKeysUploadDoesNotBumpDeviceList is the loop guard: clients
// re-upload identical device keys on every launch, and if that advanced the
// stream /sync would report a change forever and never long-poll.
func TestRepeatedKeysUploadDoesNotBumpDeviceList(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	body := `{"device_keys":` + testDeviceKeys + `}`
	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", body)

	s.Store.Mu.RLock()
	first := acc.E2EE.DeviceListPos
	s.Store.Mu.RUnlock()
	if first == 0 {
		t.Fatal("the first upload must bump the device list")
	}

	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", body)
	doJSON(t, s, http.MethodPost, "/_matrix/client/v3/keys/upload", body)

	s.Store.Mu.RLock()
	again := acc.E2EE.DeviceListPos
	s.Store.Mu.RUnlock()
	if again != first {
		t.Fatalf("identical uploads advanced the stream: %d -> %d", first, again)
	}
}

// TestRepeatedAccountDataDoesNotBumpStream is the same guard for secret storage.
func TestRepeatedAccountDataDoesNotBumpStream(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	path := "/_matrix/client/v3/user/@yh_10001:test.local/account_data/m.secret_storage.default_key"
	doJSON(t, s, http.MethodPut, path, `{"key":"abc"}`)

	s.Store.Mu.RLock()
	first := acc.Pos
	s.Store.Mu.RUnlock()

	doJSON(t, s, http.MethodPut, path, `{"key":"abc"}`)

	s.Store.Mu.RLock()
	again := acc.Pos
	s.Store.Mu.RUnlock()
	if again != first {
		t.Fatalf("identical account data advanced the stream: %d -> %d", first, again)
	}
}

// seedDeviceAndReturn is seedDevice but hands back the device struct.
func seedDeviceAndReturn(t *testing.T, st *store.Store, acc *model.Account) *model.Device {
	t.Helper()
	seedDevice(t, st, acc)
	return acc.Devices["D1"]
}
