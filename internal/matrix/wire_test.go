package matrix

// The wire shape of a sync response is a contract with matrix-rust-sdk, which
// deserialises it into ruma types. ruma marks a few fields as required, and a
// missing one rejects the *whole* response: the client then treats every sync
// as a failure, drops into its offline loop (polling /versions with no backoff)
// and never advances its position. That is indistinguishable from "cannot
// connect to the server" and is exactly how Element X used to get stuck on
// "confirm your identity".
//
// These tests pin the required fields so the mistake cannot come back.

import (
	"testing"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// requiredSlidingFields mirrors ruma's msc4186::response::* structs:
//
//	Response.pos              required
//	List.count                required
//	ToDevice.next_batch       required whenever the extension is present
func assertSlidingWireShape(t *testing.T, resp map[string]any) {
	t.Helper()

	pos, ok := resp["pos"].(string)
	if !ok || pos == "" {
		t.Fatalf("pos is required and must be a non-empty string, got %#v", resp["pos"])
	}

	lists, ok := resp["lists"].(map[string]any)
	if !ok {
		t.Fatalf("lists must be an object, got %#v", resp["lists"])
	}
	for name, raw := range lists {
		entry, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("list %q must be an object", name)
		}
		if _, ok := entry["count"]; !ok {
			t.Fatalf("list %q is missing the required count field", name)
		}
	}

	ext, ok := resp["extensions"].(map[string]any)
	if !ok {
		return
	}
	if raw, present := ext["to_device"]; present {
		toDevice, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("to_device must be an object")
		}
		batch, ok := toDevice["next_batch"].(string)
		if !ok || batch == "" {
			t.Fatalf("to_device.next_batch is required when the extension is present, got %#v", toDevice["next_batch"])
		}
	}
}

func TestSlidingToDeviceExtensionCarriesNextBatch(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hello", 1700000000000)

	req := mustSliding(t, `{"lists":{"all_rooms":{"range":[0,19],"timeline_limit":10}},`+
		`"extensions":{"to_device":{"enabled":true},"e2ee":{"enabled":true},`+
		`"account_data":{"enabled":true},"typing":{"enabled":true},"receipts":{"enabled":true}}}`)

	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	assertSlidingWireShape(t, resp)

	ext := resp["extensions"].(map[string]any)
	toDevice := ext["to_device"].(map[string]any)
	if events, ok := toDevice["events"].([]map[string]any); ok && events == nil {
		t.Fatalf("events should serialise as [] rather than null")
	}
}

// An empty to_device section must still carry next_batch: the client enables the
// extension on every request, so this is the common path, not an edge case.
func TestSlidingEmptyToDeviceStillHasNextBatch(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	req := mustSliding(t, `{"lists":{"all_rooms":{"range":[0,19]}},"extensions":{"to_device":{"enabled":true}}}`)

	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	ext := resp["extensions"].(map[string]any)
	toDevice, ok := ext["to_device"].(map[string]any)
	if !ok {
		t.Fatalf("to_device extension missing: %#v", ext)
	}
	if _, ok := toDevice["next_batch"].(string); !ok {
		t.Fatalf("next_batch missing: %#v", toDevice)
	}
}

func TestSyncToDeviceCarriesNextBatch(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hello", 1700000000000)

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 50)
	s.Store.Mu.RUnlock()

	toDevice, ok := resp["to_device"].(map[string]any)
	if !ok {
		t.Fatalf("to_device missing from /sync: %#v", resp["to_device"])
	}
	if _, ok := toDevice["next_batch"].(string); !ok {
		t.Fatalf("/sync to_device.next_batch missing: %#v", toDevice)
	}
	if _, ok := resp["next_batch"].(string); !ok {
		t.Fatalf("/sync next_batch missing")
	}
}
