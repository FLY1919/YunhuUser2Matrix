package matrix

import (
	"net/http"
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
)

// TestReceiptEchoesBackAsEphemeral is the "怎么返回 {}" question.
//
// {} is the correct response body - Matrix defines POST /receipt as returning an
// empty object. The real problem was what happened *after*: the receipt was
// stored but never echoed, so a client that marked a room read had no way to see
// its position come back. That is what moves the unread divider (and clears the
// badge on a second device).
func TestReceiptEchoesBackAsEphemeral(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))
	s.ingest(acc, "push_message", pushMsg("m2", 2, "two", 1700000001000))

	// Baseline: the client has caught up.
	first := syncOnce(t, s, "timeout=0")
	nextBatch, _ := first["next_batch"].(string)

	s.Store.Mu.RLock()
	var newestID string
	for _, ev := range acc.Rooms[dmRoom].Events {
		if !ev.IsState() {
			newestID = ev.EventID
		}
	}
	s.Store.Mu.RUnlock()
	if newestID == "" {
		t.Fatal("no timeline event to acknowledge")
	}

	// The client marks the room read. The response body is {} by definition.
	req := newAuthRequest("POST",
		"/_matrix/client/v3/rooms/"+dmRoom+"/receipt/m.read/"+newestID, "{}")
	rec := serve(s, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if body := decodeJSON(t, rec); len(body) != 0 {
		t.Fatalf("body = %v, want {} (that is what the spec says)", body)
	}

	// The next sync must report the position back as m.receipt.
	second := syncOnce(t, s, "timeout=0&since="+nextBatch)
	room := roomIn(t, second, dmRoom)
	if room == nil {
		t.Fatalf("the read change was not delivered: %v", second)
	}
	ephemeral, _ := room["ephemeral"].(map[string]any)["events"].([]any)
	if len(ephemeral) == 0 {
		t.Fatal("no ephemeral events after a receipt")
	}

	var found map[string]any
	for _, raw := range ephemeral {
		ev, _ := raw.(map[string]any)
		if asString(ev["type"]) == "m.receipt" {
			found = ev
		}
	}
	if found == nil {
		t.Fatalf("no m.receipt in ephemeral: %v", ephemeral)
	}

	content, _ := found["content"].(map[string]any)
	perEvent, _ := content[newestID].(map[string]any)
	if perEvent == nil {
		t.Fatalf("m.receipt does not name the acknowledged event %s: %v", newestID, content)
	}
	read, _ := perEvent["m.read"].(map[string]any)
	perUser, _ := read[acc.UserID].(map[string]any)
	if perUser == nil {
		t.Fatalf("m.receipt has no entry for %s: %v", acc.UserID, read)
	}
	if _, ok := perUser["ts"]; !ok {
		t.Fatalf("m.receipt entry has no ts: %v", perUser)
	}
}

// TestInitialSyncCarriesTheReadPosition: a fresh client has no idea where the
// account has read up to, so the first sync has to say.
func TestInitialSyncCarriesTheReadPosition(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))

	s.Store.Mu.RLock()
	eventID := acc.Rooms[dmRoom].Events[0].EventID
	s.Store.Mu.RUnlock()
	req := newAuthRequest("POST",
		"/_matrix/client/v3/rooms/"+dmRoom+"/receipt/m.read/"+eventID, "{}")
	if rec := serve(s, req); rec.Code != http.StatusOK {
		t.Fatalf("receipt status %d", rec.Code)
	}

	body := syncOnce(t, s, "timeout=0")
	room := roomIn(t, body, dmRoom)
	if room == nil {
		t.Fatalf("room missing: %v", body)
	}
	ephemeral, _ := room["ephemeral"].(map[string]any)["events"].([]any)
	var hasReceipt bool
	for _, raw := range ephemeral {
		if ev, _ := raw.(map[string]any); asString(ev["type"]) == "m.receipt" {
			hasReceipt = true
		}
	}
	if !hasReceipt {
		t.Fatalf("initial sync did not report the read position: %v", ephemeral)
	}
}

// TestReceiptForUnknownRoomIsRejected keeps the endpoint honest.
func TestReceiptForUnknownRoomIsRejected(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	req := newAuthRequest("POST",
		"/_matrix/client/v3/rooms/!yh-group-1:test.local/receipt/m.read/$x", "{}")
	rec := serve(s, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
}

// TestReadMarkerIsStoredAsAnEventID: m.fully_read has to name a real event. A
// client that sends a bare position token ("s42") must not leave that token in the
// field, or its own state looks broken.
func TestReadMarkerIsStoredAsAnEventID(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))

	s.Store.Mu.Lock()
	s.markReadLocked(acc, room, "s1")
	marker := room.ReadMarker
	ts := room.ReadMarkerTS
	s.Store.Mu.Unlock()

	if marker == "" || marker[0] != '$' {
		t.Fatalf("ReadMarker = %q, want an event id", marker)
	}
	if ts == 0 {
		t.Fatal("ReadMarkerTS was not stamped")
	}
}
