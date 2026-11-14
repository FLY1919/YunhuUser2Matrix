package matrix

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// newAuthRequest builds an authenticated request for the real router.
func newAuthRequest(method, path, body string) *http.Request {
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Authorization", "Bearer tok")
	return r
}

// serve runs a request through the server and returns the recorder.
func serve(s *Server, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes a recorder body.
func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	return out
}

// newMidSeqServer builds a homeserver whose upstream answers the message lookup
// used to re-read an edited message.
func newMidSeqServer(t *testing.T, f *fakeMidSeq) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// TestEmptyEditContentKeepsTheOldText: the edit payload is documented as partial,
// so its content object can be missing. Overwriting with that blanked a message
// the user could still read - and the marker then pointed at an empty bubble.
func TestEmptyEditContentKeepsTheOldText(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))

	// An edit with no content at all.
	s.ingest(acc, "edit_message", &pb.PushMessage{
		MsgId:         "m1",
		ChatId:        "10002",
		EditTimestamp: 1700000001000,
	})

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, dmRoom, "m1")
	body := asString(ev.Content["body"])
	edited := ev.EditedTS
	s.Store.Mu.RUnlock()

	if body != "原始内容" {
		t.Fatalf("body = %q, the edit blanked a readable message", body)
	}
	if edited == 0 {
		t.Fatal("the edit was not marked at all")
	}
}

// TestWsEditArrivesInIncrementalSync is the "没有已编辑标识" report: the client is
// already looking at the room, so it only ever sees an incremental /sync. If the
// edited event does not come back there, nothing tells it the text changed and no
// "edited" marker ever appears.
func TestWsEditArrivesInIncrementalSync(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))

	// The client has caught up to here.
	first := syncOnce(t, s, "timeout=0")
	nextBatch, _ := first["next_batch"].(string)

	// Now somebody edits the message upstream.
	s.ingest(acc, "edit_message", wsEditPush("m1", "改过的内容", 1700000001000))

	// The next incremental sync has to carry the edited event.
	second := syncOnce(t, s, "timeout=0&since="+nextBatch)
	room := roomIn(t, second, dmRoom)
	if room == nil {
		t.Fatalf("the room was not re-sent after an edit: %v", second)
	}

	timeline, _ := room["timeline"].(map[string]any)["events"].([]any)
	var found map[string]any
	for _, raw := range timeline {
		ev, _ := raw.(map[string]any)
		if asString(ev["event_id"]) == eventIDFor("m1") {
			found = ev
		}
	}
	if found == nil {
		t.Fatalf("the edited event is missing from the incremental timeline: %v", timeline)
	}

	// The body must already carry the new text...
	if got := asString(found["content"].(map[string]any)["body"]); got != "改过的内容" {
		t.Fatalf("body = %q", got)
	}
	// ...and the aggregate that makes a client draw the marker.
	unsigned, _ := found["unsigned"].(map[string]any)
	relations, _ := unsigned["m.relations"].(map[string]any)
	if relations["m.replace"] == nil {
		t.Fatalf("no unsigned.m.relations.m.replace on the edited event: %v", found)
	}
}

// TestEditEventIsResolvable: the aggregate points at a synthesised event id, and a
// client fetches it with GET /rooms/{id}/event/{edit_id}. A 404 there means the
// marker cannot be confirmed and some clients drop it.
func TestEditEventIsResolvable(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))
	s.ingest(acc, "edit_message", wsEditPush("m1", "改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, dmRoom, "m1")
	var editID string
	if ev != nil {
		editID = ev.EditEventID()
	}
	s.Store.Mu.RUnlock()
	if editID == "" {
		t.Fatal("no edit event id was derived")
	}

	req := newAuthRequest("GET", "/_matrix/client/v3/rooms/"+dmRoom+"/event/"+editID, "")
	rec := serve(s, req)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec)
	if got := asString(body["event_id"]); got != editID {
		t.Fatalf("event_id = %q, want %q", got, editID)
	}
	content, _ := body["content"].(map[string]any)
	newContent, _ := content["m.new_content"].(map[string]any)
	if got := asString(newContent["body"]); got != "改过的内容" {
		t.Fatalf("m.new_content.body = %q", got)
	}
}

// TestSlidingSyncCarriesEditAggregate: Element X uses sliding sync, where each
// room's timeline is rendered by the same eventJSON - so the aggregate has to be
// there too.
func TestSlidingSyncCarriesEditAggregate(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))
	s.ingest(acc, "edit_message", wsEditPush("m1", "改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	room := acc.Rooms[dmRoom]
	out, ok, _ := s.slidingRoom(acc.UserID, &roomPlan{
		room:          room,
		timelineLimit: 20,
		requiredState: []slidingStateReq{{Type: "m.room.name"}},
	}, 0, true, true)
	s.Store.Mu.RUnlock()

	if !ok {
		t.Fatal("the room was not rendered")
	}
	timeline, _ := out["timeline"].([]map[string]any)
	var found bool
	for _, ev := range timeline {
		if asString(ev["event_id"]) != eventIDFor("m1") {
			continue
		}
		unsigned, _ := ev["unsigned"].(map[string]any)
		relations, _ := unsigned["m.relations"].(map[string]any)
		if relations["m.replace"] == nil {
			t.Fatalf("sliding sync dropped the edit aggregate: %v", ev)
		}
		if got := asString(ev["content"].(map[string]any)["body"]); got != "改过的内容" {
			t.Fatalf("body = %q", got)
		}
		found = true
	}
	if !found {
		t.Fatal("the edited event is missing from the sliding sync timeline")
	}
}

// TestEditMarkerSurvivesTheRefetch: the push content is provisional and gets
// replaced by the REST copy. The second write must not clear EditedTS, or the
// marker disappears again a moment after appearing.
func TestEditMarkerSurvivesTheRefetch(t *testing.T) {
	f := &fakeMidSeq{fullText: "服务器上的完整内容"}
	s, acc := newMidSeqServer(t, f)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))
	s.ingest(acc, "edit_message", wsEditPush("m1", "被截断…", 1700000001000))

	waitFor(t, func() bool {
		s.Store.Mu.RLock()
		defer s.Store.Mu.RUnlock()
		ev := eventOf(t, acc, dmRoom, "m1")
		return ev != nil && asString(ev.Content["body"]) == "服务器上的完整内容"
	})

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, dmRoom, "m1")
	edited := ev.EditedTS
	id := ev.EditEventID()
	s.Store.Mu.RUnlock()

	if edited == 0 {
		t.Fatal("the refetch cleared EditedTS: the marker vanished again")
	}
	if id == "" {
		t.Fatal("the refetch cleared the edit event id")
	}
	// And the aggregate still resolves through /sync.
	body := syncOnce(t, s, "timeout=0")
	room := roomIn(t, body, dmRoom)
	timeline, _ := room["timeline"].(map[string]any)["events"].([]any)
	for _, raw := range timeline {
		e, _ := raw.(map[string]any)
		if asString(e["event_id"]) != eventIDFor("m1") {
			continue
		}
		unsigned, _ := e["unsigned"].(map[string]any)
		relations, _ := unsigned["m.relations"].(map[string]any)
		if relations["m.replace"] == nil {
			t.Fatalf("the aggregate disappeared after the refetch: %v", e)
		}
	}
}
