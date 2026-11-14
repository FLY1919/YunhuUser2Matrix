package matrix

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// TestContextEndpointIsRouted is the regression test for the error in the log:
//
//	GET /_matrix/client/v3/rooms/!yh-group-…/context/$yh_board_…?limit=30
//	resp: {"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}
//
// The route simply was not registered, so the request fell through to the
// catch-all. Element asks for /context when it opens the pinned-messages panel
// - which is where a Yunhu bot board is projected - so a pinned board rendered
// as an error instead of a message.
func TestContextEndpointIsRouted(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoard(acc, boardMsg("21385383", "看板正文", 1700000000000, pb.ContentTypeTextOnly_TEXT_text))

	s.Store.Mu.RLock()
	eventID := room.BoardEventByID("21385383").EventID
	s.Store.Mu.RUnlock()

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/"+room.RoomID+"/context/"+eventID+"?limit=30", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
		t.Fatalf("route is not registered: %s", body)
	}

	var out struct {
		Start        string           `json:"start"`
		Event        map[string]any   `json:"event"`
		EventsBefore []map[string]any `json:"events_before"`
		EventsAfter  []map[string]any `json:"events_after"`
		State        []map[string]any `json:"state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The board event itself must be resolvable: /context exists to render an
	// event the client never saw in a timeline.
	if got := asString(out.Event["event_id"]); got != eventID {
		t.Fatalf("event.event_id = %q, want %q", got, eventID)
	}
	if got := asString(out.Event["content"].(map[string]any)["body"]); got != "看板正文" {
		t.Fatalf("event body = %q", got)
	}
	if out.Start == "" {
		t.Fatal("start is required and must not be empty")
	}
	// Empty arrays, never null: ruma deserialises these as Vec<_> and a null
	// makes the whole response unparseable (clients then retry forever).
	if out.EventsBefore == nil {
		t.Fatalf("events_before must be [] not null: %s", rec.Body.String())
	}
	if out.EventsAfter == nil {
		t.Fatalf("events_after must be [] not null: %s", rec.Body.String())
	}
	if out.State == nil {
		t.Fatalf("state must be [] not null: %s", rec.Body.String())
	}
}

// TestContextAroundTimelineEvent: a normal message gets its neighbours, and
// events_before is reverse-chronological (the neighbour comes first).
func TestContextAroundTimelineEvent(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	for i, id := range []string{"m1", "m2", "m3"} {
		s.ingest(acc, "push_message", &pb.PushMessage{
			MsgId:       id,
			Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
			ChatId:      "10002",
			ChatType:    pb.ChatType_user,
			Content:     &pb.MsgContent{Text: id},
			ContentType: pb.ContentType_text,
			Timestamp:   uint64(1700000000000 + i*1000),
			MsgSeq:      uint64(i + 1),
		})
	}

	s.Store.Mu.RLock()
	room := acc.Rooms["!yh-user-10002:test.local"]
	mid := eventIDFor("m2")
	s.Store.Mu.RUnlock()

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/"+room.RoomID+"/context/"+mid+"?limit=10", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var out struct {
		EventsBefore []map[string]any `json:"events_before"`
		EventsAfter  []map[string]any `json:"events_after"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.EventsBefore) != 1 {
		t.Fatalf("events_before = %d, want 1", len(out.EventsBefore))
	}
	if got := asString(out.EventsBefore[0]["content"].(map[string]any)["body"]); got != "m1" {
		t.Fatalf("events_before[0] = %q, want m1 (nearest neighbour first)", got)
	}
	if len(out.EventsAfter) != 1 {
		t.Fatalf("events_after = %d, want 1", len(out.EventsAfter))
	}
	if got := asString(out.EventsAfter[0]["content"].(map[string]any)["body"]); got != "m3" {
		t.Fatalf("events_after[0] = %q, want m3", got)
	}
}

// TestContextUnknownEvent: an existing route answers M_NOT_FOUND, which is a
// different error from the M_UNRECOGNIZED an unrouted path produces.
func TestContextUnknownEvent(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/"+room.RoomID+"/context/$nope", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
		t.Fatalf("route is not registered: %s", body)
	}
}
