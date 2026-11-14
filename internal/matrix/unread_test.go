package matrix

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// pushMsg builds one incoming message for a DM.
func pushMsg(id string, seq uint64, text string, ts uint64) *pb.PushMessage {
	return &pb.PushMessage{
		MsgId:       id,
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: text},
		ContentType: pb.ContentType_text,
		Timestamp:   ts,
		MsgSeq:      seq,
	}
}

// syncOnce fetches one /sync through the real router.
func syncOnce(t *testing.T, s *Server, query string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync?"+query, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sync status %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

// roomIn returns one room object out of a /sync response.
func roomIn(t *testing.T, body map[string]any, roomID string) map[string]any {
	t.Helper()
	rooms, _ := body["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	room, _ := join[roomID].(map[string]any)
	return room
}

// unreadIn reads unread_notifications out of a /sync room object.
func unreadIn(t *testing.T, room map[string]any) (int, int) {
	t.Helper()
	un, _ := room["unread_notifications"].(map[string]any)
	count, _ := un["notification_count"].(float64)
	highlight, _ := un["highlight_count"].(float64)
	return int(count), int(highlight)
}

// receiptOnce posts a read receipt through the real router.
func receiptOnce(t *testing.T, s *Server, roomID, eventID string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/receipt/m.read/"+eventID,
		strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("receipt status %d: %s", rec.Code, rec.Body.String())
	}
}

const dmRoom = "!yh-user-10002:test.local"

// TestUnreadIsDerivedFromReadMarker is the reported bug: a room showed an unread
// badge, opening it did nothing, and the number never went away.
//
// Unread now comes from "messages after ReadMarkerPos", so a read receipt is all
// it takes to clear it - no counter to get out of sync.
func TestUnreadIsDerivedFromReadMarker(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))
	s.ingest(acc, "push_message", pushMsg("m2", 2, "two", 1700000001000))

	body := syncOnce(t, s, "timeout=0")
	room := roomIn(t, body, dmRoom)
	if room == nil {
		t.Fatalf("room missing from sync: %v", body)
	}
	// Neither message has been read: unread is derived, so both count.
	// (ReadMarkerPos is 0 here, which falls back to the seed count - 0 for a
	// freshly ingested room - so ask the room directly before the first read.)
	s.Store.Mu.RLock()
	roomObj := acc.Rooms[dmRoom]
	s.Store.Mu.RUnlock()

	s.Store.Mu.Lock()
	roomObj.ReadMarkerPos = 1 // pretend the client read up to roughly the start
	s.Store.Mu.Unlock()

	if got, _ := roomObj.UnreadFor(acc.UserID); got != 2 {
		t.Fatalf("UnreadFor = %d, want 2 (both messages are after the marker)", got)
	}

	// Reading the newest message must clear the badge completely.
	s.Store.Mu.RLock()
	newest := roomObj.NewestPos()
	s.Store.Mu.RUnlock()
	s.Store.Mu.Lock()
	roomObj.ReadMarkerPos = newest
	s.Store.Mu.Unlock()

	if got, _ := roomObj.UnreadFor(acc.UserID); got != 0 {
		t.Fatalf("UnreadFor after reading everything = %d, want 0", got)
	}
}

// TestReceiptClearsUnreadAndIsPushed is the end-to-end path: the client posts a
// read receipt and the next /sync reports zero unread for that room.
func TestReceiptClearsUnreadAndIsPushed(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))
	s.ingest(acc, "push_message", pushMsg("m2", 2, "two", 1700000001000))

	// First sync establishes a baseline and reports the position to resume from.
	first := syncOnce(t, s, "timeout=0")
	nextBatch, _ := first["next_batch"].(string)
	if nextBatch == "" {
		t.Fatal("next_batch missing")
	}

	s.Store.Mu.RLock()
	newestID := ""
	for _, ev := range acc.Rooms[dmRoom].Events {
		if !ev.IsState() {
			newestID = ev.EventID
		}
	}
	s.Store.Mu.RUnlock()
	if newestID == "" {
		t.Fatal("no timeline event to acknowledge")
	}

	receiptOnce(t, s, dmRoom, newestID)

	// The receipt alone must have advanced the marker.
	s.Store.Mu.RLock()
	pos := acc.Rooms[dmRoom].ReadMarkerPos
	s.Store.Mu.RUnlock()
	if pos <= 0 {
		t.Fatal("the read receipt did not advance ReadMarkerPos")
	}

	// And an incremental sync must now report zero unread.
	second := syncOnce(t, s, "timeout=0&since="+nextBatch)
	room := roomIn(t, second, dmRoom)
	if room == nil {
		t.Fatalf("the read change was not delivered: %v", second)
	}
	if got, _ := unreadIn(t, room); got != 0 {
		t.Fatalf("notification_count = %d after a read receipt, want 0", got)
	}
}

// TestSyncCarriesFullyReadMarker: m.fully_read is the anchor a client needs to
// draw the unread divider and to jump to the first unread message. Without it
// the client can only know "N unread", not where they start.
func TestSyncCarriesFullyReadMarker(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))

	// A room has no read marker until the client has read something. Establish
	// one, then ask for a fresh snapshot.
	s.Store.Mu.RLock()
	eventID := acc.Rooms[dmRoom].Events[0].EventID
	s.Store.Mu.RUnlock()
	receiptOnce(t, s, dmRoom, eventID)

	body := syncOnce(t, s, "timeout=0")
	room := roomIn(t, body, dmRoom)
	if room == nil {
		t.Fatalf("room missing from sync: %v", body)
	}
	ad, _ := room["account_data"].(map[string]any)
	events, _ := ad["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("room account_data = %v, want a single m.fully_read", ad)
	}
	ev, _ := events[0].(map[string]any)
	if got, _ := ev["type"].(string); got != "m.fully_read" {
		t.Fatalf("account_data type = %q, want m.fully_read", got)
	}
	content, _ := ev["content"].(map[string]any)
	if id, _ := content["event_id"].(string); id == "" {
		t.Fatalf("m.fully_read has no event_id: %v", content)
	}
}

// fakeConvList serves /v1/conversation/list, letting a test control what
// upstream claims is unread. That number is deliberately wrong after a read:
// dismissing the notification clears Yunhu's own badge but the conversation
// list keeps reporting unread_msg.
type fakeConvList struct {
	conversations []*pb.ConversationListResponse_Data
	calls         int32
}

func (f *fakeConvList) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/conversation/list", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.calls, 1)
		_, _ = io.ReadAll(r.Body)
		out := &pb.ConversationListResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data:   f.conversations,
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v1/conversation/dismiss-notification", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		body, _ := proto.Marshal(&pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

// TestReconnectDoesNotResurrectUnread is the "it comes back after a while"
// symptom: refreshAccount re-seeded the unread count from Yunhu's conversation
// list on every websocket reconnect, so a cleared badge grew back even though
// nothing new had arrived.
func TestReconnectDoesNotResurrectUnread(t *testing.T) {
	f := &fakeConvList{conversations: []*pb.ConversationListResponse_Data{{
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Name:        "林晚",
		UnreadMsg:   37, // upstream still believes 37 are unread
		TimestampMs: 1700000000000,
	}}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
		cfg.Yunhu.Backfill = 0
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))

	// The account has read everything.
	s.Store.Mu.Lock()
	room := acc.Rooms[dmRoom]
	room.ReadMarkerPos = room.NewestPos()
	room.ReadMarker = "s1"
	s.Store.Mu.Unlock()

	// A reconnect pulls the conversation list, which still says 37 unread.
	s.refreshAccount(context.Background(), acc, "")
	if atomic.LoadInt32(&f.calls) == 0 {
		t.Fatal("the conversation list was never fetched")
	}

	s.Store.Mu.RLock()
	room = acc.Rooms[dmRoom]
	seed := room.Unread
	derived, _ := room.UnreadFor(acc.UserID)
	s.Store.Mu.RUnlock()

	if derived != 0 {
		t.Fatalf("unread after a reconnect = %d, want 0 (the room was read)", derived)
	}
	if seed != 0 {
		t.Fatalf("the upstream count was written back into the seed (%d): "+
			"it must only seed rooms that were never read", seed)
	}
}

// TestJumpToFirstUnread is the second half of the report: the client wants to
// "jump to the unread messages". It does that by paginating FORWARD from the
// event id in m.fully_read, so /messages?dir=f&from=<event id> must resolve that
// id to its stream position - otherwise the token parses as 0 and the page comes
// back from the very beginning of the room.
func TestJumpToFirstUnread(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	for i := 1; i <= 5; i++ {
		s.ingest(acc, "push_message", pushMsg("m"+strconv.Itoa(i), uint64(i),
			"msg"+strconv.Itoa(i), uint64(1700000000000+i*1000)))
	}

	// The account read up to m2; m3..m5 are unread.
	s.Store.Mu.RLock()
	var readUpTo string
	for _, ev := range acc.Rooms[dmRoom].Events {
		if !ev.IsState() && ev.YunhuMsgID == "m2" {
			readUpTo = ev.EventID
		}
	}
	s.Store.Mu.RUnlock()
	if readUpTo == "" {
		t.Fatal("m2 was not projected")
	}
	receiptOnce(t, s, dmRoom, readUpTo)

	// Jumping forward from the marker gives exactly the unread messages.
	body := getMessages(t, s, dmRoom, "dir=f&from="+readUpTo+"&limit=10")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 3 {
		t.Fatalf("chunk = %d events, want the 3 unread ones", len(chunk))
	}
	for i, raw := range chunk {
		got := raw.(map[string]any)["content"].(map[string]any)["body"]
		want := "msg" + strconv.Itoa(i+3)
		if got != want {
			t.Fatalf("chunk[%d] = %v, want %s (the page must start at the marker)", i, got, want)
		}
	}
}

// TestFullyReadMarkerIsAJumpableEventID ties the two halves together: the id
// /sync publishes in m.fully_read has to be usable as a /messages token.
func TestFullyReadMarkerIsAJumpableEventID(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	for i := 1; i <= 3; i++ {
		s.ingest(acc, "push_message", pushMsg("m"+strconv.Itoa(i), uint64(i),
			"msg"+strconv.Itoa(i), uint64(1700000000000+i*1000)))
	}

	s.Store.Mu.RLock()
	var first string
	for _, ev := range acc.Rooms[dmRoom].Events {
		if !ev.IsState() && ev.YunhuMsgID == "m1" {
			first = ev.EventID
		}
	}
	s.Store.Mu.RUnlock()
	receiptOnce(t, s, dmRoom, first)

	body := syncOnce(t, s, "timeout=0")
	room := roomIn(t, body, dmRoom)
	if room == nil {
		t.Fatalf("room missing from sync: %v", body)
	}
	ad, _ := room["account_data"].(map[string]any)
	events, _ := ad["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("no m.fully_read in %v", ad)
	}
	content, _ := events[0].(map[string]any)["content"].(map[string]any)
	markerID, _ := content["event_id"].(string)
	if markerID == "" {
		t.Fatalf("m.fully_read has no event_id: %v", content)
	}

	// The published id must work as a forward-pagination anchor.
	page := getMessages(t, s, dmRoom, "dir=f&from="+markerID+"&limit=10")
	chunk, _ := page["chunk"].([]any)
	if len(chunk) != 2 {
		t.Fatalf("jumping from m.fully_read returned %d events, want the 2 unread ones", len(chunk))
	}
}

// TestReadMarkerSurvivesEviction: the marker is a stream position, so reading up
// to an event that has since been evicted from the bounded timeline must still
// clear the badge.
func TestReadMarkerSurvivesEviction(t *testing.T) {
	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Storage.MaxEventsPerRoom = 100 // the configured minimum
	})
	_ = s
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	// A marker naming an event that was never stored still resolves through its
	// stream position.
	s.Store.Mu.Lock()
	s.markReadLocked(acc, room, "s42")
	pos := room.ReadMarkerPos
	s.Store.Mu.Unlock()

	if pos != 42 {
		t.Fatalf("ReadMarkerPos = %d, want 42 (parsed from the token)", pos)
	}

	// An unrecognisable marker means "everything currently known".
	s.Store.Mu.Lock()
	s.markReadLocked(acc, room, "$gone:test.local")
	pos = room.ReadMarkerPos
	newest := room.NewestPos()
	s.Store.Mu.Unlock()
	if pos < newest {
		t.Fatalf("ReadMarkerPos = %d, want at least the newest pos %d", pos, newest)
	}
}

// TestReadMarkerOnlyMovesForward: a stale receipt (an old client replaying its
// position) must not resurrect unread messages.
func TestReadMarkerOnlyMovesForward(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "one", 1700000000000))
	s.ingest(acc, "push_message", pushMsg("m2", 2, "two", 1700000001000))

	s.Store.Mu.RLock()
	first := acc.Rooms[dmRoom].Events[0].EventID
	s.Store.Mu.RUnlock()

	receiptOnce(t, s, dmRoom, first)

	s.Store.Mu.RLock()
	afterFirst := acc.Rooms[dmRoom].ReadMarkerPos
	s.Store.Mu.RUnlock()

	// A receipt for an older position arrives late.
	s.Store.Mu.Lock()
	s.markReadLocked(acc, acc.Rooms[dmRoom], "s1")
	s.Store.Mu.Unlock()

	s.Store.Mu.RLock()
	afterStale := acc.Rooms[dmRoom].ReadMarkerPos
	s.Store.Mu.RUnlock()
	if afterStale < afterFirst {
		t.Fatalf("ReadMarkerPos went backwards: %d -> %d", afterFirst, afterStale)
	}
}
