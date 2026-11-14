package matrix

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeEdit captures the edit requests forwarded upstream.
type fakeEdit struct {
	edits   int32
	lastReq pb.SendMsgRequest
}

func (f *fakeEdit) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/edit-message", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.edits, 1)
		raw, _ := io.ReadAll(r.Body)
		_ = proto.Unmarshal(raw, &f.lastReq)
		out := &pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

// putJSON calls a Matrix endpoint through the real router with PUT, which is
// what /send is registered as.
func putJSON(t *testing.T, s *Server, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestMatrixEditGoesUpstreamAndStaysInPlace is the "edit from Matrix" path:
// the request must reach /v1/msg/edit-message carrying the message id, and the
// local projection must update the bubble instead of appending a new message.
func TestMatrixEditGoesUpstreamAndStaysInPlace(t *testing.T) {
	f := &fakeEdit{}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	// A message that exists both locally and upstream.
	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))

	s.Store.Mu.RLock()
	orig := eventOf(t, acc, dmRoom, "m1")
	origID := orig.EventID
	before := len(acc.Rooms[dmRoom].Events)
	s.Store.Mu.RUnlock()

	body := `{"msgtype":"m.text","body":"* 改过的内容",` +
		`"m.new_content":{"msgtype":"m.text","body":"改过的内容"},` +
		`"m.relates_to":{"rel_type":"m.replace","event_id":"` + origID + `"}}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+dmRoom+
		"/send/m.room.message/txn1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	// The edit reached Yunhu, addressed to the right message.
	if n := atomic.LoadInt32(&f.edits); n != 1 {
		t.Fatalf("upstream edits = %d, want 1", n)
	}
	if got := f.lastReq.GetMsgId(); got != "m1" {
		t.Fatalf("edit msg_id = %q, want m1", got)
	}
	if got := f.lastReq.GetContent().GetText(); got != "改过的内容" {
		t.Fatalf("edit text = %q, want the m.new_content body", got)
	}

	// Locally the bubble was updated in place.
	s.Store.Mu.RLock()
	after := len(acc.Rooms[dmRoom].Events)
	ev := acc.Rooms[dmRoom].FindEvent(origID)
	s.Store.Mu.RUnlock()

	if after != before {
		t.Fatalf("timeline grew from %d to %d events: the edit appended a message", before, after)
	}
	if ev == nil {
		t.Fatalf("%s not found after the edit", origID)
	}
	if got := asString(ev.Content["body"]); got != "改过的内容" {
		t.Fatalf("body = %q, want the edited text without the * fallback prefix", got)
	}
	if ev.EditedTS == 0 {
		t.Fatal("the event was not stamped as edited")
	}

	// The response names the derived edit event id, not the original.
	if got := asString(resp["event_id"]); got != origID+"_edit" {
		t.Fatalf("event_id = %q, want %q", got, origID+"_edit")
	}
}

// TestMatrixEditUnknownTargetIsRejected: editing an event this account does not
// have must fail rather than create a message out of the m.replace envelope.
func TestMatrixEditUnknownTargetIsRejected(t *testing.T) {
	f := &fakeEdit{}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	body := `{"msgtype":"m.text","body":"* x",` +
		`"m.new_content":{"msgtype":"m.text","body":"x"},` +
		`"m.relates_to":{"rel_type":"m.replace","event_id":"$nope"}}`
	code, _ := putJSON(t, s, "/_matrix/client/v3/rooms/"+dmRoom+
		"/send/m.room.message/txn1", body)
	if code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 for an unknown edit target", code)
	}

	s.Store.Mu.RLock()
	room := acc.Rooms[dmRoom]
	var leaked bool
	for _, ev := range room.Events {
		if strings.Contains(asString(ev.Content["body"]), "m.replace") {
			leaked = true
		}
	}
	s.Store.Mu.RUnlock()
	if leaked {
		t.Fatal("the m.replace envelope was stored as a message")
	}
}

// TestRecallWinsOverEdit: an edit racing a recall must not resurrect content.
func TestRecallWinsOverEdit(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))
	s.ingest(acc, "blocked_message", &pb.PushMessage{
		MsgId:           "m1",
		ChatId:          "10002",
		ChatType:        pb.ChatType_user,
		DeleteTimestamp: 1700000001000,
	})

	// An edit arrives after the recall.
	s.ingest(acc, "edit_message", editMsg("m1", "改过的内容", 1700000002000))

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, dmRoom, "m1")
	s.Store.Mu.RUnlock()

	if ev == nil {
		t.Fatal("the message disappeared")
	}
	if !ev.Recalled() {
		t.Fatal("the recall was undone by the edit")
	}
	if body := asString(ev.Content["body"]); body != "" {
		t.Fatalf("a recalled message got its content back: %q", body)
	}
}
