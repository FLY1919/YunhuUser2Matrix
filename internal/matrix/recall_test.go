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

// postJSON calls a Matrix endpoint through the real router.
func postJSON(t *testing.T, s *Server, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// eventsOfType returns the timeline events of a given type for a room.
func eventsOfType(room *model.Room, evType string) []*model.Event {
	out := make([]*model.Event, 0, 1)
	for _, ev := range room.Events {
		if ev.Type == evType {
			out = append(out, ev)
		}
	}
	return out
}

// TestRecallBecomesRedaction is the reported gap: Yunhu pushes a bare
// blocked_message and the homeserver used to wipe the original in place, which
// re-delivered it as a brand new empty bubble. It must instead append an
// m.room.redaction that targets the original event id.
func TestRecallBecomesRedaction(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "secret"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.RLock()
	room := acc.RoomByChat("10002", model.ChatTypeUser)
	if room == nil {
		s.Store.Mu.RUnlock()
		t.Fatal("room was not created")
	}
	orig := room.EventByYunhuID("m1")
	if orig == nil {
		s.Store.Mu.RUnlock()
		t.Fatal("original message missing")
	}
	origID, origPos := orig.EventID, orig.Pos
	s.Store.Mu.RUnlock()

	// A recall carries only msg_id / recv_id / chat_id.
	s.ingest(acc, "blocked_message", &pb.PushMessage{
		MsgId:           "m1",
		RecvId:          "10001",
		ChatId:          "10002",
		DeleteTimestamp: 1700000005000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	redactions := eventsOfType(room, "m.room.redaction")
	if len(redactions) != 1 {
		t.Fatalf("got %d redaction events, want 1", len(redactions))
	}
	red := redactions[0]
	if red.Redacts != origID {
		t.Fatalf("redaction targets %q, want %q", red.Redacts, origID)
	}
	if red.Sender != orig.Sender {
		t.Fatalf("redaction sender = %q, want the message author %q", red.Sender, orig.Sender)
	}
	if red.Pos <= origPos {
		t.Fatalf("redaction pos %d must be newer than the original pos %d", red.Pos, origPos)
	}
	if orig.Pos != origPos {
		t.Fatalf("original moved from pos %d to %d: it would be re-delivered as a new empty message",
			origPos, orig.Pos)
	}
	if !orig.Recalled() {
		t.Fatal("original must be marked recalled")
	}
	if orig.RecalledBy != red.EventID {
		t.Fatalf("original points at %q, want the redaction %q", orig.RecalledBy, red.EventID)
	}
	if len(orig.Content) != 0 {
		t.Fatalf("original content was not wiped: %v", orig.Content)
	}
}

// TestRecallRedactsOverTheWire pins the wire shape: room version 10 carries the
// target at the top level, and a recalled message must render as "message
// deleted" rather than an empty bubble, even for a client that never saw the
// redaction event.
func TestRecallRedactsOverTheWire(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "secret"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})
	s.ingest(acc, "blocked_message", &pb.PushMessage{
		MsgId: "m1", ChatId: "10002", DeleteTimestamp: 1700000005000,
	})

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=10")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 2 {
		t.Fatalf("chunk = %d events, want the message and its redaction", len(chunk))
	}

	var sawRedaction, sawPlaceholder bool
	for _, raw := range chunk {
		ev := raw.(map[string]any)
		switch ev["type"] {
		case "m.room.redaction":
			if ev["redacts"] == nil || ev["redacts"] == "" {
				t.Fatalf("redaction has no top-level redacts: %v", ev)
			}
			sawRedaction = true
		case "m.room.message":
			content := ev["content"].(map[string]any)
			if len(content) != 0 {
				t.Fatalf("recalled message still has content: %v", content)
			}
			unsigned, _ := ev["unsigned"].(map[string]any)
			if unsigned["redacted_because"] == nil {
				t.Fatalf("recalled message has no redacted_because: %v", ev)
			}
			sawPlaceholder = true
		}
	}
	if !sawRedaction || !sawPlaceholder {
		t.Fatalf("redaction=%v placeholder=%v", sawRedaction, sawPlaceholder)
	}
}

// TestRecalledHistoryRendersAsDeleted covers history imported from Yunhu: a
// message that was recalled before we ever saw it live carries delete_timestamp
// and has no redaction event to point at.
func TestRecalledHistoryRendersAsDeleted(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:           "m1",
		Sender:          &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:          "10002",
		ChatType:        pb.ChatType_user,
		Content:         &pb.MsgContent{Text: "gone"},
		ContentType:     pb.ContentType_text,
		Timestamp:       1700000000000,
		DeleteTimestamp: 1700000009000,
	})

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=10")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 1 {
		t.Fatalf("chunk = %d events, want 1", len(chunk))
	}
	ev := chunk[0].(map[string]any)
	if content := ev["content"].(map[string]any); len(content) != 0 {
		t.Fatalf("recalled history still has content: %v", content)
	}
	unsigned, _ := ev["unsigned"].(map[string]any)
	if unsigned["redacted_because"] == nil {
		t.Fatalf("recalled history has no redacted_because: %v", ev)
	}
}

// TestBotSenderIsAMatrixUser covers bots: their pushes often arrive without a
// sender chat_type, and deriving the member id separately from the message
// sender produced @yh_<botid> members for @yhbot_<botid> messages - so the bot
// rendered as an unknown user with no profile.
func TestBotSenderIsAMatrixUser(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId: "b1",
		// chat_type deliberately unset, exactly as the real pushes look.
		Sender:      &pb.Sender{ChatId: "21385383", Name: "Kira-chan"},
		ChatId:      "21385383",
		ChatType:    pb.ChatType_bot,
		Content:     &pb.MsgContent{Text: "hello from a bot"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	room := acc.RoomByChat("21385383", model.ChatTypeBot)
	if room == nil {
		t.Fatal("bot room was not created")
	}
	want := "@yhbot_21385383:test.local"

	var senders []string
	for _, ev := range room.Events {
		if ev.Type == "m.room.message" {
			senders = append(senders, ev.Sender)
		}
	}
	if len(senders) != 1 || senders[0] != want {
		t.Fatalf("message senders = %v, want [%s]", senders, want)
	}
	member := room.State[model.StateID("m.room.member", want)]
	if member == nil {
		t.Fatalf("no m.room.member for %s: the bot has no profile", want)
	}
	if got := asString(member.Content["displayname"]); got != "Kira-chan" {
		t.Fatalf("member displayname = %q, want Kira-chan", got)
	}
	for uid := range room.Members {
		if uid != want && uid != acc.UserID {
			t.Fatalf("unexpected member %s (group-typed sender leaked?)", uid)
		}
	}
}
