package matrix

import (
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

// TestMatrixReplyCarriesQuoteText is the reported bug: a reply sent from a Matrix
// client reached Yunhu with quote_msg_id set but quote_msg_text empty, and
// Yunhu's own client rendered the quote box as "空气" - an empty frame showing
// nobody said anything.
func TestMatrixReplyCarriesQuoteText(t *testing.T) {
	f := &fakeSend{}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	room := mentionRoom(t, s, acc)

	// The exact payload from the report.
	body := `{"msgtype":"m.text",` +
		`"body":"> <@yh_1659829:localhost> 1\n\n6464646455521",` +
		`"m.mentions":{"user_ids":["@yh_1659829:localhost"]},` +
		`"format":"org.matrix.custom.html",` +
		`"formatted_body":"<mx-reply><blockquote><a href=\"https://matrix.to/#/!yh-group-857744874:localhost/$yh_c093719070da4b0394c04bbc1ee03d5a\">In reply to</a> <a href=\"https://matrix.to/#/@yh_1659829:localhost\">@yh_1659829:localhost</a><br>1</blockquote></mx-reply>6464646455521",` +
		`"m.relates_to":{"m.in_reply_to":{"event_id":"$yh_c093719070da4b0394c04bbc1ee03d5a"}}}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/txn1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	got := f.lastReq.Load().(*pb.SendMsgRequest)
	content := got.GetContent()

	// The quoted text must travel in quote_msg_text, which is what Yunhu renders
	// in the quote box.
	if quote := content.GetQuoteMsgText(); quote != "1" {
		t.Fatalf("quote_msg_text = %q, want %q", quote, "1")
	}
	// And the id in quote_msg_id, which is what links the two messages.
	if id := got.GetQuoteMsgId(); id != "c093719070da4b0394c04bbc1ee03d5a" {
		t.Fatalf("quote_msg_id = %q", id)
	}

	// The new message body must be only the new text: the "> …" quote prefix is
	// Matrix fallback syntax, not part of what the user typed.
	text := content.GetText()
	if strings.Contains(text, "6464646455521") == false {
		t.Fatalf("the new text is missing: %q", text)
	}
	if strings.Contains(text, "> <@") {
		t.Fatalf("the quoted block leaked into the body: %q", text)
	}
	if strings.Contains(text, "1\n\n") {
		t.Fatalf("the quoted text is still in the body: %q", text)
	}
}

// TestMatrixReplyMultiLineQuote: the quoted text of a multi-line reply is the
// whole block, and only the part after the blank line is the new message.
func TestMatrixReplyMultiLineQuote(t *testing.T) {
	quoted, rest := splitMatrixReply("> <@alice:x> first\n> second\n\nhello there")
	if quoted != "first\nsecond" {
		t.Fatalf("quoted = %q", quoted)
	}
	if rest != "hello there" {
		t.Fatalf("rest = %q", rest)
	}
}

// TestMatrixReplyWithoutQuotePrefix: a client that only sets the relation, with a
// plain body, must not have its text mangled.
func TestMatrixReplyWithoutQuotePrefix(t *testing.T) {
	quoted, rest := splitMatrixReply("just the message")
	if quoted != "" {
		t.Fatalf("quoted = %q, want empty", quoted)
	}
	if rest != "just the message" {
		t.Fatalf("rest = %q", rest)
	}
}

// TestConversationWithoutChatTypeIsClassified: several bots were reported as
// showing up as group chats. A conversation list entry without chat_type used to
// become a chatType=0 room, whose id then disagreed with every other path.
func TestConversationWithoutChatTypeIsClassified(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	// The account already knows this id as a bot (it spoke somewhere).
	s.Store.Mu.Lock()
	other := s.ensureRoomLocked(acc, "23090440", model.ChatTypeBot, "复读机", "")
	s.putMemberLocked(acc, other, "@yhbot_23090440:test.local", "复读机", "")
	s.Store.Mu.Unlock()

	s.Store.Mu.Lock()
	got := s.inferChatTypeLocked(acc, "23090440")
	s.Store.Mu.Unlock()
	if got != model.ChatTypeBot {
		t.Fatalf("inferred chat type = %d, want bot (%d)", got, model.ChatTypeBot)
	}

	// A conversation already projected keeps its type.
	s.Store.Mu.Lock()
	got = s.inferChatTypeLocked(acc, "23090440")
	s.Store.Mu.Unlock()
	if got != model.ChatTypeBot {
		t.Fatalf("inferred chat type for a known room = %d, want bot", got)
	}
}

// TestRetypeRoomMovesTheRoomID: when the conversation list corrects a type, the
// room id has to change with it, otherwise the client keeps addressing a room
// that no longer exists under that key.
func TestRetypeRoomMovesTheRoomID(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "23090440", model.ChatTypeUser, "复读机", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "23090440", ChatType: pb.ChatType_user, Name: "复读机"},
		ChatId:      "23090440",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "hi"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.Lock()
	oldID := room.RoomID
	before := len(room.Events)
	moved := s.retypeRoomLocked(acc, room, model.ChatTypeBot)
	s.Store.Mu.Unlock()

	if moved.RoomID == oldID {
		t.Fatal("the room id did not change with the type")
	}
	if moved.ChatType != model.ChatTypeBot {
		t.Fatalf("chat type = %d, want bot", moved.ChatType)
	}
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if acc.Rooms[oldID] != nil {
		t.Fatal("the old room id is still in the account")
	}
	if acc.Rooms[moved.RoomID] != moved {
		t.Fatal("the room is not registered under its new id")
	}
	// Nothing the user can see may be lost in the move.
	if len(moved.Events) != before {
		t.Fatalf("events %d -> %d: the retype dropped history", before, len(moved.Events))
	}
}

// fakeSend captures the send request forwarded upstream.
type fakeSend struct {
	calls   int32
	lastReq atomic.Value
}

func (f *fakeSend) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/send-message", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.SendMsgRequest
		_ = proto.Unmarshal(raw, &req)
		atomic.AddInt32(&f.calls, 1)
		f.lastReq.Store(&req)
		out, _ := proto.Marshal(&pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	return mux
}
