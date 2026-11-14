package matrix

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// wsEditPush is what the websocket actually delivers for an edit: no
// chat_type, no sender, no timestamp. The PushMessage comment spells it out
// ("发送者信息,编辑消息无此项" / "会话类型,编辑消息无此项" / "时间戳(毫秒),编辑消息无此项"),
// and relying on any of them is what made edits get lost.
func wsEditPush(msgID, text string, editTS uint64) *pb.PushMessage {
	return wsEditPushIn("10002", msgID, text, editTS)
}

// wsEditPushIn is wsEditPush for a specific conversation.
func wsEditPushIn(chatID, msgID, text string, editTS uint64) *pb.PushMessage {
	return &pb.PushMessage{
		MsgId:         msgID,
		ChatId:        chatID,
		Content:       &pb.MsgContent{Text: text},
		ContentType:   pb.ContentType_text,
		EditTimestamp: editTS,
	}
}

// waitFor polls until cond is true or the deadline expires.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}

// TestWsEditWithoutChatTypeUpdatesTheMessage is the reported bug: the websocket
// edit push has no chat_type, so looking the room up as (chatID, 0) missed, a
// phantom chatType=0 room was created, and the message the user was looking at
// never changed.
func TestWsEditWithoutChatTypeUpdatesTheMessage(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))

	s.Store.Mu.RLock()
	roomsBefore := len(acc.Rooms)
	s.Store.Mu.RUnlock()

	// A real edit push: no chat type, no sender, no timestamp.
	s.ingest(acc, "edit_message", wsEditPush("m1", "改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	// No phantom room was created for the same conversation.
	if got := len(acc.Rooms); got != roomsBefore {
		t.Fatalf("room count %d -> %d: the edit push created a phantom room", roomsBefore, got)
	}

	orig := eventOf(t, acc, dmRoom, "m1")
	if orig == nil {
		t.Fatal("the original message disappeared")
	}
	if got := asString(orig.Content["body"]); got != "改过的内容" {
		t.Fatalf("body = %q, want the edited text", got)
	}
	if orig.EditedTS != 1700000001000 {
		t.Fatalf("EditedTS = %d, want the push's edit_time", orig.EditedTS)
	}
	// The sender must be preserved: the push omits it, and a blank sender would
	// make the bubble anonymous.
	if orig.Sender != "@yh_10002:test.local" {
		t.Fatalf("sender = %q, want the original sender", orig.Sender)
	}
}

// TestWsEditIntoGroupWithoutChatType: the same fallback has to work for groups,
// where the room id cannot be guessed from the chat id alone.
func TestWsEditIntoGroupWithoutChatType(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "g1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		Content:     &pb.MsgContent{Text: "群里的原始内容"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.ingest(acc, "edit_message", wsEditPushIn("979377289", "g1", "群里改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	orig := eventOf(t, acc, room.RoomID, "g1")
	if orig == nil {
		t.Fatal("the group message disappeared")
	}
	if got := asString(orig.Content["body"]); got != "群里改过的内容" {
		t.Fatalf("body = %q, want the edited text", got)
	}
	// The group room must not have been duplicated as a chatType=0 one.
	for id := range acc.Rooms {
		if id == room.RoomID {
			continue
		}
		if acc.Rooms[id].ChatID == "979377289" {
			t.Fatalf("a duplicate room was created for the group: %s", id)
		}
	}
}

// TestWsEditWithoutChatIDFindsTheMessage: the push may even omit chat_id. The
// message id is unique per account, so the room is found from the message it
// names - which is exactly how Yunhu's own client resolves the target.
func TestWsEditWithoutChatIDFindsTheMessage(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))

	// No chat id, no chat type, no sender, no timestamp.
	s.ingest(acc, "edit_message", &pb.PushMessage{
		MsgId:         "m1",
		Content:       &pb.MsgContent{Text: "改过的内容"},
		ContentType:   pb.ContentType_text,
		EditTimestamp: 1700000001000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	orig := eventOf(t, acc, dmRoom, "m1")
	if orig == nil {
		t.Fatal("the message disappeared")
	}
	if got := asString(orig.Content["body"]); got != "改过的内容" {
		t.Fatalf("body = %q, want the edited text", got)
	}
}

// TestEditForUnknownMessageCreatesNoRoom is the "空气会话" report: a websocket
// edit for a message this homeserver never projected (an old message, or one
// outside the hydration window) created a brand new empty room, which then
// showed up in the client's conversation list with nothing in it.
//
// An edit is a mutation of an existing message. If the message is not here, the
// push is dropped - not turned into a conversation.
func TestEditForUnknownMessageCreatesNoRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.RLock()
	before := len(acc.Rooms)
	s.Store.Mu.RUnlock()

	// An edit naming a conversation and a message the account has never seen.
	s.ingest(acc, "edit_message", wsEditPushIn("979377289", "never-seen", "改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	after := len(acc.Rooms)
	exists := acc.Rooms["!yh-group-979377289:test.local"] != nil
	s.Store.Mu.RUnlock()

	if after != before {
		t.Fatalf("room count %d -> %d: an edit for an unknown message created a room", before, after)
	}
	if exists {
		t.Fatal("an empty room was created for an edit whose target was never projected")
	}
}

// TestRecallForUnknownMessageCreatesNoRoom: the same rule for recalls, which
// arrive as bare blocked_message pushes.
func TestRecallForUnknownMessageCreatesNoRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.RLock()
	before := len(acc.Rooms)
	s.Store.Mu.RUnlock()

	s.ingest(acc, "blocked_message", &pb.PushMessage{
		MsgId:           "never-seen",
		ChatId:          "979377289",
		ChatType:        pb.ChatType_group,
		DeleteTimestamp: 1700000001000,
	})

	s.Store.Mu.RLock()
	after := len(acc.Rooms)
	s.Store.Mu.RUnlock()

	if after != before {
		t.Fatalf("room count %d -> %d: a recall for an unknown message created a room", before, after)
	}
}

// TestEditInAnExistingRoomStillWorks guards the other direction: the "no room is
// created" rule must not break an edit whose target does exist.
func TestEditInAnExistingRoomStillWorks(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	room := mentionRoom(t, s, acc)
	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "g1",
		Sender:      &pb.Sender{ChatId: "8932561", ChatType: pb.ChatType_user, Name: "логикатеКсус"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		Content:     &pb.MsgContent{Text: "原始内容"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.ingest(acc, "edit_message", wsEditPushIn("979377289", "g1", "改过的内容", 1700000001000))

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "g1")
	s.Store.Mu.RUnlock()
	if ev == nil {
		t.Fatal("the message disappeared")
	}
	if got := asString(ev.Content["body"]); got != "改过的内容" {
		t.Fatalf("body = %q, want the edited text", got)
	}
}

// TestPurgeEmptyRoomsDropsOnlyPhantoms: the cleanup must remove the empty rooms
// older builds left behind without touching a real conversation.
func TestPurgeEmptyRoomsDropsOnlyPhantoms(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	// A phantom: state only, no name, never opened.
	s.Store.Mu.Lock()
	phantom := s.ensureRoomLocked(acc, "979377289", 0, "", "")
	phantomID := phantom.RoomID

	// A real room: it has a name and a message.
	real := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	realID := real.RoomID
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", pushMsg("m1", 1, "hello", 1700000000000))

	s.purgeEmptyRooms(acc)

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if acc.Rooms[phantomID] != nil {
		t.Fatalf("the empty room %s survived the purge", phantomID)
	}
	if acc.Rooms[realID] == nil {
		t.Fatalf("the real room %s was purged", realID)
	}
}

// TestPurgeKeepsOpenedRoom: a room the user has read has a read marker, which
// means it is a real conversation even if it currently holds no messages.
func TestPurgeKeepsOpenedRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "", "")
	room.ReadMarkerPos = 5
	id := room.RoomID
	s.Store.Mu.Unlock()

	s.purgeEmptyRooms(acc)

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if acc.Rooms[id] == nil {
		t.Fatal("a room the user had opened was purged")
	}
}

// TestWsEditIsRefetchedFromApi covers the second half of the official client's
// behaviour: the edit push is partial, so the authoritative text is read back
// with list-message-by-mid-seq and replaces whatever the push carried.
func TestWsEditIsRefetchedFromApi(t *testing.T) {
	f := &fakeMidSeq{fullText: "服务器上的完整内容"}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.ingest(acc, "push_message", pushMsg("m1", 1, "原始内容", 1700000000000))
	// The push carries a truncated body, which the refetch must overwrite.
	s.ingest(acc, "edit_message", wsEditPush("m1", "被截断…", 1700000001000))

	// The refetch runs in the background; wait for it to land.
	waitFor(t, func() bool {
		s.Store.Mu.RLock()
		defer s.Store.Mu.RUnlock()
		ev := eventOf(t, acc, dmRoom, "m1")
		return ev != nil && asString(ev.Content["body"]) == "服务器上的完整内容"
	})

	if n := atomic.LoadInt32(&f.calls); n == 0 {
		t.Fatal("list-message-by-mid-seq was never called")
	}
	if got := f.lastMsgID.Load().(string); got != "m1" {
		t.Fatalf("refetched msg_id = %v, want m1", got)
	}
}

// fakeMidSeq serves list-message-by-mid-seq, the endpoint Yunhu's own client
// uses to hydrate a message it only saw as a websocket edit.
type fakeMidSeq struct {
	fullText  string
	calls     int32
	lastMsgID atomic.Value
}

func (f *fakeMidSeq) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/list-message-by-mid-seq", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.ListMsgByMidSeqRequest
		_ = proto.Unmarshal(raw, &req)
		atomic.AddInt32(&f.calls, 1)
		f.lastMsgID.Store(req.GetMsgId())

		out := &pb.ListMsgResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: []*pb.ListMsgResponse_MsgData{{
				MsgId:           req.GetMsgId(),
				Sender:          &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
				Content:         &pb.MsgContent{Text: f.fullText},
				ContentType:     pb.ContentType_text,
				SendTimestampMs: 1700000000000,
				EditTimestamp:   1700000001000,
			}},
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}
