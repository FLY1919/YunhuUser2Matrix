package matrix

import (
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// TestBotConversationIsDirect: a bot conversation is a 1:1 conversation. Only
// groups are multi-party. Getting this wrong made clients render every bot DM as
// a group chat: IsDirect drives `is_dm` (the DM layout) and `heroes` (the list a
// client uses to name a direct room).
func TestBotConversationIsDirect(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	bot := s.ensureRoomLocked(acc, "23090440", model.ChatTypeBot, "复读机", "")
	dm := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	group := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	// A 1:1 room needs its other participant recorded, which is what makes it
	// nameable from heroes. The conversation-list path does this.
	s.ensureDirectPeerLocked(acc, bot)
	s.ensureDirectPeerLocked(acc, dm)
	s.Store.Mu.Unlock()

	if !bot.IsDirect {
		t.Fatal("a bot conversation is not marked direct")
	}
	if !dm.IsDirect {
		t.Fatal("a 1:1 user conversation is not marked direct")
	}
	if group.IsDirect {
		t.Fatal("a group conversation is marked direct")
	}

	// Which means a bot room has heroes, so the client can name it without a
	// room name.
	botID := s.mxUserID("23090440", model.ChatTypeBot)
	if got := heroes(bot, acc.UserID); len(got) != 1 || got[0] != botID {
		t.Fatalf("heroes = %v, want [%s]", got, botID)
	}
}

// TestPrivateMessageFromBotDoesNotOpenAPhantomRoom is the reported "机器人消息单独开
// 个空聊天": a 1:1 push carries the account's own id in chat_id *and* recv_id, and
// the conversation is the peer. Taking chat_id at face value created a room under
// our own id, so the client showed a brand new empty chat holding the bot's text.
func TestPrivateMessageFromBotDoesNotOpenAPhantomRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	// The bot conversation already exists, from the conversation list.
	s.Store.Mu.Lock()
	bot := s.ensureRoomLocked(acc, "23090440", model.ChatTypeBot, "复读机", "")
	roomsBefore := len(acc.Rooms)
	s.Store.Mu.Unlock()

	// A push shaped the way Yunhu sends a 1:1 message: chat_id == recv_id == us.
	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "b1",
		Sender:      &pb.Sender{ChatId: "23090440", ChatType: pb.ChatType_bot, Name: "复读机"},
		RecvId:      "10001", // the account itself
		ChatId:      "10001", // …and again, which is what makes it 1:1
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "机器人说话了"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	// No new room was invented, and the message landed in the bot conversation.
	if got := len(acc.Rooms); got != roomsBefore {
		t.Fatalf("room count %d -> %d: the bot message opened a phantom room", roomsBefore, got)
	}
	ev := eventOf(t, acc, bot.RoomID, "b1")
	if ev == nil {
		t.Fatal("the message did not land in the bot conversation")
	}
	if got := asString(ev.Content["body"]); got != "机器人说话了" {
		t.Fatalf("body = %q", got)
	}
	// And it is attributed to the bot, not to the account.
	if got := ev.Sender; got != s.mxUserID("23090440", model.ChatTypeBot) {
		t.Fatalf("sender = %q, want the bot's own user", got)
	}
}

// TestPrivateMessageFromUserDoesNotOpenAPhantomRoom: the same rule for a plain
// 1:1 conversation.
func TestPrivateMessageFromUserDoesNotOpenAPhantomRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	dm := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	roomsBefore := len(acc.Rooms)
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "d1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		RecvId:      "10001",
		ChatId:      "10001",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "私聊消息"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if got := len(acc.Rooms); got != roomsBefore {
		t.Fatalf("room count %d -> %d", roomsBefore, got)
	}
	if ev := eventOf(t, acc, dm.RoomID, "d1"); ev == nil {
		t.Fatal("the private message did not land in the 1:1 conversation")
	}
}

// TestGroupMessageKeepsItsChatID guards the other direction: for a group the
// conversation is chat_id, and the sender is just a member.
func TestGroupMessageKeepsItsChatID(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	group := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "g1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		RecvId:      "979377289",
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		Content:     &pb.MsgContent{Text: "群里说话"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	ev := eventOf(t, acc, group.RoomID, "g1")
	if ev == nil {
		t.Fatal("the group message did not land in the group")
	}
	// The sender is the member, not the group.
	if got := ev.Sender; got != "@yh_10002:test.local" {
		t.Fatalf("sender = %q", got)
	}
}

// TestConversationOfResolves1to1 pins the resolution rules directly.
func TestConversationOfResolves1to1(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "23090440", model.ChatTypeBot, "复读机", "")
	s.Store.Mu.Unlock()

	// 1:1 from a bot: the peer owns the conversation.
	push := &pb.PushMessage{
		Sender:   &pb.Sender{ChatId: "23090440", ChatType: pb.ChatType_bot},
		RecvId:   "10001",
		ChatId:   "10001",
		ChatType: pb.ChatType_user,
	}
	id, typ, ok := conversationOf(push, acc)
	if !ok || id != "23090440" || typ != model.ChatTypeBot {
		t.Fatalf("1:1 bot: got (%q, %d, %v)", id, typ, ok)
	}

	// Group: chat_id is the conversation.
	push = &pb.PushMessage{
		Sender:   &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user},
		RecvId:   "979377289",
		ChatId:   "979377289",
		ChatType: pb.ChatType_group,
	}
	id, typ, ok = conversationOf(push, acc)
	if !ok || id != "979377289" || typ != model.ChatTypeGroup {
		t.Fatalf("group: got (%q, %d, %v)", id, typ, ok)
	}

	// 1:1 whose type is missing: the bot's known id settles it.
	push = &pb.PushMessage{
		Sender: &pb.Sender{ChatId: "23090440"},
		RecvId: "10001",
		ChatId: "10001",
	}
	id, typ, ok = conversationOf(push, acc)
	if !ok || id != "23090440" || typ != model.ChatTypeBot {
		t.Fatalf("untyped 1:1: got (%q, %d, %v)", id, typ, ok)
	}
}
