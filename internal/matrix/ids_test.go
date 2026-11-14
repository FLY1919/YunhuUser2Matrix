package matrix

import (
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
)

func TestRoomIDRoundTrip(t *testing.T) {
	s := &Server{Cfg: testConfig()}
	cases := []struct {
		chatID   string
		chatType int32
	}{
		{"10002", model.ChatTypeUser},
		{"20001", model.ChatTypeGroup},
		{"bot-7", model.ChatTypeBot},
		{"weird id/with:chars", model.ChatTypeUser},
	}
	for _, c := range cases {
		roomID := s.roomIDFor(c.chatID, c.chatType)
		gotID, gotType, ok := s.parseRoomID(roomID)
		if !ok {
			t.Fatalf("parseRoomID(%q) failed", roomID)
		}
		if gotType != c.chatType {
			t.Fatalf("chat type mismatch: got %d want %d", gotType, c.chatType)
		}
		if c.chatID == "weird id/with:chars" {
			// Sanitised, so only the shape must be stable.
			if gotID == "" {
				t.Fatalf("expected sanitised id, got empty")
			}
			continue
		}
		if gotID != c.chatID {
			t.Fatalf("chat id mismatch: got %q want %q", gotID, c.chatID)
		}
	}
}

func TestEventIDRoundTrip(t *testing.T) {
	for _, msgID := range []string{"abc123", "5f2b9c0d4e6a7b8c9d0e1f2a3b4c5d6e", "with/slash", "中文 id"} {
		evID := eventIDFor(msgID)
		if got := yunhuMsgIDFromEvent(evID); got != msgID {
			t.Fatalf("roundtrip %q -> %q -> %q", msgID, evID, got)
		}
	}
	// An empty upstream id still has to yield a usable placeholder.
	if eventIDFor("") == "" {
		t.Fatal("empty msg id produced an empty event id")
	}
}

func TestUserIDMapping(t *testing.T) {
	s := &Server{Cfg: testConfig()}
	uid := s.mxUserID("10002", model.ChatTypeUser)
	if uid != "@yh_10002:test.local" {
		t.Fatalf("unexpected user id %q", uid)
	}
	id, ct, ok := s.yunhuIDFromUser(uid)
	if !ok || id != "10002" || ct != model.ChatTypeUser {
		t.Fatalf("reverse mapping failed: %q %d %v", id, ct, ok)
	}
}
