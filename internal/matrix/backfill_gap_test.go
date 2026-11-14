package matrix

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// nowUnixMilli is time.Now in milliseconds, named to keep the assertions short.
func nowUnixMilli() int64 { return time.Now().UnixMilli() }

// gapServer builds a homeserver pointed at the fake history endpoint with a
// small backfill window, which is what makes a gap easy to construct.
func gapServer(t *testing.T, f *fakeHistory, backfill int) (*Server, *model.Account) {
	t.Helper()
	return newHistoryServerWith(t, f, func(cfg *config.Config) {
		cfg.Yunhu.Backfill = backfill
	})
}

// reconcile runs the backfill pass the way refreshAccount does.
func reconcile(s *Server, acc *model.Account, room *model.Room) {
	s.backfillRoom(context.Background(), acc, s.YunhuClient().WithToken(acc.Token), room)
}

// idsIn returns the body texts of a room's timeline in stored order.
func idsIn(acc *model.Account, roomID string) []string {
	var out []string
	for _, ev := range acc.Rooms[roomID].Events {
		if ev.IsState() {
			continue
		}
		if body, ok := ev.Content["body"].(string); ok {
			out = append(out, body)
		}
	}
	return out
}

// TestBackfillRepairsGapAfterDowntime is the reported bug: whatever arrived
// while the process was down stayed missing forever, because a room was only
// ever hydrated once. The gap must be filled on the next reconcile.
//
// The shape of the bug matters: the local timeline is *not* empty and *not*
// stale at the bottom - it holds m1..m4 - while m5..m8 were sent during the
// downtime. The newest page from upstream (m6..m8) does not touch m4, so the
// two ranges leave a hole at m5.
func TestBackfillRepairsGapAfterDowntime(t *testing.T) {
	f := &fakeHistory{}
	for i := 1; i <= 8; i++ {
		f.msgs = append(f.msgs, historyMsg("m"+strconv.Itoa(i), "msg"+strconv.Itoa(i),
			uint64(1700000000000+i*1000)))
	}

	s, acc := gapServer(t, f, 3)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	// The room was hydrated while only m1..m4 existed, then the process went
	// down while m5..m8 were sent.
	for i := 1; i <= 4; i++ {
		ingestText(t, s, acc, "m"+strconv.Itoa(i), "10002", pb.ChatType_user,
			"msg"+strconv.Itoa(i), uint64(1700000000000+i*1000))
	}
	room.Backfilled = true
	s.Store.Mu.Lock()
	room.BackfilledPos = room.NewestPos()
	room.BackfillTS = 1 // last reconciled long ago
	s.Store.Mu.Unlock()

	s.Store.Mu.RLock()
	if !room.GapSuspected() {
		s.Store.Mu.RUnlock()
		t.Fatal("a room left behind by a downtime must be reconciled")
	}
	s.Store.Mu.RUnlock()

	reconcile(s, acc, room)

	s.Store.Mu.RLock()
	after := idsIn(acc, "!yh-user-10002:test.local")
	s.Store.Mu.RUnlock()

	seen := map[string]bool{}
	for _, id := range after {
		if seen[id] {
			t.Fatalf("message %q was inserted twice: %v", id, after)
		}
		seen[id] = true
	}
	// m5 is the hole; m6..m8 arrived with the newest page.
	for i := 5; i <= 8; i++ {
		want := "msg" + strconv.Itoa(i)
		if !seen[want] {
			t.Fatalf("%q is still missing after a reconcile (have %v)", want, after)
		}
	}
	// The messages that were already here must not have been duplicated or
	// dropped in the process.
	for i := 1; i <= 4; i++ {
		want := "msg" + strconv.Itoa(i)
		if !seen[want] {
			t.Fatalf("pre-existing %q disappeared (have %v)", want, after)
		}
	}
}

// TestBackfillFailureIsRetried: a failed fetch must not mark the room done. The
// old code stamped Backfilled even on error, which is one of the ways a room
// ended up permanently empty.
func TestBackfillFailureIsRetried(t *testing.T) {
	s, acc := newHistoryServerWith(t, &fakeHistory{}, func(cfg *config.Config) {
		cfg.Yunhu.Backfill = 15
	})
	// Point at a closed port so every request fails.
	s.Cfg.Yunhu.BaseURL = "http://127.0.0.1:1"

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	reconcile(s, acc, room)

	s.Store.Mu.RLock()
	done := room.Backfilled
	s.Store.Mu.RUnlock()
	if done {
		t.Fatal("a failed backfill was marked complete: the room would never be hydrated")
	}
}

// TestGapSuspectedOnlyAfterAResyncWindow: the cheap local filter must not turn a
// 200-room account into 200 list-message calls on every reconnect.
func TestGapSuspectedOnlyAfterAResyncWindow(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	// Never hydrated: always worth checking.
	if !room.GapSuspected() {
		t.Fatal("a room that was never hydrated must be reconciled")
	}

	room.Backfilled = true
	room.BackfillTS = 1 // long ago
	if !room.GapSuspected() {
		t.Fatal("a room last reconciled ages ago must be reconciled again")
	}

	room.BackfillTS = nowUnixMilli()
	if room.GapSuspected() {
		t.Fatal("a just-reconciled room must not be fetched again")
	}
}

// TestGapAfterIgnoresStateEvents: state events carry "when this homeserver first
// saw the room", not "when somebody last spoke". Counting them would make a
// silent room look up to date and hide a real gap.
func TestGapAfterIgnoresStateEvents(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	// Only state events exist, all stamped "now" by ensureRoomLocked.
	if !room.GapAfter(1700000000000) {
		t.Fatal("a room with no timeline messages must report a gap")
	}
}
