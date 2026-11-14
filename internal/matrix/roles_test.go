package matrix

import (
	"context"
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

// fakeGroupInfo serves /v1/group/info, the endpoint that knows who owns a Yunhu
// group and what the caller's own permission_level is.
type fakeGroupInfo struct {
	data  *pb.GroupData
	calls int32
}

func (f *fakeGroupInfo) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/info", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.calls, 1)
		out := &pb.GroupInfoResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data:   f.data,
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

func newGroupInfoServer(t *testing.T, f *fakeGroupInfo, yunhuID string) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := model.NewAccount(yunhuID, "@yh_"+yunhuID+":test.local")
	acc.Name = "我"
	acc.Token = "token"
	st.PutAccount(acc)
	seedDevice(t, st, acc)
	return s, acc
}

// powerLevelsOf returns the users map of a room's m.room.power_levels event.
func powerLevelsOf(t *testing.T, room *model.Room) map[string]any {
	t.Helper()
	pl := room.State[model.StateID("m.room.power_levels", "")]
	if pl == nil {
		t.Fatal("no m.room.power_levels state event")
	}
	users, ok := pl.Content["users"].(map[string]any)
	if !ok {
		t.Fatalf("power_levels.users is %T, want a map", pl.Content["users"])
	}
	return users
}

// TestGroupRoomStartsWithNoOwner is the reported bug: every conversation showed
// the account as its 管理员. The bridge creates the Matrix room, so seeding
// {account: 100} looked reasonable - but a Yunhu group belongs to whoever
// created it upstream, and the account is usually just a member.
func TestGroupRoomStartsWithNoOwner(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	group := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	dm := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	if users := powerLevelsOf(t, group); len(users) != 0 {
		t.Fatalf("a fresh group room must not name an owner, got %v", users)
	}
	// A DM really is the account's own room, so it keeps full power.
	if got := asInt(powerLevelsOf(t, dm)[acc.UserID]); got != 100 {
		t.Fatalf("DM power level = %d, want 100", got)
	}
}

// TestGroupInfoDrivesPowerLevels pins the mapping of Yunhu's group/info onto
// Matrix power levels: owner = 100, admins = 50, and the account only gets a
// level when it actually holds one upstream.
func TestGroupInfoDrivesPowerLevels(t *testing.T) {
	cases := []struct {
		name      string
		accountID string
		data      *pb.GroupData
		want      map[string]int
		absent    []string
	}{
		{
			name:      "ordinary member is not badged at all",
			accountID: "10009",
			data: &pb.GroupData{
				GroupId: "20001", Headcount: 42,
				Owner: "10001", Admin: []string{"10002", "10003"},
				PermissonLevel: 0,
			},
			want:   map[string]int{"@yh_10001:test.local": 100, "@yh_10002:test.local": 50, "@yh_10003:test.local": 50},
			absent: []string{"@yh_10009:test.local"},
		},
		{
			name:      "group owner gets 100",
			accountID: "10001",
			data: &pb.GroupData{
				GroupId: "20001", Headcount: 42,
				Owner: "10001", Admin: []string{"10002"},
				PermissonLevel: 100,
			},
			want: map[string]int{"@yh_10001:test.local": 100, "@yh_10002:test.local": 50},
		},
		{
			name:      "administrator gets 50, not 100",
			accountID: "10002",
			data: &pb.GroupData{
				GroupId: "20001", Headcount: 42,
				Owner: "10001", Admin: []string{"10002"},
				PermissonLevel: 2,
			},
			want: map[string]int{"@yh_10001:test.local": 100, "@yh_10002:test.local": 50},
		},
		{
			name:      "only create_by is known",
			accountID: "10009",
			data: &pb.GroupData{
				GroupId: "20001", Headcount: 7,
				CreateBy: "10005", PermissonLevel: 0,
			},
			want:   map[string]int{"@yh_10005:test.local": 100},
			absent: []string{"@yh_10009:test.local"},
		},
		{
			name:      "nothing known means nobody is badged",
			accountID: "10009",
			data: &pb.GroupData{
				GroupId: "20001", Headcount: 7,
				HideGroupMembers: true,
			},
			want:   map[string]int{},
			absent: []string{"@yh_10009:test.local"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeGroupInfo{data: c.data}
			s, acc := newGroupInfoServer(t, f, c.accountID)

			s.Store.Mu.Lock()
			room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
			s.Store.Mu.Unlock()

			s.refreshMemberCounts(context.Background(), acc, s.yunhuFor(acc, nil))

			s.Store.Mu.RLock()
			users := powerLevelsOf(t, room)
			count := room.MemberCount
			s.Store.Mu.RUnlock()

			if got := atomic.LoadInt32(&f.calls); got == 0 {
				t.Fatal("group/info was never called")
			}
			if len(users) != len(c.want) {
				t.Fatalf("users = %v, want %v", users, c.want)
			}
			for uid, level := range c.want {
				if got := asInt(users[uid]); got != level {
					t.Fatalf("%s = %d, want %d (users: %v)", uid, got, level, users)
				}
			}
			for _, uid := range c.absent {
				if _, has := users[uid]; has {
					t.Fatalf("%s must not have an explicit level (users: %v)", uid, users)
				}
			}
			if want := int(c.data.GetHeadcount()); count != want {
				t.Fatalf("member count = %d, want %d", count, want)
			}
		})
	}
}

// TestStaleFakeOwnerIsRepaired is the migration case: rooms written by an older
// build already have MemberCount set (so a naive "is the count fresh?" check
// skips them) and still carry {account: 100}. They must be re-fetched once.
func TestStaleFakeOwnerIsRepaired(t *testing.T) {
	f := &fakeGroupInfo{data: &pb.GroupData{
		GroupId: "20001", Headcount: 42,
		Owner: "10001", Admin: []string{"10002"},
		PermissonLevel: 0,
	}}
	s, acc := newGroupInfoServer(t, f, "10009")

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	// Simulate the old state: a fabricated owner and a headcount that was
	// fetched long enough ago to still be "fresh".
	pl := room.State[model.StateID("m.room.power_levels", "")]
	pl.Content["users"] = map[string]any{acc.UserID: 100}
	room.MemberCount = 42
	room.MemberCountTS = time.Now().UnixMilli()
	s.Store.Mu.Unlock()

	s.refreshMemberCounts(context.Background(), acc, s.yunhuFor(acc, nil))

	s.Store.Mu.RLock()
	users := powerLevelsOf(t, room)
	s.Store.Mu.RUnlock()

	if _, has := users[acc.UserID]; has {
		t.Fatalf("the fabricated owner was not removed: %v", users)
	}
	if got := asInt(users["@yh_10001:test.local"]); got != 100 {
		t.Fatalf("real owner = %d, want 100 (users: %v)", got, users)
	}
}

// TestFreshRolesAreNotRefetched: once the roles are known, reconnecting must not
// ask group/info again for every group.
func TestFreshRolesAreNotRefetched(t *testing.T) {
	f := &fakeGroupInfo{data: &pb.GroupData{
		GroupId: "20001", Headcount: 42, Owner: "10001", PermissonLevel: 0,
	}}
	s, acc := newGroupInfoServer(t, f, "10009")

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.refreshMemberCounts(context.Background(), acc, s.yunhuFor(acc, nil))
	first := atomic.LoadInt32(&f.calls)
	if first == 0 {
		t.Fatal("group/info was never called")
	}
	s.refreshMemberCounts(context.Background(), acc, s.yunhuFor(acc, nil))
	if got := atomic.LoadInt32(&f.calls); got != first {
		t.Fatalf("group/info called %d times, want the cache to serve %d", got, first)
	}
}

// TestSlidingSyncAlwaysCarriesPowerLevels: Element X asks for a narrow
// required_state, and a corrected role that never reaches the client is a role
// the client keeps rendering wrongly. The event is tiny, so it always ships.
func TestSlidingSyncAlwaysCarriesPowerLevels(t *testing.T) {
	f := &fakeGroupInfo{data: &pb.GroupData{
		GroupId: "20001", Headcount: 42, Owner: "10001", PermissonLevel: 0,
	}}
	s, acc := newGroupInfoServer(t, f, "10009")

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()
	s.refreshMemberCounts(context.Background(), acc, s.yunhuFor(acc, nil))

	// Empty required_state: the client asked for nothing in particular.
	state := s.slidingState(room, nil, 0, false, acc.UserID)
	found := false
	for _, ev := range state {
		if ev["type"] == "m.room.power_levels" {
			found = true
			content := ev["content"].(map[string]any)
			if _, has := content["users"].(map[string]any)[acc.UserID]; has {
				t.Fatalf("the account must not be named as an admin: %v", content["users"])
			}
		}
	}
	if !found {
		t.Fatalf("m.room.power_levels missing from the sliding sync state: %v", state)
	}
}

// TestDirectRoomsNeverGetPowerLevelsFromUpstream keeps the DM path untouched:
// group/info is not a thing for a 1:1 chat, and the seeded 100 must survive.
func TestDirectRoomsNeverGetPowerLevelsFromUpstream(t *testing.T) {
	f := &fakeGroupInfo{data: &pb.GroupData{GroupId: "10002", Headcount: 2}}
	s, acc := newGroupInfoServer(t, f, "10009")

	s.Store.Mu.Lock()
	dm := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	if changed := func() bool {
		s.Store.Mu.Lock()
		defer s.Store.Mu.Unlock()
		return s.syncGroupPowerLevelsLocked(acc, dm)
	}(); changed {
		t.Fatal("a DM must not have its power levels rewritten from group roles")
	}
	if got := asInt(powerLevelsOf(t, dm)[acc.UserID]); got != 100 {
		t.Fatalf("DM power level = %d, want 100", got)
	}
	if got := atomic.LoadInt32(&f.calls); got != 0 {
		t.Fatalf("group/info was called %d times for a DM", got)
	}
}
