package matrix

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeRoster serves /v1/group/list-member with 1-based paging.
type fakeRoster struct {
	members []*pb.ListMemberResponse_User
	calls   int32
	// lastPage records the page number of the final request, so a test can
	// prove the paging loop stopped where it should.
	lastPage int32
}

func (f *fakeRoster) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/list-member", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.calls, 1)
		raw, _ := io.ReadAll(r.Body)
		var req pb.ListMemberRequest
		_ = proto.Unmarshal(raw, &req)

		page := req.GetData().GetPage()
		size := int(req.GetData().GetSize())
		if page < 1 {
			page = 1
		}
		if size <= 0 {
			size = 50
		}
		f.lastPage = page

		start := int(page-1) * size
		end := start + size
		if start > len(f.members) {
			start = len(f.members)
		}
		if end > len(f.members) {
			end = len(f.members)
		}

		out := &pb.ListMemberResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			User:   f.members[start:end],
			Total:  int32(len(f.members)),
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

func rosterUser(id, name string, level int32) *pb.ListMemberResponse_User {
	return &pb.ListMemberResponse_User{
		UserInfo: &pb.ListMemberResponse_User_UserInfo{
			UserId:    id,
			Name:      name,
			AvatarUrl: "https://chat-img.jwznb.com/" + id + ".png",
		},
		PermissionLevel: level,
	}
}

// newRosterServer builds a homeserver whose Yunhu REST base points at the fake
// roster, plus the store it uses.
func newRosterServer(t *testing.T, f *fakeRoster) (*Server, *store.Store) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
}

func newMemberServer(t *testing.T, f *fakeRoster) (*Server, *model.Account) {
	t.Helper()
	s, st := newRosterServer(t, f)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// newMemberServerFor builds a server whose only account has the given Yunhu ID,
// so a test can control which role the logged-in user has.
func newMemberServerFor(t *testing.T, f *fakeRoster, yunhuID string) (*Server, *model.Account) {
	t.Helper()
	s, st := newRosterServer(t, f)
	acc := model.NewAccount(yunhuID, "@yh_"+yunhuID+":test.local")
	acc.Name = "我"
	acc.Token = "token"
	st.PutAccount(acc)
	seedDevice(t, st, acc)
	return s, acc
}

// getMembers calls the endpoint through the real router.
func getMembers(t *testing.T, s *Server, roomID, suffix string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/rooms/"+roomID+"/"+suffix, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status %d: %s", suffix, rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

// TestMembersConnectsYunhuRoster is the reported gap: /members only ever
// returned the handful of people who had spoken. It must now return the real
// group roster.
func TestMembersConnectsYunhuRoster(t *testing.T) {
	f := &fakeRoster{members: []*pb.ListMemberResponse_User{
		rosterUser("10001", "群主大人", 100),
		rosterUser("10002", "管理员小张", 2),
		rosterUser("10003", "普通成员", 0),
	}}
	s, acc := newMemberServer(t, f)

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	body := getMembers(t, s, "!yh-group-20001:test.local", "members")
	chunk, _ := body["chunk"].([]any)

	got := map[string]string{}
	for _, raw := range chunk {
		ev := raw.(map[string]any)
		if ev["type"] != "m.room.member" {
			continue
		}
		content := ev["content"].(map[string]any)
		got[asString(ev["state_key"])] = asString(content["displayname"])
	}

	want := map[string]string{
		acc.UserID:             "云湖测试",
		"@yh_10001:test.local": "群主大人",
		"@yh_10002:test.local": "管理员小张",
		"@yh_10003:test.local": "普通成员",
	}
	for uid, name := range want {
		if got[uid] != name {
			t.Fatalf("member %s = %q, want %q (full list: %v)", uid, got[uid], name, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d members, want %d: %v", len(got), len(want), got)
	}
}

// TestMembersPublishesPowerLevels pins the role mapping the user asked for:
// 群主 = 100, 管理员 = 50 (Element zh-CN renders those as 管理员 / 主持人).
func TestMembersPublishesPowerLevels(t *testing.T) {
	f := &fakeRoster{members: []*pb.ListMemberResponse_User{
		rosterUser("10001", "群主大人", 100),
		rosterUser("10002", "管理员小张", 2),
		rosterUser("10003", "普通成员", 0),
	}}
	s, acc := newMemberServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	getMembers(t, s, room.RoomID, "members")

	s.Store.Mu.RLock()
	pl := room.State[model.StateID("m.room.power_levels", "")]
	s.Store.Mu.RUnlock()
	if pl == nil {
		t.Fatal("no m.room.power_levels state event")
	}
	users := pl.Content["users"].(map[string]any)

	if got := asInt(users["@yh_10001:test.local"]); got != 100 {
		t.Fatalf("owner power level = %d, want 100", got)
	}
	if got := asInt(users["@yh_10002:test.local"]); got != 50 {
		t.Fatalf("admin power level = %d, want 50", got)
	}
	if _, has := users["@yh_10003:test.local"]; has {
		t.Fatalf("an ordinary member must not get an explicit power level: %v", users)
	}
	// The seeded account is 10001, which is this roster's 群主, so 100 here is
	// its real role - not the old hard-coded default.
	if got := asInt(users[acc.UserID]); got != 100 {
		t.Fatalf("the group owner must be 100, got %d", got)
	}
}

// TestMembersCachesRoster: opening the member list repeatedly must not hammer
// the upstream API.
func TestMembersCachesRoster(t *testing.T) {
	f := &fakeRoster{members: []*pb.ListMemberResponse_User{
		rosterUser("10001", "群主大人", 100),
	}}
	s, acc := newMemberServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	getMembers(t, s, room.RoomID, "members")
	first := atomic.LoadInt32(&f.calls)
	if first == 0 {
		t.Fatal("the roster was never fetched")
	}
	getMembers(t, s, room.RoomID, "members")
	getMembers(t, s, room.RoomID, "joined_members")
	if got := atomic.LoadInt32(&f.calls); got != first {
		t.Fatalf("upstream was called %d times, want the cache to serve %d", got, first)
	}
}

// TestMembersPagesThroughRoster: a group bigger than one page must be read
// completely, not truncated at the page size.
func TestMembersPagesThroughRoster(t *testing.T) {
	old := groupMemberPageSize
	groupMemberPageSize = 2
	t.Cleanup(func() { groupMemberPageSize = old })

	f := &fakeRoster{}
	for i := 1; i <= 5; i++ {
		f.members = append(f.members, rosterUser("1000"+string(rune('0'+i)), "成员", 0))
	}
	s, acc := newMemberServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	getMembers(t, s, room.RoomID, "members")

	s.Store.Mu.RLock()
	n := len(room.GroupMembers)
	s.Store.Mu.RUnlock()
	if n != 5 {
		t.Fatalf("roster has %d members, want 5 (paging stopped early?)", n)
	}
	if got := atomic.LoadInt32(&f.calls); got != 3 {
		t.Fatalf("upstream calls = %d, want 3 (2+2+1)", got)
	}
}

// TestMembersDoesNotFetchForDirectChats: DMs have no group roster, and calling
// list-member for one would just error.
func TestMembersDoesNotFetchForDirectChats(t *testing.T) {
	f := &fakeRoster{}
	s, acc := newMemberServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.ensureDirectPeerLocked(acc, room)
	s.Store.Mu.Unlock()

	body := getMembers(t, s, room.RoomID, "members")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 2 {
		t.Fatalf("DM member list = %d entries, want 2", len(chunk))
	}
	if got := atomic.LoadInt32(&f.calls); got != 0 {
		t.Fatalf("list-member was called %d times for a DM", got)
	}
}

// TestMembersReflectsAccountOwnRole is the reported bug: the logged-in account
// was hard-coded to power 100, so an ordinary admin (or plain member) showed up
// as the group owner - "管理员" in Element's zh-CN instead of "协管员".
func TestMembersReflectsAccountOwnRole(t *testing.T) {
	cases := []struct {
		name      string
		accountID string
		roster    []*pb.ListMemberResponse_User
		wantLevel int
		wantEntry bool
	}{
		{
			name:      "owner stays at 100",
			accountID: "10001",
			roster: []*pb.ListMemberResponse_User{
				rosterUser("10001", "我", 100),
				rosterUser("10009", "别人", 0),
			},
			wantLevel: 100,
			wantEntry: true,
		},
		{
			name:      "admin becomes 50, not 100",
			accountID: "10002",
			roster: []*pb.ListMemberResponse_User{
				rosterUser("10001", "群主", 100),
				rosterUser("10002", "我", 2),
			},
			wantLevel: 50,
			wantEntry: true,
		},
		{
			name:      "plain member gets no explicit level",
			accountID: "10003",
			roster: []*pb.ListMemberResponse_User{
				rosterUser("10001", "群主", 100),
				rosterUser("10002", "管理员", 2),
				rosterUser("10003", "我", 0),
			},
			wantLevel: 0,
			wantEntry: false,
		},
		{
			name:      "absent from the roster is not the owner",
			accountID: "10004",
			roster: []*pb.ListMemberResponse_User{
				rosterUser("10001", "群主", 100),
			},
			// A hidden member list must not turn the account into the owner:
			// that is what made every group claim the account was its creator.
			wantLevel: 0,
			wantEntry: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeRoster{members: c.roster}
			s, acc := newMemberServerFor(t, f, c.accountID)

			s.Store.Mu.Lock()
			room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
			s.Store.Mu.Unlock()

			getMembers(t, s, room.RoomID, "members")

			s.Store.Mu.RLock()
			pl := room.State[model.StateID("m.room.power_levels", "")]
			s.Store.Mu.RUnlock()
			if pl == nil {
				t.Fatal("no m.room.power_levels state event")
			}
			users := pl.Content["users"].(map[string]any)

			level, has := users[acc.UserID]
			if has != c.wantEntry {
				t.Fatalf("users[%s] present = %v, want %v (users: %v)", acc.UserID, has, c.wantEntry, users)
			}
			if has && asInt(level) != c.wantLevel {
				t.Fatalf("own power level = %d, want %d", asInt(level), c.wantLevel)
			}
			// The real group owner must still be 100 regardless.
			if owner := asInt(users["@yh_10001:test.local"]); owner != 100 {
				t.Fatalf("group owner level = %d, want 100 (users: %v)", owner, users)
			}
		})
	}
}

// TestMatrixPowerLevel pins the Yunhu -> Matrix role mapping.
func TestMatrixPowerLevel(t *testing.T) {
	cases := []struct {
		yunhu int32
		want  int
	}{
		{100, 100}, // 群主 -> 管理员
		{101, 100}, // defensive: anything above owner stays owner
		{2, 50},    // 管理员 -> 主持人
		{1, 0},
		{0, 0},
		{-1, 0},
	}
	for _, c := range cases {
		if got := matrixPowerLevel(c.yunhu); got != c.want {
			t.Fatalf("matrixPowerLevel(%d) = %d, want %d", c.yunhu, got, c.want)
		}
	}
}
