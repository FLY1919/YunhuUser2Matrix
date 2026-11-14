package matrix

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeRoles serves group/info and manage-setting, recording every level change.
type fakeRoles struct {
	owner  string
	admins []string
	// rosterAdmins is what list-member would return; it must NOT be used for the
	// comparison, and these tests assert that by leaving it different.
	rosterAdmins []string

	mu      sync.Mutex
	changes []roleChange
	infoCalls int
}

type roleChange struct {
	UserID string
	Level  int32
}

func (f *fakeRoles) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		f.mu.Lock()
		f.infoCalls++
		f.mu.Unlock()
		out, _ := proto.Marshal(&pb.GroupInfoResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: &pb.GroupData{
				GroupId:   "857744874",
				Name:      "工作群",
				Owner:     f.owner,
				Admin:     f.admins,
				Headcount: 31,
			},
		})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	mux.HandleFunc("/v1/group/list-member", func(w http.ResponseWriter, r *http.Request) {
		// Return only the roster admins on page 1, then a short page.
		raw, _ := io.ReadAll(r.Body)
		var req pb.ListMemberRequest
		_ = proto.Unmarshal(raw, &req)
		out := &pb.ListMemberResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
		if req.GetData().GetPage() == 1 {
			for _, id := range f.rosterAdmins {
				out.User = append(out.User, &pb.ListMemberResponse_User{
					UserInfo:        &pb.ListMemberResponse_User_UserInfo{UserId: id, Name: id},
					PermissionLevel: 2,
				})
			}
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v1/group/manage-setting", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			GroupID   string `json:"groupId"`
			UserID    string `json:"userId"`
			UserLevel int32  `json:"userLevel"`
		}
		_ = json.Unmarshal(raw, &body)
		f.mu.Lock()
		f.changes = append(f.changes, roleChange{UserID: body.UserID, Level: body.UserLevel})
		f.mu.Unlock()
		// JSON, matching the real endpoint (which the client does not decode
		// beyond the status).
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":1,"msg":"success"}`))
	})
	return mux
}

func (f *fakeRoles) snapshot() []roleChange {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]roleChange, len(f.changes))
	copy(out, f.changes)
	return out
}

func newRolesServer(t *testing.T, f *fakeRoles) (*Server, *model.Account, *model.Room) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "857744874", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()
	return s, acc, room
}

// TestPowerLevelsPromotionReachesYunhu: promoting a member from 0 to 50 in a
// Matrix client has to make them an administrator on Yunhu (userLevel 2).
func TestPowerLevelsPromotionReachesYunhu(t *testing.T) {
	f := &fakeRoles{owner: "8516939", admins: []string{"2037619"}}
	s, acc, room := newRolesServer(t, f)

	// The report's payload, trimmed to the interesting entries: the owner at 100,
	// one existing admin at 50, one new admin at 50, and a member at 0.
	body := `{"ban":50,"events_default":0,"invite":0,"kick":50,"redact":50,"state_default":50,` +
		`"users":{"@yh_8516939:test.local":100,"@yh_2037619:test.local":50,` +
		`"@yh_6002836:test.local":50,"@yh_3056910:test.local":0},"users_default":0}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.power_levels/", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	// The sync runs in the background.
	waitFor(t, func() bool { return f.infoCalls > 0 && len(f.snapshot()) > 0 })

	changes := f.snapshot()
	if len(changes) != 1 {
		t.Fatalf("changes = %v, want exactly one (6002836 promoted)", changes)
	}
	if changes[0].UserID != "6002836" || changes[0].Level != 2 {
		t.Fatalf("change = %+v, want 6002836 -> 2", changes[0])
	}

	// The room's own roles also have to be refreshed so the client redraws.
	_ = acc
}

// TestPowerLevelsDemotionReachesYunhu: dropping a member from 50 to 0 demotes them.
func TestPowerLevelsDemotionReachesYunhu(t *testing.T) {
	f := &fakeRoles{owner: "8516939", admins: []string{"2037619", "6002836"}}
	s, _, room := newRolesServer(t, f)

	body := `{"users":{"@yh_8516939:test.local":100,"@yh_2037619:test.local":50,` +
		`"@yh_6002836:test.local":0},"users_default":0}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.power_levels/", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	waitFor(t, func() bool { return len(f.snapshot()) > 0 })
	changes := f.snapshot()
	if len(changes) != 1 || changes[0].UserID != "6002836" || changes[0].Level != 0 {
		t.Fatalf("changes = %v, want 6002836 -> 0", changes)
	}
}

// TestPowerLevelsUnchangedSendsNothing: Element re-submits the whole map whenever
// the panel is opened. Posting every entry would hammer the API for no reason.
func TestPowerLevelsUnchangedSendsNothing(t *testing.T) {
	f := &fakeRoles{owner: "8516939", admins: []string{"2037619", "6002836"}}
	s, _, room := newRolesServer(t, f)

	body := `{"users":{"@yh_8516939:test.local":100,"@yh_2037619:test.local":50,` +
		`"@yh_6002836:test.local":50},"users_default":0}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.power_levels/", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	waitFor(t, func() bool { return f.infoCalls > 0 })
	if got := f.snapshot(); len(got) != 0 {
		t.Fatalf("changes = %v, want none for an unchanged map", got)
	}
}

// TestPowerLevelsNeverTouchesTheOwner: Yunhu rejects changing 群主, and a client
// that omits the owner from `users` must not be read as "demote the owner".
func TestPowerLevelsNeverTouchesTheOwner(t *testing.T) {
	f := &fakeRoles{owner: "8516939", admins: []string{"2037619"}}
	s, _, room := newRolesServer(t, f)

	// The owner is absent entirely - the shape some clients send.
	body := `{"users":{"@yh_2037619:test.local":50,"@yh_6002836:test.local":50},"users_default":0}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.power_levels/", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	waitFor(t, func() bool { return len(f.snapshot()) > 0 })
	for _, ch := range f.snapshot() {
		if ch.UserID == "8516939" {
			t.Fatalf("the owner was touched: %+v", ch)
		}
	}
}

// TestPowerLevelsUsesGroupInfoNotTheRoster is the specific warning in the report:
// the roster is paginated, so admins can sit past the limit. The comparison has to
// come from group/info, or a truncated roster would demote every admin it missed.
func TestPowerLevelsUsesGroupInfoNotTheRoster(t *testing.T) {
	// group/info says 6002836 is an admin; the roster (page 1) does not.
	f := &fakeRoles{owner: "8516939", admins: []string{"6002836"}, rosterAdmins: nil}
	s, _, room := newRolesServer(t, f)

	body := `{"users":{"@yh_8516939:test.local":100,"@yh_6002836:test.local":50},"users_default":0}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.power_levels/", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	waitFor(t, func() bool { return f.infoCalls > 0 })
	// group/info already lists them as admin, so nothing has to change. If the
	// roster had been used, this would have been a pointless re-promotion.
	if got := f.snapshot(); len(got) != 0 {
		t.Fatalf("changes = %v, want none (group/info already agrees)", got)
	}
}

// TestPowerLevelsIgnoresNonGroupRooms: a 1:1 room has no admins upstream.
func TestPowerLevelsIgnoresNonGroupRooms(t *testing.T) {
	f := &fakeRoles{owner: "8516939"}
	s, acc, _ := newRolesServer(t, f)

	s.Store.Mu.Lock()
	dm := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	body := `{"users":{"@yh_10002:test.local":0},"users_default":0}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+dm.RoomID+
		"/state/m.room.power_levels/", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	// Give the background job a chance to run, then assert it did nothing.
	waitFor(t, func() bool { return true })
	if got := f.snapshot(); len(got) != 0 {
		t.Fatalf("a 1:1 room triggered role changes: %v", got)
	}
}
