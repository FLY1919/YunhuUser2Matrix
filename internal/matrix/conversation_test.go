package matrix

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeConversations stands in for the Yunhu REST endpoints involved in "new
// chat": group/info answers with a headcount, and create-group is counted so a
// test can prove it is never called.
type fakeConversations struct {
	headcount    uint64
	createGroup  int32
	infoRequests int32
}

func (f *fakeConversations) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/info", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.infoRequests, 1)
		_, _ = io.ReadAll(r.Body)
		out := &pb.GroupInfoResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data:   &pb.GroupData{GroupId: "20001", Name: "工作群", Headcount: f.headcount},
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v1/group/create-group", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.createGroup, 1)
		http.Error(w, "must not be called", http.StatusInternalServerError)
	})
	return mux
}

func newConversationServer(t *testing.T, f *fakeConversations) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// TestCreateRoomEntersExistingDM is the reported behaviour change: "new chat"
// in a Matrix client must enter the existing Yunhu conversation, not silently
// create a brand new empty Yunhu group.
func TestCreateRoomEntersExistingDM(t *testing.T) {
	f := &fakeConversations{}
	s, _ := newConversationServer(t, f)

	code, body := postJSON(t, s, "/_matrix/client/v3/createRoom",
		`{"is_direct":true,"invite":["@yh_10002:test.local"]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if got := asString(body["room_id"]); got != "!yh-user-10002:test.local" {
		t.Fatalf("room_id = %q, want the 1:1 conversation with 10002", got)
	}
	if n := atomic.LoadInt32(&f.createGroup); n != 0 {
		t.Fatalf("create-group was called %d times: a new Yunhu group was created", n)
	}
}

// TestCreateRoomEntersBotChat makes sure the bot prefix resolves too: a bot DM
// is addressed by the bot's own id, so the room can be materialised locally.
func TestCreateRoomEntersBotChat(t *testing.T) {
	f := &fakeConversations{}
	s, _ := newConversationServer(t, f)

	code, body := postJSON(t, s, "/_matrix/client/v3/createRoom",
		`{"is_direct":true,"invite":["@yhbot_21385383:test.local"]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if got := asString(body["room_id"]); got != "!yh-bot-21385383:test.local" {
		t.Fatalf("room_id = %q, want the bot conversation", got)
	}
	if n := atomic.LoadInt32(&f.createGroup); n != 0 {
		t.Fatalf("create-group was called %d times", n)
	}
}

// TestCreateRoomMatchesExistingName covers the group case: a client that asks
// for a room by name gets the conversation that already exists.
func TestCreateRoomMatchesExistingName(t *testing.T) {
	f := &fakeConversations{}
	s, acc := newConversationServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	code, body := postJSON(t, s, "/_matrix/client/v3/createRoom", `{"name":"工作群"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	if got := asString(body["room_id"]); got != room.RoomID {
		t.Fatalf("room_id = %q, want the existing %q", got, room.RoomID)
	}
	if n := atomic.LoadInt32(&f.createGroup); n != 0 {
		t.Fatalf("create-group was called %d times", n)
	}
}

// newCreateGroupServer builds a homeserver whose upstream answers create-group.
func newCreateGroupServer(t *testing.T, f *fakeCreateGroup) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// TestCreateRoomCreatesAGroupOnYunhu is the new behaviour: "new room" in a Matrix
// client is a request to create a group, and Yunhu has an endpoint for exactly
// that. This used to be forbidden outright, so the feature was unreachable.
func TestCreateRoomCreatesAGroupOnYunhu(t *testing.T) {
	f := &fakeCreateGroup{groupID: "857744874"}
	s, acc := newCreateGroupServer(t, f)

	code, body := postJSON(t, s, "/_matrix/client/v3/createRoom",
		`{"name":"shfjfjdmd","topic":"群简介来了","preset":"public_chat","visibility":"public"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}

	// The upstream call carried the name and the topic-as-introduction.
	if n := atomic.LoadInt32(&f.calls); n != 1 {
		t.Fatalf("create-group was called %d times, want 1", n)
	}
	req := f.lastReq.Load().(*pb.CreateGroupRequest)
	if got := req.GetName(); got != "shfjfjdmd" {
		t.Fatalf("name = %q", got)
	}
	if got := req.GetIntroduction(); got != "群简介来了" {
		t.Fatalf("introduction = %q, want the topic", got)
	}

	// The room is projected under the id Yunhu returned.
	wantID := "!yh-group-857744874:test.local"
	if got := asString(body["room_id"]); got != wantID {
		t.Fatalf("room_id = %q, want %q", got, wantID)
	}
	s.Store.Mu.RLock()
	room := acc.Rooms[wantID]
	s.Store.Mu.RUnlock()
	if room == nil {
		t.Fatal("the new group was not projected")
	}
	if room.Topic != "群简介来了" {
		t.Fatalf("room topic = %q", room.Topic)
	}
}

// TestCreateRoomWithoutNameIsRejected: a Yunhu group needs a name, so a request
// with none is answered with a clear error instead of an upstream failure.
func TestCreateRoomWithoutNameIsRejected(t *testing.T) {
	f := &fakeCreateGroup{groupID: "1"}
	s, _ := newCreateGroupServer(t, f)

	code, body := postJSON(t, s, "/_matrix/client/v3/createRoom", `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %v", code, body)
	}
	if n := atomic.LoadInt32(&f.calls); n != 0 {
		t.Fatalf("create-group was called %d times for a nameless request", n)
	}
}

// fakeCreateGroup serves /v1/group/create-group and records the request.
type fakeCreateGroup struct {
	groupID string
	calls   int32
	lastReq atomic.Value
}

func (f *fakeCreateGroup) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/create-group", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.CreateGroupRequest
		_ = proto.Unmarshal(raw, &req)
		atomic.AddInt32(&f.calls, 1)
		f.lastReq.Store(&req)
		out, _ := proto.Marshal(&pb.CreateGroupResponse{
			Status:  &pb.Status{Code: 1, Msg: "success"},
			GroupId: f.groupID,
		})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	return mux
}

// TestCreateRoomDoesNotInventGroups: a DM can be materialised from the peer id
// alone, but a group cannot - inviting a group you are not in must not conjure a
// room for it. Creating a *new* group is a different request (no invite target).
func TestCreateRoomDoesNotInventGroups(t *testing.T) {
	f := &fakeConversations{}
	s, _ := newConversationServer(t, f)

	code, _ := postJSON(t, s, "/_matrix/client/v3/createRoom",
		`{"invite":["@yhgrp_999999:test.local"]}`)
	if code != http.StatusForbidden {
		t.Fatalf("status %d, want 403 for a group the account is not in", code)
	}
	if n := atomic.LoadInt32(&f.createGroup); n != 0 {
		t.Fatalf("create-group was called %d times", n)
	}
}

// TestMemberCountComesFromGroupInfo is the reported wrong number: the room only
// knows the members who spoke, so a 500-person group claimed to have a handful.
// headcount from group/info must be what the client sees, in both sync dialects.
func TestMemberCountComesFromGroupInfo(t *testing.T) {
	f := &fakeConversations{headcount: 500}
	s, acc := newConversationServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.refreshMemberCounts(context.Background(), acc, s.YunhuClient().WithToken(acc.Token))

	if n := atomic.LoadInt32(&f.infoRequests); n == 0 {
		t.Fatal("group/info was never asked for the headcount")
	}
	if room.MemberCount != 500 {
		t.Fatalf("MemberCount = %d, want 500", room.MemberCount)
	}
	if room.MemberCountTS == 0 {
		t.Fatal("MemberCountTS was not stamped, every reconnect would refetch")
	}

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 10)
	s.Store.Mu.RUnlock()

	join := resp["rooms"].(map[string]any)["join"].(map[string]any)
	entry, ok := join[room.RoomID].(map[string]any)
	if !ok {
		t.Fatalf("room missing from sync: %v", join)
	}
	summary := entry["summary"].(map[string]any)
	if got := summary["m.joined_member_count"]; got != 500 {
		t.Fatalf("m.joined_member_count = %v, want 500", got)
	}

	s.Store.Mu.RLock()
	sl, _, _ := s.slidingRoom(acc.UserID, &roomPlan{room: room, timelineLimit: 10}, 0, true, false)
	s.Store.Mu.RUnlock()
	if got := sl["joined_count"]; got != 500 {
		t.Fatalf("sliding joined_count = %v, want 500", got)
	}
}

// TestMemberCountFallsBackToLocalMembers: DMs and bots have no headcount, so the
// count must still be sane rather than zero.
func TestMemberCountFallsBackToLocalMembers(t *testing.T) {
	s, acc := newConversationServer(t, &fakeConversations{})

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	room.Members["@yh_10002:test.local"] = &model.Member{UserID: "@yh_10002:test.local", Membership: model.MembershipJoin}
	got := room.JoinedCount()
	s.Store.Mu.Unlock()

	if got != 2 {
		t.Fatalf("JoinedCount = %d, want 2 (self plus the peer)", got)
	}
}
