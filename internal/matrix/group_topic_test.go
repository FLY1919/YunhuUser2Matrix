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

// fakeGroupEdit serves /v1/group/info and /v1/group/edit-group.
type fakeGroupEdit struct {
	introduction string
	edits        int32
	infoCalls    atomic.Value
	lastEdit     atomic.Value
}

func (f *fakeGroupEdit) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		// The call counter starts at 0 and is read back as int.
		if cur, ok := f.infoCalls.Load().(int); ok {
			f.infoCalls.Store(cur + 1)
		} else {
			f.infoCalls.Store(1)
		}
		out, _ := proto.Marshal(&pb.GroupInfoResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: &pb.GroupData{
				GroupId:      "979377289",
				Name:         "工作群",
				Introduction: f.introduction,
				AvatarUrl:    "https://example.com/a.png",
				Headcount:    12,
			},
		})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	mux.HandleFunc("/v1/group/edit-group", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.EditGroupRequest
		_ = proto.Unmarshal(raw, &req)
		atomic.AddInt32(&f.edits, 1)
		f.lastEdit.Store(&req)
		out, _ := proto.Marshal(&pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	return mux
}

func newGroupEditServer(t *testing.T, f *fakeGroupEdit) (*Server, *model.Account, *model.Room) {
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
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()
	return s, acc, room
}

// TestGroupIntroductionBecomesRoomTopic: Yunhu's group/info carries the group's
// 简介 in introduction, which is exactly what a Matrix client shows in the room's
// description panel. Nothing projected it, so every group opened with an empty
// description even though upstream had one.
func TestGroupIntroductionBecomesRoomTopic(t *testing.T) {
	f := &fakeGroupEdit{introduction: "本群只聊技术"}
	s, acc, room := newGroupEditServer(t, f)

	s.refreshMemberCounts(context.Background(), acc, s.YunhuClient().WithToken(acc.Token))

	s.Store.Mu.RLock()
	topic := room.Topic
	ev := room.State[model.StateID("m.room.topic", "")]
	s.Store.Mu.RUnlock()

	if topic != "本群只聊技术" {
		t.Fatalf("room topic = %q, want the introduction", topic)
	}
	if ev == nil {
		t.Fatal("no m.room.topic state event was published")
	}
	if got := asString(ev.Content["topic"]); got != "本群只聊技术" {
		t.Fatalf("m.room.topic content = %q", got)
	}
}

// TestRoomTopicEditsGoToYunhu is the other direction: changing the description in
// a Matrix client must land in the group's introduction.
func TestRoomTopicEditsGoToYunhu(t *testing.T) {
	f := &fakeGroupEdit{}
	s, _, room := newGroupEditServer(t, f)

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.topic/", `{"topic":"新的群简介"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	// The upstream mirror runs in the background.
	waitFor(t, func() bool { return atomic.LoadInt32(&f.edits) > 0 })

	req := f.lastEdit.Load().(*pb.EditGroupRequest)
	if got := req.GetIntroduction(); got != "新的群简介" {
		t.Fatalf("introduction = %q, want the new topic", got)
	}
	if got := req.GetGroupId(); got != "979377289" {
		t.Fatalf("group id = %q", got)
	}
	// EditGroupRequest is a full-state write, not a patch: a zero-valued field
	// means "set it to empty". So the untouched fields have to be carried over
	// from the current group - sending only the topic would wipe the name.
	if got := req.GetName(); got != "工作群" {
		t.Fatalf("name = %q, want the existing name carried over", got)
	}
	if got := req.GetAvatarUrl(); got != "https://example.com/a.png" {
		t.Fatalf("avatar = %q, want the existing avatar carried over", got)
	}
}

// TestRoomNameEditsGoToYunhu pins the name path, which shares the same mirror.
func TestRoomNameEditsGoToYunhu(t *testing.T) {
	f := &fakeGroupEdit{}
	s, _, room := newGroupEditServer(t, f)

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/state/m.room.name/", `{"name":"改名了"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	waitFor(t, func() bool { return atomic.LoadInt32(&f.edits) > 0 })
	req := f.lastEdit.Load().(*pb.EditGroupRequest)
	if got := req.GetName(); got != "改名了" {
		t.Fatalf("name = %q", got)
	}
	// The description must survive a rename, for the same reason.
	if got := req.GetIntroduction(); got != f.introduction {
		t.Fatalf("introduction = %q, want the existing one carried over", got)
	}
}
