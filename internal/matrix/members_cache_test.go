package matrix

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeRosterCount serves list-member, optionally failing, and counts calls.
type fakeRosterCount struct {
	fail  bool
	calls int32
	mu    sync.Mutex
}

func (f *fakeRosterCount) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/group/list-member", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.calls, 1)
		raw, _ := io.ReadAll(r.Body)
		var req pb.ListMemberRequest
		_ = proto.Unmarshal(raw, &req)
		if f.fail {
			http.Error(w, "upstream down", http.StatusInternalServerError)
			return
		}
		out := &pb.ListMemberResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
		if req.GetData().GetPage() == 1 {
			out.User = []*pb.ListMemberResponse_User{{
				UserInfo:        &pb.ListMemberResponse_User_UserInfo{UserId: "2068882", Name: "RosterOnly"},
				PermissionLevel: 2,
			}}
		}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

func newRosterCacheServer(t *testing.T, f *fakeRosterCount) (*Server, *model.Account, *model.Room) {
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
	room := s.ensureRoomLocked(acc, "105836869", model.ChatTypeGroup, "WorkGroup", "")
	s.Store.Mu.Unlock()
	return s, acc, room
}

// TestFailedRosterFetchIsCached is the reported runaway loop: opening the invite
// picker hits /members repeatedly, and a fetch that fails used to leave the TTL at
// zero - so every single call retried upstream, forever.
func TestFailedRosterFetchIsCached(t *testing.T) {
	f := &fakeRosterCount{fail: true}
	s, _, room := newRosterCacheServer(t, f)

	for i := 0; i < 5; i++ {
		req := newAuthRequest("GET",
			"/_matrix/client/v3/rooms/"+room.RoomID+"/members", "")
		if rec := serve(s, req); rec.Code != http.StatusOK {
			t.Fatalf("call %d: status %d", i, rec.Code)
		}
	}

	// One attempt, then the TTL keeps the rest from hitting upstream.
	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("upstream list-member called %d times for 5 requests, want 1 "+
			"(a failed fetch must still start the TTL)", got)
	}
}

// TestConcurrentMemberRequestsShareOneFetch: /members is synchronous, so without
// an in-flight guard every concurrent request starts its own page-through.
func TestConcurrentMemberRequestsShareOneFetch(t *testing.T) {
	f := &fakeRosterCount{}
	s, _, room := newRosterCacheServer(t, f)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := newAuthRequest("GET",
				"/_matrix/client/v3/rooms/"+room.RoomID+"/members", "")
			serve(s, req)
		}()
	}
	wg.Wait()

	// The fetch is fast here, so a couple may serialize; what must not happen is
	// one upstream call per request.
	if got := atomic.LoadInt32(&f.calls); got > 3 {
		t.Fatalf("upstream list-member called %d times for 8 concurrent requests; "+
			"the in-flight guard is not working", got)
	}
}

// TestRosterIsCachedForItsTTL: two sequential requests share one fetch.
func TestRosterIsCachedForItsTTL(t *testing.T) {
	f := &fakeRosterCount{}
	s, _, room := newRosterCacheServer(t, f)

	for i := 0; i < 3; i++ {
		req := newAuthRequest("GET",
			"/_matrix/client/v3/rooms/"+room.RoomID+"/members", "")
		if rec := serve(s, req); rec.Code != http.StatusOK {
			t.Fatalf("call %d: status %d", i, rec.Code)
		}
	}

	if got := atomic.LoadInt32(&f.calls); got != 1 {
		t.Fatalf("upstream called %d times, want 1", got)
	}

	s.Store.Mu.RLock()
	ts := room.GroupMembersTS
	s.Store.Mu.RUnlock()
	if ts == 0 {
		t.Fatal("GroupMembersTS was not stamped")
	}
}

// TestRosterMembersAppearInTheMemberList: the roster has to reach the chunk, not
// just the people who spoke.
func TestRosterMembersAppearInTheMemberList(t *testing.T) {
	f := &fakeRosterCount{}
	s, _, room := newRosterCacheServer(t, f)

	req := newAuthRequest("GET", "/_matrix/client/v3/rooms/"+room.RoomID+"/members", "")
	rec := serve(s, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := decodeJSON(t, rec)
	chunk, _ := body["chunk"].([]any)

	var found bool
	for _, raw := range chunk {
		ev, _ := raw.(map[string]any)
		if asString(ev["state_key"]) == "@yh_2068882:test.local" {
			found = true
			content, _ := ev["content"].(map[string]any)
			if asString(content["displayname"]) != "RosterOnly" {
				t.Fatalf("displayname = %q", content["displayname"])
			}
		}
	}
	if !found {
		t.Fatalf("a roster-only member is missing from the chunk: %v", chunk)
	}
}
