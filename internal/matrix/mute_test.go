package matrix

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeMute serves the conversation list and the no-notify endpoint.
type fakeMute struct {
	doNotDisturb bool
	mutes        int32
	lastMuted    atomic.Value
}

func (f *fakeMute) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/conversation/list", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		out, _ := proto.Marshal(&pb.ConversationListResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: []*pb.ConversationListResponse_Data{{
				ChatId:       "979377289",
				ChatType:     pb.ChatType_group,
				Name:         "工作群",
				DoNotDisturb: f.doNotDisturb,
				TimestampMs:  1700000000000,
			}},
		})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	mux.HandleFunc("/v1/friend/no-notify", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			ChatID   string `json:"chatId"`
			NoNotify int    `json:"noNotify"`
		}
		_ = json.Unmarshal(raw, &body)
		atomic.AddInt32(&f.mutes, 1)
		f.lastMuted.Store(body.NoNotify == 1)
		// The real endpoint answers JSON, not protobuf. A fake that disagrees with
		// the wire is worse than none: it once made this pass with the wrong
		// decoder and fail in production.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":1,"msg":"success"}`))
	})
	return mux
}

func newMuteServer(t *testing.T, f *fakeMute) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
		cfg.Yunhu.Backfill = 0
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// pushRulePUT issues a PUT with a bearer token, the way a client mutes a room.
func pushRulePUT(t *testing.T, s *Server, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestPutOverridePushRuleMutes is the reported 400: Element X mutes a room with
// an *override* rule, not a room rule:
//
//	PUT /pushrules/global/override/<room_id>
//	{"actions": [], "conditions": [{"kind":"event_match","key":"room_id","pattern":"<room_id>"}]}
//
// Only `room` was accepted, so Element X got M_INVALID_PARAM and the mute never
// happened - and because it re-applies its mutes on every launch, it kept trying.
func TestPutOverridePushRuleMutes(t *testing.T) {
	f := &fakeMute{}
	s, acc := newMuteServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "105836869", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	// The exact payload from the report.
	body := `{"actions":[],"conditions":[{"kind":"event_match","key":"room_id",` +
		`"pattern":"` + room.RoomID + `"}]}`
	code, resp := pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/override/"+room.RoomID, body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	s.Store.Mu.RLock()
	muted := acc.Rooms[room.RoomID].Muted
	s.Store.Mu.RUnlock()
	if !muted {
		t.Fatal("the override rule did not mute the room")
	}
	if n := atomic.LoadInt32(&f.mutes); n != 1 {
		t.Fatalf("upstream mute calls = %d, want 1", n)
	}
	if got, _ := f.lastMuted.Load().(bool); !got {
		t.Fatal("upstream was told to unmute")
	}
}

// TestMutedRoomIsReportedAsOverride is the other half: Element X reads its mutes
// back from global.override. Reporting them only under global.room meant the
// client never saw them and re-applied every mute on every launch.
func TestMutedRoomIsReportedAsOverride(t *testing.T) {
	f := &fakeMute{doNotDisturb: true}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	roomID := "!yh-group-979377289:test.local"

	_, body := doJSON(t, s, http.MethodGet, "/_matrix/client/v3/pushrules/", "")
	global, _ := body["global"].(map[string]any)

	for _, kind := range []string{"override", "room"} {
		rules, _ := global[kind].([]any)
		if len(rules) != 1 {
			t.Fatalf("%s rules = %v, want one mute rule", kind, global[kind])
		}
		rule, _ := rules[0].(map[string]any)
		if got, _ := rule["rule_id"].(string); got != roomID {
			t.Fatalf("%s rule_id = %q", kind, got)
		}
		actions, _ := rule["actions"].([]any)
		if len(actions) != 0 {
			t.Fatalf("%s actions = %v, want []", kind, actions)
		}
	}

	// The override variant must carry the condition Element X filters by.
	overrides, _ := global["override"].([]any)
	rule, _ := overrides[0].(map[string]any)
	conds, _ := rule["conditions"].([]any)
	if len(conds) != 1 {
		t.Fatalf("override conditions = %v, want one event_match", conds)
	}
	cond, _ := conds[0].(map[string]any)
	if asString(cond["kind"]) != "event_match" ||
		asString(cond["key"]) != "room_id" ||
		asString(cond["pattern"]) != roomID {
		t.Fatalf("condition = %v", cond)
	}
}

// TestDeleteOverridePushRuleUnmutes: Element X clears a mute by deleting the
// override rule.
func TestDeleteOverridePushRuleUnmutes(t *testing.T) {
	f := &fakeMute{doNotDisturb: true}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	roomID := "!yh-group-979377289:test.local"

	req := httptest.NewRequest(http.MethodDelete,
		"/_matrix/client/v3/pushrules/global/override/"+roomID, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	s.Store.Mu.RLock()
	muted := acc.Rooms[roomID].Muted
	s.Store.Mu.RUnlock()
	if muted {
		t.Fatal("the room is still muted after deleting the override rule")
	}
}

// TestMuteFromYunhuBecomesPushRule: muting a room in the Yunhu client has to show
// up as a muted room in Matrix - which means the room's push rule exists with
// empty actions. The rule set used to be hard-coded empty, so the mute was
// invisible.
func TestMuteFromYunhuBecomesPushRule(t *testing.T) {
	f := &fakeMute{doNotDisturb: true}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	roomID := "!yh-group-979377289:test.local"

	s.Store.Mu.RLock()
	muted := acc.Rooms[roomID].Muted
	s.Store.Mu.RUnlock()
	if !muted {
		t.Fatal("do_not_disturb was not projected onto the room")
	}

	_, body := doJSON(t, s, http.MethodGet, "/_matrix/client/v3/pushrules/", "")
	global, _ := body["global"].(map[string]any)
	rules, _ := global["room"].([]any)
	if len(rules) != 1 {
		t.Fatalf("room rules = %v, want one mute rule", global["room"])
	}
	rule, _ := rules[0].(map[string]any)
	if got, _ := rule["rule_id"].(string); got != roomID {
		t.Fatalf("rule_id = %q, want %q", got, roomID)
	}
	actions, _ := rule["actions"].([]any)
	if len(actions) != 0 {
		t.Fatalf("actions = %v, want [] for a muted room", actions)
	}
}

// TestUnmutedRoomHasNoRule keeps the response small: an absent rule already means
// "notify", so emitting one per room would flood the list.
func TestUnmutedRoomHasNoRule(t *testing.T) {
	f := &fakeMute{doNotDisturb: false}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	_, body := doJSON(t, s, http.MethodGet, "/_matrix/client/v3/pushrules/", "")
	global, _ := body["global"].(map[string]any)
	rules, _ := global["room"].([]any)
	if len(rules) != 0 {
		t.Fatalf("room rules = %v, want none for an unmuted room", rules)
	}
}

// TestPutPushRuleMutesAndSyncs is the reported request: the client PUTs
// {"actions":[]} to mute a room. That used to 404, so the toggle did nothing.
func TestPutPushRuleMutesAndSyncs(t *testing.T) {
	f := &fakeMute{}
	s, acc := newMuteServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	code, resp := pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/room/"+room.RoomID, `{"actions":[]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	s.Store.Mu.RLock()
	muted := acc.Rooms[room.RoomID].Muted
	s.Store.Mu.RUnlock()
	if !muted {
		t.Fatal("the room was not marked muted")
	}
	// And it reached Yunhu, spelled the way the API wants (noNotify: 1).
	if n := atomic.LoadInt32(&f.mutes); n != 1 {
		t.Fatalf("upstream mute calls = %d, want 1", n)
	}
	if got, _ := f.lastMuted.Load().(bool); !got {
		t.Fatal("upstream was told to unmute")
	}

	// The rule is now readable, which is how the client draws the bell.
	code, got := doJSON(t, s, http.MethodGet, "/_matrix/client/v3/pushrules/global/room/"+room.RoomID, "")
	if code != http.StatusOK {
		t.Fatalf("GET rule status %d", code)
	}
	actions, _ := got["actions"].([]any)
	if len(actions) != 0 {
		t.Fatalf("actions = %v, want []", actions)
	}
}

// TestPutPushRuleUnmutes: ["notify"] clears the mute.
func TestPutPushRuleUnmutes(t *testing.T) {
	f := &fakeMute{doNotDisturb: true}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	roomID := "!yh-group-979377289:test.local"

	code, resp := pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/room/"+roomID,
		`{"actions":["notify"]}`)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	s.Store.Mu.RLock()
	muted := acc.Rooms[roomID].Muted
	s.Store.Mu.RUnlock()
	if muted {
		t.Fatal("the room is still muted")
	}
	if got, _ := f.lastMuted.Load().(bool); got {
		t.Fatal("upstream was told to mute")
	}
}

// TestDeletePushRuleUnmutes: removing the rule is how a client clears a mute.
func TestDeletePushRuleUnmutes(t *testing.T) {
	f := &fakeMute{doNotDisturb: true}
	s, acc := newMuteServer(t, f)

	s.refreshAccount(context.Background(), acc, "")
	roomID := "!yh-group-979377289:test.local"

	req := httptest.NewRequest(http.MethodDelete,
		"/_matrix/client/v3/pushrules/global/room/"+roomID, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	s.Store.Mu.RLock()
	muted := acc.Rooms[roomID].Muted
	s.Store.Mu.RUnlock()
	if muted {
		t.Fatal("the room is still muted after deleting the rule")
	}
}

// TestPutPushRuleRejectsUnknownActions: guessing at an action list would write the
// wrong state both locally and upstream.
func TestPutPushRuleRejectsUnknownActions(t *testing.T) {
	f := &fakeMute{}
	s, acc := newMuteServer(t, f)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	code, _ := pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/room/"+room.RoomID,
		`{"actions":["dont_notify","coalesce"]}`)
	// dont_notify is Matrix's old spelling of "silence" and is accepted as a mute.
	if code != http.StatusOK {
		t.Fatalf("status %d for dont_notify, want 200", code)
	}
	code, _ = pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/room/"+room.RoomID,
		`{"actions":["nonsense"]}`)
	// A list with no known meaning has no "notify" in it, which reads as muted -
	// which is the safe direction, and matches what the client asked for.
	if code != http.StatusOK {
		t.Fatalf("status %d, want 200 (unknown actions read as muted)", code)
	}
}

// TestPushRuleUnknownRoom: a rule for a room this account does not have is a 404,
// not a silent success that would later confuse the client.
func TestPushRuleUnknownRoom(t *testing.T) {
	f := &fakeMute{}
	s, _ := newMuteServer(t, f)

	code, _ := pushRulePUT(t, s, "/_matrix/client/v3/pushrules/global/room/!yh-group-1:test.local",
		`{"actions":[]}`)
	if code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", code)
	}
}
