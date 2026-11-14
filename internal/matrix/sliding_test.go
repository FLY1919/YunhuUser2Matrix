package matrix

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// mustSliding decodes a request body exactly as the HTTP handler would, so the
// tests cover wire parsing as well as response building.
func mustSliding(t *testing.T, body string) *slidingRequest {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v4/sync", strings.NewReader(body))
	req, err := decodeSlidingRequest(r)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	return req
}

func ingestText(t *testing.T, s *Server, acc *accountRef, msgID, chatID string, chatType pb.ChatType, body string, ts uint64) {
	t.Helper()
	msg := &pb.PushMessage{
		MsgId:       msgID,
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      chatID,
		ChatType:    chatType,
		Content:     &pb.MsgContent{Text: body},
		ContentType: pb.ContentType_text,
		Timestamp:   ts,
	}
	s.ingest(acc, "push_message", msg)
}

func TestSlidingInitialListsRooms(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hello", 1700000000000)

	req := mustSliding(t, `{"lists":{"main":{"range":[0,19],"timeline_limit":10,`+
		`"required_state":[{"type":"m.room.name"},{"type":"m.room.member","state_key":"*"}]}},"timeout":0}`)

	s.Store.Mu.RLock()
	resp, has := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	if !has {
		t.Fatal("expected content on an initial request")
	}
	if pos, _ := resp["pos"].(string); pos == "" {
		t.Fatal("pos must be set")
	}

	lists := resp["lists"].(map[string]any)
	main := lists["main"].(map[string]any)
	if got := main["count"].(int); got != 1 {
		t.Fatalf("list count = %d, want 1", got)
	}

	rooms := resp["rooms"].(map[string]any)
	room, ok := rooms["!yh-user-10002:test.local"].(map[string]any)
	if !ok {
		t.Fatalf("room missing from response: %v", rooms)
	}
	if room["initial"] != true {
		t.Fatal("initial flag must be set on the first sync")
	}
	// A direct room may have no name; the key is still present (null) so the
	// client can tell "unchanged" from "cleared", and heroes name the room.
	if _, ok := room["name"]; !ok {
		t.Fatal("name key must be present on initial sync")
	}
	if room["name"] == nil {
		if _, ok := room["heroes"]; !ok {
			t.Fatal("an unnamed room must carry heroes so clients can label it")
		}
	}
	timeline := room["timeline"].([]map[string]any)
	if len(timeline) != 1 {
		t.Fatalf("timeline = %d events, want 1", len(timeline))
	}
	if got := timeline[0]["content"].(map[string]any)["body"]; got != "hello" {
		t.Fatalf("unexpected body %v", got)
	}

	// The wildcard state_key must let member events through, and types that
	// were not requested must be filtered out. Two types are deliberately
	// exempt: m.room.pinned_events (the bot board) and m.room.power_levels
	// (role badges) are always shipped.
	state := room["required_state"].([]map[string]any)
	var sawMember bool
	for _, ev := range state {
		typ, _ := ev["type"].(string)
		switch typ {
		case "m.room.member":
			sawMember = true
		case "m.room.name":
			// present only when the room actually has a name
		case "m.room.power_levels", "m.room.pinned_events":
			// always included, see slidingState
		default:
			t.Fatalf("required_state returned an unrequested type %q", typ)
		}
	}
	if !sawMember {
		t.Fatalf("wildcard state_key did not match member events: %v", state)
	}
}

func TestSlidingIncrementalOnlyChangedRooms(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "first", 1700000000000)

	initial := mustSliding(t, `{"lists":{"main":{"range":[0,19],"timeline_limit":10,"required_state":[{"type":"m.room.name"}]}}}`)
	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, initial, 0, true, false, nil)
	cursor := parseSince(resp["pos"].(string))
	s.Store.Mu.RUnlock()

	ingestText(t, s, acc, "m2", "10002", pb.ChatType_user, "second", 1700000001000)

	incr := mustSliding(t, `{"pos":"s`+resp["pos"].(string)[1:]+`","lists":{"main":{"range":[0,19],"timeline_limit":10,"required_state":[{"type":"m.room.name"}]}}}`)
	s.Store.Mu.RLock()
	resp2, has := s.buildSliding(acc, nil, incr, cursor, false, false, nil)
	s.Store.Mu.RUnlock()

	if !has {
		t.Fatal("expected the new message to produce content")
	}
	rooms := resp2["rooms"].(map[string]any)
	if len(rooms) != 1 {
		t.Fatalf("expected only the changed room, got %d", len(rooms))
	}
	room := rooms["!yh-user-10002:test.local"].(map[string]any)

	// Delta semantics: unchanged fields must be omitted, initial must be absent.
	if _, ok := room["initial"]; ok {
		t.Fatal("initial must be omitted on incremental sync")
	}
	if _, ok := room["name"]; ok {
		t.Fatal("name must be omitted when it did not change")
	}
	timeline := room["timeline"].([]map[string]any)
	if len(timeline) != 1 {
		t.Fatalf("timeline = %d events, want only the new one", len(timeline))
	}
	if got := timeline[0]["content"].(map[string]any)["body"]; got != "second" {
		t.Fatalf("unexpected body %v", got)
	}
	if got := parseSince(resp2["pos"].(string)); got <= cursor {
		t.Fatalf("pos must advance: %d -> %d", cursor, got)
	}
}

// TestSlidingResendsRoomsForUnknownConnection covers the failure mode where a
// client knows how many rooms exist but has none of them: the server used to
// answer `{"count":N,"rooms":{}}` forever, because it only ever sent deltas
// keyed off the pos token and never asked whether the client actually held the
// room. A connection with no memory of a room must get the full payload again.
func TestSlidingResendsRoomsForUnknownConnection(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hello", 1700000000000)

	const roomID = "!yh-user-10002:test.local"
	const list = `"lists":{"main":{"range":[0,19],"timeline_limit":10,` +
		`"required_state":[{"type":"m.room.name"}]}}`

	req := mustSliding(t, "{"+list+"}")

	// A connection that has already been given the room.
	known := newSlidingConnState()
	s.Store.Mu.RLock()
	resp, has := s.buildSliding(acc, nil, req, 0, true, false, known)
	s.Store.Mu.RUnlock()
	if !has {
		t.Fatal("initial sync must produce content")
	}
	cursor := parseSince(resp["pos"].(string))
	pos := "s" + strconv.FormatInt(cursor, 10)

	// Nothing happened since, so the known connection correctly gets an empty
	// delta - that part of the old behaviour was right.
	incr := mustSliding(t, `{"pos":"`+pos+`",`+list+`}`)
	s.Store.Mu.RLock()
	resp, has = s.buildSliding(acc, nil, incr, cursor, false, false, known)
	s.Store.Mu.RUnlock()
	if has || len(resp["rooms"].(map[string]any)) != 0 {
		t.Fatalf("a known connection must get an empty delta, got %v", resp["rooms"])
	}

	// A connection that never saw the room - cache cleared, app reinstalled,
	// data dir rebuilt - must get it back instead of an empty delta it can
	// never render.
	stranger := newSlidingConnState()
	s.Store.Mu.RLock()
	resp, has = s.buildSliding(acc, nil, incr, cursor, false, false, stranger)
	s.Store.Mu.RUnlock()
	if !has {
		t.Fatal("an unknown connection must be re-sent the room")
	}
	room, ok := resp["rooms"].(map[string]any)[roomID].(map[string]any)
	if !ok {
		t.Fatalf("room missing from the re-send: %v", resp["rooms"])
	}
	if room["initial"] != true {
		t.Fatal("a re-sent room must be flagged initial so the client replaces its copy")
	}
	if _, ok := room["name"]; !ok {
		t.Fatal("a re-sent room must carry its name again")
	}
	if got := len(room["timeline"].([]map[string]any)); got != 1 {
		t.Fatalf("re-sent timeline = %d events, want 1", got)
	}

	// Once delivered, the same connection goes back to deltas.
	s.Store.Mu.RLock()
	resp, has = s.buildSliding(acc, nil, incr, cursor, false, false, stranger)
	s.Store.Mu.RUnlock()
	if has || len(resp["rooms"].(map[string]any)) != 0 {
		t.Fatalf("re-send must happen once, got %v", resp["rooms"])
	}
}

func TestSlidingClassicEmitsOps(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hello", 1700000000000)

	// Classic dialect: ranges (plural) and required_state as [type, key] pairs.
	req := mustSliding(t, `{"lists":{"main":{"ranges":[[0,19]],"timeline_limit":5,`+
		`"required_state":[["m.room.name","*"]]}},"timeout":0}`)

	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, true, nil)
	s.Store.Mu.RUnlock()

	main := resp["lists"].(map[string]any)["main"].(map[string]any)
	if got := main["count"].(int); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
	ops, ok := main["ops"].([]any)
	if !ok || len(ops) == 0 {
		t.Fatalf("classic lists must carry ops, got %v", main["ops"])
	}
	op := ops[0].(map[string]any)
	if op["op"] != "SYNC" {
		t.Fatalf("op = %v, want SYNC", op["op"])
	}
	ids := op["room_ids"].([]string)
	if len(ids) != 1 || ids[0] != "!yh-user-10002:test.local" {
		t.Fatalf("room_ids = %v", ids)
	}

	// MSC3575 keeps per-room unread counters that MSC4186 removed.
	room := resp["rooms"].(map[string]any)["!yh-user-10002:test.local"].(map[string]any)
	if _, ok := room["notification_count"]; !ok {
		t.Fatal("classic responses must include notification_count")
	}
}

func TestSlidingRangeWindow(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "dm", 1700000000000)
	ingestText(t, s, acc, "m2", "20001", pb.ChatType_group, "group", 1700000001000)

	req := mustSliding(t, `{"lists":{"main":{"range":[0,0],"timeline_limit":10,"required_state":[]}}}`)
	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	// count covers the whole filtered list, the window only one room.
	if got := resp["lists"].(map[string]any)["main"].(map[string]any)["count"].(int); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
	if got := len(resp["rooms"].(map[string]any)); got != 1 {
		t.Fatalf("rooms = %d, want 1 (window [0,0])", got)
	}
}

func TestSlidingIsDmFilter(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "dm", 1700000000000)
	ingestText(t, s, acc, "m2", "20001", pb.ChatType_group, "group", 1700000001000)

	req := mustSliding(t, `{"lists":{"dms":{"range":[0,19],"timeline_limit":10,`+
		`"required_state":[],"filters":{"is_dm":true}}}}`)
	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	rooms := resp["rooms"].(map[string]any)
	if len(rooms) != 1 {
		t.Fatalf("is_dm filter returned %d rooms, want 1", len(rooms))
	}
	if _, ok := rooms["!yh-user-10002:test.local"]; !ok {
		t.Fatalf("wrong room matched: %v", rooms)
	}
}

func TestParseRequiredStateBothDialects(t *testing.T) {
	objForm := parseRequiredState(wireRequiredState{
		[]byte(`{"type":"m.room.name","state_key":"*"}`),
		[]byte(`{"type":"m.room.member","state_key":"@a:b","lazy_members":true}`),
		[]byte(`{}`),
	})
	if len(objForm) != 3 {
		t.Fatalf("got %d entries, want 3", len(objForm))
	}
	if objForm[0].Type != "m.room.name" || objForm[0].StateKey != nil {
		t.Fatalf("wildcard state_key should normalise to nil: %+v", objForm[0])
	}
	if objForm[1].StateKey == nil || *objForm[1].StateKey != "@a:b" || !objForm[1].LazyMembers {
		t.Fatalf("state_key/lazy_members lost: %+v", objForm[1])
	}
	if !objForm[2].matchAll {
		t.Fatal("{} must match everything")
	}

	pairForm := parseRequiredState(wireRequiredState{
		[]byte(`["m.room.name","*"]`),
		[]byte(`["*","*"]`),
	})
	if len(pairForm) != 2 {
		t.Fatalf("got %d entries, want 2", len(pairForm))
	}
	if pairForm[0].Type != "m.room.name" || pairForm[0].StateKey != nil {
		t.Fatalf("pair form parsed wrong: %+v", pairForm[0])
	}
	if !pairForm[1].matchAll {
		t.Fatal(`["*","*"] must match everything`)
	}
}

func TestSlidingLazyMembersKeepsSelf(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hi", 1700000000000)

	room := acc.Rooms["!yh-user-10002:test.local"]
	if room == nil {
		t.Fatal("room not created")
	}
	reqs := []slidingStateReq{{Type: "m.room.member", LazyMembers: true}}

	s.Store.Mu.RLock()
	events := s.slidingState(room, reqs, 0, true, acc.UserID)
	s.Store.Mu.RUnlock()

	if len(events) == 0 {
		t.Fatal("lazy loading must still return the client's own membership")
	}
	for _, ev := range events {
		if ev["type"] != "m.room.member" {
			// m.room.power_levels is always shipped, whatever was requested.
			continue
		}
		if key, _ := ev["state_key"].(string); key != acc.UserID {
			t.Fatalf("lazy loading leaked another member: %q", key)
		}
	}
}

func TestSlidingHandlerRequiresAuth(t *testing.T) {
	s, _ := newTestServer(t)
	rec := httptest.NewRecorder()
	body := `{"lists":{"main":{"range":[0,19],"timeline_limit":10,"required_state":[]}}}`
	req := httptest.NewRequest(http.MethodPost,
		"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", strings.NewReader(body))
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// TestSlidingQueryParams covers Element X, which puts pos/timeout in the query
// string while the body carries only conn_id/lists/extensions.
func TestSlidingQueryParams(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost,
		"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?pos=s42&timeout=30000&conn_id=encryption",
		strings.NewReader(`{"extensions":{"to_device":{"enabled":true}}}`))
	req, err := decodeSlidingRequest(r)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.Pos != "s42" {
		t.Fatalf("pos = %q, want s42 (query must be read)", req.Pos)
	}
	if req.Timeout == nil || *req.Timeout != 30000 {
		t.Fatalf("timeout = %v, want 30000", req.Timeout)
	}
	if req.ConnID != "encryption" {
		t.Fatalf("conn_id = %q", req.ConnID)
	}

	// The body wins when it carries a value.
	r2 := httptest.NewRequest(http.MethodPost, "/sync?pos=s1&timeout=1000",
		strings.NewReader(`{"pos":"s99","timeout":2500}`))
	req2, err := decodeSlidingRequest(r2)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req2.Pos != "s99" {
		t.Fatalf("body pos should win, got %q", req2.Pos)
	}
	if req2.Timeout == nil || *req2.Timeout != 2500 {
		t.Fatalf("body timeout should win, got %v", req2.Timeout)
	}
}

// TestParseRequiredStateSentinels checks $ME and $LAZY, which Element X sends.
func TestParseRequiredStateSentinels(t *testing.T) {
	got := parseRequiredState(wireRequiredState{
		[]byte(`["m.room.member","$LAZY"]`),
		[]byte(`["m.room.member","$ME"]`),
		[]byte(`["m.room.name",""]`),
		[]byte(`["m.space.child","*"]`),
	})
	if len(got) != 4 {
		t.Fatalf("got %d entries", len(got))
	}
	if !got[0].LazyMembers {
		t.Fatal(`$LAZY must enable lazy loading`)
	}
	if !got[1].selfOnly {
		t.Fatal(`$ME must mean the requesting user`)
	}
	if got[2].StateKey == nil || *got[2].StateKey != "" {
		t.Fatalf(`["m.room.name",""] must mean the empty state key, got %v`, got[2].StateKey)
	}
	if got[3].StateKey != nil {
		t.Fatalf(`"*" must mean any state key, got %v`, *got[3].StateKey)
	}
}

// TestSlidingSelfOnlyState verifies $ME actually returns the caller's membership.
func TestSlidingSelfOnlyState(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	ingestText(t, s, acc, "m1", "10002", pb.ChatType_user, "hi", 1700000000000)

	room := acc.Rooms["!yh-user-10002:test.local"]
	if room == nil {
		t.Fatal("room not created")
	}
	reqs := []slidingStateReq{{Type: "m.room.member", selfOnly: true}}

	s.Store.Mu.RLock()
	events := s.slidingState(room, reqs, 0, true, acc.UserID)
	s.Store.Mu.RUnlock()

	// m.room.power_levels is always included; only the membership matters here.
	members := make([]map[string]any, 0, 1)
	for _, ev := range events {
		if ev["type"] == "m.room.member" {
			members = append(members, ev)
		}
	}
	if len(members) != 1 {
		t.Fatalf("$ME returned %d member events, want exactly the caller's membership", len(members))
	}
	if key, _ := members[0]["state_key"].(string); key != acc.UserID {
		t.Fatalf("$ME returned %q", key)
	}
}
