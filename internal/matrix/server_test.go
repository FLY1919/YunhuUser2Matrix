package matrix

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/hub"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Server.Name = "test.local"
	cfg.Yunhu.Backfill = 0
	// config.Default() points at the real Yunhu API. Pin it to a closed port so
	// a test that accidentally reaches the network fails instantly instead of
	// talking to production with a bogus token.
	cfg.Yunhu.BaseURL = "http://127.0.0.1:1"
	cfg.Yunhu.WSURL = "ws://127.0.0.1:1/"
	return cfg
}

// newTestServer builds a homeserver with a throwaway data directory. No network
// is used: the store is local and the Yunhu client is never dialled.
func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	return newTestServerWith(t, nil)
}

// newTestServerWith is newTestServer with a hook to adjust the config before the
// server is built, used by tests that need a fake upstream.
func newTestServerWith(t *testing.T, tweak func(*config.Config)) (*Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	cfg := testConfig()
	cfg.Storage.DataDir = dir
	if tweak != nil {
		tweak(cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(dir, time.Hour, 1000, log)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s, err := NewServer(cfg, st, hub.New(), log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	// /login starts a websocket runner; without this every test would leak a
	// goroutine that keeps retrying the (deliberately unreachable) upstream.
	t.Cleanup(s.StopRunners)
	return s, st
}

func seedAccount(t *testing.T, st *store.Store) *model.Account {
	t.Helper()
	acc := model.NewAccount("10001", "@yh_10001:test.local")
	acc.Name = "云湖测试"
	acc.Token = "token"
	st.PutAccount(acc)
	return acc
}

func TestSyncProjectsYunhuMessage(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	msg := &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{Text: "hello"},
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
		MsgSeq:      1,
	}
	s.ingest(acc, "push_message", msg)

	s.Store.Mu.RLock()
	resp, has := s.buildSync(acc, nil, 0, true, 30)
	s.Store.Mu.RUnlock()
	if !has {
		t.Fatal("expected sync content")
	}

	rooms := resp["rooms"].(map[string]any)["join"].(map[string]any)
	room, ok := rooms["!yh-user-10002:test.local"].(map[string]any)
	if !ok {
		t.Fatalf("room missing from sync: %v", rooms)
	}
	timeline := room["timeline"].(map[string]any)["events"].([]map[string]any)
	if len(timeline) != 1 {
		t.Fatalf("expected 1 timeline event, got %d", len(timeline))
	}
	if got := timeline[0]["sender"]; got != "@yh_10002:test.local" {
		t.Fatalf("unexpected sender %v", got)
	}
	if body := timeline[0]["content"].(map[string]any)["body"]; body != "hello" {
		t.Fatalf("unexpected body %v", body)
	}

	// The same message must not be appended twice (websocket echo dedupe).
	s.ingest(acc, "push_message", msg)
	s.Store.Mu.RLock()
	count := len(acc.Rooms["!yh-user-10002:test.local"].Events)
	s.Store.Mu.RUnlock()
	if count != len(timeline)+7 { // 7 state events + 1 message
		t.Logf("events after duplicate ingest: %d", count)
	}
	if count > 8 {
		t.Fatalf("duplicate message was appended: %d events", count)
	}
}

func TestIncrementalSyncRespectsCursor(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	first := &pb.PushMessage{
		MsgId: "a", Sender: &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user},
		ChatId: "10002", ChatType: pb.ChatType_user,
		Content: &pb.MsgContent{Text: "one"}, ContentType: pb.ContentType_text,
		Timestamp: 1700000000000,
	}
	s.ingest(acc, "push_message", first)

	s.Store.Mu.RLock()
	pos := acc.Pos
	s.Store.Mu.RUnlock()

	second := &pb.PushMessage{
		MsgId: "b", Sender: &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user},
		ChatId: "10002", ChatType: pb.ChatType_user,
		Content: &pb.MsgContent{Text: "two"}, ContentType: pb.ContentType_text,
		Timestamp: 1700000001000,
	}
	s.ingest(acc, "push_message", second)

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, pos, false, 30)
	s.Store.Mu.RUnlock()

	rooms := resp["rooms"].(map[string]any)["join"].(map[string]any)
	room := rooms["!yh-user-10002:test.local"].(map[string]any)
	timeline := room["timeline"].(map[string]any)["events"].([]map[string]any)
	if len(timeline) != 1 {
		t.Fatalf("expected only the new event, got %d", len(timeline))
	}
	if body := timeline[0]["content"].(map[string]any)["body"]; body != "two" {
		t.Fatalf("unexpected body %v", body)
	}
}

func TestBuildSendRequestText(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	req, err := s.buildSendRequest(context.Background(), nil, room.ChatID, room.ChatType, "m.room.message",
		map[string]any{"msgtype": "m.text", "body": "hi"}, "mid1")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if req.ChatId != "10002" || req.ContentType != pb.ContentType_text {
		t.Fatalf("unexpected request %+v", req)
	}
	if req.Content.Text != "hi" {
		t.Fatalf("unexpected text %q", req.Content.Text)
	}
}

func TestBuildSendRequestReply(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "群", "")
	s.Store.Mu.Unlock()

	req, err := s.buildSendRequest(context.Background(), nil, room.ChatID, room.ChatType, "m.room.message",
		map[string]any{
			"msgtype": "m.text",
			"body":    "reply",
			"m.relates_to": map[string]any{
				"m.in_reply_to": map[string]any{"event_id": eventIDFor("orig")},
			},
		}, "mid2")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if req.QuoteMsgId != "orig" {
		t.Fatalf("expected quote orig, got %q", req.QuoteMsgId)
	}
}

func TestMarkdownRendering(t *testing.T) {
	out := mdToHTML("**粗体** 和 `code`")
	if !strings.Contains(out, "<strong>粗体</strong>") {
		t.Fatalf("bold not rendered: %s", out)
	}
	if !strings.Contains(out, "<code>code</code>") {
		t.Fatalf("inline code not rendered: %s", out)
	}
	if got := stripHTML("<b>hi</b> there"); got != "hi there" {
		t.Fatalf("stripHTML = %q", got)
	}
}

// TestSyncHandlerHTTP exercises the real router, including auth.
func TestSyncHandlerHTTP(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	dev := &model.Device{ID: "D1", Token: "tok", LastSeen: time.Now().UnixMilli()}
	st.AddDevice(acc, dev)

	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync?timeout=0", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := body["next_batch"]; !ok {
		t.Fatalf("missing next_batch: %v", body)
	}

	// Unauthenticated access must be rejected.
	rec2 := httptest.NewRecorder()
	s.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/sync", nil))
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec2.Code)
	}
}

// TestRouterEmptyStateKey makes sure a trailing slash maps to an empty state key.
func TestRouterEmptyStateKey(t *testing.T) {
	var gotKey string
	rt := NewRouter(nil)
	rt.GET("/_matrix/client/:ver/rooms/:roomId/state/:eventType/:stateKey", false, func(c *Ctx) {
		gotKey = c.Param("stateKey")
	})
	rt.GET("/_matrix/client/:ver/rooms/:roomId/state/:eventType", false, func(c *Ctx) {
		gotKey = "<none>"
	})

	w := httptest.NewRecorder()
	rt.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/rooms/!a:b/state/m.room.member/", nil))
	if gotKey != "" {
		t.Fatalf("expected empty state key, got %q", gotKey)
	}

	w2 := httptest.NewRecorder()
	rt.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/rooms/!a:b/state/m.room.name", nil))
	if gotKey != "<none>" {
		t.Fatalf("expected default handler, got %q", gotKey)
	}
}
