package matrix

import (
	"context"
	"encoding/json"
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

// fakeBoard serves /v1/bot/board.
type fakeBoard struct {
	boards []*pb.BotBoardMessage
	calls  int32
}

func (f *fakeBoard) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/bot/board", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.calls, 1)
		_, _ = io.ReadAll(r.Body)
		out := &pb.BoardResponse{Status: &pb.Status{Code: 1, Msg: "success"}, Board: f.boards}
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

// boardMsg builds one bot's board.
func boardMsg(botID, content string, ts int64, ct pb.ContentTypeTextOnly) *pb.BotBoardMessage {
	return &pb.BotBoardMessage{
		BotId:               orDefault(botID, "21385383"),
		ChatId:              "20001",
		ChatType:            pb.ChatType_group,
		Content:             content,
		ContentType:         ct,
		LastUpdateTimestamp: ts,
		BotName:             "看板机器人" + botID,
	}
}

// pinnedOf returns the pinned event ids of a room.
func pinnedOf(room *model.Room) []string {
	ev := room.State[model.StateID("m.room.pinned_events", "")]
	if ev == nil {
		return nil
	}
	switch raw := ev.Content["pinned"].(type) {
	case []string:
		return raw
	case []any:
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			out = append(out, asString(v))
		}
		return out
	}
	return nil
}

// TestBoardBecomesPinnedEvent is the feature: Yunhu's 群机器人看板 must show up
// as a pinned message, which is what a Matrix client's pin panel reads.
func TestBoardBecomesPinnedEvent(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	b := boardMsg("21385383", "欢迎来到本群，规则见下", 1700000000000, pb.ContentTypeTextOnly_TEXT_text)
	s.applyBoard(acc, b)

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	board := room.BoardEventByID("21385383")
	if board == nil {
		t.Fatal("board was not projected")
	}
	if board.Type != "m.room.message" {
		t.Fatalf("board type = %s, want m.room.message", board.Type)
	}
	if want := "@yhbot_21385383:test.local"; board.Sender != want {
		t.Fatalf("board sender = %s, want the bot's own user %s", board.Sender, want)
	}
	if got := asString(board.Content["body"]); got != "欢迎来到本群，规则见下" {
		t.Fatalf("body = %q", got)
	}
	if board.OriginTS != 1700000000000 {
		t.Fatalf("origin_server_ts = %d, want the board's last_update_timestamp", board.OriginTS)
	}

	pinned := pinnedOf(room)
	if len(pinned) != 1 || pinned[0] != board.EventID {
		t.Fatalf("pinned = %v, want [%s]", pinned, board.EventID)
	}

	// The board must not pollute the conversation.
	for _, ev := range room.Events {
		if ev.EventID == board.EventID {
			t.Fatal("the board event leaked into the timeline")
		}
	}

	// ...but /event/{id} has to resolve it, which is how the pin panel renders
	// an item it never saw in the timeline.
	if found := room.FindEvent(board.EventID); found != board {
		t.Fatalf("FindEvent(%s) did not return the board", board.EventID)
	}

	// The bot needs a profile so the pinned card is not anonymous.
	if m := room.State[model.StateID("m.room.member", board.Sender)]; m == nil {
		t.Fatal("the board bot has no m.room.member event")
	} else if got := asString(m.Content["displayname"]); got != "看板机器人21385383" {
		t.Fatalf("bot displayname = %q", got)
	}
}

// TestBoardMarkdownIsFormatted: the board has its own text flavour.
func TestBoardMarkdownIsFormatted(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoard(acc, boardMsg("1", "**重要** 通知", 1700000000000, pb.ContentTypeTextOnly_TEXT_markdown))

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	board := room.BoardEventByID("1")
	if board == nil {
		t.Fatal("board was not projected")
	}
	if got := asString(board.Content["format"]); got != "org.matrix.custom.html" {
		t.Fatalf("format = %q", got)
	}
	if got := asString(board.Content["formatted_body"]); got != "<strong>重要</strong> 通知" {
		t.Fatalf("formatted_body = %q", got)
	}
}

// TestBoardRefreshIsIdempotent: the same revision must not produce a new event
// or a new pin state, or every /messages page would republish it.
func TestBoardRefreshIsIdempotent(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	b := boardMsg("1", "看板内容", 1700000000000, pb.ContentTypeTextOnly_TEXT_text)
	s.applyBoard(acc, b)

	s.Store.Mu.RLock()
	firstID := room.BoardEventByID("1").EventID
	firstPos := room.State[model.StateID("m.room.pinned_events", "")].Pos
	s.Store.Mu.RUnlock()

	s.applyBoard(acc, b)

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if got := room.BoardEventByID("1").EventID; got != firstID {
		t.Fatalf("event id changed on an unchanged board: %s -> %s", firstID, got)
	}
	if got := room.State[model.StateID("m.room.pinned_events", "")].Pos; got != firstPos {
		t.Fatalf("pin state was republished (pos %d -> %d)", firstPos, got)
	}
}

// TestBoardRevisionUpdatesPin: a new revision replaces the pinned event.
func TestBoardRevisionUpdatesPin(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoard(acc, boardMsg("1", "第一版", 1700000000000, pb.ContentTypeTextOnly_TEXT_text))
	s.Store.Mu.RLock()
	first := room.BoardEventByID("1").EventID
	s.Store.Mu.RUnlock()

	s.applyBoard(acc, boardMsg("1", "第二版", 1700000005000, pb.ContentTypeTextOnly_TEXT_text))

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	board := room.BoardEventByID("1")
	if board.EventID == first {
		t.Fatal("a new board revision reused the old event id")
	}
	if got := asString(board.Content["body"]); got != "第二版" {
		t.Fatalf("body = %q, want 第二版", got)
	}
	pinned := pinnedOf(room)
	if len(pinned) != 1 || pinned[0] != board.EventID {
		t.Fatalf("pinned = %v, want the new event %s", pinned, board.EventID)
	}
}

// TestBoardClearedUnpins: an empty board removes the pin instead of pinning an
// empty message.
func TestBoardClearedUnpins(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoard(acc, boardMsg("1", "有内容", 1700000000000, pb.ContentTypeTextOnly_TEXT_text))
	s.applyBoard(acc, boardMsg("1", "   ", 1700000009000, pb.ContentTypeTextOnly_TEXT_text))

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if room.BoardEventByID("1") != nil {
		t.Fatal("an empty board must not stay pinned")
	}
	if got := pinnedOf(room); len(got) != 0 {
		t.Fatalf("pinned = %v, want none", got)
	}
}

// TestMultipleBoardsAreAllPinned is the multi-bot case: Yunhu's own client reads
// `repeated board`, because one group can host several bots and each keeps its
// own board. Projecting only one would hide the rest from the pin panel.
func TestMultipleBoardsAreAllPinned(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoards(acc, "20001", model.ChatTypeGroup, []*pb.BotBoardMessage{
		boardMsg("100", "签到机器人看板", 1700000000000, pb.ContentTypeTextOnly_TEXT_text),
		boardMsg("200", "投票机器人看板", 1700000001000, pb.ContentTypeTextOnly_TEXT_text),
		boardMsg("300", "公告机器人看板", 1700000002000, pb.ContentTypeTextOnly_TEXT_text),
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	if got := len(room.BoardEvents); got != 3 {
		t.Fatalf("projected %d boards, want 3", got)
	}
	pinned := pinnedOf(room)
	if len(pinned) != 3 {
		t.Fatalf("pinned = %v, want one entry per bot", pinned)
	}
	for _, id := range []string{"100", "200", "300"} {
		ev := room.BoardEventByID(id)
		if ev == nil {
			t.Fatalf("board of bot %s is missing", id)
		}
		want := s.mxUserID(id, model.ChatTypeBot)
		if ev.Sender != want {
			t.Fatalf("board %s sender = %s, want %s", id, ev.Sender, want)
		}
	}
	// Every pin must point at a live board event, or the panel shows a gap.
	for _, id := range pinned {
		if room.FindEvent(id) == nil {
			t.Fatalf("pinned event %s does not resolve", id)
		}
	}
}

// TestBoardsRemovedUpstreamUnpin: a bot that loses its board must lose its pin,
// otherwise the pin panel keeps a dead entry forever.
func TestBoardsRemovedUpstreamUnpin(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoards(acc, "20001", model.ChatTypeGroup, []*pb.BotBoardMessage{
		boardMsg("100", "A", 1700000000000, pb.ContentTypeTextOnly_TEXT_text),
		boardMsg("200", "B", 1700000001000, pb.ContentTypeTextOnly_TEXT_text),
	})

	// The second bot dropped its board.
	s.applyBoards(acc, "20001", model.ChatTypeGroup, []*pb.BotBoardMessage{
		boardMsg("100", "A", 1700000000000, pb.ContentTypeTextOnly_TEXT_text),
	})

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if room.BoardEventByID("200") != nil {
		t.Fatal("a board that vanished upstream is still projected")
	}
	pinned := pinnedOf(room)
	if len(pinned) != 1 || pinned[0] != room.BoardEventByID("100").EventID {
		t.Fatalf("pinned = %v, want only the surviving board", pinned)
	}
}

// TestRefreshRoomBoardFetchesUpstream covers the lazy path: opening a room pulls
// the boards from /v1/bot/board.
func TestRefreshRoomBoardFetchesUpstream(t *testing.T) {
	f := &fakeBoard{boards: []*pb.BotBoardMessage{
		boardMsg("100", "来自上游的看板", 1700000000000, pb.ContentTypeTextOnly_TEXT_text),
		boardMsg("200", "第二块看板", 1700000001000, pb.ContentTypeTextOnly_TEXT_text),
	}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.refreshRoomBoard(context.Background(), acc, nil, room.RoomID)

	if n := atomic.LoadInt32(&f.calls); n != 1 {
		t.Fatalf("upstream calls = %d, want 1", n)
	}
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if got := len(room.BoardEvents); got != 2 {
		t.Fatalf("projected %d boards, want 2 (one call returns them all)", got)
	}
	if got := asString(room.BoardEventByID("100").Content["body"]); got != "来自上游的看板" {
		t.Fatalf("body = %q", got)
	}
	if room.BoardFetchedTS == 0 {
		t.Fatal("BoardFetchedTS was not stamped: every pagination request would refetch")
	}
}

// TestBoardPinReachesSlidingSync: clients almost never list
// m.room.pinned_events in required_state, so the type is included unconditionally
// - otherwise the pin panel stays empty on Element X.
func TestBoardPinReachesSlidingSync(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoards(acc, "20001", model.ChatTypeGroup, []*pb.BotBoardMessage{
		boardMsg("100", "看板一", 1700000000000, pb.ContentTypeTextOnly_TEXT_text),
		boardMsg("200", "看板二", 1700000001000, pb.ContentTypeTextOnly_TEXT_text),
	})

	s.Store.Mu.RLock()
	state := s.slidingState(room, []slidingStateReq{{Type: "m.room.name"}}, 0, true, acc.UserID)
	s.Store.Mu.RUnlock()

	var found bool
	for _, ev := range state {
		if ev["type"] == "m.room.pinned_events" {
			found = true
			content := ev["content"].(map[string]any)
			if got := sameStringList(content["pinned"], pinnedOf(room)); !got {
				t.Fatalf("pinned = %v, want both boards", content["pinned"])
			}
		}
	}
	if !found {
		t.Fatalf("m.room.pinned_events was filtered out of required_state: %v", state)
	}
}

// TestBoardEventIsServedByEventEndpoint: the client resolves a pinned event it
// has never seen through /rooms/{id}/event/{eventId}.
func TestBoardEventIsServedByEventEndpoint(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.applyBoard(acc, boardMsg("1", "看板正文", 1700000000000, pb.ContentTypeTextOnly_TEXT_text))

	s.Store.Mu.RLock()
	eventID := room.BoardEventByID("1").EventID
	s.Store.Mu.RUnlock()

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/"+room.RoomID+"/event/"+eventID, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := asString(body["event_id"]); got != eventID {
		t.Fatalf("event_id = %q, want %q", got, eventID)
	}
	if got := asString(body["content"].(map[string]any)["body"]); got != "看板正文" {
		t.Fatalf("body = %q", got)
	}
}
