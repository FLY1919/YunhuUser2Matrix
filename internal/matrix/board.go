package matrix

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// boardRefreshTTL is how long a fetched board is trusted. The websocket pushes
// bot_board_message when it changes, so this only bounds how stale the board can
// be for a client that was offline when the change happened.
const boardRefreshTTL = 5 * time.Minute

// boardBotKey is the map key of a board. One bot owns one board per
// conversation, so the bot id identifies it; the fallback keeps boards that
// arrive without a bot id from overwriting each other.
func boardBotKey(board *pb.BotBoardMessage) string {
	botID := strings.TrimSpace(board.GetBotId())
	if botID == "" {
		return "board"
	}
	return botID
}

// applyBoard projects a bot board pushed over the websocket.
//
// Yunhu's 群机器人看板 is a single announcement a bot maintains per
// conversation (content + content_type + last_update_timestamp). A group can
// host several bots, each with its own board, so a room keeps one projected
// event per bot and pins all of them: that is what a client's "pinned
// messages" panel lists.
func (s *Server) applyBoard(acc *model.Account, board *pb.BotBoardMessage) {
	if board == nil || board.GetChatId() == "" {
		return
	}
	chatID := board.GetChatId()
	chatType := int32(board.GetChatType())
	if chatType == 0 {
		// Older pushes omit it; a board only exists on groups and bots.
		chatType = model.ChatTypeGroup
	}

	s.Store.Mu.Lock()
	room := acc.RoomByChat(chatID, chatType)
	if room == nil {
		room = s.ensureRoomLocked(acc, chatID, chatType, "", "")
	}
	changed := s.applyBoardLocked(acc, room, board)
	s.Store.Mu.Unlock()

	if changed {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
}

// applyBoardLocked stores one bot's board and republishes the pin. The caller
// must hold the store lock. It reports whether anything changed.
func (s *Server) applyBoardLocked(acc *model.Account, room *model.Room, board *pb.BotBoardMessage) bool {
	room.BoardFetchedTS = time.Now().UnixMilli()
	if room.BoardEvents == nil {
		room.BoardEvents = map[string]*model.Event{}
	}
	if room.BoardTS == nil {
		room.BoardTS = map[string]int64{}
	}

	key := boardBotKey(board)
	cur := room.BoardEvents[key]

	body := strings.TrimSpace(board.GetContent())
	if body == "" {
		// This bot's board was cleared while others may still have one: drop
		// only this entry, never the whole set.
		if cur == nil {
			return false
		}
		delete(room.BoardEvents, key)
		delete(room.BoardTS, key)
		return s.setPinnedLocked(acc, room)
	}

	// Same revision we already projected: nothing to do.
	if cur != nil && room.BoardTS[key] == board.GetLastUpdateTimestamp() {
		return false
	}

	ts := board.GetLastUpdateTimestamp()
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	} else if ts < 1e12 { // seconds instead of milliseconds
		ts *= 1000
	}

	botID := board.GetBotId()
	if botID == "" {
		botID = "board"
	}
	// The board is a bot's announcement, so the event must come from the bot's
	// own Matrix user - otherwise the pin panel shows an anonymous note.
	sender := s.mxUserID(botID, model.ChatTypeBot)
	s.putMemberLocked(acc, room, sender, board.GetBotName(), "")

	// The event id is derived from (conversation, bot, revision): a new revision
	// of the same bot's board replaces its pin without touching the others.
	eventID := eventIDFor(fmt.Sprintf("board_%s_%s_%d", room.ChatID, botID, board.GetLastUpdateTimestamp()))
	if cur != nil && cur.EventID == eventID {
		// Same revision, different content object: refresh in place.
		cur.Content = boardContent(board)
		return false
	}

	room.BoardEvents[key] = &model.Event{
		EventID:  eventID,
		Type:     "m.room.message",
		Sender:   sender,
		Content:  boardContent(board),
		OriginTS: ts,
	}
	room.BoardTS[key] = board.GetLastUpdateTimestamp()
	return s.setPinnedLocked(acc, room)
}

// applyBoards projects a whole /v1/bot/board response at once.
//
// Boards that disappeared upstream have to be un-pinned here: applying the list
// entry by entry would leave a removed bot's pin in place forever, because
// nothing ever tells us "this bot no longer has a board".
func (s *Server) applyBoards(acc *model.Account, chatID string, chatType int32, boards []*pb.BotBoardMessage) {
	if chatID == "" {
		return
	}
	if chatType == 0 {
		chatType = model.ChatTypeGroup
	}

	s.Store.Mu.Lock()
	room := acc.RoomByChat(chatID, chatType)
	if room == nil {
		room = s.ensureRoomLocked(acc, chatID, chatType, "", "")
	}

	seen := make(map[string]struct{}, len(boards))
	changed := false
	for _, b := range boards {
		if b == nil {
			continue
		}
		// The list endpoint does not always echo the chat back; fill it in so a
		// board is filed under the right conversation.
		if b.ChatId == "" {
			b.ChatId = chatID
		}
		if b.ChatType == pb.ChatType_unknown1 {
			b.ChatType = pb.ChatType(chatType)
		}
		seen[boardBotKey(b)] = struct{}{}
		if s.applyBoardLocked(acc, room, b) {
			changed = true
		}
	}

	// Anything we still pin but the upstream list no longer mentions is gone.
	var dropped bool
	for key := range room.BoardEvents {
		if _, ok := seen[key]; ok {
			continue
		}
		delete(room.BoardEvents, key)
		delete(room.BoardTS, key)
		dropped = true
	}
	if dropped && s.setPinnedLocked(acc, room) {
		changed = true
	}
	// An empty list with nothing pinned is still a successful fetch: stamp the
	// TTL so an idle group is not refetched on every pagination request.
	room.BoardFetchedTS = time.Now().UnixMilli()
	s.Store.Mu.Unlock()

	if changed {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
}

// boardContent renders the board body as Matrix message content, honouring the
// board's own text/markdown/html flavour.
func boardContent(board *pb.BotBoardMessage) map[string]any {
	text := board.GetContent()
	switch board.GetContentType() {
	case pb.ContentTypeTextOnly_TEXT_markdown:
		return map[string]any{
			"msgtype":        "m.text",
			"body":           text,
			"format":         "org.matrix.custom.html",
			"formatted_body": mdToHTML(text),
		}
	case pb.ContentTypeTextOnly_TEXT_html:
		return map[string]any{
			"msgtype":        "m.text",
			"body":           stripHTML(text),
			"format":         "org.matrix.custom.html",
			"formatted_body": text,
		}
	}
	return map[string]any{"msgtype": "m.text", "body": text}
}

// setPinnedLocked republishes m.room.pinned_events to match the boards currently
// projected. It reports whether the state event actually changed, so repeated
// refreshes are silent.
func (s *Server) setPinnedLocked(acc *model.Account, room *model.Room) bool {
	// One pin per bot, in a stable order, so the list does not reshuffle.
	boards := room.BoardEventList()
	pinned := make([]string, 0, len(boards))
	for _, ev := range boards {
		pinned = append(pinned, ev.EventID)
	}
	sort.SliceStable(pinned, func(i, j int) bool { return pinned[i] < pinned[j] })

	cur := room.State[model.StateID("m.room.pinned_events", "")]
	if cur != nil && sameStringList(cur.Content["pinned"], pinned) {
		return false
	}

	key := ""
	pos := acc.NextPos()
	room.AppendEvent(&model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     "m.room.pinned_events",
		Sender:   acc.UserID,
		StateKey: &key,
		Content:  map[string]any{"pinned": pinned},
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}, 0)
	return true
}

// sameStringList compares a stored JSON list with a freshly built one.
func sameStringList(raw any, want []string) bool {
	switch got := raw.(type) {
	case []string:
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	case []any:
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if asString(got[i]) != want[i] {
				return false
			}
		}
		return true
	}
	return false
}

// boardStaleLocked reports whether the boards should be refetched.
func (s *Server) boardStaleLocked(room *model.Room) bool {
	if room.BoardFetchedTS == 0 {
		return true
	}
	return time.Since(time.UnixMilli(room.BoardFetchedTS)) > boardRefreshTTL
}

// refreshRoomBoard asks Yunhu for the conversation's boards and projects them.
//
// Called lazily when a client opens a room: fetching boards for every group on
// connect would double the request count for rooms nobody looks at.
func (s *Server) refreshRoomBoard(ctx context.Context, acc *model.Account, dev *model.Device, roomID string) {
	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		return
	}
	chatID, chatType := room.ChatID, room.ChatType
	s.Store.Mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := s.yunhuFor(acc, dev).Board(ctx, chatID, pb.ChatType(chatType))
	if err != nil {
		// Stamp the attempt so a failing group is not retried on every
		// pagination request; the next TTL window tries again.
		s.Store.Mu.Lock()
		if r := acc.Rooms[roomID]; r != nil {
			r.BoardFetchedTS = time.Now().UnixMilli()
		}
		s.Store.Mu.Unlock()
		s.Log.Debug("bot board fetch failed", "room", roomID, "err", err)
		return
	}

	boards := resp.GetBoard()
	s.Log.Debug("bot boards fetched", "room", roomID, "count", len(boards))
	s.applyBoards(acc, chatID, chatType, boards)
}
