package matrix

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// joinedRooms lists the rooms the account is currently in.
func (s *Server) joinedRooms(c *Ctx) {
	s.Store.Mu.RLock()
	rooms := make([]string, 0, len(c.Account.Rooms))
	for id, r := range c.Account.Rooms {
		if r.Membership == model.MembershipJoin {
			rooms = append(rooms, id)
		}
	}
	s.Store.Mu.RUnlock()
	sort.Strings(rooms)
	writeJSON(c.W, http.StatusOK, map[string]any{"joined_rooms": rooms})
}

// sendEvent handles PUT /rooms/{roomId}/send/{eventType}/{txnId}.
func (s *Server) sendEvent(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	evType := c.Param("eventType")
	txnID := c.Param("txnId")

	var content map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 2<<20)).Decode(&content); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Request body is not valid JSON")
		return
	}
	if content == nil {
		content = map[string]any{}
	}

	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	var cached string
	if txnID != "" {
		cached = acc.Txns[txnID]
	}
	var chatID string
	var chatType int32
	if room != nil {
		chatID, chatType = room.ChatID, room.ChatType
	}
	s.Store.Mu.RUnlock()

	if cached != "" {
		writeJSON(c.W, http.StatusOK, map[string]any{"event_id": cached})
		return
	}
	if room == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}

	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	yc := s.yunhuFor(acc, c.Device)

	if evType == "m.room.redaction" {
		s.redactEvent(c, acc, room, content)
		return
	}

	msgID := randomHex(16)
	// The Yunhu message id of the note that accompanies a file, when one was
	// sent. It is projected as its own timeline event so the split is visible on
	// this side too: a client that only saw the file would otherwise lose the
	// text it typed.
	sentNoteMsgID := ""
	// A client edit (m.replace) is forwarded as an in-place Yunhu edit.
	editTarget := ""
	isReplace := false
	if rel := asMap(content["m.relates_to"]); rel != nil && asString(rel["rel_type"]) == "m.replace" {
		isReplace = true
		editTarget = yunhuMsgIDFromEvent(asString(rel["event_id"]))
	}
	// isReplace, not "editTarget parsed successfully": an m.replace whose target
	// id does not resolve is still an edit. Treating it as a normal message sent
	// the m.replace envelope upstream as a chat message, which is worse than
	// rejecting it.
	isEdit := evType == "m.room.message" && isReplace

	// The text a client is actually asserting, for both the upstream call and
	// the local echo.
	newContent := content
	if isEdit {
		if nc := asMap(content["m.new_content"]); nc != nil {
			newContent = nc
		}
		// Resolve the target before touching upstream: an edit for an event this
		// account does not have cannot be forwarded anywhere (there is no Yunhu
		// message id to name), and sending the edit anyway just produces an
		// opaque upstream error.
		s.Store.Mu.RLock()
		var target *model.Event
		if editTarget != "" {
			target = room.EventByYunhuID(editTarget)
		}
		if target == nil {
			target = room.FindEvent(asString(asMap(content["m.relates_to"])["event_id"]))
		}
		s.Store.Mu.RUnlock()
		if target == nil || target.YunhuMsgID == "" {
			writeError(c.W, http.StatusNotFound, ErrNotFound, "Edited event not found")
			return
		}
		editTarget = target.YunhuMsgID
	}

	if isEdit {
		req, err := s.buildSendRequest(ctx, yc, chatID, chatType, evType, newContent, editTarget,
			mentionNameResolver(acc, room))
		if err != nil {
			writeError(c.W, http.StatusBadRequest, ErrUnknown, err.Error())
			return
		}
		if err := yc.EditMessage(ctx, req); err != nil {
			s.Log.Warn("edit message failed", "room", roomID, "err", err)
			writeError(c.W, http.StatusBadGateway, ErrUnknown, "云湖编辑失败: "+err.Error())
			return
		}
	} else if evType == "m.room.message" {
		req, err := s.buildSendRequest(ctx, yc, chatID, chatType, evType, content, msgID,
			mentionNameResolver(acc, room))
		if err != nil {
			writeError(c.W, http.StatusBadRequest, ErrUnknown, err.Error())
			return
		}
		if err := yc.SendMessage(ctx, req); err != nil {
			s.Log.Warn("send message failed", "room", roomID, "err", err)
			writeError(c.W, http.StatusBadGateway, ErrUnknown, "云湖发送失败: "+err.Error())
			return
		}

		// A media message with a caption becomes two Yunhu messages: the media
		// first, then the caption as a normal text message. Yunhu's media
		// messages have no caption field, and dropping the caption is worse than
		// splitting it - the whole point of typing a description next to an
		// attachment is that the other side reads it.
		//
		// The order is deliberate: the media lands first so the caption reads as
		// the comment on what was just sent, which is how people actually post
		// "here is the build" + "changelog: …". The caption is sent even if it
		// fails to attach, because by then the media is already gone upstream and
		// the client is entitled to know the second half did not make it.
		if note := carryNote(content); note != "" {
			noteMsgID := randomHex(16)
			noteReq, err := s.buildSendRequest(ctx, yc, chatID, chatType, evType,
				map[string]any{"msgtype": "m.text", "body": note}, noteMsgID,
				mentionNameResolver(acc, room))
			if err != nil {
				s.Log.Warn("build media note failed", "room", roomID, "err", err)
			} else if err := yc.SendMessage(ctx, noteReq); err != nil {
				s.Log.Warn("send media note failed", "room", roomID, "err", err)
			} else {
				sentNoteMsgID = noteMsgID
			}
		}
	} else {
		// Non-message events are stored locally only.
		msgID = ""
	}

	s.Store.Mu.Lock()
	room = acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}

	// An edit never creates a timeline entry of its own: it updates the target's
	// content in place and stamps EditedTS, which is what makes the client draw
	// one bubble with an "edited" marker. Appending the m.replace event (as this
	// used to) produced a second message reading "* new text" and left the
	// original showing the old text.
	if isEdit {
		target := room.EventByYunhuID(editTarget)
		if target == nil {
			s.Store.Mu.Unlock()
			writeError(c.W, http.StatusNotFound, ErrNotFound, "Edited event not found")
			return
		}
		target.Content = newContent
		target.EditedTS = time.Now().UnixMilli()
		target.Pos = acc.NextPos()
		eventID := target.EditEventID()
		if txnID != "" {
			acc.Txns[txnID] = eventID
			acc.TrimTxns(2000)
		}
		s.Store.Mu.Unlock()

		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		writeJSON(c.W, http.StatusOK, map[string]any{"event_id": eventID})
		return
	}

	var eventID string
	if msgID != "" {
		if prior := room.EventByYunhuID(msgID); prior != nil {
			// The websocket echo beat us to it.
			eventID = prior.EventID
		}
	}
	if eventID == "" {
		key := msgID
		if key == "" {
			key = "local" + randomHex(12)
		}
		ev := &model.Event{
			EventID:    eventIDFor(key),
			Type:       evType,
			Sender:     acc.UserID,
			Content:    content,
			OriginTS:   time.Now().UnixMilli(),
			Pos:        acc.NextPos(),
			YunhuMsgID: msgID,
		}
		room.AppendEvent(ev, s.Cfg.Storage.MaxEventsPerRoom)
		eventID = ev.EventID
	}

	// The note that accompanied the file is a message of its own upstream, so it
	// gets its own event here. Without it the sender's own timeline would show
	// the attachment with no explanation - the text only exists on the other
	// side.
	if sentNoteMsgID != "" && room.EventByYunhuID(sentNoteMsgID) == nil {
		room.AppendEvent(&model.Event{
			EventID:    eventIDFor(sentNoteMsgID),
			Type:       evType,
			Sender:     acc.UserID,
			Content:    map[string]any{"msgtype": "m.text", "body": carryNote(content)},
			OriginTS:   time.Now().UnixMilli(),
			Pos:        acc.NextPos(),
			YunhuMsgID: sentNoteMsgID,
		}, s.Cfg.Storage.MaxEventsPerRoom)
	}
	if txnID != "" {
		acc.Txns[txnID] = eventID
		acc.TrimTxns(2000)
	}
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{"event_id": eventID})
}

// messages implements history pagination.
//
// The local timeline is a bounded window: yunhu.backfill messages hydrated on
// first contact, then whatever arrived over the websocket, capped by
// storage.max_events_per_room. Paginating only inside that window means the
// client hits the end of the slice after one or two pages and Element simply
// stops offering "load more" — the scrollback looks like it disappeared.
//
// So backwards pagination continues upstream: once the local window is
// exhausted we ask Yunhu for the messages before our oldest known message and
// splice them into the same chunk. Those pages are not persisted (they are old
// history, not news, and letting them into room.Events would make /sync
// re-deliver them as fresh messages and fight the retention cap), which is why
// the continuation token carries an upstream message ID instead of a stream
// position. Tokens are opaque to clients, so mixing the two families is safe.
func (s *Server) messages(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	from := c.Query("from")
	dir := orDefault(c.Query("dir"), "b")
	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 200 {
		limit = 200
	}

	// Snapshot under the lock and release it before any network call: /messages
	// must never hold the account lock across an upstream round trip.
	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}

	// /messages carries timeline events only. State events (create, power_levels,
	// join_rules, member, ...) are bootstrap data delivered via /sync and
	// /state; putting them in a pagination chunk makes clients render them as
	// broken messages and, worse, lets them consume the page limit.
	timeline := make([]*model.Event, 0, len(room.Events))
	known := make(map[string]struct{}, len(room.Events))
	for _, ev := range room.Events {
		if ev.IsState() {
			continue
		}
		timeline = append(timeline, ev)
		known[ev.EventID] = struct{}{}
	}
	// A client asking to "jump to the first unread message" paginates forward
	// from the read marker, and the value it has is the event id from
	// m.fully_read - not a stream position. Resolve it before releasing the
	// lock, or the token would parse as 0 and the page would come back from the
	// very beginning.
	if from != "" && dir == "f" {
		if pos, ok := roomPosOf(room, from); ok {
			from = "s" + strconv.FormatInt(pos, 10)
		}
	}

	canBackfill := room.ChatID != "" && acc.Token != ""
	// Opening a room is the natural moment to sync its bot board: that is what
	// the "pinned messages" panel shows, and fetching boards for every group on
	// connect would double the request count for rooms nobody opens.
	needBoard := room.ChatType == model.ChatTypeGroup && acc.Token != "" && s.boardStaleLocked(room)
	s.Store.Mu.RUnlock()

	if needBoard && dir == "b" {
		// Detached from the request context: the response may finish before the
		// upstream round trip does, and the result is delivered over /sync.
		s.safego("board-refresh", func() {
			s.refreshRoomBoard(context.WithoutCancel(c.R.Context()), acc, c.Device, roomID)
		})
	}

	// A token minted by a previous upstream page resumes there directly; there
	// is nothing local left to scan.
	upstreamFrom, isUpstream := upstreamMsgID(from)
	isUpstream = isUpstream && dir == "b"

	// window is chronological (oldest first) in every branch.
	var window []*model.Event
	moreLocal := false
	if !isUpstream {
		switch {
		case from == "" || from == "end":
			start := len(timeline) - limit
			if start < 0 {
				start = 0
			}
			// start == 0 means the page reached the first event: nothing local
			// is left, so moreLocal must stay false or the chain stops one page
			// short of Yunhu's older history.
			moreLocal = start > 0
			window = append(window, timeline[start:]...)
		case dir == "f":
			// Forward pagination: events strictly after the token.
			pos := parseSince(from)
			for _, ev := range timeline {
				if ev.Pos <= pos {
					continue
				}
				if len(window) >= limit {
					moreLocal = true
					break
				}
				window = append(window, ev)
			}
		default:
			// Backward pagination: the page ends at the token, newest first.
			pos := parseSince(from)
			end := -1
			for i := len(timeline) - 1; i >= 0; i-- {
				if timeline[i].Pos <= pos {
					end = i
					break
				}
			}
			if end >= 0 {
				start := end - limit + 1
				if start < 0 {
					start = 0
				}
				moreLocal = start > 0
				window = append(window, timeline[start:end+1]...)
			}
		}
	}

	// Top up from Yunhu when the local window ran out.
	var upstreamPage []*model.Event
	upstreamOldest := ""
	if dir == "b" && canBackfill {
		need := 0
		anchor := upstreamFrom
		switch {
		case isUpstream:
			need = limit
		case len(window) < limit:
			need = limit - len(window)
			if anchor == "" {
				anchor = oldestYunhuMsgID(window)
			}
			if anchor == "" {
				// Nothing local at all: ask for the newest page.
				anchor = oldestYunhuMsgID(timeline)
			}
		case !moreLocal:
			// The page happens to be exactly the whole local timeline (a small
			// room, or limit == len(timeline)). Nothing to append, but the chain
			// must not stop here either: hand out an upstream token so the next
			// page continues past the oldest local message.
			if anchor == "" {
				anchor = oldestYunhuMsgID(window)
			}
			upstreamOldest = anchor
		}
		if need > 0 {
			page, err := s.upstreamHistory(c.R.Context(), acc, room, anchor, need)
			if err != nil {
				s.Log.Debug("history backfill failed", "room", roomID, "anchor", anchor, "err", err)
			} else {
				// The continuation token follows the API's own oldest message,
				// not the first one we actually kept: if a page happens to be
				// entirely duplicates the chain still has to advance.
				if len(page) > 0 {
					upstreamOldest = page[0].YunhuMsgID
				}
				for _, ev := range page {
					if _, dup := known[ev.EventID]; dup {
						continue
					}
					known[ev.EventID] = struct{}{}
					upstreamPage = append(upstreamPage, ev)
				}
				s.Log.Debug("history backfill", "room", roomID,
					"anchor", anchor, "want", need,
					"got", len(page), "kept", len(upstreamPage))
			}
		}
	}

	// Chronological order: upstream page (oldest) followed by the local window.
	selected := make([]*model.Event, 0, len(upstreamPage)+len(window))
	selected = append(selected, upstreamPage...)
	selected = append(selected, window...)

	chunk := make([]map[string]any, 0, len(selected))
	minPos, maxPos := int64(0), int64(0)
	for _, ev := range selected {
		chunk = append(chunk, s.eventJSON(ev, ""))
		if ev.Pos <= 0 {
			continue // upstream events carry no stream position
		}
		if minPos == 0 || ev.Pos < minPos {
			minPos = ev.Pos
		}
		if ev.Pos > maxPos {
			maxPos = ev.Pos
		}
	}
	if dir == "b" {
		// Reverse chronological for backwards pagination, as the spec requires.
		for i, j := 0, len(chunk)-1; i < j; i, j = i+1, j-1 {
			chunk[i], chunk[j] = chunk[j], chunk[i]
		}
	}

	// start is mandatory and must never be empty, even for an empty page.
	startTok := from
	if startTok == "" || startTok == "end" {
		if maxPos > 0 {
			startTok = "s" + strconv.FormatInt(maxPos, 10)
		} else {
			startTok = "s0"
		}
	}

	resp := map[string]any{"chunk": chunk, "start": startTok}
	// end is omitted when there is nothing further, which is how clients learn
	// to stop paginating. An empty string is not a valid token.
	endTok := ""
	switch {
	case upstreamOldest != "":
		endTok = upstreamToken(upstreamOldest)
	case dir == "f":
		if moreLocal && maxPos > 0 {
			endTok = "s" + strconv.FormatInt(maxPos, 10)
		}
	case moreLocal && minPos > 1:
		endTok = "s" + strconv.FormatInt(minPos-1, 10)
	}
	// Never hand back the token we were given: the client would loop forever.
	if endTok != "" && endTok != from {
		resp["end"] = endTok
	}

	writeJSON(c.W, http.StatusOK, resp)
}

// roomPosOf resolves a pagination token to a stream position.
//
// Clients hand back whatever they were given, which is one of:
//
//   - "s123"                 a stream position we minted
//   - an event id            what m.fully_read carries, and what a client uses
//                            as the anchor when jumping to the first unread
//   - "y<base64>"            an upstream continuation token (handled elsewhere)
//
// Only the first two are resolvable locally; everything else reports false so
// the caller leaves the token alone.
func roomPosOf(room *model.Room, token string) (int64, bool) {
	if strings.HasPrefix(token, "s") {
		if pos := parseSince(token); pos > 0 {
			return pos, true
		}
		return 0, false
	}
	if ev := room.FindEvent(token); ev != nil && ev.Pos > 0 {
		return ev.Pos, true
	}
	return 0, false
}

// upstreamTokenPrefix marks a pagination token that points into Yunhu rather
// than into the local timeline. Clients treat tokens as opaque, so the format
// is ours; encoding the message ID keeps a scrollback chain resumable without
// inventing stream positions for events we never stored.
const upstreamTokenPrefix = "y"

func upstreamToken(msgID string) string {
	if msgID == "" {
		return ""
	}
	return upstreamTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(msgID))
}

// upstreamMsgID reverses upstreamToken. The second result reports whether the
// token was an upstream one at all, so a local "s123" is never mistaken for it.
func upstreamMsgID(tok string) (string, bool) {
	if !strings.HasPrefix(tok, upstreamTokenPrefix) {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(tok, upstreamTokenPrefix))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// oldestYunhuMsgID finds the oldest upstream message we can anchor a request on.
// Edits and recalls carry synthetic keys ("<id>_edit", "<id>_recall<ts>") that
// Yunhu would not recognise, so they are skipped.
func oldestYunhuMsgID(evs []*model.Event) string {
	for _, ev := range evs {
		id := ev.YunhuMsgID
		if id == "" || strings.HasSuffix(id, "_edit") || strings.Contains(id, "_recall") {
			continue
		}
		return id
	}
	return ""
}

// upstreamHistory pulls one page of older messages straight from Yunhu.
func (s *Server) upstreamHistory(ctx context.Context, acc *model.Account, room *model.Room, before string, count int) ([]*model.Event, error) {
	if count <= 0 {
		count = 10
	}
	if count > 100 {
		count = 100
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	yc := s.YunhuClient().WithToken(acc.Token)
	resp, err := yc.ListMessages(ctx, room.ChatID, pb.ChatType(room.ChatType), before, int32(count))
	if err != nil {
		return nil, err
	}

	data := resp.GetData()
	sortMsgsChronological(data)

	out := make([]*model.Event, 0, len(data))
	for _, md := range data {
		if md.GetMsgId() == "" {
			continue
		}
		pm := &pb.PushMessage{
			MsgId:       md.GetMsgId(),
			Sender:      md.GetSender(),
			ChatId:      room.ChatID,
			ChatType:    pb.ChatType(room.ChatType),
			Content:     md.GetContent(),
			ContentType: md.GetContentType(),
			Timestamp:   md.GetSendTimestampMs(),
			QuoteMsgId:  md.GetQuoteMsgId(),
			MsgSeq:      md.GetMsgSeq(),
			// See backfillRoom: recall_timestamp marks a message that was
			// already recalled upstream.
			DeleteTimestamp: md.GetRecallTimestamp(),
		}
		out = append(out, s.yunhuToEvent(room, "push_message", pm, 0))
	}
	return out, nil
}

// sortMsgsChronological orders a Yunhu message list oldest-first. The API does
// not document its ordering, and getting it wrong inverts the timeline (and the
// stream positions we derive from it), so sort on the payload instead.
func sortMsgsChronological(data []*pb.ListMsgResponse_MsgData) {
	sort.SliceStable(data, func(i, j int) bool {
		ti, tj := data[i].GetSendTimestampMs(), data[j].GetSendTimestampMs()
		if ti != tj && ti > 0 && tj > 0 {
			return ti < tj
		}
		si, sj := data[i].GetMsgSeq(), data[j].GetMsgSeq()
		if si != sj && si > 0 && sj > 0 {
			return si < sj
		}
		return false
	})
}

// getEvent returns a single event.
func (s *Server) getEvent(c *Ctx) {
	eventID := c.Param("eventId")
	s.Store.Mu.RLock()
	room := c.Account.Rooms[c.Param("roomId")]
	var ev *model.Event
	var editOf *model.Event
	if room != nil {
		ev = room.FindEvent(eventID)
		if ev == nil {
			// An edit event id was derived from the original, so a client that
			// learned about a replacement through unsigned.m.relations can ask
			// for it directly.
			if orig := room.EventByEditID(eventID); orig != nil {
				editOf = orig
			}
		}
	}
	s.Store.Mu.RUnlock()

	if editOf != nil {
		// Synthesised on demand: the m.replace event is never stored.
		writeJSON(c.W, http.StatusOK, s.editRelationJSON(editOf))
		return
	}
	if ev == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Event not found")
		return
	}
	writeJSON(c.W, http.StatusOK, s.eventJSON(ev, room.RoomID))
}

// eventContext implements GET /rooms/{roomId}/context/{eventId}.
//
// This is how a client renders an event it has never seen in a timeline: the
// "pinned messages" panel asks for the context of a pinned event, and the
// thread/permalink views do the same. Without the route the request fell
// through to the catch-all and the client got M_UNRECOGNIZED, which is exactly
// what a pinned Yunhu bot board produced:
//
//	GET /_matrix/client/v3/rooms/!yh-group-…/context/$yh_board_… → M_UNRECOGNIZED
//
// The board events are also the tricky case: they live in Room.BoardEvents,
// never in Room.Events, and they are only fetched lazily when a room is opened.
// A client that opens the pin panel first races that refresh, so the boards are
// pulled here synchronously when the target is missing and the TTL allows it.
func (s *Server) eventContext(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	eventID := c.Param("eventId")

	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 100 {
		limit = 100
	}

	// Snapshot under the lock and release it before any upstream call:
	// /context must never hold the account lock across a network round trip.
	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	target := room.FindEvent(eventID)
	needBoard := target == nil && room.ChatType == model.ChatTypeGroup &&
		acc.Token != "" && s.boardStaleLocked(room)
	s.Store.Mu.RUnlock()

	if needBoard {
		s.refreshRoomBoard(c.R.Context(), acc, c.Device, roomID)
		s.Store.Mu.RLock()
		if room = acc.Rooms[roomID]; room != nil {
			target = room.FindEvent(eventID)
		}
		s.Store.Mu.RUnlock()
	}

	if room == nil || target == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Event not found")
		return
	}

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	// Timeline events only, exactly like /messages: state events are bootstrap
	// data and mixing them in would make clients render them as messages.
	timeline := make([]*model.Event, 0, len(room.Events))
	for _, ev := range room.Events {
		if ev.IsState() {
			continue
		}
		timeline = append(timeline, ev)
	}
	idx := -1
	for i, ev := range timeline {
		if ev.EventID == target.EventID {
			idx = i
			break
		}
	}

	before := make([]map[string]any, 0, limit)
	after := make([]map[string]any, 0, limit)
	if idx >= 0 {
		start := idx - limit
		if start < 0 {
			start = 0
		}
		// events_before is reverse-chronological per the spec: the neighbour of
		// the target comes first, exactly like /messages?dir=b.
		for i := idx - 1; i >= start; i-- {
			before = append(before, s.eventJSON(timeline[i], ""))
		}
		end := idx + 1 + limit
		if end > len(timeline) {
			end = len(timeline)
		}
		for _, ev := range timeline[idx+1 : end] {
			after = append(after, s.eventJSON(ev, ""))
		}
	}

	// The state at the target: clients need membership and power levels to
	// render a pinned item they never saw in the timeline.
	state := make([]map[string]any, 0, len(room.State))
	for _, ev := range room.State {
		state = append(state, s.eventJSON(ev, ""))
	}

	startTok := "s0"
	if target.Pos > 0 {
		startTok = "s" + strconv.FormatInt(target.Pos, 10)
	}
	resp := map[string]any{
		"start":         startTok,
		"events_before": before,
		"event":         s.eventJSON(target, ""),
		"events_after":  after,
		"state":         state,
	}
	// end is omitted when nothing follows: that is how a client learns that
	// paginating forwards would yield nothing.
	if idx >= 0 && idx+1+limit < len(timeline) {
		resp["end"] = "s" + strconv.FormatInt(timeline[idx+limit].Pos, 10)
	}
	writeJSON(c.W, http.StatusOK, resp)
}

// getState returns either the whole room state or a single state event.
func (s *Server) getState(c *Ctx) {
	evType := c.Param("eventType")
	stateKey := c.Param("stateKey")

	s.Store.Mu.RLock()
	room := c.Account.Rooms[c.Param("roomId")]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	if evType == "" {
		out := make([]map[string]any, 0, len(room.State))
		for _, ev := range room.State {
			out = append(out, s.eventJSON(ev, ""))
		}
		s.Store.Mu.RUnlock()
		writeJSON(c.W, http.StatusOK, out)
		return
	}
	ev := room.State[model.StateID(evType, stateKey)]
	var content map[string]any
	if ev != nil {
		content = ev.Content
	}
	s.Store.Mu.RUnlock()
	if ev == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "State event not found")
		return
	}
	writeJSON(c.W, http.StatusOK, content)
}

// powerLevelsFromContent turns a client's power_levels body into user -> level.
//
// Matrix levels are 100/50/0 for owner/admin/member; Yunhu uses the same numbers
// (permission_level 100/2/0), so the mapping is not a scale but a translation of
// the two middle values - handled where the comparison happens.
func powerLevelsFromContent(content map[string]any) map[string]int64 {
	users, ok := content["users"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]int64, len(users))
	for uid, raw := range users {
		switch v := raw.(type) {
		case float64:
			out[uid] = int64(v)
		case int64:
			out[uid] = v
		case int:
			out[uid] = int64(v)
		}
	}
	return out
}

// syncGroupRolesFromPowerLevels pushes the admin/owner changes a client asked for.
//
// The comparison uses Yunhu's own numbers:
//
//	Matrix 100 == Yunhu 100  (群主 - cannot be granted, only observed)
//	Matrix  50 == Yunhu   2  (管理员)
//	Matrix   0 == Yunhu   0  (普通成员)
//
// Three deliberate choices:
//
//  1. The current state comes from group/info, NOT from the member roster. The
//     roster is paginated at 100 per page, so a big group's admins can sit past
//     the first page - and diffing against a truncated list would demote every
//     admin it did not happen to return.
//  2. The owner is never touched: Yunhu rejects changing 群主, and a client that
//     omits the owner from `users` must not be read as "demote the owner".
//  3. Only actual differences are sent. A client re-submits the whole map on
//     almost every state write (Element does it when you open the panel), so
//     posting every entry would hammer the API with no changes.
func (s *Server) syncGroupRolesFromPowerLevels(ctx context.Context, acc *model.Account, groupID string, wanted map[string]int64) {
	yc := s.yunhuFor(acc, nil)

	info, err := yc.GroupInfo(ctx, groupID)
	if err != nil {
		s.Log.Warn("group role sync needs group/info", "group", groupID, "err", err)
		return
	}
	data := info.GetData()
	if data == nil {
		return
	}

	ownerID := orDefault(data.GetOwner(), data.GetCreateBy())
	current := map[string]int32{}
	if ownerID != "" {
		current[ownerID] = 100
	}
	for _, id := range data.GetAdmin() {
		if id != "" {
			current[id] = 2
		}
	}

	// Walk only the users the client mentioned: an id absent from the request was
	// not part of this edit, and treating "absent" as "demote" would wipe every
	// admin the client's list happened not to include.
	for userID, level := range wanted {
		yunhuID, chatType, ok := s.yunhuIDFromUser(userID)
		if !ok || chatType == model.ChatTypeGroup || yunhuID == "" {
			continue
		}
		if yunhuID == ownerID {
			// The owner's level is not ours to change.
			continue
		}

		var want int32
		switch {
		case level >= 100:
			// Promote to owner is not expressible upstream; skip rather than
			// silently demoting through the admin path.
			continue
		case level >= 50:
			want = 2
		default:
			want = 0
		}

		have := current[yunhuID]
		if have == 100 {
			continue // also the owner, reached by another id form
		}
		if have == want {
			continue
		}
		if err := yc.SetGroupMemberLevel(ctx, groupID, yunhuID, want); err != nil {
			s.Log.Warn("set group member level failed",
				"group", groupID, "user", yunhuID, "level", want, "err", err)
			continue
		}
		s.Log.Info("group role changed", "group", groupID, "user", yunhuID, "level", want)
	}
}

// putState updates a room state event and mirrors supported changes upstream.
func (s *Server) putState(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	evType := c.Param("eventType")
	stateKey := c.Param("stateKey")

	var content map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 1<<20)).Decode(&content); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Request body is not valid JSON")
		return
	}

	s.Store.Mu.Lock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	key := stateKey
	pos := acc.NextPos()
	ev := &model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     evType,
		Sender:   acc.UserID,
		StateKey: &key,
		Content:  content,
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}
	room.AppendEvent(ev, 0)

	switch evType {
	case "m.room.name":
		room.Name = asString(content["name"])
	case "m.room.topic":
		room.Topic = asString(content["topic"])
	case "m.room.avatar":
		room.AvatarURL = asString(content["url"])
	}
	chatID, chatType := room.ChatID, room.ChatType
	var wantedLevels map[string]int64
	if evType == "m.room.power_levels" && chatType == model.ChatTypeGroup {
		wantedLevels = powerLevelsFromContent(content)
	}
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)

	// A power_levels write is how a client promotes or demotes a member: it sends
	// the whole map, with 100 = owner, 50 = admin, 0 = ordinary. Mirroring that
	// means diffing it against what Yunhu currently thinks and only touching the
	// entries that actually changed - see syncGroupRolesFromPowerLevels.
	if wantedLevels != nil {
		s.safego("group-roles", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			s.syncGroupRolesFromPowerLevels(ctx, acc, chatID, wantedLevels)
		})
	}

	// Mirror name/topic/avatar changes to group chats upstream.
	//
	// The topic is the interesting one: Matrix calls it "topic" (what clients
	// label 简介 / description) and Yunhu calls the same field introduction, so
	// it maps 1:1. The avatar does not: Matrix stores an mxc:// URI and Yunhu a
	// URL, so the bytes have to be re-hosted first.
	//
	// Every branch reads the group first and sends the *whole* current record with
	// just the changed field replaced. EditGroupRequest is a full-state write, not
	// a patch: a zero-valued field means "set it to empty". Sending only the
	// introduction silently wiped the group's name, and sending only the name
	// wiped its description - which is what actually happened when a user edited
	// one of them.
	if chatType == model.ChatTypeGroup {
		switch evType {
		case "m.room.name", "m.room.topic", "m.room.avatar":
			s.safego("group-edit", func() {
				ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.Timeout())
				defer cancel()
				yc := s.yunhuFor(acc, c.Device)

				req := &pb.EditGroupRequest{GroupId: chatID}
				// Seed from upstream so untouched fields keep their value.
				if info, err := yc.GroupInfo(ctx, chatID); err != nil {
					s.Log.Warn("group edit needs the current group, fetch failed",
						"room", roomID, "err", err)
					return
				} else if data := info.GetData(); data != nil {
					req.Name = data.GetName()
					req.Introduction = data.GetIntroduction()
					req.AvatarUrl = data.GetAvatarUrl()
					req.DirectJoin = data.GetDirectJoin()
					req.HistoryMsg = data.GetHistoryMsg()
					req.CategoryName = data.GetCategoryName()
					req.CategoryId = data.GetCategoryId()
					req.Private = data.GetPrivate()
					req.HideGroupMembers = data.GetHideGroupMembers()
				}

				switch evType {
				case "m.room.name":
					req.Name = asString(content["name"])
				case "m.room.topic":
					req.Introduction = asString(content["topic"])
				case "m.room.avatar":
					ref := asString(content["url"])
					if ref == "" {
						return
					}
					resolved, err := s.Media.ResolveForYunhu(ctx, yc, ref, "image")
					if err != nil {
						s.Log.Debug("group avatar upload failed", "room", roomID, "err", err)
						return
					}
					req.AvatarUrl = yunhu.MediaURL(resolved)
				}
				if err := yc.EditGroup(ctx, req); err != nil {
					s.Log.Debug("upstream group edit failed", "room", roomID, "err", err)
				}
			})
		}
	}

	writeJSON(c.W, http.StatusOK, map[string]any{"event_id": ev.EventID})
}

// readMarkers records the read position and clears unread counters.
func (s *Server) readMarkers(c *Ctx) {
	var body struct {
		FullyRead string `json:"m.fully_read"`
		Read      string `json:"m.read"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)

	acc := c.Account
	roomID := c.Param("roomId")
	s.Store.Mu.Lock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	// The marker is stored as a stream position, not just an event id: unread
	// is derived by comparing positions, so "read up to here" has to be
	// expressible even when the referenced event has been evicted from the
	// bounded timeline.
	marker := orDefault(body.Read, body.FullyRead)
	if marker != "" {
		// markReadLocked normalises the marker to a real event id (m.fully_read
		// must name one), so it is not assigned directly here - that would leave
		// a bare "s123" token in a field clients treat as an event id.
		s.markReadLocked(acc, room, marker)
	}
	room.Unread = 0
	room.Highlight = 0
	chatID := room.ChatID
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.yunhuFor(acc, c.Device).DismissNotification(ctx, chatID); err != nil {
			s.Log.Debug("dismiss notification failed", "err", err)
		}
	}()
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// markReadLocked advances ReadMarkerPos to the event the marker names.
//
// An unknown marker (a client sending a bare "s123" token, or an event id that
// is no longer in the bounded window) is parsed as a stream position when it
// looks like one; anything else falls back to "everything currently known",
// which is what a client means when it says "I have read this room".
func (s *Server) markReadLocked(acc *model.Account, room *model.Room, marker string) {
	pos := int64(0)
	eventID := marker
	if ev := room.FindEvent(marker); ev != nil {
		pos = ev.Pos
	} else if strings.HasPrefix(marker, "s") {
		pos = parseSince(marker)
		// A bare position token is not an event id, so resolve it to whichever
		// event sits at that position. m.fully_read has to name a real event:
		// a client that receives "s42" cannot anchor anything on it.
		eventID = eventAtOrBefore(room, pos)
	}
	if pos <= 0 {
		pos = room.NewestPos()
		eventID = eventAtOrBefore(room, pos)
	}
	if pos > room.ReadMarkerPos {
		room.ReadMarkerPos = pos
	}
	if eventID != "" {
		room.ReadMarker = eventID
		room.ReadMarkerTS = time.Now().UnixMilli()
	}
	// Advance the change cursor even when the marked position did not move: a
	// client re-acknowledging the same event still expects its badge gone, and
	// /sync decides whether to re-send a room from this cursor.
	room.ReadMarkerChangePos = acc.NextPos()
}

// eventAtOrBefore finds the newest timeline event at or before a stream position.
//
// m.fully_read must name a real event, but the position a client acknowledges can
// land on a state event (which is not in the timeline) or between two messages.
// Falling back to "nothing" left the marker empty, so the client could not anchor
// its read position at all - hence "at or before" rather than an exact match.
func eventAtOrBefore(room *model.Room, pos int64) string {
	if pos <= 0 {
		return ""
	}
	best := ""
	bestPos := int64(-1)
	earliest := ""
	earliestPos := int64(-1)
	for _, ev := range room.Events {
		if ev.IsState() || ev.Pos <= 0 {
			continue
		}
		if earliest == "" || ev.Pos < earliestPos {
			earliest = ev.EventID
			earliestPos = ev.Pos
		}
		if ev.Pos <= pos && ev.Pos > bestPos {
			bestPos = ev.Pos
			best = ev.EventID
		}
	}
	if best != "" {
		return best
	}
	// The position predates every message in the window (a room whose first
	// events are all state, or a stale token): anchor at the oldest message, which
	// is the closest thing to "read up to the beginning".
	return earliest
}

// receipt accepts read receipts.
//
// A read receipt is how a client says "I have seen the room up to this event".
// This used to be a bare 200 with no side effect, which is why opening a room
// and scrolling to the bottom never cleared the badge: the client reported the
// read, the server discarded it, and the next sync handed back the same unread
// count. It also means the private read marker (m.fully_read) is advanced even
// when the client only sends a public receipt, because for a single-user
// projection they mean the same thing.
func (s *Server) receipt(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	eventID := c.Param("eventId")

	s.Store.Mu.Lock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	before := room.ReadMarkerPos
	s.markReadLocked(acc, room, eventID)
	room.Unread = 0
	room.Highlight = 0
	changed := room.ReadMarkerPos != before
	chatID := room.ChatID
	s.Store.Mu.Unlock()

	if changed {
		// Marking a room read is a visible change (the badge disappears), so the
		// client has to be woken for it; otherwise it keeps the old count until
		// something else happens in the room.
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.yunhuFor(acc, c.Device).DismissNotification(ctx, chatID); err != nil {
				s.Log.Debug("dismiss notification failed", "err", err)
			}
		}()
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// typing mirrors typing notifications to Yunhu draft sync.
func (s *Server) typing(c *Ctx) {
	var body struct {
		Typing  bool `json:"typing"`
		Timeout int  `json:"timeout"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)
	acc := c.Account

	s.Store.Mu.Lock()
	room := acc.Rooms[c.Param("roomId")]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	if body.Typing {
		ttl := body.Timeout
		if ttl <= 0 {
			ttl = 5000
		}
		room.Typing[c.Param("userId")] = time.Now().UnixMilli() + int64(ttl)
	} else {
		delete(room.Typing, c.Param("userId"))
	}
	chatID := room.ChatID
	s.Store.Mu.Unlock()

	_ = chatID
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// leave marks the room as left locally.
func (s *Server) leave(c *Ctx) {
	acc := c.Account
	s.Store.Mu.Lock()
	room := acc.Rooms[c.Param("roomId")]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	room.Membership = model.MembershipLeave
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// join re-joins a previously left room.
func (s *Server) join(c *Ctx) {
	acc := c.Account
	s.Store.Mu.Lock()
	room := acc.Rooms[c.Param("roomId")]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	room.Membership = model.MembershipJoin
	roomID := room.RoomID
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{"room_id": roomID})
}

// invite explains why inviting is not possible.
func (s *Server) invite(c *Ctx) {
	writeError(c.W, http.StatusForbidden, ErrForbidden,
		"云湖不支持通过 Matrix 邀请成员，请在云湖客户端内邀请")
}

// forget drops a left room from the account.
func (s *Server) forget(c *Ctx) {
	acc := c.Account
	s.Store.Mu.Lock()
	delete(acc.Rooms, c.Param("roomId"))
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// redact recalls a message upstream and records a local m.room.redaction.
func (s *Server) redact(c *Ctx) {
	acc := c.Account
	s.Store.Mu.Lock()
	room := acc.Rooms[c.Param("roomId")]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	ev := room.FindEvent(c.Param("eventId"))
	if ev == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Event not found")
		return
	}
	msgID := ev.YunhuMsgID
	var reason string
	var body map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 1<<20)).Decode(&body); err == nil {
		reason = asString(body["reason"])
	}
	redaction := s.recallEventLocked(acc, room, ev, acc.UserID, reason, 0)
	chatID, chatType := room.ChatID, room.ChatType
	eventID := ev.EventID
	if redaction != nil {
		eventID = redaction.EventID
	}
	s.Store.Mu.Unlock()

	if msgID != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.Timeout())
			defer cancel()
			if err := s.yunhuFor(acc, c.Device).RecallMessage(ctx, chatID, pb.ChatType(chatType), []string{msgID}); err != nil {
				s.Log.Warn("recall failed", "msg", msgID, "err", err)
			}
		}()
	}
	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{"event_id": eventID})
}

func (s *Server) redactEvent(c *Ctx, acc *model.Account, room *model.Room, content map[string]any) {
	targetID := asString(content["redacts"])
	s.Store.Mu.Lock()
	ev := room.FindEvent(targetID)
	if ev == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Event not found")
		return
	}
	msgID := ev.YunhuMsgID
	redaction := s.recallEventLocked(acc, room, ev, acc.UserID, asString(content["reason"]), 0)
	chatID, chatType := room.ChatID, room.ChatType
	eventID := ev.EventID
	if redaction != nil {
		eventID = redaction.EventID
	}
	s.Store.Mu.Unlock()

	if msgID != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.Timeout())
			defer cancel()
			_ = s.yunhuFor(acc, c.Device).RecallMessage(ctx, chatID, pb.ChatType(chatType), []string{msgID})
		}()
	}
	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{"event_id": eventID})
}

// relations implements GET /rooms/{roomId}/relations/{eventId}.
//
// Element X asks for this on every pinned item to render the "edited" marker and
// the reply/edit list. It used to answer an empty chunk unconditionally, which is
// fine for a message nobody edited - but for an edited one the client then shows
// the *old* text: the aggregate in unsigned.m.relations tells it that a
// replacement exists, and it expects the replacement to come back from here.
//
// Supported:
//
//	relType=m.replace      the m.replace events (edits) of the event
//	relType=m.annotation   empty: Yunhu has no reactions
//	(no relType)           everything, which for this projection means edits
//
// Yunhu keeps the newest text on the original message, so the edit event is
// synthesised on the fly instead of being stored: it is a pure function of the
// original event's id, content and EditedTS.
func (s *Server) relations(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")
	eventID := c.Param("eventId")
	relType := c.Query("relType")
	dir := orDefault(c.Query("dir"), "b")

	limit := 10
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 100 {
		limit = 100
	}

	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	target := room.FindEvent(eventID)
	if target == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Event not found")
		return
	}

	chunk := make([]map[string]any, 0, 1)
	if target.EditedTS > 0 && !target.Recalled() && (relType == "" || relType == "m.replace") {
		chunk = append(chunk, s.editRelationJSON(target))
	}
	s.Store.Mu.RUnlock()

	if dir == "f" {
		// Relations have no inherent order beyond the timestamps; the projection
		// only ever produces one edit, so reversing is a no-op that keeps the
		// contract honest.
		for i, j := 0, len(chunk)-1; i < j; i, j = i+1, j-1 {
			chunk[i], chunk[j] = chunk[j], chunk[i]
		}
	}
	// next_batch is a string, never null: Element X parses it as one and a null
	// makes the whole response unparseable.
	writeJSON(c.W, http.StatusOK, map[string]any{
		"chunk":      chunk,
		"next_batch": "",
	})
}

// editRelationJSON renders the synthetic m.replace event of an edited message.
//
// The client merges m.new_content into the original bubble, so the newest text
// has to appear there. This is also why the original's own content is kept as
// the current text: a client that never asks for relations still renders
// something correct.
func (s *Server) editRelationJSON(target *model.Event) map[string]any {
	newContent := target.Content
	if current := asString(target.Content["body"]); current == "" {
		newContent = map[string]any{"msgtype": "m.text", "body": ""}
	}
	return map[string]any{
		"event_id":         target.EditEventID(),
		"type":             "m.room.message",
		"sender":           target.Sender,
		"origin_server_ts": target.EditedTS,
		"content": map[string]any{
			"msgtype":       orDefault(asString(newContent["msgtype"]), "m.text"),
			"body":          editFallbackBody(newContent),
			"m.new_content": newContent,
			"m.relates_to": map[string]any{
				"rel_type": "m.replace",
				"event_id": target.EventID,
			},
		},
	}
}

// threads implements GET /rooms/{roomId}/threads (MSC3856).
//
// Yunhu has no threads, so the list is always empty - but the route has to
// exist. Element X requests it as soon as the thread panel is opened, and an
// unregistered route answers 404 M_UNRECOGNIZED (`status=404 dur_ms=0`), which
// the client renders as "failed to load threads".
//
// The shape follows Synapse: `next_batch` is null when there is nothing more.
func (s *Server) threads(c *Ctx) {
	s.Store.Mu.RLock()
	_, ok := c.Account.Rooms[c.Param("roomId")]
	s.Store.Mu.RUnlock()
	if !ok {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{
		"chunk":      []any{},
		"next_batch": nil,
	})
}

// createRoom handles Matrix's "new chat".
//
// A direct chat enters the existing 1:1 conversation, which is addressable by the
// peer's own id and needs no upstream call. A group is genuinely created on Yunhu
// - but only when the client actually asked for a group (a name and no invite
// target), never as a side effect of picking an existing conversation.
//
// The old code never created anything upstream, which is why "new room" in
// Element could only ever join a conversation that already existed. It went too
// far the other way before that (every new chat called create-group, minting
// empty Yunhu groups nobody wanted), so the resolver below is explicit about
// which case it is in:
//
//  1. `invite` names a user/bot -> enter that 1:1 conversation;
//  2. `invite` names a group    -> enter it if it is already projected;
//  3. otherwise a name/topic/alias is matched against existing rooms;
//  4. still nothing             -> create the group on Yunhu.
func (s *Server) createRoom(c *Ctx) {
	var body struct {
		Name          string            `json:"name"`
		Topic         string            `json:"topic"`
		Preset        string            `json:"preset"`
		IsDirect      bool              `json:"is_direct"`
		Invite        []string          `json:"invite"`
		RoomAliasName string            `json:"room_alias_name"`
		InitialState  []json.RawMessage `json:"initial_state"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 1<<20)).Decode(&body); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Request body is not valid JSON")
		return
	}

	acc := c.Account

	// 1. Direct chat: the invite target names the conversation.
	//
	// An invite naming a group is a request to *enter* that group, which is only
	// possible when it is already in the conversation list: inventing a room for
	// an arbitrary group id would show the user an empty room they are not
	// actually a member of. That is a different thing from creating a new group,
	// which happens only when no invite target was given at all.
	for _, uid := range body.Invite {
		chatID, chatType, ok := s.yunhuIDFromUser(uid)
		if !ok || chatID == "" || chatID == acc.YunhuID {
			continue
		}
		if chatType == model.ChatTypeGroup {
			if roomID := s.roomIDForExisting(acc, chatID, chatType); roomID != "" {
				writeJSON(c.W, http.StatusOK, map[string]any{"room_id": roomID})
				return
			}
			writeError(c.W, http.StatusForbidden, ErrForbidden,
				"这个群不在你的云湖会话列表里：请先在云湖客户端加入它，或在创建房间时不要指定邀请对象来新建一个群")
			return
		}
		roomID := s.enterConversation(acc, chatID, chatType, "", "")
		writeJSON(c.W, http.StatusOK, map[string]any{"room_id": roomID})
		return
	}

	// 2. Existing conversation by name / alias / topic.
	if roomID := s.findConversationByName(acc, body.Name, body.RoomAliasName, body.Topic); roomID != "" {
		s.Store.Mu.Lock()
		if room := acc.Rooms[roomID]; room != nil {
			room.Membership = model.MembershipJoin
		}
		s.Store.Mu.Unlock()
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		writeJSON(c.W, http.StatusOK, map[string]any{"room_id": roomID})
		return
	}

	// 3. Create it upstream.
	s.createYunhuGroup(c, acc, &body)
}

// createYunhuGroup materialises a brand new group conversation on Yunhu.
//
// The request carries everything the client asked for, and most of it has a Yunhu
// equivalent:
//
//	name            -> name
//	topic           -> introduction
//	initial_state   -> m.room.avatar.url  -> avatar_url
//
// The avatar is the awkward one: Matrix clients send an mxc:// URI, but Yunhu
// stores a plain URL, so the bytes have to be re-hosted on Yunhu's bucket first -
// exactly what sending an image does. When that fails the group is still created,
// just without an avatar, because a missing picture is much better than a group
// that failed to exist.
func (s *Server) createYunhuGroup(c *Ctx, acc *model.Account, body *struct {
	Name          string            `json:"name"`
	Topic         string            `json:"topic"`
	Preset        string            `json:"preset"`
	IsDirect      bool              `json:"is_direct"`
	Invite        []string          `json:"invite"`
	RoomAliasName string            `json:"room_alias_name"`
	InitialState  []json.RawMessage `json:"initial_state"`
}) {
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = strings.TrimSpace(body.RoomAliasName)
	}
	if name == "" {
		writeError(c.W, http.StatusBadRequest, ErrInvalidParam,
			"云湖群聊需要名字：请在创建房间时填写名称")
		return
	}

	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	yc := s.yunhuFor(acc, c.Device)

	// The avatar, if the client supplied one.
	avatarURL := ""
	if ref, err := s.avatarRefFromInitialState(body.InitialState); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrUnknown, err.Error())
		return
	} else if ref != "" {
		resolved, err := s.Media.ResolveForYunhu(ctx, yc, ref, "image")
		if err != nil {
			s.Log.Warn("group avatar upload failed, creating without it", "err", err)
		} else {
			// ResolveForYunhu returns a bare Qiniu key after uploading (that is
			// what a message carries), but a group avatar is stored as a URL -
			// its own client puts an absolute one here.
			avatarURL = yunhu.MediaURL(resolved)
		}
	}

	groupID, err := yc.CreateGroup(ctx, name, strings.TrimSpace(body.Topic), avatarURL)
	if err != nil {
		s.Log.Warn("create group failed", "name", name, "err", err)
		writeError(c.W, http.StatusBadGateway, ErrUnknown, "云湖创建群聊失败: "+err.Error())
		return
	}
	if groupID == "" {
		writeError(c.W, http.StatusBadGateway, ErrUnknown, "云湖创建群聊失败：没有返回群 ID")
		return
	}

	roomID := s.enterConversation(acc, groupID, model.ChatTypeGroup, name, avatarURL)
	s.Store.Mu.Lock()
	if room := acc.Rooms[roomID]; room != nil {
		if body.Topic != "" {
			room.Topic = body.Topic
		}
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)

	s.Log.Info("created yunhu group", "group", groupID, "name", name, "room", roomID)
	writeJSON(c.W, http.StatusOK, map[string]any{"room_id": roomID})
}

// avatarRefFromInitialState pulls the m.room.avatar url out of a createRoom
// initial_state array.
//
// The events are raw JSON because a client may send any state type there; only
// the avatar is acted on, and anything malformed is ignored rather than failing
// the whole request.
func (s *Server) avatarRefFromInitialState(raw []json.RawMessage) (string, error) {
	for _, entry := range raw {
		var ev struct {
			Type    string         `json:"type"`
			Content map[string]any `json:"content"`
		}
		if err := json.Unmarshal(entry, &ev); err != nil {
			continue
		}
		if ev.Type != "m.room.avatar" {
			continue
		}
		if url := asString(ev.Content["url"]); url != "" {
			return url, nil
		}
	}
	return "", nil
}

// roomIDForExisting returns the room already projected from a conversation, or
// "" when the account has never seen it.
func (s *Server) roomIDForExisting(acc *model.Account, chatID string, chatType int32) string {
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	if room := acc.RoomByChat(chatID, chatType); room != nil {
		return room.RoomID
	}
	return ""
}

// enterConversation returns the room projected from a Yunhu conversation,
// materialising it if this account has never seen it. Nothing is sent
// upstream: DMs and bot chats are addressable by the peer's own id, so the room
// can exist locally before a single message has been exchanged.
func (s *Server) enterConversation(acc *model.Account, chatID string, chatType int32, name, avatar string) string {
	s.Store.Mu.Lock()
	room := acc.RoomByChat(chatID, chatType)
	if room == nil {
		if name == "" {
			name = s.nameForPeerLocked(acc, chatID, chatType)
		}
		room = s.ensureRoomLocked(acc, chatID, chatType, name, avatar)
	}
	room.Membership = model.MembershipJoin
	s.ensureDirectPeerLocked(acc, room)
	roomID := room.RoomID
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
	return roomID
}

// nameForPeerLocked resolves a display name for a conversation that has not
// been imported yet, so a freshly entered DM is not titled "!yh-user-123".
// The caller must hold the store lock.
func (s *Server) nameForPeerLocked(acc *model.Account, chatID string, chatType int32) string {
	uid := s.mxUserID(chatID, chatType)
	for _, room := range acc.Rooms {
		if m := room.Members[uid]; m != nil && m.DisplayName != "" {
			return m.DisplayName
		}
	}
	return ""
}

// findConversationByName matches an existing projected room and returns its
// room ID, or "" when nothing matches. It returns an ID rather than the room so
// the caller never mutates a pointer obtained under a read lock.
func (s *Server) findConversationByName(acc *model.Account, name, alias, topic string) string {
	want := strings.ToLower(strings.TrimSpace(orDefault(name, alias)))
	if want == "" {
		return ""
	}

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	// Exact name first, then a case-insensitive prefix match so "工作" still
	// finds "工作群".
	prefix := ""
	for _, room := range acc.Rooms {
		if room.Membership != model.MembershipJoin {
			continue
		}
		got := strings.ToLower(strings.TrimSpace(room.Name))
		if got == "" {
			continue
		}
		if got == want {
			return room.RoomID
		}
		if prefix == "" && strings.HasPrefix(got, want) {
			prefix = room.RoomID
		}
	}
	if prefix != "" {
		return prefix
	}

	if topic = strings.TrimSpace(topic); topic != "" {
		for _, room := range acc.Rooms {
			if strings.EqualFold(strings.TrimSpace(room.Topic), topic) {
				return room.RoomID
			}
		}
	}
	return ""
}

// findMember looks up a participant profile across every room of the account.
//
// Bot senders only exist here: Yunhu has no per-bot profile endpoint, so the
// name and avatar captured from the first message they sent is the only
// display data available.
func (s *Server) findMember(acc *model.Account, userID string) *model.Member {
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	for _, room := range acc.Rooms {
		if m := room.Members[userID]; m != nil {
			return m
		}
		if ev := room.State[model.StateID("m.room.member", userID)]; ev != nil {
			return &model.Member{
				UserID:      userID,
				DisplayName: asString(ev.Content["displayname"]),
				AvatarURL:   asString(ev.Content["avatar_url"]),
				Membership:  orDefault(asString(ev.Content["membership"]), model.MembershipJoin),
			}
		}
	}
	return nil
}

// profile returns display name and avatar for a user.
func (s *Server) profile(c *Ctx) {
	userID := c.Param("userId")
	if userID == c.Account.UserID {
		writeJSON(c.W, http.StatusOK, map[string]any{
			"displayname": c.Account.Name,
			"avatar_url":  s.Media.RemoteMXC(c.Account.AvatarURL),
		})
		return
	}
	if member := s.findMember(c.Account, userID); member != nil && (member.DisplayName != "" || member.AvatarURL != "") {
		writeJSON(c.W, http.StatusOK, map[string]any{
			"displayname": member.DisplayName,
			// Member avatars are stored as whatever Yunhu handed us; wrap them
			// so the client loads them from this homeserver, never from the
			// upstream CDN.
			"avatar_url": s.Media.RemoteMXC(member.AvatarURL),
		})
		return
	}

	// Fall back to an upstream lookup.
	if id, chatType, ok := s.yunhuIDFromUser(userID); ok {
		if chatType == model.ChatTypeGroup {
			// Groups are not users; answering with the group profile would give
			// the client a name for a "person" that does not exist.
			writeJSON(c.W, http.StatusOK, map[string]any{"displayname": model.LocalpartOf(userID)})
			return
		}
		ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
		defer cancel()
		if resp, err := s.yunhuFor(c.Account, c.Device).GetUser(ctx, id); err == nil && resp.GetData() != nil {
			writeJSON(c.W, http.StatusOK, map[string]any{
				"displayname": resp.GetData().GetName(),
				"avatar_url":  s.Media.RemoteMXC(resp.GetData().GetAvatarUrl()),
			})
			return
		}
	}
	writeJSON(c.W, http.StatusOK, map[string]any{"displayname": model.LocalpartOf(userID)})
}

// profileDisplayName returns only the display name.
func (s *Server) profileDisplayName(c *Ctx) {
	userID := c.Param("userId")
	if userID == c.Account.UserID {
		writeJSON(c.W, http.StatusOK, map[string]any{"displayname": c.Account.Name})
		return
	}
	if member := s.findMember(c.Account, userID); member != nil && member.DisplayName != "" {
		writeJSON(c.W, http.StatusOK, map[string]any{"displayname": member.DisplayName})
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{"displayname": model.LocalpartOf(userID)})
}

// setDisplayName renames the account upstream.
func (s *Server) setDisplayName(c *Ctx) {
	var body struct {
		DisplayName string `json:"displayname"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)
	if c.Param("userId") != c.Account.UserID {
		writeError(c.W, http.StatusForbidden, ErrForbidden, "Can only change your own display name")
		return
	}
	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	if err := s.yunhuFor(c.Account, c.Device).EditNickname(ctx, body.DisplayName); err != nil {
		writeError(c.W, http.StatusBadGateway, ErrUnknown, err.Error())
		return
	}
	s.Store.Mu.Lock()
	c.Account.Name = body.DisplayName
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(c.Account.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// profileAvatar returns the avatar mxc URI.
func (s *Server) profileAvatar(c *Ctx) {
	if c.Param("userId") == c.Account.UserID {
		writeJSON(c.W, http.StatusOK, map[string]any{"avatar_url": s.Media.RemoteMXC(c.Account.AvatarURL)})
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// setAvatarURL updates the account avatar upstream.
func (s *Server) setAvatarURL(c *Ctx) {
	var body struct {
		AvatarURL string `json:"avatar_url"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)
	if c.Param("userId") != c.Account.UserID {
		writeError(c.W, http.StatusForbidden, ErrForbidden, "Can only change your own avatar")
		return
	}
	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()

	url := body.AvatarURL
	if len(url) > 6 && url[:6] == "mxc://" {
		resolved, err := s.Media.ResolveForYunhu(ctx, s.yunhuFor(c.Account, c.Device), url, "image")
		if err == nil {
			url = resolved
		}
	}
	if err := s.yunhuFor(c.Account, c.Device).EditAvatar(ctx, url); err != nil {
		writeError(c.W, http.StatusBadGateway, ErrUnknown, err.Error())
		return
	}
	s.Store.Mu.Lock()
	c.Account.AvatarURL = url
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(c.Account.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// createFilter stores a sync filter.
func (s *Server) createFilter(c *Ctx) {
	var raw map[string]any
	_ = json.NewDecoder(c.R.Body).Decode(&raw)

	filter := &model.Filter{ID: strconv.Itoa(int(time.Now().UnixNano())), Raw: raw}
	if room, ok := raw["room"].(map[string]any); ok {
		if tl, ok := room["timeline"].(map[string]any); ok {
			filter.TimelineLimit = asInt(tl["limit"])
		}
	}
	if filter.TimelineLimit == 0 {
		filter.Limit = 50
	}

	s.Store.Mu.Lock()
	c.Account.Filters[filter.ID] = filter
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(c.Account.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{"filter_id": filter.ID})
}

// getFilter returns a stored filter.
func (s *Server) getFilter(c *Ctx) {
	s.Store.Mu.RLock()
	f := c.Account.Filters[c.Param("filterId")]
	s.Store.Mu.RUnlock()
	if f == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown filter")
		return
	}
	if f.Raw != nil {
		writeJSON(c.W, http.StatusOK, f.Raw)
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}
