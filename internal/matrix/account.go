package matrix

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// Runner owns the upstream websocket for one account and feeds incoming
// messages into the Matrix event log.
type Runner struct {
	srv  *Server
	acc  *model.Account
	rt   *yunhu.Realtime
	ctx  context.Context
	stop context.CancelFunc
}

// StartRunner creates (or returns) the runner for an account.
func (s *Server) StartRunner(parent context.Context, acc *model.Account) *Runner {
	s.runnerMu.Lock()
	if existing := s.runners[acc.YunhuID]; existing != nil {
		s.runnerMu.Unlock()
		return existing
	}
	ctx, cancel := context.WithCancel(parent)
	r := &Runner{srv: s, acc: acc, ctx: ctx, stop: cancel}
	s.runners[acc.YunhuID] = r
	s.runnerMu.Unlock()

	r.start()
	return r
}

func (r *Runner) start() {
	s := r.srv
	deviceID := r.deviceID()
	rt := &yunhu.Realtime{
		URL:        s.Cfg.Yunhu.WSURL,
		UserID:     r.acc.YunhuID,
		Token:      r.acc.Token,
		Platform:   s.Cfg.Yunhu.Platform,
		DeviceID:   deviceID,
		Heartbeat:  time.Duration(s.Cfg.Yunhu.HeartbeatMS) * time.Millisecond,
		MinBackoff: time.Duration(s.Cfg.Yunhu.ReconnectMinMS) * time.Millisecond,
		MaxBackoff: time.Duration(s.Cfg.Yunhu.ReconnectMaxMS) * time.Millisecond,
		Log:        s.Log,
		CB: yunhu.Callbacks{
			OnConnect: func() {
				s.Log.Info("yunhu realtime connected", "user", r.acc.YunhuID)
				s.safego("refresh-on-connect", func() { s.refreshAccount(context.Background(), r.acc, deviceID) })
			},
			OnPush: func(cmd string, msg *pb.PushMessage) {
				// Runs on the websocket reader goroutine; a panic here would
				// kill the process outright.
				s.guard("ingest", func() { s.ingest(r.acc, cmd, msg) })
			},
			OnInvite: func() {
				s.safego("refresh-on-invite", func() { s.refreshAccount(context.Background(), r.acc, deviceID) })
			},
			OnBoard: func(board *pb.BotBoardMessage) {
				// The bot board is a pinned announcement; a panic here would
				// take down the websocket reader.
				s.guard("board", func() { s.applyBoard(r.acc, board) })
			},
			OnError: func(err error) {
				s.Log.Debug("yunhu realtime error", "user", r.acc.YunhuID, "err", err)
			},
		},
	}
	r.rt = rt
	go rt.Run(r.ctx)
}

// Stop terminates the websocket.
func (r *Runner) Stop() {
	if r.stop != nil {
		r.stop()
	}
}

func (r *Runner) deviceID() string {
	s := r.srv
	if dev := r.acc.PrimaryDevice(); dev != nil && dev.YunhuDeviceID != "" {
		return dev.YunhuDeviceID
	}
	return s.Cfg.Yunhu.DevicePrefix + "-" + r.acc.YunhuID
}

// SendInput mirrors typing notifications to Yunhu draft sync.
func (r *Runner) SendInput(chatID, text string) {
	if r.rt == nil {
		return
	}
	if err := r.rt.SendInput(chatID, text); err != nil {
		r.srv.Log.Debug("draft sync failed", "err", err)
	}
}

// ---------------------------------------------------------------------------
// ingestion
// ---------------------------------------------------------------------------

// ingest projects an upstream message into the account's event log.
func (s *Server) ingest(acc *model.Account, cmd string, m *pb.PushMessage) {
	if m == nil || m.GetMsgId() == "" {
		return
	}
	chatID := m.GetChatId()
	chatType := int32(m.GetChatType())

	s.Store.Mu.Lock()
	// A 1:1 push carries our own id in chat_id *and* recv_id, and the
	// conversation is actually the peer. Resolving that first is what keeps a
	// private message from opening a phantom room under our own id - see
	// conversationOf.
	resolvedID, resolvedType, haveConversation := conversationOf(m, acc)
	if haveConversation {
		chatID, chatType = resolvedID, resolvedType
	}
	// An edit or a recall names a message we have already projected, and the
	// push may omit chat_id entirely. Yunhu's own client finds the message by id
	// alone in that case, so do the same: the message id is unique per account,
	// and without this fallback the push was dropped before it could change
	// anything.
	if chatID == "" {
		if room, orig := acc.RoomByEventMsgID(m.GetMsgId()); room != nil {
			chatID, chatType = room.ChatID, room.ChatType
			_ = orig
		}
	}
	if chatID == "" {
		s.Store.Mu.Unlock()
		s.Log.Debug("dropping push without a conversation", "cmd", cmd, "msg", m.GetMsgId())
		return
	}

	room := acc.RoomByChat(chatID, chatType)
	if room == nil && chatType == 0 {
		// Edit and recall pushes omit chat_type (see the PushMessage comment:
		// "编辑消息无此项" / blocked_message only carries 1,3,4). Without this
		// fallback the lookup above missed every time and ensureRoomLocked
		// created a brand new chatType=0 room per push - so the edit landed in a
		// phantom room and the original message never changed.
		room = acc.RoomByChatID(chatID)
		if room != nil {
			chatType = room.ChatType
		}
	}

	// Edits and recalls are mutations of a message that must already exist here.
	// They never introduce a conversation, so a miss is dropped instead of
	// materialising one: creating a room for them is what made an empty
	// "空气会话" appear in the client's list every time somebody edited a message
	// this homeserver had not projected (an older message, or one from before a
	// restart with a narrower history window).
	if cmd == "edit_message" || cmd == "blocked_message" {
		if room == nil {
			if r, _ := acc.RoomByEventMsgID(m.GetMsgId()); r != nil {
				room, chatType = r, r.ChatType
			}
		}
		if room == nil {
			s.Store.Mu.Unlock()
			s.Log.Debug("dropping mutation for an unknown conversation",
				"cmd", cmd, "msg", m.GetMsgId(), "chat", chatID)
			return
		}
	}

	if room == nil {
		room = s.ensureRoomLocked(acc, chatID, chatType, "", "")
	}

	// Recalls do not carry new content: Yunhu pushes a bare blocked_message
	// with only msg_id/recv_id/chat_id, so it is projected as a real
	// m.room.redaction targeting the original event. The original keeps its
	// stream position - bumping it would re-deliver the message as a brand new
	// (and empty) timeline entry at the bottom of the room.
	if cmd == "blocked_message" {
		if orig := room.EventByYunhuID(m.GetMsgId()); orig != nil && !orig.Recalled() {
			ts := int64(m.GetDeleteTimestamp())
			if ts == 0 {
				ts = int64(m.GetTimestamp())
			}
			s.recallEventLocked(acc, room, orig, orig.Sender, "", ts)
		}
		s.Store.Mu.Unlock()
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		return
	}

	// An edit is not a timeline entry. It updates the message it replaces (which
	// is keyed by its own Yunhu id, not by the "<id>_edit" key the relation event
	// carries) and stamps EditedTS, which is what makes a client draw one bubble
	// with an "edited" marker. Appending the relation event here - as this used
	// to - produced a second message reading "* new text" while the original
	// kept showing the old text.
	//
	// An edit for a message we never saw is dropped: the newest text is already
	// what history would return, and a standalone "* text" bubble is noise.
	if cmd == "edit_message" {
		chatID, chatType := room.ChatID, room.ChatType
		s.applyEditLocked(acc, room, m)
		s.Store.Mu.Unlock()
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)

		// The websocket edit payload is known to be partial, so the text it
		// carries is not authoritative (the proto says "编辑消息的会话内容和发送
		// 消息可以共用但是编辑消息有缺失"). Yunhu's own client reacts by reading
		// the message back; do the same, in the background so the websocket
		// reader is never blocked on a REST round trip.
		s.safego("edit-refetch", func() {
			s.refetchEditedMessage(acc, chatID, chatType, m)
		})
		return
	}

	ev := s.yunhuToEvent(room, cmd, m, 0)
	if existing := room.EventByYunhuID(ev.YunhuMsgID); existing != nil {
		// Already delivered (for example our own optimistic echo).
		s.Store.Mu.Unlock()
		return
	}
	ev.Pos = acc.NextPos()

	room.AppendEvent(ev, s.Cfg.Storage.MaxEventsPerRoom)
	if m.GetMsgSeq() > room.YunhuMsgSeq {
		room.YunhuMsgSeq = m.GetMsgSeq()
	}
	s.recordMemberLocked(acc, room, m)
	// Unread is derived from the read marker (Room.UnreadFor), so nothing is
	// counted here. Incrementing a counter on ingest is exactly what made the
	// badge impossible to clear: it grew with every message and was re-seeded
	// from upstream on each reconnect.
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
}

// refetchEditedMessage reads an edited message back from REST and replaces the
// projected content with the authoritative copy.
//
// Why this exists at all: the websocket edit push is documented as partial
// (chat_type, sender, timestamp and parts of the content object can be missing),
// so the text it carries cannot be trusted to be complete. Yunhu's own Android
// client does exactly this - it merges the push, then immediately calls
// list-message-by-mid-seq and overwrites the bubble with the API payload.
//
// Failures are silent: the push-derived content is already in place, this only
// upgrades it.
func (s *Server) refetchEditedMessage(acc *model.Account, chatID string, chatType int32, m *pb.PushMessage) {
	msgID := m.GetMsgId()
	if msgID == "" || chatID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := s.yunhuFor(acc, nil).ListMessagesByMidSeq(ctx, chatID, pb.ChatType(chatType), msgID, 1)
	if err != nil {
		s.Log.Debug("edit refetch failed", "msg", msgID, "err", err)
		return
	}
	var full *pb.ListMsgResponse_MsgData
	for _, md := range resp.GetData() {
		if md.GetMsgId() == msgID {
			full = md
			break
		}
	}
	if full == nil {
		// Nothing came back for this id; keep what the push gave us.
		return
	}

	s.Store.Mu.Lock()
	room := acc.RoomByChat(chatID, chatType)
	if room == nil {
		room = acc.RoomByChatID(chatID)
	}
	var orig *model.Event
	if room != nil {
		orig = room.EventByYunhuID(msgID)
	}
	if orig == nil || orig.Recalled() {
		s.Store.Mu.Unlock()
		return
	}
	next := s.buildContent(full.GetContentType(), full.GetContent(), botNameResolver(room))
	if isEmptyContent(next) {
		// The API returned nothing usable; keep what the push gave us rather than
		// blanking a message the user can read.
		s.Store.Mu.Unlock()
		return
	}
	// Only the content is replaced: the edit timestamp and stream position came
	// from the push and are already correct.
	orig.Content = next
	if ts := int64(full.GetEditTimestamp()); ts > 0 {
		if ts < 1e12 {
			ts *= 1000
		}
		orig.EditedTS = ts
	}
	s.Store.Mu.Unlock()

	s.Store.MarkDirty(acc.YunhuID)
	s.Hub.Notify(acc.YunhuID)
}

// applyEditLocked folds an upstream edit into the message it replaces. The
// caller must hold the store lock.
//
// Yunhu edits mutate a message in place; Matrix has no such operation, so the
// projection keeps the original event, puts the new content on it and records
// when. The stream position is bumped so /sync re-delivers the message and the
// client re-renders it - without that the edit would not reach an open room
// until something else happened.
func (s *Server) applyEditLocked(acc *model.Account, room *model.Room, m *pb.PushMessage) {
	orig := room.EventByYunhuID(m.GetMsgId())
	if orig == nil {
		// Never saw the message: the newest text is already what any later
		// backfill will return, so there is nothing to reconcile.
		return
	}
	if orig.Recalled() {
		// An edit racing a recall must not resurrect the content: the redaction
		// already wiped it and the client renders "message deleted".
		return
	}
	next := s.buildContent(m.GetContentType(), m.GetContent(), botNameResolver(room))
	if !isEmptyContent(next) {
		orig.Content = next
	}
	// An edit push can carry an empty content object (the proto notes the edit
	// payload has missing fields). Overwriting with that would blank a message the
	// user can still read, and the refetch that follows may fail - so the old
	// content is kept and only the marker is set.
	//
	// The position is bumped either way: that is what makes /sync deliver the
	// event again, and without it a client that is already looking at the room
	// never learns the message changed - no "edited" marker ever appears.
	ts := int64(m.GetEditTimestamp())
	if ts == 0 {
		ts = int64(m.GetTimestamp())
	}
	if ts > 0 && ts < 1e12 {
		ts *= 1000
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	orig.EditedTS = ts
	orig.Pos = acc.NextPos()
}

// isEmptyContent reports whether message content carries no visible body or media.
//
// It guards against an edit push whose content object is missing: buildContent
// turns that into {"msgtype":"m.text","body":""}, and writing that over a real
// message would empty a bubble the user can still read.
func isEmptyContent(content map[string]any) bool {
	if content == nil {
		return true
	}
	if strings.TrimSpace(asString(content["body"])) != "" {
		return false
	}
	for _, key := range []string{"url", "file", "filename"} {
		if strings.TrimSpace(asString(content[key])) != "" {
			return false
		}
	}
	return true
}

// recallEventLocked appends an m.room.redaction that removes target from the
// timeline and wipes the target's content in place. The caller must hold the
// store lock.
//
// Matrix models a recall as a *new* event that points at the old one, never as
// a mutation of the old one: the target keeps its event ID, timestamp and
// stream position so clients redact it where it stands, and the redaction
// carries the fresh position that /sync delivers.
func (s *Server) recallEventLocked(acc *model.Account, room *model.Room, target *model.Event, sender, reason string, ts int64) *model.Event {
	if target == nil {
		return nil
	}
	if sender == "" {
		sender = target.Sender
	}
	if sender == "" {
		sender = acc.UserID
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	if ts < 1e12 { // seconds instead of milliseconds
		ts *= 1000
	}

	pos := acc.NextPos()
	redaction := &model.Event{
		EventID:  fmt.Sprintf("$redact_%s_%d", sanitize(target.EventID), pos),
		Type:     "m.room.redaction",
		Sender:   sender,
		Redacts:  target.EventID,
		Content:  map[string]any{"reason": reason},
		OriginTS: ts,
		Pos:      pos,
	}

	target.Content = map[string]any{}
	if target.Unsigned == nil {
		target.Unsigned = map[string]any{}
	}
	target.Unsigned["yunhu.recalled"] = true
	target.RecalledBy = redaction.EventID

	room.AppendEvent(redaction, s.Cfg.Storage.MaxEventsPerRoom)
	return redaction
}

// mentionUser is unused by the counting path now that unread is derived: the
// derived counter asks model.Event.MentionsUser directly. Kept out of the build
// on purpose - see Room.UnreadFor.

// recordMemberLocked stores sender profile information and emits an
// m.room.member state event the first time a participant is seen, so clients
// can render names and avatars in the timeline and member list.
//
// The user ID must come from senderUserID, not from a raw mxUserID call: bot
// pushes often arrive without a sender chat_type, and deriving the ID here
// independently produced a member record for @yh_<botid> while the message
// itself was sent by @yhbot_<botid> - so the bot showed up as an unknown user
// with no profile at all.
func (s *Server) recordMemberLocked(acc *model.Account, room *model.Room, m *pb.PushMessage) {
	senderID := m.GetSender().GetChatId()
	if senderID == "" {
		return
	}
	s.putMemberLocked(acc, room, s.senderUserID(room, m),
		m.GetSender().GetName(), m.GetSender().GetAvatarUrl())
}

// putMemberLocked records a participant and emits m.room.member the first time
// that user is seen, so clients can render names and avatars in the timeline and
// the member list.
func (s *Server) putMemberLocked(acc *model.Account, room *model.Room, uid, displayName, avatar string) {
	if uid == "" {
		return
	}
	avatarMXC := s.Media.RemoteMXC(avatar)

	room.Members[uid] = &model.Member{
		UserID:      uid,
		DisplayName: displayName,
		AvatarURL:   avatarMXC,
		Membership:  model.MembershipJoin,
	}

	if _, exists := room.State[model.StateID("m.room.member", uid)]; exists {
		return
	}
	key := uid
	pos := acc.NextPos()
	room.AppendEvent(&model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     "m.room.member",
		Sender:   uid,
		StateKey: &key,
		Content: map[string]any{
			"membership":  "join",
			"displayname": displayName,
			"avatar_url":  avatarMXC,
		},
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}, 0)
}

// ---------------------------------------------------------------------------
// room projection
// ---------------------------------------------------------------------------

// ensureRoomLocked creates the Matrix room for a Yunhu conversation. The caller
// must hold the store lock.
func (s *Server) ensureRoomLocked(acc *model.Account, chatID string, chatType int32, name, avatar string) *model.Room {
	roomID := s.roomIDFor(chatID, chatType)
	if r := acc.Rooms[roomID]; r != nil {
		return r
	}

	room := model.NewRoom(roomID, chatID, chatType)
	room.Name = name
	room.AvatarURL = avatar
	// A bot conversation is a 1:1 conversation. Only groups are multi-party, so
	// treating bots as non-direct made clients render them as group chats:
	// IsDirect drives `is_dm`, which drives the DM layout (avatar instead of a
	// room name in the header) and `heroes`, which is what lets a client name a
	// direct room from its participant.
	room.IsDirect = chatType == model.ChatTypeUser || chatType == model.ChatTypeBot
	acc.Rooms[roomID] = room

	now := time.Now().UnixMilli()
	addState := func(eventType, stateKey string, content map[string]any) {
		pos := acc.NextPos()
		key := stateKey
		ev := &model.Event{
			EventID:  fmt.Sprintf("$state_%s_%d", sanitize(roomID), pos),
			Type:     eventType,
			Sender:   acc.UserID,
			StateKey: &key,
			Content:  content,
			OriginTS: now,
			Pos:      pos,
		}
		room.AppendEvent(ev, 0)
	}

	addState("m.room.create", "", map[string]any{
		"creator":      acc.UserID,
		"room_version": "10",
	})
	addState("m.room.member", acc.UserID, map[string]any{
		"membership":  "join",
		"displayname": acc.Name,
		"avatar_url":  s.Media.RemoteMXC(acc.AvatarURL),
	})
	// Who owns the room?
	//
	// In a DM or a bot chat the account is the only participant, so it holds
	// full power. A Yunhu group is different: the conversation belongs to
	// whoever created it upstream, and this homeserver merely projects it. The
	// seeded 100 used to make every group render the account as its "管理员"
	// (Element's zh-CN for power 100) even when it was a plain member, so a
	// group starts with an empty users map and syncGroupPowerLevelsLocked fills
	// it from /v1/group/info (owner, admin list, own permission_level).
	powerUsers := map[string]any{}
	if chatType != model.ChatTypeGroup {
		powerUsers[acc.UserID] = powerLevelOwner
	}
	addState("m.room.power_levels", "", map[string]any{
		"users":          powerUsers,
		"users_default":  0,
		"events_default": 0,
		"state_default":  50,
		"ban":            50,
		"kick":           50,
		"redact":         50,
		"invite":         0,
	})
	addState("m.room.join_rules", "", map[string]any{"join_rule": "public"})
	addState("m.room.history_visibility", "", map[string]any{"history_visibility": "shared"})
	addState("m.room.guest_access", "", map[string]any{"guest_access": "can_join"})
	if name != "" {
		addState("m.room.name", "", map[string]any{"name": name})
	}
	if avatar != "" {
		addState("m.room.avatar", "", map[string]any{"url": s.Media.RemoteMXC(avatar)})
	}
	return room
}

// ensureDirectPeerLocked makes sure a 1:1 room lists its other participant.
//
// Without this a bot conversation contains only the account itself: the bot is
// only recorded when it speaks, and many bots never do. Clients then render the
// room as "just you", and member counts, heroes and avatars are all wrong.
// It reports whether anything was added.
func (s *Server) ensureDirectPeerLocked(acc *model.Account, room *model.Room) bool {
	if room.ChatType != model.ChatTypeUser && room.ChatType != model.ChatTypeBot {
		return false
	}
	peerID := s.mxUserID(room.ChatID, room.ChatType)
	if peerID == "" || peerID == acc.UserID {
		return false
	}
	if _, exists := room.State[model.StateID("m.room.member", peerID)]; exists {
		return false
	}

	displayName := room.Name
	avatar := room.AvatarURL
	if m := room.Members[peerID]; m != nil {
		if m.DisplayName != "" {
			displayName = m.DisplayName
		}
		if m.AvatarURL != "" {
			avatar = m.AvatarURL
		}
	}
	avatarMXC := s.Media.RemoteMXC(avatar)

	room.Members[peerID] = &model.Member{
		UserID:      peerID,
		DisplayName: displayName,
		AvatarURL:   avatarMXC,
		Membership:  model.MembershipJoin,
	}

	key := peerID
	pos := acc.NextPos()
	room.AppendEvent(&model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     "m.room.member",
		Sender:   peerID,
		StateKey: &key,
		Content: map[string]any{
			"membership":  model.MembershipJoin,
			"displayname": displayName,
			"avatar_url":  avatarMXC,
		},
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}, 0)
	return true
}

// inferChatTypeLocked recovers a conversation's type from what is already known.
//
// The conversation list normally carries chat_type, but an entry can arrive
// without it. Guessing "user" for those put bots into 1:1 rooms - which is the
// reported symptom, several bots showing up as group chats once one of them was
// also seen through a path that said "group". The type is derivable from the
// room that already holds this conversation, so prefer that over a default.
func (s *Server) inferChatTypeLocked(acc *model.Account, chatID string) int32 {
	if existing := acc.RoomByChatID(chatID); existing != nil {
		return existing.ChatType
	}
	// A conversation whose id is also one of the account's known bot ids is a
	// bot conversation: bots are addressed by their own id.
	botPrefix := "@" + userPrefixBot
	for _, room := range acc.Rooms {
		for memberID := range room.Members {
			if !strings.HasPrefix(memberID, botPrefix) {
				continue
			}
			if id, _, ok := s.yunhuIDFromUser(memberID); ok && id == chatID {
				return model.ChatTypeBot
			}
		}
	}
	return model.ChatTypeUser
}

// retypeRoomLocked moves a room to a different chat type, keeping everything the
// user can see.
//
// The room id is derived from (chat id, chat type), so a correction cannot be
// just a field write: the account map key has to change too, or the client keeps
// addressing the old id. Everything else - events, members, state, read marker -
// is carried over untouched.
func (s *Server) retypeRoomLocked(acc *model.Account, room *model.Room, chatType int32) *model.Room {
	if room.ChatType == chatType {
		return room
	}
	oldID := room.RoomID
	newID := s.roomIDFor(room.ChatID, chatType)
	room.ChatType = chatType
	room.RoomID = newID
	room.IsDirect = chatType == model.ChatTypeUser || chatType == model.ChatTypeBot
	delete(acc.Rooms, oldID)
	acc.Rooms[newID] = room
	s.Log.Info("retyped conversation", "chat", room.ChatID, "from", oldID, "to", newID)
	return room
}

// refreshAccount pulls the conversation list and backfills new rooms.
func (s *Server) refreshAccount(ctx context.Context, acc *model.Account, deviceID string) {
	yc := s.yunhuFor(acc, nil).WithDevice(deviceID)
	resp, err := yc.Conversations(ctx, "")
	if err != nil {
		s.Log.Warn("conversation list failed", "user", acc.YunhuID, "err", err)
		return
	}

	s.Store.Mu.Lock()
	changedMute := false
	for _, c := range resp.GetData() {
		name := orDefault(c.GetRemark(), c.GetName())
		chatType := int32(c.GetChatType())
		if chatType == 0 {
			// A conversation list entry can arrive without its type. Creating a
			// chatType=0 room is not harmless: the room id is derived from the
			// type, so the bot would show up under !yh-user-… and every later
			// lookup would disagree about which room a conversation is. Recover
			// the type from what the account already knows instead.
			chatType = s.inferChatTypeLocked(acc, c.GetChatId())
		}
		room := s.ensureRoomLocked(acc, c.GetChatId(), chatType, name, c.GetAvatarUrl())
		if c.GetChatType() != 0 && room.ChatType != int32(c.GetChatType()) {
			// The upstream list is authoritative: if it now says this
			// conversation is a group, the room is a group, and the room id has
			// to move with it.
			room = s.retypeRoomLocked(acc, room, int32(c.GetChatType()))
		}
		if name != "" && room.Name != name {
			room.Name = name
			s.setRoomNameLocked(acc, room, name)
		}
		// The upstream count is only a seed for rooms that have never been
		// read here. Overwriting it unconditionally is what made a cleared
		// badge grow back on every reconnect: dismissing the notification
		// upstream clears their badge, but the conversation list keeps
		// reporting unread_msg because the messages were never read there.
		// Once this account has a read marker, the number is derived from the
		// timeline instead (Room.UnreadFor).
		if c.GetUnreadMsg() > 0 && room.ReadMarkerPos == 0 && int(c.GetUnreadMsg()) > room.Unread {
			room.Unread = int(c.GetUnreadMsg())
		}
		if ts := int64(c.GetTimestampMs()); ts > room.LastActivityTS {
			room.LastActivityTS = ts
		}
		// The mute state lives upstream and this is the only place it arrives:
		// conversation list entries carry do_not_disturb. Matrix expresses the
		// same thing as the room's push rule, so a room muted in the Yunhu client
		// has to show up muted here too.
		if c.GetDoNotDisturb() != room.Muted {
			room.Muted = c.GetDoNotDisturb()
			changedMute = true
		}
		room.MutedTS = time.Now().UnixMilli()
		s.ensureDirectPeerLocked(acc, room)
	}
	acc.ConversationsSynced = true
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
	if changedMute {
		// The room list draws a mute badge, so the change has to reach clients
		// now rather than on the next unrelated event.
		s.Hub.Notify(acc.YunhuID)
	}

	// Participant counts before history: a client renders the member count in
	// the room header immediately, while history loads lazily. Detach from the
	// caller's context: the /sync that triggered the import gives up after 12
	// seconds, and aborting here would leave most rooms with the local fallback
	// count until the next reconnect.
	s.refreshMemberCounts(context.WithoutCancel(ctx), acc, yc)

	// Backfill rooms that have never been hydrated, and reconcile the ones
	// whose newest local message is behind upstream.
	//
	// Only "never hydrated" used to be considered. Everything that arrived
	// while the process was down - or during a failed backfill, which was still
	// marked done - stayed missing forever, so a client showed a timeline with a
	// hole in it and no amount of reconnecting repaired it. The only cure was
	// deleting data/, which is exactly the symptom that was reported.
	s.Store.Mu.RLock()
	pending := make([]*model.Room, 0)
	for _, r := range acc.Rooms {
		if !r.Backfilled || r.GapSuspected() {
			pending = append(pending, r)
		}
	}
	s.Store.Mu.RUnlock()

	for _, room := range pending {
		if ctx.Err() != nil {
			return
		}
		s.backfillRoom(ctx, acc, yc, room)
	}

	s.Hub.Notify(acc.YunhuID)
}

// memberCountTTL is how long a fetched headcount is trusted before it is
// refreshed. A group's size changes slowly; re-fetching on every reconnect
// would hammer the API for no visible gain.
const memberCountTTL = 30 * time.Minute

// refreshMemberCounts fills Room.MemberCount from Yunhu's /v1/group/info and, in
// the same pass, the group's roles (owner / admins / own permission_level).
//
// Counting the members this homeserver happens to know about is wrong by an
// order of magnitude: only people who spoke in the hydrated window exist
// locally, so a 500-person group reported "6 members". headcount is the only
// authoritative number, and clients render it in the room header and the
// member list, so it has to be fetched rather than guessed.
//
// The roles matter just as much: a Matrix room has to say who its admins are,
// and the answer is never "the account, because this homeserver created the
// room". Yunhu's group/info is the only place that knows.
func (s *Server) refreshMemberCounts(ctx context.Context, acc *model.Account, yc *yunhu.Client) {
	now := time.Now().UnixMilli()
	stale := int64(memberCountTTL / time.Millisecond)

	s.Store.Mu.RLock()
	pending := make([]*model.Room, 0, len(acc.Rooms))
	for _, r := range acc.Rooms {
		if r.ChatType != model.ChatTypeGroup {
			continue
		}
		// Both the headcount and the roles come from this one request, so a
		// room is only skipped when it has both and neither is stale. Requiring
		// RolesTS > 0 also migrates rooms created by an older build, which still
		// carry the fabricated "the account owns every group" power level.
		//
		// TopicTS is the third piece: the same response carries the group's
		// introduction, and a room whose description was never read has to be
		// asked once regardless of how fresh its headcount is. Remembering the
		// attempt (rather than just "topic is empty") keeps a group that
		// genuinely has no description from being re-fetched on every reconnect.
		fresh := now-r.MemberCountTS < stale && now-r.RolesTS < stale && now-r.TopicTS < stale
		if r.MemberCount > 0 && r.RolesTS > 0 && r.TopicTS > 0 && fresh {
			continue
		}
		pending = append(pending, r)
	}
	s.Store.Mu.RUnlock()
	if len(pending) == 0 {
		return
	}

	// Fetch concurrently: a couple of hundred groups one after another would
	// hold up the first /sync for far longer than the client is willing to
	// wait.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	type countResult struct {
		room *model.Room
		data *pb.GroupData
	}
	jobs := make(chan *model.Room)
	results := make(chan countResult, len(pending))

	var wg sync.WaitGroup
	workers := 8
	if len(pending) < workers {
		workers = len(pending)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for room := range jobs {
				if ctx.Err() != nil {
					return
				}
				resp, err := yc.GroupInfo(ctx, room.ChatID)
				if err != nil {
					s.Log.Debug("group info failed", "room", room.RoomID, "err", err)
					continue
				}
				results <- countResult{room: room, data: resp.GetData()}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, room := range pending {
			select {
			case jobs <- room:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(results)

	changed := false
	now = time.Now().UnixMilli()
	s.Store.Mu.Lock()
	for res := range results {
		data := res.data
		if data == nil {
			continue
		}
		if n := int(data.GetHeadcount()); n > 0 {
			if res.room.MemberCount != n {
				res.room.MemberCount = n
				// A fresh stream position is what makes /sync include this room
				// even though no timeline event was produced, so the client
				// actually redraws the header.
				res.room.MemberCountPos = acc.NextPos()
				changed = true
			}
			res.room.MemberCountTS = now
		}
		// The same response carries the group's owner, its admins and the
		// account's own permission level, which is what decides who a client
		// badges as 管理员/主持人. Without this the room keeps the roles it was
		// created with, i.e. the account looks like the creator of every group.
		if s.applyGroupRolesLocked(acc, res.room, data) {
			changed = true
		}
		// And its introduction, which is exactly what a Matrix client shows as
		// the room's description. Nothing ever projected this, so every group
		// opened with an empty "简介" even though Yunhu has one.
		if intro := strings.TrimSpace(data.GetIntroduction()); intro != "" && intro != res.room.Topic {
			res.room.Topic = intro
			s.setRoomTopicLocked(acc, res.room, intro)
			changed = true
		}
		// Record that we asked, even when the group has no introduction at all:
		// that is a real answer, and re-asking on every reconnect would defeat
		// the TTL this whole function is built around.
		res.room.TopicTS = now
	}
	s.Store.Mu.Unlock()

	if changed {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
}

func (s *Server) setRoomNameLocked(acc *model.Account, room *model.Room, name string) {
	s.setRoomStateLocked(acc, room, "m.room.name", map[string]any{"name": name})
}

// setRoomTopicLocked publishes the group's introduction as m.room.topic.
func (s *Server) setRoomTopicLocked(acc *model.Account, room *model.Room, topic string) {
	s.setRoomStateLocked(acc, room, "m.room.topic", map[string]any{"topic": topic})
}

// setRoomStateLocked appends one room state event. The caller must hold the
// store lock.
func (s *Server) setRoomStateLocked(acc *model.Account, room *model.Room, eventType string, content map[string]any) {
	key := ""
	pos := acc.NextPos()
	room.AppendEvent(&model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     eventType,
		Sender:   acc.UserID,
		StateKey: &key,
		Content:  content,
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}, 0)
}

// backfillRoom hydrates a room's timeline from the REST API.
//
// Two modes:
//
//   - first hydration (!Backfilled): pull the newest Backfill messages and
//     insert them oldest-first so stream positions are monotonic.
//   - gap repair (Backfilled but behind upstream): walk backwards from the
//     newest upstream message until the page reaches something we already have,
//     splicing the missing messages in between.
//
// The gap-repair walk is bounded (maxBackfillPages) so a room whose history was
// never fully downloaded cannot turn a reconnect into an unbounded crawl. When
// the bound is hit the position is still recorded, so the *next* reconcile
// continues where this one stopped rather than restarting from the top.
func (s *Server) backfillRoom(ctx context.Context, acc *model.Account, yc *yunhu.Client, room *model.Room) {
	n := s.Cfg.Yunhu.Backfill
	if n <= 0 {
		s.markBackfilled(acc, room)
		return
	}

	s.Store.Mu.RLock()
	alreadyDone := room.Backfilled
	haveNewest := room.BackfilledPos
	s.Store.Mu.RUnlock()

	resp, err := yc.ListMessages(ctx, room.ChatID, pb.ChatType(room.ChatType), "", int32(n))
	if err != nil {
		// Try the sequence endpoint: some chat types only answer that one.
		resp, err = yc.ListMessagesBySeq(ctx, room.ChatID, pb.ChatType(room.ChatType), 0)
	}
	if err != nil {
		s.Log.Debug("backfill failed", "room", room.RoomID, "err", err)
		// NOT marking the room as backfilled: a failed fetch must be retried on
		// the next refresh, otherwise the room stays empty forever. The old code
		// marked it done even here, which is one of the ways a room ended up
		// permanently missing its history.
		return
	}

	// Stream positions are assigned in iteration order, so the response has to
	// be oldest-first or the timeline (and every pagination token derived from
	// it) comes out inverted.
	data := resp.GetData()
	sortMsgsChronological(data)
	if len(data) == 0 {
		s.finishBackfill(acc, room, 0)
		return
	}

	// Decide whether the fetched page touches what we already have.
	//
	// The newest timestamp alone is not a sufficient test: a room can be current
	// at the bottom and still be missing everything in the middle (messages sent
	// while the process was down). What matters is whether the page *overlaps*
	// the local timeline.
	//
	// Two independent holes, both worth a repair pass:
	//   - the page starts after our newest message (a run of messages in the
	//     middle was never ingested), or
	//   - the page ends after our newest message (messages arrived during the
	//     downtime).
	// In both cases repairGap collects what is missing and dedupes the rest.
	pageOldest := data[0].GetSendTimestampMs()
	pageNewest := data[len(data)-1].GetSendTimestampMs()

	if alreadyDone && haveNewest > 0 {
		if room.GapBetween(pageOldest, pageNewest) || room.GapAfter(pageNewest) {
			s.repairGap(ctx, acc, yc, room, data)
			return
		}
		s.finishBackfill(acc, room, 0)
		return
	}

	s.Store.Mu.Lock()
	added := 0
	for _, md := range data {
		if s.appendHistoryLocked(acc, room, md) {
			added++
		}
	}
	room.Backfilled = true
	room.BackfillTS = time.Now().UnixMilli()
	newest := room.NewestPos()
	s.Store.Mu.Unlock()

	if added > 0 {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
	s.setBackfilledPos(acc, room, newest)
}

// maxBackfillPages bounds the backwards walk of a gap repair.
const maxBackfillPages = 6

// repairGap downloads the messages between the local newest and upstream newest.
//
// It pages backwards from "the newest upstream message" until a page contains
// something already present locally, which marks the seam. Pages are collected
// newest-first and then inserted in chronological order so positions stay
// monotonic.
func (s *Server) repairGap(ctx context.Context, acc *model.Account, yc *yunhu.Client, room *model.Room, newestPage []*pb.ListMsgResponse_MsgData) {
	perPage := s.Cfg.Yunhu.Backfill
	if perPage <= 0 {
		perPage = 10
	}

	// Everything in the newest page that we do not have yet.
	var collected []*pb.ListMsgResponse_MsgData
	seen := map[string]struct{}{}      // every id looked at, across pages
	collectedSeen := map[string]struct{}{} // ids already queued for insertion
	addPage := func(page []*pb.ListMsgResponse_MsgData) bool {
		sortMsgsChronological(page)
		// Pages are chronological, so the seam is the *last* known message:
		// everything after it is newer and still missing.
		lastKnown := -1
		for i, md := range page {
			id := md.GetMsgId()
			if id == "" {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			s.Store.Mu.RLock()
			known := room.EventByYunhuID(id) != nil
			s.Store.Mu.RUnlock()
			if known {
				lastKnown = i
			}
		}
		for i, md := range page {
			if i <= lastKnown {
				continue
			}
			id := md.GetMsgId()
			if id == "" {
				continue
			}
			if _, dup := collectedSeen[id]; dup {
				continue
			}
			collectedSeen[id] = struct{}{}
			collected = append(collected, md)
		}
		return lastKnown >= 0
	}

	if addPage(newestPage) {
		s.finishGap(acc, room, collected)
		return
	}

	// Keep walking back from the oldest message of the current page.
	anchor := newestPage[0].GetMsgId()
	for page := 0; page < maxBackfillPages && anchor != ""; page++ {
		if ctx.Err() != nil {
			break
		}
		resp, err := yc.ListMessages(ctx, room.ChatID, pb.ChatType(room.ChatType), anchor, int32(perPage))
		if err != nil {
			s.Log.Debug("gap repair page failed", "room", room.RoomID, "anchor", anchor, "err", err)
			break
		}
		data := resp.GetData()
		if len(data) == 0 {
			break
		}
		sortMsgsChronological(data)
		seam := addPage(data)
		anchor = data[0].GetMsgId()
		if seam {
			break
		}
	}

	s.finishGap(acc, room, collected)
}

// finishGap inserts the collected gap messages in chronological order.
func (s *Server) finishGap(acc *model.Account, room *model.Room, collected []*pb.ListMsgResponse_MsgData) {
	if len(collected) == 0 {
		// Nothing missing after all: refresh the watermark so the next
		// reconcile does not re-download the same page.
		s.finishBackfill(acc, room, 0)
		return
	}
	sortMsgsChronological(collected)

	s.Store.Mu.Lock()
	added := 0
	for _, md := range collected {
		if s.appendHistoryLocked(acc, room, md) {
			added++
		}
	}
	room.Backfilled = true
	room.BackfillTS = time.Now().UnixMilli()
	newest := room.NewestPos()
	s.Store.Mu.Unlock()

	if added > 0 {
		s.Log.Info("filled history gap", "room", room.RoomID, "messages", added)
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
	s.setBackfilledPos(acc, room, newest)
}

// appendHistoryLocked inserts one upstream message into the timeline. The caller
// must hold the store lock. It reports whether anything was added.
func (s *Server) appendHistoryLocked(acc *model.Account, room *model.Room, md *pb.ListMsgResponse_MsgData) bool {
	if md.GetMsgId() == "" {
		return false
	}
	if room.EventByYunhuID(md.GetMsgId()) != nil {
		return false
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
		// recall_timestamp is how list-message reports a message that was
		// recalled before we ever saw it: it comes back with empty content and
		// this stamp. Carrying it over is what makes the history page render
		// "message deleted" instead of a blank bubble.
		DeleteTimestamp: md.GetRecallTimestamp(),
	}
	ev := s.yunhuToEvent(room, "push_message", pm, acc.NextPos())
	// History comes back already edited: list-message returns the *final*
	// content plus an edit_timestamp, never the text that was replaced. So the
	// projection is a normal message carrying an "edited" marker - appending an
	// m.replace relation here would be wrong, because its m.new_content would
	// duplicate the body the client already has.
	if editTS := int64(md.GetEditTimestamp()); editTS > 0 {
		if ev.OriginTS > 0 {
			// Keep the marker after the message it belongs to.
			if editTS < ev.OriginTS {
				editTS = ev.OriginTS
			}
		}
		ev.EditedTS = editTS
	}
	room.AppendEvent(ev, s.Cfg.Storage.MaxEventsPerRoom)
	s.recordMemberLocked(acc, room, pm)
	return true
}

// finishBackfill refreshes the watermark without inserting anything.
func (s *Server) finishBackfill(acc *model.Account, room *model.Room, _ int) {
	s.Store.Mu.Lock()
	room.Backfilled = true
	room.BackfillTS = time.Now().UnixMilli()
	newest := room.NewestPos()
	s.Store.Mu.Unlock()
	s.setBackfilledPos(acc, room, newest)
}

func (s *Server) setBackfilledPos(acc *model.Account, room *model.Room, pos int64) {
	if pos <= 0 {
		return
	}
	s.Store.Mu.Lock()
	if pos > room.BackfilledPos {
		room.BackfilledPos = pos
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
}

func (s *Server) markBackfilled(acc *model.Account, room *model.Room) {
	s.Store.Mu.Lock()
	room.Backfilled = true
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)
}

// ensureAccountRooms makes sure the conversation list has been imported.
func (s *Server) ensureAccountRooms(ctx context.Context, acc *model.Account) {
	s.Store.Mu.RLock()
	done := acc.ConversationsSynced
	s.Store.Mu.RUnlock()
	if done {
		return
	}
	deviceID := s.Cfg.Yunhu.DevicePrefix + "-" + acc.YunhuID
	if dev := acc.PrimaryDevice(); dev != nil && dev.YunhuDeviceID != "" {
		deviceID = dev.YunhuDeviceID
	}
	s.refreshAccount(ctx, acc, deviceID)
}
