package matrix

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// The roster of a Yunhu group lives upstream, so /members has to ask for it.
// Caching keeps opening the member list from hitting the API every time, while
// still picking up joins and leaves within a few minutes.
const groupMemberTTL = 5 * time.Minute

// Page size and page cap for /v1/group/list-member. Yunhu pages are 1-based.
// The page size is a variable only so tests can shrink it and exercise the
// paging loop without generating hundreds of fixtures.
var groupMemberPageSize int32 = 100

const groupMemberMaxPages = 50

// Matrix power levels. Element's zh-CN labels are 100 = 管理员, 50 = 主持人,
// 0 = 默认, which is exactly how a Yunhu group's owner and admins should read.
const (
	powerLevelOwner   = 100
	powerLevelAdmin   = 50
	powerLevelDefault = 0
)

// matrixPowerLevel maps Yunhu's permission_level onto a Matrix power level.
//
//	Yunhu  100 = 群主      -> 100 (管理员)
//	Yunhu    2 = 管理员    ->  50 (主持人)
//	otherwise              ->   0 (默认)
func matrixPowerLevel(yunhuLevel int32) int {
	switch {
	case yunhuLevel >= 100:
		return powerLevelOwner
	case yunhuLevel >= 2:
		return powerLevelAdmin
	}
	return powerLevelDefault
}

// members implements GET /rooms/{roomId}/members.
//
// For a Yunhu group the local state only holds the people who spoke, so the
// member list used to be a handful of names. The authoritative roster comes
// from /v1/group/list-member; it is fetched on demand (Element requests exactly
// this endpoint when the member list is opened), cached for groupMemberTTL, and
// merged with whatever state we already have.
func (s *Server) members(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")

	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	needsFetch := room.ChatType == model.ChatTypeGroup && acc.Token != "" && s.rosterStaleLocked(room)
	s.Store.Mu.RUnlock()

	if needsFetch {
		s.refreshGroupMembers(c.R.Context(), acc, c.Device, roomID)
	}

	s.Store.Mu.RLock()
	room = acc.Rooms[roomID]
	chunk := s.memberChunkLocked(acc, room)
	s.Store.Mu.RUnlock()

	writeJSON(c.W, http.StatusOK, map[string]any{"chunk": chunk})
}

// joinedMembers implements GET /rooms/{roomId}/joined_members, which is the
// compact form of the same data.
func (s *Server) joinedMembers(c *Ctx) {
	acc := c.Account
	roomID := c.Param("roomId")

	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.RUnlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	needsFetch := room.ChatType == model.ChatTypeGroup && acc.Token != "" && s.rosterStaleLocked(room)
	s.Store.Mu.RUnlock()

	if needsFetch {
		s.refreshGroupMembers(c.R.Context(), acc, c.Device, roomID)
	}

	s.Store.Mu.RLock()
	room = acc.Rooms[roomID]
	joined := s.joinedMembersLocked(acc, room)
	s.Store.Mu.RUnlock()

	writeJSON(c.W, http.StatusOK, map[string]any{"joined": joined})
}

func (s *Server) rosterStaleLocked(room *model.Room) bool {
	if room.GroupMembersTS == 0 {
		return true
	}
	return time.Since(time.UnixMilli(room.GroupMembersTS)) > groupMemberTTL
}

// rosterFetchingLocked reports whether a fetch is already in flight.
func (s *Server) rosterFetchingLocked(room *model.Room) bool {
	return room.RosterFetching
}

// memberChunkLocked renders the member list as m.room.member events.
//
// State events come first (they carry the membership the client already knows
// about), then the rest of the roster is synthesised so the list is complete
// even for people who never said anything in the hydrated window.
func (s *Server) memberChunkLocked(acc *model.Account, room *model.Room) []map[string]any {
	chunk := make([]map[string]any, 0, len(room.State)+len(room.GroupMembers))
	seen := make(map[string]struct{}, len(room.State)+len(room.GroupMembers))

	// Deterministic order: room.State is a map, and a member list that shuffles
	// on every request is confusing to paginate through in the client.
	stateEvents := make([]*model.Event, 0, len(room.State))
	for _, ev := range room.State {
		if ev.Type == "m.room.member" {
			stateEvents = append(stateEvents, ev)
		}
	}
	sort.Slice(stateEvents, func(i, j int) bool {
		return stateEvents[i].StateKeyString() < stateEvents[j].StateKeyString()
	})
	for _, ev := range stateEvents {
		chunk = append(chunk, s.eventJSON(ev, ""))
		seen[ev.StateKeyString()] = struct{}{}
	}

	for _, gm := range room.GroupMembers {
		if gm == nil || gm.UserID == "" {
			continue
		}
		uid := s.mxUserID(gm.UserID, model.ChatTypeUser)
		if _, dup := seen[uid]; dup {
			continue
		}
		seen[uid] = struct{}{}
		chunk = append(chunk, s.syntheticMemberJSON(room, uid, gm.DisplayName, gm.AvatarURL))
	}
	return chunk
}

// joinedMembersLocked is the same data in the /joined_members shape.
func (s *Server) joinedMembersLocked(acc *model.Account, room *model.Room) map[string]any {
	joined := map[string]any{}

	if me := room.State[model.StateID("m.room.member", acc.UserID)]; me != nil {
		joined[acc.UserID] = map[string]any{
			"display_name": asString(me.Content["displayname"]),
			"avatar_url":   s.Media.RemoteMXC(asString(me.Content["avatar_url"])),
		}
	}
	for uid, m := range room.Members {
		if m.Membership != model.MembershipJoin {
			continue
		}
		joined[uid] = map[string]any{
			"display_name": m.DisplayName,
			"avatar_url":   s.Media.RemoteMXC(m.AvatarURL),
		}
	}
	for _, gm := range room.GroupMembers {
		if gm == nil || gm.UserID == "" {
			continue
		}
		uid := s.mxUserID(gm.UserID, model.ChatTypeUser)
		if _, exists := joined[uid]; exists {
			continue
		}
		joined[uid] = map[string]any{
			"display_name": gm.DisplayName,
			"avatar_url":   s.Media.RemoteMXC(gm.AvatarURL),
		}
	}
	return joined
}

// syntheticMemberJSON builds an m.room.member event for a roster entry that has
// never been seen in the timeline.
func (s *Server) syntheticMemberJSON(room *model.Room, uid, displayName, avatar string) map[string]any {
	return map[string]any{
		"event_id":  fmt.Sprintf("$member_%s_%s", sanitize(room.RoomID), sanitize(uid)),
		"type":      "m.room.member",
		"sender":    uid,
		"state_key": uid,
		"content": map[string]any{
			"membership":  model.MembershipJoin,
			"displayname": displayName,
			"avatar_url":  s.Media.RemoteMXC(avatar),
		},
		"origin_server_ts": room.CreatedTS,
	}
}

// refreshGroupMembers pulls the full roster for a group room and stores it,
// then republishes m.room.power_levels so clients can badge the owner and the
// admins. Errors are logged and swallowed: an empty member list is a much
// better answer than a 502 on a client that is just opening a side panel.
func (s *Server) refreshGroupMembers(ctx context.Context, acc *model.Account, dev *model.Device, roomID string) {
	s.Store.Mu.RLock()
	room := acc.Rooms[roomID]
	if room == nil || room.ChatType != model.ChatTypeGroup {
		s.Store.Mu.RUnlock()
		return
	}
	if room.RosterFetching {
		// A fetch is already running. Returning immediately is what keeps a client
		// that opens the member list from stacking requests: /members is
		// synchronous, so without this every concurrent request would start its own
		// upstream page-through.
		s.Store.Mu.RUnlock()
		return
	}
	chatID := room.ChatID
	s.Store.Mu.RUnlock()

	// Claim the fetch before releasing the lock, so two requests cannot both
	// start one.
	s.Store.Mu.Lock()
	room = acc.Rooms[roomID]
	if room == nil || room.RosterFetching {
		s.Store.Mu.Unlock()
		return
	}
	room.RosterFetching = true
	s.Store.Mu.Unlock()

	defer func() {
		s.Store.Mu.Lock()
		if r := acc.Rooms[roomID]; r != nil {
			r.RosterFetching = false
		}
		s.Store.Mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	yc := s.yunhuFor(acc, dev)
	members, err := s.fetchGroupMembers(ctx, yc, chatID)
	if err != nil {
		s.Log.Debug("group member list failed", "room", roomID, "err", err)
		// Stamp the attempt anyway. Without this the TTL never starts, so a group
		// whose roster cannot be fetched is retried on *every* /members call - which
		// is exactly the runaway request loop a client opening the invite picker
		// produces.
		s.Store.Mu.Lock()
		if r := acc.Rooms[roomID]; r != nil {
			r.GroupMembersTS = time.Now().UnixMilli()
		}
		s.Store.Mu.Unlock()
		return
	}

	s.Store.Mu.Lock()
	room = acc.Rooms[roomID]
	if room == nil {
		s.Store.Mu.Unlock()
		return
	}
	room.GroupMembers = members
	room.GroupMembersTS = time.Now().UnixMilli()
	// The roster is also the only reliable source for the participant count, so
	// use it when group/info has not answered yet.
	if room.MemberCount == 0 && len(members) > 0 {
		room.MemberCount = len(members)
		room.MemberCountPos = acc.NextPos()
	}
	changed := s.syncGroupPowerLevelsLocked(acc, room)
	s.Store.Mu.Unlock()

	if changed {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
}

// fetchGroupMembers pages through /v1/group/list-member until every member is
// in hand.
func (s *Server) fetchGroupMembers(ctx context.Context, yc *yunhu.Client, groupID string) ([]*model.GroupMember, error) {
	out := make([]*model.GroupMember, 0, 64)
	seen := make(map[string]struct{}, 64)

	for page := int32(1); page <= groupMemberMaxPages; page++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		resp, err := yc.ListMembers(ctx, groupID, "", page, groupMemberPageSize)
		if err != nil {
			return nil, err
		}
		users := resp.GetUser()
		for _, u := range users {
			info := u.GetUserInfo()
			if info == nil || info.GetUserId() == "" {
				continue
			}
			if _, dup := seen[info.GetUserId()]; dup {
				continue
			}
			seen[info.GetUserId()] = struct{}{}
			out = append(out, &model.GroupMember{
				UserID:      info.GetUserId(),
				DisplayName: info.GetName(),
				AvatarURL:   info.GetAvatarUrl(),
				Level:       u.GetPermissionLevel(),
			})
		}
		// A short page is the last page; total is a hint, not a contract (it
		// counts filtered-out entries too).
		if int32(len(users)) < groupMemberPageSize {
			break
		}
		if total := int(resp.GetTotal()); total > 0 && len(out) >= total {
			break
		}
	}
	return out, nil
}

// syncGroupPowerLevelsLocked republishes m.room.power_levels from the roles
// Yunhu reports. It reports whether anything changed, so a refresh that finds
// the same people does not push a pointless state event.
//
// Three sources, in decreasing authority:
//
//  1. /v1/group/info — owner (群主), the admin list (管理员) and the account's
//     own permission_level. One request per group, already made for the
//     headcount.
//  2. the roster from /v1/group/list-member — per-member permission_level.
//  3. nothing at all — then nobody is badged, including the account itself.
//
// Point 3 matters: the old code gave the account power 100 whenever the roster
// did not mention it, so a group whose member list is hidden (or which simply
// had not been opened yet) rendered the account as the group's owner.
func (s *Server) syncGroupPowerLevelsLocked(acc *model.Account, room *model.Room) bool {
	if room.ChatType != model.ChatTypeGroup {
		// DMs and bot chats are owned by the account itself; ensureRoomLocked
		// already seeded that and there is nothing upstream to ask.
		return false
	}

	users := map[string]any{}
	raise := func(uid string, level int) {
		if uid == "" || level <= powerLevelDefault {
			return
		}
		if prev, ok := users[uid].(int); ok && prev >= level {
			return
		}
		users[uid] = level
	}

	// 1. group/info: owner and admins.
	if room.RolesTS > 0 && room.OwnerID != "" {
		raise(s.mxUserID(room.OwnerID, model.ChatTypeUser), powerLevelOwner)
	}
	if room.RolesTS > 0 {
		for _, id := range room.AdminIDs {
			raise(s.mxUserID(id, model.ChatTypeUser), powerLevelAdmin)
		}
	}

	// 2. the roster, which knows every member's level.
	accountLevel := -1
	for _, gm := range room.GroupMembers {
		if gm == nil || gm.UserID == "" {
			continue
		}
		uid := s.mxUserID(gm.UserID, model.ChatTypeUser)
		level := matrixPowerLevel(gm.Level)
		if uid == acc.UserID {
			accountLevel = level
		}
		raise(uid, level)
	}

	// The account's own level: its real role, or nothing. Never a fabricated
	// 100 — that is exactly what made every conversation claim the account was
	// its creator.
	own := powerLevelDefault
	switch {
	case room.RolesTS > 0:
		own = matrixPowerLevel(room.MyLevel)
	case accountLevel >= 0:
		own = accountLevel
	}
	if own > powerLevelDefault {
		users[acc.UserID] = own
	} else {
		delete(users, acc.UserID)
	}

	if cur := room.State[model.StateID("m.room.power_levels", "")]; cur != nil {
		if samePowerUsers(cur.Content["users"], users) {
			return false
		}
	}

	key := ""
	pos := acc.NextPos()
	room.AppendEvent(&model.Event{
		EventID:  fmt.Sprintf("$state_%s_%d", sanitize(room.RoomID), pos),
		Type:     "m.room.power_levels",
		Sender:   acc.UserID,
		StateKey: &key,
		Content: map[string]any{
			"users":          users,
			"users_default":  powerLevelDefault,
			"events_default": powerLevelDefault,
			"state_default":  50,
			"ban":            50,
			"kick":           50,
			"redact":         50,
			"invite":         0,
		},
		OriginTS: time.Now().UnixMilli(),
		Pos:      pos,
	}, 0)
	return true
}

// applyGroupRolesLocked stores the roles group/info reported and republishes the
// power levels. The caller must hold the store lock.
func (s *Server) applyGroupRolesLocked(acc *model.Account, room *model.Room, data *pb.GroupData) bool {
	if data == nil || room == nil {
		return false
	}
	owner := data.GetOwner()
	if owner == "" {
		// `owner` is the current 群主 (it can be transferred), `create_by` the
		// original creator. Either is the room's authority when only one is set.
		owner = data.GetCreateBy()
	}
	if owner == "" && data.GetPermissonLevel() >= 100 {
		owner = acc.YunhuID
	}

	room.OwnerID = owner
	room.AdminIDs = data.GetAdmin()
	room.MyLevel = data.GetPermissonLevel()
	if owner != "" && owner == acc.YunhuID {
		// Defensive: some payloads omit permisson_level for the owner.
		room.MyLevel = 100
	}
	room.RolesTS = time.Now().UnixMilli()

	return s.syncGroupPowerLevelsLocked(acc, room)
}

// samePowerUsers compares a stored users map with a freshly computed one.
func samePowerUsers(raw any, want map[string]any) bool {
	got, ok := raw.(map[string]any)
	if !ok || len(got) != len(want) {
		return false
	}
	for uid, level := range want {
		if asInt(got[uid]) != asInt(level) {
			return false
		}
	}
	return true
}
