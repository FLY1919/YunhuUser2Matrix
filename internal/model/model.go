// Package model contains the persistent data structures shared by the Matrix
// frontend and the Yunhu backend.
package model

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// Chat types used by Yunhu.
const (
	ChatTypeUser  int32 = 1
	ChatTypeGroup int32 = 2
	ChatTypeBot   int32 = 3
)

// Membership states.
const (
	MembershipJoin   = "join"
	MembershipLeave  = "leave"
	MembershipInvite = "invite"
	MembershipBan    = "ban"
)

// Event is a Matrix room event. The same struct is used for timeline and state
// events; state events carry a non-nil StateKey.
type Event struct {
	EventID  string         `json:"event_id"`
	Type     string         `json:"type"`
	Sender   string         `json:"sender"`
	StateKey *string        `json:"state_key,omitempty"`
	Content  map[string]any `json:"content"`
	OriginTS int64          `json:"origin_server_ts"`
	Unsigned map[string]any `json:"unsigned,omitempty"`

	// Pos is the account-wide stream position, used as the /sync cursor.
	Pos int64 `json:"pos"`
	// YunhuMsgID links the event back to the upstream message, when known.
	YunhuMsgID string `json:"yunhu_msg_id,omitempty"`
	// Redacts is the event ID an m.room.redaction targets (room version < 11
	// keeps it at the top level, which is what capabilities advertises).
	Redacts string `json:"redacts,omitempty"`
	// RecalledBy is the event ID of the m.room.redaction that wiped this
	// event. It lets a client that joins later - and therefore never saw the
	// redaction itself - still render "message deleted" instead of an empty
	// bubble.
	RecalledBy string `json:"recalled_by,omitempty"`
	// EditedTS is when the upstream message behind this event was last edited.
	//
	// It is kept on the *original* event rather than on the edit event because
	// that is what a client renders: the bubble shows the newest content and an
	// "edited" marker, and the m.replace event that carries the new text is only
	// an implementation detail of how the edit was transmitted.
	EditedTS int64 `json:"edited_ts,omitempty"`
}

// EditEventID is the event id of the m.replace event that carries this event's
// newest content. Empty when the event was never edited.
//
// Deriving it instead of storing another field keeps the id stable across
// restarts: it is a pure function of the original event id, which the client
// already has.
func (e *Event) EditEventID() string {
	if e.EditedTS <= 0 || e.EventID == "" {
		return ""
	}
	return e.EventID + "_edit"
}

// IsState reports whether the event carries room state.
func (e *Event) IsState() bool { return e.StateKey != nil }

// Recalled reports whether the upstream message behind this event was recalled.
// The content has been wiped, so such events must not be rendered: clients show
// them as blank bubbles. Live clients learn about the recall from the
// m.room.redaction event instead.
func (e *Event) Recalled() bool {
	if e.Unsigned == nil {
		return false
	}
	_, ok := e.Unsigned["yunhu.recalled"]
	return ok
}

// StateKeyString returns the state key or "" for timeline events.
func (e *Event) StateKeyString() string {
	if e.StateKey == nil {
		return ""
	}
	return *e.StateKey
}

// MentionsUser reports whether the event mentions the given Matrix user ID.
func (e *Event) MentionsUser(userID string) bool {
	if userID == "" || e.Content == nil {
		return false
	}
	mentions, ok := e.Content["m.mentions"].(map[string]any)
	if !ok {
		return false
	}
	switch ids := mentions["user_ids"].(type) {
	case []any:
		for _, raw := range ids {
			if s, _ := raw.(string); s == userID {
				return true
			}
		}
	case []string:
		for _, s := range ids {
			if s == userID {
				return true
			}
		}
	}
	return false
}

// StateID builds the map key for a (type, state_key) pair.
func StateID(eventType, stateKey string) string {
	return eventType + "\x00" + stateKey
}

// ID returns the StateID of the event.
func (e *Event) ID() string { return StateID(e.Type, e.StateKeyString()) }

// Member describes a known participant of a room.
type Member struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	Membership  string `json:"membership"`
}

// GroupMember is one entry of a Yunhu group's authoritative member list
// (/v1/group/list-member).
type GroupMember struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
	// Level is Yunhu's raw permission_level: 100 = 群主, 2 = 管理员,
	// 0/absent = 普通成员.
	Level int32 `json:"level,omitempty"`
}

// Room is a virtual Matrix room mapped onto one Yunhu conversation.
type Room struct {
	RoomID   string `json:"room_id"`
	ChatID   string `json:"chat_id"`
	ChatType int32  `json:"chat_type"`

	Name      string `json:"name,omitempty"`
	Topic     string `json:"topic,omitempty"`
	AvatarURL string `json:"avatar_url,omitempty"`
	IsDirect  bool   `json:"is_direct,omitempty"`

	// TopicTS is when Topic was last confirmed upstream.
	//
	// It exists because "no description" is a legitimate state: plenty of groups
	// genuinely have an empty introduction. Without remembering that we asked,
	// every refresh would re-fetch the group just to find "" again, collapsing
	// the TTL for intro-less rooms to "every reconnect" - exactly what the
	// member-count and roles caches exist to avoid.
	TopicTS int64 `json:"topic_ts,omitempty"`

	// Muted reports whether the conversation is muted on Yunhu
	// (conversation list `do_not_disturb`, settable via /v1/friend/no-notify).
	//
	// Matrix has no per-room "mute" flag: it is expressed as the state of the
	// room's push rule (global/room/<room_id>), whose actions are [] when muted
	// and ["notify"] when not. Keeping the authoritative value here and deriving
	// the push rule from it means the two can never disagree - which is what
	// happened when the rule was hard-coded to "no such rule".
	Muted bool `json:"muted,omitempty"`
	// MutedTS is when Muted was last confirmed upstream.
	MutedTS int64 `json:"muted_ts,omitempty"`

	Membership string             `json:"membership"`
	Members    map[string]*Member `json:"members"`
	State      map[string]*Event  `json:"state"`
	Events     []*Event           `json:"events"`

	// Typing maps user ID to the unix-milli timestamp until which they type.
	Typing map[string]int64 `json:"typing,omitempty"`
	// ReadMarker is the event ID the user has read up to.
	ReadMarker string `json:"read_marker,omitempty"`
	// ReadMarkerTS is when that position was recorded, which the m.receipt
	// ephemeral event reports as its `ts`.
	ReadMarkerTS int64 `json:"read_marker_ts,omitempty"`
	// ReadMarkerPos is the stream position the user has fully read up to.
	//
	// Unread used to be a pair of counters bumped on ingest and zeroed on read,
	// which cannot work: every reconnect re-seeded them from Yunhu's
	// conversation list (which still said "37 unread" because the read never
	// reached upstream), so a room that had been opened still showed a badge,
	// and the badge for rooms that were never opened grew without bound.
	// Unread is now *derived* from this position, so it can only ever describe
	// what actually arrived after the last read.
	ReadMarkerPos int64 `json:"read_marker_pos,omitempty"`
	// ReadMarkerChangePos is the stream position of the last read-marker
	// update, as opposed to the position being marked.
	//
	// These are two different numbers and conflating them made the badge
	// unkillable: ReadMarkerPos is always <= the client's since token (it names
	// a past event), so "did the marker change since the last sync?" can never
	// be answered from it. This one advances on every update, which is what
	// tells /sync to re-send a room whose only change is "you read it".
	ReadMarkerChangePos int64 `json:"read_marker_change_pos,omitempty"`
	// Unread and Highlight are the seed counts for a room that has never been
	// opened. They come from Yunhu and are only used while ReadMarkerPos is 0;
	// after the first read the numbers are computed from the timeline.
	Unread    int `json:"unread,omitempty"`
	Highlight int `json:"highlight,omitempty"`

	// YunhuMsgSeq is the newest upstream message sequence seen for this chat.
	YunhuMsgSeq uint64 `json:"yunhu_msg_seq,omitempty"`
	// MemberCount is the authoritative participant count from Yunhu's
	// /v1/group/info (headcount). Rooms only ever know the members who spoke,
	// so counting room.Members gives a number that is far too small.
	MemberCount int `json:"member_count,omitempty"`
	// MemberCountTS is when MemberCount was last refreshed, so it is not fetched
	// on every reconnect.
	MemberCountTS int64 `json:"member_count_ts,omitempty"`
	// MemberCountPos is the stream position of the last MemberCount change.
	// /sync uses it to re-send a room whose only change is its member count,
	// which would otherwise never reach the client.
	MemberCountPos int64 `json:"member_count_pos,omitempty"`
	// GroupMembers is the cached roster fetched from /v1/group/list-member.
	//
	// Deliberately NOT persisted: the account file is already ~4 MB for 200
	// rooms, and a roster of a few hundred people per group would multiply that
	// and be rewritten on every change. Refetching after a restart costs one
	// request per room that the user actually opens.
	GroupMembers []*GroupMember `json:"-"`
	// GroupMembersTS is when GroupMembers was fetched.
	GroupMembersTS int64 `json:"-"`
	// RosterFetching marks a roster fetch that is currently running.
	//
	// /members is synchronous: it waits for the upstream page-through before
	// answering. Without this flag every concurrent request started its own fetch,
	// so a client that opens the invite picker (which hits /members repeatedly)
	// multiplied the upstream load by its own request count.
	RosterFetching bool `json:"-"`
	// OwnerID is the Yunhu ID of the group owner (群主), from /v1/group/info.
	OwnerID string `json:"owner_id,omitempty"`
	// AdminIDs are the group administrators (管理员) reported by group/info.
	AdminIDs []string `json:"admin_ids,omitempty"`
	// MyLevel is the account's own Yunhu permission_level in this group
	// (100 owner, 2 admin, 0 ordinary member).
	//
	// It exists because a Matrix room needs a power level for the logged-in
	// user, and guessing it is how every conversation ended up showing the
	// account as its "creator": the bridge creates the room, Yunhu does not.
	MyLevel int32 `json:"my_level,omitempty"`
	// RolesTS is when OwnerID/AdminIDs/MyLevel were last confirmed upstream.
	// Zero means "not known yet", which is different from "ordinary member".
	RolesTS int64 `json:"roles_ts,omitempty"`
	// BoardEvents are the Yunhu bot boards (群机器人看板) projected as events,
	// keyed by bot id. m.room.pinned_events points at all of them.
	//
	// A group can host several bots and each maintains its own board, so this is
	// a map rather than a single slot: the old single-BoardEvent model silently
	// dropped every board except the last one written.
	//
	// They are deliberately kept OUT of Events: a board is a pinned
	// announcement, not a chat message, and putting it in the timeline would
	// both pollute the conversation and let the retention cap evict the very
	// event the pin points at.
	BoardEvents map[string]*Event `json:"board_events,omitempty"`
	// BoardTS maps bot id to the upstream last_update_timestamp already
	// projected, so an unchanged board is not republished.
	BoardTS map[string]int64 `json:"board_ts,omitempty"`
	// BoardFetchedTS is when the boards were last requested, for the refresh TTL.
	BoardFetchedTS int64 `json:"board_fetched_ts,omitempty"`
	CreatedTS      int64 `json:"created_ts"`
	// LastActivityTS is used to sort rooms in the client.
	LastActivityTS int64 `json:"last_activity_ts"`
	// Backfilled marks rooms whose history has already been imported.
	Backfilled bool `json:"backfilled"`
	// BackfilledPos is the stream position of the newest event the last
	// backfill produced.
	//
	// The old code hydrated a room once and never looked again, so whatever
	// arrived while the process was offline (or while backfill failed and was
	// still marked done) stayed missing forever - the timeline simply had a hole
	// in the middle. Comparing this against the newest upstream message tells
	// the refresh path whether a gap exists, and only then does it download the
	// messages in between.
	BackfilledPos int64 `json:"backfilled_pos,omitempty"`
	// BackfillTS is when the room was last reconciled with upstream history.
	BackfillTS int64 `json:"backfill_ts,omitempty"`
}

// UnreadFor derives the unread counters from the read marker for the given
// Matrix user.
//
// Counting "messages after ReadMarkerPos" instead of incrementing a counter is
// what makes a badge disappear reliably: the counter used to be re-seeded from
// Yunhu's conversation list on every reconnect (upstream still says "37 unread"
// because dismissing a notification only clears the badge there, it does not
// rewrite history), so the badge came straight back after being cleared.
//
// A room that has never been opened (ReadMarkerPos == 0) falls back to the seed
// counts Yunhu reported, which is the only number available before the first
// read.
func (r *Room) UnreadFor(self string) (unread, highlight int) {
	if r.ReadMarkerPos <= 0 {
		return r.Unread, r.Highlight
	}
	for _, ev := range r.Events {
		if ev.IsState() || ev.Pos <= r.ReadMarkerPos || ev.Sender == self {
			continue
		}
		unread++
		if ev.MentionsUser(self) {
			highlight++
		}
	}
	return unread, highlight
}

// NewestPos returns the newest stream position the room holds.
func (r *Room) NewestPos() int64 {
	newest := int64(0)
	for _, ev := range r.Events {
		if ev.Pos > newest {
			newest = ev.Pos
		}
	}
	return newest
}

// NewestMessageTS returns the origin timestamp of the newest timeline message.
//
// State events are skipped: their timestamps are "when this homeserver first
// saw the room", not "when somebody last spoke", so including them would make a
// silent room look busy and hide a real gap.
func (r *Room) NewestMessageTS() int64 {
	newest := int64(0)
	for _, ev := range r.Events {
		if ev.IsState() {
			continue
		}
		if ev.OriginTS > newest {
			newest = ev.OriginTS
		}
	}
	return newest
}

// gapSlackMS is how much newer upstream has to be before we call it a gap.
//
// Yunhu timestamps are milliseconds everywhere, but a message can be stamped
// slightly before the one it followed (clock skew, server-side rounding), so an
// exact comparison would re-download the newest page forever. Two seconds is
// enough to absorb that without hiding a real hole: messages that belong to a
// gap are seconds to hours apart, not milliseconds.
const gapSlackMS = 2000

// GapAfter reports whether upstream holds messages newer than what is stored
// locally. upstreamNewestTS is the timestamp of the newest message the API
// returned for this conversation.
//
// This catches "the process was down while people kept talking". It cannot
// catch a hole in the middle of an otherwise current room - that is what
// GapBetween is for - so the caller checks both.
func (r *Room) GapAfter(upstreamNewestTS uint64) bool {
	if upstreamNewestTS == 0 {
		return false
	}
	local := r.NewestMessageTS()
	if local == 0 {
		return true
	}
	return int64(upstreamNewestTS)-local > gapSlackMS
}

// GapBetween reports whether the fetched page overlaps what we already have.
//
// pageNewestTS/pageOldestTS bound one upstream page. If our newest local message
// is older than the page's oldest message, the two ranges do not touch: the
// messages in between were never ingested and the timeline has a hole. Comparing
// only the newest timestamp is not enough - a room can be "up to date" at the
// bottom and still be missing everything in the middle.
func (r *Room) GapBetween(pageOldestTS, pageNewestTS uint64) bool {
	local := r.NewestMessageTS()
	if local == 0 {
		return true
	}
	return int64(pageOldestTS)-local > gapSlackMS
}

// GapSuspected reports whether the room is worth re-checking against upstream.
//
// A room is reconciled when it has never been hydrated, or when it has been
// more than backfillRecheckAfter since the last reconcile. The actual decision
// needs a network call (the newest upstream timestamp), which is why this is
// only a cheap local filter: it keeps a quiet 200-room account from issuing 200
// list-message calls on every websocket reconnect.
func (r *Room) GapSuspected() bool {
	if !r.Backfilled {
		return true
	}
	const recheckAfterMS = 10 * 60 * 1000
	if r.BackfillTS == 0 {
		return true
	}
	return time.Now().UnixMilli()-r.BackfillTS > recheckAfterMS
}

// BoardEventByID returns the projected board event of one bot.
func (r *Room) BoardEventByID(botID string) *Event {
	if r.BoardEvents == nil {
		return nil
	}
	return r.BoardEvents[botID]
}

// BoardEventList returns the projected boards in a stable order (by bot id), so
// the pinned list does not shuffle between requests.
func (r *Room) BoardEventList() []*Event {
	if len(r.BoardEvents) == 0 {
		return nil
	}
	ids := make([]string, 0, len(r.BoardEvents))
	for id := range r.BoardEvents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Event, 0, len(ids))
	for _, id := range ids {
		if ev := r.BoardEvents[id]; ev != nil {
			out = append(out, ev)
		}
	}
	return out
}

// JoinedCount is the number of participants to report to the client.
//
// Yunhu's /v1/group/info headcount is authoritative: a room only ever knows
// the members who actually spoke, so counting Room.Members under-reports a
// busy group by an order of magnitude. The local count is the fallback for
// conversations that have no headcount (DMs, bots) or before the first fetch.
func (r *Room) JoinedCount() int {
	if r.MemberCount > 0 {
		return r.MemberCount
	}
	n := len(r.Members)
	if r.Membership == MembershipJoin {
		n++
	}
	return n
}

// NewRoom allocates an empty room.
func NewRoom(roomID, chatID string, chatType int32) *Room {
	now := time.Now().UnixMilli()
	return &Room{
		RoomID:         roomID,
		ChatID:         chatID,
		ChatType:       chatType,
		Membership:     MembershipJoin,
		Members:        map[string]*Member{},
		State:          map[string]*Event{},
		Typing:         map[string]int64{},
		CreatedTS:      now,
		LastActivityTS: now,
	}
}

// AppendEvent adds an event to the timeline, enforcing the retention cap.
// State events are also indexed in the room state map.
func (r *Room) AppendEvent(ev *Event, maxEvents int) {
	if ev.IsState() {
		r.State[ev.ID()] = ev
	}
	r.Events = append(r.Events, ev)
	if maxEvents > 0 && len(r.Events) > maxEvents {
		drop := len(r.Events) - maxEvents
		// Never drop state events: they are needed for client bootstrap.
		kept := make([]*Event, 0, maxEvents)
		for _, e := range r.Events[drop:] {
			kept = append(kept, e)
		}
		r.Events = kept
	}
	if ev.OriginTS > r.LastActivityTS {
		r.LastActivityTS = ev.OriginTS
	}
}

// EventByEditID finds the event whose derived edit event has this id.
//
// The m.replace event is never stored: it is a pure function of the original
// event (id, content and EditedTS), so it can be synthesised on demand. That is
// what makes /rooms/{id}/event/{edit_id} resolvable after a client has been told
// a replacement exists through unsigned.m.relations.
func (r *Room) EventByEditID(eventID string) *Event {
	if eventID == "" {
		return nil
	}
	for i := len(r.Events) - 1; i >= 0; i-- {
		if r.Events[i].EditEventID() == eventID {
			return r.Events[i]
		}
	}
	return nil
}

// FindEvent looks up an event by ID in the timeline.
func (r *Room) FindEvent(eventID string) *Event {
	for i := len(r.Events) - 1; i >= 0; i-- {
		if r.Events[i].EventID == eventID {
			return r.Events[i]
		}
	}
	// Board events never enter the timeline, but /rooms/{id}/event/{eventId}
	// has to resolve them: that is how a client renders a pinned item it has
	// never seen in the timeline.
	for _, ev := range r.BoardEvents {
		if ev != nil && ev.EventID == eventID {
			return ev
		}
	}
	for _, ev := range r.State {
		if ev.EventID == eventID {
			return ev
		}
	}
	return nil
}

// EventByYunhuID finds a previously imported upstream message.
func (r *Room) EventByYunhuID(msgID string) *Event {
	if msgID == "" {
		return nil
	}
	for i := len(r.Events) - 1; i >= 0; i-- {
		if r.Events[i].YunhuMsgID == msgID {
			return r.Events[i]
		}
	}
	return nil
}

// Device is a logged-in Matrix session.
type Device struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name,omitempty"`

	// ToDevicePos is the stream position up to which this session has consumed
	// to-device messages. Session verification (SAS) runs entirely over that
	// channel, so it has to be per-device.
	ToDevicePos int64  `json:"to_device_pos,omitempty"`
	Token       string `json:"token"`
	LastSeen    int64  `json:"last_seen"`
	IP          string `json:"ip,omitempty"`
	UserAgent   string `json:"user_agent,omitempty"`
	// YunhuDeviceID is the device id reported to Yunhu for this session.
	YunhuDeviceID string `json:"yunhu_device_id,omitempty"`
	CreatedTS     int64  `json:"created_ts"`
}

// Filter is a stored /sync filter (only timeline/room limits are honoured).
type Filter struct {
	ID    string `json:"id"`
	Limit int    `json:"limit,omitempty"`
	// TimelineLimit overrides Limit for timeline events.
	TimelineLimit int            `json:"timeline_limit,omitempty"`
	Raw           map[string]any `json:"raw,omitempty"`
}

// AccountDataEntry is one stored global account-data event. Secret storage
// (m.secret_storage.*, m.cross_signing.*) lives here, which is why it has to be
// persisted instead of acknowledged and dropped.
type AccountDataEntry struct {
	Type    string          `json:"type"`
	Content json.RawMessage `json:"content"`
	// Pos is the stream position of the write, so sync can tell "new" from
	// "unchanged".
	Pos int64 `json:"pos"`
}

// E2EEState is the public half of a user's end-to-end encryption material.
//
// The homeserver never holds private keys. It stores what clients upload -
// device keys, one-time keys, cross-signing keys and signatures - and hands it
// back to other sessions of the same user, exactly as Synapse does. Without
// this, a client cannot even see its own device, which is what left Element X
// stuck on the "confirm your identity" screen, re-querying /keys/query every
// five seconds forever.
type E2EEState struct {
	// Devices maps device id to the uploaded device_keys object.
	Devices map[string]json.RawMessage `json:"devices,omitempty"`
	// OneTimeKeys maps device id -> key id -> key object.
	OneTimeKeys map[string]map[string]json.RawMessage `json:"one_time_keys,omitempty"`
	// FallbackKeys maps device id -> key id -> key object.
	FallbackKeys map[string]map[string]json.RawMessage `json:"fallback_keys,omitempty"`
	// CrossSigning keys, stored verbatim as uploaded.
	MasterKey      json.RawMessage `json:"master_key,omitempty"`
	SelfSigningKey json.RawMessage `json:"self_signing_key,omitempty"`
	UserSigningKey json.RawMessage `json:"user_signing_key,omitempty"`
	// AccountData holds global account data, including secret storage.
	AccountData map[string]*AccountDataEntry `json:"account_data,omitempty"`
	// DeviceListPos is the stream position at which the device list or the
	// cross-signing keys last changed, so sync can tell clients to re-query.
	DeviceListPos int64 `json:"device_list_pos,omitempty"`
	// BackupVersion is the current server-side key backup version, if any.
	// Resetting an identity creates one, so it has to exist.
	BackupVersion   string          `json:"backup_version,omitempty"`
	BackupAlgorithm json.RawMessage `json:"backup_algorithm,omitempty"`
	BackupAuthData  json.RawMessage `json:"backup_auth_data,omitempty"`
	// RoomKeys is the opaque key-backup payload.
	RoomKeys json.RawMessage `json:"room_keys,omitempty"`
}

// EnsureE2EE returns the encryption state, allocating it on first use.
// Accounts saved before this field existed load with a nil pointer.
func (a *Account) EnsureE2EE() *E2EEState {
	if a.E2EE == nil {
		a.E2EE = &E2EEState{}
	}
	if a.E2EE.Devices == nil {
		a.E2EE.Devices = map[string]json.RawMessage{}
	}
	if a.E2EE.OneTimeKeys == nil {
		a.E2EE.OneTimeKeys = map[string]map[string]json.RawMessage{}
	}
	if a.E2EE.FallbackKeys == nil {
		a.E2EE.FallbackKeys = map[string]map[string]json.RawMessage{}
	}
	if a.E2EE.AccountData == nil {
		a.E2EE.AccountData = map[string]*AccountDataEntry{}
	}
	return a.E2EE
}

// HasCrossSigning reports whether the user has uploaded cross-signing keys.
func (e *E2EEState) HasCrossSigning() bool {
	return e != nil && len(e.MasterKey) > 0
}

// ToDeviceEvent is a direct message between sessions, queued per target
// device. Cross-device session verification (m.key.verification.*) runs
// entirely over this channel, which is why an empty to_device section leaves
// "verify with another device" spinning forever.
type ToDeviceEvent struct {
	Pos      int64           `json:"pos"`
	Type     string          `json:"type"`
	Sender   string          `json:"sender"`
	Content  json.RawMessage `json:"content"`
	ToUser   string          `json:"to_user,omitempty"`
	ToDevice string          `json:"to_device,omitempty"` // "*" means every session
}

// Account is one Yunhu account, which is also one Matrix user.
type Account struct {
	YunhuID   string `json:"yunhu_id"`
	UserID    string `json:"user_id"`
	Token     string `json:"token"`
	Name      string `json:"name"`
	AvatarURL string `json:"avatar_url,omitempty"`
	Phone     string `json:"phone,omitempty"`
	Email     string `json:"email,omitempty"`
	IsVIP     bool   `json:"is_vip,omitempty"`

	Devices map[string]*Device `json:"devices"`
	Rooms   map[string]*Room   `json:"rooms"`
	Filters map[string]*Filter `json:"filters,omitempty"`

	// E2EE holds the public encryption material clients uploaded.
	E2EE *E2EEState `json:"e2ee,omitempty"`

	// ToDevice queues direct session-to-session messages (verification, key
	// requests). Bounded by TrimToDevice.
	ToDevice []*ToDeviceEvent `json:"to_device,omitempty"`

	// Pos is the account-wide monotonic stream position.
	Pos int64 `json:"pos"`
	// Txns maps Matrix transaction IDs to event IDs for idempotent sends.
	Txns map[string]string `json:"txns,omitempty"`

	CreatedTS    int64 `json:"created_ts"`
	LastLoginTS  int64 `json:"last_login_ts"`
	LastSyncedTS int64 `json:"last_synced_ts"`
	// ConversationsSynced marks whether the room list has been imported.
	ConversationsSynced bool `json:"conversations_synced"`
}

// NewAccount allocates an empty account.
func NewAccount(yunhuID, userID string) *Account {
	now := time.Now().UnixMilli()
	return &Account{
		YunhuID:   yunhuID,
		UserID:    userID,
		Devices:   map[string]*Device{},
		Rooms:     map[string]*Room{},
		Filters:   map[string]*Filter{},
		Txns:      map[string]string{},
		CreatedTS: now,
	}
}

// NextPos returns the next stream position and advances the counter.
func (a *Account) NextPos() int64 {
	a.Pos++
	return a.Pos
}

// RoomByChat finds the room mapped to a Yunhu conversation.
func (a *Account) RoomByChat(chatID string, chatType int32) *Room {
	for _, r := range a.Rooms {
		if r.ChatID == chatID && r.ChatType == chatType {
			return r
		}
	}
	return nil
}

// RoomByEventMsgID finds the room holding the event whose Yunhu message id is
// msgID, together with that event.
//
// It exists for edit and recall pushes that arrive without a chat id at all: the
// message id is unique per account, so the message itself is the best possible
// pointer, and Yunhu's own client resolves the target the same way (it scans its
// loaded list for the msg id before touching the conversation).
func (a *Account) RoomByEventMsgID(msgID string) (*Room, *Event) {
	if msgID == "" {
		return nil, nil
	}
	for _, r := range a.Rooms {
		if ev := r.EventByYunhuID(msgID); ev != nil {
			return r, ev
		}
	}
	return nil, nil
}

// RoomByChatID finds a room by conversation id alone, ignoring the chat type.
// Edit and recall pushes do not carry chat_type (the PushMessage comment says
// so explicitly: "编辑消息无此项", and blocked_message only has fields 1/3/4).
// Looking such a push up by (chatID, 0) always missed, so ensureRoomLocked
// minted a fresh chatType=0 room for every edit - the edit landed in a phantom
// room and the message the user was looking at never changed.
//
// A conversation id is unique across the three chat types in practice (a group
// id and a user id never collide), so the id alone is enough to find the room.
// When it somehow matches more than one, the group wins: groups are the only
// conversation that can host bots, and a phantom 1:1 room is the cheaper
// mistake to leave untouched.
func (a *Account) RoomByChatID(chatID string) *Room {
	var fallback *Room
	for _, r := range a.Rooms {
		if r.ChatID != chatID {
			continue
		}
		if r.ChatType == ChatTypeGroup {
			return r
		}
		if fallback == nil {
			fallback = r
		}
	}
	return fallback
}

// PrimaryDevice returns the most recently used device, or nil.
func (a *Account) PrimaryDevice() *Device {
	var best *Device
	for _, d := range a.Devices {
		if best == nil || d.LastSeen > best.LastSeen {
			best = d
		}
	}
	return best
}

// SortedRooms returns rooms ordered by most recent activity.
func (a *Account) SortedRooms() []*Room {
	out := make([]*Room, 0, len(a.Rooms))
	for _, r := range a.Rooms {
		out = append(out, r)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].LastActivityTS > out[j-1].LastActivityTS; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// TrimToDevice keeps the to-device queue bounded. Events are dropped oldest
// first, which only matters for a session that has been offline for a very long
// time.
func (a *Account) TrimToDevice(max int) {
	if max <= 0 || len(a.ToDevice) <= max {
		return
	}
	drop := len(a.ToDevice) - max
	kept := make([]*ToDeviceEvent, max)
	copy(kept, a.ToDevice[drop:])
	a.ToDevice = kept
}

// TrimTxns keeps the transaction cache bounded.
func (a *Account) TrimTxns(max int) {
	if max <= 0 || len(a.Txns) <= max {
		return
	}
	// Maps have no order; dropping arbitrary entries is acceptable because the
	// cache only exists to make retries idempotent within a short window.
	for k := range a.Txns {
		delete(a.Txns, k)
		if len(a.Txns) <= max {
			break
		}
	}
}

// LocalpartOf strips the @ and :server from a Matrix user ID.
func LocalpartOf(userID string) string {
	if i := strings.IndexByte(userID, ':'); i > 0 {
		return strings.TrimPrefix(userID[:i], "@")
	}
	return strings.TrimPrefix(userID, "@")
}
