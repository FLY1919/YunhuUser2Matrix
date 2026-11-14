package matrix

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
)

// sync implements GET /_matrix/client/{ver}/sync with long polling.
func (s *Server) sync(c *Ctx) {
	acc := c.Account
	sinceRaw := c.Query("since")
	initial := sinceRaw == ""
	since := parseSince(sinceRaw)

	timeout := s.Cfg.SyncTimeout()
	if v := c.Query("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeout = time.Duration(n) * time.Millisecond
		}
	}
	if timeout < 0 {
		timeout = 0
	}
	if timeout > time.Minute {
		timeout = time.Minute
	}

	limit := 30
	if f := s.filterFor(acc, c.Query("filter")); f != nil {
		if f.TimelineLimit > 0 {
			limit = f.TimelineLimit
		} else if f.Limit > 0 {
			limit = f.Limit
		}
	}

	// First sync after login: make sure the conversation list exists.
	s.Store.Mu.RLock()
	synced := acc.ConversationsSynced
	s.Store.Mu.RUnlock()
	if !synced {
		ctx := c.R.Context()
		done := make(chan struct{})
		s.safego("initial-import", func() {
			defer close(done)
			s.ensureAccountRooms(ctx, acc)
		})
		select {
		case <-done:
		case <-time.After(12 * time.Second):
			s.Log.Warn("initial conversation import timed out", "user", acc.UserID)
		case <-ctx.Done():
			return
		}
	}

	deadline := time.Now().Add(timeout)
	for {
		// Subscribe before building so no event can slip between the two.
		wake, cancel := s.Hub.Subscribe(acc.YunhuID)

		s.Store.Mu.RLock()
		resp, hasContent := s.buildSync(acc, c.Device, since, initial, limit)
		s.Store.Mu.RUnlock()

		if hasContent || timeout == 0 {
			cancel()
			writeJSON(c.W, http.StatusOK, resp)
			return
		}

		wait := time.Until(deadline)
		if wait <= 0 {
			cancel()
			writeJSON(c.W, http.StatusOK, resp)
			return
		}
		select {
		case <-wake:
			cancel()
		case <-time.After(wait):
			cancel()
			writeJSON(c.W, http.StatusOK, resp)
			return
		case <-c.R.Context().Done():
			cancel()
			return
		}
	}
}

func parseSince(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.TrimPrefix(s, "s")
	if i := strings.IndexAny(s, "-_"); i > 0 {
		s = s[:i]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func (s *Server) filterFor(acc *model.Account, filterParam string) *model.Filter {
	if filterParam == "" {
		return nil
	}
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	return acc.Filters[filterParam]
}

// buildSync assembles a /sync response. The caller must hold the store lock.
func (s *Server) buildSync(acc *model.Account, dev *model.Device, since int64, initial bool, limit int) (map[string]any, bool) {
	if limit <= 0 {
		limit = 30
	}
	join := map[string]any{}
	hasContent := false
	now := time.Now().UnixMilli()

	rooms := acc.SortedRooms()
	for _, room := range rooms {
		if room.Membership != model.MembershipJoin {
			continue
		}

		timeline := []map[string]any{}
		state := []map[string]any{}
		firstPos := int64(0)

		limited := false
		if initial {
			start := len(room.Events) - limit
			if start < 0 {
				start = 0
			} else if start > 0 {
				limited = true
			}
			for _, ev := range room.Events[start:] {
				if ev.IsState() {
					continue
				}
				timeline = append(timeline, s.eventJSON(ev, ""))
				if firstPos == 0 || ev.Pos < firstPos {
					firstPos = ev.Pos
				}
			}
			for _, ev := range room.State {
				state = append(state, s.eventJSON(ev, ""))
			}
		} else {
			for _, ev := range room.Events {
				if ev.Pos <= since {
					continue
				}
				if ev.IsState() {
					state = append(state, s.eventJSON(ev, ""))
					continue
				}
				timeline = append(timeline, s.eventJSON(ev, ""))
				if firstPos == 0 || ev.Pos < firstPos {
					firstPos = ev.Pos
				}
			}
		}

		// A room whose only change is its member count still has to be sent,
		// otherwise the client keeps rendering the old header number forever.
		summaryChanged := !initial && room.MemberCountPos > since
		// A read marker advanced since the last poll is a change the client has
		// to see: it is what removes the badge and redraws the unread divider.
		// Without this the room is skipped (no new events, no new state) and the
		// count stays on screen until something else happens in the room.
		//
		// ReadMarkerChangePos, not ReadMarkerPos: the marked position always
		// names a past event and is therefore never greater than `since`, so
		// comparing that one would make this branch dead code.
		readChanged := !initial && room.ReadMarkerChangePos > since
		if len(timeline) == 0 && len(state) == 0 && !summaryChanged && !readChanged {
			continue
		}
		hasContent = true

		// Typing notifications are ephemeral.
		ephemeral := []map[string]any{}
		typingUsers := make([]string, 0, len(room.Typing))
		for uid, until := range room.Typing {
			if until > now {
				typingUsers = append(typingUsers, uid)
			}
		}
		if len(typingUsers) > 0 {
			sort.Strings(typingUsers)
			ephemeral = append(ephemeral, map[string]any{
				"type": "m.typing",
				"content": map[string]any{
					"user_ids": typingUsers,
				},
			})
		}
		// The read receipt echoes back as an m.receipt ephemeral event.
		//
		// A client that marks a room read also needs to *see* that position come
		// back: that is what moves the unread divider and, on a second device,
		// clears the badge. Without it the receipt endpoint was a black hole -
		// the client said "read up to here", the server remembered it, and the
		// client never heard about it again.
		if readChanged || initial {
			if rc := readReceiptEvent(room, acc.UserID); rc != nil {
				ephemeral = append(ephemeral, rc)
			}
		}

		prevBatch := "s0"
		if firstPos > 0 {
			prevBatch = "s" + strconv.FormatInt(firstPos-1, 10)
		}

		// Unread is derived from the read marker, never a raw counter: the
		// counter used to come back on every reconnect because Yunhu's
		// conversation list still reported the old number.
		unread, highlight := room.UnreadFor(acc.UserID)
		roomChanged := len(timeline) > 0 || len(state) > 0 || summaryChanged || readChanged

		join[room.RoomID] = map[string]any{
			"timeline": map[string]any{
				"events":     timeline,
				"limited":    limited,
				"prev_batch": prevBatch,
			},
			"state":        map[string]any{"events": state},
			"ephemeral":    map[string]any{"events": ephemeral},
			"account_data": map[string]any{"events": roomAccountData(room, roomChanged)},
			"unread_notifications": map[string]any{
				"notification_count": unread,
				"highlight_count":    highlight,
			},
			"summary": map[string]any{
				"m.joined_member_count":  room.JoinedCount(),
				"m.invited_member_count": 0,
				"m.heroes":               []string{},
			},
		}
	}

	// Account data carries secret storage, and the device list delta is what
	// makes a client re-query /keys/query after it uploads new keys. Without
	// either, an E2EE client can never leave the identity-confirmation step.
	accountData := accountDataEvents(acc, since, initial)
	deviceChanged := deviceListChanged(acc, since)
	// To-device messages carry session verification. Delivering them is what
	// lets "verify with another device" actually finish. next_batch is a
	// required field of the to_device object, not an optional nicety: a
	// response without it is rejected by the client as malformed.
	toDevice := pendingToDevice(acc, dev)
	toDeviceBatch := toDeviceToken(acc, dev)
	devID := primaryDeviceID(acc)

	hasContent = hasContent || len(accountData) > 0 || len(deviceChanged) > 0 || len(toDevice) > 0
	// An initial sync is a snapshot, not a poll: it must answer immediately even
	// when the account is empty, or a new client sits on a spinner until the
	// long poll expires.
	if initial {
		hasContent = true
	}

	resp := map[string]any{
		"next_batch": "s" + strconv.FormatInt(acc.Pos, 10),
		"rooms": map[string]any{
			"join":   join,
			"leave":  map[string]any{},
			"invite": map[string]any{},
		},
		"presence":     map[string]any{"events": []any{}},
		"account_data": map[string]any{"events": accountData},
		"to_device": map[string]any{
			"events":     toDevice,
			"next_batch": toDeviceBatch,
		},
		"device_lists":                     map[string]any{"changed": deviceChanged, "left": []string{}},
		"device_one_time_keys_count":       oneTimeKeyCountsFor(acc, devID),
		"device_unused_fallback_key_types": fallbackAlgorithmsFor(acc, devID),
	}
	return resp, hasContent
}

// roomAccountData renders the per-room account data a client needs to draw the
// unread divider and to offer "jump to my first unread message".
//
// m.fully_read is the private read position, and without it a client has no
// anchor: it can only know "there are N unread", not *where* the unread region
// starts. That is why the room opened at the bottom of the timeline instead of
// at the first unread message. Sending it also lets the client clear its own
// badge without waiting for a server push.
//
// Only emitted when the room is actually being delivered (and, after the first
// sync, only when the room changed): resending it every poll would make /sync
// never go quiet.
func roomAccountData(room *model.Room, include bool) []map[string]any {
	if !include || room.ReadMarker == "" {
		return []map[string]any{}
	}
	return []map[string]any{{
		"type":    "m.fully_read",
		"content": map[string]any{"event_id": room.ReadMarker},
	}}
}

// primaryDeviceID returns the device whose key counts /sync reports.
func primaryDeviceID(acc *model.Account) string {
	if d := acc.PrimaryDevice(); d != nil {
		return d.ID
	}
	return ""
}

func oneTimeKeyCountsFor(acc *model.Account, deviceID string) map[string]int {
	if acc.E2EE == nil || deviceID == "" {
		return map[string]int{}
	}
	return oneTimeKeyCounts(acc.E2EE.OneTimeKeys[deviceID])
}

func fallbackAlgorithmsFor(acc *model.Account, deviceID string) []string {
	if acc.E2EE == nil || deviceID == "" {
		return []string{}
	}
	return fallbackAlgorithms(acc.E2EE.FallbackKeys[deviceID])
}

// heroes returns the other participants of a direct room, which clients use to
// name conversations that have no explicit room name.
func heroes(room *model.Room, self string) []string {
	out := []string{}
	if !room.IsDirect {
		return out
	}
	for uid := range room.Members {
		if uid != self {
			out = append(out, uid)
		}
	}
	sort.Strings(out)
	return out
}

// readReceiptEvent renders the account's read position as an m.receipt ephemeral
// event.
//
// The shape is the one clients parse:
//
//	{"type":"m.receipt","content":{"<event_id>":{"m.read":{"<user_id>":{"ts":…}}}}}
//
// It is omitted when the room has no read position yet: an empty receipt would
// tell the client "read up to nowhere", which is worse than saying nothing.
func readReceiptEvent(room *model.Room, userID string) map[string]any {
	if room.ReadMarker == "" || userID == "" {
		return nil
	}
	ts := room.ReadMarkerTS
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	return map[string]any{
		"type": "m.receipt",
		"content": map[string]any{
			room.ReadMarker: map[string]any{
				"m.read": map[string]any{
					userID: map[string]any{"ts": ts},
				},
			},
		},
	}
}

// eventJSON renders an event in client-server format.
func (s *Server) eventJSON(ev *model.Event, roomID string) map[string]any {
	out := map[string]any{
		"event_id":         ev.EventID,
		"type":             ev.Type,
		"sender":           ev.Sender,
		"content":          ev.Content,
		"origin_server_ts": ev.OriginTS,
	}
	if ev.StateKey != nil {
		out["state_key"] = *ev.StateKey
	}
	// Room version 10 carries the redaction target at the top level (that is
	// the version capabilities advertises); newer clients additionally read
	// the m.relates_to relation, which redactEvent fills in.
	if ev.Redacts != "" {
		out["redacts"] = ev.Redacts
	}
	if ev.Unsigned != nil {
		out["unsigned"] = ev.Unsigned
	}
	// A recalled message must never reach the client as an empty bubble. The
	// content is already wiped; adding unsigned.redacted_because is what makes
	// every client (including one that paginates into history long after the
	// recall happened) render the standard "message deleted" placeholder.
	if ev.Recalled() {
		out["content"] = map[string]any{}
		unsigned := map[string]any{}
		for k, v := range ev.Unsigned {
			unsigned[k] = v
		}
		redactionID := ev.RecalledBy
		if redactionID == "" {
			// History imported from Yunhu already carries delete_timestamp, so
			// the message was recalled before we ever saw it live and there is
			// no redaction event to point at. A stable synthetic id keeps the
			// placeholder rendering deterministic across requests.
			redactionID = "$recalled_" + sanitize(ev.EventID)
		}
		unsigned["redacted_because"] = map[string]any{
			"event_id":         redactionID,
			"type":             "m.room.redaction",
			"sender":           ev.Sender,
			"content":          map[string]any{},
			"redacts":          ev.EventID,
			"origin_server_ts": ev.OriginTS,
		}
		out["unsigned"] = unsigned
	}
	if roomID != "" {
		out["room_id"] = roomID
	}
	// An edited message keeps its original event and advertises the edit through
	// unsigned.m.relations. That aggregate is what a client reads to draw the
	// "edited" marker; the m.replace event itself is fetched from /relations.
	if ev.EditedTS > 0 && !ev.Recalled() {
		unsigned := map[string]any{}
		if ev.Unsigned != nil {
			for k, v := range ev.Unsigned {
				unsigned[k] = v
			}
		}
		unsigned["m.relations"] = map[string]any{
			"m.replace": map[string]any{
				"event_id":         ev.EditEventID(),
				"origin_server_ts": ev.EditedTS,
				"sender":           ev.Sender,
			},
		}
		out["unsigned"] = unsigned
	}
	return out
}
