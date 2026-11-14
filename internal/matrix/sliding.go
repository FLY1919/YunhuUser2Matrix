package matrix

// Sliding sync (MSC3575 "sync v3" and its successor MSC4186 "simplified
// sliding sync").
//
// Why this matters: modern clients (Element X, and Element Web behind a labs
// flag) prefer sliding sync over /sync. Historically it required running a
// separate proxy (matrix-org/sliding-sync) plus a Postgres database, advertised
// through /.well-known/matrix/client. This homeserver implements both dialects
// directly, so the proxy step disappears entirely.
//
// The two dialects differ in ways that matter:
//
//   MSC3575 (classic)              MSC4186 (simplified)
//   ---------------------------    ----------------------------
//   lists[key].ranges [[a,b]]      lists[key].range [a,b]
//   required_state [["type",""]]   required_state [{type,state_key}]
//   response lists[key].ops[]      response lists[key].count only
//   per-room notification_count    removed; deltas via initial/limited flags
//
// Both share the same room payload shape, so one builder serves both and only
// the list envelope differs.

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
)

const (
	// slidingMaxLists and slidingMaxRooms mirror the spec's 100-entry caps.
	slidingMaxLists = 100
	slidingMaxRooms = 100
	// slidingDefaultTimeline is used when a client omits timeline_limit.
	slidingDefaultTimeline = 20
	// slidingMaxTimeline bounds a single response. Events beyond it are not
	// dropped: the returned pos stops before them so the next call delivers
	// them, which keeps the stream lossless.
	slidingMaxTimeline = 200
	slidingBodyLimit   = 4 << 20

	// slidingConnTTL and slidingMaxConns bound the connection table so a
	// client that invents a new conn_id on every request cannot grow it
	// without limit.
	slidingConnTTL  = 24 * time.Hour
	slidingMaxConns = 512
)

// slidingConnState is the server-side memory of one sliding sync connection.
//
// Why it is needed: simplified sliding sync answers with deltas only. If the
// client ever ends up without a room's payload - it cleared its cache, the
// data directory was rebuilt, an earlier response was truncated, or the app
// was reinstalled - a pure delta stream answers `{"count":201,"rooms":{}}`
// forever. The client knows 201 rooms exist but can never render one, so it
// waits, the long poll expires, and the UI reports a dropped connection.
//
// Remembering which rooms were actually handed to each connection lets the
// server repair that state instead of trusting the pos token blindly: any
// room inside a requested window this connection has never seen is sent as a
// full initial payload again, while known rooms keep receiving plain deltas.
type slidingConnState struct {
	mu sync.Mutex
	// Pos is the last token handed to this connection.
	Pos int64
	// Delivered is the set of room ids whose full payload this connection has
	// received. Rooms missing from it are re-sent in full.
	Delivered map[string]struct{}
	// Updated drives expiry of idle connections.
	Updated time.Time
}

func newSlidingConnState() *slidingConnState {
	return &slidingConnState{Delivered: map[string]struct{}{}, Updated: time.Now()}
}

func (c *slidingConnState) has(roomID string) bool {
	_, ok := c.Delivered[roomID]
	return ok
}

func (c *slidingConnState) mark(roomID string) {
	c.Delivered[roomID] = struct{}{}
}

// slidingConnKey identifies a connection the way the spec does: one client,
// one device, one conn_id.
func slidingConnKey(userID, deviceID, connID string) string {
	return userID + "\x1f" + deviceID + "\x1f" + connID
}

// slidingConnLocked returns the state for a connection, creating it on first
// use. The caller must hold s.slidingMu.
func (s *Server) slidingConnLocked(key string) *slidingConnState {
	if s.slidingConns == nil {
		s.slidingConns = map[string]*slidingConnState{}
	}
	st, ok := s.slidingConns[key]
	if !ok {
		s.expireSlidingConnsLocked()
		st = newSlidingConnState()
		s.slidingConns[key] = st
	}
	return st
}

// expireSlidingConnsLocked drops idle connections and enforces the cap.
// The caller must hold s.slidingMu.
func (s *Server) expireSlidingConnsLocked() {
	now := time.Now()
	for k, c := range s.slidingConns {
		c.mu.Lock()
		idle := now.Sub(c.Updated)
		c.mu.Unlock()
		if idle > slidingConnTTL {
			delete(s.slidingConns, k)
		}
	}
	for len(s.slidingConns) >= slidingMaxConns {
		oldestKey := ""
		var oldest time.Time
		for k, c := range s.slidingConns {
			c.mu.Lock()
			ts := c.Updated
			c.mu.Unlock()
			if oldestKey == "" || ts.Before(oldest) {
				oldestKey, oldest = k, ts
			}
		}
		if oldestKey == "" {
			break
		}
		delete(s.slidingConns, oldestKey)
	}
}

// slidingStateReq is one entry of required_state. Both dialects accept a
// "match everything" entry: MSC4186 uses {}, MSC3575 uses ["*","*"].
type slidingStateReq struct {
	Type        string
	StateKey    *string
	LazyMembers bool
	// matchAll is set for {}, {"type":null} and ["*","*"].
	matchAll bool
	// selfOnly is set for the $ME state key, meaning "the requesting user".
	selfOnly bool
}

// slidingListConfig accepts both dialects' list shape.
type slidingListConfig struct {
	TimelineLimit   int
	RequiredState   []slidingStateReq
	Range           *[2]int
	Ranges          [][2]int
	Filters         map[string]any
	SlowGetAllRooms bool
}

// slidingSubscription is a room_subscriptions entry.
type slidingSubscription struct {
	TimelineLimit int
	RequiredState []slidingStateReq
}

type slidingRequest struct {
	ConnID            string
	Pos               string
	Timeout           *int
	SetPresence       string
	Lists             map[string]slidingListConfig
	RoomSubscriptions map[string]slidingSubscription
	Extensions        map[string]json.RawMessage
}

// --- request decoding -------------------------------------------------------

// wire types use RawMessage where the two dialects disagree.
type wireRequiredState []json.RawMessage

type wireList struct {
	TimelineLimit   int               `json:"timeline_limit"`
	RequiredState   wireRequiredState `json:"required_state"`
	Range           *[2]int           `json:"range"`
	Ranges          [][2]int          `json:"ranges"`
	Filters         map[string]any    `json:"filters"`
	SlowGetAllRooms bool              `json:"slow_get_all_rooms"`
}

type wireSub struct {
	TimelineLimit int               `json:"timeline_limit"`
	RequiredState wireRequiredState `json:"required_state"`
}

type wireRequest struct {
	ConnID            string                     `json:"conn_id"`
	Pos               string                     `json:"pos"`
	Timeout           *int                       `json:"timeout"`
	SetPresence       string                     `json:"set_presence"`
	Lists             map[string]wireList        `json:"lists"`
	RoomSubscriptions map[string]wireSub         `json:"room_subscriptions"`
	Extensions        map[string]json.RawMessage `json:"extensions"`
}

// applyStateKey interprets the state-key sentinels shared by both dialects.
// "" and "*" mean "any key"; $ME means the requesting user; $LAZY enables lazy
// member loading. Element X sends ["m.room.member","$LAZY"] and
// ["m.room.member","$ME"], which used to be matched as literal keys and so
// never matched anything.
func applyStateKey(req *slidingStateReq, key string) {
	switch key {
	case "*":
		// any state key
	case "$ME":
		req.selfOnly = true
	case "$LAZY":
		req.LazyMembers = true
	default:
		k := key
		req.StateKey = &k
	}
}

// parseRequiredState accepts either [{"type":..,"state_key":..}] or
// [["type","state_key"]].
func parseRequiredState(raw wireRequiredState) []slidingStateReq {
	out := make([]slidingStateReq, 0, len(raw))
	for _, item := range raw {
		trimmed := trimSpace(item)
		if len(trimmed) == 0 {
			continue
		}
		switch trimmed[0] {
		case '{':
			var obj struct {
				Type        *string `json:"type"`
				StateKey    *string `json:"state_key"`
				LazyMembers bool    `json:"lazy_members"`
			}
			if err := json.Unmarshal(item, &obj); err != nil {
				continue
			}
			req := slidingStateReq{LazyMembers: obj.LazyMembers}
			if obj.Type != nil {
				req.Type = *obj.Type
			}
			req.matchAll = req.Type == "" || req.Type == "*"
			if req.matchAll {
				req.Type = ""
			}
			if obj.StateKey != nil {
				applyStateKey(&req, *obj.StateKey)
			}
			out = append(out, req)
		case '[':
			var pair []string
			if err := json.Unmarshal(item, &pair); err != nil || len(pair) == 0 {
				continue
			}
			req := slidingStateReq{}
			if pair[0] != "*" {
				req.Type = pair[0]
			} else {
				req.matchAll = true
			}
			if len(pair) > 1 {
				applyStateKey(&req, pair[1])
			}
			out = append(out, req)
		}
	}
	return out
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	return b
}

func decodeSlidingRequest(r *http.Request) (*slidingRequest, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, slidingBodyLimit))
	if err != nil {
		return nil, err
	}
	var w wireRequest
	if len(trimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &w); err != nil {
			return nil, err
		}
	}

	req := &slidingRequest{
		ConnID:            w.ConnID,
		Pos:               w.Pos,
		Timeout:           w.Timeout,
		SetPresence:       w.SetPresence,
		Lists:             map[string]slidingListConfig{},
		RoomSubscriptions: map[string]slidingSubscription{},
		Extensions:        w.Extensions,
	}

	// Element X sends pos/timeout as query parameters, not in the body:
	//   POST .../sync?pos=s5667&timeout=30000
	//   {"conn_id":"encryption","extensions":{...}}
	// Reading only the body made every request look like an initial sync with a
	// zero timeout, so the server answered instantly and the client re-asked
	// immediately - a busy loop that looks like the app is frozen.
	// The body still wins when it carries a value.
	q := r.URL.Query()
	if req.Pos == "" {
		req.Pos = q.Get("pos")
	}
	if req.Timeout == nil {
		if v := q.Get("timeout"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				req.Timeout = &n
			}
		}
	}
	if req.SetPresence == "" {
		req.SetPresence = q.Get("set_presence")
	}
	if req.ConnID == "" {
		req.ConnID = q.Get("conn_id")
	}
	for key, l := range w.Lists {
		if len(req.Lists) >= slidingMaxLists {
			break
		}
		cfg := slidingListConfig{
			TimelineLimit:   l.TimelineLimit,
			RequiredState:   parseRequiredState(l.RequiredState),
			Range:           l.Range,
			Ranges:          l.Ranges,
			Filters:         l.Filters,
			SlowGetAllRooms: l.SlowGetAllRooms,
		}
		// Classic clients send ranges (plural); keep the first window.
		if cfg.Range == nil && len(cfg.Ranges) > 0 {
			r0 := cfg.Ranges[0]
			cfg.Range = &r0
		}
		req.Lists[key] = cfg
	}
	for roomID, sub := range w.RoomSubscriptions {
		if len(req.RoomSubscriptions) >= slidingMaxRooms {
			break
		}
		req.RoomSubscriptions[roomID] = slidingSubscription{
			TimelineLimit: sub.TimelineLimit,
			RequiredState: parseRequiredState(sub.RequiredState),
		}
	}
	return req, nil
}

// --- handlers ---------------------------------------------------------------

// slidingSync serves MSC4186 simplified sliding sync.
func (s *Server) slidingSync(c *Ctx) { s.handleSliding(c, false) }

// slidingSyncClassic serves MSC3575 sync v3, the dialect the standalone proxy
// implements.
func (s *Server) slidingSyncClassic(c *Ctx) { s.handleSliding(c, true) }

func (s *Server) handleSliding(c *Ctx, classic bool) {
	req, err := decodeSlidingRequest(c.R)
	if err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Malformed request body")
		return
	}
	if req.SetPresence != "" {
		switch req.SetPresence {
		case "online", "offline", "unavailable":
		default:
			writeError(c.W, http.StatusBadRequest, ErrInvalidParam, "Invalid set_presence value")
			return
		}
	}

	acc := c.Account
	initial := req.Pos == ""
	since := parseSince(req.Pos)

	// A sliding sync request is only debuggable if the connection, the token,
	// the timeout and the requested window are visible. Element X's behaviour
	// is driven entirely by these: it sends timeout=0 (no long poll) until the
	// list it asked for is fully loaded, so a missing `lists` or a count that
	// never converges turns into an invisible request storm.
	s.Log.Debug("sliding sync request",
		"user", acc.UserID,
		"conn", req.ConnID,
		"pos", req.Pos,
		"timeout", timeoutParam(req.Timeout),
		"lists", describeLists(req.Lists),
		"subs", len(req.RoomSubscriptions),
		"ext", extensionKeys(req.Extensions))

	deviceID := ""
	if c.Device != nil {
		deviceID = c.Device.ID
	}
	connKey := slidingConnKey(acc.UserID, deviceID, req.ConnID)

	s.slidingMu.Lock()
	conn := s.slidingConnLocked(connKey)
	s.slidingMu.Unlock()

	conn.mu.Lock()
	connPos := conn.Pos
	conn.mu.Unlock()

	// Match /sync: the first request after login may need the conversation
	// list imported before there is anything to return.
	s.Store.Mu.RLock()
	ready := acc.ConversationsSynced
	serverPos := acc.Pos
	s.Store.Mu.RUnlock()

	// Two tokens are untrustworthy: one ahead of our own stream, and one this
	// connection was never handed. Both mean the client is replaying state
	// from a previous run - a data dir reset, a re-created account, a server
	// restart with a fresh connection table - so answer as an initial sync
	// instead of with deltas the client has no base for.
	if !initial {
		switch {
		case since > serverPos:
			s.Log.Info("sliding sync token ahead of stream, resyncing",
				"user", acc.UserID, "pos", since, "server_pos", serverPos)
			initial, since = true, 0
		case connPos > 0 && since > connPos:
			s.Log.Info("sliding sync token unknown to this connection, resyncing",
				"user", acc.UserID, "conn", req.ConnID, "pos", since, "conn_pos", connPos)
			initial, since = true, 0
		}
	}
	if !ready {
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

	// timeout is ignored on an initial request per spec.
	timeout := time.Duration(0)
	if !initial && req.Timeout != nil {
		timeout = time.Duration(*req.Timeout) * time.Millisecond
	}
	if timeout < 0 {
		timeout = 0
	}
	if timeout > time.Minute {
		timeout = time.Minute
	}

	deadline := time.Now().Add(timeout)
	for {
		// Subscribe before building so nothing can slip in between.
		wake, cancel := s.Hub.Subscribe(acc.YunhuID)

		s.Store.Mu.RLock()
		resp, hasContent := s.buildSliding(acc, c.Device, req, since, initial, classic, conn)
		s.Store.Mu.RUnlock()

		if hasContent || timeout == 0 {
			cancel()
			s.logSlidingReply(c, req, resp, hasContent, initial, timeout)
			writeJSON(c.W, http.StatusOK, resp)
			return
		}

		wait := time.Until(deadline)
		if wait <= 0 {
			cancel()
			s.logSlidingReply(c, req, resp, hasContent, initial, timeout)
			writeJSON(c.W, http.StatusOK, resp)
			return
		}
		select {
		case <-wake:
			cancel()
		case <-time.After(wait):
			cancel()
			s.logSlidingReply(c, req, resp, hasContent, initial, timeout)
			writeJSON(c.W, http.StatusOK, resp)
			return
		case <-c.R.Context().Done():
			cancel()
			// Nothing is written: the client hung up. Worth a line of its own,
			// otherwise it looks like an instant 200 in the access log.
			s.Log.Debug("sliding sync abandoned by client",
				"conn", req.ConnID, "pos", req.Pos, "waited_ms", timeout.Milliseconds())
			return
		}
	}
}

// timeoutParam renders the client's timeout for the debug log. A nil timeout
// and an explicit 0 mean different things to a client but look identical on the
// wire once the server applies its defaults, so keep them apart.
func timeoutParam(t *int) any {
	if t == nil {
		return "absent"
	}
	return *t
}

// describeLists renders the requested windows, e.g. "all_rooms=[0,19]".
// matrix-rust-sdk sends no lists at all for its encryption-only connection,
// which is the difference between "the client is loading a room list" and "the
// client is only pumping to-device events".
func describeLists(lists map[string]slidingListConfig) string {
	if len(lists) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(lists))
	for k := range lists {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		cfg := lists[k]
		win := "all"
		if cfg.Range != nil {
			win = strconv.Itoa(cfg.Range[0]) + "," + strconv.Itoa(cfg.Range[1])
		} else if len(cfg.Ranges) > 0 {
			win = strconv.Itoa(cfg.Ranges[0][0]) + "," + strconv.Itoa(cfg.Ranges[0][1])
		}
		parts = append(parts, k+"=["+win+"]")
	}
	return strings.Join(parts, " ")
}

func extensionKeys(ext map[string]json.RawMessage) string {
	if len(ext) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(ext))
	for k := range ext {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// logSlidingReply records what was handed to the client. `rooms` and `count`
// together decide whether the client considers its room list fully loaded: it
// keeps re-asking with timeout=0 until the count it was given is covered by the
// window it requested.
func (s *Server) logSlidingReply(c *Ctx, req *slidingRequest, resp map[string]any, hasContent, initial bool, timeout time.Duration) {
	if !s.Log.Enabled(c.R.Context(), slog.LevelDebug) {
		return
	}
	rooms, _ := resp["rooms"].(map[string]any)
	lists := ""
	if l, ok := resp["lists"].(map[string]any); ok {
		keys := make([]string, 0, len(l))
		for k := range l {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			if entry, ok := l[k].(map[string]any); ok {
				parts = append(parts, k+"="+strconv.Itoa(toInt(entry["count"])))
			}
		}
		lists = strings.Join(parts, " ")
		if lists == "" {
			lists = "none"
		}
	}
	s.Log.Debug("sliding sync reply",
		"conn", req.ConnID,
		"pos", resp["pos"],
		"initial", initial,
		"timeout_ms", timeout.Milliseconds(),
		"rooms", len(rooms),
		"lists", lists,
		"content", hasContent)
}

func toInt(v any) int {
	if n, ok := v.(int); ok {
		return n
	}
	return 0
}

// --- response building ------------------------------------------------------

// roomPlan records how a room was reached and with what limits.
type roomPlan struct {
	room          *model.Room
	lists         []string
	timelineLimit int
	requiredState []slidingStateReq
	subscribed    bool
}

// buildSliding assembles a sliding sync response. The caller must hold the
// store lock. conn may be nil (tests), in which case only the pos token drives
// delta calculation and no per-connection memory is kept.
func (s *Server) buildSliding(acc *model.Account, dev *model.Device, req *slidingRequest, since int64, initial, classic bool, conn *slidingConnState) (map[string]any, bool) {
	if conn != nil {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		conn.Updated = time.Now()
		if initial {
			// A client starting from scratch holds no state, so nothing we
			// remembered about it is worth keeping.
			conn.Delivered = map[string]struct{}{}
		}
	}

	plans := map[string]*roomPlan{}
	order := []string{} // insertion order for determinism

	addPlan := func(room *model.Room, listKey string, limit int, state []slidingStateReq) *roomPlan {
		p, ok := plans[room.RoomID]
		if !ok {
			p = &roomPlan{room: room, timelineLimit: slidingDefaultTimeline}
			plans[room.RoomID] = p
			order = append(order, room.RoomID)
		}
		if limit > p.timelineLimit {
			p.timelineLimit = limit
		}
		if len(state) > 0 && len(p.requiredState) == 0 {
			p.requiredState = state
		}
		if listKey != "" {
			p.lists = append(p.lists, listKey)
		}
		return p
	}

	listKeys := make([]string, 0, len(req.Lists))
	for k := range req.Lists {
		listKeys = append(listKeys, k)
	}
	sort.Strings(listKeys)

	// Per-list ordered membership, computed once and reused for ops and rooms.
	listRooms := map[string][]*model.Room{}

	all := acc.SortedRooms()
	for _, key := range listKeys {
		cfg := req.Lists[key]
		filtered := make([]*model.Room, 0, len(all))
		for _, room := range all {
			if room.Membership != model.MembershipJoin {
				continue
			}
			if !roomMatchesFilter(room, cfg.Filters) {
				continue
			}
			filtered = append(filtered, room)
		}
		listRooms[key] = filtered

		limit := cfg.TimelineLimit
		if limit <= 0 {
			limit = slidingDefaultTimeline
		}
		// A list without a range means "everything that matches".
		if cfg.Range == nil {
			for _, room := range filtered {
				addPlan(room, key, limit, cfg.RequiredState)
			}
			continue
		}
		lo, hi := cfg.Range[0], cfg.Range[1]
		if lo < 0 {
			lo = 0
		}
		if hi >= len(filtered) {
			hi = len(filtered) - 1
		}
		for i := lo; i <= hi && i < len(filtered); i++ {
			addPlan(filtered[i], key, limit, cfg.RequiredState)
		}
	}

	for roomID, sub := range req.RoomSubscriptions {
		room, ok := acc.Rooms[roomID]
		if !ok || room.Membership != model.MembershipJoin {
			continue
		}
		limit := sub.TimelineLimit
		if limit <= 0 {
			limit = slidingDefaultTimeline
		}
		p := addPlan(room, "", limit, sub.RequiredState)
		p.subscribed = true
	}

	// Build room payloads. On an incremental request only rooms with new
	// events are returned; unchanged rooms are omitted entirely, which is how
	// simplified sliding sync expresses "nothing changed".
	rooms := map[string]any{}
	hasContent := false
	newPos := acc.Pos
	resent := 0

	for _, roomID := range order {
		p := plans[roomID]
		// A room this connection has never been given must be sent in full
		// even on an incremental request. Skipping it would leave the client
		// staring at a count it cannot render.
		roomInitial := initial || (conn != nil && !conn.has(roomID))
		payload, changed, capPos := s.slidingRoom(acc.UserID, p, since, roomInitial, classic)
		if !changed {
			continue
		}
		if roomInitial && !initial {
			resent++
		}
		rooms[roomID] = payload
		hasContent = true
		if conn != nil {
			conn.mark(roomID)
		}
		if capPos > 0 && capPos < newPos {
			newPos = capPos
		}
	}
	if conn != nil {
		conn.Pos = newPos
	}
	if resent > 0 {
		s.Log.Info("sliding sync re-sending rooms the connection never received",
			"user", acc.UserID, "conn", req.ConnID, "rooms", resent)
	}

	// Extensions can carry content on their own (to-device verification
	// messages, secret storage). If they do, this must count as content or the
	// long poll would keep waiting and the event would be dropped on timeout.
	extensions, extContent := s.slidingExtensions(req, acc, dev, since, initial)
	if extContent {
		hasContent = true
	}

	resp := map[string]any{
		"pos":        "s" + strconv.FormatInt(newPos, 10),
		"rooms":      rooms,
		"extensions": extensions,
	}

	if classic {
		resp["lists"] = s.classicLists(req, listKeys, listRooms, since)
	} else {
		counts := map[string]any{}
		for _, key := range listKeys {
			counts[key] = map[string]any{"count": len(listRooms[key])}
		}
		resp["lists"] = counts
	}
	return resp, hasContent
}

// classicLists emits MSC3575 ops. SYNC carries the full ordered room_ids for
// the window, which is the spec's way of saying "replace this range".
func (s *Server) classicLists(req *slidingRequest, keys []string, listRooms map[string][]*model.Room, since int64) map[string]any {
	out := map[string]any{}
	for _, key := range keys {
		rooms := listRooms[key]
		cfg := req.Lists[key]
		lo, hi := 0, len(rooms)-1
		if cfg.Range != nil {
			lo, hi = cfg.Range[0], cfg.Range[1]
			if lo < 0 {
				lo = 0
			}
			if hi >= len(rooms) {
				hi = len(rooms) - 1
			}
		}
		ids := []string{}
		if hi >= lo {
			for i := lo; i <= hi && i < len(rooms); i++ {
				ids = append(ids, rooms[i].RoomID)
			}
		}
		out[key] = map[string]any{
			"count": len(rooms),
			"ops": []any{
				map[string]any{
					"op":       "SYNC",
					"range":    []int{lo, maxInt(hi, lo)},
					"room_ids": ids,
				},
			},
		}
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// slidingRoom renders one room. It reports whether the room should be included
// and, when an incremental timeline was capped, the last stream position
// actually sent so the caller can hold the pos token back.
//
// roomInitial means "render this room as if the client had never seen it":
// full state plus the newest slice of the timeline. A room also gets that
// treatment when the connection has no record of delivering it, which is how a
// client that lost its cache gets repaired.
func (s *Server) slidingRoom(self string, p *roomPlan, since int64, roomInitial, classic bool) (map[string]any, bool, int64) {
	room := p.room
	limit := p.timelineLimit
	if limit <= 0 {
		limit = slidingDefaultTimeline
	}
	if limit > slidingMaxTimeline {
		limit = slidingMaxTimeline
	}

	// Collect the timeline slice for this request.
	all := make([]*model.Event, 0, len(room.Events))
	for _, ev := range room.Events {
		if ev.IsState() {
			continue
		}
		all = append(all, ev)
	}

	var picked []*model.Event
	limited := false
	if roomInitial {
		start := len(all) - limit
		if start < 0 {
			start = 0
		} else if start > 0 {
			limited = true
		}
		picked = all[start:]
	} else {
		for _, ev := range all {
			if ev.Pos > since {
				picked = append(picked, ev)
			}
		}
		if len(picked) > slidingMaxTimeline {
			// Keep the oldest ones and hold pos back so the rest arrive on the
			// next call instead of being lost.
			picked = picked[:slidingMaxTimeline]
			limited = true
		}
	}

	// Required state: everything on a room's first appearance, only the delta
	// afterwards.
	stateEvents := s.slidingState(room, p.requiredState, since, roomInitial, self)

	if !roomInitial && len(picked) == 0 && len(stateEvents) == 0 {
		return nil, false, 0
	}

	timeline := make([]map[string]any, 0, len(picked))
	for _, ev := range picked {
		timeline = append(timeline, s.eventJSON(ev, ""))
	}

	firstPos := int64(0)
	if len(picked) > 0 {
		firstPos = picked[0].Pos
	}
	prevBatch := "s0"
	if firstPos > 0 {
		prevBatch = "s" + strconv.FormatInt(firstPos-1, 10)
	}

	// The participant count comes from Yunhu's group/info headcount when we
	// have it; counting the members we happen to know about is always too low.
	joined := room.JoinedCount()

	out := map[string]any{
		"required_state": stateEvents,
		"timeline":       timeline,
		"prev_batch":     prevBatch,
		"limited":        limited,
		"num_live":       len(picked),
		"joined_count":   joined,
		"invited_count":  0,
		"membership":     room.Membership,
		"bump_stamp":     room.LastActivityTS,
		"is_dm":          room.IsDirect,
	}
	if roomInitial {
		out["initial"] = true
	}
	if len(p.lists) > 0 {
		out["lists"] = p.lists
	}

	// Room metadata is only re-sent when it changed, otherwise omitted fields
	// mean "unchanged" to the client.
	nameChanged := roomInitial || containsStateType(stateEvents, "m.room.name")
	avatarChanged := roomInitial || containsStateType(stateEvents, "m.room.avatar")
	if nameChanged {
		if room.Name == "" {
			out["name"] = nil
		} else {
			out["name"] = room.Name
		}
	}
	if avatarChanged {
		if room.AvatarURL == "" {
			out["avatar"] = nil
		} else {
			// Sliding sync is the path Element X uses, so a bare upstream URL
			// here means every room avatar is fetched straight from the CDN.
			out["avatar"] = s.Media.RemoteMXC(room.AvatarURL)
		}
	}
	if roomInitial && room.Name == "" {
		out["heroes"] = s.heroesFor(room, self)
	}

	// Room account data carries m.fully_read, the anchor a client needs to draw
	// the unread divider and jump to the first unread message. Sliding sync has
	// its own room-level account_data array; leaving it out is the reason
	// Element X opened rooms at the newest message with the badge still set.
	if ad := roomAccountData(room, roomInitial || len(picked) > 0 || len(stateEvents) > 0); len(ad) > 0 {
		out["account_data"] = ad
	}

	// MSC3575 still carries these; MSC4186 removed them.
	if classic {
		// Derived from the read marker, not a stored counter (see UnreadFor).
		unread, highlight := room.UnreadFor(self)
		out["notification_count"] = unread
		out["highlight_count"] = highlight
		out["unread_notifications"] = map[string]any{
			"notification_count": unread,
			"highlight_count":    highlight,
		}
		out["timestamp"] = room.LastActivityTS
	}

	// Hold pos back only when an *incremental* timeline was truncated, so the
	// rest arrives on the next call instead of being lost. A full payload
	// already ends at the room's newest event, so there is nothing to hold
	// back; the client reaches older history through prev_batch.
	if limited && !roomInitial && len(picked) > 0 {
		return out, true, picked[len(picked)-1].Pos
	}
	return out, true, 0
}

// slidingState filters room state according to required_state.
func (s *Server) slidingState(room *model.Room, reqs []slidingStateReq, since int64, initial bool, self string) []map[string]any {
	if len(reqs) == 0 {
		// Sensible default so a client that asks for nothing still learns the
		// room name and its own membership.
		reqs = []slidingStateReq{
			{Type: "m.room.create"},
			{Type: "m.room.name"},
			{Type: "m.room.avatar"},
			{Type: "m.room.canonical_alias"},
			{Type: "m.room.topic"},
			{Type: "m.room.member"},
			{Type: "m.room.join_rules"},
			{Type: "m.room.history_visibility"},
		}
	}

	matches := func(ev *model.Event) bool {
		for _, req := range reqs {
			// $ME: only the requesting user's own event of this type.
			if req.selfOnly {
				if (req.Type == "" || req.Type == ev.Type) && ev.StateKeyString() == self {
					return true
				}
				continue
			}
			if req.matchAll {
				return true
			}
			if req.Type != "" && req.Type != ev.Type {
				continue
			}
			if req.StateKey != nil && *req.StateKey != ev.StateKeyString() {
				continue
			}
			return true
		}
		return false
	}

	lazy := false
	for _, req := range reqs {
		if req.LazyMembers {
			lazy = true
		}
	}

	out := []map[string]any{}
	for _, ev := range room.State {
		// Two state events are always included, whatever the client asked for.
		//
		// m.room.pinned_events is a single small event and the only way a client
		// learns that the Yunhu bot board is pinned; clients rarely list it in
		// required_state, so filtering it out left the pin panel silently empty.
		//
		// m.room.power_levels decides the role badge next to every member. When
		// it was filtered out, a corrected role (the account is not the group's
		// owner) never reached the client and the stale "管理员" stuck forever.
		if !matches(ev) && ev.Type != "m.room.pinned_events" && ev.Type != "m.room.power_levels" {
			continue
		}
		if !initial && ev.Pos <= since {
			continue
		}
		// Lazy loading: keep only the client's own membership event.
		if lazy && ev.Type == "m.room.member" && ev.StateKeyString() != self {
			continue
		}
		out = append(out, s.eventJSON(ev, ""))
	}
	// Deterministic ordering keeps responses diffable in tests and logs.
	sort.Slice(out, func(i, j int) bool {
		ti, _ := out[i]["type"].(string)
		tj, _ := out[j]["type"].(string)
		if ti != tj {
			return ti < tj
		}
		ki, _ := out[i]["state_key"].(string)
		kj, _ := out[j]["state_key"].(string)
		return ki < kj
	})
	return out
}

func containsStateType(events []map[string]any, want string) bool {
	for _, ev := range events {
		if t, _ := ev["type"].(string); t == want {
			return true
		}
	}
	return false
}

// heroesFor returns the other participants of a direct room. Avatars go
// through the media proxy like every other avatar, so a client never reaches
// the upstream CDN directly.
func (s *Server) heroesFor(room *model.Room, self string) []map[string]any {
	out := []map[string]any{}
	if !room.IsDirect {
		return out
	}
	ids := make([]string, 0, len(room.Members))
	for uid := range room.Members {
		if uid != self {
			ids = append(ids, uid)
		}
	}
	sort.Strings(ids)
	for _, uid := range ids {
		hero := map[string]any{"user_id": uid}
		if m := room.Members[uid]; m != nil {
			if m.DisplayName != "" {
				hero["displayname"] = m.DisplayName
			}
			if m.AvatarURL != "" {
				hero["avatar_url"] = s.Media.RemoteMXC(m.AvatarURL)
			}
		}
		out = append(out, hero)
	}
	return out
}

// roomMatchesFilter applies the subset of SlidingRoomFilter this homeserver can
// honour. Rooms are Yunhu conversations: they have no type, tags or encryption,
// so filters that require those match nothing.
func roomMatchesFilter(room *model.Room, f map[string]any) bool {
	if f == nil {
		return true
	}
	if v, ok := f["is_dm"].(bool); ok && v != room.IsDirect {
		return false
	}
	// There are no invites or encrypted rooms in this projection.
	if v, ok := f["is_invited"].(bool); ok && v {
		return false
	}
	if v, ok := f["is_encrypted"].(bool); ok && v {
		return false
	}
	// Non-empty room_types/tags cannot match a room that has neither.
	if v, ok := f["room_types"].([]any); ok && len(v) > 0 {
		return false
	}
	if v, ok := f["tags"].([]any); ok && len(v) > 0 {
		return false
	}
	return true
}

// slidingExtensions answers enabled extensions with empty results. Returning
// the keys (rather than omitting them) stops clients from treating the
// extension as unsupported.
func (s *Server) slidingExtensions(req *slidingRequest, acc *model.Account, dev *model.Device, since int64, initial bool) (map[string]any, bool) {
	out := map[string]any{}
	content := false
	for key, raw := range req.Extensions {
		var cfg struct {
			Enabled bool `json:"enabled"`
		}
		_ = json.Unmarshal(raw, &cfg)
		if !cfg.Enabled {
			continue
		}
		switch key {
		case "typing":
			rooms := map[string]any{}
			now := time.Now().UnixMilli()
			for _, room := range acc.Rooms {
				users := []string{}
				for uid, until := range room.Typing {
					if until > now {
						users = append(users, uid)
					}
				}
				if len(users) > 0 {
					sort.Strings(users)
					rooms[room.RoomID] = map[string]any{"type": "m.typing", "content": map[string]any{"user_ids": users}}
				}
			}
			out[key] = map[string]any{"rooms": rooms}
		case "receipts":
			out[key] = map[string]any{"rooms": map[string]any{}}
		case "account_data":
			// Secret storage arrives here when a client uses sliding sync, so
			// this cannot be a fixed empty list: an empty account_data section
			// is why RecoveryState stayed on WAITING_FOR_SYNC forever.
			events := accountDataEvents(acc, since, initial)
			if len(events) > 0 {
				content = true
			}
			out[key] = map[string]any{"global": events, "rooms": map[string]any{}}
		case "to_device":
			// Session verification travels here; an always-empty list is why
			// "verify with another device" never progressed. next_batch is a
			// required field: without it the whole response fails to parse and
			// the client loops on an "offline" error forever.
			events := pendingToDevice(acc, dev)
			if len(events) > 0 {
				content = true
			}
			out[key] = map[string]any{
				"events":     events,
				"next_batch": toDeviceToken(acc, dev),
			}
		case "e2ee":
			// device_lists.changed is what prompts the client to call
			// /keys/query again after it uploads new keys or cross-signs.
			if len(deviceListChanged(acc, since)) > 0 {
				content = true
			}
			out[key] = map[string]any{
				"device_lists": map[string]any{
					"changed": deviceListChanged(acc, since),
					"left":    []string{},
				},
				"device_one_time_keys_count":       oneTimeKeyCountsFor(acc, primaryDeviceID(acc)),
				"device_unused_fallback_key_types": fallbackAlgorithmsFor(acc, primaryDeviceID(acc)),
			}
		case "presence":
			out[key] = map[string]any{"events": []any{}}
		default:
			out[key] = map[string]any{}
		}
	}
	return out, content
}
