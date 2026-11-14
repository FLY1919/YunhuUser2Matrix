package matrix

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
)

// wellKnownClient is what clients probe before login. When public_url is not
// configured the URL is derived from the request, which keeps LAN deployments
// (http://192.168.x.x:8008) usable instead of advertising https://localhost.
//
// org.matrix.msc3575.proxy points at this same server: sliding sync is
// implemented natively here, so the separate proxy that older clients expect is
// not needed. Clients that only look for the proxy key (Element Web's labs
// flag) therefore still find a working endpoint.
func (s *Server) wellKnownClient(c *Ctx) {
	base := s.baseURL(c.R)
	// Cross-origin fetch by web clients: the spec says discovery documents
	// SHOULD carry CORS headers.
	c.W.Header().Set("Access-Control-Allow-Origin", "*")
	writeJSON(c.W, http.StatusOK, map[string]any{
		"m.homeserver":             map[string]any{"base_url": base},
		"org.matrix.msc3575.proxy": map[string]any{"url": base},
	})
}

// wellKnownServer advertises the federation endpoint (not used: no federation).
func (s *Server) wellKnownServer(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"m.server": s.Cfg.Server.Name + ":443",
	})
}

// wellKnownSupport publishes who to contact about this homeserver (Matrix v1.10,
// MSC1929). Clients fetch it to render "server admin" / "report a problem" in
// settings, and Element X shows the admin contact on the "cannot connect"
// screen - which is exactly when a self-hosted bridge needs it.
//
// The document is served from this process rather than a static file so it
// stays in sync with the accounts on disk. Resolution order:
//
//  1. server.support.contacts in config, verbatim (invalid entries dropped);
//  2. otherwise the first local account's Matrix ID as m.role.admin, because on
//     a single-user bridge the account owner *is* the administrator;
//  3. server.support.support_page is always included when set.
//
// With nothing at all to publish the answer is 404, which the spec defines as
// "no support information available" - clients handle it silently.
func (s *Server) wellKnownSupport(c *Ctx) {
	cfg := s.Cfg.Server.Support

	contacts := make([]map[string]any, 0, len(cfg.Contacts))
	for _, ct := range cfg.Contacts {
		matrixID := strings.TrimSpace(ct.MatrixID)
		email := strings.TrimSpace(ct.EmailAddress)
		if matrixID == "" && email == "" {
			// The spec requires at least one of matrix_id / email_address; a
			// contact with neither is unreachable and would make the whole
			// document invalid.
			continue
		}
		role := strings.TrimSpace(ct.Role)
		if role == "" {
			role = roleAdmin
		}
		entry := map[string]any{"role": role}
		if matrixID != "" {
			entry["matrix_id"] = matrixID
		}
		if email != "" {
			entry["email_address"] = email
		}
		contacts = append(contacts, entry)
	}

	// contacts absent (nil) means "derive it"; contacts: [] means "publish none".
	if cfg.Contacts == nil && len(contacts) == 0 {
		if admin := s.firstAccountUserID(); admin != "" {
			contacts = append(contacts, map[string]any{
				"role":      roleAdmin,
				"matrix_id": admin,
			})
		}
	}

	page := strings.TrimSpace(cfg.SupportPage)
	if page == "" && len(contacts) == 0 {
		writeError(c.W, http.StatusNotFound, ErrNotFound,
			"This server publishes no support information")
		return
	}

	doc := map[string]any{}
	if page != "" {
		doc["support_page"] = page
	}
	if len(contacts) > 0 {
		doc["contacts"] = contacts
	}
	// Discovery documents are fetched cross-origin by web clients; the spec
	// says servers "SHOULD offer CORS headers" for them, and this endpoint is
	// unauthenticated anyway.
	c.W.Header().Set("Access-Control-Allow-Origin", "*")
	writeJSON(c.W, http.StatusOK, doc)
}

// roleAdmin is the catch-all contact role from MSC1929.
const roleAdmin = "m.role.admin"

// firstAccountUserID returns the Matrix ID of a local account, used as the
// default administrator contact.
func (s *Server) firstAccountUserID() string {
	accounts := s.Store.AllAccounts()
	if len(accounts) == 0 {
		return ""
	}
	// Stable choice: the lexicographically smallest user ID, so the published
	// document does not flip between accounts as they are loaded.
	best := ""
	for _, acc := range accounts {
		if acc == nil || acc.UserID == "" {
			continue
		}
		if best == "" || acc.UserID < best {
			best = acc.UserID
		}
	}
	return best
}

// versions lists the supported spec versions.
func (s *Server) versions(c *Ctx) {
	versions := []string{
		"v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6",
		"v1.7", "v1.8", "v1.9", "v1.10", "v1.11",
	}
	// Advertise both sliding sync dialects. Clients gate the feature on these
	// flags: Element X looks for simplified_msc3575, Element Web for msc3575.
	writeJSON(c.W, http.StatusOK, map[string]any{
		"versions": versions,
		"unstable_features": map[string]any{
			"org.matrix.simplified_msc3575": true,
			"org.matrix.msc3575":            true,
		},
	})
}

// capabilities reports room versions and changeable profile fields.
func (s *Server) capabilities(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"capabilities": map[string]any{
			"m.room_versions": map[string]any{
				"default":   "10",
				"available": map[string]any{"10": "stable"},
			},
			"m.change_password": map[string]any{"enabled": false},
			"m.set_displayname": map[string]any{"enabled": true},
			"m.set_avatar_url":  map[string]any{"enabled": true},
			"m.3pid_changes":    map[string]any{"enabled": false},
		},
	})
}

// roomKeysVersion reports that no key backup exists. The key endpoints proper

// pushRules returns the account's push rules.
//
// Matrix has no per-room "mute" flag: muting a room *is* the state of its
// global/room/<room id> push rule. A client toggles the bell by PUTting that rule
// with actions [] (muted) or ["notify"] (not muted), and reads the current state
// from this endpoint on startup. Answering a fixed empty list therefore made
// muting a no-op: the client set the rule, the server forgot it, and the next
// fetch said "no rules" - so every room came back unmuted.
//
// The authoritative value lives on the room (synced from Yunhu's
// do_not_disturb) and the rule is derived from it, so the two cannot disagree.
func (s *Server) pushRules(c *Ctx) {
	s.Store.Mu.RLock()
	roomRules := make([]any, 0)
	overrides := make([]any, 0)
	for _, room := range c.Account.Rooms {
		if rule := roomMuteRule(room); rule != nil {
			roomRules = append(roomRules, rule)
		}
		if rule := roomMuteOverride(room); rule != nil {
			overrides = append(overrides, rule)
		}
	}
	s.Store.Mu.RUnlock()

	// A stable order keeps the response byte-identical between calls, which
	// stops clients from thinking the rules changed.
	for _, list := range [][]any{roomRules, overrides} {
		sort.Slice(list, func(i, j int) bool {
			return asString(list[i].(map[string]any)["rule_id"]) <
				asString(list[j].(map[string]any)["rule_id"])
		})
	}

	empty := []any{}
	writeJSON(c.W, http.StatusOK, map[string]any{
		"global": map[string]any{
			"content":   empty,
			"override":  overrides,
			"room":      roomRules,
			"sender":    empty,
			"underride": empty,
		},
	})
}

// roomMuteRule renders a room's mute state as a Matrix push rule.
//
// Only muted rooms get a rule: an absent rule means "notified", which is the same
// thing as ["notify"] and is what a real homeserver reports for a room nobody has
// muted. Emitting a rule for every joined room would flood the response.
//
// The rule is published as an *override*, not a room rule. Element X expresses a
// per-room mute exactly that way - it PUTs
//
//	/pushrules/global/override/<room_id>
//	{"actions": [], "conditions": [{"kind":"event_match","key":"room_id","pattern":"<room_id>"}]}
//
// because override rules can carry a condition, while `global/room/<id>` rules
// are keyed by room and cannot. Reporting the mute only under `room` meant the
// client never saw its own mute and re-applied it on every launch.
//
// Both kinds are reported for the same room: a client that looks at `room` rules
// (older Element Web) finds it too.
func roomMuteRule(room *model.Room) map[string]any {
	if !room.Muted {
		return nil
	}
	return map[string]any{
		"rule_id":    room.RoomID,
		"default":    false,
		"enabled":    true,
		"actions":    []any{},
		"conditions": []any{},
	}
}

// roomMuteOverride is the same mute expressed as an override rule, which is what
// Element X reads and writes.
func roomMuteOverride(room *model.Room) map[string]any {
	if !room.Muted {
		return nil
	}
	return map[string]any{
		"rule_id":    room.RoomID,
		"default":    false,
		"enabled":    true,
		"actions":    []any{},
		"conditions": []any{muteCondition(room.RoomID)},
	}
}

// muteCondition is the event_match every per-room override rule carries.
func muteCondition(roomID string) map[string]any {
	return map[string]any{
		"kind":    "event_match",
		"key":     "room_id",
		"pattern": roomID,
	}
}

// pushRule returns one rule, which for this projection can only be a room mute.
func (s *Server) pushRule(c *Ctx) {
	scope := c.Param("scope")
	kind := c.Param("kind")
	ruleID := c.Param("ruleId")
	if scope != "global" || !isRoomRuleKind(kind) {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "No such rule")
		return
	}

	s.Store.Mu.RLock()
	room := c.Account.Rooms[ruleID]
	var rule map[string]any
	if room != nil {
		if kind == "override" {
			rule = roomMuteOverride(room)
		} else {
			rule = roomMuteRule(room)
		}
	}
	s.Store.Mu.RUnlock()

	if rule == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "No such rule")
		return
	}
	writeJSON(c.W, http.StatusOK, rule)
}

// isRoomRuleKind reports whether a rule kind is one this server implements: the
// two ways a client can express a per-room mute.
//
//	room      keyed by room id (Element Web)
//	override  carries an event_match condition (Element X)
func isRoomRuleKind(kind string) bool {
	return kind == "room" || kind == "override"
}

// putPushRule creates or replaces a room's mute rule.
//
// The documented meaning of the two shapes:
//
//	{"actions": []}          -> mute
//	{"actions": ["notify"]}  -> unmute
//
// Anything else is rejected rather than guessed at, because writing the wrong
// state into the room would also push the wrong value to Yunhu.
func (s *Server) putPushRule(c *Ctx) {
	scope := c.Param("scope")
	kind := c.Param("kind")
	ruleID := c.Param("ruleId")
	if scope != "global" || !isRoomRuleKind(kind) {
		writeError(c.W, http.StatusBadRequest, ErrInvalidParam,
			"本服务端只支持 global/room|override/<room_id> 规则（房间静音）")
		return
	}

	var body struct {
		Actions    []any `json:"actions"`
		Conditions []any `json:"conditions"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 1<<20)).Decode(&body); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Request body is not valid JSON")
		return
	}

	// An override rule names its room through a condition instead of the rule id
	// alone; take that as the authoritative target when it is present, because
	// that is what the client will filter by.
	target := ruleID
	if kind == "override" {
		if roomID := roomIDFromConditions(body.Conditions); roomID != "" {
			target = roomID
		}
	}

	muted, ok := mutedFromActions(body.Actions)
	if !ok {
		writeError(c.W, http.StatusBadRequest, ErrInvalidParam,
			`actions 只能是 []（静音）或 ["notify"]（取消静音）`)
		return
	}

	s.setRoomMuted(c, target, muted)
}

// deletePushRule unmutes a room.
//
// Deleting the rule is how a client clears a mute: the rule stops existing, which
// means "notify" again. It maps onto the same upstream call as an explicit
// unmute.
func (s *Server) deletePushRule(c *Ctx) {
	scope := c.Param("scope")
	kind := c.Param("kind")
	if scope != "global" || !isRoomRuleKind(kind) {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "No such rule")
		return
	}
	s.setRoomMuted(c, c.Param("ruleId"), false)
}

// roomIDFromConditions pulls the room id out of an override rule's conditions.
//
// Element X sends exactly one: {"kind":"event_match","key":"room_id","pattern":"!…"}.
// Anything else is ignored, and the caller falls back to the rule id.
func roomIDFromConditions(conditions []any) string {
	for _, raw := range conditions {
		cond, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if asString(cond["kind"]) != "event_match" || asString(cond["key"]) != "room_id" {
			continue
		}
		if pattern := asString(cond["pattern"]); pattern != "" {
			return pattern
		}
	}
	return ""
}

// mutedFromActions reads a push rule's actions as a mute flag.
//
// An empty action list silences the room; ["notify"] keeps it noisy. The check is
// on whether "notify" is present rather than on the list being exactly one
// element, so a client that adds other actions (highlight, sound) still means
// "notify".
func mutedFromActions(actions []any) (muted, ok bool) {
	for _, raw := range actions {
		if asString(raw) == "notify" {
			return false, true
		}
	}
	// No "notify": muted. This covers both [] and a list of other actions.
	return true, true
}

// setRoomMuted records the mute state and mirrors it to Yunhu.
func (s *Server) setRoomMuted(c *Ctx, ruleID string, muted bool) {
	acc := c.Account

	s.Store.Mu.Lock()
	room := acc.Rooms[ruleID]
	if room == nil {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown room")
		return
	}
	changed := room.Muted != muted
	room.Muted = muted
	room.MutedTS = time.Now().UnixMilli()
	chatID := room.ChatID
	s.Store.Mu.Unlock()

	if changed {
		s.Store.MarkDirty(acc.YunhuID)
		// A mute change is visible in the room list (the bell icon), so clients
		// have to be told rather than polling for it.
		s.Hub.Notify(acc.YunhuID)
	}

	// Mirror upstream. A failure is reported but does not roll back the local
	// state: the user asked for the room to be muted, and losing that because the
	// network hiccuped would be worse than a temporarily unsynced server.
	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	if err := s.yunhuFor(acc, c.Device).SetConversationMuted(ctx, chatID, muted); err != nil {
		s.Log.Warn("upstream mute failed", "room", ruleID, "muted", muted, "err", err)
		writeError(c.W, http.StatusBadGateway, ErrUnknown, "云湖设置免打扰失败: "+err.Error())
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// turnServer reports no TURN service.
func (s *Server) turnServer(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"username": "",
		"password": "",
		"uris":     []string{},
		"ttl":      0,
	})
}

// publicRooms exposes the directory as empty (rooms live in Yunhu).
func (s *Server) publicRooms(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"chunk":                     []any{},
		"total_room_count_estimate": 0,
	})
}

// directoryRoom resolves no aliases.
func (s *Server) directoryRoom(c *Ctx) {
	writeError(c.W, http.StatusNotFound, ErrNotFound, "Room alias not found")
}

// search returns an empty result set.
func (s *Server) search(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"search_categories": map[string]any{
			"room_events": map[string]any{"count": 0, "results": []any{}},
		},
	})
}

// userDirectorySearch returns an empty result set.
func (s *Server) userDirectorySearch(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{"results": []any{}, "limited": false})
}

// emptyObject is the generic "nothing to do" response.
func (s *Server) emptyObject(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// mediaConfig reports the upload limit.
func (s *Server) mediaConfig(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"m.upload.size": s.Cfg.Server.MaxUploadSize,
	})
}

// upload stores a Matrix media upload locally.
func (s *Server) upload(c *Ctx) {
	limit := s.Cfg.Server.MaxUploadSize
	c.R.Body = http.MaxBytesReader(c.W, c.R.Body, limit)

	contentType := c.R.Header.Get("Content-Type")
	var data []byte
	var err error
	filename := c.Query("filename")

	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := c.R.ParseMultipartForm(limit); err != nil {
			writeError(c.W, http.StatusRequestEntityTooLarge, ErrTooLarge, "Upload too large")
			return
		}
		file, header, ferr := c.R.FormFile("file")
		if ferr != nil {
			writeError(c.W, http.StatusBadRequest, ErrUnknown, "Missing file field")
			return
		}
		defer file.Close()
		if filename == "" {
			filename = header.Filename
		}
		data, err = io.ReadAll(io.LimitReader(file, limit))
	} else {
		data, err = io.ReadAll(c.R.Body)
	}
	if err != nil {
		writeError(c.W, http.StatusBadRequest, ErrUnknown, "Could not read upload")
		return
	}

	id, err := s.Media.Put(data, contentType)
	if err != nil {
		writeError(c.W, http.StatusInternalServerError, ErrUnknown, err.Error())
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{"content_uri": s.Media.MXC(id)})
}

// download serves local media or proxies Yunhu media.
func (s *Server) download(c *Ctx) {
	s.serveMedia(c, false)
}

// thumbnail serves the original image (no resizing is performed).
func (s *Server) thumbnail(c *Ctx) {
	s.serveMedia(c, true)
}

func (s *Server) serveMedia(c *Ctx, thumb bool) {
	serverName := c.Param("serverName")
	mediaID := c.Param("mediaId")

	// A proxied reference is self-describing (yh_<base64url of the upstream
	// URL>), so it can be served under any server name. That keeps mxc URIs
	// minted before a server.name change working instead of 404ing with
	// "Unknown media server". Local uploads stay guarded, since their IDs only
	// mean something for this homeserver.
	ref, isRemote := s.Media.decodeRemote(mediaID)
	if !isRemote && serverName != s.Cfg.Server.Name {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown media server")
		return
	}

	ctx, cancel := context.WithTimeout(c.R.Context(), 60*time.Second)
	defer cancel()

	if isRemote {
		data, ctype, err := s.Media.FetchRemote(ctx, ref)
		if err != nil {
			s.Log.Debug("remote media fetch failed", "ref", ref, "err", err)
			writeError(c.W, http.StatusBadGateway, ErrNotFound, "Could not fetch media")
			return
		}
		writeMedia(c, data, ctype)
		return
	}

	f, _, err := s.Media.Open(mediaID)
	if err != nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Media not found")
		return
	}
	defer f.Close()

	ctype := s.Media.ContentType(mediaID)
	if ctype == "" {
		head := make([]byte, 512)
		n, _ := io.ReadFull(f, head)
		ctype = http.DetectContentType(head[:n])
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			writeError(c.W, http.StatusInternalServerError, ErrUnknown, "Could not read media")
			return
		}
	}
	c.W.Header().Set("Content-Type", ctype)
	c.W.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	name := c.Query("filename")
	if name == "" {
		name = mediaID
	}
	c.W.Header().Set("Content-Disposition", "inline; filename=\""+name+"\"")
	// ServeContent handles Range, If-Range and HEAD. Doing that by hand is how
	// media ends up showing "0 B": a client that asks for `bytes=1000-` and gets
	// a 200 from offset 0 cannot seek, and audio/video players give up.
	http.ServeContent(c.W, c.R, name, time.Time{}, f)
}

func writeMedia(c *Ctx, data []byte, ctype string) {
	if ctype == "" {
		ctype = http.DetectContentType(data)
	}
	c.W.Header().Set("Content-Type", ctype)
	c.W.Header().Set("Cache-Control", "public, max-age=86400")
	http.ServeContent(c.W, c.R, "", time.Time{}, bytes.NewReader(data))
}

// previewURL returns the URL a client should use to preview remote media.
func (s *Server) previewURL(c *Ctx) {
	raw := c.Query("url")
	writeJSON(c.W, http.StatusOK, map[string]any{
		"preview_url": raw,
		"info":        map[string]any{},
	})
}
