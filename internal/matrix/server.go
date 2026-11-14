// Package matrix implements the Matrix client-server API on top of Yunhu.
//
// The design is that of a "personal homeserver": every Matrix user maps 1:1 to
// a Yunhu account, and every Yunhu conversation is projected as a Matrix room.
// Events are appended to an in-memory log that doubles as the /sync stream, so
// the same code path serves live messages (websocket), history (REST backfill)
// and locally echoed sends.
package matrix

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/hub"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
	"github.com/yunhu/yhmatrix/internal/yunhu"
)

// Aliases keep handler signatures readable.
type accountRef = model.Account
type deviceRef = model.Device

// Server is the Matrix homeserver.
type Server struct {
	Cfg   *config.Config
	Store *store.Store
	Hub   *hub.Hub
	Log   *slog.Logger
	Media *MediaStore

	router *Router

	runnerMu sync.Mutex
	runners  map[string]*Runner

	// slidingConns remembers the last pos handed out per (user, device,
	// conn_id) so a client that lost its local state can be resynced instead of
	// being fed empty deltas forever.
	slidingMu    sync.Mutex
	slidingConns map[string]*slidingConnState

	started time.Time
}

// NewServer wires the homeserver together.
func NewServer(cfg *config.Config, st *store.Store, hb *hub.Hub, log *slog.Logger) (*Server, error) {
	media, err := NewMediaStore(cfg, log)
	if err != nil {
		return nil, err
	}
	s := &Server{
		Cfg:          cfg,
		Store:        st,
		Hub:          hb,
		Log:          log,
		Media:        media,
		runners:      map[string]*Runner{},
		slidingConns: map[string]*slidingConnState{},
		started:      time.Now(),
	}
	s.router = NewRouter(s.authenticate)
	s.registerRoutes()
	return s, nil
}

// yunhuFor returns a client bound to an account, using the session's device id.
func (s *Server) yunhuFor(acc *model.Account, dev *model.Device) *yunhu.Client {
	c := s.YunhuClient().WithToken(acc.Token)
	if dev != nil && dev.YunhuDeviceID != "" {
		c = c.WithDevice(dev.YunhuDeviceID)
	}
	return c
}

// YunhuClient returns the unauthenticated client template.
func (s *Server) YunhuClient() *yunhu.Client {
	c := yunhu.NewClient(s.Cfg.Yunhu.BaseURL, s.Cfg.Timeout())
	c.Platform = s.Cfg.Yunhu.Platform
	c.Wire = s.Cfg.Yunhu.Wire
	c.Log = s.Log
	return c
}

func (s *Server) registerRoutes() {
	r := s.router

	// --- discovery -----------------------------------------------------------
	r.GET("/.well-known/matrix/client", false, s.wellKnownClient)
	r.GET("/.well-known/matrix/server", false, s.wellKnownServer)
	r.GET("/.well-known/matrix/support", false, s.wellKnownSupport)
	r.GET("/_matrix/client/versions", false, s.versions)
	r.GET("/_matrix/client/:ver/capabilities", true, s.capabilities)

	// --- authentication ------------------------------------------------------
	r.GET("/_matrix/client/:ver/login", false, s.loginFlows)
	r.POST("/_matrix/client/:ver/login", false, s.login)
	r.POST("/_matrix/client/:ver/register", false, s.register)
	r.GET("/_matrix/client/:ver/register/available", false, s.registerAvailable)
	r.POST("/_matrix/client/:ver/logout", true, s.logout)
	r.POST("/_matrix/client/:ver/logout/all", true, s.logoutAll)
	r.GET("/_matrix/client/:ver/account/whoami", true, s.whoami)
	r.GET("/_matrix/client/:ver/account/3pid", true, s.emptyObject)
	r.POST("/_matrix/client/:ver/account/deactivate", true, s.deactivate)
	r.GET("/_matrix/client/:ver/devices", true, s.listDevices)
	r.GET("/_matrix/client/:ver/devices/:deviceId", true, s.getDevice)
	r.PUT("/_matrix/client/:ver/devices/:deviceId", true, s.updateDevice)
	r.DELETE("/_matrix/client/:ver/devices/:deviceId", true, s.deleteDevice)
	r.POST("/_matrix/client/:ver/delete_devices", true, s.deleteDevices)
	r.POST("/_matrix/client/:ver/login/yunhu/sms", false, s.requestSMSCode)
	r.GET("/_matrix/client/:ver/login/yunhu/captcha", false, s.captcha)

	// --- sync ----------------------------------------------------------------
	r.GET("/_matrix/client/:ver/sync", true, s.sync)

	// --- sliding sync ---------------------------------------------------------
	// Both dialects are served natively, so no external sliding-sync proxy (and
	// no Postgres) is needed.
	r.POST("/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", true, s.slidingSync)
	r.POST("/_matrix/client/unstable/org.matrix.msc3575/sync", true, s.slidingSyncClassic)
	r.POST("/_matrix/client/v4/sync", true, s.slidingSync)

	// --- rooms ---------------------------------------------------------------
	r.GET("/_matrix/client/:ver/joined_rooms", true, s.joinedRooms)
	r.GET("/_matrix/client/:ver/rooms/:roomId/messages", true, s.messages)
	r.GET("/_matrix/client/:ver/rooms/:roomId/event/:eventId", true, s.getEvent)
	// /context is how a client renders an event it never saw in a timeline: the
	// pinned-messages panel and permalink views ask for it. Without the route it
	// fell through to the catch-all and answered M_UNRECOGNIZED.
	r.GET("/_matrix/client/:ver/rooms/:roomId/context/:eventId", true, s.eventContext)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/send/:eventType/:txnId", true, s.sendEvent)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/state/:eventType", true, s.putState)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/state/:eventType/:stateKey", true, s.putState)
	r.GET("/_matrix/client/:ver/rooms/:roomId/state", true, s.getState)
	r.GET("/_matrix/client/:ver/rooms/:roomId/state/:eventType", true, s.getState)
	r.GET("/_matrix/client/:ver/rooms/:roomId/state/:eventType/:stateKey", true, s.getState)
	r.GET("/_matrix/client/:ver/rooms/:roomId/members", true, s.members)
	r.GET("/_matrix/client/:ver/rooms/:roomId/joined_members", true, s.joinedMembers)
	r.POST("/_matrix/client/:ver/rooms/:roomId/read_markers", true, s.readMarkers)
	r.POST("/_matrix/client/:ver/rooms/:roomId/receipt/:receiptType/:eventId", true, s.receipt)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/typing/:userId", true, s.typing)
	r.POST("/_matrix/client/:ver/rooms/:roomId/leave", true, s.leave)
	r.POST("/_matrix/client/:ver/rooms/:roomId/join", true, s.join)
	r.POST("/_matrix/client/:ver/rooms/:roomId/invite", true, s.invite)
	r.POST("/_matrix/client/:ver/rooms/:roomId/forget", true, s.forget)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/redact/:eventId/:txnId", true, s.redact)
	r.POST("/_matrix/client/:ver/rooms/:roomId/redact/:eventId/:txnId", true, s.redact)
	r.POST("/_matrix/client/:ver/rooms/:roomId/report/:eventId", true, s.emptyObject)
	r.GET("/_matrix/client/:ver/rooms/:roomId/relations/:eventId", true, s.relations)
	r.GET("/_matrix/client/:ver/rooms/:roomId/threads", true, s.threads)
	r.POST("/_matrix/client/:ver/createRoom", true, s.createRoom)
	r.GET("/_matrix/client/:ver/rooms/:roomId/tags", true, s.emptyObject)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/tags/:tag", true, s.emptyObject)

	// --- profile -------------------------------------------------------------
	r.GET("/_matrix/client/:ver/profile/:userId", true, s.profile)
	r.GET("/_matrix/client/:ver/profile/:userId/displayname", true, s.profileDisplayName)
	r.PUT("/_matrix/client/:ver/profile/:userId/displayname", true, s.setDisplayName)
	r.GET("/_matrix/client/:ver/profile/:userId/avatar_url", true, s.profileAvatar)
	r.PUT("/_matrix/client/:ver/profile/:userId/avatar_url", true, s.setAvatarURL)

	// --- filters -------------------------------------------------------------
	r.POST("/_matrix/client/:ver/user/:userId/filter", true, s.createFilter)
	r.GET("/_matrix/client/:ver/user/:userId/filter/:filterId", true, s.getFilter)

	// --- account data --------------------------------------------------------
	r.GET("/_matrix/client/:ver/user/:userId/account_data/:type", true, s.getAccountData)
	r.PUT("/_matrix/client/:ver/user/:userId/account_data/:type", true, s.putAccountData)
	r.GET("/_matrix/client/:ver/rooms/:roomId/account_data/:type", true, s.getAccountData)
	r.PUT("/_matrix/client/:ver/rooms/:roomId/account_data/:type", true, s.putAccountData)

	// --- end-to-end encryption ----------------------------------------------
	// Yunhu does not offer E2EE, so no room ever advertises m.room.encryption.
	// The key endpoints still have to behave like a real homeserver's, though:
	// E2EE-first clients refuse to finish their sign-in flow without them.
	r.POST("/_matrix/client/:ver/keys/upload", true, s.keysUpload)
	r.POST("/_matrix/client/:ver/keys/query", true, s.keysQuery)
	r.POST("/_matrix/client/:ver/keys/claim", true, s.keysClaim)
	r.GET("/_matrix/client/:ver/keys/changes", true, s.keysChanges)
	r.POST("/_matrix/client/:ver/keys/device_signing/upload", true, s.deviceSigningUpload)
	r.POST("/_matrix/client/:ver/keys/signatures/upload", true, s.signatureUpload)
	// Session verification (SAS) runs over to-device messages, so this endpoint
	// is what makes "verify with another device" work at all.
	r.PUT("/_matrix/client/:ver/sendToDevice/:eventType/:txnId", true, s.sendToDevice)
	r.POST("/_matrix/client/:ver/room_keys/version", true, s.roomKeysCreate)
	r.GET("/_matrix/client/:ver/room_keys/version", true, s.roomKeysGetVersion)
	r.GET("/_matrix/client/:ver/room_keys/version/:version", true, s.roomKeysGetVersion)
	r.PUT("/_matrix/client/:ver/room_keys/version/:version", true, s.roomKeysUpdateVersion)
	r.PUT("/_matrix/client/:ver/room_keys/keys", true, s.roomKeysPut)
	r.GET("/_matrix/client/:ver/room_keys/keys", true, s.roomKeysGet)

	// --- misc ----------------------------------------------------------------
	r.PUT("/_matrix/client/:ver/presence/:userId/status", true, s.emptyObject)
	r.GET("/_matrix/client/:ver/presence/:userId/status", true, s.emptyObject)
	r.GET("/_matrix/client/:ver/pushrules/", true, s.pushRules)
	r.GET("/_matrix/client/:ver/pushrules/:scope/:kind/:ruleId", true, s.pushRule)
	// Per-room mute is expressed as this rule's actions: [] mutes, ["notify"]
	// does not. Clients PUT/DELETE it when the user toggles the bell icon, so
	// both verbs have to exist or the toggle silently does nothing.
	r.PUT("/_matrix/client/:ver/pushrules/:scope/:kind/:ruleId", true, s.putPushRule)
	r.DELETE("/_matrix/client/:ver/pushrules/:scope/:kind/:ruleId", true, s.deletePushRule)
	r.GET("/_matrix/client/:ver/voip/turnServer", true, s.turnServer)
	r.GET("/_matrix/client/:ver/publicRooms", true, s.publicRooms)
	r.POST("/_matrix/client/:ver/publicRooms", true, s.publicRooms)
	r.GET("/_matrix/client/:ver/thirdparty/protocols", true, s.emptyObject)
	r.GET("/_matrix/client/:ver/directory/room/:alias", true, s.directoryRoom)
	r.POST("/_matrix/client/:ver/search", true, s.search)
	r.POST("/_matrix/client/:ver/user_directory/search", true, s.userDirectorySearch)

	// --- media ---------------------------------------------------------------
	// Two families, both served by the same handlers:
	//
	//   /_matrix/media/:ver/...          unauthenticated, deprecated (MSC3916)
	//   /_matrix/client/:ver/media/...   authenticated, what Element X uses
	//
	// Element X (and every recent matrix-rust-sdk client) only ever asks for
	// the second family for avatars, thumbnails and downloads. Missing it means
	// the router falls through to the 404 handler and every single avatar and
	// message image shows a broken placeholder, which is exactly what happened
	// before these lines existed.
	//
	// The authenticated family is registered without requiring a token: clients
	// do send one, but rejecting a request that lacks it would break web clients
	// loading media through plain <img> tags, and there is nothing to protect
	// here — a media ID is unguessable and the bytes are the same either way.
	r.GET("/_matrix/media/:ver/config", false, s.mediaConfig)
	r.POST("/_matrix/media/:ver/upload", true, s.upload)
	r.GET("/_matrix/media/:ver/download/:serverName/:mediaId", false, s.download)
	r.GET("/_matrix/media/:ver/download/:serverName/:mediaId/*", false, s.download)
	r.GET("/_matrix/media/:ver/thumbnail/:serverName/:mediaId", false, s.thumbnail)
	r.GET("/_matrix/media/:ver/thumbnail/:serverName/:mediaId/*", false, s.thumbnail)
	r.GET("/_matrix/media/:ver/preview_url", true, s.previewURL)

	r.GET("/_matrix/client/:ver/media/config", false, s.mediaConfig)
	r.POST("/_matrix/client/:ver/media/upload", true, s.upload)
	r.GET("/_matrix/client/:ver/media/download/:serverName/:mediaId", false, s.download)
	r.GET("/_matrix/client/:ver/media/download/:serverName/:mediaId/*", false, s.download)
	r.GET("/_matrix/client/:ver/media/thumbnail/:serverName/:mediaId", false, s.thumbnail)
	r.GET("/_matrix/client/:ver/media/thumbnail/:serverName/:mediaId/*", false, s.thumbnail)
	// preview_url is a GET in the spec but some clients POST it; both are cheap.
	r.GET("/_matrix/client/:ver/media/preview_url", true, s.previewURL)
	r.POST("/_matrix/client/:ver/media/preview_url", true, s.previewURL)
}

// authenticate resolves the access token and populates the context.
func (s *Server) authenticate(c *Ctx) bool {
	token := bearerToken(c.R)
	if token == "" {
		writeError(c.W, http.StatusUnauthorized, ErrMissingToken, "Missing access token")
		return false
	}
	acc, dev := s.Store.GetByToken(token)
	if acc == nil {
		writeError(c.W, http.StatusUnauthorized, ErrUnknownToken, "Unknown or expired access token")
		return false
	}
	c.Account = acc
	c.Device = dev
	return true
}

func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	if t := r.URL.Query().Get("access_token"); t != "" {
		return t
	}
	return ""
}

// ServeHTTP adds CORS and access logging around the router.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Cfg.Server.AllowCORS {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, X-Requested-With")
		h.Set("Access-Control-Max-Age", "86400")
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// An optional bundled web client is served from the same origin.
	if s.serveWeb(w, r) {
		return
	}
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.serveRecovering(rec, r)
	s.Log.Debug("request",
		"method", r.Method, "path", r.URL.Path,
		"status", rec.status, "dur_ms", time.Since(start).Milliseconds())
}

// serveRecovering runs the router with a panic guard.
//
// Without it a bug in one handler tears down the TCP connection, which clients
// report as "cannot connect to the server" and then retry in a loop - turning a
// single bad request into a permanent outage. A 500 with a JSON body at least
// lets the client move on.
func (s *Server) serveRecovering(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if v := recover(); v != nil {
			s.Log.Error("handler panic",
				"method", r.Method, "path", r.URL.Path,
				"panic", v, "stack", string(debug.Stack()))
			writeError(w, http.StatusInternalServerError, ErrUnknown, "Internal server error")
		}
	}()
	s.router.ServeHTTP(w, r)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.wrote {
		return
	}
	s.status = code
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wrote = true
	return s.ResponseWriter.Write(b)
}

// StartRunners reconnects every known account after a restart.
func (s *Server) StartRunners(ctx context.Context) {
	for _, acc := range s.Store.AllAccounts() {
		// Repair rooms persisted by builds that never recorded the 1:1 peer.
		s.migrateDirectPeers(acc)
		// Drop the empty rooms that older builds minted for edit/recall pushes.
		s.purgeEmptyRooms(acc)
		if acc.Token == "" {
			continue
		}
		s.StartRunner(ctx, acc)
	}
}

// purgeEmptyRooms removes conversations that hold nothing but their own
// bootstrap state.
//
// Older builds created a room for every edit and recall push, even when the
// message they named had never been projected. Those rooms contain the seven
// state events makeRoom emits and no timeline at all, so a client lists them as
// empty conversations ("空气会话") that can never receive anything - the only
// thing that could fill one is a message, and messages always arrive through the
// conversation list, which would have named the room properly.
//
// The test is deliberately strict, so a real room is never touched:
//
//   - no timeline event at all (not even a recalled one),
//   - no name (the conversation list always supplies one),
//   - not the room the account is currently looking at, if it has an unread
//     marker (a read marker means the user opened it, so it is real).
func (s *Server) purgeEmptyRooms(acc *model.Account) {
	s.Store.Mu.Lock()
	var dropped []string
	for id, room := range acc.Rooms {
		if room.Name != "" || len(room.BoardEvents) > 0 {
			continue
		}
		if room.ReadMarkerPos > 0 {
			continue
		}
		hasEvent := false
		for _, ev := range room.Events {
			if !ev.IsState() {
				hasEvent = true
				break
			}
		}
		if hasEvent {
			continue
		}
		delete(acc.Rooms, id)
		dropped = append(dropped, id)
	}
	s.Store.Mu.Unlock()

	if len(dropped) > 0 {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		s.Log.Info("dropped empty projected rooms", "user", acc.UserID, "rooms", dropped)
	}
}

// migrateDirectPeers backfills the missing participant in existing 1:1 rooms.
// It is idempotent and does not touch group chats.
func (s *Server) migrateDirectPeers(acc *model.Account) {
	s.Store.Mu.Lock()
	added := 0
	for _, room := range acc.Rooms {
		if s.ensureDirectPeerLocked(acc, room) {
			added++
		}
	}
	s.Store.Mu.Unlock()

	if added > 0 {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
		s.Log.Info("added missing 1:1 participants", "user", acc.UserID, "rooms", added)
	}
}

// StopRunners tears down all websocket connections.
func (s *Server) StopRunners() {
	s.runnerMu.Lock()
	runners := s.runners
	s.runners = map[string]*Runner{}
	s.runnerMu.Unlock()
	for _, r := range runners {
		r.Stop()
	}
}

// Runner returns the live runner for an account, if any.
func (s *Server) runnerFor(yunhuID string) *Runner {
	s.runnerMu.Lock()
	defer s.runnerMu.Unlock()
	return s.runners[yunhuID]
}
