package matrix

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu"
)

func randomToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// loginRequest is the union of the fields this homeserver understands.
type loginRequest struct {
	Type     string `json:"type"`
	User     string `json:"user"`
	Password string `json:"password"`
	Token    string `json:"token"`
	Phone    string `json:"phone"`
	Email    string `json:"email"`
	Captcha  string `json:"captcha"`
	// Yunhu-specific optional fields.
	Platform   string `json:"platform"`
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"initial_device_display_name"`

	Identifier struct {
		Type string `json:"type"`
		User string `json:"user"`
		// Medium and Address carry the third-party identifier forms:
		// m.id.thirdparty/email and m.id.thirdparty/msisdn. FluffyChat sends
		// {"type":"m.id.thirdparty","medium":"email","address":"…"} when the
		// user picks the email login, and ignoring those two fields made
		// "login with a Yunhu email" fail with "missing username".
		Medium  string `json:"medium"`
		Address string `json:"address"`
	} `json:"identifier"`
}

// identifierString returns the account identifier out of whichever field shape
// the client used.
//
// Clients disagree on how to send "my email is x@y":
//
//	{"identifier":{"type":"m.id.user","user":"x@y"}}
//	{"identifier":{"type":"m.id.thirdparty","medium":"email","address":"x@y"}}
//	{"user":"x@y"}
//
// and some of them wrap a third-party address as if it were a Matrix user ID,
// producing "@x@y:localhost" (leading "@" plus the homeserver's domain). All of
// these have to resolve, otherwise a perfectly valid Yunhu email cannot be typed
// into the login box of a client that builds the third-party form.
//
// A real Matrix ID (m.id.user) is left alone: the caller resolves it against the
// local accounts through localAccountFor, which understands "@localpart:server".
func (s *Server) identifierString(req *loginRequest) string {
	id := req.Identifier
	out := strings.TrimSpace(id.User)
	if out == "" {
		out = strings.TrimSpace(id.Address)
	}
	if out == "" {
		out = strings.TrimSpace(req.User)
	}
	if out == "" {
		out = orDefault(strings.TrimSpace(req.Email), strings.TrimSpace(req.Phone))
	}
	if out == "" {
		return ""
	}

	// Only a third-party identifier carries a bare value (an email address or a
	// phone number). Those never contain a colon, so a trailing ":domain" is a
	// client that mistook the field for a Matrix user ID - drop it and the
	// leading "@" it added.
	thirdParty := id.Medium != "" || id.Type == "m.id.thirdparty" ||
		strings.TrimSpace(req.Email) == out || strings.TrimSpace(req.Phone) == out
	if thirdParty {
		out = strings.TrimPrefix(out, "@")
		if i := strings.IndexByte(out, ':'); i > 0 {
			out = out[:i]
		}
		if id.Medium == "msisdn" {
			out = strings.TrimPrefix(out, "+")
		}
	}
	return out
}

// loginFlows advertises the supported login types.
func (s *Server) loginFlows(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"flows": []map[string]any{
			{"type": "m.login.password"},
			{"type": "com.yunhu.token"},
			{"type": "com.yunhu.sms"},
			{"type": "com.yunhu.email"},
		},
	})
}

// login authenticates against Yunhu and mints a Matrix access token.
//
// The standard m.login.password flow is overloaded so that every Matrix client
// works out of the box:
//
//	username contains "@"      -> Yunhu email + password login
//	password looks like a token-> Yunhu token login (username may be the id)
//	username is all digits     -> Yunhu phone + SMS code login
func (s *Server) login(c *Ctx) {
	var req loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(c.W, c.R.Body, 1<<20)).Decode(&req); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Request body is not valid JSON")
		return
	}

	username := s.identifierString(&req)
	// Keep the identifier as the client sent it: resolving it against the local
	// accounts needs the full form, and a numeric Yunhu id must not be mistaken
	// for a phone number.
	identifier := username
	username = strings.TrimPrefix(username, "@")
	if i := strings.IndexByte(username, ':'); i > 0 {
		username = username[:i]
	}
	username = strings.TrimPrefix(username, userPrefixUser)

	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	yc := s.YunhuClient()

	var token string
	var err error
	switch req.Type {
	case "", "m.login.password":
		// An account that already exists here is answered locally. Sending a
		// numeric Yunhu id to loginPassword would classify it as a phone number
		// and start the SMS flow, which is what made "log in with my Matrix ID"
		// bounce into the Yunhu phone login instead of reusing the session.
		if acc := s.localAccountFor(identifier); acc != nil {
			if acc.Token == "" {
				writeError(c.W, http.StatusForbidden, ErrForbidden,
					"该账号在本服务端没有保存云湖 token，请用手机号 / 邮箱 / token 重新登录一次")
				return
			}
			if !s.reloginPermitted(acc, req.Password) {
				writeError(c.W, http.StatusForbidden, ErrForbidden,
					"该账号已存在于本服务端，但 server.allow_relogin 已关闭：密码需要填这个账号的云湖 token")
				return
			}
			s.Log.Info("local relogin",
				"user", acc.UserID, "yunhu_id", acc.YunhuID,
				"credential_free", s.Cfg.Server.AllowRelogin)
			writeJSON(c.W, http.StatusOK, s.mintDevice(acc, req.DeviceID, req.DeviceName))
			return
		}
		token, err = s.loginPassword(ctx, yc, username, req.Password)
	case "com.yunhu.token", "m.login.token":
		raw := orDefault(req.Token, req.Password)
		token, err = s.loginWithToken(ctx, yc, raw)
	case "com.yunhu.sms":
		phone := orDefault(req.Phone, username)
		token, err = yc.LoginSMS(ctx, phone, req.Captcha, s.yunhuDeviceID(req.DeviceID), s.Cfg.Yunhu.Platform)
	case "com.yunhu.email":
		email := orDefault(req.Email, username)
		token, err = yc.LoginEmail(ctx, email, req.Password, s.yunhuDeviceID(req.DeviceID), s.Cfg.Yunhu.Platform)
	default:
		writeError(c.W, http.StatusBadRequest, ErrUnknown, "Unsupported login type: "+req.Type)
		return
	}
	if err != nil {
		s.loginError(c, err)
		return
	}

	info, err := yc.WithToken(token).GetUserInfo(ctx)
	if err != nil {
		s.loginError(c, err)
		return
	}

	resp, err := s.completeLogin(info, token, req.DeviceID, req.DeviceName)
	if err != nil {
		writeError(c.W, http.StatusInternalServerError, ErrUnknown, err.Error())
		return
	}
	writeJSON(c.W, http.StatusOK, resp)
}

// localAccountFor resolves a login identifier to an account that already exists
// on this homeserver, without touching Yunhu.
//
// Clients send several shapes and all of them have to work:
//
//	@yh_8516939:localhost    full Matrix ID (Element X, m.id.user)
//	yh_8516939               localpart only
//	8516939                  the Yunhu id itself
//	+8613800138000 / email   whatever was stored when the account was created
//
// There is no federation here, so an ID whose domain is not this server is
// still resolved by localpart: it can only ever mean the local account.
func (s *Server) localAccountFor(identifier string) *model.Account {
	raw := strings.TrimSpace(identifier)
	if raw == "" {
		return nil
	}

	local := strings.TrimPrefix(raw, "@")
	if i := strings.IndexByte(local, ':'); i >= 0 {
		local = local[:i]
	}
	bare := strings.TrimPrefix(local, userPrefixUser)
	fullID := "@" + local + ":" + s.Cfg.Server.Name

	// A client that pastes a full ID into a field which already adds "@" and
	// ":server" produces garbage such as "@//yh_8516939/:localhost". It has been
	// seen in the wild, so try the squashed form too instead of creating a new
	// account for a typo.
	candidates := []string{bare, local}
	if strings.ContainsAny(local, "/@") {
		squashed := strings.NewReplacer("/", "", "@", "").Replace(local)
		candidates = append(candidates, squashed, strings.TrimPrefix(squashed, userPrefixUser))
	}

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	for _, key := range candidates {
		if key == "" {
			continue
		}
		if acc := s.Store.State.Accounts[key]; acc != nil {
			return acc
		}
	}
	for _, acc := range s.Store.State.Accounts {
		switch {
		case acc.UserID == raw, acc.UserID == fullID:
			return acc
		case acc.Phone != "" && acc.Phone == raw:
			return acc
		case acc.Email != "" && strings.EqualFold(acc.Email, raw):
			return acc
		}
	}
	return nil
}

// reloginPermitted decides whether an existing account may be handed a fresh
// access token without asking Yunhu for credentials.
func (s *Server) reloginPermitted(acc *model.Account, password string) bool {
	if s.Cfg.Server.AllowRelogin {
		return true
	}
	if password == "" || acc.Token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(password), []byte(acc.Token)) == 1
}

func (s *Server) loginPassword(ctx context.Context, yc *yunhu.Client, username, password string) (string, error) {
	switch {
	case username == "":
		return "", fmt.Errorf("missing username")
	case strings.Contains(username, "@"):
		return yc.LoginEmail(ctx, username, password, s.yunhuDeviceID(""), s.Cfg.Yunhu.Platform)
	case looksLikeToken(password):
		return s.loginWithToken(ctx, yc, password)
	case isDigits(username):
		return yc.LoginSMS(ctx, username, password, s.yunhuDeviceID(""), s.Cfg.Yunhu.Platform)
	default:
		return "", fmt.Errorf("无法识别的账号格式：邮箱请填完整邮箱地址，手机号请填 11 位号码并输入短信验证码，token 登录请把 token 填在密码栏")
	}
}

func (s *Server) loginWithToken(ctx context.Context, yc *yunhu.Client, token string) (string, error) {
	if token == "" {
		return "", fmt.Errorf("missing token")
	}
	if _, err := yc.WithToken(token).GetUserInfo(ctx); err != nil {
		return "", err
	}
	return token, nil
}

func looksLikeToken(s string) bool {
	return len(s) >= 20 && strings.Contains(s, "-")
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Server) yunhuDeviceID(deviceID string) string {
	if deviceID == "" {
		return s.Cfg.Yunhu.DevicePrefix + "-" + randomHex(6)
	}
	return s.Cfg.Yunhu.DevicePrefix + "-" + deviceID
}

// completeLogin creates or refreshes the account and device records.
func (s *Server) completeLogin(info *yunhu.UserInfo, token, deviceID, deviceName string) (map[string]any, error) {
	s.Store.Mu.Lock()
	acc := s.Store.State.Accounts[info.ID]
	if acc == nil {
		acc = model.NewAccount(info.ID, s.accountUserID(info.ID))
	}
	acc.Token = token
	acc.Name = orDefault(info.Name, acc.Name)
	acc.AvatarURL = orDefault(info.AvatarURL, acc.AvatarURL)
	acc.Phone = orDefault(info.Phone, acc.Phone)
	acc.Email = orDefault(info.Email, acc.Email)
	acc.IsVIP = info.IsVIP
	acc.LastLoginTS = time.Now().UnixMilli()
	if acc.Devices == nil {
		acc.Devices = map[string]*model.Device{}
	}
	s.Store.Mu.Unlock()

	return s.mintDevice(acc, deviceID, deviceName), nil
}

// mintDevice registers a new session for an account, brings it online and builds
// the /login response. Shared by the Yunhu flows and by a local relogin, which
// has no upstream round trip at all.
func (s *Server) mintDevice(acc *model.Account, deviceID, deviceName string) map[string]any {
	if deviceID == "" {
		deviceID = strings.ToUpper(randomHex(5))
	}
	accessToken := randomToken()
	now := time.Now().UnixMilli()
	dev := &model.Device{
		ID:            deviceID,
		DisplayName:   orDefault(deviceName, "yhmatrix"),
		Token:         accessToken,
		LastSeen:      now,
		CreatedTS:     now,
		YunhuDeviceID: s.yunhuDeviceID(deviceID),
	}
	s.Store.Mu.Lock()
	acc.LastLoginTS = now
	s.Store.Mu.Unlock()
	s.Store.PutAccount(acc)
	s.Store.AddDevice(acc, dev)

	// Bring the account online: websocket plus conversation import.
	s.StartRunner(context.Background(), acc)
	s.safego("refresh-after-login", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		s.refreshAccount(ctx, acc, dev.YunhuDeviceID)
	})

	resp := map[string]any{
		"user_id":      acc.UserID,
		"access_token": accessToken,
		"device_id":    deviceID,
		"home_server":  s.Cfg.Server.Name,
	}
	if s.Cfg.Server.PublicURL != "" {
		resp["well_known"] = map[string]any{
			"m.homeserver": map[string]any{"base_url": s.Cfg.Server.PublicURL},
		}
	}
	return resp
}

func (s *Server) loginError(c *Ctx, err error) {
	var apiErr *yunhu.APIError
	if asAPIError(err, &apiErr) {
		if apiErr.IsUnauthorized() {
			writeError(c.W, http.StatusForbidden, ErrForbidden, "云湖拒绝了该凭证："+apiErr.Error())
			return
		}
		writeError(c.W, http.StatusForbidden, ErrForbidden, apiErr.Error())
		return
	}
	writeError(c.W, http.StatusForbidden, ErrForbidden, err.Error())
}

func asAPIError(err error, target **yunhu.APIError) bool {
	for err != nil {
		if e, ok := err.(*yunhu.APIError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// logout removes the current device.
func (s *Server) logout(c *Ctx) {
	acc, dev := c.Account, c.Device
	s.Store.RemoveDevice(acc, dev.ID)
	if len(acc.Devices) == 0 {
		if r := s.runnerFor(acc.YunhuID); r != nil {
			r.Stop()
			s.runnerMu.Lock()
			delete(s.runners, acc.YunhuID)
			s.runnerMu.Unlock()
		}
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// logoutAll removes every device.
func (s *Server) logoutAll(c *Ctx) {
	acc := c.Account
	s.Store.Mu.Lock()
	ids := make([]string, 0, len(acc.Devices))
	for id := range acc.Devices {
		ids = append(ids, id)
	}
	s.Store.Mu.Unlock()
	for _, id := range ids {
		s.Store.RemoveDevice(acc, id)
	}
	if r := s.runnerFor(acc.YunhuID); r != nil {
		r.Stop()
		s.runnerMu.Lock()
		delete(s.runners, acc.YunhuID)
		s.runnerMu.Unlock()
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// whoami returns the identity bound to the access token.
func (s *Server) whoami(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{
		"user_id":   c.Account.UserID,
		"device_id": c.Device.ID,
		"is_guest":  false,
	})
}

// register is intentionally unsupported: accounts are Yunhu accounts.
func (s *Server) register(c *Ctx) {
	writeError(c.W, http.StatusForbidden, ErrForbidden,
		"本服务不提供独立注册，请使用云湖账号登录（支持云湖 token / 手机号+验证码 / 邮箱+密码）")
}

// registerAvailable always reports the name as taken.
func (s *Server) registerAvailable(c *Ctx) {
	writeJSON(c.W, http.StatusOK, map[string]any{"available": false})
}

// listDevices enumerates the account's sessions.
func (s *Server) listDevices(c *Ctx) {
	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()
	devices := make([]map[string]any, 0, len(c.Account.Devices))
	for _, d := range c.Account.Devices {
		devices = append(devices, map[string]any{
			"device_id":    d.ID,
			"display_name": d.DisplayName,
			"last_seen_ip": d.IP,
			"last_seen_ts": d.LastSeen,
		})
	}
	writeJSON(c.W, http.StatusOK, map[string]any{"devices": devices})
}

// getDevice returns one session.
func (s *Server) getDevice(c *Ctx) {
	s.Store.Mu.RLock()
	d := c.Account.Devices[c.Param("deviceId")]
	s.Store.Mu.RUnlock()
	if d == nil {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Unknown device")
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{
		"device_id":    d.ID,
		"display_name": d.DisplayName,
		"last_seen_ip": d.IP,
		"last_seen_ts": d.LastSeen,
	})
}

// updateDevice renames a session.
func (s *Server) updateDevice(c *Ctx) {
	var body struct {
		DisplayName string `json:"display_name"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)
	s.Store.Mu.Lock()
	if d := c.Account.Devices[c.Param("deviceId")]; d != nil && body.DisplayName != "" {
		d.DisplayName = body.DisplayName
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(c.Account.YunhuID)
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// deleteDevice removes a session.
func (s *Server) deleteDevice(c *Ctx) {
	s.Store.RemoveDevice(c.Account, c.Param("deviceId"))
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// deleteDevices removes several sessions.
func (s *Server) deleteDevices(c *Ctx) {
	var body struct {
		Devices []string `json:"devices"`
	}
	_ = json.NewDecoder(c.R.Body).Decode(&body)
	for _, id := range body.Devices {
		s.Store.RemoveDevice(c.Account, id)
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// deactivate logs the account out everywhere.
func (s *Server) deactivate(c *Ctx) {
	s.logoutAll(c)
}

// requestSMSCode asks Yunhu to send an SMS code (helper for phone login).
func (s *Server) requestSMSCode(c *Ctx) {
	var body struct {
		Phone       string `json:"phone"`
		Mobile      string `json:"mobile"`
		CaptchaID   string `json:"captcha_id"`
		CaptchaCode string `json:"captcha_code"`
		CaptchaType string `json:"captcha_type"`
		X           int    `json:"x"`
	}
	if err := json.NewDecoder(c.R.Body).Decode(&body); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrBadJSON, "Invalid JSON")
		return
	}
	phone := orDefault(body.Phone, body.Mobile)
	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	if err := s.YunhuClient().RequestSMSCode(ctx, phone, body.CaptchaCode, body.CaptchaType, body.CaptchaID, body.X, s.Cfg.Yunhu.Platform); err != nil {
		writeError(c.W, http.StatusBadRequest, ErrUnknown, err.Error())
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// captcha returns a slider captcha challenge for the SMS flow.
func (s *Server) captcha(c *Ctx) {
	ctx, cancel := context.WithTimeout(c.R.Context(), s.Cfg.Timeout())
	defer cancel()
	ch, err := s.YunhuClient().GetCaptcha(ctx)
	if err != nil {
		writeError(c.W, http.StatusBadGateway, ErrUnknown, err.Error())
		return
	}
	writeJSON(c.W, http.StatusOK, map[string]any{
		"id":            ch.ID,
		"master_image":  ch.MasterImage,
		"tile_image":    ch.TileImage,
		"tile_y":        ch.TileY,
		"type":          ch.Type,
		"master_width":  ch.MasterWidth,
		"master_height": ch.MasterHeight,
	})
}
