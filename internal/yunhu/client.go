// Package yunhu implements a typed client for the Yunhu (云湖) chat API.
//
// The upstream API is inconsistent: some endpoints speak protobuf, others plain
// JSON, and the documentation does not always say which. The client therefore
// negotiates the encoding at runtime:
//
//   - requests are sent as protobuf by default, with an automatic JSON
//     (protojson) retry when the server rejects the binary body;
//   - responses are decoded by sniffing the payload, so a JSON reply to a
//     "protobuf" endpoint still works.
//
// All application level failures are surfaced as *APIError, which carries the
// Yunhu status code (1 == success).
package yunhu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// APIError is returned for both transport level failures and Yunhu status
// codes. Code is 0 when the request never reached the application layer.
type APIError struct {
	Path   string
	Status int
	Code   int
	Msg    string
	Body   string
	Err    error
}

func (e *APIError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("yunhu %s: %v", e.Path, e.Err)
	}
	if e.Code != 0 {
		return fmt.Sprintf("yunhu %s: code=%d msg=%s", e.Path, e.Code, e.Msg)
	}
	return fmt.Sprintf("yunhu %s: http %d: %s", e.Path, e.Status, truncate(e.Body, 200))
}

// Unwrap exposes the underlying transport error.
func (e *APIError) Unwrap() error { return e.Err }

// IsUnauthorized reports whether Yunhu rejected the token.
func (e *APIError) IsUnauthorized() bool {
	return e.Code == -101 || e.Status == http.StatusUnauthorized
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Client talks to the Yunhu REST API.
type Client struct {
	BaseURL  string
	Token    string
	Platform string
	DeviceID string
	Wire     string
	HTTP     *http.Client
	Log      *slog.Logger
}

// NewClient builds a client for baseURL.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Platform: "windows",
		Wire:     "auto",
		HTTP:     &http.Client{Timeout: timeout},
		Log:      slog.Default(),
	}
}

// WithToken returns a shallow copy bound to a user token.
func (c *Client) WithToken(token string) *Client {
	cp := *c
	cp.Token = token
	return &cp
}

// WithDevice returns a shallow copy bound to a device identifier.
func (c *Client) WithDevice(id string) *Client {
	cp := *c
	cp.DeviceID = id
	return &cp
}

// ---------------------------------------------------------------------------
// transport
// ---------------------------------------------------------------------------

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	u := c.BaseURL + path
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if c.Token != "" {
		req.Header.Set("token", c.Token)
	}
	req.Header.Set("User-Agent", "yhmatrix/1.0 (+matrix homeserver)")
	req.Header.Set("Accept", "application/x-protobuf, application/json")
	if c.Platform != "" {
		req.Header.Set("platform", c.Platform)
	}
	return req, nil
}

// doJSON performs a JSON request/response exchange.
func (c *Client) doJSON(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return &APIError{Path: path, Err: err}
		}
		body = bytes.NewReader(raw)
	}
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	if resp.StatusCode >= 400 {
		return &APIError{Path: path, Status: resp.StatusCode, Body: string(raw)}
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return &APIError{Path: path, Status: resp.StatusCode, Body: string(raw), Err: err}
		}
	}
	return nil
}

// jsonEnvelope is the common {code,msg,data} wrapper.
type jsonEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// doEnvelope performs a JSON request and decodes the data field into out.
func (c *Client) doEnvelope(ctx context.Context, method, path string, in, out any) error {
	var env jsonEnvelope
	if err := c.doJSON(ctx, method, path, in, &env); err != nil {
		return err
	}
	if env.Code != 1 {
		return &APIError{Path: path, Code: env.Code, Msg: env.Msg}
	}
	if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
		if err := json.Unmarshal(env.Data, out); err != nil {
			return &APIError{Path: path, Err: fmt.Errorf("decode data: %w", err)}
		}
	}
	return nil
}

// doProto performs a protobuf request/response exchange with wire negotiation.
func (c *Client) doProto(ctx context.Context, path string, in, out proto.Message) error {
	encodings := []string{"proto"}
	switch c.Wire {
	case "json":
		encodings = []string{"json"}
	case "proto":
		encodings = []string{"proto"}
	default: // auto
		encodings = []string{"proto", "json"}
	}

	var lastErr error
	for _, enc := range encodings {
		err := c.doProtoOnce(ctx, path, enc, in, out)
		if err == nil {
			return nil
		}
		lastErr = err
		// Only retry on protocol level rejections; an application error is final.
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Code != 0 {
			return err
		}
	}
	return lastErr
}

func (c *Client) doProtoOnce(ctx context.Context, path, encoding string, in, out proto.Message) error {
	var payload []byte
	var contentType string
	var err error
	switch encoding {
	case "json":
		payload, err = protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}.Marshal(in)
		contentType = "application/json"
	default:
		payload, err = proto.Marshal(in)
		contentType = "application/x-protobuf"
	}
	if err != nil {
		return &APIError{Path: path, Err: err}
	}

	req, err := c.newRequest(ctx, http.MethodPost, path, bytes.NewReader(payload))
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return &APIError{Path: path, Err: err}
	}
	if resp.StatusCode >= 400 {
		return &APIError{Path: path, Status: resp.StatusCode, Body: string(raw)}
	}
	if out == nil {
		return nil
	}
	if err := decodeMessage(raw, out); err != nil {
		return &APIError{Path: path, Status: resp.StatusCode, Body: string(raw), Err: err}
	}
	return checkStatus(path, out)
}

func decodeMessage(raw []byte, out proto.Message) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(trimmed, out)
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, out); err == nil {
		return nil
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(trimmed, out)
}

// checkStatus inspects an embedded `Status status = 1` field and converts a
// non-success code into an APIError.
func checkStatus(path string, m proto.Message) error {
	mr := m.ProtoReflect()
	if !mr.IsValid() {
		return nil
	}
	fd := mr.Descriptor().Fields().ByName("status")
	if fd == nil || fd.Kind() != protoreflect.MessageKind {
		return nil
	}
	sm := mr.Get(fd).Message()
	if !sm.IsValid() {
		return nil
	}
	codeFD := sm.Descriptor().Fields().ByName("code")
	msgFD := sm.Descriptor().Fields().ByName("msg")
	if codeFD == nil {
		return nil
	}
	code := int(sm.Get(codeFD).Int())
	if code == 1 || code == 0 {
		return nil
	}
	msg := ""
	if msgFD != nil {
		msg = sm.Get(msgFD).String()
	}
	return &APIError{Path: path, Code: code, Msg: msg}
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

// UserInfo is the subset of /v1/user/info we care about.
type UserInfo struct {
	ID        string
	Name      string
	AvatarURL string
	Phone     string
	Email     string
	IsVIP     bool
	Coin      float64
}

// GetUserInfo fetches the account bound to the client token.
func (c *Client) GetUserInfo(ctx context.Context) (*UserInfo, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/v1/user/info", nil)
	if err != nil {
		return nil, &APIError{Path: "/v1/user/info", Err: err}
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, &APIError{Path: "/v1/user/info", Err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, &APIError{Path: "/v1/user/info", Err: err}
	}
	if resp.StatusCode >= 400 {
		return nil, &APIError{Path: "/v1/user/info", Status: resp.StatusCode, Body: string(raw)}
	}

	var msg pb.UserInfo
	if err := decodeMessage(raw, &msg); err != nil {
		return nil, &APIError{Path: "/v1/user/info", Body: string(raw), Err: err}
	}
	if err := checkStatus("/v1/user/info", &msg); err != nil {
		return nil, err
	}
	d := msg.GetData()
	if d == nil || d.GetId() == "" {
		return nil, &APIError{Path: "/v1/user/info", Err: errors.New("empty user info")}
	}
	return &UserInfo{
		ID:        d.GetId(),
		Name:      d.GetName(),
		AvatarURL: d.GetAvatarUrl(),
		Phone:     d.GetPhone(),
		Email:     d.GetEmail(),
		IsVIP:     d.GetIsVip(),
		Coin:      d.GetCoin(),
	}, nil
}

// LoginSMS exchanges an SMS code for an account token.
func (c *Client) LoginSMS(ctx context.Context, mobile, captcha, deviceID, platform string) (string, error) {
	in := map[string]any{
		"mobile":   mobile,
		"captcha":  captcha,
		"deviceId": deviceID,
		"platform": platform,
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := c.doEnvelope(ctx, http.MethodPost, "/v1/user/verification-login", in, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", &APIError{Path: "/v1/user/verification-login", Err: errors.New("no token in response")}
	}
	return out.Token, nil
}

// LoginEmail exchanges email + password for an account token.
func (c *Client) LoginEmail(ctx context.Context, email, password, deviceID, platform string) (string, error) {
	in := map[string]any{
		"email":    email,
		"password": password,
		"deviceId": deviceID,
		"platform": platform,
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := c.doEnvelope(ctx, http.MethodPost, "/v1/user/email-login", in, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", &APIError{Path: "/v1/user/email-login", Err: errors.New("no token in response")}
	}
	return out.Token, nil
}

// Captcha is a slider captcha challenge.
type Captcha struct {
	ID           string  `json:"id"`
	MasterImage  string  `json:"master_image"`
	TileImage    string  `json:"tile_image"`
	TileY        float64 `json:"tile_y"`
	Type         string  `json:"type"`
	MasterWidth  float64 `json:"-"`
	MasterHeight float64 `json:"-"`
}

// GetCaptcha fetches a slider captcha used before requesting an SMS code.
func (c *Client) GetCaptcha(ctx context.Context) (*Captcha, error) {
	var out struct {
		ID          string  `json:"id"`
		MasterImage string  `json:"master_image"`
		TileImage   string  `json:"tile_image"`
		TileY       float64 `json:"tile_y"`
		Type        string  `json:"type"`
		MasterSize  struct {
			Width  float64 `json:"width"`
			Height float64 `json:"height"`
		} `json:"master_size"`
	}
	if err := c.doEnvelope(ctx, http.MethodPost, "/v1/captcha/get", nil, &out); err != nil {
		return nil, err
	}
	return &Captcha{
		ID: out.ID, MasterImage: out.MasterImage, TileImage: out.TileImage,
		TileY: out.TileY, Type: out.Type,
		MasterWidth: out.MasterSize.Width, MasterHeight: out.MasterSize.Height,
	}, nil
}

// RequestSMSCode asks Yunhu to send an SMS verification code.
func (c *Client) RequestSMSCode(ctx context.Context, mobile, captchaCode, captchaType, captchaID string, x int, platform string) error {
	in := map[string]any{
		"mobile":       mobile,
		"code":         captchaCode,
		"captcha_type": captchaType,
		"id":           captchaID,
		"x":            x,
		"platform":     platform,
	}
	return c.doEnvelope(ctx, http.MethodPost, "/v1/verification/get-verification-code", in, nil)
}

// RequestEmailCode asks Yunhu to send an email verification code.
func (c *Client) RequestEmailCode(ctx context.Context, email, typ, captchaCode, captchaID string) error {
	in := map[string]any{
		"email": email,
		"typ":   typ,
		"code":  captchaCode,
		"id":    captchaID,
	}
	return c.doEnvelope(ctx, http.MethodPost, "/v1/verification/get-email-verification-code", in, nil)
}

// Logout invalidates the current token on the Yunhu side.
func (c *Client) Logout(ctx context.Context, deviceID string) error {
	in := map[string]any{"device-id": deviceID}
	return c.doEnvelope(ctx, http.MethodPost, "/v1/user/logout", in, nil)
}

// ---------------------------------------------------------------------------
// messages
// ---------------------------------------------------------------------------

// SendMessage posts a message. The request must have MsgId, ChatId, ChatType
// and ContentType set.
func (c *Client) SendMessage(ctx context.Context, req *pb.SendMsgRequest) error {
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/msg/send-message", req, &out)
}

// EditMessage rewrites an existing message.
func (c *Client) EditMessage(ctx context.Context, req *pb.SendMsgRequest) error {
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/msg/edit-message", req, &out)
}

// RecallMessage retracts messages.
func (c *Client) RecallMessage(ctx context.Context, chatID string, chatType pb.ChatType, msgIDs []string) error {
	req := &pb.RecallMsg{MsgId: msgIDs, ChatId: chatID, ChatType: chatType}
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/msg/recall-msg", req, &out)
}

// ButtonReport reports a click on an interactive message button.
func (c *Client) ButtonReport(ctx context.Context, msgID, chatID, userID, value string, chatType pb.ChatType) error {
	req := &pb.ButtonReportRequest{
		MsgId: msgID, ChatId: chatID, UserId: userID, ButtonValue: value, ChatType: chatType,
	}
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/msg/button-report", req, &out)
}

// ListMessages returns messages before msgID. An empty msgID asks for the most
// recent page.
func (c *Client) ListMessages(ctx context.Context, chatID string, chatType pb.ChatType, msgID string, count int32) (*pb.ListMsgResponse, error) {
	req := &pb.ListMsgRequest{MsgId: msgID, MsgCount: count, ChatId: chatID, ChatType: chatType}
	var out pb.ListMsgResponse
	if err := c.doProto(ctx, "/v1/msg/list-message", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListMessagesBySeq returns messages starting at a sequence number.
func (c *Client) ListMessagesBySeq(ctx context.Context, chatID string, chatType pb.ChatType, start uint64) (*pb.ListMsgResponse, error) {
	req := &pb.ListMsgBySeqRequest{MsgStart: start, ChatId: chatID, ChatType: chatType}
	var out pb.ListMsgResponse
	if err := c.doProto(ctx, "/v1/msg/list-message-by-seq", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListMessagesByMidSeq fetches a window around one message id.
//
// This is the endpoint Yunhu's own client uses to hydrate a message it only saw
// as a websocket edit: the edit push carries a partial content object ("编辑消息
// 有缺失" per the proto comment), so the authoritative text has to be read back.
func (c *Client) ListMessagesByMidSeq(ctx context.Context, chatID string, chatType pb.ChatType, msgID string, count int32) (*pb.ListMsgResponse, error) {
	req := &pb.ListMsgByMidSeqRequest{
		ChatId:   chatID,
		ChatType: chatType,
		MsgId:    msgID,
		MsgCount: count,
		// The official client sends 0 here and -1 (uint64 max) for msg_seq.
		Unknown: 0,
		MsgSeq:  ^uint64(0),
	}
	var out pb.ListMsgResponse
	if err := c.doProto(ctx, "/v1/msg/list-message-by-mid-seq", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// conversations & groups
// ---------------------------------------------------------------------------

// Conversations lists the account's conversation sidebar.
func (c *Client) Conversations(ctx context.Context, md5 string) (*pb.ConversationListResponse, error) {
	req := &pb.ConversationListRequest{Md5: md5}
	var out pb.ConversationListResponse
	if err := c.doProto(ctx, "/v1/conversation/list", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SetGroupMemberLevel promotes or demotes a group member.
//
// POST /v1/group/manage-setting with {"groupId","userId","userLevel"}, where
// userLevel is 0 for an ordinary member and 2 for an administrator (100 is the
// owner and cannot be granted). This endpoint is NOT in the public API docs - it
// is the one Yunhu's own Android client uses (GroupRepository.setMemberRole), so
// it is the only way to make the matrix power_levels panel actually do something
// upstream.
//
// The response body is deliberately not decoded: the official client only checks
// the HTTP status, and the shape is undocumented. Guessing it once already caused
// a decoder mismatch that broke a different endpoint.
func (c *Client) SetGroupMemberLevel(ctx context.Context, groupID, userID string, level int32) error {
	in := map[string]any{"groupId": groupID, "userId": userID, "userLevel": level}
	return c.doJSON(ctx, http.MethodPost, "/v1/group/manage-setting", in, nil)
}

// GroupMemberLevels pages the roster and returns user id -> permission_level.
//
// It is only used as a fallback: the authoritative list of admins is
// group/info's `admin` field, because the roster is paginated and a big group's
// admins can sit past the page limit.
func (c *Client) GroupMemberLevels(ctx context.Context, groupID string, max int) (map[string]int32, error) {
	const pageSize = 100
	out := map[string]int32{}
	for page := int32(1); page <= int32(max); page++ {
		resp, err := c.ListMembers(ctx, groupID, "", page, pageSize)
		if err != nil {
			return out, err
		}
		members := resp.GetUser()
		for _, m := range members {
			if id := m.GetUserInfo().GetUserId(); id != "" {
				out[id] = m.GetPermissionLevel()
			}
		}
		if len(members) < pageSize {
			break
		}
	}
	return out, nil
}

// DismissNotification marks a conversation as read.
func (c *Client) DismissNotification(ctx context.Context, chatID string) error {
	in := map[string]any{"chatId": chatID}
	return c.doEnvelope(ctx, http.MethodPost, "/v1/conversation/dismiss-notification", in, nil)
}

// SetConversationMuted mutes or unmutes a conversation.
//
// Yunhu spells this "no-notify" and sends 0/1, not a boolean: the documented body
// is {"chatId": "...", "noNotify": 0|1}. Both the request and the response are
// JSON ({code,msg}), so this goes through the ordinary envelope path.
//
// (An earlier version of this used the protobuf path on the strength of a test
// double that answered with a StatusResponse; the real endpoint answers JSON, and
// the mismatch showed up in production as "proto: cannot parse invalid wire-format
// data". A fake that does not match the wire is worse than no fake.)
func (c *Client) SetConversationMuted(ctx context.Context, chatID string, muted bool) error {
	noNotify := 0
	if muted {
		noNotify = 1
	}
	in := map[string]any{"chatId": chatID, "noNotify": noNotify}
	return c.doEnvelope(ctx, http.MethodPost, "/v1/friend/no-notify", in, nil)
}

// GroupInfo fetches group metadata, including the authoritative headcount.
func (c *Client) GroupInfo(ctx context.Context, groupID string) (*pb.GroupInfoResponse, error) {
	req := &pb.GroupInfoRequest{GroupId: groupID}
	var out pb.GroupInfoResponse
	if err := c.doProto(ctx, "/v1/group/info", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Board fetches every bot board (群机器人看板) of a conversation.
//
// A group can host several bots and each of them maintains its own board, so
// the response is a list even when the common case is a single entry. Bot
// conversations have exactly one bot, hence one board.
func (c *Client) Board(ctx context.Context, id string, chatType pb.ChatType) (*pb.BoardResponse, error) {
	req := &pb.BoardRequest{Id: id, ChatType: chatType}
	var out pb.BoardResponse
	if err := c.doProto(ctx, "/v1/bot/board", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListMembers pages through group members.
func (c *Client) ListMembers(ctx context.Context, groupID, keyword string, page, size int32) (*pb.ListMemberResponse, error) {
	req := &pb.ListMemberRequest{
		Data:     &pb.ListMemberRequest_Data{Page: page, Size: size},
		GroupId:  groupID,
		Keywords: keyword,
	}
	var out pb.ListMemberResponse
	if err := c.doProto(ctx, "/v1/group/list-member", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateGroup creates a group chat and returns its ID.
func (c *Client) CreateGroup(ctx context.Context, name, intro, avatarURL string) (string, error) {
	req := &pb.CreateGroupRequest{Name: name, Introduction: intro, AvatarUrl: avatarURL}
	var out pb.CreateGroupResponse
	if err := c.doProto(ctx, "/v1/group/create-group", req, &out); err != nil {
		return "", err
	}
	return out.GetGroupId(), nil
}

// EditNickname changes the account display name upstream.
func (c *Client) EditNickname(ctx context.Context, name string) error {
	req := &pb.EditNicknameRequest{Name: name}
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/user/edit-nickname", req, &out)
}

// EditAvatar changes the account avatar upstream. url must be absolute.
func (c *Client) EditAvatar(ctx context.Context, url string) error {
	req := &pb.EditAvatarRequest{Url: url}
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/user/edit-avatar", req, &out)
}

// EditGroup updates group metadata.
func (c *Client) EditGroup(ctx context.Context, req *pb.EditGroupRequest) error {
	var out pb.StatusResponse
	return c.doProto(ctx, "/v1/group/edit-group", req, &out)
}

// GetUser fetches another account's profile.
func (c *Client) GetUser(ctx context.Context, id string) (*pb.GetUserResponse, error) {
	req := &pb.GetUserRequest{Id: id}
	var out pb.GetUserResponse
	if err := c.doProto(ctx, "/v1/user/get-user", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// media
// ---------------------------------------------------------------------------

// QiniuToken returns an upload token for the given kind:
// image, file, video, audio.
func (c *Client) QiniuToken(ctx context.Context, kind string) (string, error) {
	path := "/v1/misc/qiniu-token"
	switch kind {
	case "file":
		path = "/v1/misc/qiniu-token2"
	case "video":
		path = "/v1/misc/qiniu-token-video"
	case "audio":
		path = "/v1/misc/qiniu-token-audio"
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := c.doEnvelope(ctx, http.MethodGet, path, nil, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", &APIError{Path: path, Err: errors.New("empty qiniu token")}
	}
	return out.Token, nil
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string { return quoteEscaper.Replace(s) }

// YunhuMediaReferer is mandatory for every object-storage host under
// jwznb.com. Without it the CDN answers
//
//	HTTP/1.1 403 Forbidden
//	Server: openresty
//	X-Error-Detail: RHIE
//
// which is a referer hotlink check, not a missing file. The Yunhu API reference
// states it plainly:
//
//	云湖数据床地址需要请求头加上 Referer: http://myapp.jwznb.com 才可正常获取内容，否则会403
//
// Note the scheme: it is http, and there is no trailing slash. Sending https
// or the bare host still 403s.
const YunhuMediaReferer = "http://myapp.jwznb.com"

// yunhuStorageHosts are the CDN hosts listed in the API reference. Every one of
// them sits behind the same referer check, so match on the parent domain rather
// than enumerating chat-img / chat-img2 / chat-img3 / chat-video1 / chat-audio1
// / chat-file / chat-storage1 / chat-file-oss one by one.
func isYunhuStorageHost(host string) bool {
	host = strings.ToLower(host)
	return host == "jwznb.com" || strings.HasSuffix(host, ".jwznb.com")
}

// Fetch downloads a remote URL, used to proxy Yunhu media through the
// homeserver so clients only ever talk to us.
func (c *Client) Fetch(ctx context.Context, rawURL string, maxBytes int64) ([]byte, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, "", fmt.Errorf("unsupported media url %q", rawURL)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", "yhmatrix/1.0")
	if isYunhuStorageHost(u.Hostname()) {
		req.Header.Set("Referer", YunhuMediaReferer)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		// X-Error-Detail is how the CDN explains itself: RHIE means the referer
		// check failed, WTR means the signed URL is missing or expired. Surface
		// it, otherwise a 403 is indistinguishable from a deleted file.
		detail := resp.Header.Get("X-Error-Detail")
		return nil, "", &APIError{Path: rawURL, Status: resp.StatusCode, Body: detail}
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return nil, "", err
	}
	return data, resp.Header.Get("Content-Type"), nil
}

// ParseRetryAfter is a helper for callers that want to honour backoff hints.
func ParseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return time.Until(t)
	}
	return 0
}
