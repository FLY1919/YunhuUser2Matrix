package yunhu

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// This file implements a small in-process replica of the Yunhu API. It exists
// so the homeserver can be exercised end to end (login -> sync -> send ->
// receive) without real credentials. It is enabled with `yunhu.mock: true` and
// is never used in production.

type mockUser struct {
	ID       string
	Name     string
	Avatar   string
	Phone    string
	Email    string
	Password string
	Token    string
}

type mockMessage struct {
	ID          string
	SenderID    string
	SenderType  pb.ChatType
	SenderName  string
	SenderAvat  string
	ContentType pb.ContentType
	Text        string
	ImageURL    string
	FileName    string
	FileURL     string
	FileSize    int64
	TS          int64
	Seq         uint64
	ChatID      string
	ChatType    pb.ChatType
	QuoteMsgID  string
}

type mockChat struct {
	ID      string
	Type    pb.ChatType
	Name    string
	Avatar  string
	Members []string
}

// MockServer emulates the Yunhu REST and websocket APIs.
type MockServer struct {
	mu      sync.Mutex
	users   map[string]*mockUser
	byToken map[string]*mockUser
	chats   map[string]*mockChat
	msgs    map[string][]*mockMessage // chat key -> messages
	seq     uint64
	subs    map[string]map[*websocket.Conn]struct{}
	blobs   map[string][]byte
	log     *slog.Logger
}

func chatKey(id string, t pb.ChatType) string {
	return strconv.Itoa(int(t)) + ":" + id
}

// NewMockServer seeds a demo account, a peer and a group.
func NewMockServer(log *slog.Logger) *MockServer {
	if log == nil {
		log = slog.Default()
	}
	m := &MockServer{
		users:   map[string]*mockUser{},
		byToken: map[string]*mockUser{},
		chats:   map[string]*mockChat{},
		msgs:    map[string][]*mockMessage{},
		subs:    map[string]map[*websocket.Conn]struct{}{},
		blobs:   map[string][]byte{},
		log:     log,
	}
	m.addUser(&mockUser{
		ID: "10001", Name: "云湖测试", Phone: "13800138000",
		Email: "demo@yunhu.dev", Password: "demo1234", Token: "mock-token-10001",
	})
	m.addUser(&mockUser{
		ID: "10002", Name: "林晚", Phone: "13900139000",
		Email: "lin@yunhu.dev", Password: "lin12345", Token: "mock-token-10002",
	})
	m.chats[chatKey("10002", pb.ChatType_user)] = &mockChat{
		ID: "10002", Type: pb.ChatType_user, Name: "林晚", Members: []string{"10001", "10002"},
	}
	m.chats[chatKey("20001", pb.ChatType_group)] = &mockChat{
		ID: "20001", Type: pb.ChatType_group, Name: "云湖开发群",
		Members: []string{"10001", "10002"},
	}
	m.seed("10002", pb.ChatType_user, "10002", "林晚", pb.ContentType_text, "在吗？Matrix 那边能收到消息吗", 12*time.Minute)
	m.seed("10002", pb.ChatType_user, "10001", "云湖测试", pb.ContentType_text, "能，刚接上 WebSocket", 11*time.Minute)
	m.seed("20001", pb.ChatType_group, "10002", "林晚", pb.ContentType_text, "群消息也要能同步过去", 6*time.Minute)
	m.seed("20001", pb.ChatType_group, "10001", "云湖测试", pb.ContentType_markdown, "**收到**，markdown 走 formatted_body", 5*time.Minute)
	return m
}

func (m *MockServer) addUser(u *mockUser) {
	m.users[u.ID] = u
	m.byToken[u.Token] = u
}

func (m *MockServer) seed(chatID string, chatType pb.ChatType, senderID, senderName string, ct pb.ContentType, text string, ago time.Duration) {
	m.seq++
	msg := &mockMessage{
		ID:          "mock" + strconv.FormatUint(m.seq, 10) + "abcdef",
		SenderID:    senderID,
		SenderType:  pb.ChatType_user,
		SenderName:  senderName,
		ContentType: ct,
		Text:        text,
		TS:          time.Now().Add(-ago).UnixMilli(),
		Seq:         m.seq,
		ChatID:      chatID,
		ChatType:    chatType,
	}
	k := chatKey(chatID, chatType)
	m.msgs[k] = append(m.msgs[k], msg)
}

// Handler returns the HTTP handler exposing the fake API.
func (m *MockServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", m.handleWS)
	mux.HandleFunc("/v1/", m.handleREST)
	mux.HandleFunc("/upload", m.handleUpload)
	mux.HandleFunc("/media/", m.handleMedia)
	return mux
}

func (m *MockServer) handleREST(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	token := r.Header.Get("token")

	switch r.URL.Path {
	case "/v1/user/email-login":
		var in struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		_ = json.Unmarshal(body, &in)
		for _, u := range m.users {
			if strings.EqualFold(u.Email, in.Email) && u.Password == in.Password {
				writeJSON(w, 1, "success", map[string]any{"token": u.Token})
				return
			}
		}
		writeJSON(w, 4021, "账号或密码错误", nil)
	case "/v1/user/verification-login":
		var in struct {
			Mobile  string `json:"mobile"`
			Captcha string `json:"captcha"`
		}
		_ = json.Unmarshal(body, &in)
		for _, u := range m.users {
			if u.Phone == in.Mobile {
				if in.Captcha == "" || len(in.Captcha) < 4 {
					writeJSON(w, 4022, "验证码错误", nil)
					return
				}
				writeJSON(w, 1, "success", map[string]any{"token": u.Token})
				return
			}
		}
		writeJSON(w, 4404, "手机号未注册", nil)
	case "/v1/user/info":
		u := m.byToken[token]
		if u == nil {
			writeJSON(w, -101, "未登录", nil)
			return
		}
		out := &pb.UserInfo{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: &pb.UserInfo_Data{
				Id: u.ID, Name: u.Name, Phone: u.Phone, Email: u.Email,
				AvatarUrl: u.Avatar, Coin: 12.5,
			},
		}
		writeProto(w, out)
	case "/v1/conversation/list":
		u := m.byToken[token]
		if u == nil {
			writeJSON(w, -101, "未登录", nil)
			return
		}
		writeProto(w, m.conversationList(u))
	case "/v1/msg/list-message", "/v1/msg/list-message-by-seq":
		u := m.byToken[token]
		if u == nil {
			writeJSON(w, -101, "未登录", nil)
			return
		}
		var chatID string
		var chatType pb.ChatType
		var count int32 = 30
		var beforeID string
		if r.URL.Path == "/v1/msg/list-message" {
			var in pb.ListMsgRequest
			_ = proto.Unmarshal(body, &in)
			if in.ChatId == "" {
				_ = protoJSONUnmarshal(body, &in)
			}
			chatID, chatType, beforeID = in.ChatId, in.ChatType, in.MsgId
			if in.MsgCount > 0 {
				count = in.MsgCount
			}
		} else {
			var in pb.ListMsgBySeqRequest
			_ = proto.Unmarshal(body, &in)
			if in.ChatId == "" {
				_ = protoJSONUnmarshal(body, &in)
			}
			chatID, chatType = in.ChatId, in.ChatType
		}
		writeProto(w, m.listMessages(chatID, chatType, beforeID, int(count)))
	case "/v1/msg/send-message":
		u := m.byToken[token]
		if u == nil {
			writeJSON(w, -101, "未登录", nil)
			return
		}
		var in pb.SendMsgRequest
		if err := proto.Unmarshal(body, &in); err != nil || in.ChatId == "" {
			_ = protoJSONUnmarshal(body, &in)
		}
		if in.ChatId == "" {
			writeJSON(w, 4000, "参数错误", nil)
			return
		}
		m.mu.Lock()
		m.seq++
		msg := &mockMessage{
			ID: in.MsgId, SenderID: u.ID, SenderType: pb.ChatType_user,
			SenderName: u.Name, SenderAvat: u.Avatar,
			ContentType: in.ContentType, TS: time.Now().UnixMilli(), Seq: m.seq,
			ChatID: in.ChatId, ChatType: in.ChatType, QuoteMsgID: in.QuoteMsgId,
		}
		if c := in.Content; c != nil {
			msg.Text = c.Text
			msg.ImageURL = c.ImageUrl
			msg.FileName = c.FileName
			msg.FileURL = c.FileUrl
			msg.FileSize = c.FileSize
		}
		if msg.ID == "" {
			msg.ID = "mock" + strconv.FormatUint(m.seq, 10)
		}
		k := chatKey(in.ChatId, in.ChatType)
		m.msgs[k] = append(m.msgs[k], msg)
		var recipients []string
		if c := m.chats[k]; c != nil {
			recipients = append([]string(nil), c.Members...)
		}
		m.mu.Unlock()

		for _, uid := range recipients {
			m.push(uid, "push_message", msg)
		}
		writeProto(w, &pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
	case "/v1/msg/recall-msg":
		writeProto(w, &pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
	case "/v1/msg/edit-message":
		writeProto(w, &pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
	case "/v1/conversation/dismiss-notification":
		writeJSON(w, 1, "success", nil)
	case "/v1/group/info":
		var in pb.GroupInfoRequest
		_ = proto.Unmarshal(body, &in)
		if in.GroupId == "" {
			_ = protoJSONUnmarshal(body, &in)
		}
		m.mu.Lock()
		c := m.chats[chatKey(in.GroupId, pb.ChatType_group)]
		m.mu.Unlock()
		if c == nil {
			writeJSON(w, 4402, "群聊不存在", nil)
			return
		}
		writeProto(w, &pb.GroupInfoResponse{
			Status: &pb.Status{Code: 1, Msg: "success"},
			Data: &pb.GroupData{
				GroupId: c.ID, Name: c.Name, AvatarUrl: c.Avatar,
				Headcount: uint64(len(c.Members)),
			},
		})
	case "/v1/group/list-member":
		var in pb.ListMemberRequest
		_ = proto.Unmarshal(body, &in)
		if in.GroupId == "" {
			_ = protoJSONUnmarshal(body, &in)
		}
		m.mu.Lock()
		c := m.chats[chatKey(in.GroupId, pb.ChatType_group)]
		out := &pb.ListMemberResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
		if c != nil {
			for _, uid := range c.Members {
				if u := m.users[uid]; u != nil {
					out.User = append(out.User, &pb.ListMemberResponse_User{
						GroupId: c.ID,
						UserInfo: &pb.ListMemberResponse_User_UserInfo{
							UserId: u.ID, Name: u.Name, AvatarUrl: u.Avatar,
						},
					})
				}
			}
			out.Total = int32(len(out.User))
		}
		m.mu.Unlock()
		writeProto(w, out)
	case "/v1/misc/qiniu-token", "/v1/misc/qiniu-token2", "/v1/misc/qiniu-token-video", "/v1/misc/qiniu-token-audio":
		writeJSON(w, 1, "success", map[string]any{"token": "mock-upload-token"})
	case "/v1/user/logout":
		writeJSON(w, 1, "success", nil)
	case "/v1/captcha/get":
		writeJSON(w, 1, "success", map[string]any{"id": "1", "type": "slide"})
	case "/v1/verification/get-verification-code":
		writeJSON(w, 1, "success", nil)
	default:
		http.NotFound(w, r)
	}
}

func (m *MockServer) conversationList(u *mockUser) *pb.ConversationListResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := &pb.ConversationListResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
	for _, c := range m.chats {
		inChat := false
		for _, mem := range c.Members {
			if mem == u.ID {
				inChat = true
			}
		}
		if !inChat {
			continue
		}
		last := m.msgs[chatKey(c.ID, c.Type)]
		item := &pb.ConversationListResponse_Data{
			ChatId: c.ID, ChatType: c.Type, Name: c.Name, AvatarUrl: c.Avatar,
		}
		if n := len(last); n > 0 {
			item.ChatContent = last[n-1].Text
			item.TimestampMs = uint64(last[n-1].TS)
			item.SendTimestamp = uint64(last[n-1].TS / 1000)
		}
		out.Data = append(out.Data, item)
	}
	sort.Slice(out.Data, func(i, j int) bool { return out.Data[i].TimestampMs > out.Data[j].TimestampMs })
	out.Total = int32(len(out.Data))
	return out
}

func (m *MockServer) listMessages(chatID string, chatType pb.ChatType, beforeID string, count int) *pb.ListMsgResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := m.msgs[chatKey(chatID, chatType)]
	out := &pb.ListMsgResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
	// Locate the cut-off.
	end := len(all)
	if beforeID != "" {
		for i, msg := range all {
			if msg.ID == beforeID {
				end = i
				break
			}
		}
	}
	start := end - count
	if start < 0 {
		start = 0
	}
	for _, msg := range all[start:end] {
		out.Data = append(out.Data, msg.toProto())
	}
	out.Total = int32(len(out.Data))
	return out
}

func (msg *mockMessage) toProto() *pb.ListMsgResponse_MsgData {
	content := &pb.MsgContent{Text: msg.Text}
	switch msg.ContentType {
	case pb.ContentType_image:
		content.ImageUrl = msg.ImageURL
	case pb.ContentType_file:
		content.FileName = msg.FileName
		content.FileUrl = msg.FileURL
		content.FileSize = msg.FileSize
	}
	return &pb.ListMsgResponse_MsgData{
		MsgId:           msg.ID,
		Sender:          &pb.Sender{ChatId: msg.SenderID, ChatType: msg.SenderType, Name: msg.SenderName, AvatarUrl: msg.SenderAvat},
		ContentType:     msg.ContentType,
		Content:         content,
		SendTimestampMs: uint64(msg.TS),
		MsgSeq:          msg.Seq,
		QuoteMsgId:      msg.QuoteMsgID,
	}
}

func (m *MockServer) handleWS(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	var userID string
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var env struct {
			Cmd  string `json:"cmd"`
			Data struct {
				UserID string `json:"userId"`
				Token  string `json:"token"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Cmd {
		case "login":
			u := m.byToken[env.Data.Token]
			if u == nil || u.ID != env.Data.UserID {
				m.log.Warn("mock ws login rejected", "user", env.Data.UserID)
				continue
			}
			userID = u.ID
			m.mu.Lock()
			if m.subs[userID] == nil {
				m.subs[userID] = map[*websocket.Conn]struct{}{}
			}
			m.subs[userID][conn] = struct{}{}
			m.mu.Unlock()
			m.log.Info("mock ws login ok", "user", userID)
		case "heartbeat":
			m.writeWS(conn, "heartbeat_ack", nil)
		case "inputInfo":
			// draft sync is a no-op in the mock
		}
	}

	m.mu.Lock()
	if set := m.subs[userID]; set != nil {
		delete(set, conn)
	}
	m.mu.Unlock()
}

func (m *MockServer) writeWS(conn *websocket.Conn, cmd string, payload proto.Message) {
	wrapper := &pb.WsWrapper{Info: &pb.WsInfo{Cmd: cmd}}
	if payload != nil {
		if any, err := anypb.New(payload); err == nil {
			wrapper.Data = any
		}
	}
	raw, err := proto.Marshal(wrapper)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (m *MockServer) push(userID, cmd string, msg *mockMessage) {
	m.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(m.subs[userID]))
	for c := range m.subs[userID] {
		conns = append(conns, c)
	}
	m.mu.Unlock()
	if len(conns) == 0 {
		return
	}
	payload := &pb.PushMessage{
		MsgId:       msg.ID,
		Sender:      &pb.Sender{ChatId: msg.SenderID, ChatType: msg.SenderType, Name: msg.SenderName, AvatarUrl: msg.SenderAvat},
		ChatId:      msg.ChatID,
		ChatType:    msg.ChatType,
		Content:     &pb.MsgContent{Text: msg.Text, ImageUrl: msg.ImageURL, FileName: msg.FileName, FileUrl: msg.FileURL, FileSize: msg.FileSize},
		ContentType: msg.ContentType,
		Timestamp:   uint64(msg.TS),
		MsgSeq:      msg.Seq,
		QuoteMsgId:  msg.QuoteMsgID,
	}
	for _, c := range conns {
		m.writeWS(c, cmd, payload)
	}
}

func writeJSON(w http.ResponseWriter, code int, msg string, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "msg": msg, "data": data})
}

func writeProto(w http.ResponseWriter, m proto.Message) {
	raw, err := proto.Marshal(m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(raw)
}

// handleUpload emulates a Qiniu direct upload endpoint.
func (m *MockServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, `{"error":"bad form"}`, http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, `{"error":"no file"}`, http.StatusBadRequest)
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 32<<20))
	if err != nil {
		http.Error(w, `{"error":"read"}`, http.StatusBadRequest)
		return
	}
	key := "mock/" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + header.Filename
	m.mu.Lock()
	m.blobs[key] = data
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"key":"` + key + `","hash":"` + key + `"}`))
}

// handleMedia serves blobs uploaded through handleUpload.
func (m *MockServer) handleMedia(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/media/")
	m.mu.Lock()
	data, ok := m.blobs[key]
	m.mu.Unlock()
	if !ok {
		// A 1x1 transparent PNG keeps image rendering paths exercisable.
		png, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
		data = png
	}
	w.Header().Set("Content-Type", http.DetectContentType(data))
	_, _ = w.Write(data)
}
