package yunhu

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// Callbacks receives realtime events. Every callback is invoked from the
// websocket reader goroutine, so implementations must not block.
type Callbacks struct {
	OnPush    func(cmd string, msg *pb.PushMessage)
	OnStream  func(msg *pb.StreamMessage)
	OnDraft   func(msg *pb.MsgInput)
	OnBoard   func(msg *pb.BotBoardMessage)
	OnInvite  func()
	OnConnect func()
	OnError   func(error)
}

// Realtime maintains the Yunhu websocket for one account.
//
// Yunhu speaks JSON for commands we send and protobuf (a WsWrapper envelope) for
// events it pushes. The reader accepts both, and dispatches on the envelope's
// command string rather than on the protobuf Any type URL, because the upstream
// type URLs are not stable.
type Realtime struct {
	URL      string
	UserID   string
	Token    string
	Platform string
	DeviceID string

	Heartbeat  time.Duration
	MinBackoff time.Duration
	MaxBackoff time.Duration

	CB  Callbacks
	Log *slog.Logger

	mu     sync.Mutex
	conn   *websocket.Conn
	seq    int64
	closed bool
}

// Run connects and reconnects until ctx is cancelled.
func (rt *Realtime) Run(ctx context.Context) {
	if rt.Log == nil {
		rt.Log = slog.Default()
	}
	if rt.Heartbeat <= 0 {
		rt.Heartbeat = 25 * time.Second
	}
	if rt.MinBackoff <= 0 {
		rt.MinBackoff = time.Second
	}
	if rt.MaxBackoff < rt.MinBackoff {
		rt.MaxBackoff = 60 * time.Second
	}

	backoff := rt.MinBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := rt.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil && rt.CB.OnError != nil {
			rt.CB.OnError(err)
		}
		if time.Since(started) > 30*time.Second {
			backoff = rt.MinBackoff
		}
		rt.Log.Warn("yunhu websocket closed, reconnecting",
			"user", rt.UserID, "err", err, "backoff", backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > rt.MaxBackoff {
			backoff = rt.MaxBackoff
		}
	}
}

func (rt *Realtime) session(ctx context.Context) error {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second

	conn, _, err := dialer.DialContext(ctx, rt.URL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	rt.mu.Lock()
	rt.conn = conn
	rt.closed = false
	rt.mu.Unlock()
	defer func() {
		rt.mu.Lock()
		rt.closed = true
		rt.conn = nil
		rt.mu.Unlock()
	}()

	if err := rt.sendJSON("login", map[string]any{
		"userId":   rt.UserID,
		"token":    rt.Token,
		"platform": rt.Platform,
		"deviceId": rt.DeviceID,
	}); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	if rt.CB.OnConnect != nil {
		rt.CB.OnConnect()
	}

	// Ping/pong plus an application level heartbeat.
	_ = conn.SetReadDeadline(time.Now().Add(3 * rt.Heartbeat))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(3 * rt.Heartbeat))
	})

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go rt.heartbeatLoop(hbCtx)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * rt.Heartbeat))
		rt.handleFrame(data)
	}
}

func (rt *Realtime) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(rt.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := rt.sendJSON("heartbeat", map[string]any{}); err != nil {
				return
			}
		}
	}
}

// SendInput mirrors Matrix typing notifications onto Yunhu draft sync.
func (rt *Realtime) SendInput(chatID, text string) error {
	return rt.sendJSON("inputInfo", map[string]any{
		"chatId":   chatID,
		"input":    text,
		"deviceId": rt.DeviceID,
	})
}

func (rt *Realtime) sendJSON(cmd string, data any) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.conn == nil || rt.closed {
		return fmt.Errorf("websocket not connected")
	}
	rt.seq++
	payload := map[string]any{
		"seq":  fmt.Sprintf("%d", time.Now().UnixNano()),
		"cmd":  cmd,
		"data": data,
	}
	_ = rt.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return rt.conn.WriteJSON(payload)
}

// handleFrame decodes either a JSON or a protobuf frame.
func (rt *Realtime) handleFrame(data []byte) {
	if len(data) == 0 {
		return
	}
	if data[0] == '{' {
		rt.handleJSONFrame(data)
		return
	}
	var wrapper pb.WsWrapper
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, &wrapper); err != nil {
		rt.Log.Debug("undecodable protobuf frame", "err", err, "len", len(data))
		return
	}
	cmd := wrapper.GetInfo().GetCmd()
	payload := wrapper.GetData().GetValue()
	rt.dispatch(cmd, payload)
}

func (rt *Realtime) handleJSONFrame(data []byte) {
	var env struct {
		Info struct {
			Seq string `json:"seq"`
			Cmd string `json:"cmd"`
		} `json:"info"`
		Cmd  string          `json:"cmd"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		rt.Log.Debug("undecodable json frame", "err", err)
		return
	}
	cmd := env.Info.Cmd
	if cmd == "" {
		cmd = env.Cmd
	}
	if cmd == "" {
		return
	}
	// JSON frames carry JSON payloads; decode into the matching message type.
	rt.dispatchJSON(cmd, env.Data)
}

func (rt *Realtime) dispatch(cmd string, payload []byte) {
	switch cmd {
	case "push_message", "edit_message", "blocked_message":
		var m pb.PushMessage
		if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, &m); err != nil {
			rt.Log.Debug("bad push payload", "cmd", cmd, "err", err)
			return
		}
		if rt.CB.OnPush != nil {
			rt.CB.OnPush(cmd, &m)
		}
	case "stream_message":
		var m pb.StreamMessage
		if err := proto.Unmarshal(payload, &m); err != nil {
			return
		}
		if rt.CB.OnStream != nil {
			rt.CB.OnStream(&m)
		}
	case "draft_input":
		var m pb.MsgInput
		if err := proto.Unmarshal(payload, &m); err != nil {
			return
		}
		if rt.CB.OnDraft != nil {
			rt.CB.OnDraft(&m)
		}
	case "bot_board_message":
		var m pb.BotBoardMessage
		if err := proto.Unmarshal(payload, &m); err != nil {
			return
		}
		if rt.CB.OnBoard != nil {
			rt.CB.OnBoard(&m)
		}
	case "invite_apply":
		if rt.CB.OnInvite != nil {
			rt.CB.OnInvite()
		}
	case "heartbeat_ack":
		// deadline already refreshed by the read loop
	default:
		rt.Log.Debug("unhandled realtime command", "cmd", cmd)
	}
}

func (rt *Realtime) dispatchJSON(cmd string, data json.RawMessage) {
	if len(data) == 0 {
		return
	}
	switch cmd {
	case "push_message", "edit_message", "blocked_message":
		var m pb.PushMessage
		if err := protoJSONUnmarshal(data, &m); err != nil {
			rt.Log.Debug("bad json push payload", "err", err)
			return
		}
		if rt.CB.OnPush != nil {
			rt.CB.OnPush(cmd, &m)
		}
	case "stream_message":
		var m pb.StreamMessage
		if err := protoJSONUnmarshal(data, &m); err == nil && rt.CB.OnStream != nil {
			rt.CB.OnStream(&m)
		}
	case "bot_board_message":
		var m pb.BotBoardMessage
		if err := protoJSONUnmarshal(data, &m); err == nil && rt.CB.OnBoard != nil {
			rt.CB.OnBoard(&m)
		}
	case "invite_apply":
		if rt.CB.OnInvite != nil {
			rt.CB.OnInvite()
		}
	case "heartbeat_ack":
	default:
		rt.Log.Debug("unhandled json realtime command", "cmd", cmd)
	}
}
