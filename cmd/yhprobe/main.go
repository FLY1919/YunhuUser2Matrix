// Command yhprobe talks to the real Yunhu API directly.
//
// It exists so the homeserver's upstream assumptions can be validated against
// production without running the whole stack:
//
//	yhprobe -token <yunhu token> info
//	yhprobe -token <token> convs
//	yhprobe -token <token> history -chat 10002 -type 1 -limit 5
//	yhprobe -token <token> send    -chat 10002 -type 1 -text "hello"
//	yhprobe -token <token> board   -chat 20001 -type 2
//	yhprobe -token <token> ws      -user 10001
//
// Every command prints the decoded protobuf as JSON, so field mismatches are
// immediately visible.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/yunhu"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

func main() {
	var (
		base  = flag.String("base", "https://chat-go.jwzhd.com", "Yunhu REST base URL")
		wsURL = flag.String("ws", "wss://chat-ws-go.jwzhd.com/ws", "Yunhu websocket URL")
		token = flag.String("token", os.Getenv("YUNHU_TOKEN"), "Yunhu account token")
		wire  = flag.String("wire", "auto", "request encoding: auto, proto or json")
		plat  = flag.String("platform", "windows", "login platform")
	)
	flag.Usage = usage
	flag.Parse()

	cmd := flag.Arg(0)
	if cmd == "" {
		usage()
		os.Exit(2)
	}

	client := yunhu.NewClient(*base, 30*time.Second)
	client.Wire = *wire
	client.Platform = *plat
	if *token != "" {
		client = client.WithToken(*token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	chatID := fs.String("chat", "", "chat id")
	chatType := fs.Int("type", 1, "chat type: 1 user, 2 group, 3 bot")
	limit := fs.Int("limit", 10, "message count")
	text := fs.String("text", "", "message text")
	userID := fs.String("user", "", "Yunhu user id (for ws login)")
	_ = fs.Parse(flag.Args()[1:])

	switch cmd {
	case "info":
		info, err := client.GetUserInfo(ctx)
		must(err)
		printJSON(info)

	case "convs":
		resp, err := client.Conversations(ctx, "")
		must(err)
		printProto(resp)

	case "history":
		resp, err := client.ListMessages(ctx, *chatID, pb.ChatType(*chatType), "", int32(*limit))
		if err != nil {
			fmt.Println("list-message failed, trying list-message-by-seq:", err)
			resp, err = client.ListMessagesBySeq(ctx, *chatID, pb.ChatType(*chatType), 0)
		}
		must(err)
		printProto(resp)

	case "send":
		req := &pb.SendMsgRequest{
			MsgId:       fmt.Sprintf("%x", time.Now().UnixNano()),
			ChatId:      *chatID,
			ChatType:    pb.ChatType(*chatType),
			ContentType: pb.ContentType_text,
			Content:     &pb.MsgContent{Text: *text},
		}
		must(client.SendMessage(ctx, req))
		fmt.Println(`{"ok":true}`)

	case "group":
		resp, err := client.GroupInfo(ctx, *chatID)
		must(err)
		printProto(resp)

	case "board":
		// /v1/bot/board returns one board per bot: a group can host several
		// bots and each maintains its own announcement. chat_type is 2 for a
		// group and 3 for a single-bot conversation (NOT 1, that is a user).
		resp, err := client.Board(ctx, *chatID, pb.ChatType(*chatType))
		must(err)
		fmt.Printf("# %d board(s)\n", len(resp.GetBoard()))
		printProto(resp)

	case "ws":
		if *userID == "" {
			fatal("-user is required for the ws command")
		}
		runWS(*wsURL, *userID, *token, *plat)

	default:
		usage()
		os.Exit(2)
	}
}

func runWS(url, userID, token, platform string) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	rt := &yunhu.Realtime{
		URL: url, UserID: userID, Token: token, Platform: platform,
		DeviceID:  "yhprobe",
		Heartbeat: 25 * time.Second,
		CB: yunhu.Callbacks{
			OnConnect: func() { fmt.Println("# connected and logged in") },
			OnPush: func(cmd string, m *pb.PushMessage) {
				fmt.Printf("# %s chat=%s/%d\n%s\n", cmd, m.GetChatId(), m.GetChatType(), protoJSON(m))
			},
			OnStream: func(m *pb.StreamMessage) { fmt.Printf("# stream %s\n", m.GetMsgId()) },
			OnDraft:  func(m *pb.MsgInput) { fmt.Printf("# draft %s: %s\n", m.GetChatId(), m.GetInput()) },
			OnError:  func(err error) { fmt.Println("# error:", err) },
		},
	}
	fmt.Println("# listening for 60s, Ctrl-C to stop")
	go rt.Run(ctx)
	select {
	case <-ctx.Done():
	case <-time.After(60 * time.Second):
	}
}

func printJSON(v any) {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: false}.
		Marshal(mustMessage(v))
	if err != nil {
		fmt.Println(v)
		return
	}
	fmt.Println(string(b))
}

func printProto(m proto.Message) {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		fmt.Println("encode failed:", err)
		return
	}
	fmt.Println(string(b))
}

func protoJSON(m proto.Message) string {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: false}.Marshal(m)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func mustMessage(v any) proto.Message {
	if m, ok := v.(proto.Message); ok {
		return m
	}
	return &pb.StatusResponse{}
}

func must(err error) {
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "error:", msg)
	os.Exit(1)
}

func usage() {
	fmt.Fprint(os.Stderr, `yhprobe - probe the Yunhu API

usage: yhprobe [flags] <command> [command flags]

commands:
  info                     fetch the account bound to -token
  convs                    list conversations
  history -chat ID -type N -limit N
  send    -chat ID -type N -text "..."
  group   -chat ID         fetch group metadata
  board   -chat ID -type N  fetch every bot board of a conversation (N: 2 group, 3 bot)
  ws      -user ID         connect the realtime websocket for 60s

flags:
`)
	flag.PrintDefaults()
}
