package matrix

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// mentionRoom seeds a group with three known members, the shape a real 云湖 group
// has: two bots and one user.
func mentionRoom(t *testing.T, s *Server, acc *model.Account) *model.Room {
	t.Helper()
	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.putMemberLocked(acc, room, "@yhbot_23090440:test.local", "复读机", "")
	s.putMemberLocked(acc, room, "@yhbot_38316593:test.local", "EC群管", "")
	s.putMemberLocked(acc, room, "@yh_8932561:test.local", "логикатеКсус", "")
	s.Store.Mu.Unlock()
	return room
}

// TestYunhuMentionBecomesMatrixMention is the reported gap: 云湖 carries a
// mention as "@[显示名]" in the body plus ids in mentioned_id, and that markup
// was passed through verbatim - so a Matrix client showed "@[复读机]" as literal
// text with no mention pill and no notification.
func TestYunhuMentionBecomesMatrixMention(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	mentionRoom(t, s, acc)

	// The exact text shape 云湖 produces, with the id list on the content.
	msg := &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "8932561", ChatType: pb.ChatType_user, Name: "логикатеКсус"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
		Content: &pb.MsgContent{
			Text:        "@[复读机] 67676@[EC群管] @[логикатеКсус] 9",
			MentionedId: []string{"23090440", "38316593", "8932561"},
		},
	}
	s.ingest(acc, "push_message", msg)

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, "!yh-group-979377289:test.local", "m1")
	s.Store.Mu.RUnlock()
	if ev == nil {
		t.Fatal("the message was not projected")
	}

	// The fallback body drops the brackets: a client that cannot render HTML
	// must still read "@复读机", not "@[复读机]".
	body := asString(ev.Content["body"])
	if strings.Contains(body, "@[") {
		t.Fatalf("body still has Yunhu markup: %q", body)
	}
	if !strings.Contains(body, "@复读机") {
		t.Fatalf("body missing the mention: %q", body)
	}
	if !strings.Contains(body, "67676") || !strings.Contains(body, "9") {
		t.Fatalf("body lost the surrounding text: %q", body)
	}

	// m.mentions.user_ids is what makes the client highlight and notify.
	ids, ok := mentionUserIDsFrom(ev.Content)
	if !ok {
		t.Fatalf("no m.mentions in %v", ev.Content)
	}
	want := []string{
		"@yhbot_23090440:test.local",
		"@yhbot_38316593:test.local",
		"@yh_8932561:test.local",
	}
	if len(ids) != len(want) {
		t.Fatalf("m.mentions.user_ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("m.mentions.user_ids[%d] = %q, want %q", i, ids[i], want[i])
		}
	}

	// formatted_body carries the matrix.to anchor, which is how the client knows
	// the mention is clickable.
	if got := asString(ev.Content["format"]); got != "org.matrix.custom.html" {
		t.Fatalf("format = %q", got)
	}
	formatted := asString(ev.Content["formatted_body"])
	for _, uid := range want {
		if !strings.Contains(formatted, matrixToURL(uid)) {
			t.Fatalf("formatted_body missing the link for %s: %s", uid, formatted)
		}
	}
	if strings.Contains(formatted, "@[") {
		t.Fatalf("formatted_body still has Yunhu markup: %s", formatted)
	}
}

// TestYunhuMentionInMarkdownIsConverted: markdown messages carry the same
// markup and must get the same treatment.
func TestYunhuMentionInMarkdownIsConverted(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	mentionRoom(t, s, acc)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "8932561", ChatType: pb.ChatType_user, Name: "логикатеКсус"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		ContentType: pb.ContentType_markdown,
		Timestamp:   1700000000000,
		Content: &pb.MsgContent{
			Text:        "**重要** @[复读机] 看一下",
			MentionedId: []string{"23090440"},
		},
	})

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, "!yh-group-979377289:test.local", "m1")
	s.Store.Mu.RUnlock()

	body := asString(ev.Content["body"])
	if strings.Contains(body, "@[") {
		t.Fatalf("body still has Yunhu markup: %q", body)
	}
	formatted := asString(ev.Content["formatted_body"])
	if !strings.Contains(formatted, "<strong>重要</strong>") {
		t.Fatalf("markdown formatting was lost: %s", formatted)
	}
	if !strings.Contains(formatted, matrixToURL("@yhbot_23090440:test.local")) {
		t.Fatalf("markdown mention was not linked: %s", formatted)
	}
	if ids, _ := mentionUserIDsFrom(ev.Content); len(ids) != 1 {
		t.Fatalf("m.mentions = %v, want one id", ids)
	}
}

// TestYunhuMentionWithoutIdKeepsTheText: a "@[name]" with no matching id cannot
// be turned into a mention. The text must survive as written rather than vanish.
func TestYunhuMentionWithoutIdKeepsTheText(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	mentionRoom(t, s, acc)

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "8932561", ChatType: pb.ChatType_user, Name: "логикатеКсус"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
		Content:     &pb.MsgContent{Text: "看看 @[不存在的人] 怎么说"},
	})

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, "!yh-group-979377289:test.local", "m1")
	s.Store.Mu.RUnlock()

	if got := asString(ev.Content["body"]); !strings.Contains(got, "@[不存在的人]") {
		t.Fatalf("unresolvable mention was dropped or mangled: %q", got)
	}
	if _, ok := mentionUserIDsFrom(ev.Content); ok {
		t.Fatalf("m.mentions invented for an unresolved name: %v", ev.Content)
	}
}

// TestMatrixMentionReachesYunhu is the other direction, using the payload from
// the report: a Matrix client sends the truth in m.mentions and spells the names
// in the body, and 云湖 must receive "@[显示名]" plus mentioned_id.
func TestMatrixMentionReachesYunhu(t *testing.T) {
	f := &fakeMention{}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	room := mentionRoom(t, s, acc)

	body := `{"msgtype":"m.text",` +
		`"body":"@[复读机] 67676@[EC群管] @[логикатеКсус] 9",` +
		`"m.mentions":{"user_ids":[` +
		`"@yhbot_23090440:test.local","@yhbot_38316593:test.local","@yh_8932561:test.local"]},` +
		`"format":"org.matrix.custom.html",` +
		`"formatted_body":"<a href=\"https://matrix.to/#/@yhbot_23090440:test.local\">@[复读机]</a> 67676"}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/txn1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	if n := atomic.LoadInt32(&f.calls); n == 0 {
		t.Fatal("the message never reached Yunhu")
	}
	got := f.lastReq.Load().(*pb.SendMsgRequest)
	content := got.GetContent()

	// The ids must be in mentioned_id, or nobody is notified.
	wantIDs := []string{"23090440", "38316593", "8932561"}
	if len(content.GetMentionedId()) != len(wantIDs) {
		t.Fatalf("mentioned_id = %v, want %v", content.GetMentionedId(), wantIDs)
	}
	for i := range wantIDs {
		if content.GetMentionedId()[i] != wantIDs[i] {
			t.Fatalf("mentioned_id[%d] = %q, want %q", i, content.GetMentionedId()[i], wantIDs[i])
		}
	}

	// The body must speak Yunhu's dialect.
	text := content.GetText()
	if strings.Contains(text, "https://matrix.to/") {
		t.Fatalf("matrix.to link leaked into the Yunhu body: %q", text)
	}
	for _, name := range []string{"复读机", "EC群管", "логикатеКсус"} {
		if !strings.Contains(text, "@["+name+"]") {
			t.Fatalf("body missing @[%s]: %q", name, text)
		}
	}
}

// TestMatrixMentionFromPlainBody covers a client that only sends "@displayname"
// (or a bare name) with m.mentions, with no Yunhu markup and no formatted_body.
func TestMatrixMentionFromPlainBody(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	room := mentionRoom(t, s, acc)

	nameOf := mentionNameResolver(acc, room)
	got := rewriteMentionsForYunhu(
		"@复读机 在吗 @EC群管",
		[]string{"@yhbot_23090440:test.local", "@yhbot_38316593:test.local"},
		nameOf,
	)
	if !strings.Contains(got, "@[复读机]") {
		t.Fatalf("plain @name was not converted: %q", got)
	}
	if !strings.Contains(got, "@[EC群管]") {
		t.Fatalf("second mention was not converted: %q", got)
	}
	if strings.Contains(got, "@复读机 ") {
		t.Fatalf("the unconverted form is still present: %q", got)
	}
}

// TestMatrixMentionBareNameIsWordBounded: a bare display name must only be
// rewritten when it stands alone, so "复读机" inside a longer word is untouched.
func TestMatrixMentionBareNameIsWordBounded(t *testing.T) {
	nameOf := func(string) string { return "Bob" }
	got := rewriteMentionsForYunhu("Bobby said hi to Bob", []string{"@yh_1:test.local"}, nameOf)
	if strings.Contains(got, "@[Bob]by") {
		t.Fatalf("rewrote inside a longer word: %q", got)
	}
	if !strings.Contains(got, "to @[Bob]") {
		t.Fatalf("standalone name was not rewritten: %q", got)
	}
}

// fakeMention captures the send request forwarded upstream.
type fakeMention struct {
	calls   int32
	lastReq atomic.Value
}

func (f *fakeMention) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/send-message", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.SendMsgRequest
		_ = proto.Unmarshal(raw, &req)
		atomic.AddInt32(&f.calls, 1)
		f.lastReq.Store(&req)
		out, _ := proto.Marshal(&pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	return mux
}
