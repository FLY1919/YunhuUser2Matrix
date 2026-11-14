package matrix

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeHistory is a stand-in for POST /v1/msg/list-message. It keeps messages in
// chronological order and honours the "messages before this id" contract, which
// is exactly what the pagination chain depends on.
type fakeHistory struct {
	msgs   []*pb.ListMsgResponse_MsgData
	calls  int
	lastID string
}

func (f *fakeHistory) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/list-message", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.ListMsgRequest
		_ = proto.Unmarshal(raw, &req)

		f.calls++
		f.lastID = req.GetMsgId()

		end := len(f.msgs)
		if req.GetMsgId() != "" {
			for i, m := range f.msgs {
				if m.GetMsgId() == req.GetMsgId() {
					end = i
					break
				}
			}
		}
		count := int(req.GetMsgCount())
		if count <= 0 {
			count = 10
		}
		start := end - count
		if start < 0 {
			start = 0
		}

		out := &pb.ListMsgResponse{Status: &pb.Status{Code: 1, Msg: "success"}}
		out.Data = append(out.Data, f.msgs[start:end]...)
		out.Total = int32(len(out.Data))
		body, _ := proto.Marshal(out)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(body)
	})
	return mux
}

func historyMsg(id, text string, ts uint64) *pb.ListMsgResponse_MsgData {
	return &pb.ListMsgResponse_MsgData{
		MsgId:           id,
		Sender:          &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ContentType:     pb.ContentType_text,
		Content:         &pb.MsgContent{Text: text},
		SendTimestampMs: ts,
		MsgSeq:          ts / 1000,
	}
}

// newHistoryServer builds a homeserver whose Yunhu REST base points at the fake.
func newHistoryServer(t *testing.T, f *fakeHistory) (*Server, *model.Account) {
	t.Helper()
	return newHistoryServerWith(t, f, nil)
}

// newHistoryServerWith is newHistoryServer with a config hook.
func newHistoryServerWith(t *testing.T, f *fakeHistory, tweak func(*config.Config)) (*Server, *model.Account) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		if tweak != nil {
			tweak(cfg)
		}
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	return s, acc
}

// TestMessagesPaginatesIntoYunhu is the reported bug: the local window holds
// three messages, and scrolling back used to stop there. The chain must keep
// going upstream and yield every message exactly once, newest first.
func TestMessagesPaginatesIntoYunhu(t *testing.T) {
	f := &fakeHistory{}
	for i := 1; i <= 6; i++ {
		f.msgs = append(f.msgs, historyMsg("m"+strconv.Itoa(i), "msg"+strconv.Itoa(i),
			uint64(1700000000000+i*1000)))
	}

	s, acc := newHistoryServer(t, f)

	// Only m4..m6 exist locally, as if the hydration window were three messages.
	for i := 4; i <= 6; i++ {
		ingestText(t, s, acc, "m"+strconv.Itoa(i), "10002", pb.ChatType_user,
			"msg"+strconv.Itoa(i), uint64(1700000000000+i*1000))
	}

	var bodies []string
	from := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		q := "dir=b&limit=2"
		if from != "" {
			q += "&from=" + from
		}
		body := getMessages(t, s, "!yh-user-10002:test.local", q)
		chunk, _ := body["chunk"].([]any)
		for _, raw := range chunk {
			bodies = append(bodies, raw.(map[string]any)["content"].(map[string]any)["body"].(string))
		}
		end, hasEnd := body["end"].(string)
		if !hasEnd || end == "" {
			break
		}
		if end == from {
			t.Fatalf("page %d: end token did not advance (%q)", pages, end)
		}
		from = end
	}

	want := []string{"msg6", "msg5", "msg4", "msg3", "msg2", "msg1"}
	if len(bodies) != len(want) {
		t.Fatalf("got %d messages (%v), want %d", len(bodies), bodies, len(want))
	}
	for i := range want {
		if bodies[i] != want[i] {
			t.Fatalf("message %d = %q, want %q (full order: %v)", i, bodies[i], want[i], bodies)
		}
	}
	if f.calls == 0 {
		t.Fatal("upstream was never asked for history")
	}
}

// TestMessagesEmptyRoomLoadsUpstream covers a room with nothing local at all
// (yunhu.backfill: 0, or a room that was never hydrated): the first page must
// come straight from Yunhu instead of returning an empty chunk.
func TestMessagesEmptyRoomLoadsUpstream(t *testing.T) {
	f := &fakeHistory{}
	for i := 1; i <= 3; i++ {
		f.msgs = append(f.msgs, historyMsg("m"+strconv.Itoa(i), "msg"+strconv.Itoa(i),
			uint64(1700000000000+i*1000)))
	}

	s, acc := newHistoryServer(t, f)

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=2")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 2 {
		t.Fatalf("chunk = %d events, want 2", len(chunk))
	}
	first := chunk[0].(map[string]any)["content"].(map[string]any)["body"]
	if first != "msg3" {
		t.Fatalf("first event = %v, want msg3 (newest first)", first)
	}
	end, ok := body["end"].(string)
	if !ok || end == "" {
		t.Fatal("end token missing, the client cannot ask for older history")
	}
	if _, isUpstream := upstreamMsgID(end); !isUpstream {
		t.Fatalf("end token %q should point into Yunhu", end)
	}
}

// TestMessagesFullLocalWindowStillContinues covers the off-by-one that is easy
// to miss: when the local timeline happens to be exactly `limit` messages, the
// page is full and there is nothing local left. That is not the end of history —
// the chain has to hand out an upstream token and carry on.
func TestMessagesFullLocalWindowStillContinues(t *testing.T) {
	f := &fakeHistory{}
	for i := 1; i <= 4; i++ {
		f.msgs = append(f.msgs, historyMsg("m"+strconv.Itoa(i), "msg"+strconv.Itoa(i),
			uint64(1700000000000+i*1000)))
	}

	s, acc := newHistoryServer(t, f)
	for i := 2; i <= 4; i++ {
		ingestText(t, s, acc, "m"+strconv.Itoa(i), "10002", pb.ChatType_user,
			"msg"+strconv.Itoa(i), uint64(1700000000000+i*1000))
	}

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=3")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 3 {
		t.Fatalf("chunk = %d events, want 3", len(chunk))
	}
	end, ok := body["end"].(string)
	if !ok || end == "" {
		t.Fatal("a full local page must still hand out a token: m1 exists upstream")
	}
	if _, isUpstream := upstreamMsgID(end); !isUpstream {
		t.Fatalf("end token %q should point into Yunhu, not the local timeline", end)
	}

	body = getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=3&from="+end)
	chunk, _ = body["chunk"].([]any)
	if len(chunk) != 1 {
		t.Fatalf("second page = %d events, want the single older message", len(chunk))
	}
	if got := chunk[0].(map[string]any)["content"].(map[string]any)["body"]; got != "msg1" {
		t.Fatalf("second page event = %v, want msg1", got)
	}
}

// TestMessagesUpstreamTokenRoundTrip pins the opaque token format: the chain
// hands out a token and must accept exactly that token back.
func TestMessagesUpstreamTokenRoundTrip(t *testing.T) {
	for _, id := range []string{"m1", "1234567890123456789", "msg_with-symbols"} {
		tok := upstreamToken(id)
		if tok == "" {
			t.Fatalf("upstreamToken(%q) is empty", id)
		}
		got, ok := upstreamMsgID(tok)
		if !ok || got != id {
			t.Fatalf("round trip %q -> %q (ok=%v)", id, got, ok)
		}
	}
	// Local tokens must never be mistaken for upstream ones.
	for _, tok := range []string{"s123", "s0", "", "end"} {
		if _, ok := upstreamMsgID(tok); ok {
			t.Fatalf("local token %q parsed as upstream", tok)
		}
	}
	if upstreamToken("") != "" {
		t.Fatal("upstreamToken(\"\") must be empty")
	}
}

// TestMessagesUpstreamStopsAtTheBeginning makes sure the chain terminates: after
// the last upstream page the next request returns an empty chunk and omits end,
// which is how the client learns to stop offering "load more".
func TestMessagesUpstreamStopsAtTheBeginning(t *testing.T) {
	f := &fakeHistory{}
	f.msgs = append(f.msgs, historyMsg("m1", "only", 1700000001000))

	s, acc := newHistoryServer(t, f)
	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "10002", model.ChatTypeUser, "林晚", "")
	s.Store.Mu.Unlock()

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=10")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 1 {
		t.Fatalf("chunk = %d events, want 1", len(chunk))
	}
	end, ok := body["end"].(string)
	if !ok || end == "" {
		t.Fatal("first page must hand out a token so the client can ask for more")
	}

	// Following the token must end the chain cleanly.
	body = getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=10&from="+end)
	if chunk, _ := body["chunk"].([]any); len(chunk) != 0 {
		t.Fatalf("expected an empty page before m1, got %d", len(chunk))
	}
	if v, hasEnd := body["end"]; hasEnd {
		t.Fatalf("end must be omitted at the beginning of history, got %v", v)
	}
}

// TestSortMsgsChronological pins the ordering fix: stream positions are assigned
// in iteration order, so an API that answers newest-first used to invert the
// timeline. Ordering must come from the payload, not from the response.
func TestSortMsgsChronological(t *testing.T) {
	data := []*pb.ListMsgResponse_MsgData{
		historyMsg("m3", "c", 1700000003000),
		historyMsg("m1", "a", 1700000001000),
		historyMsg("m2", "b", 1700000002000),
	}
	sortMsgsChronological(data)
	for i, want := range []string{"m1", "m2", "m3"} {
		if data[i].GetMsgId() != want {
			t.Fatalf("index %d = %s, want %s", i, data[i].GetMsgId(), want)
		}
	}

	// Without timestamps the sequence number decides.
	seq := []*pb.ListMsgResponse_MsgData{
		{MsgId: "b", MsgSeq: 20},
		{MsgId: "a", MsgSeq: 10},
	}
	sortMsgsChronological(seq)
	if seq[0].GetMsgId() != "a" {
		t.Fatalf("seq ordering failed: %s first", seq[0].GetMsgId())
	}
}
