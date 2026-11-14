package matrix

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// fakeFileSend records every send-message request, in order.
type fakeFileSend struct {
	mu   sync.Mutex
	reqs []*pb.SendMsgRequest
}

func (f *fakeFileSend) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/msg/send-message", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req pb.SendMsgRequest
		_ = proto.Unmarshal(raw, &req)
		f.mu.Lock()
		f.reqs = append(f.reqs, &req)
		f.mu.Unlock()
		out, _ := proto.Marshal(&pb.StatusResponse{Status: &pb.Status{Code: 1, Msg: "success"}})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	})
	return mux
}

func (f *fakeFileSend) snapshot() []*pb.SendMsgRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.SendMsgRequest, len(f.reqs))
	copy(out, f.reqs)
	return out
}

// fileServer stands up a homeserver whose upstream is the fake, with a room and
// a device ready to send into it.
func fileServer(t *testing.T, f *fakeFileSend) (*Server, *model.Account, *model.Room) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	s, st := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Yunhu.BaseURL = srv.URL
		cfg.Yunhu.Wire = "proto"
	})
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "857744874", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()
	return s, acc, room
}

// localMXC wraps an upstream reference so it resolves back to a plain key
// without touching the network: this homeserver's own mxc URIs are the only ones
// ResolveForYunhu can decode.
func localMXC(s *Server, ref string) string {
	return s.Media.RemoteMXC(ref)
}

// TestFileWithNoteSendsTwoMessages is the reported case: a Matrix file message
// whose body is a note ("464635") and whose filename is the attachment
// ("DevCheck.apk"). Yunhu's file message has no caption, so the note must go out
// as its own text message - and it comes AFTER the file, so it reads as the
// comment on what was just sent.
func TestFileWithNoteSendsTwoMessages(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	// The exact payload from the report, with the media reference rewritten to a
	// URI this homeserver can decode (a foreign mxc:// would need the network).
	body := `{"msgtype":"m.file","body":"464635","filename":"DevCheck.apk",` +
		`"url":"` + localMXC(s, "disk/960352.apk") + `",` +
		`"info":{"mimetype":"application/vnd.android.package-archive","size":15399310}}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/FluffyChat-1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("upstream got %d messages, want 2 (file then note)", len(reqs))
	}

	// The file goes first, and its name is the filename - not the note.
	file := reqs[0]
	if file.GetContentType() != pb.ContentType_file {
		t.Fatalf("first message type = %v, want file", file.GetContentType())
	}
	if got := file.GetContent().GetFileName(); got != "DevCheck.apk" {
		t.Fatalf("file name = %q, want DevCheck.apk", got)
	}
	if got := file.GetContent().GetText(); got != "" {
		t.Fatalf("the note leaked into the file message: %q", got)
	}

	// The note follows as a plain text message.
	note := reqs[1]
	if note.GetContentType() != pb.ContentType_text {
		t.Fatalf("second message type = %v, want text", note.GetContentType())
	}
	if got := note.GetContent().GetText(); got != "464635" {
		t.Fatalf("note text = %q, want 464635", got)
	}
	if note.GetContent().GetFileName() != "" {
		t.Fatalf("the note carried a file name: %q", note.GetContent().GetFileName())
	}
}

// TestFileWithoutNoteSendsOneMessage: a file whose body is just the file name
// (the common case) must not produce a second message.
func TestFileWithoutNoteSendsOneMessage(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.file","body":"DevCheck.apk","filename":"DevCheck.apk",` +
		`"url":"` + localMXC(s, "disk/abc.apk") + `","info":{"size":10}}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}
	if got := len(f.snapshot()); got != 1 {
		t.Fatalf("upstream got %d messages, want 1", got)
	}
}

// TestFileWithEmptyBodySendsOneMessage: no body at all is not a note.
func TestFileWithEmptyBodySendsOneMessage(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.file","body":"","filename":"a.bin","url":"` + localMXC(s, "disk/a.bin") + `"}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}
	reqs := f.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("upstream got %d messages, want 1", len(reqs))
	}
	if got := reqs[0].GetContent().GetFileName(); got != "a.bin" {
		t.Fatalf("file name = %q, want a.bin", got)
	}
}

// TestFileNoteBecomesItsOwnEvent: the split has to be visible on this side too.
// A client that only saw the file would lose the text the user typed.
func TestFileNoteBecomesItsOwnEvent(t *testing.T) {
	f := &fakeFileSend{}
	s, acc, room := fileServer(t, f)

	body := `{"msgtype":"m.file","body":"464635","filename":"DevCheck.apk",` +
		`"url":"` + localMXC(s, "disk/752e01.apk") + `","info":{"size":10}}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	s.Store.Mu.RLock()
	defer s.Store.Mu.RUnlock()

	var fileEv, noteEv *model.Event
	for _, ev := range acc.Rooms[room.RoomID].Events {
		if ev.IsState() {
			continue
		}
		switch asString(ev.Content["msgtype"]) {
		case "m.file":
			fileEv = ev
		case "m.text":
			noteEv = ev
		}
	}
	if fileEv == nil {
		t.Fatal("the file event is missing from the timeline")
	}
	if noteEv == nil {
		t.Fatal("the note was not projected as its own event")
	}
	if got := asString(noteEv.Content["body"]); got != "464635" {
		t.Fatalf("note body = %q, want 464635", got)
	}
	// The response still names the file's event: that is what the client sent and
	// what it will drop from its local echo.
	if got, want := asString(resp["event_id"]), fileEv.EventID; got != want {
		t.Fatalf("event_id = %q, want the file's event %q", got, want)
	}
}

// TestImageWithCaptionIsSplit is the image/video counterpart of the file case:
// a caption ("sjcjfjdg") next to a real file name must reach the other side, and
// Yunhu's image message has no caption field - so it goes as its own message.
func TestImageWithCaptionIsSplit(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	// The exact payload from the report.
	body := `{"msgtype":"m.image","body":"sjcjfjdg",` +
		`"filename":"Screenshot_20261114-212825_crDroid_Home.png",` +
		`"url":"` + localMXC(s, "image/d4d4d3.png") + `",` +
		`"info":{"mimetype":"image/png","size":167955,"w":576,"h":1152,` +
		`"thumbnail_url":"` + localMXC(s, "image/thumb.jpg") + `",` +
		`"thumbnail_info":{"mimetype":"image/jpeg","size":36087,"w":400,"h":800},` +
		`"xyz.amorgan.blurhash":"KMQ9~3~pRjNIRjt7oyDjxu"}}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("upstream got %d messages, want 2 (image then caption)", len(reqs))
	}

	img := reqs[0]
	if img.GetContentType() != pb.ContentType_image {
		t.Fatalf("first message type = %v, want image", img.GetContentType())
	}
	// The file name, not the caption.
	if got := img.GetContent().GetFileName(); got != "Screenshot_20261114-212825_crDroid_Home.png" {
		t.Fatalf("file name = %q, want the real file name", got)
	}
	// Everything Matrix knows must reach Yunhu.
	if got := img.GetContent().GetWidth(); got != 576 {
		t.Fatalf("width = %d, want 576", got)
	}
	if got := img.GetContent().GetHeight(); got != 1152 {
		t.Fatalf("height = %d, want 1152", got)
	}
	if got := img.GetContent().GetFileSize(); got != 167955 {
		t.Fatalf("size = %d, want 167955", got)
	}

	caption := reqs[1]
	if caption.GetContentType() != pb.ContentType_text {
		t.Fatalf("second message type = %v, want text", caption.GetContentType())
	}
	if got := caption.GetContent().GetText(); got != "sjcjfjdg" {
		t.Fatalf("caption = %q, want sjcjfjdg", got)
	}
}

// TestVideoCarriesInfoAndCaption: video is the one media type Yunhu models as a
// text message, so its caption fits in the same message instead of a second one.
func TestVideoCarriesInfoAndCaption(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	// The exact payload from the report.
	body := `{"msgtype":"m.video","body":"scjvkff",` +
		`"filename":"screen-20261001-222257.mp4",` +
		`"url":"` + localMXC(s, "video/7d9245.mp4") + `",` +
		`"info":{"mimetype":"video/mp4","size":1063644,"w":720,"h":1440,` +
		`"duration":16300,` +
		`"thumbnail_url":"` + localMXC(s, "image/a8c26e.jpg") + `",` +
		`"thumbnail_info":{"mimetype":"image/jpeg","size":14764}}}`

	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	// Two messages: the video, then its caption. Yunhu's client renders a
	// content_type=10 message as a player only - it never draws content.text -
	// so a caption written there would be invisible.
	if len(reqs) != 2 {
		t.Fatalf("a video produced %d messages, want 2 (video then caption)", len(reqs))
	}

	video := reqs[0]
	// Yunhu has a dedicated video content type (10). Sending it as text made the
	// receiving client render a text bubble with a url instead of a video.
	if got := video.GetContentType(); got != pb.ContentType_video {
		t.Fatalf("content type = %v, want video (10)", got)
	}
	if got := video.GetContent().GetVideoUrl(); !strings.Contains(got, "7d9245") {
		t.Fatalf("video_url = %q", got)
	}
	if got := video.GetContent().GetFileName(); got != "screen-20261001-222257.mp4" {
		t.Fatalf("file name = %q, want the real file name", got)
	}
	// Nothing in the video message's text: the client would not show it anyway.
	if got := video.GetContent().GetText(); got != "" {
		t.Fatalf("the caption leaked into the video message: %q", got)
	}
	// Duration in seconds, and the dimensions.
	if got := video.GetContent().GetVideoTime(); got != 16 {
		t.Fatalf("video_time = %d, want 16 (16300ms rounded)", got)
	}
	if got := video.GetContent().GetWidth(); got != 720 {
		t.Fatalf("width = %d, want 720", got)
	}
	if got := video.GetContent().GetHeight(); got != 1440 {
		t.Fatalf("height = %d, want 1440", got)
	}
	if got := video.GetContent().GetFileSize(); got != 1063644 {
		t.Fatalf("size = %d, want 1063644", got)
	}

	caption := reqs[1]
	if got := caption.GetContentType(); got != pb.ContentType_text {
		t.Fatalf("caption type = %v, want text", got)
	}
	if got := caption.GetContent().GetText(); got != "scjvkff" {
		t.Fatalf("caption = %q, want scjvkff", got)
	}
}

// TestMediaWithoutCaptionSendsOneMessage: when body is just the file name (what
// this homeserver itself emits), nothing is split.
func TestMediaWithoutCaptionSendsOneMessage(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.image","body":"photo.png","filename":"photo.png",` +
		`"url":"` + localMXC(s, "image/p.png") + `"}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}
	if got := len(f.snapshot()); got != 1 {
		t.Fatalf("upstream got %d messages, want 1", got)
	}
}
