package matrix

import (
	"net/http"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// videoPush builds a Yunhu video message. Yunhu has no dedicated video content
// type: the payload rides on a text message that carries video_url.
func videoPush(videoURL, text, name string, dur int32, w, h int32, size int64) *pb.PushMessage {
	return &pb.PushMessage{
		MsgId:       "v1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		ContentType: pb.ContentType_text,
		Timestamp:   1700000000000,
		Content: &pb.MsgContent{
			VideoUrl:  videoURL,
			VideoTime: dur,
			Text:      text,
			FileName:  name,
			Width:     w,
			Height:    h,
			FileSize:  size,
		},
	}
}

// TestYunhuVideoBecomesMatrixVideo is the reported gap: a video sent through 云湖
// must arrive in Matrix as m.video, not as a text message with a link.
func TestYunhuVideoBecomesMatrixVideo(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", videoPush(
		"video/7d924571cc52c4a6.mp4", "", "screen-20261001-222257.mp4",
		16, 720, 1440, 1063644,
	))

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "v1")
	s.Store.Mu.RUnlock()
	if ev == nil {
		t.Fatal("the video was not projected")
	}

	if got := asString(ev.Content["msgtype"]); got != "m.video" {
		t.Fatalf("msgtype = %q, want m.video", got)
	}
	// The url goes through the local proxy, never straight to the CDN.
	url := asString(ev.Content["url"])
	if url == "" || url[:6] != "mxc://" {
		t.Fatalf("url = %q, want an mxc:// reference", url)
	}
	if got := asString(ev.Content["body"]); got != "screen-20261001-222257.mp4" {
		t.Fatalf("body = %q, want the file name", got)
	}

	info, ok := ev.Content["info"].(map[string]any)
	if !ok {
		t.Fatalf("no info object: %v", ev.Content)
	}
	// Yunhu counts seconds; Matrix wants milliseconds.
	if got := asInt(info["duration"]); got != 16000 {
		t.Fatalf("duration = %d, want 16000 (16s in ms)", got)
	}
	if got := asInt(info["w"]); got != 720 {
		t.Fatalf("w = %d, want 720", got)
	}
	if got := asInt(info["h"]); got != 1440 {
		t.Fatalf("h = %d, want 1440", got)
	}
	if got := asInt64(info["size"]); got != 1063644 {
		t.Fatalf("size = %d, want 1063644", got)
	}
	// A mimetype has to be derived: Yunhu never sends one, and without it the
	// client does not know it can play the file.
	if got := asString(info["mimetype"]); got != "video/mp4" {
		t.Fatalf("mimetype = %q, want video/mp4", got)
	}
}

// TestYunhuVideoTextBecomesCaption: Yunhu puts a caption in text next to
// video_url, and Matrix renders body as the caption - so the text must win over
// the file name.
func TestYunhuVideoTextBecomesCaption(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", videoPush(
		"video/abc.mp4", "看我录的屏", "screen.mp4", 5, 0, 0, 0,
	))

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "v1")
	s.Store.Mu.RUnlock()

	if got := asString(ev.Content["msgtype"]); got != "m.video" {
		t.Fatalf("msgtype = %q", got)
	}
	if got := asString(ev.Content["body"]); got != "看我录的屏" {
		t.Fatalf("body = %q, want the caption", got)
	}
	// Zero dimensions must not be emitted: a 0/0 aspect ratio collapses the
	// video to nothing.
	info, _ := ev.Content["info"].(map[string]any)
	if _, set := info["w"]; set {
		t.Fatalf("w was emitted as %v despite being 0", info["w"])
	}
	if _, set := info["h"]; set {
		t.Fatalf("h was emitted as %v despite being 0", info["h"])
	}
}

// TestYunhuVideoWithNonTextContentType: the check keys on video_url being
// present, not on content_type, because the type is whatever the sender's client
// decided to set.
func TestYunhuVideoWithNonTextContentType(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	m := videoPush("video/xyz.mp4", "", "clip.mp4", 3, 100, 200, 999)
	// A type that is not text/unknown: the old guard would have missed this.
	m.ContentType = pb.ContentType_image
	s.ingest(acc, "push_message", m)

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "v1")
	s.Store.Mu.RUnlock()

	if got := asString(ev.Content["msgtype"]); got != "m.video" {
		t.Fatalf("msgtype = %q, want m.video despite content_type=image", got)
	}
}

// TestMatrixVideoUsesVideoContentType is the reported bug: sending a video from a
// Matrix client produced something the receiving Yunhu client would not render as
// a video. The cause was the content type - it went out as text (1), and Yunhu
// has a dedicated value for video (10, the one its own Android client sends).
func TestMatrixVideoUsesVideoContentType(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.video","body":"clip.mp4","filename":"clip.mp4",` +
		`"url":"` + localMXC(s, "video/clip.mp4") + `",` +
		`"info":{"mimetype":"video/mp4","size":1234,"w":720,"h":1440,"duration":5000}}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("got %d messages, want 1", len(reqs))
	}
	msg := reqs[0]
	if got := msg.GetContentType(); got != pb.ContentType_video {
		t.Fatalf("content type = %v, want video (10)", got)
	}
	// Sanity: the value really is 10 on the wire, not an enum alias.
	if got := int32(msg.GetContentType()); got != 10 {
		t.Fatalf("content type number = %d, want 10", got)
	}
	if got := msg.GetContent().GetVideoUrl(); !strings.Contains(got, "clip") {
		t.Fatalf("video_url = %q", got)
	}
	if got := msg.GetContent().GetVideoTime(); got != 5 {
		t.Fatalf("video_time = %d, want 5", got)
	}
}

// TestYunhuVideoContentTypeIsRecognised: the reverse direction has to accept the
// dedicated type too, which is what Yunhu's own client now sends.
func TestYunhuVideoContentTypeIsRecognised(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	m := videoPush("video/abc.mp4", "", "clip.mp4", 8, 100, 200, 555)
	m.ContentType = pb.ContentType_video
	s.ingest(acc, "push_message", m)

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "v1")
	s.Store.Mu.RUnlock()

	if got := asString(ev.Content["msgtype"]); got != "m.video" {
		t.Fatalf("msgtype = %q, want m.video", got)
	}
	info, _ := ev.Content["info"].(map[string]any)
	if got := asInt(info["duration"]); got != 8000 {
		t.Fatalf("duration = %d, want 8000", got)
	}
}
// TestYunhuAudioCarriesDuration: the same unit conversion applies to voice notes.
func TestYunhuAudioCarriesDuration(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "979377289", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	s.ingest(acc, "push_message", &pb.PushMessage{
		MsgId:       "a1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "979377289",
		ChatType:    pb.ChatType_group,
		ContentType: pb.ContentType_audio,
		Timestamp:   1700000000000,
		Content: &pb.MsgContent{
			AudioUrl:  "audio/abc.m4a",
			AudioTime: 7,
			FileName:  "voice.m4a",
			FileSize:  12345,
		},
	})

	s.Store.Mu.RLock()
	ev := eventOf(t, acc, room.RoomID, "a1")
	s.Store.Mu.RUnlock()

	if got := asString(ev.Content["msgtype"]); got != "m.audio" {
		t.Fatalf("msgtype = %q", got)
	}
	info, _ := ev.Content["info"].(map[string]any)
	if got := asInt(info["duration"]); got != 7000 {
		t.Fatalf("duration = %d, want 7000", got)
	}
	if got := asInt64(info["size"]); got != 12345 {
		t.Fatalf("size = %d, want 12345", got)
	}
	if got := asString(info["mimetype"]); got != "audio/mp4" {
		t.Fatalf("mimetype = %q", got)
	}
}
