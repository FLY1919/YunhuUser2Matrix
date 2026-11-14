package matrix

import (
	"context"
	"net/http"
	"testing"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// TestEmptyTopicIsFetchedOnceThenCached: a group with no introduction is a real
// state, not "unknown". It has to be asked once - otherwise a room created before
// this field existed never gets its description - but the answer must be
// remembered, or every reconnect re-fetches the group just to find "" again.
func TestEmptyTopicIsFetchedOnceThenCached(t *testing.T) {
	f := &fakeGroupEdit{introduction: ""}
	s, acc, room := newGroupEditServer(t, f)

	s.refreshMemberCounts(context.Background(), acc, s.YunhuClient().WithToken(acc.Token))
	if n := f.infoCalls.Load().(int); n == 0 {
		t.Fatal("an entry with no topic was never asked for")
	}
	s.Store.Mu.RLock()
	stamped := room.TopicTS
	s.Store.Mu.RUnlock()
	if stamped == 0 {
		t.Fatal("TopicTS was not stamped: the room would be re-fetched on every reconnect")
	}

	// A second pass must be served from the cache.
	first := f.infoCalls.Load().(int)
	s.refreshMemberCounts(context.Background(), acc, s.YunhuClient().WithToken(acc.Token))
	if got := f.infoCalls.Load().(int); got != first {
		t.Fatalf("group/info called %d times, want the fresh answer to be cached (%d)", got, first)
	}
}

// TestTopicIsFetchedForRoomsWithFreshCounts: the freshness check used to look
// only at headcount and roles, so a room that already had both was never asked
// again - and its description stayed empty forever.
func TestTopicIsFetchedForRoomsWithFreshCounts(t *testing.T) {
	f := &fakeGroupEdit{introduction: "早就存在的简介"}
	s, acc, room := newGroupEditServer(t, f)

	// Pretend a previous build filled in the counts and roles but never the
	// topic, which is exactly the state of an existing database.
	s.Store.Mu.Lock()
	room.MemberCount = 12
	room.MemberCountTS = nowUnixMilli()
	room.RolesTS = nowUnixMilli()
	s.Store.Mu.Unlock()

	s.refreshMemberCounts(context.Background(), acc, s.YunhuClient().WithToken(acc.Token))

	s.Store.Mu.RLock()
	topic := room.Topic
	s.Store.Mu.RUnlock()
	if topic != "早就存在的简介" {
		t.Fatalf("topic = %q, want the introduction", topic)
	}
}

// TestMatrixVideoSendsMediaAttachment is the reported "视频消息解析错误": the
// message arrived as an empty m.text because the send carried neither the video
// content type nor the media object Yunhu's own client fills in.
func TestMatrixVideoSendsMediaAttachment(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.video","body":"clip","filename":"clip.mp4",` +
		`"url":"` + localMXC(s, "video/abcdef123456.mp4") + `",` +
		`"info":{"mimetype":"video/mp4","size":1063644,"w":720,"h":1440,"duration":16300}}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	// The video, then its caption as a separate text message: Yunhu's client
	// renders the type-10 message as a player with no text at all.
	if len(reqs) != 2 {
		t.Fatalf("got %d messages, want 2 (video then caption)", len(reqs))
	}
	msg := reqs[0]
	if got := msg.GetContentType(); got != pb.ContentType_video {
		t.Fatalf("first message type = %v, want video", got)
	}

	// The media object is what makes the other side render a player.
	media := msg.GetMedia()
	if media == nil {
		t.Fatal("no media object: the receiving client cannot tell this is a video")
	}
	if media.GetFileKey() == "" {
		t.Fatal("media.file_key is empty")
	}
	// file_key2 repeats file_key; Yunhu's own client fills both.
	if media.GetFileKey2() != media.GetFileKey() {
		t.Fatalf("file_key2 = %q, want the same as file_key %q",
			media.GetFileKey2(), media.GetFileKey())
	}
	if got := media.GetFileType(); got != "video/mp4" {
		t.Fatalf("media.file_type = %q, want video/mp4", got)
	}
	if got := media.GetFileSuffix(); got != "mp4" {
		t.Fatalf("media.file_suffix = %q, want mp4", got)
	}
	if got := media.GetFileSize(); got != 1063644 {
		t.Fatalf("media.file_size = %d", got)
	}
	if got := media.GetImageWidth(); got != 720 {
		t.Fatalf("media.image_width = %d", got)
	}
	if got := media.GetImageHeight(); got != 1440 {
		t.Fatalf("media.image_height = %d", got)
	}
	// The hash is the object key's base name, which this homeserver derived from
	// the file's MD5.
	if got := media.GetFileHash(); got != "abcdef123456" {
		t.Fatalf("media.file_hash = %q, want the key's base name", got)
	}
}

// TestImageAndAudioSendMediaToo: the same attachment object is sent for images
// and voice notes, which is how Yunhu's client does it.
func TestImageAndAudioSendMediaToo(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	img := `{"msgtype":"m.image","body":"photo.png","filename":"photo.png",` +
		`"url":"` + localMXC(s, "image/aaa111.png") + `",` +
		`"info":{"mimetype":"image/png","size":167955,"w":576,"h":1152}}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", img); code != http.StatusOK {
		t.Fatalf("image status %d: %v", code, resp)
	}

	audio := `{"msgtype":"m.audio","body":"voice.m4a","filename":"voice.m4a",` +
		`"url":"` + localMXC(s, "audio/bbb222.m4a") + `",` +
		`"info":{"mimetype":"audio/mp4","size":1234,"duration":7000}}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t2", audio); code != http.StatusOK {
		t.Fatalf("audio status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("got %d messages, want 2", len(reqs))
	}
	for i, wantType := range []string{"image/png", "audio/mp4"} {
		media := reqs[i].GetMedia()
		if media == nil {
			t.Fatalf("message %d has no media object", i)
		}
		if got := media.GetFileType(); got != wantType {
			t.Fatalf("message %d file_type = %q, want %q", i, got, wantType)
		}
		if media.GetFileKey() == "" {
			t.Fatalf("message %d has an empty file_key", i)
		}
	}
}

// TestVideoCaptionIsSentAsItsOwnMessage is the reported "备注去哪了": the caption
// was written into the video message's content.text, but Yunhu's client renders a
// content_type=10 message as a video player and never draws its text - so the
// caption was invisible on the other side.
//
// The caption has to travel as its own text message, exactly like a file's note.
func TestVideoCaptionIsSentAsItsOwnMessage(t *testing.T) {
	f := &fakeFileSend{}
	s, acc, room := fileServer(t, f)

	body := `{"msgtype":"m.video","body":"我自己写的备注","filename":"clip.mp4",` +
		`"url":"` + localMXC(s, "video/deadbeef.mp4") + `",` +
		`"info":{"mimetype":"video/mp4","size":100,"duration":3000}}`
	code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}

	reqs := f.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("got %d messages, want 2 (video then caption)", len(reqs))
	}

	// The video message must stay clean: content.text would never be rendered.
	video := reqs[0]
	if got := video.GetContentType(); got != pb.ContentType_video {
		t.Fatalf("video type = %v, want 10", got)
	}
	if got := video.GetContent().GetText(); got != "" {
		t.Fatalf("the caption was written into the video message (%q), where Yunhu never shows it", got)
	}
	if got := video.GetContent().GetFileName(); got != "clip.mp4" {
		t.Fatalf("file name = %q", got)
	}

	// And it must exist as a separate, visible message.
	caption := reqs[1]
	if got := caption.GetContentType(); got != pb.ContentType_text {
		t.Fatalf("caption type = %v, want text", got)
	}
	if got := caption.GetContent().GetText(); got != "我自己写的备注" {
		t.Fatalf("caption = %q, want 我自己写的备注", got)
	}

	// The sender's own timeline keeps the note too, or they lose the text they
	// typed the moment it is sent.
	s.Store.Mu.RLock()
	var noteSeen bool
	for _, ev := range acc.Rooms[room.RoomID].Events {
		if ev.IsState() {
			continue
		}
		if asString(ev.Content["body"]) == "我自己写的备注" {
			noteSeen = true
		}
	}
	s.Store.Mu.RUnlock()
	if !noteSeen {
		t.Fatal("the caption is missing from the local timeline")
	}
}

// TestVideoWithoutCaptionSendsOneMessage: no caption means no second message.
func TestVideoWithoutCaptionSendsOneMessage(t *testing.T) {
	f := &fakeFileSend{}
	s, _, room := fileServer(t, f)

	body := `{"msgtype":"m.video","body":"clip.mp4","filename":"clip.mp4",` +
		`"url":"` + localMXC(s, "video/x.mp4") + `","info":{"duration":1000}}`
	if code, resp := putJSON(t, s, "/_matrix/client/v3/rooms/"+room.RoomID+
		"/send/m.room.message/t1", body); code != http.StatusOK {
		t.Fatalf("status %d: %v", code, resp)
	}
	if got := len(f.snapshot()); got != 1 {
		t.Fatalf("got %d messages, want 1 (body equals filename, so there is no caption)", got)
	}
}

// TestMediaAttachmentSkipsAbsoluteURLs: media.file_key has to be an object key.
// An absolute URL (what ResolveForYunhu returns when the upload fell back to
// public_url) would make Yunhu build a nonsense path, so no media object is sent.
func TestMediaAttachmentSkipsAbsoluteURLs(t *testing.T) {
	if got := mediaAttachment("https://example.com/a.mp4", "video/mp4", "", mediaInfo{}); got != nil {
		t.Fatalf("an absolute URL produced a media object: %+v", got)
	}
	if got := mediaAttachment("", "video/mp4", "", mediaInfo{}); got != nil {
		t.Fatalf("an empty key produced a media object: %+v", got)
	}
	// A bare key does produce one, with its suffix derived from the key.
	got := mediaAttachment("video/abc.mp4", "", "", mediaInfo{Size: 10})
	if got == nil {
		t.Fatal("a bare key produced no media object")
	}
	if got.GetFileType() != "video/mp4" {
		t.Fatalf("file_type = %q, want it derived from the key", got.GetFileType())
	}
	if got.GetFileSuffix() != "mp4" {
		t.Fatalf("file_suffix = %q", got.GetFileSuffix())
	}
}
