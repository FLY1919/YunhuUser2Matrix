package matrix

import (
	"context"
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// TestImageSendUsesImageField pins the wire field for an outgoing picture.
//
// Yunhu's client uploads to Qiniu and then sends the bare object key in
// `content.image` (field 9, "key/url"). `content.image_url` (3) is the field the
// API reference documents for the JSON API, and the server derives it from the
// key itself - writing a full URL there as well risks a double-prefixed link,
// which is why only field 9 is filled.
func TestImageSendUsesImageField(t *testing.T) {
	s, _ := newTestServer(t)

	// A reference that is already a Yunhu object (what a re-sent picture looks
	// like): ResolveForYunhu hands the bare key straight back, no upload.
	const key = "85ade3342b00c98361d3322347758104.png"
	content := map[string]any{
		"msgtype": "m.image",
		"body":    "photo.png",
		"url":     s.Media.RemoteMXC(key),
		"info":    map[string]any{"w": 640, "h": 480},
	}

	req, err := s.buildSendRequest(context.Background(), nil, "20001", model.ChatTypeGroup,
		"m.room.message", content, "m1")
	if err != nil {
		t.Fatalf("buildSendRequest: %v", err)
	}
	if req.GetContentType() != pb.ContentType_image {
		t.Fatalf("content type = %v, want image", req.GetContentType())
	}
	if got := req.GetContent().GetImage(); got != key {
		t.Fatalf("content.image = %q, want the Qiniu key %q", got, key)
	}
	if got := req.GetContent().GetImageUrl(); got != "" {
		t.Fatalf("content.image_url = %q, want it empty (the server fills it in)", got)
	}
	if got := req.GetContent().GetFileName(); got != "photo.png" {
		t.Fatalf("file name = %q", got)
	}
	if got := req.GetContent().GetWidth(); got != 640 {
		t.Fatalf("width = %d, want 640", got)
	}

	// An absolute URL (a picture that lives on the CDN already) passes through
	// unchanged - field 9 accepts "key/url".
	const url = "https://chat-img.jwznb.com/abc.gif"
	req2, err := s.buildSendRequest(context.Background(), nil, "10002", model.ChatTypeUser,
		"m.room.message", map[string]any{
			"msgtype": "m.image",
			"body":    "abc.gif",
			"url":     url,
		}, "m2")
	if err != nil {
		t.Fatalf("buildSendRequest: %v", err)
	}
	if got := req2.GetContent().GetImage(); got != url {
		t.Fatalf("content.image = %q, want %q", got, url)
	}
}

// TestFileAndAudioSendsKeepTheirKeyFields: the same "key/url" rule applies to
// files (5), video (19) and audio (21), and those must not drift either.
func TestFileAndAudioSendsKeepTheirKeyFields(t *testing.T) {
	s, _ := newTestServer(t)

	fileReq, err := s.buildSendRequest(context.Background(), nil, "20001", model.ChatTypeGroup,
		"m.room.message", map[string]any{
			"msgtype":  "m.file",
			"body":     "report.pdf",
			"filename": "report.pdf",
			"url":      s.Media.RemoteMXC("disk/abc.pdf"),
			"info":     map[string]any{"size": 1234},
		}, "m1")
	if err != nil {
		t.Fatalf("buildSendRequest: %v", err)
	}
	if got := fileReq.GetContent().GetFileUrl(); got != "disk/abc.pdf" {
		t.Fatalf("content.file_url = %q", got)
	}
	if got := fileReq.GetContent().GetFileSize(); got != 1234 {
		t.Fatalf("file size = %d", got)
	}

	audioReq, err := s.buildSendRequest(context.Background(), nil, "20001", model.ChatTypeGroup,
		"m.room.message", map[string]any{
			"msgtype": "m.audio",
			"body":    "voice.m4a",
			"url":     s.Media.RemoteMXC("abc.m4a"),
			"info":    map[string]any{"duration": 3000},
		}, "m2")
	if err != nil {
		t.Fatalf("buildSendRequest: %v", err)
	}
	if got := audioReq.GetContent().GetAudioUrl(); got != "abc.m4a" {
		t.Fatalf("content.audio_url = %q", got)
	}
	if got := audioReq.GetContent().GetAudioTime(); got != 3 {
		t.Fatalf("audio time = %d, want 3s", got)
	}
}
