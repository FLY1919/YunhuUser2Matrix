package matrix

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

const (
	upstreamAvatar     = "https://qiniu.example.com/avatar.png"
	upstreamRoomAvatar = "https://qiniu.example.com/room.png"
	upstreamImage      = "https://qiniu.example.com/photo.jpg"
)

// collectExternalURLs walks a response payload and returns every string that is
// a bare http(s) URL. Clients must never receive one: everything has to be an
// mxc URI served by this homeserver, or the client goes straight to the Yunhu
// CDN (and on a LAN it usually cannot).
func collectExternalURLs(v any, path string, out *[]string) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "http://") || strings.HasPrefix(x, "https://") {
			*out = append(*out, path+" = "+x)
		}
	case map[string]any:
		for k, vv := range x {
			collectExternalURLs(vv, path+"."+k, out)
		}
	case []any:
		for i, vv := range x {
			collectExternalURLs(vv, fmt.Sprintf("%s[%d]", path, i), out)
		}
	case []map[string]any:
		for i, vv := range x {
			collectExternalURLs(vv, fmt.Sprintf("%s[%d]", path, i), out)
		}
	case []string:
		for i, vv := range x {
			collectExternalURLs(vv, fmt.Sprintf("%s[%d]", path, i), out)
		}
	}
}

// seedMediaFixture builds an account whose avatars and message image all point
// at an upstream CDN, including a member stored before proxying was added.
func seedMediaFixture(t *testing.T, s *Server, acc *accountRef) string {
	t.Helper()
	msg := &pb.PushMessage{
		MsgId:       "m1",
		Sender:      &pb.Sender{ChatId: "10002", ChatType: pb.ChatType_user, Name: "林晚"},
		ChatId:      "10002",
		ChatType:    pb.ChatType_user,
		Content:     &pb.MsgContent{ImageUrl: upstreamImage, FileName: "photo.jpg"},
		ContentType: pb.ContentType_image,
		Timestamp:   1700000000000,
	}
	s.ingest(acc, "push_message", msg)

	s.Store.Mu.Lock()
	defer s.Store.Mu.Unlock()
	acc.AvatarURL = upstreamAvatar
	roomID := ""
	for id, room := range acc.Rooms {
		roomID = id
		room.AvatarURL = upstreamRoomAvatar
		// Legacy data: avatars used to be stored verbatim.
		for _, m := range room.Members {
			m.AvatarURL = upstreamAvatar
		}
	}
	return roomID
}

// TestSyncHasNoUpstreamMediaURLs covers /sync: room state, member state and the
// message image must all be mxc URIs.
func TestSyncHasNoUpstreamMediaURLs(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	seedMediaFixture(t, s, acc)

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 30)
	s.Store.Mu.RUnlock()

	var hits []string
	collectExternalURLs(resp, "sync", &hits)
	if len(hits) > 0 {
		t.Fatalf("upstream URLs leaked into /sync:\n%s", strings.Join(hits, "\n"))
	}
}

// TestSlidingHasNoUpstreamMediaURLs covers sliding sync, which is what Element X
// actually uses: the room avatar and the heroes' avatars used to be raw.
func TestSlidingHasNoUpstreamMediaURLs(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	seedMediaFixture(t, s, acc)

	req := mustSliding(t, `{"lists":{"main":{"range":[0,19],"timeline_limit":10,`+
		`"required_state":[{"type":"m.room.avatar"},{"type":"m.room.member","state_key":"*"}]}}}`)

	s.Store.Mu.RLock()
	resp, _ := s.buildSliding(acc, nil, req, 0, true, false, nil)
	s.Store.Mu.RUnlock()

	var hits []string
	collectExternalURLs(resp, "sliding", &hits)
	if len(hits) > 0 {
		t.Fatalf("upstream URLs leaked into sliding sync:\n%s", strings.Join(hits, "\n"))
	}

	// Sanity: the room avatar is present and really is proxied.
	rooms, _ := resp["rooms"].(map[string]any)
	var avatar string
	for _, r := range rooms {
		if m, ok := r.(map[string]any); ok {
			if a, ok := m["avatar"].(string); ok {
				avatar = a
			}
		}
	}
	if !strings.HasPrefix(avatar, "mxc://") {
		t.Fatalf("room avatar = %q, want an mxc URI", avatar)
	}
	if _, ok := s.Media.decodeRemote(mediaIDOf(avatar)); !ok {
		t.Fatalf("room avatar %q is not a proxy reference", avatar)
	}
}

// TestMemberEndpointsProxyAvatars covers the REST member endpoints, which build
// their payloads from the member map rather than from state events.
func TestMemberEndpointsProxyAvatars(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	roomID := seedMediaFixture(t, s, acc)

	code, joined := doJSON(t, s, http.MethodGet,
		"/_matrix/client/v3/rooms/"+roomID+"/joined_members", "")
	if code != http.StatusOK {
		t.Fatalf("joined_members status %d", code)
	}
	var hits []string
	collectExternalURLs(joined, "joined_members", &hits)
	if len(hits) > 0 {
		t.Fatalf("upstream URLs leaked into joined_members:\n%s", strings.Join(hits, "\n"))
	}

	code, members := doJSON(t, s, http.MethodGet,
		"/_matrix/client/v3/rooms/"+roomID+"/members", "")
	if code != http.StatusOK {
		t.Fatalf("members status %d", code)
	}
	hits = nil
	collectExternalURLs(members, "members", &hits)
	if len(hits) > 0 {
		t.Fatalf("upstream URLs leaked into members:\n%s", strings.Join(hits, "\n"))
	}

	// And the profile endpoint for that same member. Resolve the peer id from
	// the fixture so the handler answers from memory instead of falling back to
	// an upstream lookup.
	s.Store.Mu.RLock()
	var peerID string
	if room := acc.Rooms[roomID]; room != nil {
		for uid := range room.Members {
			if uid != acc.UserID {
				peerID = uid
				break
			}
		}
	}
	s.Store.Mu.RUnlock()
	if peerID == "" {
		t.Fatal("fixture has no peer member")
	}

	code, profile := doJSON(t, s, http.MethodGet,
		"/_matrix/client/v3/profile/"+peerID, "")
	if code != http.StatusOK {
		t.Fatalf("profile status %d", code)
	}
	hits = nil
	collectExternalURLs(profile, "profile", &hits)
	if len(hits) > 0 {
		t.Fatalf("upstream URLs leaked into profile:\n%s", strings.Join(hits, "\n"))
	}
	if avatar, _ := profile["avatar_url"].(string); !strings.HasPrefix(avatar, "mxc://") {
		t.Fatalf("profile avatar = %q, want an mxc URI", avatar)
	}
}

// TestImageMessageUsesProxy checks the message payload itself and that the
// proxied id decodes back to the upstream reference the fetcher needs.
func TestImageMessageUsesProxy(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)
	seedMediaFixture(t, s, acc)

	s.Store.Mu.RLock()
	resp, _ := s.buildSync(acc, nil, 0, true, 30)
	s.Store.Mu.RUnlock()

	var found bool
	for _, r := range resp["rooms"].(map[string]any)["join"].(map[string]any) {
		room := r.(map[string]any)
		timeline := room["timeline"].(map[string]any)["events"].([]map[string]any)
		for _, ev := range timeline {
			if ev["type"] != "m.room.message" {
				continue
			}
			content := ev["content"].(map[string]any)
			if content["msgtype"] != "m.image" {
				continue
			}
			found = true
			url := content["url"].(string)
			if !strings.HasPrefix(url, "mxc://") {
				t.Fatalf("image url = %q, want an mxc URI", url)
			}
			ref, ok := s.Media.decodeRemote(mediaIDOf(url))
			if !ok || ref != upstreamImage {
				t.Fatalf("image proxy reference = %q (ok=%v), want %q", ref, ok, upstreamImage)
			}
		}
	}
	if !found {
		t.Fatal("no m.image event in the timeline")
	}
}

// TestRemoteMXCPassesMXCThrough makes sure an already-proxied value is not
// wrapped twice, which would corrupt the reference.
func TestRemoteMXCPassesMXCThrough(t *testing.T) {
	s, _ := newTestServer(t)
	const already = "mxc://test.local/yh_abc"
	if got := s.Media.RemoteMXC(already); got != already {
		t.Fatalf("RemoteMXC(%q) = %q, want it unchanged", already, got)
	}
	if got := s.Media.RemoteMXC(""); got != "" {
		t.Fatalf("RemoteMXC(\"\") = %q, want empty", got)
	}
}

// mediaIDOf strips the server name from an mxc URI.
func mediaIDOf(mxc string) string {
	rest := strings.TrimPrefix(mxc, "mxc://")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i+1:]
	}
	return rest
}
