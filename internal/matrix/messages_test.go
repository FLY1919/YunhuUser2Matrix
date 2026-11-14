package matrix

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
	"github.com/yunhu/yhmatrix/internal/yunhu/pb"
)

// getMessages calls the /messages endpoint through the real router.
func getMessages(t *testing.T, s *Server, roomID, query string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/"+roomID+"/messages?"+query, nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("messages status %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

func seedDevice(t *testing.T, st *store.Store, acc *model.Account) {
	t.Helper()
	st.AddDevice(acc, &model.Device{ID: "D1", Token: "tok", LastSeen: time.Now().UnixMilli()})
}

// TestMessagesEmptyRoomReturnsValidTokens covers the reported symptom: a room
// whose only events are state must answer with an empty chunk but still a valid
// start token, and must omit end so the client stops paginating.
func TestMessagesEmptyRoomReturnsValidTokens(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	s.ensureRoomLocked(acc, "21385383", model.ChatTypeBot, "Kira-chan", "")
	s.Store.Mu.Unlock()

	body := getMessages(t, s, "!yh-bot-21385383:test.local", "from=s0&dir=b&limit=30")

	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 0 {
		t.Fatalf("state events leaked into the chunk: %d entries", len(chunk))
	}
	start, _ := body["start"].(string)
	if start == "" {
		t.Fatal("start must be a valid token, never an empty string")
	}
	if _, ok := body["end"]; ok {
		t.Fatalf("end must be omitted when nothing older exists, got %v", body["end"])
	}
}

// TestMessagesExcludesStateEvents makes sure bootstrap state never consumes the
// page limit or shows up as a fake message.
func TestMessagesExcludesStateEvents(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	for i := 1; i <= 3; i++ {
		ingestText(t, s, acc, fmt.Sprintf("m%d", i), "10002", pb.ChatType_user,
			fmt.Sprintf("msg%d", i), uint64(1700000000000+i*1000))
	}

	body := getMessages(t, s, "!yh-user-10002:test.local", "dir=b&limit=50")
	chunk, _ := body["chunk"].([]any)
	if len(chunk) != 3 {
		t.Fatalf("chunk = %d events, want 3 messages only", len(chunk))
	}
	for _, raw := range chunk {
		ev := raw.(map[string]any)
		if _, isState := ev["state_key"]; isState {
			t.Fatalf("state event in chunk: %v", ev["type"])
		}
		if ev["type"] != "m.room.message" {
			t.Fatalf("unexpected event type %v", ev["type"])
		}
	}
	// Backward pagination is reverse-chronological: newest first.
	first := chunk[0].(map[string]any)["content"].(map[string]any)["body"]
	if first != "msg3" {
		t.Fatalf("first chunk event = %v, want msg3", first)
	}
}

// TestMessagesPaginationTerminates walks the whole history and checks that the
// chain yields every message exactly once and then stops.
func TestMessagesPaginationTerminates(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	const total = 25
	for i := 1; i <= total; i++ {
		ingestText(t, s, acc, fmt.Sprintf("m%d", i), "10002", pb.ChatType_user,
			fmt.Sprintf("msg%d", i), uint64(1700000000000+i*1000))
	}

	seen := map[string]int{}
	from := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		query := "dir=b&limit=10"
		if from != "" {
			query += "&from=" + from
		}
		body := getMessages(t, s, "!yh-user-10002:test.local", query)

		chunk, _ := body["chunk"].([]any)
		for _, raw := range chunk {
			id, _ := raw.(map[string]any)["event_id"].(string)
			seen[id]++
		}

		start, _ := body["start"].(string)
		if start == "" {
			t.Fatalf("page %d: empty start token", pages)
		}

		end, hasEnd := body["end"].(string)
		if !hasEnd || end == "" {
			if len(chunk) == 0 && len(seen) < total {
				t.Fatalf("chain stopped early after %d of %d messages", len(seen), total)
			}
			break
		}
		if end == from {
			t.Fatalf("page %d: end token did not advance (%q)", pages, end)
		}
		from = end
	}

	if len(seen) != total {
		t.Fatalf("paginated %d of %d messages", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s returned %d times", id, n)
		}
	}
}

// TestMessagesUnknownRoom keeps the 404 contract.
func TestMessagesUnknownRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/!nope:test.local/messages?dir=b", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
