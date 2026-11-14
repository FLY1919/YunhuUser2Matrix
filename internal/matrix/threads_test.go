package matrix

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yunhu/yhmatrix/internal/model"
)

// TestThreadsEndpointIsRouted is the regression test for the 404 in the log:
//
//	GET /_matrix/client/v1/rooms/!yh-group-…/threads status=404 dur_ms=0
//
// `dur_ms=0` means the router never matched, so the client got M_UNRECOGNIZED
// and rendered "failed to load threads". Yunhu has no threads, so an empty list
// is the correct answer - but the route has to exist.
func TestThreadsEndpointIsRouted(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	s.Store.Mu.Lock()
	room := s.ensureRoomLocked(acc, "20001", model.ChatTypeGroup, "工作群", "")
	s.Store.Mu.Unlock()

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v1/rooms/"+room.RoomID+"/threads?limit=5&include=all", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
		t.Fatalf("route is not registered: %s", body)
	}

	var out struct {
		Chunk     []any   `json:"chunk"`
		NextBatch *string `json:"next_batch"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Chunk == nil {
		t.Fatalf("chunk must be an empty array, not null: %s", rec.Body.String())
	}
	if len(out.Chunk) != 0 {
		t.Fatalf("chunk = %d entries, want 0 (Yunhu has no threads)", len(out.Chunk))
	}
	// Synapse answers next_batch: null when there is nothing more to page.
	if out.NextBatch != nil {
		t.Fatalf("next_batch = %q, want null", *out.NextBatch)
	}
}

// TestThreadsUnknownRoom: an existing route answers M_NOT_FOUND for a room this
// account does not have, which is a different error from an unrouted path.
func TestThreadsUnknownRoom(t *testing.T) {
	s, st := newTestServer(t)
	acc := seedAccount(t, st)
	seedDevice(t, st, acc)

	req := httptest.NewRequest(http.MethodGet,
		"/_matrix/client/v3/rooms/!yh-group-99999:test.local/threads", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "M_UNRECOGNIZED") {
		t.Fatalf("route is not registered: %s", body)
	}
}
