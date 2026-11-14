package matrix

// To-device messaging.
//
// Matrix has two channels: room events, and direct session-to-session messages
// that never touch a room. Session verification runs entirely on the second
// one: m.key.verification.request / ready / start / accept / key / mac are all
// to-device events. With an always-empty to_device section, Element X's
// "verify with another device" screen accepts the tap and then waits forever,
// because the other device never receives the request - which looks exactly
// like an endless retry loop.

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	"github.com/yunhu/yhmatrix/internal/model"
)

// toDeviceQueueLimit bounds the per-account queue.
const toDeviceQueueLimit = 500

// sendToDevice implements PUT /_matrix/client/{ver}/sendToDevice/{eventType}/{txnId}.
func (s *Server) sendToDevice(c *Ctx) {
	var body struct {
		Messages map[string]map[string]json.RawMessage `json:"messages"`
	}
	raw, ok := readBody(c, e2eeBodyLimit)
	if !ok {
		return
	}
	if len(trimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Malformed request body")
			return
		}
	}

	eventType := c.Param("eventType")
	acc := c.Account

	// Only this user's sessions exist here, but the payload is still addressed
	// by user id, so a message for someone else is silently dropped the way a
	// real homeserver would drop it for a non-existent user.
	var queued int

	s.Store.Mu.Lock()
	for targetUser, devices := range body.Messages {
		if targetUser != acc.UserID {
			continue
		}
		// Deterministic order keeps the queue stable for tests and logs.
		ids := make([]string, 0, len(devices))
		for devID := range devices {
			ids = append(ids, devID)
		}
		sort.Strings(ids)
		for _, devID := range ids {
			content := devices[devID]
			if len(content) == 0 {
				continue
			}
			pos := acc.NextPos()
			acc.ToDevice = append(acc.ToDevice, &model.ToDeviceEvent{
				Pos:      pos,
				Type:     eventType,
				Sender:   acc.UserID,
				Content:  content,
				ToUser:   targetUser,
				ToDevice: devID,
			})
			queued++
		}
	}
	acc.TrimToDevice(toDeviceQueueLimit)
	s.Store.Mu.Unlock()

	if queued > 0 {
		s.Store.MarkDirty(acc.YunhuID)
		s.Hub.Notify(acc.YunhuID)
	}
	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// pendingToDevice returns the events a session has not consumed yet and moves
// its cursor past them. The caller must hold the store lock.
//
// The cursor is only advanced when events are actually returned, and the caller
// always answers immediately in that case, so nothing is dropped on the floor.
// pendingToDevice returns the events this device has not consumed yet.
//
// The result is never nil: a nil slice marshals to `null`, and serde's
// `#[serde(default)]` only covers a *missing* key, not an explicit null. So
// `"events": null` fails to deserialize just as hard as a missing next_batch,
// and the whole sync response is thrown away.
func pendingToDevice(acc *model.Account, dev *model.Device) []map[string]any {
	out := []map[string]any{}
	if dev == nil || len(acc.ToDevice) == 0 {
		return out
	}
	maxPos := dev.ToDevicePos
	for _, e := range acc.ToDevice {
		if e.Pos <= dev.ToDevicePos {
			continue
		}
		if e.ToDevice != "*" && e.ToDevice != dev.ID {
			continue
		}
		out = append(out, map[string]any{
			"type":    e.Type,
			"sender":  e.Sender,
			"content": json.RawMessage(e.Content),
		})
		if e.Pos > maxPos {
			maxPos = e.Pos
		}
	}
	if len(out) > 0 {
		dev.ToDevicePos = maxPos
	}
	return out
}

// toDeviceToken is the `next_batch` value the to_device extension must carry.
//
// It is not decorative: ruma declares `ToDevice { next_batch: String, .. }` with
// next_batch as a *required* field, so a response containing
// `"to_device": {"events": []}` and nothing else fails to deserialize. The
// client then reports a sync error, drops into its offline loop (which polls
// /versions with no backoff) and never advances its pos - which looks like an
// endless "cannot connect to the server" retry storm.
func toDeviceToken(acc *model.Account, dev *model.Device) string {
	pos := acc.Pos
	if dev != nil && dev.ToDevicePos > pos {
		pos = dev.ToDevicePos
	}
	return "t" + strconv.FormatInt(pos, 10)
}
