package matrix

// End-to-end encryption plumbing: device keys, one-time keys, cross-signing
// keys, signatures, secret storage and key backup.
//
// Why a homeserver that never encrypts anything still needs all of this:
// Element X is E2EE-first. After login it runs a first-time-user flow that
// ends on the "confirm your identity" screen, and that screen is fed by
// /keys/query and by the secret-storage account data. The old stubs answered
// with empty maps and threw account data away, so:
//
//   - the client could not see its own device,
//   - RecoveryState never left WAITING_FOR_SYNC,
//   - the screen rendered a spinner with no buttons,
//   - and RustEncryptionService.pollEvery5s kept calling /keys/query forever
//     (see RustEncryptionService.hasDevicesToVerifyAgainst), which is the
//     endless request loop users see.
//
// Storing and redistributing the public halves fixes all four: the client can
// read its own keys, bootstrap or reset cross-signing, and finish the flow.
// The server still never holds a private key.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/yunhu/yhmatrix/internal/model"
)

// e2eeBodyLimit bounds key uploads. A device keys object is a couple of KB;
// one-time keys are uploaded in batches of a few hundred.
const e2eeBodyLimit = 4 << 20

// readBody returns the request body, capped.
func readBody(c *Ctx, limit int64) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(c.R.Body, limit))
	if err != nil {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Could not read request body")
		return nil, false
	}
	return body, true
}

// deviceIDOf returns the authenticated device id, or "".
func deviceIDOf(c *Ctx) string {
	if c.Device == nil {
		return ""
	}
	return c.Device.ID
}

// --- /keys/upload -----------------------------------------------------------

// keysUpload stores device keys, one-time keys and fallback keys.
func (s *Server) keysUpload(c *Ctx) {
	var body struct {
		DeviceKeys   json.RawMessage            `json:"device_keys"`
		OneTimeKeys  map[string]json.RawMessage `json:"one_time_keys"`
		FallbackKeys map[string]json.RawMessage `json:"fallback_keys"`
		// MSC2732 name, still sent by some clients.
		MSC2732Fallback map[string]json.RawMessage `json:"org.matrix.msc2732.fallback_keys"`
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

	acc := c.Account
	devID := deviceIDOf(c)

	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()

	// device_keys carries its own device id; trust it over the token when both
	// are present, because a client may upload for the device it just created.
	if len(body.DeviceKeys) > 0 {
		var dk struct {
			DeviceID string `json:"device_id"`
		}
		_ = json.Unmarshal(body.DeviceKeys, &dk)
		if dk.DeviceID != "" {
			devID = dk.DeviceID
		}
		// Only a real change bumps the stream. A client that re-uploads the
		// same device keys (they do, on every launch) would otherwise make
		// /sync report device_lists.changed forever, so the sync would always
		// have "content" and never long-poll - a busy loop.
		if devID != "" && !bytes.Equal(st.Devices[devID], body.DeviceKeys) {
			st.Devices[devID] = body.DeviceKeys
			st.DeviceListPos = acc.NextPos()
		}
	}

	if len(body.OneTimeKeys) > 0 && devID != "" {
		m := st.OneTimeKeys[devID]
		if m == nil {
			m = map[string]json.RawMessage{}
			st.OneTimeKeys[devID] = m
		}
		for keyID, key := range body.OneTimeKeys {
			m[keyID] = key
		}
	}

	fallback := body.FallbackKeys
	if len(fallback) == 0 {
		fallback = body.MSC2732Fallback
	}
	if len(fallback) > 0 && devID != "" {
		m := st.FallbackKeys[devID]
		if m == nil {
			m = map[string]json.RawMessage{}
			st.FallbackKeys[devID] = m
		}
		for keyID, key := range fallback {
			m[keyID] = key
		}
	}

	counts := oneTimeKeyCounts(st.OneTimeKeys[devID])
	unusedFallback := fallbackAlgorithms(st.FallbackKeys[devID])
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{
		"one_time_key_counts":              counts,
		"device_unused_fallback_key_types": unusedFallback,
	})
}

// oneTimeKeyCounts reports how many stored keys each algorithm has. Returning
// the real counts stops clients from re-uploading a full batch on every sync.
func oneTimeKeyCounts(m map[string]json.RawMessage) map[string]int {
	out := map[string]int{}
	for keyID := range m {
		out[algorithmOf(keyID)]++
	}
	return out
}

func fallbackAlgorithms(m map[string]json.RawMessage) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(m))
	for keyID := range m {
		alg := algorithmOf(keyID)
		if _, ok := seen[alg]; ok {
			continue
		}
		seen[alg] = struct{}{}
		out = append(out, alg)
	}
	sort.Strings(out)
	return out
}

// algorithmOf splits "signed_curve25519:AAAAAQ" into "signed_curve25519".
func algorithmOf(keyID string) string {
	if i := strings.IndexByte(keyID, ':'); i > 0 {
		return keyID[:i]
	}
	return keyID
}

// --- /keys/query ------------------------------------------------------------

// keysQuery returns stored device keys and cross-signing keys.
func (s *Server) keysQuery(c *Ctx) {
	var body struct {
		DeviceKeys map[string][]string `json:"device_keys"`
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

	acc := c.Account
	s.Store.Mu.RLock()
	st := acc.E2EE
	deviceKeys := map[string]any{}
	master := map[string]any{}
	selfSigning := map[string]any{}
	userSigning := map[string]any{}
	if st != nil {
		// Only this user exists as a Matrix user here, so only this user can
		// have keys. An empty device list means "all devices"; only a non-empty
		// list filters.
		want, asked := body.DeviceKeys[acc.UserID]
		asked = asked && len(want) > 0
		devices := map[string]any{}
		for devID, dk := range st.Devices {
			if asked && !containsString(want, devID) {
				continue
			}
			devices[devID] = json.RawMessage(dk)
		}
		if len(devices) > 0 {
			deviceKeys[acc.UserID] = devices
		}
		if len(st.MasterKey) > 0 {
			master[acc.UserID] = json.RawMessage(st.MasterKey)
		}
		if len(st.SelfSigningKey) > 0 {
			selfSigning[acc.UserID] = json.RawMessage(st.SelfSigningKey)
		}
		if len(st.UserSigningKey) > 0 {
			userSigning[acc.UserID] = json.RawMessage(st.UserSigningKey)
		}
	}
	s.Store.Mu.RUnlock()

	resp := map[string]any{
		"device_keys": deviceKeys,
		"failures":    map[string]any{},
	}
	// Cross-signing maps are omitted rather than empty when absent, which is
	// how clients tell "not set up" from "exists but empty".
	if len(master) > 0 {
		resp["master_keys"] = master
	}
	if len(selfSigning) > 0 {
		resp["self_signing_keys"] = selfSigning
	}
	if len(userSigning) > 0 {
		resp["user_signing_keys"] = userSigning
	}
	writeJSON(c.W, http.StatusOK, resp)
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// --- /keys/claim ------------------------------------------------------------

// keysClaim hands out one-time keys, consuming them.
func (s *Server) keysClaim(c *Ctx) {
	var body struct {
		OneTimeKeys map[string]map[string]string `json:"one_time_keys"`
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

	acc := c.Account
	out := map[string]any{}

	s.Store.Mu.Lock()
	st := acc.E2EE
	if st != nil {
		for userID, devices := range body.OneTimeKeys {
			if userID != acc.UserID {
				continue
			}
			claimed := map[string]any{}
			for devID, alg := range devices {
				keyID, key := takeOneTimeKey(st.OneTimeKeys[devID], alg)
				if key == nil {
					// Fall back to the fallback key, which is never consumed:
					// that is exactly what it exists for.
					if fb := pickFallback(st.FallbackKeys[devID], alg); fb != nil {
						claimed[devID] = fb
					}
					continue
				}
				claimed[devID] = map[string]json.RawMessage{keyID: key}
			}
			if len(claimed) > 0 {
				out[userID] = claimed
			}
		}
	}
	s.Store.Mu.Unlock()
	if len(out) > 0 {
		s.Store.MarkDirty(acc.YunhuID)
	}

	writeJSON(c.W, http.StatusOK, map[string]any{
		"one_time_keys": out,
		"failures":      map[string]any{},
	})
}

// takeOneTimeKey removes and returns the first stored key of an algorithm.
func takeOneTimeKey(m map[string]json.RawMessage, alg string) (string, json.RawMessage) {
	ids := make([]string, 0, len(m))
	for keyID := range m {
		if algorithmOf(keyID) == alg {
			ids = append(ids, keyID)
		}
	}
	if len(ids) == 0 {
		return "", nil
	}
	sort.Strings(ids)
	keyID := ids[0]
	key := m[keyID]
	delete(m, keyID)
	return keyID, key
}

func pickFallback(m map[string]json.RawMessage, alg string) map[string]json.RawMessage {
	ids := make([]string, 0, len(m))
	for keyID := range m {
		if algorithmOf(keyID) == alg {
			ids = append(ids, keyID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	return map[string]json.RawMessage{ids[0]: m[ids[0]]}
}

// --- /keys/changes ----------------------------------------------------------

// keysChanges reports whether the caller's own device list changed since from.
func (s *Server) keysChanges(c *Ctx) {
	acc := c.Account
	from := parseSince(c.Query("from"))

	s.Store.Mu.RLock()
	pos := int64(0)
	if acc.E2EE != nil {
		pos = acc.E2EE.DeviceListPos
	}
	s.Store.Mu.RUnlock()

	changed := []string{}
	if pos > from {
		changed = append(changed, acc.UserID)
	}
	writeJSON(c.W, http.StatusOK, map[string]any{"changed": changed, "left": []string{}})
}

// --- /keys/device_signing/upload --------------------------------------------

// deviceSigningUpload stores the cross-signing keys a client bootstrapped.
//
// Synapse guards this with user-interactive auth. This homeserver has no
// password of its own to re-check (the credential is the Yunhu token), so the
// upload is accepted directly; refusing would leave clients unable to finish
// or reset their identity at all.
func (s *Server) deviceSigningUpload(c *Ctx) {
	var body struct {
		MasterKey      json.RawMessage `json:"master_key"`
		SelfSigningKey json.RawMessage `json:"self_signing_key"`
		UserSigningKey json.RawMessage `json:"user_signing_key"`
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

	acc := c.Account
	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	if len(body.MasterKey) > 0 {
		st.MasterKey = body.MasterKey
	}
	if len(body.SelfSigningKey) > 0 {
		st.SelfSigningKey = body.SelfSigningKey
	}
	if len(body.UserSigningKey) > 0 {
		st.UserSigningKey = body.UserSigningKey
	}
	st.DeviceListPos = acc.NextPos()
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// --- /keys/signatures/upload ------------------------------------------------

// signatureUpload merges signatures onto already-stored keys.
//
// The body maps user id -> key id -> the full signed object. Signatures for a
// device are merged into that device's stored keys; signatures for a
// cross-signing key are merged into the matching key, which is how a device
// becomes cross-signed and the client finally reports the session as verified.
func (s *Server) signatureUpload(c *Ctx) {
	var body map[string]map[string]json.RawMessage
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

	acc := c.Account
	failures := map[string]any{}

	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	for userID, keys := range body {
		if userID != acc.UserID {
			continue
		}
		for keyID, signed := range keys {
			var obj struct {
				DeviceID string   `json:"device_id"`
				Usage    []string `json:"usage"`
			}
			_ = json.Unmarshal(signed, &obj)

			switch {
			case obj.DeviceID != "":
				stored, exists := st.Devices[obj.DeviceID]
				merged := signed
				if exists {
					merged = mergeSignatures(stored, signed)
				}
				if !exists || !bytes.Equal(stored, merged) {
					st.Devices[obj.DeviceID] = merged
					st.DeviceListPos = acc.NextPos()
				}
			case containsString(obj.Usage, "master"):
				if merged := mergeSignatures(st.MasterKey, signed); !bytes.Equal(st.MasterKey, merged) {
					st.MasterKey = merged
					st.DeviceListPos = acc.NextPos()
				}
			case containsString(obj.Usage, "self_signing"):
				if merged := mergeSignatures(st.SelfSigningKey, signed); !bytes.Equal(st.SelfSigningKey, merged) {
					st.SelfSigningKey = merged
					st.DeviceListPos = acc.NextPos()
				}
			case containsString(obj.Usage, "user_signing"):
				if merged := mergeSignatures(st.UserSigningKey, signed); !bytes.Equal(st.UserSigningKey, merged) {
					st.UserSigningKey = merged
					st.DeviceListPos = acc.NextPos()
				}
			default:
				failures[keyID] = map[string]any{
					"errcode": "M_NOT_FOUND",
					"error":   "No key to attach the signature to",
				}
			}
		}
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{"failures": failures})
}

// mergeSignatures copies the signatures block of incoming onto stored, keeping
// everything else from stored. Either side may be empty.
func mergeSignatures(stored, incoming json.RawMessage) json.RawMessage {
	var base map[string]any
	if len(stored) > 0 {
		if err := json.Unmarshal(stored, &base); err != nil {
			base = nil
		}
	}
	var add map[string]any
	if err := json.Unmarshal(incoming, &add); err != nil {
		return stored
	}
	if base == nil {
		base = map[string]any{}
		for k, v := range add {
			base[k] = v
		}
		return mustJSON(base)
	}

	// When the stored object has no signatures at all, adopt the incoming
	// object wholesale: it is the same key material plus signatures.
	if _, ok := base["signatures"]; !ok {
		for k, v := range add {
			base[k] = v
		}
		return mustJSON(base)
	}

	baseSigs, _ := base["signatures"].(map[string]any)
	addSigs, _ := add["signatures"].(map[string]any)
	if baseSigs == nil {
		baseSigs = map[string]any{}
	}
	for signer, sigs := range addSigs {
		existing, _ := baseSigs[signer].(map[string]any)
		if existing == nil {
			existing = map[string]any{}
		}
		incomingSigs, _ := sigs.(map[string]any)
		for keyID, sig := range incomingSigs {
			existing[keyID] = sig
		}
		baseSigs[signer] = existing
	}
	base["signatures"] = baseSigs
	return mustJSON(base)
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// --- key backup -------------------------------------------------------------

// roomKeysCreate stores a new key backup version.
//
// Resetting an identity enables backup as part of the same flow, so answering
// 404 here would make "I can't confirm" fail after it had already replaced the
// user's cross-signing keys - leaving them worse off than before.
func (s *Server) roomKeysCreate(c *Ctx) {
	var body struct {
		Algorithm json.RawMessage `json:"algorithm"`
		AuthData  json.RawMessage `json:"auth_data"`
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

	acc := c.Account
	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	st.BackupVersion = "1"
	st.BackupAlgorithm = body.Algorithm
	st.BackupAuthData = body.AuthData
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{"version": st.BackupVersion})
}

// roomKeysGetVersion returns the current backup metadata.
func (s *Server) roomKeysGetVersion(c *Ctx) {
	acc := c.Account
	s.Store.Mu.RLock()
	st := acc.E2EE
	var version string
	var algorithm, authData json.RawMessage
	if st != nil {
		version = st.BackupVersion
		algorithm = st.BackupAlgorithm
		authData = st.BackupAuthData
	}
	s.Store.Mu.RUnlock()

	want := c.Param("version")
	if version == "" || (want != "" && want != version) {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "No backup version")
		return
	}
	resp := map[string]any{
		"version":   version,
		"count":     0,
		"etag":      version,
		"algorithm": json.RawMessage(algorithm),
		"auth_data": json.RawMessage(authData),
	}
	writeJSON(c.W, http.StatusOK, resp)
}

// roomKeysUpdateVersion replaces the auth data of a backup version.
func (s *Server) roomKeysUpdateVersion(c *Ctx) {
	var body struct {
		AuthData json.RawMessage `json:"auth_data"`
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

	acc := c.Account
	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	if st.BackupVersion == "" || st.BackupVersion != c.Param("version") {
		s.Store.Mu.Unlock()
		writeError(c.W, http.StatusNotFound, ErrNotFound, "No backup version")
		return
	}
	if len(body.AuthData) > 0 {
		st.BackupAuthData = body.AuthData
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// roomKeysPut stores the opaque key-backup payload.
func (s *Server) roomKeysPut(c *Ctx) {
	raw, ok := readBody(c, e2eeBodyLimit)
	if !ok {
		return
	}
	raw = trimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Key backup must be a JSON object")
		return
	}

	acc := c.Account
	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	st.RoomKeys = append(json.RawMessage(nil), raw...)
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{
		"count": 0,
		"etag":  st.BackupVersion,
	})
}

// roomKeysGet returns the stored key-backup payload.
func (s *Server) roomKeysGet(c *Ctx) {
	acc := c.Account
	s.Store.Mu.RLock()
	st := acc.E2EE
	var keys json.RawMessage
	if st != nil {
		keys = st.RoomKeys
	}
	s.Store.Mu.RUnlock()

	if len(keys) == 0 {
		writeJSON(c.W, http.StatusOK, map[string]any{"rooms": map[string]any{}})
		return
	}
	c.W.Header().Set("Content-Type", "application/json")
	c.W.WriteHeader(http.StatusOK)
	_, _ = c.W.Write(keys)
}

// --- account data -----------------------------------------------------------

// accountDataKey namespaces room-scoped account data away from global data.
func accountDataKey(c *Ctx) string {
	typ := c.Param("type")
	if roomID := c.Param("roomId"); roomID != "" {
		return "room\x00" + roomID + "\x00" + typ
	}
	return typ
}

// getAccountData returns a stored account data event.
func (s *Server) getAccountData(c *Ctx) {
	key := accountDataKey(c)
	acc := c.Account

	s.Store.Mu.RLock()
	var content json.RawMessage
	if acc.E2EE != nil {
		if e := acc.E2EE.AccountData[key]; e != nil {
			content = e.Content
		}
	}
	s.Store.Mu.RUnlock()

	if len(content) == 0 {
		writeError(c.W, http.StatusNotFound, ErrNotFound, "Account data not found")
		return
	}
	c.W.Header().Set("Content-Type", "application/json")
	c.W.WriteHeader(http.StatusOK)
	_, _ = c.W.Write(content)
}

// putAccountData stores an account data event.
//
// Secret storage lives here: m.secret_storage.default_key, the encrypted
// m.cross_signing.* blobs and the recovery key material. Discarding the write
// (as the old stub did) meant the client could never reload its own recovery
// state after a restart, so it re-entered the identity flow every launch.
func (s *Server) putAccountData(c *Ctx) {
	key := accountDataKey(c)
	body, ok := readBody(c, 1<<20)
	if !ok {
		return
	}
	body = trimSpace(body)
	if len(body) == 0 || body[0] != '{' {
		writeError(c.W, http.StatusBadRequest, ErrNotJSON, "Account data must be a JSON object")
		return
	}

	acc := c.Account
	s.Store.Mu.Lock()
	st := acc.EnsureE2EE()
	// Rewriting identical content must be a no-op: clients re-PUT account data
	// on every launch, and advancing the stream each time would make /sync
	// answer with "new" data forever.
	if prev := st.AccountData[key]; prev == nil || !bytes.Equal(prev.Content, body) {
		pos := acc.NextPos()
		st.AccountData[key] = &model.AccountDataEntry{Type: key, Content: body, Pos: pos}
	}
	s.Store.Mu.Unlock()
	s.Store.MarkDirty(acc.YunhuID)

	writeJSON(c.W, http.StatusOK, map[string]any{})
}

// accountDataEvents renders stored account data newer than since.
// The caller must hold the store lock.
func accountDataEvents(acc *model.Account, since int64, initial bool) []map[string]any {
	out := []map[string]any{}
	if acc.E2EE == nil {
		return out
	}
	keys := make([]string, 0, len(acc.E2EE.AccountData))
	for key := range acc.E2EE.AccountData {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		e := acc.E2EE.AccountData[key]
		if e == nil || strings.HasPrefix(key, "room\x00") {
			continue
		}
		if !initial && e.Pos <= since {
			continue
		}
		out = append(out, map[string]any{
			"type":    e.Type,
			"content": json.RawMessage(e.Content),
		})
	}
	return out
}

// deviceListChanged reports whether the caller's device list changed since
// the given stream position. The caller must hold the store lock.
func deviceListChanged(acc *model.Account, since int64) []string {
	if acc.E2EE == nil || acc.E2EE.DeviceListPos <= since {
		return []string{}
	}
	return []string{acc.UserID}
}
