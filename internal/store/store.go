// Package store persists accounts to disk as one JSON document per account.
//
// The store is intentionally simple: a homeserver that fronts a single upstream
// chat service does not need a relational schema, and keeping the state in
// memory makes /sync assembly trivial. Writes are debounced by a background
// flusher so that a busy timeline does not turn into a write storm.
package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yunhu/yhmatrix/internal/model"
)

// State is the in-memory index.
type State struct {
	Accounts map[string]*model.Account // keyed by Yunhu user id
	// ByUser maps Matrix user ID to Yunhu user ID.
	ByUser map[string]string
	// ByToken maps access token to (yunhu user id, device id).
	ByToken map[string]TokenRef
}

// TokenRef points at a device.
type TokenRef struct {
	YunhuID  string
	DeviceID string
}

// Store owns the state and its persistence.
type Store struct {
	Mu    sync.RWMutex
	State *State

	dir       string
	flush     time.Duration
	maxEvents int

	dirtyMu sync.Mutex
	dirty   map[string]struct{}

	stop chan struct{}
	done chan struct{}
	log  *slog.Logger
}

// Open loads all accounts from dir.
func Open(dir string, flush time.Duration, maxEvents int, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(filepath.Join(dir, "accounts"), 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	s := &Store{
		State: &State{
			Accounts: map[string]*model.Account{},
			ByUser:   map[string]string{},
			ByToken:  map[string]TokenRef{},
		},
		dir:       dir,
		flush:     flush,
		maxEvents: maxEvents,
		dirty:     map[string]struct{}{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		log:       log,
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	go s.flusher()
	return s, nil
}

func (s *Store) load() error {
	entries, err := os.ReadDir(filepath.Join(s.dir, "accounts"))
	if err != nil {
		return fmt.Errorf("scan accounts: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.dir, "accounts", e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			s.log.Warn("skip unreadable account", "path", path, "err", err)
			continue
		}
		var acc model.Account
		if err := json.Unmarshal(raw, &acc); err != nil {
			s.log.Warn("skip corrupt account", "path", path, "err", err)
			continue
		}
		if acc.YunhuID == "" || acc.UserID == "" {
			continue
		}
		s.normalizeAccount(&acc)
		s.State.Accounts[acc.YunhuID] = &acc
		s.State.ByUser[acc.UserID] = acc.YunhuID
		for _, d := range acc.Devices {
			if d.Token != "" {
				s.State.ByToken[d.Token] = TokenRef{YunhuID: acc.YunhuID, DeviceID: d.ID}
			}
		}
	}
	s.log.Info("loaded accounts", "count", len(s.State.Accounts))
	return nil
}

func (s *Store) normalizeAccount(acc *model.Account) {
	if acc.Devices == nil {
		acc.Devices = map[string]*model.Device{}
	}
	if acc.Rooms == nil {
		acc.Rooms = map[string]*model.Room{}
	}
	if acc.Filters == nil {
		acc.Filters = map[string]*model.Filter{}
	}
	if acc.Txns == nil {
		acc.Txns = map[string]string{}
	}
	// Encryption material is loaded from disk as null for accounts created
	// before it existed; allocate the maps so the first upload can write.
	if acc.E2EE != nil {
		acc.EnsureE2EE()
	}
	for _, r := range acc.Rooms {
		if r.Members == nil {
			r.Members = map[string]*model.Member{}
		}
		if r.State == nil {
			r.State = map[string]*model.Event{}
		}
		if r.Typing == nil {
			r.Typing = map[string]int64{}
		}
		if r.Membership == "" {
			r.Membership = model.MembershipJoin
		}
	}
}

// Close flushes and stops the background writer.
func (s *Store) Close() error {
	close(s.stop)
	<-s.done
	return s.FlushAll()
}

// MarkDirty schedules an account for persistence.
func (s *Store) MarkDirty(yunhuID string) {
	s.dirtyMu.Lock()
	s.dirty[yunhuID] = struct{}{}
	s.dirtyMu.Unlock()
}

func (s *Store) flusher() {
	defer close(s.done)
	t := time.NewTicker(s.flush)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.flushDirty()
		}
	}
}

func (s *Store) flushDirty() {
	s.dirtyMu.Lock()
	ids := make([]string, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}
	s.dirty = map[string]struct{}{}
	s.dirtyMu.Unlock()

	for _, id := range ids {
		if err := s.SaveAccount(id); err != nil {
			s.log.Error("persist account failed", "yunhu_id", id, "err", err)
		}
	}
}

// FlushAll persists every account.
func (s *Store) FlushAll() error {
	s.Mu.RLock()
	ids := make([]string, 0, len(s.State.Accounts))
	for id := range s.State.Accounts {
		ids = append(ids, id)
	}
	s.Mu.RUnlock()
	var firstErr error
	for _, id := range ids {
		if err := s.SaveAccount(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SaveAccount writes a single account atomically.
func (s *Store) SaveAccount(yunhuID string) error {
	s.Mu.RLock()
	acc := s.State.Accounts[yunhuID]
	if acc == nil {
		s.Mu.RUnlock()
		return nil
	}
	raw, err := json.Marshal(acc)
	s.Mu.RUnlock()
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, "accounts", safeName(yunhuID)+".json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func safeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "empty"
	}
	return b.String()
}

// GetByToken resolves an access token. The caller must not hold the lock.
func (s *Store) GetByToken(token string) (*model.Account, *model.Device) {
	if token == "" {
		return nil, nil
	}
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	ref, ok := s.State.ByToken[token]
	if !ok {
		return nil, nil
	}
	acc := s.State.Accounts[ref.YunhuID]
	if acc == nil {
		return nil, nil
	}
	dev := acc.Devices[ref.DeviceID]
	if dev == nil {
		return nil, nil
	}
	return acc, dev
}

// GetByUserID resolves a Matrix user ID.
func (s *Store) GetByUserID(userID string) *model.Account {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	id, ok := s.State.ByUser[userID]
	if !ok {
		return nil
	}
	return s.State.Accounts[id]
}

// GetByYunhuID resolves a Yunhu user ID.
func (s *Store) GetByYunhuID(id string) *model.Account {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.State.Accounts[id]
}

// PutAccount inserts or replaces an account and refreshes the indexes.
func (s *Store) PutAccount(acc *model.Account) {
	s.Mu.Lock()
	s.normalizeAccount(acc)
	s.State.Accounts[acc.YunhuID] = acc
	s.State.ByUser[acc.UserID] = acc.YunhuID
	for _, d := range acc.Devices {
		if d.Token != "" {
			s.State.ByToken[d.Token] = TokenRef{YunhuID: acc.YunhuID, DeviceID: d.ID}
		}
	}
	s.Mu.Unlock()
	s.MarkDirty(acc.YunhuID)
}

// AddDevice registers a device and indexes its token.
func (s *Store) AddDevice(acc *model.Account, dev *model.Device) {
	s.Mu.Lock()
	if acc.Devices == nil {
		acc.Devices = map[string]*model.Device{}
	}
	// Re-login with the same device id rotates the token: drop the old index
	// entry so a stale access token cannot be replayed.
	if old, ok := acc.Devices[dev.ID]; ok && old.Token != "" && old.Token != dev.Token {
		delete(s.State.ByToken, old.Token)
	}
	acc.Devices[dev.ID] = dev
	if dev.Token != "" {
		s.State.ByToken[dev.Token] = TokenRef{YunhuID: acc.YunhuID, DeviceID: dev.ID}
	}
	s.Mu.Unlock()
	s.MarkDirty(acc.YunhuID)
}

// RemoveDevice drops a device and its token.
func (s *Store) RemoveDevice(acc *model.Account, deviceID string) {
	s.Mu.Lock()
	if dev, ok := acc.Devices[deviceID]; ok {
		delete(s.State.ByToken, dev.Token)
		delete(acc.Devices, deviceID)
	}
	s.Mu.Unlock()
	s.MarkDirty(acc.YunhuID)
}

// AllAccounts returns a snapshot of every account pointer.
func (s *Store) AllAccounts() []*model.Account {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	out := make([]*model.Account, 0, len(s.State.Accounts))
	for _, a := range s.State.Accounts {
		out = append(out, a)
	}
	return out
}
