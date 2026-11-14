// Package hub provides a tiny in-process fan-out used to wake up /sync
// long-polls when new events land for an account.
package hub

import "sync"

// Hub tracks one wake-up channel per account.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

// New returns an empty hub.
func New() *Hub {
	return &Hub{subs: map[string]map[chan struct{}]struct{}{}}
}

// Subscribe returns a channel that is closed on the next Notify for key, along
// with a cancel function that must always be called.
func (h *Hub) Subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	h.mu.Lock()
	if h.subs[key] == nil {
		h.subs[key] = map[chan struct{}]struct{}{}
	}
	h.subs[key][ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if set, ok := h.subs[key]; ok {
				delete(set, ch)
				if len(set) == 0 {
					delete(h.subs, key)
				}
			}
			h.mu.Unlock()
		})
	}
	return ch, cancel
}

// Notify wakes every waiter for key.
func (h *Hub) Notify(key string) {
	h.mu.Lock()
	set := h.subs[key]
	delete(h.subs, key)
	h.mu.Unlock()
	for ch := range set {
		close(ch)
	}
}
