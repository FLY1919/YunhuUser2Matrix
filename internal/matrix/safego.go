package matrix

import "runtime/debug"

// safego runs fn in a new goroutine with a panic guard.
//
// A panic in a background goroutine is fatal to the entire process - Go cannot
// recover it from anywhere else. From the client's side that looks like "cannot
// connect to the server" followed by endless reconnects, because the homeserver
// simply disappears. Every long-lived worker therefore goes through here, the
// same way serveRecovering guards the request path.
func (s *Server) safego(task string, fn func()) {
	go func() {
		s.guard(task, fn)
	}()
}

// guard runs fn synchronously with a panic guard. Use it on goroutines the
// server does not own - the websocket reader in particular: a panic there would
// take the process down and, with it, every client's sync loop.
func (s *Server) guard(task string, fn func()) {
	defer func() {
		if v := recover(); v != nil {
			s.Log.Error("task panic", "task", task, "panic", v, "stack", string(debug.Stack()))
		}
	}()
	fn()
}
