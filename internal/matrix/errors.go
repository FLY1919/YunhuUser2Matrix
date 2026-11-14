package matrix

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Matrix standard error codes used by this homeserver.
const (
	ErrForbidden      = "M_FORBIDDEN"
	ErrUnknownToken   = "M_UNKNOWN_TOKEN"
	ErrMissingToken   = "M_MISSING_TOKEN"
	ErrBadJSON        = "M_BAD_JSON"
	ErrNotJSON        = "M_NOT_JSON"
	ErrNotFound       = "M_NOT_FOUND"
	ErrUnrecognized   = "M_UNRECOGNIZED"
	ErrUserInUse      = "M_USER_IN_USE"
	ErrInvalidParam   = "M_INVALID_PARAM"
	ErrTooLarge       = "M_TOO_LARGE"
	ErrUnknown        = "M_UNKNOWN"
	ErrUnauthorized   = "M_UNAUTHORIZED"
	ErrLimitExceeded  = "M_LIMIT_EXCEEDED"
	ErrGuestForbidden = "M_GUEST_ACCESS_FORBIDDEN"
)

type matrixError struct {
	ErrCode string `json:"errcode"`
	Error   string `json:"error"`
	// RetryAfterMS is only used for rate limiting.
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

// writeJSON serialises v with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		_, _ = w.Write([]byte("{}"))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		slog.Default().Warn("encode response failed", "err", err)
	}
}

// writeError emits a Matrix error object.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, matrixError{ErrCode: code, Error: msg})
}

// writeUnknown reports an unimplemented endpoint in the shape clients expect.
func writeUnknown(w http.ResponseWriter) {
	writeError(w, http.StatusNotFound, ErrUnrecognized, "Unrecognized request")
}
