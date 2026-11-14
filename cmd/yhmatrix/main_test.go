package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/model"
	"github.com/yunhu/yhmatrix/internal/store"
)

func openTestStore(t *testing.T) (*store.Store, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	st, err := store.Open(t.TempDir(), time.Hour, 1000, log)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, &buf
}

// Changing server.name after the fact renames every user, because user IDs embed
// it. The accounts on disk then belong to nobody, which is indistinguishable
// from "all my rooms disappeared".
func TestWarnServerNameMismatchWarnsOnStaleAccounts(t *testing.T) {
	st, buf := openTestStore(t)
	st.PutAccount(&model.Account{YunhuID: "8516939", UserID: "@yh_8516939:localhost"})

	buf.Reset()
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnServerNameMismatch(log, st, "matrix.example.com")

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected a warning, got %q", out)
	}
	if !strings.Contains(out, "@yh_8516939:localhost") {
		t.Fatalf("warning does not name the stale account: %q", out)
	}
	if !strings.Contains(out, "stale_accounts=1") {
		t.Fatalf("warning does not count the stale accounts: %q", out)
	}
}

func TestWarnServerNameMismatchSilentWhenNamesMatch(t *testing.T) {
	st, buf := openTestStore(t)
	st.PutAccount(&model.Account{YunhuID: "8516939", UserID: "@yh_8516939:matrix.example.com"})

	buf.Reset()
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnServerNameMismatch(log, st, "matrix.example.com")

	if out := buf.String(); out != "" {
		t.Fatalf("expected silence, got %q", out)
	}
}

func TestWarnServerNameMismatchIgnoresEmptyUserID(t *testing.T) {
	st, buf := openTestStore(t)
	st.PutAccount(&model.Account{YunhuID: "1"})

	buf.Reset()
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnServerNameMismatch(log, st, "localhost")

	if out := buf.String(); out != "" {
		t.Fatalf("expected silence, got %q", out)
	}
}

// The shipped example config sets public_url to https://matrix.example.com.
// Leaving it in place makes /.well-known/matrix/client advertise a host that
// does not exist, so every client fails with "cannot reach your account
// provider" while this server only logs successful well-known requests.
func TestWarnPublicURLFlagsPlaceholder(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Name = "localhost"
	cfg.Server.PublicURL = "https://matrix.example.com"

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnPublicURL(log, cfg)

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected a warning, got %q", out)
	}
	if !strings.Contains(out, "placeholder") {
		t.Fatalf("warning should call out the placeholder: %q", out)
	}
}

func TestWarnPublicURLSilentWhenUnset(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Name = "localhost"
	cfg.Server.PublicURL = ""

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnPublicURL(log, cfg)

	if out := buf.String(); out != "" {
		t.Fatalf("expected silence, got %q", out)
	}
}

func TestWarnPublicURLSilentWhenHostMatches(t *testing.T) {
	cfg := config.Default()
	cfg.Server.Name = "matrix.example.com"
	cfg.Server.PublicURL = "https://matrix.example.com"

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	warnPublicURL(log, cfg)

	if out := buf.String(); out != "" {
		t.Fatalf("expected silence, got %q", out)
	}
}
