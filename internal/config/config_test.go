package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chdir moves the process into dir for the duration of the test. Tests that
// use it must not call t.Parallel.
func chdir(t *testing.T, dir string) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(old); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})
}

func writeConfig(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// The config next to the binary must win over the working directory, otherwise
// running the service from a different directory silently changes its
// behaviour.
func TestResolveConfigPathPrefersExecutableDir(t *testing.T) {
	base := t.TempDir()
	want := writeConfig(t, base, "server:\n  name: from-exe\n")

	wd := t.TempDir()
	writeConfig(t, wd, "server:\n  name: from-wd\n")
	chdir(t, wd)

	got, err := resolveConfigPath("", base)
	if err != nil {
		t.Fatalf("resolveConfigPath: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestResolveConfigPathFallsBackToWorkingDir(t *testing.T) {
	base := t.TempDir() // empty: no config next to the binary
	wd := t.TempDir()
	want := writeConfig(t, wd, "server:\n  name: from-wd\n")
	chdir(t, wd)

	got, err := resolveConfigPath("", base)
	if err != nil {
		t.Fatalf("resolveConfigPath: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestResolveConfigPathNoneFound(t *testing.T) {
	base := t.TempDir()
	chdir(t, t.TempDir())

	got, err := resolveConfigPath("", base)
	if err != nil {
		t.Fatalf("resolveConfigPath: %v", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty (built-in defaults)", got)
	}
}

// A relative -config must be tried against the binary's directory first, so
// `-config config.yaml` works from any shell directory.
func TestResolveConfigPathRelativeAgainstBaseDir(t *testing.T) {
	base := t.TempDir()
	want := writeConfig(t, base, "server:\n  name: from-exe\n")
	chdir(t, t.TempDir())

	got, err := resolveConfigPath("config.yaml", base)
	if err != nil {
		t.Fatalf("resolveConfigPath: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestResolveConfigPathAbsolute(t *testing.T) {
	base := t.TempDir()
	want := writeConfig(t, t.TempDir(), "server:\n  name: elsewhere\n")
	chdir(t, base)

	got, err := resolveConfigPath(want, base)
	if err != nil {
		t.Fatalf("resolveConfigPath: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Naming a file that does not exist must be an error. Silently ignoring it is
// exactly what makes "my config is not applied" impossible to debug.
func TestResolveConfigPathMissingExplicitIsError(t *testing.T) {
	base := t.TempDir()
	chdir(t, base)

	if _, err := resolveConfigPath("nope.yaml", base); err == nil {
		t.Fatal("expected an error for a missing relative config file")
	}
	if _, err := resolveConfigPath(filepath.Join(base, "nope.yaml"), base); err == nil {
		t.Fatal("expected an error for a missing absolute config file")
	}
}

func TestLoadReadsExplicitConfig(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, strings.Join([]string{
		"server:",
		"  name: yunhu.example",
		"  bind: \":9009\"",
		"storage:",
		"  data_dir: ./state",
		"",
	}, "\n"))
	t.Setenv("YHMATRIX_CONFIG", path)
	// Neutralise any ambient overrides that would mask the file.
	t.Setenv("YHMATRIX_SERVER_NAME", "")
	t.Setenv("YHMATRIX_SERVER_BIND", "")
	t.Setenv("YHMATRIX_DATA_DIR", "")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SourcePath != path {
		t.Fatalf("SourcePath = %q, want %q", cfg.SourcePath, path)
	}
	if cfg.Server.Name != "yunhu.example" || cfg.Server.Bind != ":9009" {
		t.Fatalf("file not applied: name=%q bind=%q", cfg.Server.Name, cfg.Server.Bind)
	}
	// ./state is relative to the binary, not to the shell's cwd.
	wantDir := filepath.Join(cfg.BaseDir, "state")
	if cfg.Storage.DataDir != wantDir {
		t.Fatalf("DataDir = %q, want %q", cfg.Storage.DataDir, wantDir)
	}
}

func TestLoadMissingConfigIsError(t *testing.T) {
	t.Setenv("YHMATRIX_CONFIG", filepath.Join(t.TempDir(), "absent.yaml"))
	if _, err := Load(""); err == nil {
		t.Fatal("expected an error for a missing YHMATRIX_CONFIG file")
	}
}

func TestResolvePathsIsIdempotent(t *testing.T) {
	base := t.TempDir()
	c := Default()
	c.BaseDir = base
	c.Storage.DataDir = "./data"
	c.Server.WebDir = "web"
	c.Server.TLSCert = "cert.pem"
	absKey := filepath.Join(t.TempDir(), "key.pem")
	c.Server.TLSKey = absKey

	c.ResolvePaths()
	first := []string{c.Storage.DataDir, c.Server.WebDir, c.Server.TLSCert, c.Server.TLSKey}
	c.ResolvePaths()
	second := []string{c.Storage.DataDir, c.Server.WebDir, c.Server.TLSCert, c.Server.TLSKey}

	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("field %d changed on second call: %q -> %q", i, first[i], second[i])
		}
	}
	if want := filepath.Join(base, "data"); first[0] != want {
		t.Fatalf("DataDir = %q, want %q", first[0], want)
	}
	if first[3] != absKey {
		t.Fatalf("absolute path was rewritten: %q", first[3])
	}
}

// A relative web_dir must be validated against the binary's directory too, not
// against wherever the process was started.
func TestValidateResolvesWebDirBeforeChecking(t *testing.T) {
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "web"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	chdir(t, t.TempDir())

	c := Default()
	c.BaseDir = base
	c.Server.WebDir = "web"
	if err := Validate(c); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if want := filepath.Join(base, "web"); c.Server.WebDir != want {
		t.Fatalf("WebDir = %q, want %q", c.Server.WebDir, want)
	}

	c.Server.WebDir = "missing"
	if err := Validate(c); err == nil {
		t.Fatal("expected an error for a web_dir that does not exist")
	}
}
