// Package config loads and validates the yhmatrix runtime configuration.
//
// Configuration is read from a YAML file next to the executable (config.yaml),
// overridable with -config or YHMATRIX_CONFIG, and may be overridden by
// environment variables prefixed with YHMATRIX_.
package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration object.
type Config struct {
	Server  Server  `yaml:"server"`
	Yunhu   Yunhu   `yaml:"yunhu"`
	Storage Storage `yaml:"storage"`
	Log     Log     `yaml:"log"`

	// SourcePath is the config file that was actually loaded. Empty means the
	// built-in defaults plus environment variables were used. Not part of the
	// YAML document.
	SourcePath string `yaml:"-"`
	// BaseDir is the directory relative paths are resolved against: the one
	// holding the executable. Not part of the YAML document.
	BaseDir string `yaml:"-"`
}

// Server holds settings for the Matrix side of the homeserver.
type Server struct {
	// Name is the Matrix server name (the part after the colon in user IDs).
	// It must match the hostname clients use to reach the server.
	Name string `yaml:"name"`
	// Bind is the listen address, e.g. ":8008".
	Bind string `yaml:"bind"`
	// PublicURL is the externally reachable base URL, used to build media links
	// that must be handed to Yunhu when no Qiniu upload is possible.
	PublicURL string `yaml:"public_url"`
	// MaxUploadSize limits media uploads, in bytes.
	MaxUploadSize int64 `yaml:"max_upload_size"`
	// SyncTimeoutMS is the default long-poll window for /sync.
	SyncTimeoutMS int `yaml:"sync_timeout_ms"`
	// AllowRegister enables the (disabled by default) /register endpoint.
	AllowRegister bool `yaml:"allow_register"`
	// AllowRelogin lets an already-known Matrix user ID log in again without
	// upstream credentials. The Yunhu token and all room data are already on
	// disk, so asking for the phone/SMS flow again just to open a second device
	// is pointless — and with a numeric Yunhu ID the identifier would otherwise
	// be mistaken for a phone number.
	//
	// On by default because this is a single-user bridge meant for a LAN. Set it
	// to false before exposing the homeserver to the internet: with it enabled,
	// anyone who can reach the port can obtain an access token for any account
	// that already exists here. When disabled, the password must equal the
	// account's stored Yunhu token.
	AllowRelogin bool `yaml:"allow_relogin"`
	// AllowCORS emits permissive CORS headers for web clients.
	AllowCORS bool `yaml:"allow_cors"`
	// WebDir optionally serves a static Element Web build from this directory.
	// Serving the client from the same origin avoids the browser's mixed-content
	// and private-network restrictions that block https://app.element.io from
	// talking to a plain http:// LAN homeserver.
	WebDir string `yaml:"web_dir"`
	// TLSCert / TLSKey enable native HTTPS when both are set.
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
	// Support is published at /.well-known/matrix/support so clients can show
	// "who do I ask for help" (Matrix v1.10, MSC1929).
	Support Support `yaml:"support"`
}

// Support is the /.well-known/matrix/support document.
//
// At least one of SupportPage / Contacts must be published; otherwise the
// endpoint answers 404, which is the spec's way of saying "this server
// publishes no support information".
type Support struct {
	// SupportPage is a URL with help specific to this homeserver.
	SupportPage string `yaml:"support_page"`
	// Contacts are the ways to reach the administrator. Leaving this key out
	// entirely publishes a derived admin contact (the first local account);
	// setting it to an empty list (contacts: []) disables that and publishes
	// nothing but support_page.
	Contacts []SupportContact `yaml:"contacts"`
}

// SupportContact is one entry of the support document's contacts array.
type SupportContact struct {
	// Role is what the contact is for: m.role.admin, m.role.security, or a
	// namespaced value of your own. Defaults to m.role.admin.
	Role string `yaml:"role"`
	// MatrixID is a Matrix user ID. May be on another homeserver, so the admin
	// stays reachable when this one is down.
	MatrixID string `yaml:"matrix_id"`
	// EmailAddress is an email address for the same purpose.
	EmailAddress string `yaml:"email_address"`
}

// Yunhu holds settings for the upstream Yunhu (chat-go.jwzhd.com) API.
type Yunhu struct {
	// BaseURL is the REST base, e.g. https://chat-go.jwzhd.com
	BaseURL string `yaml:"base_url"`
	// WSURL is the realtime endpoint, e.g. wss://chat-ws-go.jwzhd.com/ws
	WSURL string `yaml:"ws_url"`
	// Platform is reported at login. Must be one of the values Yunhu accepts:
	// windows, macos, android, linux, ios, fuchsia, Web (case sensitive).
	Platform string `yaml:"platform"`
	// DevicePrefix prefixes generated device identifiers.
	DevicePrefix string `yaml:"device_prefix"`
	// MediaBase overrides the CDN prefix used for bare Qiniu object keys.
	// Leave it empty (the default) to route each key to the host Yunhu's API
	// reference assigns to it — stickers and images on chat-img.jwznb.com,
	// audio on chat-audio1, video on chat-video1, documents on chat-file.
	// Setting it forces every bare key through one base, which is only correct
	// for the in-process mock upstream.
	MediaBase string `yaml:"media_base"`
	// UploadHost overrides the Qiniu upload endpoint used when a Matrix client
	// sends an image/file and Yunhu requires a Qiniu object key.
	//
	// Leave it empty (the default): Yunhu's buckets live in different Qiniu
	// regions, and the upload domain is discovered per bucket via
	// https://api.qiniu.com/v4/query. Pointing it at a fixed host is only
	// correct for the in-process mock upstream.
	UploadHost string `yaml:"upload_host"`
	// Wire selects the request encoding: proto, json or auto (default).
	Wire string `yaml:"wire"`
	// TimeoutMS is the per-request HTTP timeout.
	TimeoutMS int `yaml:"timeout_ms"`
	// Mock starts an in-process fake Yunhu API (for development only).
	Mock bool `yaml:"mock"`
	// Backfill is how many historical messages to import per room on first sync.
	Backfill int `yaml:"backfill"`
	// ReconnectMinMS is the initial websocket reconnect delay.
	ReconnectMinMS int `yaml:"reconnect_min_ms"`
	// ReconnectMaxMS caps the websocket reconnect delay.
	ReconnectMaxMS int `yaml:"reconnect_max_ms"`
	// HeartbeatMS is the websocket heartbeat interval.
	HeartbeatMS int `yaml:"heartbeat_ms"`
}

// Storage holds filesystem paths.
type Storage struct {
	// DataDir holds accounts, media and caches.
	DataDir string `yaml:"data_dir"`
	// FlushIntervalMS controls how often dirty accounts are persisted.
	FlushIntervalMS int `yaml:"flush_interval_ms"`
	// MaxEventsPerRoom caps the in-memory timeline per room.
	MaxEventsPerRoom int `yaml:"max_events_per_room"`
}

// Log holds logging settings.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns a configuration with sane defaults.
func Default() *Config {
	return &Config{
		Server: Server{
			Name:          "localhost",
			Bind:          ":8008",
			MaxUploadSize: 100 << 20,
			SyncTimeoutMS: 25000,
			AllowCORS:     true,
			AllowRelogin:  true,
		},
		Yunhu: Yunhu{
			BaseURL:        "https://chat-go.jwzhd.com",
			WSURL:          "wss://chat-ws-go.jwzhd.com/ws",
			Platform:       "windows",
			DevicePrefix:   "yhmatrix",
			MediaBase:      "",
			UploadHost:     "",
			Wire:           "auto",
			TimeoutMS:      30000,
			Backfill:       15,
			ReconnectMinMS: 1000,
			ReconnectMaxMS: 60000,
			HeartbeatMS:    25000,
		},
		Storage: Storage{
			DataDir:          "./data",
			FlushIntervalMS:  1500,
			MaxEventsPerRoom: 1000,
		},
		Log: Log{Level: "info", Format: "text"},
	}
}

// Load reads configuration and applies environment overrides.
//
// Resolution order:
//  1. the path passed in (-config);
//  2. $YHMATRIX_CONFIG;
//  3. config.yaml next to the executable;
//  4. config.yaml in the working directory (handy for `go run`).
//
// A relative path is tried against the executable's directory first and then
// against the working directory, so `-config config.yaml` behaves the same no
// matter where the service was started from.
//
// Previously an empty path read nothing at all, so a config.yaml sitting next
// to the binary was silently ignored and the server ran on defaults.
func Load(path string) (*Config, error) {
	cfg := Default()
	cfg.BaseDir = executableDir()

	resolved, err := resolveConfigPath(path, cfg.BaseDir)
	if err != nil {
		return nil, err
	}
	if resolved != "" {
		data, err := os.ReadFile(resolved)
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", resolved, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", resolved, err)
		}
		cfg.SourcePath = resolved
	}

	applyEnv(cfg)

	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// resolveConfigPath returns the config file to read, or "" when the built-in
// defaults should be used. An explicitly named file that does not exist is an
// error: silently ignoring it is what makes "why is my config not applied" so
// hard to diagnose.
func resolveConfigPath(explicit, baseDir string) (string, error) {
	if explicit == "" {
		explicit = os.Getenv("YHMATRIX_CONFIG")
	}
	if explicit == "" {
		for _, dir := range searchDirs(baseDir) {
			candidate := filepath.Join(dir, "config.yaml")
			if isFile(candidate) {
				return candidate, nil
			}
		}
		return "", nil
	}

	if filepath.IsAbs(explicit) {
		if isFile(explicit) {
			return explicit, nil
		}
		return "", fmt.Errorf("config file %s does not exist", explicit)
	}
	for _, dir := range searchDirs(baseDir) {
		candidate := filepath.Join(dir, explicit)
		if isFile(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("config file %s does not exist (looked in %s and the working directory)",
		explicit, baseDir)
}

// searchDirs is the executable directory followed by the working directory,
// without duplicates or empty entries.
func searchDirs(baseDir string) []string {
	dirs := make([]string, 0, 2)
	seen := map[string]struct{}{}
	add := func(dir string) {
		if dir == "" {
			return
		}
		if abs, err := filepath.Abs(dir); err == nil {
			dir = abs
		}
		if _, ok := seen[dir]; ok {
			return
		}
		seen[dir] = struct{}{}
		dirs = append(dirs, dir)
	}
	add(baseDir)
	if wd, err := os.Getwd(); err == nil {
		add(wd)
	}
	return dirs
}

// executableDir returns the directory holding the running binary, which is the
// root the service should be configured from. It falls back to the working
// directory when the executable path cannot be determined (for example under
// `go run`).
func executableDir() string {
	exe, err := os.Executable()
	if err != nil {
		if wd, wdErr := os.Getwd(); wdErr == nil {
			return wd
		}
		return "."
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return filepath.Dir(exe)
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// resolvePath makes a relative path absolute against the base directory, so
// "./data" always means "data next to the binary" rather than "wherever the
// shell happened to be".
func (c *Config) resolvePath(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	base := c.BaseDir
	if base == "" {
		if wd, err := os.Getwd(); err == nil {
			base = wd
		}
	}
	return filepath.Join(base, p)
}

func applyEnv(c *Config) {
	str := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			*dst = v
		}
	}
	num := func(key string, dst *int) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	boolean := func(key string, dst *bool) {
		if v, ok := os.LookupEnv(key); ok && v != "" {
			if b, err := strconv.ParseBool(v); err == nil {
				*dst = b
			}
		}
	}

	str("YHMATRIX_SERVER_NAME", &c.Server.Name)
	str("YHMATRIX_SERVER_BIND", &c.Server.Bind)
	str("YHMATRIX_SERVER_PUBLIC_URL", &c.Server.PublicURL)
	str("YHMATRIX_SERVER_WEB_DIR", &c.Server.WebDir)
	str("YHMATRIX_SERVER_TLS_CERT", &c.Server.TLSCert)
	str("YHMATRIX_SERVER_TLS_KEY", &c.Server.TLSKey)
	str("YHMATRIX_SERVER_SUPPORT_PAGE", &c.Server.Support.SupportPage)
	num("YHMATRIX_SERVER_SYNC_TIMEOUT_MS", &c.Server.SyncTimeoutMS)
	boolean("YHMATRIX_SERVER_ALLOW_CORS", &c.Server.AllowCORS)

	str("YHMATRIX_YUNHU_BASE_URL", &c.Yunhu.BaseURL)
	str("YHMATRIX_YUNHU_WS_URL", &c.Yunhu.WSURL)
	str("YHMATRIX_YUNHU_PLATFORM", &c.Yunhu.Platform)
	str("YHMATRIX_YUNHU_MEDIA_BASE", &c.Yunhu.MediaBase)
	str("YHMATRIX_YUNHU_UPLOAD_HOST", &c.Yunhu.UploadHost)
	str("YHMATRIX_YUNHU_WIRE", &c.Yunhu.Wire)
	boolean("YHMATRIX_YUNHU_MOCK", &c.Yunhu.Mock)
	num("YHMATRIX_YUNHU_BACKFILL", &c.Yunhu.Backfill)

	str("YHMATRIX_DATA_DIR", &c.Storage.DataDir)
	str("YHMATRIX_LOG_LEVEL", &c.Log.Level)
}

// Validate re-checks a configuration after programmatic overrides.
func Validate(c *Config) error { return c.normalize() }

func (c *Config) normalize() error {
	c.Server.Name = strings.TrimSpace(strings.ToLower(c.Server.Name))
	if c.Server.Name == "" {
		return fmt.Errorf("server.name must not be empty")
	}
	if strings.ContainsAny(c.Server.Name, " /:@") {
		return fmt.Errorf("server.name %q must be a bare hostname without scheme, port or path", c.Server.Name)
	}
	if c.Server.Bind == "" {
		c.Server.Bind = ":8008"
	}
	if c.Server.SyncTimeoutMS <= 0 {
		c.Server.SyncTimeoutMS = 25000
	}
	if c.Server.SyncTimeoutMS > 60000 {
		c.Server.SyncTimeoutMS = 60000
	}
	if c.Server.MaxUploadSize <= 0 {
		c.Server.MaxUploadSize = 100 << 20
	}
	if (c.Server.TLSCert == "") != (c.Server.TLSKey == "") {
		return fmt.Errorf("server.tls_cert and server.tls_key must be set together")
	}
	c.Server.PublicURL = strings.TrimRight(c.Server.PublicURL, "/")
	if c.Server.PublicURL != "" {
		if _, err := url.Parse(c.Server.PublicURL); err != nil {
			return fmt.Errorf("server.public_url: %w", err)
		}
	}

	c.Yunhu.BaseURL = strings.TrimRight(c.Yunhu.BaseURL, "/")
	if c.Yunhu.BaseURL == "" {
		c.Yunhu.BaseURL = "https://chat-go.jwzhd.com"
	}
	if c.Yunhu.WSURL == "" {
		c.Yunhu.WSURL = "wss://chat-ws-go.jwzhd.com/ws"
	}
	switch c.Yunhu.Platform {
	case "windows", "macos", "android", "linux", "ios", "fuchsia", "Web":
	default:
		return fmt.Errorf("yunhu.platform %q is not accepted by Yunhu; use windows, macos, android, linux, ios, fuchsia or Web", c.Yunhu.Platform)
	}
	switch strings.ToLower(c.Yunhu.Wire) {
	case "", "auto":
		c.Yunhu.Wire = "auto"
	case "proto", "json":
		c.Yunhu.Wire = strings.ToLower(c.Yunhu.Wire)
	default:
		return fmt.Errorf("yunhu.wire must be proto, json or auto")
	}
	if c.Yunhu.TimeoutMS <= 0 {
		c.Yunhu.TimeoutMS = 30000
	}
	if c.Yunhu.Backfill < 0 {
		c.Yunhu.Backfill = 0
	}
	if c.Yunhu.Backfill > 100 {
		c.Yunhu.Backfill = 100
	}
	if c.Yunhu.ReconnectMinMS <= 0 {
		c.Yunhu.ReconnectMinMS = 1000
	}
	if c.Yunhu.ReconnectMaxMS < c.Yunhu.ReconnectMinMS {
		c.Yunhu.ReconnectMaxMS = 60000
	}
	if c.Yunhu.HeartbeatMS <= 0 {
		c.Yunhu.HeartbeatMS = 25000
	}
	if c.Yunhu.DevicePrefix == "" {
		c.Yunhu.DevicePrefix = "yhmatrix"
	}
	if !strings.HasSuffix(c.Yunhu.MediaBase, "/") && c.Yunhu.MediaBase != "" {
		c.Yunhu.MediaBase += "/"
	}

	if c.Storage.DataDir == "" {
		c.Storage.DataDir = "./data"
	}
	if c.Storage.FlushIntervalMS <= 0 {
		c.Storage.FlushIntervalMS = 1500
	}
	if c.Storage.MaxEventsPerRoom < 100 {
		c.Storage.MaxEventsPerRoom = 1000
	}
	c.ResolvePaths()

	// Checked after ResolvePaths so a relative web_dir is judged against the
	// binary's directory rather than the shell's working directory.
	if c.Server.WebDir != "" {
		if st, err := os.Stat(c.Server.WebDir); err != nil || !st.IsDir() {
			return fmt.Errorf("server.web_dir %q is not a directory", c.Server.WebDir)
		}
	}
	return nil
}

// ResolvePaths makes relative filesystem settings absolute against BaseDir, so
// a relative path always means "next to the binary". It is idempotent, which
// lets it run both during Load and again after flag overrides.
func (c *Config) ResolvePaths() {
	c.Storage.DataDir = c.resolvePath(c.Storage.DataDir)
	c.Server.WebDir = c.resolvePath(c.Server.WebDir)
	c.Server.TLSCert = c.resolvePath(c.Server.TLSCert)
	c.Server.TLSKey = c.resolvePath(c.Server.TLSKey)
}

// Timeout returns the upstream HTTP timeout.
func (c *Config) Timeout() time.Duration {
	return time.Duration(c.Yunhu.TimeoutMS) * time.Millisecond
}

// SyncTimeout returns the long-poll window for /sync.
func (c *Config) SyncTimeout() time.Duration {
	return time.Duration(c.Server.SyncTimeoutMS) * time.Millisecond
}
