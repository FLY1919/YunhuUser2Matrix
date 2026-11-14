// Command yhmatrix runs a Matrix homeserver backed by Yunhu (云湖).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yunhu/yhmatrix/internal/config"
	"github.com/yunhu/yhmatrix/internal/hub"
	"github.com/yunhu/yhmatrix/internal/matrix"
	"github.com/yunhu/yhmatrix/internal/store"
	"github.com/yunhu/yhmatrix/internal/webdist"
	"github.com/yunhu/yhmatrix/internal/yunhu"
)

var version = "dev"

func main() {
	var (
		cfgPath = flag.String("config", "", "path to config.yaml (default: config.yaml next to the executable)")
		bind    = flag.String("bind", "", "override server.bind")
		dataDir = flag.String("data", "", "override storage.data_dir")
		name    = flag.String("name", "", "override server.name (Matrix domain in user IDs)")
		webDir  = flag.String("web", "", "serve a static Element Web build from this directory")
		fetchW  = flag.String("fetch-web", "", "download an Element Web build into this directory and exit")
		force   = flag.Bool("force", false, "with -fetch-web, replace an existing build")
		webVer  = flag.String("web-version", "", "with -fetch-web, pin a version such as v1.12.35 (default: latest)")
		mirror  = flag.String("mirror", "", "with -fetch-web, download mirror: a ghproxy-style prefix or a github.com replacement base")
		tlsCert = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS)")
		tlsKey  = flag.String("tls-key", "", "TLS key file (enables HTTPS)")
		mock    = flag.Bool("mock", false, "run against the built-in fake Yunhu API")
		verbose = flag.Bool("v", false, "enable debug logging")
		showVer = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("yhmatrix", version)
		return
	}

	// Downloading a web client needs no config, so handle it before loading one.
	if *fetchW != "" {
		if err := fetchWebClient(*fetchW, *webVer, *force, *mirror); err != nil {
			fmt.Fprintln(os.Stderr, "fetch-web:", err)
			os.Exit(1)
		}
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(2)
	}
	if *bind != "" {
		cfg.Server.Bind = *bind
	}
	if *dataDir != "" {
		cfg.Storage.DataDir = *dataDir
	}
	if *name != "" {
		cfg.Server.Name = *name
	}
	if *webDir != "" {
		cfg.Server.WebDir = *webDir
	}
	if *tlsCert != "" {
		cfg.Server.TLSCert = *tlsCert
	}
	if *tlsKey != "" {
		cfg.Server.TLSKey = *tlsKey
	}
	if *mock {
		cfg.Yunhu.Mock = true
	}
	// Flags may have introduced relative paths; resolve them against the
	// executable's directory the same way the config file was.
	cfg.ResolvePaths()
	// Re-validate after flag overrides.
	if err := config.Validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(2)
	}

	level := slog.LevelInfo
	if *verbose || strings.EqualFold(cfg.Log.Level, "debug") {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	// Announce which config file was actually read. Running on defaults is the
	// usual cause of "my config.yaml is being ignored", so say it out loud
	// instead of failing silently.
	if cfg.SourcePath != "" {
		log.Info("config loaded", "path", cfg.SourcePath, "base_dir", cfg.BaseDir)
	} else {
		log.Warn("no config.yaml found, using built-in defaults",
			"base_dir", cfg.BaseDir,
			"hint", "put config.yaml next to the executable, or pass -config <path>")
	}
	log.Info("data directory", "path", cfg.Storage.DataDir)

	// A leftover server.public_url is invisible until a client logs in: it is
	// advertised as m.homeserver.base_url and every request then goes to a host
	// that does not exist.
	warnPublicURL(log, cfg)

	// Optional in-process fake upstream, used for development and CI.
	if cfg.Yunhu.Mock {
		url, wsURL, stop, err := startMock(log)
		if err != nil {
			log.Error("failed to start mock Yunhu API", "err", err)
			os.Exit(1)
		}
		defer stop()
		cfg.Yunhu.BaseURL = url
		cfg.Yunhu.WSURL = wsURL
		cfg.Yunhu.MediaBase = url + "/media/"
		cfg.Yunhu.UploadHost = url + "/upload"
		log.Warn("running against the MOCK Yunhu API - do not use in production",
			"base", url, "demo_account", "demo@yunhu.dev / demo1234")
	}

	st, err := store.Open(cfg.Storage.DataDir,
		time.Duration(cfg.Storage.FlushIntervalMS)*time.Millisecond,
		cfg.Storage.MaxEventsPerRoom, log)
	if err != nil {
		log.Error("failed to open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Now that the config file is really read, a stale example config can change
	// server.name, which silently renames every user: the accounts on disk are
	// keyed by the old domain and would come back empty. Say so loudly.
	warnServerNameMismatch(log, st, cfg.Server.Name)

	srv, err := matrix.NewServer(cfg, st, hub.New(), log)
	if err != nil {
		log.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Reconnect every known account in the background.
	srv.StartRunners(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.Server.Bind,
		Handler:           srv,
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.Server.Bind)
	if err != nil {
		log.Error("listen failed", "addr", cfg.Server.Bind, "err", err)
		os.Exit(1)
	}

	scheme := "http"
	if cfg.Server.TLSCert != "" {
		scheme = "https"
	}
	log.Info("yhmatrix listening",
		"addr", ln.Addr().String(),
		"url", scheme+"://"+displayHost(cfg, ln.Addr().String()),
		"server_name", cfg.Server.Name,
		"yunhu", cfg.Yunhu.BaseURL,
		"version", version)

	if cfg.Server.WebDir != "" {
		if _, statErr := os.Stat(filepath.Join(cfg.Server.WebDir, "index.html")); statErr != nil {
			log.Warn("web dir has no index.html, the client will not load",
				"dir", cfg.Server.WebDir,
				"hint", "yhmatrix -fetch-web "+cfg.Server.WebDir)
		} else {
			log.Info("serving bundled web client", "dir", cfg.Server.WebDir)
		}
	} else if scheme == "http" {
		log.Warn("plain HTTP homeserver: browsers block https://app.element.io from calling it " +
			"(mixed content). Use Element Desktop, a tunnel, or -web <element dist> to serve the client same-origin")
	}

	go func() {
		var err error
		if cfg.Server.TLSCert != "" {
			err = httpSrv.ServeTLS(ln, cfg.Server.TLSCert, cfg.Server.TLSKey)
		} else {
			err = httpSrv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Info("shutting down")
	cancel()
	srv.StopRunners()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

// fetchWebClient downloads an Element Web build so -web can serve it later.
func fetchWebClient(dir, version string, force bool, mirror string) error {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	rel, err := webdist.Fetch(ctx, webdist.Options{
		Dir:     dir,
		Version: version,
		Force:   force,
		Mirror:  mirror,
		Log:     log,
	})
	if err != nil {
		return err
	}

	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	fmt.Printf("\nElement Web %s is ready in %s\n", rel.Version, abs)
	fmt.Printf("Start the server with the client same-origin:\n\n")
	fmt.Printf("    yhmatrix -config config.yaml -web %s\n\n", dir)
	fmt.Printf("Then open the URL it prints, for example http://192.168.1.12:8008/\n")
	fmt.Printf("Log in with your Yunhu token, phone number, or email.\n")
	return nil
}

// startMock boots the fake Yunhu API on a random loopback port.
func startMock(log *slog.Logger) (baseURL, wsURL string, stop func(), err error) {
	m := yunhu.NewMockServer(log)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", "", nil, err
	}
	srv := &http.Server{Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()

	base := "http://" + ln.Addr().String()
	return base, "ws://" + ln.Addr().String() + "/ws", func() {
		_ = srv.Close()
	}, nil
}

// warnPublicURL catches the config landmine behind "cannot reach your account
// provider".
//
// When server.public_url is set, /.well-known/matrix/client advertises it as
// m.homeserver.base_url and every client — new logins, Element X's provider
// check, media downloads — resolves the homeserver to that address. The shipped
// example value is https://matrix.example.com, which does not exist. The only
// trace on the server side is a stream of successful well-known requests while
// the client fails, which is exactly what makes this hard to spot.
func warnPublicURL(log *slog.Logger, cfg *config.Config) {
	raw := strings.TrimSpace(cfg.Server.PublicURL)
	if raw == "" {
		return
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		log.Warn("server.public_url is not a usable URL",
			"public_url", raw,
			"fix", "comment it out to derive the address from each request")
		return
	}
	host := u.Hostname()
	if host == cfg.Server.Name {
		return
	}

	hint := "clients are told to use this address; comment it out unless a reverse proxy really serves this host"
	switch {
	case strings.Contains(host, "example.com"), strings.Contains(host, "example.org"),
		strings.Contains(host, "example.net"):
		hint = "this is the placeholder shipped in config.example.yaml — comment server.public_url out"
	}
	log.Warn("server.public_url does not match server.name; clients will be sent to a different host",
		"public_url", raw,
		"server_name", cfg.Server.Name,
		"fix", hint)
}

// warnServerNameMismatch reports accounts that were stored under a different
// server.name. User IDs embed the server name, so changing it makes the existing
// accounts unreachable: the client logs in as a brand-new user with no rooms.
// This became reachable once config.yaml next to the binary started being read.
func warnServerNameMismatch(log *slog.Logger, st *store.Store, serverName string) {
	suffix := ":" + serverName
	var stale []string
	for _, acc := range st.AllAccounts() {
		if acc.UserID == "" || strings.HasSuffix(acc.UserID, suffix) {
			continue
		}
		stale = append(stale, acc.UserID)
	}
	if len(stale) == 0 {
		return
	}
	examples := stale
	if len(examples) > 3 {
		examples = examples[:3]
	}
	log.Warn("existing accounts were created under a different server.name and will look empty",
		"server_name", serverName,
		"stale_accounts", len(stale),
		"examples", strings.Join(examples, ", "),
		"fix", "set server.name to the domain these user IDs end with, or point storage.data_dir at the old data")
}

// displayHost turns a listen address into something a user can paste.
func displayHost(cfg *config.Config, addr string) string {
	if cfg.Server.PublicURL != "" {
		return cfg.Server.PublicURL
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = localIPv4()
	}
	if port == "80" || port == "443" {
		return host
	}
	return net.JoinHostPort(host, port)
}

// localIPv4 guesses the LAN address used to reach this machine. Virtual
// adapters (docker, proxies, WSL) are common and would be misleading, so
// private LAN ranges are preferred over everything else.
func localIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "localhost"
	}
	best, bestScore := "", -1
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil || ip4.IsLinkLocalUnicast() {
			continue
		}
		score := 0
		switch {
		case ip4[0] == 192 && ip4[1] == 168:
			score = 3
		case ip4[0] == 10:
			score = 2
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			score = 2
		case ip4[0] == 198 && (ip4[1] == 18 || ip4[1] == 19):
			score = -1 // benchmark range used by VPN/proxy adapters
		}
		if score > bestScore {
			best, bestScore = ip4.String(), score
		}
	}
	if best == "" {
		return "localhost"
	}
	return best
}
