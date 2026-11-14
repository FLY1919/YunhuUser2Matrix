package matrix

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// This file serves an optional static Element Web build from the homeserver's
// own origin.
//
// Why: browsers refuse to let a page loaded over https:// (for example
// app.element.io) call a plain http:// homeserver on a private address. Chrome
// reports it as mixed content / private network access, the request never
// reaches us, and Element surfaces the failure as "Invalid /versions response".
// Serving the client from http://<host>:<port>/ makes every request same-origin,
// so a LAN deployment works without certificates.

// baseURL returns the externally visible base URL of this server.
func (s *Server) baseURL(r *http.Request) string {
	if s.Cfg.Server.PublicURL != "" {
		return s.Cfg.Server.PublicURL
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = strings.TrimSpace(strings.Split(p, ",")[0])
	}
	host := r.Host
	if host == "" {
		host = s.Cfg.Server.Name
	}
	return scheme + "://" + host
}

// webConfig builds the Element config.json served at the site root.
func (s *Server) webConfig(r *http.Request) map[string]any {
	base := s.baseURL(r)
	return map[string]any{
		"default_server_config": map[string]any{
			"m.homeserver": map[string]any{
				"base_url":    base,
				"server_name": s.Cfg.Server.Name,
			},
		},
		"brand":               "yhmatrix",
		"default_server_name": s.Cfg.Server.Name,
		"show_labs_settings":  true,
		"disable_guests":      true,
		"room_directory":      map[string]any{"servers": []string{}},
		"setting_defaults":    map[string]any{"UIFeature.registration": false},
	}
}

// serveWeb serves the configured static client. It reports whether the request
// was handled.
func (s *Server) serveWeb(w http.ResponseWriter, r *http.Request) bool {
	dir := s.Cfg.Server.WebDir
	if dir == "" {
		return false
	}
	p := r.URL.Path
	// Matrix endpoints always win over the static site.
	if strings.HasPrefix(p, "/_matrix") || strings.HasPrefix(p, "/.well-known") {
		return false
	}

	if p == "/config.json" {
		writeJSON(w, http.StatusOK, s.webConfig(r))
		return true
	}

	clean := path.Clean("/" + p)
	full := filepath.Join(dir, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
	// Guard against path traversal.
	if !strings.HasPrefix(full, filepath.Clean(dir)+string(os.PathSeparator)) && full != filepath.Clean(dir) {
		http.NotFound(w, r)
		return true
	}
	if st, err := os.Stat(full); err == nil && !st.IsDir() {
		http.ServeFile(w, r, full)
		return true
	}
	if clean == "/" {
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
		return true
	}
	if st, err := os.Stat(full); err == nil && st.IsDir() {
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
		return true
	}
	// Element uses hash routing, so unknown paths fall back to the shell.
	http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	return true
}
