package matrix

import (
	"net/http"
	"strings"
)

// Ctx bundles a request with its path parameters and authenticated session.
type Ctx struct {
	W      http.ResponseWriter
	R      *http.Request
	Params map[string]string

	// Account and Device are set for authenticated routes.
	Account *accountRef
	Device  *deviceRef
}

// Param returns a path parameter.
func (c *Ctx) Param(name string) string { return c.Params[name] }

// Query returns a query parameter.
func (c *Ctx) Query(name string) string { return c.R.URL.Query().Get(name) }

// HandlerFunc is a Matrix request handler.
type HandlerFunc func(*Ctx)

type route struct {
	method  string
	segs    []string
	rest    bool
	auth    bool
	handler HandlerFunc
}

// Router is a small path router tailored to the Matrix API. It exists instead
// of http.ServeMux because Matrix routes need optional trailing segments (state
// keys), version wildcards and per-route auth flags.
type Router struct {
	routes []route
	// auth resolves the bearer token; nil means unauthenticated route.
	auth func(*Ctx) bool
}

// NewRouter builds a router using auth as the authentication hook.
func NewRouter(auth func(*Ctx) bool) *Router {
	return &Router{auth: auth}
}

// Handle registers a route. pattern looks like
//
//	GET /_matrix/client/:ver/sync
//	PUT /_matrix/client/:ver/rooms/:roomId/send/:eventType/:txnId
//
// A trailing "/*" matches the remainder of the path (used for media names).
func (rt *Router) Handle(method, pattern string, auth bool, h HandlerFunc) {
	segs := splitPath(pattern)
	rest := false
	if n := len(segs); n > 0 && segs[n-1] == "*" {
		rest = true
		segs = segs[:n-1]
	}
	rt.routes = append(rt.routes, route{method: method, segs: segs, rest: rest, auth: auth, handler: h})
}

// GET registers a GET route.
func (rt *Router) GET(pattern string, auth bool, h HandlerFunc) {
	rt.Handle(http.MethodGet, pattern, auth, h)
}

// POST registers a POST route.
func (rt *Router) POST(pattern string, auth bool, h HandlerFunc) {
	rt.Handle(http.MethodPost, pattern, auth, h)
}

// PUT registers a PUT route.
func (rt *Router) PUT(pattern string, auth bool, h HandlerFunc) {
	rt.Handle(http.MethodPut, pattern, auth, h)
}

// DELETE registers a DELETE route.
func (rt *Router) DELETE(pattern string, auth bool, h HandlerFunc) {
	rt.Handle(http.MethodDelete, pattern, auth, h)
}

func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func (rt *Router) match(method string, segs []string) (*route, map[string]string) {
	var best *route
	var bestParams map[string]string
	for i := range rt.routes {
		r := &rt.routes[i]
		if r.method != method {
			continue
		}
		if r.rest {
			if len(segs) < len(r.segs) {
				continue
			}
		} else if len(segs) != len(r.segs) {
			continue
		}
		params := map[string]string{}
		ok := true
		for j, ps := range r.segs {
			if strings.HasPrefix(ps, ":") {
				params[ps[1:]] = segs[j]
				continue
			}
			if ps != segs[j] {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if r.rest {
			params["*"] = strings.Join(segs[len(r.segs):], "/")
		}
		best = r
		bestParams = params
		break
	}
	return best, bestParams
}

// ServeHTTP implements http.Handler.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	segs := splitPath(r.URL.Path)
	// Go's URL path keeps a trailing slash as an empty last segment, which is
	// exactly what empty state keys need; strings.Split preserves it, but
	// splitPath trims it, so restore it here.
	if strings.HasSuffix(r.URL.Path, "/") && len(segs) > 0 {
		segs = append(segs, "")
	}

	route, params := rt.match(r.Method, segs)
	if route == nil && strings.HasSuffix(r.URL.Path, "/") && len(segs) > 0 {
		// Patterns that legitimately end in "/" (e.g. /pushrules/) match the
		// trimmed form, while empty state keys need the extra empty segment.
		route, params = rt.match(r.Method, segs[:len(segs)-1])
	}
	if route == nil {
		// A HEAD request should behave like GET for media.
		if r.Method == http.MethodHead {
			route, params = rt.match(http.MethodGet, segs)
		}
	}
	if route == nil {
		writeUnknown(w)
		return
	}

	c := &Ctx{W: w, R: r, Params: params}
	if route.auth {
		if rt.auth == nil || !rt.auth(c) {
			return // auth already wrote the error
		}
	}
	route.handler(c)
}
