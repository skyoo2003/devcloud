// SPDX-License-Identifier: Apache-2.0

// internal/shared/restrouter.go
package shared

import (
	"net/http"
	"strings"
)

// RESTRoute defines a mapping from an HTTP method + URL path pattern to an
// AWS operation name.  Path segments enclosed in {} are treated as wildcards
// that match any single segment.
//
// Example:
//
//	RESTRoute{"GET", "/v2/email/identities/{}", "ListEmailIdentities"} — no trailing segment
//	RESTRoute{"GET", "/v2/email/identities/{id}", "GetEmailIdentity"}  — with trailing segment
type RESTRoute struct {
	Method  string
	Pattern string // segments with {} are wildcards
	Op      string
}

// RESTRouter resolves an AWS operation name from the HTTP method and URL path
// for REST-style services where the operation is implicit in the URL.
type RESTRouter struct {
	routes []RESTRoute
}

// NewRESTRouter creates a router from the given route definitions.
// Routes are matched in order; place more specific routes before general ones.
func NewRESTRouter(routes []RESTRoute) *RESTRouter {
	return &RESTRouter{routes: routes}
}

// Resolve returns the operation name for the given request, or "" if no route
// matches.
func (rr *RESTRouter) Resolve(req *http.Request) string {
	method := req.Method
	path := strings.TrimRight(req.URL.Path, "/")
	pathParts := strings.Split(path, "/")

	for _, r := range rr.routes {
		if r.Method != method {
			continue
		}
		pattern := strings.TrimRight(r.Pattern, "/")
		patParts := strings.Split(pattern, "/")
		if len(patParts) != len(pathParts) {
			continue
		}
		match := true
		for i := range patParts {
			if strings.HasPrefix(patParts[i], "{") {
				continue // wildcard
			}
			if patParts[i] != pathParts[i] {
				match = false
				break
			}
		}
		if match {
			return r.Op
		}
	}
	return ""
}
