// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/dashboard"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Gateway wraps an HTTP server and ties together the plugin registry with
// the full middleware chain.
type Gateway struct {
	server   *http.Server
	registry *plugin.Registry
}

// isAWSRequest returns true when the request looks like an AWS API call.
// It checks for SigV4 authorization, X-Amz-Target header, form-encoded
// Action parameter, or known AWS path prefixes (e.g. Lambda).
func isAWSRequest(r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
		return true
	}
	if r.Header.Get("X-Amz-Target") != "" {
		return true
	}
	if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		return true
	}
	// Lambda management API
	if strings.HasPrefix(r.URL.Path, "/2015-03-31/") {
		return true
	}
	return false
}

// New creates a Gateway that listens on the given port.
//
// Routing:
//   - Requests starting with /devcloud/api/ → dashAPI handler
//   - AWS API requests (SigV4, X-Amz-Target, form-encoded Action, Lambda paths) → service router
//   - Everything else (when webDir is non-empty) → static files from webDir (dashboard SPA)
//   - Everything else (when webDir is empty) → service router
//
// A logging middleware wraps the service router and records each request to
// logCollector after the response has been written.
func New(port int, registry *plugin.Registry, dashAPI http.Handler, logCollector *dashboard.LogCollector, webDir string) *Gateway {
	router := NewServiceRouter(registry)

	// Logging middleware: records AWS API requests to logCollector.
	loggedRouter := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := newStatusRecorder(w)
		router.ServeHTTP(rec, r)
		logCollector.Add(dashboard.RequestLog{
			Method:    r.Method,
			Path:      r.URL.Path,
			Status:    rec.statusCode,
			Duration:  time.Since(start).String(),
			Timestamp: start,
			Service:   detectService(r.URL.Path),
		})
	})

	awsHandler := ChainMiddleware(loggedRouter,
		ErrorRecoveryMiddleware,
		BodyLimitMiddleware,
		CORSMiddleware,
		RequestIDMiddleware,
		RequestLoggerMiddleware,
	)

	// Top-level mux: dashboard API takes priority.
	mux := http.NewServeMux()
	mux.Handle("/devcloud/api/", dashAPI)

	if webDir != "" {
		fs := http.FileServer(http.Dir(webDir))
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// AWS API requests bypass static serving and go to the service router.
			if isAWSRequest(r) {
				awsHandler.ServeHTTP(w, r)
				return
			}
			// Try serving the exact static file; fall back to index.html for SPA routing.
			path := filepath.Join(webDir, filepath.Clean("/"+r.URL.Path))
			if _, err := os.Stat(path); os.IsNotExist(err) {
				http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
				return
			}
			fs.ServeHTTP(w, r)
		}))
	} else {
		mux.Handle("/", awsHandler)
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       5 * time.Minute,
		IdleTimeout:       120 * time.Second,
	}

	return &Gateway{
		server:   srv,
		registry: registry,
	}
}

// detectService attempts to identify the AWS service from a request path.
// It returns an empty string when the service cannot be determined.
func detectService(path string) string {
	// Strip leading slash and take the first path segment.
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "s3"
	}
	parts := strings.SplitN(trimmed, "/", 2)
	return parts[0]
}

// Start begins accepting connections. It blocks until the server is stopped
// and returns http.ErrServerClosed on a clean shutdown.
func (g *Gateway) Start() error {
	return g.server.ListenAndServe()
}

// Shutdown gracefully stops the server, waiting for in-flight requests to
// complete or until ctx is cancelled.
func (g *Gateway) Shutdown(ctx context.Context) error {
	return g.server.Shutdown(ctx)
}
