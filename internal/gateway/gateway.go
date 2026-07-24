// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/admin"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Gateway wraps an HTTP server and ties together the plugin registry with
// the full middleware chain.
type Gateway struct {
	server   *http.Server
	registry *plugin.Registry
}

// New creates a Gateway that listens on the given port.
//
// Routing:
//   - Requests starting with /devcloud/api/ → adminAPI handler
//   - Everything else → service router (AWS API)
//
// When logCollector is non-nil, a logging middleware wraps the service router
// and records each request after the response has been written. A nil
// collector (admin API disabled) skips that wrapper entirely, keeping the
// mutex lock + ring-buffer write off the request hot path.
func New(port int, registry *plugin.Registry, adminAPI http.Handler, logCollector *admin.LogCollector) *Gateway {
	router := NewServiceRouter(registry)

	// Logging middleware: records AWS API requests to logCollector. Only
	// installed when there is a collector to read them.
	var serviceHandler http.Handler = router
	if logCollector != nil {
		serviceHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := newStatusRecorder(w)
			router.ServeHTTP(rec, r)
			logCollector.Add(admin.RequestLog{
				Method:    r.Method,
				Path:      r.URL.Path,
				Status:    rec.statusCode,
				Duration:  time.Since(start).String(),
				Timestamp: start,
				Service:   detectService(r.URL.Path),
			})
		})
	}

	awsHandler := ChainMiddleware(serviceHandler,
		ErrorRecoveryMiddleware,
		BodyLimitMiddleware,
		CORSMiddleware,
		RequestIDMiddleware,
		RequestLoggerMiddleware,
	)

	// Top-level mux: admin API takes priority; everything else is an AWS API call.
	mux := http.NewServeMux()
	mux.Handle("/devcloud/api/", adminAPI)
	mux.Handle("/", awsHandler)

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
