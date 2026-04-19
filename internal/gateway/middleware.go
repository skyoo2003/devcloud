// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Middleware is a function that wraps an http.Handler to add behaviour.
type Middleware func(http.Handler) http.Handler

// ChainMiddleware applies middlewares to h in reverse order so that the first
// middleware in the list is the outermost (first to execute).
func ChainMiddleware(h http.Handler, middlewares ...Middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// statusRecorder wraps http.ResponseWriter to capture the HTTP status code.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.statusCode = code
	sr.ResponseWriter.WriteHeader(code)
}

// RequestLoggerMiddleware logs the HTTP method, path, status code, and
// request duration using the standard slog package.
func RequestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := newStatusRecorder(w)
		next.ServeHTTP(rec, r)
		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.statusCode,
			"duration", time.Since(start),
		)
	})
}

// RequestIDMiddleware attaches a unique X-Amz-Request-Id header to every response.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b [8]byte
		rand.Read(b[:]) //nolint:errcheck
		requestID := hex.EncodeToString(b[:])
		w.Header().Set("X-Amz-Request-Id", requestID)
		next.ServeHTTP(w, r)
	})
}

// ErrorRecoveryMiddleware catches panics and returns a 500 InternalError response.
// It detects the protocol from the request to return XML or JSON as appropriate.
func ErrorRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				// JSON-protocol services use X-Amz-Target or application/x-amz-json content types.
				if r.Header.Get("X-Amz-Target") != "" ||
					strings.Contains(r.Header.Get("Content-Type"), "application/x-amz-json") ||
					strings.HasPrefix(r.URL.Path, "/2015-03-31/") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = fmt.Fprintf(w, `{"__type":"InternalError","message":"%v"}`, rec)
				} else {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>%v</Message></Error>`, rec)
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// BodyLimitMiddleware restricts the request body size to prevent memory exhaustion.
// The default limit is 256 MiB, generous enough for S3 uploads and Lambda code zips.
func BodyLimitMiddleware(next http.Handler) http.Handler {
	const maxBodySize int64 = 256 << 20 // 256 MiB
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		next.ServeHTTP(w, r)
	})
}

// CORSMiddleware adds permissive CORS headers required for browser-based AWS SDK clients.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Amz-Date, X-Amz-Target, X-Amz-Security-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
