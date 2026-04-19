// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/dashboard"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateway_Integration(t *testing.T) {
	resp := &plugin.Response{
		StatusCode:  http.StatusOK,
		Headers:     map[string]string{},
		Body:        []byte("<ListBucketResult/>"),
		ContentType: "application/xml",
	}
	reg := newRegistryWithStub("s3", resp)

	// Build the full middleware-chained handler the same way Gateway does,
	// but serve it through httptest.NewServer so we exercise the real stack.
	router := NewServiceRouter(reg)
	handler := ChainMiddleware(router,
		ErrorRecoveryMiddleware,
		CORSMiddleware,
		RequestIDMiddleware,
		RequestLoggerMiddleware,
	)

	srv := httptest.NewServer(handler)
	defer srv.Close()

	res, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()

	assert.Equal(t, http.StatusOK, res.StatusCode)
	// Middleware headers must be present.
	assert.NotEmpty(t, res.Header.Get("X-Amz-Request-Id"), "RequestID middleware must set X-Amz-Request-Id")
	assert.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"), "CORS middleware must set Access-Control-Allow-Origin")
}

func TestGateway_StartShutdown(t *testing.T) {
	reg := plugin.NewRegistry()
	lc := dashboard.NewLogCollector(100)
	dashMux := http.NewServeMux()
	gw := New(0, reg, dashMux, lc, "") // port 0 lets the OS pick a free port

	errCh := make(chan error, 1)
	go func() {
		// Start() blocks; an http.ErrServerClosed from Shutdown is expected.
		if err := gw.Start(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()

	err := gw.Shutdown(context.Background())
	require.NoError(t, err)

	startErr := <-errCh
	assert.NoError(t, startErr)
}
