// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestLambdaDurableAndStreaming(t *testing.T) {
	tmpDir := t.TempDir()
	p := &LambdaProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. CheckpointDurableExecution
	reqCheckpoint := httptest.NewRequest(http.MethodPost, "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:execution:123/checkpoint", bytes.NewReader([]byte(`{"step":"step1"}`)))
	resp, err := p.HandleRequest(ctx, "CheckpointDurableExecution", reqCheckpoint)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Heartbeat callback
	reqHeartbeat := httptest.NewRequest(http.MethodPost, "/2025-12-01/durable-execution-callbacks/cb-123/heartbeat", nil)
	resp, err = p.HandleRequest(ctx, "SendDurableExecutionCallbackHeartbeat", reqHeartbeat)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 3. Callback success
	reqSucceed := httptest.NewRequest(http.MethodPost, "/2025-12-01/durable-execution-callbacks/cb-123/succeed", nil)
	resp, err = p.HandleRequest(ctx, "SendDurableExecutionCallbackSuccess", reqSucceed)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. Callback failure
	reqFail := httptest.NewRequest(http.MethodPost, "/2025-12-01/durable-execution-callbacks/cb-456/fail", nil)
	resp, err = p.HandleRequest(ctx, "SendDurableExecutionCallbackFailure", reqFail)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 5. Stop durable execution
	reqStop := httptest.NewRequest(http.MethodPost, "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:execution:123/stop", nil)
	resp, err = p.HandleRequest(ctx, "StopDurableExecution", reqStop)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 6. InvokeAsync with nonexistent function returns 404
	reqAsync := httptest.NewRequest(http.MethodPost, "/2014-11-13/functions/nonexistent/invoke-async", bytes.NewReader([]byte(`{"key":"val"}`)))
	resp, err = p.HandleRequest(ctx, "InvokeAsync", reqAsync)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// 7. Create function using test helpers and test InvokeAsync
	_, err = p.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)

	reqAsyncValid := httptest.NewRequest(http.MethodPost, "/2014-11-13/functions/immutable/invoke-async", bytes.NewReader([]byte(`{"key":"val"}`)))
	resp, err = p.HandleRequest(ctx, "InvokeAsync", reqAsyncValid)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)

	// 8. InvokeWithResponseStream returns chunked / streaming headers
	reqStream := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions/immutable/response-streaming-invocations", bytes.NewReader([]byte(`{}`)))
	resp, err = p.HandleRequest(ctx, "InvokeWithResponseStream", reqStream)
	require.NoError(t, err)
	assert.Equal(t, "chunked", resp.Headers["Transfer-Encoding"])
}
