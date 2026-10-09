// SPDX-License-Identifier: Apache-2.0

package sagemakerruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSageMakerRuntimeOperations(t *testing.T) {
	p := &Provider{}
	ctx := context.Background()

	// 1. InvokeEndpoint
	req, err := http.NewRequestWithContext(ctx, "POST", "/endpoints/my-bert-endpoint/invocations", strings.NewReader(`{"inputs": "Hello world"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Amzn-SageMaker-Custom-Attributes", "custom-attr-1")

	resp, err := p.HandleRequest(ctx, "InvokeEndpoint", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "AllTraffic", resp.Headers["x-Amzn-Invoked-Production-Variant"])
	assert.Equal(t, "custom-attr-1", resp.Headers["X-Amzn-SageMaker-Custom-Attributes"])

	var body map[string]any
	err = json.Unmarshal(resp.Body, &body)
	require.NoError(t, err)
	assert.Equal(t, "my-bert-endpoint", body["endpoint"])

	// 2. InvokeEndpointAsync
	asyncReq, err := http.NewRequestWithContext(ctx, "POST", "/endpoints/my-bert-endpoint/async-invocations", strings.NewReader(`{}`))
	require.NoError(t, err)

	asyncResp, err := p.HandleRequest(ctx, "InvokeEndpointAsync", asyncReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, asyncResp.StatusCode)
	assert.NotEmpty(t, asyncResp.Headers["x-Amzn-SageMaker-OutputLocation"])

	var asyncBody map[string]any
	err = json.Unmarshal(asyncResp.Body, &asyncBody)
	require.NoError(t, err)
	assert.NotEmpty(t, asyncBody["inferenceId"])
	assert.NotEmpty(t, asyncBody["outputLocation"])

	// 3. InvokeEndpointWithResponseStream
	streamReq, err := http.NewRequestWithContext(ctx, "POST", "/endpoints/my-bert-endpoint/invocations-response-stream", strings.NewReader(`{}`))
	require.NoError(t, err)

	streamResp, err := p.HandleRequest(ctx, "InvokeEndpointWithResponseStream", streamReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, streamResp.StatusCode)
}
