// SPDX-License-Identifier: Apache-2.0

package sagemakerruntimehttp2

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSageMakerRuntimeHttp2Operations(t *testing.T) {
	p := &Provider{}
	ctx := context.Background()

	req, err := http.NewRequestWithContext(ctx, "POST", "/endpoints/my-h2-endpoint/invocations-bidirectional-stream", strings.NewReader(`{}`))
	require.NoError(t, err)

	resp, err := p.HandleRequest(ctx, "InvokeEndpointWithBidirectionalStream", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "AllTraffic", resp.Headers["x-Amzn-Invoked-Production-Variant"])

	var body map[string]any
	err = json.Unmarshal(resp.Body, &body)
	require.NoError(t, err)
	assert.Equal(t, "my-h2-endpoint", body["endpoint"])
}
