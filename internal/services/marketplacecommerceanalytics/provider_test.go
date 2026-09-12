// SPDX-License-Identifier: Apache-2.0

// internal/services/marketplacecommerceanalytics/provider_test.go
package marketplacecommerceanalytics

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// callJSON mirrors how the gateway calls a json-1.1 provider: the operation
// comes from X-Amz-Target, so it is passed in rather than resolved from a path.
func callJSON(t *testing.T, p *Provider, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestDataSetOperations(t *testing.T) {
	p := newTestProvider(t)

	for _, op := range []string{"GenerateDataSet", "StartSupportDataExport"} {
		t.Run(op, func(t *testing.T) {
			resp := callJSON(t, p, op, `{"destinationS3BucketName":"reports"}`)
			assert.Equal(t, 200, resp.StatusCode)
			// lowerCamel on the wire, verified against the model.
			assert.NotEmpty(t, parseBody(t, resp)["dataSetRequestId"])
		})
	}
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "marketplacecommerceanalytics", p.ServiceID())
	assert.Equal(t, "MarketplaceCommerceAnalytics20150701", p.ServiceName())
	assert.Equal(t, plugin.ProtocolJSON11, p.Protocol())

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resources)
}

func TestUnknownOperationIsUnhandled(t *testing.T) {
	p := newTestProvider(t)
	req := httptest.NewRequest("POST", "/", strings.NewReader("{}"))
	_, err := p.HandleRequest(context.Background(), "NoSuchThing", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)
}
