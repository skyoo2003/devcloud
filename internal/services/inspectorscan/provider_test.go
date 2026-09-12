// SPDX-License-Identifier: Apache-2.0

// internal/services/inspectorscan/provider_test.go
package inspectorscan

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

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
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

func TestScanSbomEchoesTheDocument(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/scan/sbom", "",
		`{"sbom":{"bomFormat":"CycloneDX","specVersion":"1.5"}}`)
	assert.Equal(t, 200, resp.StatusCode)

	sbom, ok := parseBody(t, resp)["sbom"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CycloneDX", sbom["bomFormat"])
	assert.Equal(t, "1.5", sbom["specVersion"])
}

// An empty body is what the smoke suite sends, and it must still be a 200 with
// the member the model declares.
func TestScanSbomEmptyBody(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/scan/sbom", "", "")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, map[string]any{}, parseBody(t, resp)["sbom"])
}

func TestScanSbomInvalidJSON(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/scan/sbom", "", "not json")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "SerializationException", parseBody(t, resp)["__type"])
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "inspectorscan", p.ServiceID())
	assert.Equal(t, "InspectorScan", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resources)
}

func TestUnknownOperationIsUnhandled(t *testing.T) {
	p := newTestProvider(t)
	req := httptest.NewRequest("POST", "/nope", strings.NewReader("{}"))
	_, err := p.HandleRequest(context.Background(), "NoSuchThing", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)
}
