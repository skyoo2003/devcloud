// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesisvideowebrtcstorage/provider_test.go
package kinesisvideowebrtcstorage

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

func TestJoinStorageSessionOperations(t *testing.T) {
	p := newTestProvider(t)
	const body = `{"channelArn":"arn:aws:kinesisvideo:us-east-1:000000000000:channel/demo/1"}`

	cases := []struct{ op, path string }{
		{"JoinStorageSession", "/joinStorageSession"},
		{"JoinStorageSessionAsViewer", "/joinStorageSessionAsViewer"},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			// op resolved from the path, as the gateway does for rest-json.
			resp := callREST(t, p, "POST", c.path, "", body)
			assert.Equal(t, 200, resp.StatusCode)
			// Both model a Smithy Unit output.
			assert.Equal(t, map[string]any{}, parseBody(t, resp))
		})
	}
}

func TestJoinStorageSessionRequiresChannelArn(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/joinStorageSession", "", `{}`)
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "InvalidArgumentException", parseBody(t, resp)["__type"])

	// An empty body is the same miss, not a 500.
	resp = callREST(t, p, "POST", "/joinStorageSession", "", "")
	assert.Equal(t, 400, resp.StatusCode)
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "kinesisvideowebrtcstorage", p.ServiceID())
	assert.Equal(t, "AWSAcuityRoutingServiceLambda", p.ServiceName())
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
