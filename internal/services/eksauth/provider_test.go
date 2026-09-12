// SPDX-License-Identifier: Apache-2.0

// internal/services/eksauth/provider_test.go
package eksauth

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

func TestAssumeRoleForPodIdentity(t *testing.T) {
	p := newTestProvider(t)

	// op is resolved from the path, the way the gateway calls a rest-json
	// provider.
	resp := callREST(t, p, "POST", "/clusters/my-cluster/assume-role-for-pod-identity", "", `{"token":"jwt"}`)
	assert.Equal(t, 200, resp.StatusCode)

	rb := parseBody(t, resp)
	assert.Equal(t, "pods.eks.amazonaws.com", rb["audience"])
	require.Contains(t, rb, "subject")
	require.Contains(t, rb, "assumedRoleUser")

	assoc, ok := rb["podIdentityAssociation"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, assoc["associationArn"], "my-cluster")
	assert.NotEmpty(t, assoc["associationId"])

	creds, ok := rb["credentials"].(map[string]any)
	require.True(t, ok)
	for _, k := range []string{"accessKeyId", "secretAccessKey", "sessionToken", "expiration"} {
		assert.NotEmpty(t, creds[k], k)
	}
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "eksauth", p.ServiceID())
	assert.Equal(t, "EKSAuthFrontend", p.ServiceName())
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
