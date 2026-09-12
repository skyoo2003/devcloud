// SPDX-License-Identifier: Apache-2.0

// internal/services/ec2instanceconnect/provider_test.go
package ec2instanceconnect

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

func TestSendPublicKeyOperations(t *testing.T) {
	p := newTestProvider(t)

	for _, op := range []string{"SendSSHPublicKey", "SendSerialConsoleSSHPublicKey"} {
		t.Run(op, func(t *testing.T) {
			resp := callJSON(t, p, op, `{"InstanceId":"i-1234567890abcdef0","SSHPublicKey":"ssh-rsa AAAA"}`)
			assert.Equal(t, 200, resp.StatusCode)

			rb := parseBody(t, resp)
			// PascalCase on the wire, verified against the model.
			assert.NotEmpty(t, rb["RequestId"])
			assert.Equal(t, true, rb["Success"])
		})
	}
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "ec2instanceconnect", p.ServiceID())
	assert.Equal(t, "AWSEC2InstanceConnectService", p.ServiceName())
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
