// SPDX-License-Identifier: Apache-2.0

// internal/services/iotdataplane/provider_test.go
package iotdataplane

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
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
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

func TestThingShadowCRUD(t *testing.T) {
	p := newTestProvider(t)

	// UpdateThingShadow (create)
	payload := `{"state":{"reported":{"temperature":22}}}`
	resp := callREST(t, p, "POST", "/things/myThing/shadow", "UpdateThingShadow", payload)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, float64(1), rb["version"])
	assert.Equal(t, payload, rb["payload"])

	// GetThingShadow
	resp2 := callREST(t, p, "GET", "/things/myThing/shadow", "GetThingShadow", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, float64(1), rb2["version"])
	assert.Equal(t, payload, rb2["payload"])

	// UpdateThingShadow again — version should increment
	payload2 := `{"state":{"reported":{"temperature":25}}}`
	resp3 := callREST(t, p, "POST", "/things/myThing/shadow", "UpdateThingShadow", payload2)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	assert.Equal(t, float64(2), rb3["version"])

	// ListNamedShadowsForThing — create a named shadow first
	callREST(t, p, "POST", "/things/myThing/shadow?name=namedShadow", "UpdateThingShadow", `{}`)
	resp4 := callREST(t, p, "GET", "/api/things/shadow/ListNamedShadowsForThing/myThing", "ListNamedShadowsForThing", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	results, ok := rb4["results"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(results), 2) // classic + namedShadow

	// DeleteThingShadow
	resp5 := callREST(t, p, "DELETE", "/things/myThing/shadow", "DeleteThingShadow", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetThingShadow after delete — should 404
	resp6 := callREST(t, p, "GET", "/things/myThing/shadow", "GetThingShadow", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// DeleteThingShadow non-existent — should 404
	resp7 := callREST(t, p, "DELETE", "/things/nonexistent/shadow", "DeleteThingShadow", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestPublishAndRetainedMessages(t *testing.T) {
	p := newTestProvider(t)

	// Publish non-retained — no-op, success
	nonRetainBody := `{"topic":"test/noop","payload":"hello","retain":false,"qos":0}`
	resp := callREST(t, p, "POST", "/topics/test/noop", "Publish", nonRetainBody)
	assert.Equal(t, 200, resp.StatusCode)

	// GetRetainedMessage for non-retained topic — should 404
	resp2 := callREST(t, p, "GET", "/retainedMessage/test/noop", "GetRetainedMessage", "")
	assert.Equal(t, 404, resp2.StatusCode)

	// Publish retained message
	retainBody := `{"topic":"sensor/temp","payload":"42","retain":true,"qos":1}`
	resp3 := callREST(t, p, "POST", "/topics/sensor/temp", "Publish", retainBody)
	assert.Equal(t, 200, resp3.StatusCode)

	// GetRetainedMessage
	resp4 := callREST(t, p, "GET", "/retainedMessage/sensor/temp", "GetRetainedMessage", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "sensor/temp", rb4["topic"])
	assert.Equal(t, float64(1), rb4["qos"])

	// Publish another retained message
	retain2 := `{"topic":"sensor/humidity","payload":"60","retain":true,"qos":0}`
	callREST(t, p, "POST", "/topics/sensor/humidity", "Publish", retain2)

	// ListRetainedMessages
	resp5 := callREST(t, p, "GET", "/retainedMessage", "ListRetainedMessages", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	topics, ok := rb5["retainedTopics"].([]any)
	require.True(t, ok)
	assert.Len(t, topics, 2)

	// DeleteConnection — no-op
	resp6 := callREST(t, p, "DELETE", "/connections/client123", "DeleteConnection", "")
	assert.Equal(t, 200, resp6.StatusCode)
}
