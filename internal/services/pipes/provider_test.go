// SPDX-License-Identifier: Apache-2.0

// internal/services/pipes/provider_test.go
package pipes

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *PipesProvider {
	t.Helper()
	p := &PipesProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *PipesProvider, method, path, op string, body []byte) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
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

func TestCreateAndDescribePipe(t *testing.T) {
	p := newTestProvider(t)

	body, _ := json.Marshal(map[string]any{
		"Source":      "arn:aws:sqs:us-east-1:000000000000:my-queue",
		"Target":      "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
		"RoleArn":     "arn:aws:iam::000000000000:role/PipeRole",
		"Description": "test pipe",
	})
	resp := callOp(t, p, "POST", "/v1/pipes/test-pipe", "CreatePipe", body)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "test-pipe", rb["Name"])
	assert.Contains(t, rb["Arn"].(string), "test-pipe")
	assert.Equal(t, "RUNNING", rb["CurrentState"])

	// Describe
	resp2 := callOp(t, p, "GET", "/v1/pipes/test-pipe", "DescribePipe", nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "test-pipe", rb2["Name"])
	assert.Equal(t, "test pipe", rb2["Description"])
}

func TestListPipes(t *testing.T) {
	p := newTestProvider(t)

	for _, name := range []string{"pipe-a", "pipe-b"} {
		body, _ := json.Marshal(map[string]any{"Source": "src", "Target": "tgt"})
		callOp(t, p, "POST", "/v1/pipes/"+name, "CreatePipe", body)
	}

	resp := callOp(t, p, "GET", "/v1/pipes", "ListPipes", nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pipes, ok := rb["Pipes"].([]any)
	require.True(t, ok)
	assert.Len(t, pipes, 2)
}

func TestUpdatePipe(t *testing.T) {
	p := newTestProvider(t)

	body, _ := json.Marshal(map[string]any{"Source": "src", "Target": "tgt"})
	callOp(t, p, "POST", "/v1/pipes/my-pipe", "CreatePipe", body)

	body2, _ := json.Marshal(map[string]any{"Description": "updated"})
	resp := callOp(t, p, "PUT", "/v1/pipes/my-pipe", "UpdatePipe", body2)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "updated", rb["Description"])
}

func TestDeletePipe(t *testing.T) {
	p := newTestProvider(t)

	body, _ := json.Marshal(map[string]any{"Source": "src", "Target": "tgt"})
	callOp(t, p, "POST", "/v1/pipes/del-pipe", "CreatePipe", body)

	resp := callOp(t, p, "DELETE", "/v1/pipes/del-pipe", "DeletePipe", nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "DELETING", rb["CurrentState"])

	// Describe should 404
	resp2 := callOp(t, p, "GET", "/v1/pipes/del-pipe", "DescribePipe", nil)
	assert.Equal(t, 404, resp2.StatusCode)
}

func TestStartStopPipe(t *testing.T) {
	p := newTestProvider(t)

	body, _ := json.Marshal(map[string]any{"Source": "src", "Target": "tgt", "DesiredState": "STOPPED"})
	callOp(t, p, "POST", "/v1/pipes/ss-pipe", "CreatePipe", body)

	resp := callOp(t, p, "POST", "/v1/pipes/ss-pipe/start", "StartPipe", nil)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "RUNNING", rb["CurrentState"])

	resp2 := callOp(t, p, "POST", "/v1/pipes/ss-pipe/stop", "StopPipe", nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "STOPPED", rb2["CurrentState"])
}

func TestTagOperations(t *testing.T) {
	p := newTestProvider(t)

	body, _ := json.Marshal(map[string]any{"Source": "src", "Target": "tgt"})
	createResp := callOp(t, p, "POST", "/v1/pipes/tag-pipe", "CreatePipe", body)
	rb := parseBody(t, createResp)
	arn := rb["Arn"].(string)

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{"tags": map[string]any{"env": "prod", "team": "platform"}})
	resp := callOp(t, p, "POST", "/tags/"+arn, "TagResource", tagBody)
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callOp(t, p, "GET", "/tags/"+arn, "ListTagsForResource", nil)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=env", nil)
	resp3, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify env tag gone
	resp4 := callOp(t, p, "GET", "/tags/"+arn, "ListTagsForResource", nil)
	rb4 := parseBody(t, resp4)
	tags4 := rb4["tags"].(map[string]any)
	_, hasEnv := tags4["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "platform", tags4["team"])
}
