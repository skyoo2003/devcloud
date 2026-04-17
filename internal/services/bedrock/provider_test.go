// SPDX-License-Identifier: Apache-2.0

package bedrock

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

func call(t *testing.T, p *Provider, method, path, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestListFoundationModels(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GET", "/foundation-models", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	models, ok := rb["modelSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, models, 4)
}

func TestGetFoundationModel(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GET", "/foundation-models/anthropic.claude-3-opus-20240229-v1:0", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Contains(t, rb, "modelDetails")

	// Not found
	resp2 := call(t, p, "GET", "/foundation-models/nonexistent", "")
	assert.Equal(t, 404, resp2.StatusCode)
}

func TestInvokeModel(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "POST", "/model/anthropic.claude-3-sonnet-20240229-v1:0/invoke", `{"prompt":"hello"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "stub response", rb["completion"])
}

func TestCustomizationJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"jobName":"test-job","customModelName":"my-model","baseModelIdentifier":"amazon.titan-text-express-v1"}`
	resp := call(t, p, "POST", "/model-customization-jobs", body)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	jobARN, ok := rb["jobArn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobARN)

	// Extract job ID from ARN
	parts := strings.Split(jobARN, "/")
	jobID := parts[len(parts)-1]

	// Get
	resp2 := call(t, p, "GET", "/model-customization-jobs/"+jobID, "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "test-job", rb2["jobName"])

	// List
	resp3 := call(t, p, "GET", "/model-customization-jobs", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jobs := rb3["modelCustomizationJobSummaries"].([]any)
	assert.Len(t, jobs, 1)

	// Stop
	resp4 := call(t, p, "POST", "/model-customization-jobs/"+jobID+"/stop", "")
	assert.Equal(t, 204, resp4.StatusCode)
}

func TestCustomModelCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create customization job first (also creates custom model)
	body := `{"jobName":"model-job","customModelName":"custom-model-1","baseModelIdentifier":"amazon.titan-text-express-v1"}`
	resp := call(t, p, "POST", "/model-customization-jobs", body)
	require.Equal(t, 201, resp.StatusCode)

	// List custom models
	resp2 := call(t, p, "GET", "/custom-models", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	models := rb2["modelSummaries"].([]any)
	require.Len(t, models, 1)

	// Get custom model ARN
	modelSummary := models[0].(map[string]any)
	modelARN := modelSummary["modelArn"].(string)
	modelParts := strings.Split(modelARN, "/")
	modelID := modelParts[len(modelParts)-1]

	// Get by ID
	resp3 := call(t, p, "GET", "/custom-models/"+modelID, "")
	assert.Equal(t, 200, resp3.StatusCode)

	// Delete
	resp4 := call(t, p, "DELETE", "/custom-models/"+modelID, "")
	assert.Equal(t, 204, resp4.StatusCode)
}

func TestGuardrailCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	body := `{"name":"my-guardrail","description":"test guardrail"}`
	resp := call(t, p, "POST", "/guardrails", body)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	guardrailID, ok := rb["guardrailId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, guardrailID)

	// Get
	resp2 := call(t, p, "GET", "/guardrails/"+guardrailID, "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-guardrail", rb2["name"])

	// List
	resp3 := call(t, p, "GET", "/guardrails", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	gs := rb3["guardrails"].([]any)
	assert.Len(t, gs, 1)

	// Update
	resp4 := call(t, p, "PUT", "/guardrails/"+guardrailID, `{"name":"updated-guardrail"}`)
	assert.Equal(t, 202, resp4.StatusCode)

	// Delete
	resp5 := call(t, p, "DELETE", "/guardrails/"+guardrailID, "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Get after delete - should be 404
	resp6 := call(t, p, "GET", "/guardrails/"+guardrailID, "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestTagging(t *testing.T) {
	p := newTestProvider(t)

	testARN := "arn:aws:bedrock:us-east-1:000000000000:guardrail/test123"

	// Tag
	resp := call(t, p, "POST", "/tags/"+testARN, `{"tags":{"env":"test","tier":"free"}}`)
	assert.Equal(t, 204, resp.StatusCode)

	// List
	resp2 := call(t, p, "GET", "/tags/"+testARN, "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags := rb2["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])

	// Untag
	resp3 := call(t, p, "DELETE", "/tags/"+testARN+"?tagKeys=env", "")
	assert.Equal(t, 204, resp3.StatusCode)
}
