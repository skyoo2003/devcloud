// SPDX-License-Identifier: Apache-2.0

// internal/services/codepipeline/provider_test.go
package codepipeline

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

func call(t *testing.T, p *Provider, op string, body string) *plugin.Response {
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

func TestPipelineCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreatePipeline
	createBody := `{"pipeline": {"name": "my-pipeline", "roleArn": "arn:aws:iam::000000000000:role/pipeline-role", "stages": [{"name": "Source", "actions": []}, {"name": "Build", "actions": []}]}}`
	resp := call(t, p, "CreatePipeline", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pl, ok := rb["pipeline"].(map[string]any)
	require.True(t, ok, "expected pipeline key")
	assert.Equal(t, "my-pipeline", pl["name"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/pipeline-role", pl["roleArn"])
	stages, _ := pl["stages"].([]any)
	assert.Len(t, stages, 2)

	// Create duplicate → error
	resp2 := call(t, p, "CreatePipeline", createBody)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "PipelineNameInUseException", rb2["__type"])

	// GetPipeline
	getResp := call(t, p, "GetPipeline", `{"name": "my-pipeline"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	pl2, _ := gb["pipeline"].(map[string]any)
	assert.Equal(t, "my-pipeline", pl2["name"])
	meta, _ := gb["metadata"].(map[string]any)
	assert.NotEmpty(t, meta["pipelineArn"])

	// GetPipeline non-existent
	getResp2 := call(t, p, "GetPipeline", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, getResp2.StatusCode)

	// CreatePipeline second
	call(t, p, "CreatePipeline", `{"pipeline": {"name": "second-pipeline", "roleArn": "arn:aws:iam::000000000000:role/role2"}}`)

	// ListPipelines
	listResp := call(t, p, "ListPipelines", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	pipelines, _ := lb["pipelines"].([]any)
	assert.Len(t, pipelines, 2)

	// UpdatePipeline
	updateBody := `{"pipeline": {"name": "my-pipeline", "roleArn": "arn:aws:iam::000000000000:role/new-role", "stages": [{"name": "Source", "actions": []}]}}`
	updateResp := call(t, p, "UpdatePipeline", updateBody)
	assert.Equal(t, 200, updateResp.StatusCode)
	ub := parseBody(t, updateResp)
	updPl, _ := ub["pipeline"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/new-role", updPl["roleArn"])
	assert.EqualValues(t, float64(2), updPl["version"])

	// UpdatePipeline non-existent
	updateResp2 := call(t, p, "UpdatePipeline", `{"pipeline": {"name": "does-not-exist"}}`)
	assert.Equal(t, 400, updateResp2.StatusCode)

	// GetPipelineState
	stateResp := call(t, p, "GetPipelineState", `{"name": "my-pipeline"}`)
	assert.Equal(t, 200, stateResp.StatusCode)
	sb := parseBody(t, stateResp)
	assert.Equal(t, "my-pipeline", sb["pipelineName"])
	stateStages, _ := sb["stageStates"].([]any)
	assert.Len(t, stateStages, 1)

	// DeletePipeline
	delResp := call(t, p, "DeletePipeline", `{"name": "my-pipeline"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// GetPipeline after delete
	getResp3 := call(t, p, "GetPipeline", `{"name": "my-pipeline"}`)
	assert.Equal(t, 400, getResp3.StatusCode)

	// DeletePipeline non-existent
	delResp2 := call(t, p, "DeletePipeline", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
}

func TestStartAndListExecutions(t *testing.T) {
	p := newTestProvider(t)

	// Create pipeline
	call(t, p, "CreatePipeline", `{"pipeline": {"name": "exec-pipeline", "roleArn": "arn:aws:iam::000000000000:role/role"}}`)

	// StartPipelineExecution
	startResp := call(t, p, "StartPipelineExecution", `{"name": "exec-pipeline"}`)
	assert.Equal(t, 200, startResp.StatusCode)
	sb := parseBody(t, startResp)
	execID, _ := sb["pipelineExecutionId"].(string)
	assert.NotEmpty(t, execID)

	// Start second execution
	startResp2 := call(t, p, "StartPipelineExecution", `{"name": "exec-pipeline"}`)
	assert.Equal(t, 200, startResp2.StatusCode)
	sb2 := parseBody(t, startResp2)
	execID2, _ := sb2["pipelineExecutionId"].(string)
	assert.NotEmpty(t, execID2)
	assert.NotEqual(t, execID, execID2)

	// StartPipelineExecution on non-existent pipeline
	startResp3 := call(t, p, "StartPipelineExecution", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, startResp3.StatusCode)

	// ListPipelineExecutions
	listResp := call(t, p, "ListPipelineExecutions", `{"pipelineName": "exec-pipeline"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	execs, _ := lb["pipelineExecutionSummaries"].([]any)
	assert.Len(t, execs, 2)

	// GetPipelineExecution
	getResp := call(t, p, "GetPipelineExecution", `{"pipelineName": "exec-pipeline", "pipelineExecutionId": "`+execID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	execMap, _ := gb["pipelineExecution"].(map[string]any)
	assert.Equal(t, execID, execMap["pipelineExecutionId"])
	assert.Equal(t, "InProgress", execMap["status"])

	// GetPipelineExecution non-existent
	getResp2 := call(t, p, "GetPipelineExecution", `{"pipelineName": "exec-pipeline", "pipelineExecutionId": "no-such-id"}`)
	assert.Equal(t, 400, getResp2.StatusCode)

	// StopPipelineExecution
	stopResp := call(t, p, "StopPipelineExecution", `{"pipelineName": "exec-pipeline", "pipelineExecutionId": "`+execID+`"}`)
	assert.Equal(t, 200, stopResp.StatusCode)
	stb := parseBody(t, stopResp)
	assert.Equal(t, execID, stb["pipelineExecutionId"])

	// Verify status changed
	getResp3 := call(t, p, "GetPipelineExecution", `{"pipelineName": "exec-pipeline", "pipelineExecutionId": "`+execID+`"}`)
	gb3 := parseBody(t, getResp3)
	execMap3, _ := gb3["pipelineExecution"].(map[string]any)
	assert.Equal(t, "Stopped", execMap3["status"])

	// StopPipelineExecution non-existent
	stopResp2 := call(t, p, "StopPipelineExecution", `{"pipelineName": "exec-pipeline", "pipelineExecutionId": "no-such-id"}`)
	assert.Equal(t, 400, stopResp2.StatusCode)
}

func TestWebhookCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create pipeline first
	call(t, p, "CreatePipeline", `{"pipeline": {"name": "webhook-pipeline", "roleArn": "arn:aws:iam::000000000000:role/role"}}`)

	// PutWebhook
	putBody := `{"webhook": {"name": "my-webhook", "targetPipeline": "webhook-pipeline", "targetAction": "Source", "filters": [{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"}], "authentication": "GITHUB_HMAC", "authenticationConfiguration": {"SecretToken": "my-secret"}}}`
	putResp := call(t, p, "PutWebhook", putBody)
	assert.Equal(t, 200, putResp.StatusCode)
	pb := parseBody(t, putResp)
	wh, ok := pb["webhook"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-webhook", wh["name"])
	assert.Equal(t, "webhook-pipeline", wh["targetPipeline"])
	assert.Equal(t, "Source", wh["targetAction"])
	filters, _ := wh["filters"].([]any)
	assert.Len(t, filters, 1)

	// PutWebhook again (upsert)
	putBody2 := `{"webhook": {"name": "my-webhook", "targetPipeline": "webhook-pipeline", "targetAction": "UpdatedSource", "filters": [], "authentication": "GITHUB_HMAC", "authenticationConfiguration": {}}}`
	putResp2 := call(t, p, "PutWebhook", putBody2)
	assert.Equal(t, 200, putResp2.StatusCode)
	pb2 := parseBody(t, putResp2)
	wh2, _ := pb2["webhook"].(map[string]any)
	assert.Equal(t, "UpdatedSource", wh2["targetAction"])

	// PutWebhook second webhook
	call(t, p, "PutWebhook", `{"webhook": {"name": "second-webhook", "targetPipeline": "webhook-pipeline", "targetAction": "Deploy", "filters": [], "authentication": "IP", "authenticationConfiguration": {}}}`)

	// ListWebhooks
	listResp := call(t, p, "ListWebhooks", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	webhooks, _ := lb["webhooks"].([]any)
	assert.Len(t, webhooks, 2)

	// DeleteWebhook
	delResp := call(t, p, "DeleteWebhook", `{"name": "my-webhook"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// ListWebhooks after delete
	listResp2 := call(t, p, "ListWebhooks", `{}`)
	lb2 := parseBody(t, listResp2)
	webhooks2, _ := lb2["webhooks"].([]any)
	assert.Len(t, webhooks2, 1)

	// DeleteWebhook non-existent
	delResp2 := call(t, p, "DeleteWebhook", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create pipeline with tags
	createBody := `{"pipeline": {"name": "tagged-pipeline", "roleArn": "arn:aws:iam::000000000000:role/role"}, "tags": [{"key": "Env", "value": "prod"}]}`
	resp := call(t, p, "CreatePipeline", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	pl := rb["pipeline"].(map[string]any)
	_ = pl

	// Need the ARN for tag operations
	getResp := call(t, p, "GetPipeline", `{"name": "tagged-pipeline"}`)
	gb := parseBody(t, getResp)
	meta := gb["metadata"].(map[string]any)
	arn, _ := meta["pipelineArn"].(string)
	require.NotEmpty(t, arn)

	// ListTagsForResource
	lr := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	tagsList, ok := lrb["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList, 1)

	// TagResource — add more tags
	tagBody := `{"resourceArn": "` + arn + `", "tags": [{"key": "Team", "value": "platform"}, {"key": "Owner", "value": "alice"}]}`
	tr := call(t, p, "TagResource", tagBody)
	assert.Equal(t, 200, tr.StatusCode)

	// Verify 3 tags
	lr2 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lrb2 := parseBody(t, lr2)
	tagsList2, _ := lrb2["tags"].([]any)
	assert.Len(t, tagsList2, 3)

	// UntagResource
	untagBody := `{"resourceArn": "` + arn + `", "tagKeys": ["Env", "Team"]}`
	utr := call(t, p, "UntagResource", untagBody)
	assert.Equal(t, 200, utr.StatusCode)

	// Verify only Owner remains
	lr3 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lrb3 := parseBody(t, lr3)
	tagsList3, _ := lrb3["tags"].([]any)
	assert.Len(t, tagsList3, 1)
	firstTag := tagsList3[0].(map[string]any)
	assert.Equal(t, "Owner", firstTag["key"])
	assert.Equal(t, "alice", firstTag["value"])

	// Unknown action returns empty success
	unknownResp := call(t, p, "GetActionType", `{}`)
	assert.Equal(t, 200, unknownResp.StatusCode)
}
