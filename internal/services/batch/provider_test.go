// SPDX-License-Identifier: Apache-2.0

// internal/services/batch/provider_test.go
package batch

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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callBatch(t *testing.T, p *Provider, op string, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/"+strings.ToLower(op), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func callBatchPath(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
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

func TestComputeEnvironmentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callBatch(t, p, "CreateComputeEnvironment", `{
		"computeEnvironmentName": "test-ce",
		"type": "MANAGED",
		"state": "ENABLED",
		"serviceRole": "arn:aws:iam::000000000000:role/batch-role"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "test-ce", rb["computeEnvironmentName"])
	arn, ok := rb["computeEnvironmentArn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, arn)

	// Duplicate create should fail
	resp2 := callBatch(t, p, "CreateComputeEnvironment", `{"computeEnvironmentName":"test-ce","type":"MANAGED"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	resp3 := callBatch(t, p, "DescribeComputeEnvironments", `{"computeEnvironments":["test-ce"]}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	ces, ok := rb3["computeEnvironments"].([]any)
	require.True(t, ok)
	require.Len(t, ces, 1)
	ce := ces[0].(map[string]any)
	assert.Equal(t, "test-ce", ce["computeEnvironmentName"])
	assert.Equal(t, "MANAGED", ce["type"])
	assert.Equal(t, "ENABLED", ce["state"])

	// Update
	resp4 := callBatch(t, p, "UpdateComputeEnvironment", `{
		"computeEnvironment": "test-ce",
		"state": "DISABLED"
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callBatch(t, p, "DescribeComputeEnvironments", `{"computeEnvironments":["test-ce"]}`)
	rb5 := parseBody(t, resp5)
	ces5 := rb5["computeEnvironments"].([]any)
	ce5 := ces5[0].(map[string]any)
	assert.Equal(t, "DISABLED", ce5["state"])

	// Delete
	resp6 := callBatch(t, p, "DeleteComputeEnvironment", `{"computeEnvironment":"test-ce"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete — should return empty
	resp7 := callBatch(t, p, "DescribeComputeEnvironments", `{"computeEnvironments":["test-ce"]}`)
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	ces7 := rb7["computeEnvironments"].([]any)
	assert.Len(t, ces7, 0)
}

func TestJobQueueCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create CE first
	callBatch(t, p, "CreateComputeEnvironment", `{"computeEnvironmentName":"ce-for-queue","type":"MANAGED"}`)

	// Create job queue
	resp := callBatch(t, p, "CreateJobQueue", `{
		"jobQueueName": "test-queue",
		"state": "ENABLED",
		"priority": 10,
		"computeEnvironmentOrder": [{"order":1,"computeEnvironment":"ce-for-queue"}]
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "test-queue", rb["jobQueueName"])
	jqARN, ok := rb["jobQueueArn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jqARN)

	// Duplicate
	resp2 := callBatch(t, p, "CreateJobQueue", `{"jobQueueName":"test-queue","priority":1}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	resp3 := callBatch(t, p, "DescribeJobQueues", `{"jobQueues":["test-queue"]}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jqs, ok := rb3["jobQueues"].([]any)
	require.True(t, ok)
	require.Len(t, jqs, 1)
	jq := jqs[0].(map[string]any)
	assert.Equal(t, "test-queue", jq["jobQueueName"])
	assert.Equal(t, float64(10), jq["priority"])

	// Update
	resp4 := callBatch(t, p, "UpdateJobQueue", `{"jobQueue":"test-queue","state":"DISABLED","priority":5}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callBatch(t, p, "DescribeJobQueues", `{"jobQueues":["test-queue"]}`)
	rb5 := parseBody(t, resp5)
	jqs5 := rb5["jobQueues"].([]any)
	jq5 := jqs5[0].(map[string]any)
	assert.Equal(t, "DISABLED", jq5["state"])
	assert.Equal(t, float64(5), jq5["priority"])

	// Delete
	resp6 := callBatch(t, p, "DeleteJobQueue", `{"jobQueue":"test-queue"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete — empty
	resp7 := callBatch(t, p, "DescribeJobQueues", `{"jobQueues":["test-queue"]}`)
	rb7 := parseBody(t, resp7)
	jqs7 := rb7["jobQueues"].([]any)
	assert.Len(t, jqs7, 0)
}

func TestJobDefinitionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Register
	resp := callBatch(t, p, "RegisterJobDefinition", `{
		"jobDefinitionName": "test-jd",
		"type": "container",
		"containerProperties": {"image":"my-image","vcpus":1,"memory":512}
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "test-jd", rb["jobDefinitionName"])
	assert.Equal(t, float64(1), rb["revision"])
	jdARN, ok := rb["jobDefinitionArn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jdARN)

	// Register again — revision should increment
	resp2 := callBatch(t, p, "RegisterJobDefinition", `{"jobDefinitionName":"test-jd","type":"container"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, float64(2), rb2["revision"])

	// Describe
	resp3 := callBatch(t, p, "DescribeJobDefinitions", `{"jobDefinitionName":"test-jd"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jds, ok := rb3["jobDefinitions"].([]any)
	require.True(t, ok)
	assert.Len(t, jds, 2) // both revisions

	// Deregister
	resp4 := callBatch(t, p, "DeregisterJobDefinition", `{"jobDefinition":"test-jd"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Describe after deregister — should show INACTIVE
	resp5 := callBatch(t, p, "DescribeJobDefinitions", `{"jobDefinitionName":"test-jd","status":"INACTIVE"}`)
	rb5 := parseBody(t, resp5)
	jds5 := rb5["jobDefinitions"].([]any)
	for _, item := range jds5 {
		jd := item.(map[string]any)
		assert.Equal(t, "INACTIVE", jd["status"])
	}
}

func TestSubmitAndDescribeJob(t *testing.T) {
	p := newTestProvider(t)

	// Register a job definition
	callBatch(t, p, "RegisterJobDefinition", `{"jobDefinitionName":"my-jd","type":"container"}`)

	// Submit a job
	resp := callBatch(t, p, "SubmitJob", `{
		"jobName": "my-job",
		"jobQueue": "my-queue",
		"jobDefinition": "my-jd"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-job", rb["jobName"])
	jobID, ok := rb["jobId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, jobID)

	// Describe job
	descBody, _ := json.Marshal(map[string]any{"jobs": []string{jobID}})
	resp2 := callBatch(t, p, "DescribeJobs", string(descBody))
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	jobs, ok := rb2["jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 1)
	job := jobs[0].(map[string]any)
	assert.Equal(t, "my-job", job["jobName"])
	assert.Equal(t, "SUCCEEDED", job["status"])

	// List jobs
	resp3 := callBatch(t, p, "ListJobs", `{"jobQueue":"my-queue"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	summaries, ok := rb3["jobSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	// Cancel job
	cancelBody, _ := json.Marshal(map[string]any{"jobId": jobID, "reason": "test cancel"})
	resp4 := callBatch(t, p, "CancelJob", string(cancelBody))
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify status
	resp5 := callBatch(t, p, "DescribeJobs", string(descBody))
	rb5 := parseBody(t, resp5)
	jobs5 := rb5["jobs"].([]any)
	job5 := jobs5[0].(map[string]any)
	assert.Equal(t, "CANCELLED", job5["status"])
}

func TestSchedulingPolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callBatch(t, p, "CreateSchedulingPolicy", `{
		"name": "test-sp",
		"fairsharePolicy": {"computeReservation":10,"shareDecaySeconds":300}
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "test-sp", rb["name"])
	spARN, ok := rb["arn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, spARN)

	// Duplicate
	resp2 := callBatch(t, p, "CreateSchedulingPolicy", `{"name":"test-sp"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	descBody, _ := json.Marshal(map[string]any{"arns": []string{spARN}})
	resp3 := callBatch(t, p, "DescribeSchedulingPolicies", string(descBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	sps, ok := rb3["schedulingPolicies"].([]any)
	require.True(t, ok)
	require.Len(t, sps, 1)
	sp := sps[0].(map[string]any)
	assert.Equal(t, "test-sp", sp["name"])

	// List
	resp4 := callBatch(t, p, "ListSchedulingPolicies", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	list, ok := rb4["schedulingPolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 1)

	// Update
	updateBody, _ := json.Marshal(map[string]any{
		"arn":             spARN,
		"fairsharePolicy": map[string]any{"computeReservation": 20, "shareDecaySeconds": 600},
	})
	resp5 := callBatch(t, p, "UpdateSchedulingPolicy", string(updateBody))
	assert.Equal(t, 200, resp5.StatusCode)

	// Delete
	deleteBody, _ := json.Marshal(map[string]any{"arn": spARN})
	resp6 := callBatch(t, p, "DeleteSchedulingPolicy", string(deleteBody))
	assert.Equal(t, 200, resp6.StatusCode)

	// List after delete
	resp7 := callBatch(t, p, "ListSchedulingPolicies", `{}`)
	rb7 := parseBody(t, resp7)
	list7 := rb7["schedulingPolicies"].([]any)
	assert.Len(t, list7, 0)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a compute environment to tag
	createResp := callBatch(t, p, "CreateComputeEnvironment", `{"computeEnvironmentName":"tagged-ce","type":"MANAGED"}`)
	rb := parseBody(t, createResp)
	arn := rb["computeEnvironmentArn"].(string)
	require.NotEmpty(t, arn)

	// Tag via POST /v1/tags/{resourceArn}
	tagBody, _ := json.Marshal(map[string]any{
		"tags": map[string]string{"Env": "prod", "Team": "data"},
	})
	tagResp := callBatchPath(t, p, "POST", "/v1/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, tagResp.StatusCode)

	// ListTagsForResource via GET /v1/tags/{resourceArn}
	listResp := callBatchPath(t, p, "GET", "/v1/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, listResp.StatusCode)
	rb2 := parseBody(t, listResp)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "data", tags["Team"])

	// UntagResource via DELETE /v1/tags/{resourceArn}?tagKeys=Env
	untagReq := httptest.NewRequest("DELETE", "/v1/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", untagReq)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// List again — should have 1 tag left
	listResp2 := callBatchPath(t, p, "GET", "/v1/tags/"+arn, "ListTagsForResource", "")
	rb3 := parseBody(t, listResp2)
	tags3 := rb3["tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "data", tags3["Team"])
}
