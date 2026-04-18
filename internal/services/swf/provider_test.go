// SPDX-License-Identifier: Apache-2.0

// internal/services/swf/provider_test.go
package swf

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

func callSWF(t *testing.T, p *Provider, action, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService."+action)
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

func TestDomainCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Register
	resp := callSWF(t, p, "RegisterDomain", `{
		"name": "test-domain",
		"workflowExecutionRetentionPeriodInDays": "90",
		"description": "A test domain"
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Describe
	resp2 := callSWF(t, p, "DescribeDomain", `{"name":"test-domain"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	info := b2["domainInfo"].(map[string]any)
	assert.Equal(t, "test-domain", info["name"])
	assert.Equal(t, "REGISTERED", info["status"])
	assert.Equal(t, "A test domain", info["description"])

	// List (REGISTERED)
	resp3 := callSWF(t, p, "ListDomains", `{"registrationStatus":"REGISTERED"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	domains, _ := b3["domainInfos"].([]any)
	assert.Len(t, domains, 1)

	// Deprecate
	resp4 := callSWF(t, p, "DeprecateDomain", `{"name":"test-domain"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify deprecated
	resp5 := callSWF(t, p, "DescribeDomain", `{"name":"test-domain"}`)
	b5 := parseBody(t, resp5)
	info5 := b5["domainInfo"].(map[string]any)
	assert.Equal(t, "DEPRECATED", info5["status"])

	// Double deprecate fails
	resp6 := callSWF(t, p, "DeprecateDomain", `{"name":"test-domain"}`)
	assert.Equal(t, 400, resp6.StatusCode)

	// Undeprecate
	resp7 := callSWF(t, p, "UndeprecateDomain", `{"name":"test-domain"}`)
	assert.Equal(t, 200, resp7.StatusCode)

	// Verify back to REGISTERED
	resp8 := callSWF(t, p, "DescribeDomain", `{"name":"test-domain"}`)
	b8 := parseBody(t, resp8)
	info8 := b8["domainInfo"].(map[string]any)
	assert.Equal(t, "REGISTERED", info8["status"])

	// Duplicate register
	resp9 := callSWF(t, p, "RegisterDomain", `{"name":"test-domain","workflowExecutionRetentionPeriodInDays":"30"}`)
	assert.Equal(t, 400, resp9.StatusCode)
}

func TestWorkflowTypeCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Register workflow type
	resp := callSWF(t, p, "RegisterWorkflowType", `{
		"domain": "my-domain",
		"name": "MyWorkflow",
		"version": "1.0",
		"description": "test workflow"
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Describe
	resp2 := callSWF(t, p, "DescribeWorkflowType", `{
		"domain": "my-domain",
		"workflowType": {"name": "MyWorkflow", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	typeInfo := b2["typeInfo"].(map[string]any)
	assert.Equal(t, "REGISTERED", typeInfo["status"])
	wfType := typeInfo["workflowType"].(map[string]any)
	assert.Equal(t, "MyWorkflow", wfType["name"])
	assert.Equal(t, "1.0", wfType["version"])

	// List
	resp3 := callSWF(t, p, "ListWorkflowTypes", `{"domain":"my-domain","registrationStatus":"REGISTERED"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	types, _ := b3["typeInfos"].([]any)
	assert.Len(t, types, 1)

	// Deprecate
	resp4 := callSWF(t, p, "DeprecateWorkflowType", `{
		"domain": "my-domain",
		"workflowType": {"name": "MyWorkflow", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Delete (only allowed when DEPRECATED)
	resp5 := callSWF(t, p, "DeleteWorkflowType", `{
		"domain": "my-domain",
		"workflowType": {"name": "MyWorkflow", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify deleted
	resp6 := callSWF(t, p, "DescribeWorkflowType", `{
		"domain": "my-domain",
		"workflowType": {"name": "MyWorkflow", "version": "1.0"}
	}`)
	assert.Equal(t, 400, resp6.StatusCode)

	// Duplicate register
	callSWF(t, p, "RegisterWorkflowType", `{"domain":"my-domain","name":"WF2","version":"1.0"}`)
	resp7 := callSWF(t, p, "RegisterWorkflowType", `{"domain":"my-domain","name":"WF2","version":"1.0"}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestActivityTypeCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Register
	resp := callSWF(t, p, "RegisterActivityType", `{
		"domain": "my-domain",
		"name": "MyActivity",
		"version": "1.0"
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Describe
	resp2 := callSWF(t, p, "DescribeActivityType", `{
		"domain": "my-domain",
		"activityType": {"name": "MyActivity", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	typeInfo := b2["typeInfo"].(map[string]any)
	assert.Equal(t, "REGISTERED", typeInfo["status"])

	// List
	resp3 := callSWF(t, p, "ListActivityTypes", `{"domain":"my-domain","registrationStatus":"REGISTERED"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	types, _ := b3["typeInfos"].([]any)
	assert.Len(t, types, 1)

	// Undeprecate before deprecate (should still work - just sets to REGISTERED)
	resp4 := callSWF(t, p, "UndeprecateActivityType", `{
		"domain": "my-domain",
		"activityType": {"name": "MyActivity", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Deprecate
	resp5 := callSWF(t, p, "DeprecateActivityType", `{
		"domain": "my-domain",
		"activityType": {"name": "MyActivity", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Delete
	resp6 := callSWF(t, p, "DeleteActivityType", `{
		"domain": "my-domain",
		"activityType": {"name": "MyActivity", "version": "1.0"}
	}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callSWF(t, p, "DescribeActivityType", `{
		"domain": "my-domain",
		"activityType": {"name": "MyActivity", "version": "1.0"}
	}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestWorkflowExecution(t *testing.T) {
	p := newTestProvider(t)

	// Start
	resp := callSWF(t, p, "StartWorkflowExecution", `{
		"domain": "test-domain",
		"workflowId": "wf-001",
		"workflowType": {"name": "MyWorkflow", "version": "1.0"},
		"input": "{\"key\":\"value\"}"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	runID, _ := b["runId"].(string)
	assert.NotEmpty(t, runID)

	// Describe
	resp2 := callSWF(t, p, "DescribeWorkflowExecution", `{
		"domain": "test-domain",
		"execution": {"workflowId": "wf-001", "runId": "`+runID+`"}
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	execInfo := b2["executionInfo"].(map[string]any)
	assert.Equal(t, "OPEN", execInfo["executionStatus"])

	// List open
	resp3 := callSWF(t, p, "ListOpenWorkflowExecutions", `{
		"domain": "test-domain",
		"startTimeFilter": {"oldestDate": 0}
	}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	execInfos, _ := b3["executionInfos"].([]any)
	assert.Len(t, execInfos, 1)

	// Get history
	resp4 := callSWF(t, p, "GetWorkflowExecutionHistory", `{
		"domain": "test-domain",
		"execution": {"workflowId": "wf-001", "runId": "`+runID+`"}
	}`)
	assert.Equal(t, 200, resp4.StatusCode)
	b4 := parseBody(t, resp4)
	events, _ := b4["events"].([]any)
	assert.Len(t, events, 2)

	// Terminate
	resp5 := callSWF(t, p, "TerminateWorkflowExecution", `{
		"domain": "test-domain",
		"workflowId": "wf-001",
		"runId": "`+runID+`",
		"reason": "test termination"
	}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// List closed
	resp6 := callSWF(t, p, "ListClosedWorkflowExecutions", `{
		"domain": "test-domain",
		"startTimeFilter": {"oldestDate": 0}
	}`)
	assert.Equal(t, 200, resp6.StatusCode)
	b6 := parseBody(t, resp6)
	closedInfos, _ := b6["executionInfos"].([]any)
	assert.Len(t, closedInfos, 1)

	// Count open (should be 0 now)
	resp7 := callSWF(t, p, "CountOpenWorkflowExecutions", `{
		"domain": "test-domain",
		"startTimeFilter": {"oldestDate": 0}
	}`)
	assert.Equal(t, 200, resp7.StatusCode)
	b7 := parseBody(t, resp7)
	assert.Equal(t, float64(0), b7["count"])

	// Count closed
	resp8 := callSWF(t, p, "CountClosedWorkflowExecutions", `{
		"domain": "test-domain",
		"startTimeFilter": {"oldestDate": 0}
	}`)
	assert.Equal(t, 200, resp8.StatusCode)
	b8 := parseBody(t, resp8)
	assert.Equal(t, float64(1), b8["count"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Register domain to get an ARN
	callSWF(t, p, "RegisterDomain", `{"name":"tag-domain","workflowExecutionRetentionPeriodInDays":"30"}`)
	arn := buildDomainARN("tag-domain")

	// TagResource
	resp := callSWF(t, p, "TagResource", `{
		"resourceArn": "`+arn+`",
		"tags": [{"key":"env","value":"test"},{"key":"team","value":"platform"}]
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callSWF(t, p, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	tags, _ := b2["tags"].([]any)
	assert.Len(t, tags, 2)

	// UntagResource
	resp3 := callSWF(t, p, "UntagResource", `{"resourceArn":"`+arn+`","tagKeys":["env"]}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify one tag removed
	resp4 := callSWF(t, p, "ListTagsForResource", `{"resourceArn":"`+arn+`"}`)
	b4 := parseBody(t, resp4)
	tags4, _ := b4["tags"].([]any)
	assert.Len(t, tags4, 1)
	tag0, _ := tags4[0].(map[string]any)
	assert.Equal(t, "team", tag0["key"])
	assert.Equal(t, "platform", tag0["value"])
}
