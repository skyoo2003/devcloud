// SPDX-License-Identifier: Apache-2.0

// internal/services/sfn/provider_test.go
package sfn

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

func callSFN(t *testing.T, p *Provider, action, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonStepFunctions."+action)
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

// ---- Tests ----

func TestStateMachineCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callSFN(t, p, "CreateStateMachine", `{
		"name": "my-sm",
		"definition": "{\"Comment\":\"test\"}",
		"roleArn": "arn:aws:iam::000000000000:role/test",
		"type": "STANDARD"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	smARN, _ := b["stateMachineArn"].(string)
	assert.NotEmpty(t, smARN)

	// Describe
	resp2 := callSFN(t, p, "DescribeStateMachine", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	assert.Equal(t, "my-sm", b2["name"])
	assert.Equal(t, "STANDARD", b2["type"])
	assert.Equal(t, "ACTIVE", b2["status"])

	// List
	resp3 := callSFN(t, p, "ListStateMachines", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	sms, _ := b3["stateMachines"].([]any)
	assert.Len(t, sms, 1)

	// Update
	resp4 := callSFN(t, p, "UpdateStateMachine", `{
		"stateMachineArn":"`+smARN+`",
		"definition": "{\"Comment\":\"updated\"}"
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify updated definition
	resp5 := callSFN(t, p, "DescribeStateMachine", `{"stateMachineArn":"`+smARN+`"}`)
	b5 := parseBody(t, resp5)
	assert.Equal(t, `{"Comment":"updated"}`, b5["definition"])

	// Delete
	resp6 := callSFN(t, p, "DeleteStateMachine", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callSFN(t, p, "DescribeStateMachine", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 400, resp7.StatusCode)

	// Duplicate create
	resp8 := callSFN(t, p, "CreateStateMachine", `{"name":"dup-sm","definition":"{}","roleArn":""}`)
	assert.Equal(t, 200, resp8.StatusCode)
	resp9 := callSFN(t, p, "CreateStateMachine", `{"name":"dup-sm","definition":"{}","roleArn":""}`)
	assert.Equal(t, 400, resp9.StatusCode)
}

func TestStartAndDescribeExecution(t *testing.T) {
	p := newTestProvider(t)

	// Create SM
	cr := callSFN(t, p, "CreateStateMachine", `{"name":"exec-sm","definition":"{}","roleArn":""}`)
	smARN := parseBody(t, cr)["stateMachineArn"].(string)

	// StartExecution
	resp := callSFN(t, p, "StartExecution", `{
		"stateMachineArn":"`+smARN+`",
		"name":"exec-1",
		"input":"{\"key\":\"val\"}"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	execARN, _ := b["executionArn"].(string)
	assert.NotEmpty(t, execARN)
	assert.Contains(t, execARN, "exec-sm")
	assert.Contains(t, execARN, "exec-1")

	// DescribeExecution
	resp2 := callSFN(t, p, "DescribeExecution", `{"executionArn":"`+execARN+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	assert.Equal(t, "SUCCEEDED", b2["status"])
	assert.Equal(t, `{"key":"val"}`, b2["input"])
	assert.Equal(t, execARN, b2["executionArn"])

	// StartSyncExecution
	resp3 := callSFN(t, p, "StartSyncExecution", `{
		"stateMachineArn":"`+smARN+`",
		"input":"{}"
	}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	assert.Equal(t, "SUCCEEDED", b3["status"])

	// GetExecutionHistory
	resp4 := callSFN(t, p, "GetExecutionHistory", `{"executionArn":"`+execARN+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	b4 := parseBody(t, resp4)
	events, _ := b4["events"].([]any)
	assert.Len(t, events, 2)

	// DescribeStateMachineForExecution
	resp5 := callSFN(t, p, "DescribeStateMachineForExecution", `{"executionArn":"`+execARN+`"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	b5 := parseBody(t, resp5)
	assert.Equal(t, smARN, b5["stateMachineArn"])
}

func TestListExecutions(t *testing.T) {
	p := newTestProvider(t)

	cr := callSFN(t, p, "CreateStateMachine", `{"name":"list-sm","definition":"{}","roleArn":""}`)
	smARN := parseBody(t, cr)["stateMachineArn"].(string)

	callSFN(t, p, "StartExecution", `{"stateMachineArn":"`+smARN+`","name":"e1","input":"{}"}`)
	callSFN(t, p, "StartExecution", `{"stateMachineArn":"`+smARN+`","name":"e2","input":"{}"}`)
	callSFN(t, p, "StartExecution", `{"stateMachineArn":"`+smARN+`","name":"e3","input":"{}"}`)

	resp := callSFN(t, p, "ListExecutions", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	execs, _ := b["executions"].([]any)
	assert.Len(t, execs, 3)

	// Filter by status
	resp2 := callSFN(t, p, "ListExecutions", `{"stateMachineArn":"`+smARN+`","statusFilter":"SUCCEEDED"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	execs2, _ := b2["executions"].([]any)
	assert.Len(t, execs2, 3)
}

func TestStopExecution(t *testing.T) {
	p := newTestProvider(t)

	cr := callSFN(t, p, "CreateStateMachine", `{"name":"stop-sm","definition":"{}","roleArn":""}`)
	smARN := parseBody(t, cr)["stateMachineArn"].(string)

	sr := callSFN(t, p, "StartExecution", `{"stateMachineArn":"`+smARN+`","name":"stop-exec","input":"{}"}`)
	execARN := parseBody(t, sr)["executionArn"].(string)

	// Stop
	resp := callSFN(t, p, "StopExecution", `{"executionArn":"`+execARN+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	assert.NotNil(t, b["stopDate"])

	// Verify status
	resp2 := callSFN(t, p, "DescribeExecution", `{"executionArn":"`+execARN+`"}`)
	b2 := parseBody(t, resp2)
	assert.Equal(t, "ABORTED", b2["status"])

	// Stop non-existent
	resp3 := callSFN(t, p, "StopExecution", `{"executionArn":"arn:aws:states:us-east-1:000000000000:execution:fake:fake"}`)
	assert.Equal(t, 400, resp3.StatusCode)
}

func TestActivityCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callSFN(t, p, "CreateActivity", `{"name":"my-activity"}`)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	actARN, _ := b["activityArn"].(string)
	assert.NotEmpty(t, actARN)
	assert.Contains(t, actARN, "my-activity")

	// Describe
	resp2 := callSFN(t, p, "DescribeActivity", `{"activityArn":"`+actARN+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	assert.Equal(t, "my-activity", b2["name"])

	// List
	resp3 := callSFN(t, p, "ListActivities", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	acts, _ := b3["activities"].([]any)
	assert.Len(t, acts, 1)

	// GetActivityTask
	resp4 := callSFN(t, p, "GetActivityTask", `{"activityArn":"`+actARN+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	b4 := parseBody(t, resp4)
	assert.NotEmpty(t, b4["taskToken"])

	// Delete
	resp5 := callSFN(t, p, "DeleteActivity", `{"activityArn":"`+actARN+`"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify deleted
	resp6 := callSFN(t, p, "DescribeActivity", `{"activityArn":"`+actARN+`"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestStateMachineAliasCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create SM and publish version
	cr := callSFN(t, p, "CreateStateMachine", `{"name":"alias-sm","definition":"{}","roleArn":""}`)
	smARN := parseBody(t, cr)["stateMachineArn"].(string)

	pvr := callSFN(t, p, "PublishStateMachineVersion", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 200, pvr.StatusCode)
	pvb := parseBody(t, pvr)
	versionARN, _ := pvb["stateMachineVersionArn"].(string)
	assert.NotEmpty(t, versionARN)

	// Create alias
	createBody := `{
		"name":"prod",
		"routingConfiguration":[{"stateMachineVersionArn":"` + versionARN + `","weight":100}]
	}`
	resp := callSFN(t, p, "CreateStateMachineAlias", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	b := parseBody(t, resp)
	aliasARN, _ := b["stateMachineAliasArn"].(string)
	assert.NotEmpty(t, aliasARN)

	// Describe alias
	resp2 := callSFN(t, p, "DescribeStateMachineAlias", `{"stateMachineAliasArn":"`+aliasARN+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	assert.Equal(t, "prod", b2["name"])

	// List aliases
	resp3 := callSFN(t, p, "ListStateMachineAliases", `{"stateMachineArn":"`+smARN+`"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	b3 := parseBody(t, resp3)
	aliases, _ := b3["stateMachineAliases"].([]any)
	assert.Len(t, aliases, 1)

	// Update alias
	resp4 := callSFN(t, p, "UpdateStateMachineAlias", `{
		"stateMachineAliasArn":"`+aliasARN+`",
		"description":"updated desc"
	}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Delete alias
	resp5 := callSFN(t, p, "DeleteStateMachineAlias", `{"stateMachineAliasArn":"`+aliasARN+`"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify deleted
	resp6 := callSFN(t, p, "DescribeStateMachineAlias", `{"stateMachineAliasArn":"`+aliasARN+`"}`)
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create SM
	cr := callSFN(t, p, "CreateStateMachine", `{"name":"tag-sm","definition":"{}","roleArn":""}`)
	smARN := parseBody(t, cr)["stateMachineArn"].(string)

	// TagResource
	resp := callSFN(t, p, "TagResource", `{
		"resourceArn":"`+smARN+`",
		"tags":[{"key":"env","value":"test"},{"key":"team","value":"platform"}]
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callSFN(t, p, "ListTagsForResource", `{"resourceArn":"`+smARN+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	b2 := parseBody(t, resp2)
	tags, _ := b2["tags"].([]any)
	assert.Len(t, tags, 2)

	// UntagResource
	resp3 := callSFN(t, p, "UntagResource", `{"resourceArn":"`+smARN+`","tagKeys":["env"]}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify one tag removed
	resp4 := callSFN(t, p, "ListTagsForResource", `{"resourceArn":"`+smARN+`"}`)
	b4 := parseBody(t, resp4)
	tags4, _ := b4["tags"].([]any)
	assert.Len(t, tags4, 1)
	tag0, _ := tags4[0].(map[string]any)
	assert.Equal(t, "team", tag0["key"])
}
