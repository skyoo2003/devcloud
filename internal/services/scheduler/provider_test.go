// SPDX-License-Identifier: Apache-2.0

// internal/services/scheduler/provider_test.go
package scheduler

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

func newTestProvider(t *testing.T) *SchedulerProvider {
	t.Helper()
	p := &SchedulerProvider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *SchedulerProvider, method, path, body string) *plugin.Response {
	t.Helper()
	var bodyStr string
	if body != "" {
		bodyStr = body
	} else {
		bodyStr = "{}"
	}
	req := httptest.NewRequest(method, path, strings.NewReader(bodyStr))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestScheduleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create schedule
	resp := callREST(t, p, "POST", "/schedules/my-schedule", `{
		"ScheduleExpression": "rate(5 minutes)",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-func", "RoleArn": "arn:aws:iam::123:role/test"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ScheduleArn"], "arn:aws:")

	// Get schedule
	resp2 := callREST(t, p, "GET", "/schedules/my-schedule", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-schedule", m2["Name"])
	assert.Equal(t, "rate(5 minutes)", m2["ScheduleExpression"])

	// Update schedule
	resp3 := callREST(t, p, "PATCH", "/schedules/my-schedule", `{
		"ScheduleExpression": "rate(10 minutes)",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-func", "RoleArn": "arn:aws:iam::123:role/test"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callREST(t, p, "GET", "/schedules/my-schedule", "")
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "rate(10 minutes)", m4["ScheduleExpression"])

	// List schedules
	resp5 := callREST(t, p, "GET", "/schedules", "")
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	schedules := m5["Schedules"].([]any)
	assert.Len(t, schedules, 1)

	// Delete schedule
	resp6 := callREST(t, p, "DELETE", "/schedules/my-schedule", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Get should fail
	resp7 := callREST(t, p, "GET", "/schedules/my-schedule", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestScheduleGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	resp := callREST(t, p, "POST", "/schedule-groups/my-group", `{
		"Tags": {"env": "test"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ScheduleGroupArn"], "arn:aws:")

	// Get group
	resp2 := callREST(t, p, "GET", "/schedule-groups/my-group", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-group", m2["Name"])
	assert.Equal(t, "ACTIVE", m2["State"])

	// List groups
	resp3 := callREST(t, p, "GET", "/schedule-groups", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	groups := m3["ScheduleGroups"].([]any)
	assert.NotEmpty(t, groups)

	// Delete group
	resp4 := callREST(t, p, "DELETE", "/schedule-groups/my-group", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Get should fail
	resp5 := callREST(t, p, "GET", "/schedule-groups/my-group", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestScheduleInGroup(t *testing.T) {
	p := newTestProvider(t)

	// Create a schedule in a specific group
	resp := callREST(t, p, "POST", "/schedules/grp-group/grp-schedule", `{
		"ScheduleExpression": "cron(0 12 * * ? *)",
		"GroupName": "grp-group",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123:function:fn", "RoleArn": "arn:aws:iam::123:role/test"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get it back
	resp2 := callREST(t, p, "GET", "/schedules/grp-group/grp-schedule", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "grp-schedule", m2["Name"])
}

func TestEnableDisableSchedule(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/schedules/ed-schedule", `{
		"ScheduleExpression": "rate(5 minutes)",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123:function:fn", "RoleArn": "arn:aws:iam::123:role/test"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`)

	// Disable
	resp := callREST(t, p, "POST", "/schedules/default/ed-schedule/disable", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Get
	resp2 := callREST(t, p, "GET", "/schedules/ed-schedule", "")
	m := parseJSON(t, resp2)
	assert.Equal(t, "DISABLED", m["State"])

	// Enable
	resp3 := callREST(t, p, "POST", "/schedules/default/ed-schedule/enable", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// Get
	resp4 := callREST(t, p, "GET", "/schedules/ed-schedule", "")
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "ENABLED", m4["State"])
}

func TestRateLimit(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:scheduler:us-east-1:000000000000:schedule-group/default"

	// Put rate limit
	resp := callREST(t, p, "PUT", "/rate-limits/"+arn, `{"MaxCalls": 500, "TimeWindow": "1h"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get rate limit
	resp2 := callREST(t, p, "GET", "/rate-limits/"+arn, "")
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.Equal(t, float64(500), m["MaxCalls"])

	// List
	resp3 := callREST(t, p, "GET", "/rate-limits", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// Batch check
	req := httptest.NewRequest("POST", "/rate-limits", strings.NewReader(`{"ResourceArns": ["`+arn+`"]}`))
	resp4, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp4.StatusCode)

	// Delete
	resp5 := callREST(t, p, "DELETE", "/rate-limits/"+arn, "")
	assert.Equal(t, 200, resp5.StatusCode)
}

func TestBatchOperations(t *testing.T) {
	p := newTestProvider(t)

	// Batch create
	resp := callREST(t, p, "POST", "/batch/create-schedules", `{
		"Schedules": [
			{"Name": "b1", "ScheduleExpression": "rate(1 minute)", "Target": {}, "FlexibleTimeWindow": {"Mode": "OFF"}},
			{"Name": "b2", "ScheduleExpression": "rate(2 minutes)", "Target": {}, "FlexibleTimeWindow": {"Mode": "OFF"}}
		]
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	created := m["Created"].([]any)
	assert.Len(t, created, 2)

	// Batch update
	resp2 := callREST(t, p, "POST", "/batch/update-schedules", `{
		"Schedules": [
			{"Name": "b1", "ScheduleExpression": "rate(10 minutes)"}
		]
	}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// List fleet
	resp3 := callREST(t, p, "GET", "/fleet-schedules", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	schedules := m3["Schedules"].([]any)
	assert.GreaterOrEqual(t, len(schedules), 2)

	// Batch delete
	resp4 := callREST(t, p, "POST", "/batch/delete-schedules", `{
		"Schedules": [{"Name": "b1"}, {"Name": "b2"}]
	}`)
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestValidateAndPreview(t *testing.T) {
	p := newTestProvider(t)

	// Validate
	resp := callREST(t, p, "POST", "/validate", `{"ScheduleExpression": "rate(5 minutes)"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, true, m["Valid"])

	// Invalid expression
	resp2 := callREST(t, p, "POST", "/validate", `{"ScheduleExpression": "bogus"}`)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, false, m2["Valid"])

	// Preview
	resp3 := callREST(t, p, "POST", "/preview", `{"ScheduleExpression": "rate(5 minutes)"}`)
	assert.Equal(t, 200, resp3.StatusCode)
}

func TestScheduleMetricsAndLock(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/schedules/mt-schedule", `{
		"ScheduleExpression": "rate(5 minutes)",
		"Target": {},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`)

	// Lock/unlock need custom op since resolveOp won't map these paths
	req := httptest.NewRequest("POST", "/schedules/default/mt-schedule/lock", strings.NewReader("{}"))
	respL, _ := p.HandleRequest(context.Background(), "LockSchedule", req)
	assert.Equal(t, 200, respL.StatusCode)

	req2 := httptest.NewRequest("POST", "/schedules/default/mt-schedule/unlock", strings.NewReader("{}"))
	respU, _ := p.HandleRequest(context.Background(), "UnlockSchedule", req2)
	assert.Equal(t, 200, respU.StatusCode)

	// Execution status
	req3 := httptest.NewRequest("GET", "/schedules/default/mt-schedule/execution-status", strings.NewReader("{}"))
	respE, _ := p.HandleRequest(context.Background(), "GetScheduleExecutionStatus", req3)
	assert.Equal(t, 200, respE.StatusCode)
}

func TestUpdateScheduleGroup(t *testing.T) {
	p := newTestProvider(t)

	// Create a group
	callREST(t, p, "POST", "/schedule-groups/update-grp", `{}`)

	// Update (using resolveOp on PATCH)
	resp := callREST(t, p, "PATCH", "/schedule-groups/update-grp", `{"State": "ACTIVE"}`)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestTagResource(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:scheduler:us-east-1:123456789012:schedule/default/test"

	// Tag resource
	resp := callREST(t, p, "POST", "/tags/"+arn, `{"Tags": {"env": "prod"}}`)
	assert.Equal(t, 200, resp.StatusCode)

	// List tags
	resp2 := callREST(t, p, "GET", "/tags/"+arn, "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	tags := m2["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
}
