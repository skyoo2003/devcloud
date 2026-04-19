// SPDX-License-Identifier: Apache-2.0

// internal/services/eventbridge/provider_test.go
package eventbridge

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

func call(t *testing.T, p *Provider, action, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSEvents."+action)
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

func TestCreateEventBusAndRule(t *testing.T) {
	p := newTestProvider(t)

	// Create a custom bus.
	resp := call(t, p, "CreateEventBus", `{"Name":"my-bus"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["EventBusArn"].(string), "my-bus")

	// List buses — default + my-bus.
	listResp := call(t, p, "ListEventBuses", `{}`)
	lm := parseJSON(t, listResp)
	buses := lm["EventBuses"].([]any)
	names := make([]string, 0, len(buses))
	for _, b := range buses {
		names = append(names, b.(map[string]any)["Name"].(string))
	}
	assert.Contains(t, names, "default")
	assert.Contains(t, names, "my-bus")

	// Put a rule on the default bus.
	ruleResp := call(t, p, "PutRule", `{"Name":"my-rule","EventBusName":"default","EventPattern":"{\"source\":[\"com.example\"]}","State":"ENABLED"}`)
	assert.Equal(t, 200, ruleResp.StatusCode)
	rm := parseJSON(t, ruleResp)
	assert.Contains(t, rm["RuleArn"].(string), "my-rule")

	// List rules.
	listRulesResp := call(t, p, "ListRules", `{"EventBusName":"default"}`)
	lrm := parseJSON(t, listRulesResp)
	rules := lrm["Rules"].([]any)
	ruleNames := make([]string, 0)
	for _, r := range rules {
		ruleNames = append(ruleNames, r.(map[string]any)["Name"].(string))
	}
	assert.Contains(t, ruleNames, "my-rule")
}

func TestPutTargetsAndEvents(t *testing.T) {
	p := newTestProvider(t)

	call(t, p, "PutRule", `{"Name":"target-rule","EventBusName":"default","State":"ENABLED"}`)

	targetsResp := call(t, p, "PutTargets", `{"Rule":"target-rule","EventBusName":"default","Targets":[{"Id":"t1","Arn":"arn:aws:sqs:us-east-1:000000000000:my-queue"}]}`)
	assert.Equal(t, 200, targetsResp.StatusCode)
	tm := parseJSON(t, targetsResp)
	assert.Equal(t, float64(0), tm["FailedEntryCount"])

	listTargetsResp := call(t, p, "ListTargetsByRule", `{"Rule":"target-rule","EventBusName":"default"}`)
	ltm := parseJSON(t, listTargetsResp)
	targets := ltm["Targets"].([]any)
	require.Len(t, targets, 1)
	assert.Equal(t, "t1", targets[0].(map[string]any)["Id"])

	eventsResp := call(t, p, "PutEvents", `{"Entries":[{"EventBusName":"default","Source":"com.example","DetailType":"TestEvent","Detail":"{\"key\":\"value\"}"}]}`)
	assert.Equal(t, 200, eventsResp.StatusCode)
	em := parseJSON(t, eventsResp)
	assert.Equal(t, float64(0), em["FailedEntryCount"])
	entries := em["Entries"].([]any)
	require.Len(t, entries, 1)
	_, hasEventID := entries[0].(map[string]any)["EventId"]
	assert.True(t, hasEventID)
}

func TestRulePatternMatching(t *testing.T) {
	p := newTestProvider(t)

	// Rule matches only "com.example" source.
	call(t, p, "PutRule", `{"Name":"pattern-rule","EventBusName":"default","EventPattern":"{\"source\":[\"com.example\"]}","State":"ENABLED"}`)
	call(t, p, "PutTargets", `{"Rule":"pattern-rule","EventBusName":"default","Targets":[{"Id":"t1","Arn":"arn:aws:sqs:us-east-1:000000000000:q"}]}`)

	// Matching event — should succeed.
	matchResp := call(t, p, "PutEvents", `{"Entries":[{"EventBusName":"default","Source":"com.example","DetailType":"T","Detail":"{}"}]}`)
	mm := parseJSON(t, matchResp)
	assert.Equal(t, float64(0), mm["FailedEntryCount"])
	assert.Len(t, mm["Entries"].([]any), 1)

	// Non-matching source — still succeeds (PutEvents doesn't fail on no match).
	noMatchResp := call(t, p, "PutEvents", `{"Entries":[{"EventBusName":"default","Source":"com.other","DetailType":"T","Detail":"{}"}]}`)
	nm := parseJSON(t, noMatchResp)
	assert.Equal(t, float64(0), nm["FailedEntryCount"])
}

func TestEB_EnableDisableRule(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutRule", `{"Name":"toggle-rule","EventBusName":"default","State":"ENABLED"}`)

	disableResp := call(t, p, "DisableRule", `{"Name":"toggle-rule","EventBusName":"default"}`)
	assert.Equal(t, 200, disableResp.StatusCode)

	listResp := call(t, p, "ListRules", `{"EventBusName":"default"}`)
	lm := parseJSON(t, listResp)
	for _, r := range lm["Rules"].([]any) {
		rm := r.(map[string]any)
		if rm["Name"] == "toggle-rule" {
			assert.Equal(t, "DISABLED", rm["State"])
		}
	}

	enableResp := call(t, p, "EnableRule", `{"Name":"toggle-rule","EventBusName":"default"}`)
	assert.Equal(t, 200, enableResp.StatusCode)
}

func TestEB_ListResources(t *testing.T) {
	p := newTestProvider(t)
	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	// "default" bus is seeded.
	require.GreaterOrEqual(t, len(resources), 1)
	assert.Equal(t, "event-bus", resources[0].Type)
}

func TestEB_ArchiveLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Create archive.
	resp := call(t, p, "CreateArchive", `{"ArchiveName":"test-archive","EventSourceArn":"arn:aws:events:us-east-1:000000000000:event-bus/default","Description":"desc","RetentionDays":7}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ArchiveArn"].(string), "test-archive")

	// Describe archive.
	descResp := call(t, p, "DescribeArchive", `{"ArchiveName":"test-archive"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	assert.Equal(t, "test-archive", dm["ArchiveName"])
	assert.Equal(t, float64(7), dm["RetentionDays"])

	// List archives.
	listResp := call(t, p, "ListArchives", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	archives := lm["Archives"].([]any)
	names := make([]string, 0)
	for _, a := range archives {
		names = append(names, a.(map[string]any)["ArchiveName"].(string))
	}
	assert.Contains(t, names, "test-archive")

	// Update archive.
	updResp := call(t, p, "UpdateArchive", `{"ArchiveName":"test-archive","Description":"updated"}`)
	assert.Equal(t, 200, updResp.StatusCode)

	// Verify update.
	descResp2 := call(t, p, "DescribeArchive", `{"ArchiveName":"test-archive"}`)
	dm2 := parseJSON(t, descResp2)
	assert.Equal(t, "updated", dm2["Description"])

	// Delete archive.
	delResp := call(t, p, "DeleteArchive", `{"ArchiveName":"test-archive"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Confirm deleted.
	descAfterDel := call(t, p, "DescribeArchive", `{"ArchiveName":"test-archive"}`)
	assert.Equal(t, 400, descAfterDel.StatusCode)
}

func TestEB_ReplayLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Create an archive to replay from.
	call(t, p, "CreateArchive", `{"ArchiveName":"replay-src","EventSourceArn":"arn:aws:events:us-east-1:000000000000:event-bus/default"}`)

	// Start replay.
	resp := call(t, p, "StartReplay", `{"ReplayName":"my-replay","EventSourceArn":"arn:aws:events:us-east-1:000000000000:archive/replay-src","EventStartTime":1000000,"EventEndTime":1000001,"Destination":{"Arn":"arn:aws:events:us-east-1:000000000000:event-bus/default"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "COMPLETED", m["State"])

	// Describe replay.
	descResp := call(t, p, "DescribeReplay", `{"ReplayName":"my-replay"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	assert.Equal(t, "my-replay", dm["ReplayName"])

	// List replays.
	listResp := call(t, p, "ListReplays", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	replays := lm["Replays"].([]any)
	names := make([]string, 0)
	for _, r := range replays {
		names = append(names, r.(map[string]any)["ReplayName"].(string))
	}
	assert.Contains(t, names, "my-replay")

	// Cancel replay.
	cancelResp := call(t, p, "CancelReplay", `{"ReplayName":"my-replay"}`)
	assert.Equal(t, 200, cancelResp.StatusCode)
	cm := parseJSON(t, cancelResp)
	assert.Equal(t, "CANCELLED", cm["State"])
}

func TestEB_TestEventPattern(t *testing.T) {
	p := newTestProvider(t)

	// Matching event.
	resp := call(t, p, "TestEventPattern", `{"EventPattern":"{\"source\":[\"myapp\"]}","Event":"{\"source\":\"myapp\",\"detail-type\":\"test\",\"detail\":{}}"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, true, m["Result"])

	// Non-matching event.
	resp2 := call(t, p, "TestEventPattern", `{"EventPattern":"{\"source\":[\"other\"]}","Event":"{\"source\":\"myapp\",\"detail-type\":\"test\",\"detail\":{}}"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, false, m2["Result"])
}
