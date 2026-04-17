// SPDX-License-Identifier: Apache-2.0

// internal/services/xray/provider_test.go
package xray

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

func TestGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createBody := `{
		"GroupName": "my-group",
		"FilterExpression": "service(\"myservice\")",
		"InsightsConfiguration": {"InsightsEnabled": true}
	}`
	resp := callREST(t, p, "POST", "/groups", "CreateGroup", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	grp, ok := rb["Group"].(map[string]any)
	require.True(t, ok, "expected Group key")
	assert.Equal(t, "my-group", grp["GroupName"])
	assert.NotEmpty(t, grp["GroupARN"])
	assert.Equal(t, "service(\"myservice\")", grp["FilterExpression"])

	// Get
	resp2 := callREST(t, p, "GET", "/groups?GroupName=my-group", "GetGroup", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	grp2 := rb2["Group"].(map[string]any)
	assert.Equal(t, "my-group", grp2["GroupName"])

	// Get non-existent
	resp3 := callREST(t, p, "GET", "/groups?GroupName=doesnotexist", "GetGroup", "")
	assert.Equal(t, 400, resp3.StatusCode)

	// GetGroups (list)
	callREST(t, p, "POST", "/groups", "CreateGroup", `{"GroupName":"group2"}`)
	listResp := callREST(t, p, "GET", "/groups", "GetGroups", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	groups, ok := listBody["Groups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 2)

	// Update
	updateResp := callREST(t, p, "PUT", "/groups", "UpdateGroup",
		`{"GroupName":"my-group","FilterExpression":"service(\"updated\")"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	updateBody := parseBody(t, updateResp)
	updGrp := updateBody["Group"].(map[string]any)
	assert.Equal(t, "service(\"updated\")", updGrp["FilterExpression"])

	// Update non-existent
	updateResp2 := callREST(t, p, "PUT", "/groups", "UpdateGroup", `{"GroupName":"nonexistent"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/groups?GroupName=my-group", "DeleteGroup",
		`{"GroupName":"my-group"}`)
	assert.Equal(t, 200, deleteResp.StatusCode)

	// Get after delete
	resp4 := callREST(t, p, "GET", "/groups?GroupName=my-group", "GetGroup", "")
	assert.Equal(t, 400, resp4.StatusCode)
}

func TestSamplingRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createBody := `{
		"SamplingRule": {
			"RuleName": "my-rule",
			"Priority": 100,
			"FixedRate": 0.10,
			"ReservoirSize": 5,
			"ServiceName": "my-service",
			"ServiceType": "AWS::ECS::Container",
			"Host": "*",
			"HTTPMethod": "GET",
			"URLPath": "/api/*",
			"ResourceARN": "*",
			"Version": 1
		}
	}`
	resp := callREST(t, p, "POST", "/samplingrules", "CreateSamplingRule", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	rec, ok := rb["SamplingRuleRecord"].(map[string]any)
	require.True(t, ok, "expected SamplingRuleRecord key")
	rule, ok := rec["SamplingRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-rule", rule["RuleName"])
	assert.NotEmpty(t, rule["RuleARN"])
	assert.Equal(t, float64(100), rule["Priority"])
	assert.Equal(t, 0.10, rule["FixedRate"])

	// GetSamplingRules (list)
	callREST(t, p, "POST", "/samplingrules", "CreateSamplingRule",
		`{"SamplingRule":{"RuleName":"rule2","Priority":200,"FixedRate":0.05,"ReservoirSize":1,"ServiceName":"*","ServiceType":"*","Host":"*","HTTPMethod":"*","URLPath":"*","ResourceARN":"*","Version":1}}`)
	listResp := callREST(t, p, "GET", "/samplingrules", "GetSamplingRules", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	records, ok := listBody["SamplingRuleRecords"].([]any)
	require.True(t, ok)
	assert.Len(t, records, 2)

	// Update
	updateResp := callREST(t, p, "PUT", "/samplingrules", "UpdateSamplingRule",
		`{"SamplingRuleUpdate":{"RuleName":"my-rule","FixedRate":0.20,"Priority":50}}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	updateBody := parseBody(t, updateResp)
	updRec := updateBody["SamplingRuleRecord"].(map[string]any)
	updRule := updRec["SamplingRule"].(map[string]any)
	assert.Equal(t, 0.20, updRule["FixedRate"])
	assert.Equal(t, float64(50), updRule["Priority"])

	// Update non-existent
	updateResp2 := callREST(t, p, "PUT", "/samplingrules", "UpdateSamplingRule",
		`{"SamplingRuleUpdate":{"RuleName":"nonexistent","FixedRate":0.5}}`)
	assert.Equal(t, 400, updateResp2.StatusCode)

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/samplingrules", "DeleteSamplingRule",
		`{"RuleName":"my-rule"}`)
	assert.Equal(t, 200, deleteResp.StatusCode)
	delBody := parseBody(t, deleteResp)
	delRec := delBody["SamplingRuleRecord"].(map[string]any)
	delRule := delRec["SamplingRule"].(map[string]any)
	assert.Equal(t, "my-rule", delRule["RuleName"])

	// GetSamplingRules should have 1 left
	listResp2 := callREST(t, p, "GET", "/samplingrules", "GetSamplingRules", "")
	listBody2 := parseBody(t, listResp2)
	records2 := listBody2["SamplingRuleRecords"].([]any)
	assert.Len(t, records2, 1)

	// Delete non-existent
	deleteResp2 := callREST(t, p, "DELETE", "/samplingrules", "DeleteSamplingRule",
		`{"RuleName":"nonexistent"}`)
	assert.Equal(t, 400, deleteResp2.StatusCode)
}

func TestPutAndGetTraces(t *testing.T) {
	p := newTestProvider(t)

	// PutTraceSegments
	seg1 := `{"trace_id":"1-abc12345-abcdef012345678901234567","id":"seg001","name":"my-segment","start_time":1.0,"end_time":2.0}`
	seg2 := `{"trace_id":"1-abc12345-abcdef012345678901234567","id":"seg002","name":"sub-segment","start_time":1.1,"end_time":1.9}`
	seg3 := `{"trace_id":"1-def67890-abcdef012345678901234567","id":"seg003","name":"other-segment","start_time":2.0,"end_time":3.0}`

	putBody, _ := json.Marshal(map[string]any{
		"TraceSegmentDocuments": []string{seg1, seg2, seg3},
	})
	putResp := callREST(t, p, "POST", "/TraceSegments", "PutTraceSegments", string(putBody))
	assert.Equal(t, 200, putResp.StatusCode)
	putRB := parseBody(t, putResp)
	unprocessed, ok := putRB["UnprocessedTraceSegments"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 0)

	// BatchGetTraces
	batchBody, _ := json.Marshal(map[string]any{
		"TraceIds": []string{
			"1-abc12345-abcdef012345678901234567",
			"1-def67890-abcdef012345678901234567",
			"1-notexist-00000000000000000000000",
		},
	})
	batchResp := callREST(t, p, "POST", "/Traces", "BatchGetTraces", string(batchBody))
	assert.Equal(t, 200, batchResp.StatusCode)
	batchRB := parseBody(t, batchResp)
	traces, ok := batchRB["Traces"].([]any)
	require.True(t, ok)
	assert.Len(t, traces, 2)
	unprocessed2, ok := batchRB["UnprocessedTraceIds"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed2, 1)

	// Verify the first trace has 2 segments
	var traceWithTwo map[string]any
	for _, tr := range traces {
		tm := tr.(map[string]any)
		if tm["Id"] == "1-abc12345-abcdef012345678901234567" {
			traceWithTwo = tm
		}
	}
	require.NotNil(t, traceWithTwo)
	segs := traceWithTwo["Segments"].([]any)
	assert.Len(t, segs, 2)

	// GetTraceSummaries
	summaryResp := callREST(t, p, "POST", "/TraceSummaries", "GetTraceSummaries", `{}`)
	assert.Equal(t, 200, summaryResp.StatusCode)
	summaryRB := parseBody(t, summaryResp)
	summaries, ok := summaryRB["TraceSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	cr := callREST(t, p, "POST", "/groups", "CreateGroup",
		`{"GroupName":"tagged-group","Tags":{"Env":"prod"}}`)
	assert.Equal(t, 200, cr.StatusCode)
	crb := parseBody(t, cr)
	grp := crb["Group"].(map[string]any)
	arn := grp["GroupARN"].(string)
	require.NotEmpty(t, arn)

	// ListTagsForResource
	lr := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	tags, ok := lrb["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["Env"])

	// TagResource — add more tags
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Team": "platform", "Owner": "alice"},
	})
	tr := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, tr.StatusCode)

	// Verify
	lr2 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	lrb2 := parseBody(t, lr2)
	tags2 := lrb2["Tags"].(map[string]any)
	assert.Len(t, tags2, 3)
	assert.Equal(t, "prod", tags2["Env"])
	assert.Equal(t, "platform", tags2["Team"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?TagKeys=Env&TagKeys=Team", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify only Owner remains
	lr3 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	lrb3 := parseBody(t, lr3)
	tags3 := lrb3["Tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "alice", tags3["Owner"])
}
