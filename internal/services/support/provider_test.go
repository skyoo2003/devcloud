// SPDX-License-Identifier: Apache-2.0

// internal/services/support/provider_test.go
package support

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
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

func TestCreateAndDescribeCase(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Support_20130415.CreateCase",
		`{"subject":"Test issue","serviceCode":"amazon-ec2","severityCode":"low","communicationBody":"Hello"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	caseID, _ := m["caseId"].(string)
	assert.NotEmpty(t, caseID)

	desc := callJSON(t, p, "Support_20130415.DescribeCases",
		`{"caseIdList":["`+caseID+`"]}`)
	assert.Equal(t, 200, desc.StatusCode)
	dm := parseJSON(t, desc)
	cases := dm["cases"].([]any)
	require.Len(t, cases, 1)
	c := cases[0].(map[string]any)
	assert.Equal(t, caseID, c["caseId"])
	assert.Equal(t, "Test issue", c["subject"])
	assert.Equal(t, "opened", c["status"])
}

func TestAddCommunicationAndDescribe(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Support_20130415.CreateCase",
		`{"subject":"Comm test","communicationBody":"initial"}`)
	m := parseJSON(t, resp)
	caseID := m["caseId"].(string)

	addResp := callJSON(t, p, "Support_20130415.AddCommunicationToCase",
		`{"caseId":"`+caseID+`","communicationBody":"follow up message"}`)
	assert.Equal(t, 200, addResp.StatusCode)
	am := parseJSON(t, addResp)
	assert.Equal(t, true, am["result"])

	commResp := callJSON(t, p, "Support_20130415.DescribeCommunications",
		`{"caseId":"`+caseID+`"}`)
	assert.Equal(t, 200, commResp.StatusCode)
	cm := parseJSON(t, commResp)
	comms := cm["communications"].([]any)
	require.Len(t, comms, 1)
	comm := comms[0].(map[string]any)
	assert.Equal(t, "follow up message", comm["body"])
}

func TestResolveCase(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Support_20130415.CreateCase",
		`{"subject":"To be resolved"}`)
	m := parseJSON(t, resp)
	caseID := m["caseId"].(string)

	resolveResp := callJSON(t, p, "Support_20130415.ResolveCase",
		`{"caseId":"`+caseID+`"}`)
	assert.Equal(t, 200, resolveResp.StatusCode)
	rm := parseJSON(t, resolveResp)
	assert.Equal(t, "opened", rm["initialCaseStatus"])
	assert.Equal(t, "resolved", rm["finalCaseStatus"])

	// Resolved case should not appear without includeResolvedCases
	descResp := callJSON(t, p, "Support_20130415.DescribeCases", `{}`)
	dm := parseJSON(t, descResp)
	cases := dm["cases"].([]any)
	assert.Empty(t, cases)

	// Should appear with includeResolvedCases=true
	descResp2 := callJSON(t, p, "Support_20130415.DescribeCases", `{"includeResolvedCases":true}`)
	dm2 := parseJSON(t, descResp2)
	cases2 := dm2["cases"].([]any)
	require.Len(t, cases2, 1)
	assert.Equal(t, "resolved", cases2[0].(map[string]any)["status"])
}

func TestDescribeServices(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Support_20130415.DescribeServices", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	services := m["services"].([]any)
	assert.NotEmpty(t, services)

	// Verify each service has code and name
	for _, svc := range services {
		s := svc.(map[string]any)
		assert.NotEmpty(t, s["code"])
		assert.NotEmpty(t, s["name"])
	}
}
