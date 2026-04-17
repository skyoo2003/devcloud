// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroupstaggingapi/provider_test.go
package resourcegroupstaggingapi

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

func TestGetResources(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetResources", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	list, ok := m["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	assert.Empty(t, list)

	resp2 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetTagKeys", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	keys, ok := m2["TagKeys"].([]any)
	require.True(t, ok)
	assert.Empty(t, keys)

	resp3 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetTagValues", `{"Key":"env"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	vals, ok := m3["TagValues"].([]any)
	require.True(t, ok)
	assert.Empty(t, vals)
}

func TestTagAndUntag(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.TagResources",
		`{"ResourceARNList":["arn:aws:s3:::my-bucket"],"Tags":{"env":"prod"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	failed, ok := m["FailedResourcesMap"].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, failed)

	resp2 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.UntagResources",
		`{"ResourceARNList":["arn:aws:s3:::my-bucket"],"TagKeys":["env"]}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	failed2, ok := m2["FailedResourcesMap"].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, failed2)
}

func TestTagAndGetResources(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.TagResources",
		`{"ResourceARNList":["arn:aws:s3:::bucket1","arn:aws:lambda:us-east-1:000000000000:function:fn1"],"Tags":{"env":"prod","team":"platform"}}`)

	// GetResources - all
	resp := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetResources", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	list, ok := m["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	// GetResources with tag filter
	resp2 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetResources",
		`{"TagFilters":[{"Key":"env","Values":["prod"]}]}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	list2, ok := m2["ResourceTagMappingList"].([]any)
	require.True(t, ok)
	assert.Len(t, list2, 2)
}

func TestGetTagKeysAndValues(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.TagResources",
		`{"ResourceARNList":["arn:aws:s3:::b1","arn:aws:s3:::b2"],"Tags":{"env":"prod","region":"us-east-1"}}`)
	callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.TagResources",
		`{"ResourceARNList":["arn:aws:s3:::b3"],"Tags":{"env":"staging"}}`)

	resp := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetTagKeys", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	keys, ok := m["TagKeys"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(keys), 2)

	resp2 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetTagValues", `{"Key":"env"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	vals, ok := m2["TagValues"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(vals), 2) // prod and staging
}

func TestStubOperations(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.StartReportCreation",
		`{"S3Bucket":"my-bucket"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp2 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.DescribeReportCreation", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "COMPLETE", m2["Status"])

	resp3 := callJSON(t, p, "ResourceGroupsTaggingAPI_20170126.GetComplianceSummary", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
}
