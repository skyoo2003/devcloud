// SPDX-License-Identifier: Apache-2.0

// internal/services/elasticsearchservice/provider_test.go
package elasticsearchservice

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

func callREST(t *testing.T, p *Provider, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func callRESTWithPath(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
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

func TestCreateAndDescribeDomain(t *testing.T) {
	p := newTestProvider(t)

	// Create domain
	resp := callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"my-domain","ElasticsearchVersion":"7.10"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	status, ok := body["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-domain", status["DomainName"])
	assert.Equal(t, "7.10", status["ElasticsearchVersion"])

	// Describe domain
	resp2 := callRESTWithPath(t, p, "GET", "/2015-01-01/es/domain/my-domain", "DescribeElasticsearchDomain", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	status2, ok := body2["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-domain", status2["DomainName"])

	// Duplicate create should fail
	resp3 := callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"my-domain"}`)
	assert.Equal(t, 409, resp3.StatusCode)
}

func TestListDomainNames(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"domain-a"}`)
	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"domain-b"}`)

	resp := callRESTWithPath(t, p, "GET", "/2015-01-01/domain", "ListDomainNames", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	names, ok := body["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

func TestDeleteDomain(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"to-delete"}`)

	// Delete
	resp := callRESTWithPath(t, p, "DELETE", "/2015-01-01/es/domain/to-delete", "DeleteElasticsearchDomain", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Describe after delete should return 404
	resp2 := callRESTWithPath(t, p, "GET", "/2015-01-01/es/domain/to-delete", "DescribeElasticsearchDomain", "")
	assert.Equal(t, 404, resp2.StatusCode)

	// Delete non-existent
	resp3 := callRESTWithPath(t, p, "DELETE", "/2015-01-01/es/domain/nonexistent", "DeleteElasticsearchDomain", "")
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestUpdateDomainConfig(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"cfg-domain"}`)

	// Update config
	resp := callRESTWithPath(t, p, "POST", "/2015-01-01/es/domain/cfg-domain/config", "UpdateElasticsearchDomainConfig",
		`{"SnapshotOptions":{"AutomatedSnapshotStartHour":0}}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	_, ok := body["DomainConfig"]
	assert.True(t, ok)

	// Describe config
	resp2 := callRESTWithPath(t, p, "GET", "/2015-01-01/es/domain/cfg-domain/config", "DescribeElasticsearchDomainConfig", "")
	assert.Equal(t, 200, resp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create domain to get ARN
	createResp := callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"tagged-domain"}`)
	body := parseBody(t, createResp)
	status := body["DomainStatus"].(map[string]any)
	arn := status["ARN"].(string)
	assert.NotEmpty(t, arn)

	// Add tags
	addBody, _ := json.Marshal(map[string]any{
		"ARN":     arn,
		"TagList": []map[string]string{{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "platform"}},
	})
	resp := callREST(t, p, "AddTags", string(addBody))
	assert.Equal(t, 200, resp.StatusCode)

	// List tags
	req := httptest.NewRequest("GET", "/2015-01-01/tags?arn="+arn, nil)
	listResp, err := p.HandleRequest(context.Background(), "ListTags", req)
	require.NoError(t, err)
	assert.Equal(t, 200, listResp.StatusCode)
	var listBody map[string]any
	_ = json.Unmarshal(listResp.Body, &listBody)
	tagList := listBody["TagList"].([]any)
	assert.Len(t, tagList, 2)

	// Remove a tag
	removeBody, _ := json.Marshal(map[string]any{"ARN": arn, "TagKeys": []string{"Env"}})
	resp2 := callREST(t, p, "RemoveTags", string(removeBody))
	assert.Equal(t, 200, resp2.StatusCode)

	// Confirm only 1 tag remains
	req2 := httptest.NewRequest("GET", "/2015-01-01/tags?arn="+arn, nil)
	listResp2, _ := p.HandleRequest(context.Background(), "ListTags", req2)
	var listBody2 map[string]any
	_ = json.Unmarshal(listResp2.Body, &listBody2)
	assert.Len(t, listBody2["TagList"].([]any), 1)
}

func TestStaticInfoOperations(t *testing.T) {
	p := newTestProvider(t)

	// GetCompatibleElasticsearchVersions
	resp := callREST(t, p, "GetCompatibleElasticsearchVersions", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.NotEmpty(t, body["CompatibleElasticsearchVersions"])

	// ListElasticsearchVersions
	resp2 := callREST(t, p, "ListElasticsearchVersions", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	versions := body2["ElasticsearchVersions"].([]any)
	assert.NotEmpty(t, versions)

	// ListElasticsearchInstanceTypes
	resp3 := callREST(t, p, "ListElasticsearchInstanceTypes", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// DescribeElasticsearchInstanceTypeLimits
	resp4 := callREST(t, p, "DescribeElasticsearchInstanceTypeLimits", "")
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestDescribeElasticsearchDomains(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"multi-a"}`)
	callREST(t, p, "CreateElasticsearchDomain", `{"DomainName":"multi-b"}`)

	resp := callREST(t, p, "DescribeElasticsearchDomains", `{"DomainNames":["multi-a","multi-b","nonexistent"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	list := body["DomainStatusList"].([]any)
	// nonexistent is skipped
	assert.Len(t, list, 2)
}
