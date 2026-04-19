// SPDX-License-Identifier: Apache-2.0

// internal/services/opensearch/provider_test.go
package opensearch

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
	resp := callREST(t, p, "CreateDomain", `{"DomainName":"my-domain","EngineVersion":"OpenSearch_2.11"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	status, ok := body["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-domain", status["DomainName"])
	assert.Equal(t, "OpenSearch_2.11", status["EngineVersion"])

	// Describe domain
	resp2 := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/domain/my-domain", "DescribeDomain", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	status2, ok := body2["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-domain", status2["DomainName"])

	// Duplicate create should fail
	resp3 := callREST(t, p, "CreateDomain", `{"DomainName":"my-domain"}`)
	assert.Equal(t, 409, resp3.StatusCode)
}

func TestListDomainNames(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateDomain", `{"DomainName":"domain-a"}`)
	callREST(t, p, "CreateDomain", `{"DomainName":"domain-b"}`)

	resp := callRESTWithPath(t, p, "GET", "/2021-01-01/domain", "ListDomainNames", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	names, ok := body["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

func TestDeleteDomain(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateDomain", `{"DomainName":"to-delete"}`)

	// Delete
	resp := callRESTWithPath(t, p, "DELETE", "/2021-01-01/opensearch/domain/to-delete", "DeleteDomain", "")
	assert.Equal(t, 200, resp.StatusCode)

	// Describe after delete should return 404
	resp2 := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/domain/to-delete", "DescribeDomain", "")
	assert.Equal(t, 404, resp2.StatusCode)

	// Delete non-existent
	resp3 := callRESTWithPath(t, p, "DELETE", "/2021-01-01/opensearch/domain/nonexistent", "DeleteDomain", "")
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestCreateAndListApplications(t *testing.T) {
	p := newTestProvider(t)

	// Create application
	resp := callREST(t, p, "CreateApplication", `{"name":"my-app"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.Equal(t, "my-app", body["name"])
	appID, ok := body["id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, appID)

	// Create another
	callREST(t, p, "CreateApplication", `{"name":"my-app-2"}`)

	// List applications
	listResp := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/list-applications", "ListApplications", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	apps, ok := listBody["ApplicationSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, apps, 2)

	// Get application
	getResp := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/application/"+appID, "GetApplication", "")
	assert.Equal(t, 200, getResp.StatusCode)
	getBody := parseBody(t, getResp)
	assert.Equal(t, appID, getBody["id"])

	// Delete application
	delResp := callRESTWithPath(t, p, "DELETE", "/2021-01-01/opensearch/application/"+appID, "DeleteApplication", "")
	assert.Equal(t, 200, delResp.StatusCode)

	// Get after delete should return 404
	getResp2 := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/application/"+appID, "GetApplication", "")
	assert.Equal(t, 404, getResp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create domain to get ARN
	createResp := callREST(t, p, "CreateDomain", `{"DomainName":"tagged-domain"}`)
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
	req := httptest.NewRequest("GET", "/2021-01-01/tags?arn="+arn, nil)
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
	req2 := httptest.NewRequest("GET", "/2021-01-01/tags?arn="+arn, nil)
	listResp2, _ := p.HandleRequest(context.Background(), "ListTags", req2)
	var listBody2 map[string]any
	_ = json.Unmarshal(listResp2.Body, &listBody2)
	assert.Len(t, listBody2["TagList"].([]any), 1)
}

func TestStaticInfoOperations(t *testing.T) {
	p := newTestProvider(t)

	// GetCompatibleVersions
	resp := callREST(t, p, "GetCompatibleVersions", "")
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.NotEmpty(t, body["CompatibleVersions"])

	// ListVersions
	resp2 := callREST(t, p, "ListVersions", "")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := parseBody(t, resp2)
	versions := body2["Versions"].([]any)
	assert.NotEmpty(t, versions)

	// ListInstanceTypeDetails
	resp3 := callREST(t, p, "ListInstanceTypeDetails", "")
	assert.Equal(t, 200, resp3.StatusCode)

	// DescribeInstanceTypeLimits
	resp4 := callREST(t, p, "DescribeInstanceTypeLimits", "")
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestDescribeDomains(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateDomain", `{"DomainName":"multi-a"}`)
	callREST(t, p, "CreateDomain", `{"DomainName":"multi-b"}`)

	resp := callREST(t, p, "DescribeDomains", `{"DomainNames":["multi-a","multi-b","nonexistent"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	list := body["DomainStatusList"].([]any)
	// nonexistent is skipped
	assert.Len(t, list, 2)
}

func TestUpdateDomainConfig(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "CreateDomain", `{"DomainName":"cfg-domain"}`)

	// Update config
	resp := callRESTWithPath(t, p, "POST", "/2021-01-01/opensearch/domain/cfg-domain/config", "UpdateDomainConfig",
		`{"SnapshotOptions":{"AutomatedSnapshotStartHour":0}}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	_, ok := body["DomainConfig"]
	assert.True(t, ok)

	// Describe config
	resp2 := callRESTWithPath(t, p, "GET", "/2021-01-01/opensearch/domain/cfg-domain/config", "DescribeDomainConfig", "")
	assert.Equal(t, 200, resp2.StatusCode)
}

func TestStubOperations(t *testing.T) {
	p := newTestProvider(t)

	stubs := []struct {
		op   string
		body string
	}{
		{"AcceptInboundConnection", `{}`},
		{"AssociatePackage", `{}`},
		{"AuthorizeVpcEndpointAccess", `{}`},
		{"CancelDomainConfigChange", `{}`},
		{"CancelServiceSoftwareUpdate", `{}`},
		{"CreateOutboundConnection", `{}`},
		{"CreatePackage", `{}`},
		{"CreateVpcEndpoint", `{}`},
		{"DeleteInboundConnection", `{}`},
		{"DeleteOutboundConnection", `{}`},
		{"DeletePackage", `{}`},
		{"DeleteVpcEndpoint", `{}`},
		{"DescribeDomainAutoTunes", `{}`},
		{"DescribeDomainChangeProgress", `{}`},
		{"DescribeInboundConnections", `{}`},
		{"DescribeOutboundConnections", `{}`},
		{"DescribePackages", `{}`},
		{"DescribeReservedInstanceOfferings", `{}`},
		{"DescribeReservedInstances", `{}`},
		{"DescribeVpcEndpoints", `{}`},
		{"DissociatePackage", `{}`},
		{"GetPackageVersionHistory", `{}`},
		{"GetUpgradeHistory", `{}`},
		{"GetUpgradeStatus", `{}`},
		{"ListDomainsForPackage", `{}`},
		{"ListPackagesForDomain", `{}`},
		{"ListScheduledActions", `{}`},
		{"ListVpcEndpointAccess", `{}`},
		{"ListVpcEndpoints", `{}`},
		{"ListVpcEndpointsForDomain", `{}`},
		{"PurchaseReservedInstanceOffering", `{}`},
		{"RejectInboundConnection", `{}`},
		{"RevokeVpcEndpointAccess", `{}`},
		{"StartDomainMaintenance", `{}`},
		{"StartServiceSoftwareUpdate", `{}`},
		{"UpdatePackage", `{}`},
		{"UpdateVpcEndpoint", `{}`},
		{"UpgradeDomain", `{}`},
	}

	for _, s := range stubs {
		t.Run(s.op, func(t *testing.T) {
			resp := callREST(t, p, s.op, s.body)
			assert.Equal(t, 200, resp.StatusCode, "op=%s body=%s", s.op, string(resp.Body))
		})
	}
}
