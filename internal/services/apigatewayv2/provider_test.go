// SPDX-License-Identifier: Apache-2.0

// internal/services/apigatewayv2/provider_test.go
package apigatewayv2

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

func createTestAPI(t *testing.T, p *Provider, name string) string {
	t.Helper()
	body := `{"name":"` + name + `","protocolType":"HTTP"}`
	resp := callREST(t, p, "POST", "/v2/apis", "CreateApi", body)
	require.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	id, ok := rb["apiId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	return id
}

func TestApiCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/v2/apis", "CreateApi",
		`{"name":"my-api","protocolType":"HTTP","description":"test"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	apiID, ok := rb["apiId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, apiID)
	assert.Equal(t, "my-api", rb["name"])
	assert.Equal(t, "HTTP", rb["protocolType"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/apis/"+apiID, "GetApi", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, apiID, rb2["apiId"])

	// GetApis (list)
	createTestAPI(t, p, "second-api")
	resp3 := callREST(t, p, "GET", "/v2/apis", "GetApis", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, ok := rb3["items"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(items), 2)

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/apis/"+apiID, "UpdateApi",
		`{"name":"updated-api","description":"new desc"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "updated-api", rb4["name"])

	// Get non-existent
	resp5 := callREST(t, p, "GET", "/v2/apis/nonexistent", "GetApi", "")
	assert.Equal(t, 404, resp5.StatusCode)

	// Delete
	resp6 := callREST(t, p, "DELETE", "/v2/apis/"+apiID, "DeleteApi", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Get after delete
	resp7 := callREST(t, p, "GET", "/v2/apis/"+apiID, "GetApi", "")
	assert.Equal(t, 404, resp7.StatusCode)

	// Delete non-existent
	resp8 := callREST(t, p, "DELETE", "/v2/apis/nonexistent", "DeleteApi", "")
	assert.Equal(t, 404, resp8.StatusCode)

	// Create with missing name
	resp9 := callREST(t, p, "POST", "/v2/apis", "CreateApi", `{}`)
	assert.Equal(t, 400, resp9.StatusCode)
}

func TestRouteCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestAPI(t, p, "route-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v2/apis/"+apiID+"/routes", "CreateRoute",
		`{"routeKey":"GET /items","target":"integrations/abc"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	routeID, ok := rb["routeId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, routeID)
	assert.Equal(t, "GET /items", rb["routeKey"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/routes/"+routeID, "GetRoute", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, routeID, rb2["routeId"])

	// List
	callREST(t, p, "POST", "/v2/apis/"+apiID+"/routes", "CreateRoute",
		`{"routeKey":"POST /items"}`)
	resp3 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/routes", "GetRoutes", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, ok := rb3["items"].([]any)
	require.True(t, ok)
	assert.Equal(t, 2, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/apis/"+apiID+"/routes/"+routeID, "UpdateRoute",
		`{"routeKey":"GET /updated"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "GET /updated", rb4["routeKey"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v2/apis/"+apiID+"/routes/"+routeID, "DeleteRoute", "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Get after delete
	resp6 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/routes/"+routeID, "GetRoute", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Create without routeKey
	resp7 := callREST(t, p, "POST", "/v2/apis/"+apiID+"/routes", "CreateRoute", `{}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestIntegrationCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestAPI(t, p, "integration-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v2/apis/"+apiID+"/integrations", "CreateIntegration",
		`{"integrationType":"HTTP_PROXY","integrationUri":"https://example.com","integrationMethod":"GET"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	integrationID, ok := rb["integrationId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, integrationID)
	assert.Equal(t, "HTTP_PROXY", rb["integrationType"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/integrations/"+integrationID, "GetIntegration", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// List
	resp3 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/integrations", "GetIntegrations", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, _ := rb3["items"].([]any)
	assert.Equal(t, 1, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/apis/"+apiID+"/integrations/"+integrationID, "UpdateIntegration",
		`{"integrationUri":"https://updated.com"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "https://updated.com", rb4["integrationUri"])

	// CreateIntegrationResponse
	irResp := callREST(t, p, "POST",
		"/v2/apis/"+apiID+"/integrations/"+integrationID+"/integrationresponses",
		"CreateIntegrationResponse",
		`{"integrationResponseKey":"$default"}`)
	assert.Equal(t, 201, irResp.StatusCode)
	irBody := parseBody(t, irResp)
	irID, ok := irBody["integrationResponseId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, irID)

	// GetIntegrationResponse
	resp5 := callREST(t, p, "GET",
		"/v2/apis/"+apiID+"/integrations/"+integrationID+"/integrationresponses/"+irID,
		"GetIntegrationResponse", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// GetIntegrationResponses
	resp6 := callREST(t, p, "GET",
		"/v2/apis/"+apiID+"/integrations/"+integrationID+"/integrationresponses",
		"GetIntegrationResponses", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// DeleteIntegrationResponse
	resp7 := callREST(t, p, "DELETE",
		"/v2/apis/"+apiID+"/integrations/"+integrationID+"/integrationresponses/"+irID,
		"DeleteIntegrationResponse", "")
	assert.Equal(t, 204, resp7.StatusCode)

	// Delete integration
	resp8 := callREST(t, p, "DELETE", "/v2/apis/"+apiID+"/integrations/"+integrationID, "DeleteIntegration", "")
	assert.Equal(t, 204, resp8.StatusCode)

	// Get after delete
	resp9 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/integrations/"+integrationID, "GetIntegration", "")
	assert.Equal(t, 404, resp9.StatusCode)
}

func TestAuthorizerCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestAPI(t, p, "authorizer-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v2/apis/"+apiID+"/authorizers", "CreateAuthorizer",
		`{"name":"my-auth","authorizerType":"JWT","identitySource":"$request.header.Authorization"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	authorizerID, ok := rb["authorizerId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, authorizerID)
	assert.Equal(t, "my-auth", rb["name"])
	assert.Equal(t, "JWT", rb["authorizerType"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/authorizers/"+authorizerID, "GetAuthorizer", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// List
	resp3 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/authorizers", "GetAuthorizers", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, _ := rb3["items"].([]any)
	assert.Equal(t, 1, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/apis/"+apiID+"/authorizers/"+authorizerID, "UpdateAuthorizer",
		`{"name":"updated-auth"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "updated-auth", rb4["name"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v2/apis/"+apiID+"/authorizers/"+authorizerID, "DeleteAuthorizer", "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Get after delete
	resp6 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/authorizers/"+authorizerID, "GetAuthorizer", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Create missing name
	resp7 := callREST(t, p, "POST", "/v2/apis/"+apiID+"/authorizers", "CreateAuthorizer", `{}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestStageCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestAPI(t, p, "stage-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v2/apis/"+apiID+"/stages", "CreateStage",
		`{"stageName":"prod","description":"production stage","autoDeploy":false}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "prod", rb["stageName"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/stages/prod", "GetStage", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "prod", rb2["stageName"])

	// List
	callREST(t, p, "POST", "/v2/apis/"+apiID+"/stages", "CreateStage",
		`{"stageName":"dev","description":"dev stage"}`)
	resp3 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/stages", "GetStages", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, _ := rb3["items"].([]any)
	assert.Equal(t, 2, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/apis/"+apiID+"/stages/prod", "UpdateStage",
		`{"description":"updated prod"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "updated prod", rb4["description"])

	// Conflict on duplicate
	resp5 := callREST(t, p, "POST", "/v2/apis/"+apiID+"/stages", "CreateStage",
		`{"stageName":"prod"}`)
	assert.Equal(t, 409, resp5.StatusCode)

	// Delete
	resp6 := callREST(t, p, "DELETE", "/v2/apis/"+apiID+"/stages/prod", "DeleteStage", "")
	assert.Equal(t, 204, resp6.StatusCode)

	// Get after delete
	resp7 := callREST(t, p, "GET", "/v2/apis/"+apiID+"/stages/prod", "GetStage", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestDomainNameCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/v2/domainnames", "CreateDomainName",
		`{"domainName":"api.example.com"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "api.example.com", rb["domainName"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/domainnames/api.example.com", "GetDomainName", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "api.example.com", rb2["domainName"])

	// List
	callREST(t, p, "POST", "/v2/domainnames", "CreateDomainName",
		`{"domainName":"api2.example.com"}`)
	resp3 := callREST(t, p, "GET", "/v2/domainnames", "GetDomainNames", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, _ := rb3["items"].([]any)
	assert.Equal(t, 2, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/domainnames/api.example.com", "UpdateDomainName",
		`{"domainNameConfigurations":[]}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Conflict on duplicate
	resp5 := callREST(t, p, "POST", "/v2/domainnames", "CreateDomainName",
		`{"domainName":"api.example.com"}`)
	assert.Equal(t, 409, resp5.StatusCode)

	// Create ApiMapping
	apiID := createTestAPI(t, p, "mapping-test-api")
	mrResp := callREST(t, p, "POST", "/v2/domainnames/api.example.com/apimappings",
		"CreateApiMapping",
		`{"apiId":"`+apiID+`","stage":"prod"}`)
	assert.Equal(t, 201, mrResp.StatusCode)
	mrBody := parseBody(t, mrResp)
	mappingID, ok := mrBody["apiMappingId"].(string)
	require.True(t, ok)

	// Get ApiMapping
	resp6 := callREST(t, p, "GET",
		"/v2/domainnames/api.example.com/apimappings/"+mappingID,
		"GetApiMapping", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// List ApiMappings
	resp7 := callREST(t, p, "GET",
		"/v2/domainnames/api.example.com/apimappings",
		"GetApiMappings", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Delete ApiMapping
	resp8 := callREST(t, p, "DELETE",
		"/v2/domainnames/api.example.com/apimappings/"+mappingID,
		"DeleteApiMapping", "")
	assert.Equal(t, 204, resp8.StatusCode)

	// Delete domain name
	resp9 := callREST(t, p, "DELETE", "/v2/domainnames/api.example.com", "DeleteDomainName", "")
	assert.Equal(t, 204, resp9.StatusCode)

	// Get after delete
	resp10 := callREST(t, p, "GET", "/v2/domainnames/api.example.com", "GetDomainName", "")
	assert.Equal(t, 404, resp10.StatusCode)
}

func TestVpcLinkCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/v2/vpclinks", "CreateVpcLink",
		`{"name":"my-vpc-link","subnetIds":["subnet-1","subnet-2"],"securityGroupIds":["sg-1"]}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	vpcLinkID, ok := rb["vpcLinkId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, vpcLinkID)
	assert.Equal(t, "my-vpc-link", rb["name"])
	assert.Equal(t, "AVAILABLE", rb["vpcLinkStatus"])

	// Get
	resp2 := callREST(t, p, "GET", "/v2/vpclinks/"+vpcLinkID, "GetVpcLink", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// List
	callREST(t, p, "POST", "/v2/vpclinks", "CreateVpcLink",
		`{"name":"second-vpc-link","subnetIds":["subnet-3"]}`)
	resp3 := callREST(t, p, "GET", "/v2/vpclinks", "GetVpcLinks", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	items, _ := rb3["items"].([]any)
	assert.Equal(t, 2, len(items))

	// Update
	resp4 := callREST(t, p, "PATCH", "/v2/vpclinks/"+vpcLinkID, "UpdateVpcLink",
		`{"name":"updated-vpc-link"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	assert.Equal(t, "updated-vpc-link", rb4["name"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v2/vpclinks/"+vpcLinkID, "DeleteVpcLink", "")
	assert.Equal(t, 204, resp5.StatusCode)

	// Get after delete
	resp6 := callREST(t, p, "GET", "/v2/vpclinks/"+vpcLinkID, "GetVpcLink", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Create missing name
	resp7 := callREST(t, p, "POST", "/v2/vpclinks", "CreateVpcLink", `{}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:apigateway:us-east-1:000000000000:restapis/test-api"

	// Tag
	resp := callREST(t, p, "POST", "/v2/tags/"+arn, "TagResource",
		`{"tags":{"env":"prod","team":"backend"}}`)
	assert.Equal(t, 201, resp.StatusCode)

	// Get tags
	resp2 := callREST(t, p, "GET", "/v2/tags/"+arn, "GetTags", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "backend", tags["team"])

	// Untag
	resp3 := callREST(t, p, "DELETE", "/v2/tags/"+arn+"?tagKeys=env", "UntagResource", "")
	assert.Equal(t, 204, resp3.StatusCode)

	// Verify tag removed
	resp4 := callREST(t, p, "GET", "/v2/tags/"+arn, "GetTags", "")
	rb4 := parseBody(t, resp4)
	tags2, _ := rb4["tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "backend", tags2["team"])
}
