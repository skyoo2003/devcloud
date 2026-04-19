// SPDX-License-Identifier: Apache-2.0

// internal/services/appsync/provider_test.go
package appsync

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

func createTestApi(t *testing.T, p *Provider, name string) string {
	t.Helper()
	body := `{"name":"` + name + `","authenticationType":"API_KEY"}`
	resp := callREST(t, p, "POST", "/v1/apis", "CreateGraphqlApi", body)
	require.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	api, ok := rb["graphqlApi"].(map[string]any)
	require.True(t, ok)
	id, ok := api["apiId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	return id
}

func TestGraphqlApiCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/v1/apis", "CreateGraphqlApi",
		`{"name":"my-api","authenticationType":"API_KEY"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	api, ok := rb["graphqlApi"].(map[string]any)
	require.True(t, ok)
	apiID := api["apiId"].(string)
	assert.NotEmpty(t, apiID)
	assert.Equal(t, "my-api", api["name"])
	assert.Equal(t, "API_KEY", api["authenticationType"])
	assert.NotEmpty(t, api["arn"])

	// Get
	resp2 := callREST(t, p, "GET", "/v1/apis/"+apiID, "GetGraphqlApi", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	api2 := rb2["graphqlApi"].(map[string]any)
	assert.Equal(t, apiID, api2["apiId"])
	assert.Equal(t, "my-api", api2["name"])

	// List
	resp3 := callREST(t, p, "GET", "/v1/apis", "ListGraphqlApis", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	apis, ok := rb3["graphqlApis"].([]any)
	require.True(t, ok)
	assert.Len(t, apis, 1)

	// Update
	resp4 := callREST(t, p, "POST", "/v1/apis/"+apiID, "UpdateGraphqlApi",
		`{"name":"updated-api"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	api4 := rb4["graphqlApi"].(map[string]any)
	assert.Equal(t, "updated-api", api4["name"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v1/apis/"+apiID, "DeleteGraphqlApi", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete -> 404
	resp6 := callREST(t, p, "GET", "/v1/apis/"+apiID, "GetGraphqlApi", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestDataSourceCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestApi(t, p, "ds-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v1/apis/"+apiID+"/datasources", "CreateDataSource",
		`{"name":"MyDS","type":"NONE"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	ds, ok := rb["dataSource"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "MyDS", ds["name"])
	assert.Equal(t, "NONE", ds["type"])

	// Get
	resp2 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/datasources/MyDS", "GetDataSource", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	ds2 := rb2["dataSource"].(map[string]any)
	assert.Equal(t, "MyDS", ds2["name"])

	// List
	resp3 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/datasources", "ListDataSources", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	dsList := rb3["dataSources"].([]any)
	assert.Len(t, dsList, 1)

	// Update
	resp4 := callREST(t, p, "POST", "/v1/apis/"+apiID+"/datasources/MyDS", "UpdateDataSource",
		`{"type":"HTTP"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	ds4 := rb4["dataSource"].(map[string]any)
	assert.Equal(t, "HTTP", ds4["type"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v1/apis/"+apiID+"/datasources/MyDS", "DeleteDataSource", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete -> 404
	resp6 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/datasources/MyDS", "GetDataSource", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestResolverCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestApi(t, p, "resolver-test-api")

	path := "/v1/apis/" + apiID + "/types/Query/resolvers"

	// Create
	resp := callREST(t, p, "POST", path, "CreateResolver",
		`{"fieldName":"getUser","dataSourceName":"MyDS","kind":"UNIT"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	r, ok := rb["resolver"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "getUser", r["fieldName"])
	assert.Equal(t, "Query", r["typeName"])
	assert.Equal(t, "UNIT", r["kind"])

	// Get
	resp2 := callREST(t, p, "GET", path+"/getUser", "GetResolver", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	r2 := rb2["resolver"].(map[string]any)
	assert.Equal(t, "getUser", r2["fieldName"])

	// List
	resp3 := callREST(t, p, "GET", path, "ListResolvers", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	rList := rb3["resolvers"].([]any)
	assert.Len(t, rList, 1)

	// Update
	resp4 := callREST(t, p, "POST", path+"/getUser", "UpdateResolver",
		`{"dataSourceName":"OtherDS"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	r4 := rb4["resolver"].(map[string]any)
	assert.Equal(t, "OtherDS", r4["dataSourceName"])

	// ListResolversByFunction
	resp5 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/functions/func1/resolvers", "ListResolversByFunction", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Delete
	resp6 := callREST(t, p, "DELETE", path+"/getUser", "DeleteResolver", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Get after delete -> 404
	resp7 := callREST(t, p, "GET", path+"/getUser", "GetResolver", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestFunctionCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestApi(t, p, "fn-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v1/apis/"+apiID+"/functions", "CreateFunction",
		`{"name":"myFunction","dataSourceName":"MyDS"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	fn, ok := rb["functionConfiguration"].(map[string]any)
	require.True(t, ok)
	fnID := fn["functionId"].(string)
	assert.NotEmpty(t, fnID)
	assert.Equal(t, "myFunction", fn["name"])

	// Get
	resp2 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/functions/"+fnID, "GetFunction", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	fn2 := rb2["functionConfiguration"].(map[string]any)
	assert.Equal(t, fnID, fn2["functionId"])

	// List
	resp3 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/functions", "ListFunctions", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	fnList := rb3["functions"].([]any)
	assert.Len(t, fnList, 1)

	// Update
	resp4 := callREST(t, p, "POST", "/v1/apis/"+apiID+"/functions/"+fnID, "UpdateFunction",
		`{"name":"updatedFunction"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	fn4 := rb4["functionConfiguration"].(map[string]any)
	assert.Equal(t, "updatedFunction", fn4["name"])

	// Delete
	resp5 := callREST(t, p, "DELETE", "/v1/apis/"+apiID+"/functions/"+fnID, "DeleteFunction", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete -> 404
	resp6 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/functions/"+fnID, "GetFunction", "")
	assert.Equal(t, 404, resp6.StatusCode)
}

func TestApiKeyCRUD(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestApi(t, p, "key-test-api")

	// Create
	resp := callREST(t, p, "POST", "/v1/apis/"+apiID+"/apikeys", "CreateApiKey",
		`{"description":"my key"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	key, ok := rb["apiKey"].(map[string]any)
	require.True(t, ok)
	keyID := key["id"].(string)
	assert.NotEmpty(t, keyID)
	assert.Equal(t, "my key", key["description"])

	// List
	resp2 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/apikeys", "ListApiKeys", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	keyList := rb2["apiKeys"].([]any)
	assert.Len(t, keyList, 1)

	// Update
	resp3 := callREST(t, p, "POST", "/v1/apis/"+apiID+"/apikeys/"+keyID, "UpdateApiKey",
		`{"description":"updated key"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	key3 := rb3["apiKey"].(map[string]any)
	assert.Equal(t, "updated key", key3["description"])

	// Delete
	resp4 := callREST(t, p, "DELETE", "/v1/apis/"+apiID+"/apikeys/"+keyID, "DeleteApiKey", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// List after delete -> empty
	resp5 := callREST(t, p, "GET", "/v1/apis/"+apiID+"/apikeys", "ListApiKeys", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	keyList5 := rb5["apiKeys"].([]any)
	assert.Len(t, keyList5, 0)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)
	apiID := createTestApi(t, p, "tag-test-api")

	// Get the ARN first
	resp := callREST(t, p, "GET", "/v1/apis/"+apiID, "GetGraphqlApi", "")
	rb := parseBody(t, resp)
	api := rb["graphqlApi"].(map[string]any)
	arn := api["arn"].(string)
	assert.NotEmpty(t, arn)

	// TagResource
	tagPath := "/v1/tags/" + arn
	resp2 := callREST(t, p, "POST", tagPath, "TagResource", `{"tags":{"env":"prod","team":"backend"}}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// ListTagsForResource
	resp3 := callREST(t, p, "GET", tagPath, "ListTagsForResource", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	tags := rb3["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "backend", tags["team"])

	// UntagResource
	resp4 := callREST(t, p, "DELETE", tagPath+"?tagKeys=env", "UntagResource", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// List again
	resp5 := callREST(t, p, "GET", tagPath, "ListTagsForResource", "")
	rb5 := parseBody(t, resp5)
	tags5 := rb5["tags"].(map[string]any)
	_, hasEnv := tags5["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "backend", tags5["team"])
}
