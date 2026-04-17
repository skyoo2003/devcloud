// SPDX-License-Identifier: Apache-2.0

// internal/services/amplify/provider_test.go
package amplify

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

// createTestApp is a helper that creates an app and returns its ID.
func createTestApp(t *testing.T, p *Provider, name string) string {
	t.Helper()
	body := `{"name":"` + name + `"}`
	resp := callREST(t, p, "POST", "/apps", "CreateApp", body)
	require.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	app, ok := rb["app"].(map[string]any)
	require.True(t, ok)
	id, ok := app["appId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)
	return id
}

func TestAppCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/apps", "CreateApp", `{"name":"my-app","description":"test app","platform":"WEB"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	app, ok := rb["app"].(map[string]any)
	require.True(t, ok)
	appID := app["appId"].(string)
	assert.NotEmpty(t, appID)
	assert.Equal(t, "my-app", app["name"])
	assert.Equal(t, "WEB", app["platform"])
	assert.NotEmpty(t, app["defaultDomain"])

	// Get
	resp2 := callREST(t, p, "GET", "/apps/"+appID, "GetApp", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	app2, ok := rb2["app"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, appID, app2["appId"])
	assert.Equal(t, "my-app", app2["name"])

	// List
	createTestApp(t, p, "another-app")
	resp3 := callREST(t, p, "GET", "/apps", "ListApps", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	apps, ok := rb3["apps"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(apps), 2)

	// Update
	resp4 := callREST(t, p, "POST", "/apps/"+appID, "UpdateApp", `{"name":"updated-app","description":"new desc"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	updApp, ok := rb4["app"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated-app", updApp["name"])

	// Get non-existent
	resp5 := callREST(t, p, "GET", "/apps/nonexistent", "GetApp", "")
	assert.Equal(t, 404, resp5.StatusCode)

	// Delete
	resp6 := callREST(t, p, "DELETE", "/apps/"+appID, "DeleteApp", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Get after delete should 404
	resp7 := callREST(t, p, "GET", "/apps/"+appID, "GetApp", "")
	assert.Equal(t, 404, resp7.StatusCode)

	// Delete non-existent
	resp8 := callREST(t, p, "DELETE", "/apps/nonexistent", "DeleteApp", "")
	assert.Equal(t, 404, resp8.StatusCode)

	// Create with missing name
	resp9 := callREST(t, p, "POST", "/apps", "CreateApp", `{}`)
	assert.Equal(t, 400, resp9.StatusCode)
}

func TestBranchCRUD(t *testing.T) {
	p := newTestProvider(t)
	appID := createTestApp(t, p, "branch-test-app")

	// Create branch
	body := `{"branchName":"main","stage":"PRODUCTION","framework":"React","enableAutoBuild":true}`
	resp := callREST(t, p, "POST", "/apps/"+appID+"/branches", "CreateBranch", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	branch, ok := rb["branch"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "main", branch["branchName"])
	assert.Equal(t, "PRODUCTION", branch["stage"])
	assert.Equal(t, "React", branch["framework"])

	// Get branch
	resp2 := callREST(t, p, "GET", "/apps/"+appID+"/branches/main", "GetBranch", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	branch2, ok := rb2["branch"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "main", branch2["branchName"])

	// Create second branch
	callREST(t, p, "POST", "/apps/"+appID+"/branches", "CreateBranch", `{"branchName":"dev"}`)

	// List branches
	resp3 := callREST(t, p, "GET", "/apps/"+appID+"/branches", "ListBranches", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	branches, ok := rb3["branches"].([]any)
	require.True(t, ok)
	assert.Len(t, branches, 2)

	// Update branch
	resp4 := callREST(t, p, "POST", "/apps/"+appID+"/branches/main", "UpdateBranch", `{"stage":"DEVELOPMENT","framework":"Vue"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	updBranch, ok := rb4["branch"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "DEVELOPMENT", updBranch["stage"])

	// Duplicate create
	resp5 := callREST(t, p, "POST", "/apps/"+appID+"/branches", "CreateBranch", `{"branchName":"main"}`)
	assert.Equal(t, 409, resp5.StatusCode)

	// Get non-existent branch
	resp6 := callREST(t, p, "GET", "/apps/"+appID+"/branches/nonexistent", "GetBranch", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Delete branch
	resp7 := callREST(t, p, "DELETE", "/apps/"+appID+"/branches/main", "DeleteBranch", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Get after delete
	resp8 := callREST(t, p, "GET", "/apps/"+appID+"/branches/main", "GetBranch", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestDomainAssociationCRUD(t *testing.T) {
	p := newTestProvider(t)
	appID := createTestApp(t, p, "domain-test-app")

	// Create domain association
	body := `{"domainName":"example.com"}`
	resp := callREST(t, p, "POST", "/apps/"+appID+"/domains", "CreateDomainAssociation", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	da, ok := rb["domainAssociation"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "example.com", da["domainName"])
	assert.Equal(t, "AVAILABLE", da["domainStatus"])

	// Get
	resp2 := callREST(t, p, "GET", "/apps/"+appID+"/domains/example.com", "GetDomainAssociation", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	da2, ok := rb2["domainAssociation"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "example.com", da2["domainName"])

	// Create second domain
	callREST(t, p, "POST", "/apps/"+appID+"/domains", "CreateDomainAssociation", `{"domainName":"other.com"}`)

	// List
	resp3 := callREST(t, p, "GET", "/apps/"+appID+"/domains", "ListDomainAssociations", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	domains, ok := rb3["domainAssociations"].([]any)
	require.True(t, ok)
	assert.Len(t, domains, 2)

	// Update
	resp4 := callREST(t, p, "POST", "/apps/"+appID+"/domains/example.com", "UpdateDomainAssociation",
		`{"subDomainSettings":[{"branchName":"main","prefix":"www"}]}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Duplicate create
	resp5 := callREST(t, p, "POST", "/apps/"+appID+"/domains", "CreateDomainAssociation", body)
	assert.Equal(t, 409, resp5.StatusCode)

	// Get non-existent
	resp6 := callREST(t, p, "GET", "/apps/"+appID+"/domains/nonexistent.com", "GetDomainAssociation", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Delete
	resp7 := callREST(t, p, "DELETE", "/apps/"+appID+"/domains/example.com", "DeleteDomainAssociation", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Get after delete
	resp8 := callREST(t, p, "GET", "/apps/"+appID+"/domains/example.com", "GetDomainAssociation", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestWebhookCRUD(t *testing.T) {
	p := newTestProvider(t)
	appID := createTestApp(t, p, "webhook-test-app")

	// Create webhook
	body := `{"branchName":"main"}`
	resp := callREST(t, p, "POST", "/apps/"+appID+"/webhooks", "CreateWebhook", body)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	wh, ok := rb["webhook"].(map[string]any)
	require.True(t, ok)
	webhookID, ok := wh["webhookId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, webhookID)
	assert.Equal(t, "main", wh["branchName"])
	assert.NotEmpty(t, wh["webhookUrl"])

	// Get
	resp2 := callREST(t, p, "GET", "/webhooks/"+webhookID, "GetWebhook", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	wh2, ok := rb2["webhook"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, webhookID, wh2["webhookId"])

	// Create second webhook
	resp3 := callREST(t, p, "POST", "/apps/"+appID+"/webhooks", "CreateWebhook", `{"branchName":"dev"}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// List
	resp4 := callREST(t, p, "GET", "/apps/"+appID+"/webhooks", "ListWebhooks", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	webhooks, ok := rb4["webhooks"].([]any)
	require.True(t, ok)
	assert.Len(t, webhooks, 2)

	// Update
	resp5 := callREST(t, p, "POST", "/webhooks/"+webhookID, "UpdateWebhook", `{"branchName":"feature"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	updWh, ok := rb5["webhook"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "feature", updWh["branchName"])

	// Get non-existent
	resp6 := callREST(t, p, "GET", "/webhooks/nonexistent", "GetWebhook", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// Delete
	resp7 := callREST(t, p, "DELETE", "/webhooks/"+webhookID, "DeleteWebhook", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Get after delete
	resp8 := callREST(t, p, "GET", "/webhooks/"+webhookID, "GetWebhook", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)
	appID := createTestApp(t, p, "tags-test-app")

	// Get ARN
	getResp := callREST(t, p, "GET", "/apps/"+appID, "GetApp", "")
	rb := parseBody(t, getResp)
	app := rb["app"].(map[string]any)
	arn := app["appArn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	resp := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "platform", tags["Team"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify 1 tag remains
	resp3 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	rb3 := parseBody(t, resp3)
	tags3 := rb3["tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "platform", tags3["Team"])
}
