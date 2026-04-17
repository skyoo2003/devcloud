// SPDX-License-Identifier: Apache-2.0

// internal/services/appconfig/provider_test.go
package appconfig

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

func TestApplicationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"my-app","Description":"test app"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	appID, ok := rb["Id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, appID)
	assert.Equal(t, "my-app", rb["Name"])

	// Get
	resp2 := callREST(t, p, "GET", "/applications/"+appID, "GetApplication", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-app", rb2["Name"])

	// Get non-existent
	resp3 := callREST(t, p, "GET", "/applications/nonexistent", "GetApplication", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List
	callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"app-b"}`)
	listResp := callREST(t, p, "GET", "/applications", "ListApplications", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	items, ok := listBody["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)

	// Update
	updResp := callREST(t, p, "PATCH", "/applications/"+appID, "UpdateApplication", `{"Name":"my-app-updated","Description":"updated desc"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	updBody := parseBody(t, updResp)
	assert.Equal(t, "my-app-updated", updBody["Name"])
	assert.Equal(t, "updated desc", updBody["Description"])

	// Delete
	delResp := callREST(t, p, "DELETE", "/applications/"+appID, "DeleteApplication", "")
	assert.Equal(t, 204, delResp.StatusCode)

	// Get after delete
	resp4 := callREST(t, p, "GET", "/applications/"+appID, "GetApplication", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// Delete non-existent
	resp5 := callREST(t, p, "DELETE", "/applications/nonexistent", "DeleteApplication", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestEnvironmentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create app first
	appResp := callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"env-test-app"}`)
	appID := parseBody(t, appResp)["Id"].(string)

	// Create env
	resp := callREST(t, p, "POST", "/applications/"+appID+"/environments", "CreateEnvironment",
		`{"Name":"production","Description":"prod env"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	envID, ok := rb["Id"].(string)
	require.True(t, ok)
	assert.Equal(t, "production", rb["Name"])
	assert.Equal(t, "READY_FOR_DEPLOYMENT", rb["State"])

	// Get
	resp2 := callREST(t, p, "GET", "/applications/"+appID+"/environments/"+envID, "GetEnvironment", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "production", rb2["Name"])

	// Get non-existent
	resp3 := callREST(t, p, "GET", "/applications/"+appID+"/environments/nonexistent", "GetEnvironment", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List
	callREST(t, p, "POST", "/applications/"+appID+"/environments", "CreateEnvironment", `{"Name":"staging"}`)
	listResp := callREST(t, p, "GET", "/applications/"+appID+"/environments", "ListEnvironments", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	items := listBody["Items"].([]any)
	assert.Len(t, items, 2)

	// Update
	updResp := callREST(t, p, "PATCH", "/applications/"+appID+"/environments/"+envID, "UpdateEnvironment",
		`{"Name":"production-v2","Description":"updated"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	updBody := parseBody(t, updResp)
	assert.Equal(t, "production-v2", updBody["Name"])

	// Delete
	delResp := callREST(t, p, "DELETE", "/applications/"+appID+"/environments/"+envID, "DeleteEnvironment", "")
	assert.Equal(t, 204, delResp.StatusCode)

	// Get after delete
	resp4 := callREST(t, p, "GET", "/applications/"+appID+"/environments/"+envID, "GetEnvironment", "")
	assert.Equal(t, 404, resp4.StatusCode)
}

func TestConfigProfileCRUD(t *testing.T) {
	p := newTestProvider(t)

	appResp := callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"profile-test-app"}`)
	appID := parseBody(t, appResp)["Id"].(string)

	// Create profile
	resp := callREST(t, p, "POST", "/applications/"+appID+"/configurationprofiles", "CreateConfigurationProfile",
		`{"Name":"my-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	profileID, ok := rb["Id"].(string)
	require.True(t, ok)
	assert.Equal(t, "my-profile", rb["Name"])
	assert.Equal(t, "hosted", rb["LocationUri"])

	// Get
	resp2 := callREST(t, p, "GET", "/applications/"+appID+"/configurationprofiles/"+profileID, "GetConfigurationProfile", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// List
	callREST(t, p, "POST", "/applications/"+appID+"/configurationprofiles", "CreateConfigurationProfile",
		`{"Name":"profile-b","LocationUri":"hosted"}`)
	listResp := callREST(t, p, "GET", "/applications/"+appID+"/configurationprofiles", "ListConfigurationProfiles", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	items := listBody["Items"].([]any)
	assert.Len(t, items, 2)

	// Update
	updResp := callREST(t, p, "PATCH", "/applications/"+appID+"/configurationprofiles/"+profileID, "UpdateConfigurationProfile",
		`{"Name":"my-profile-v2"}`)
	assert.Equal(t, 200, updResp.StatusCode)
	updBody := parseBody(t, updResp)
	assert.Equal(t, "my-profile-v2", updBody["Name"])

	// Delete
	delResp := callREST(t, p, "DELETE", "/applications/"+appID+"/configurationprofiles/"+profileID, "DeleteConfigurationProfile", "")
	assert.Equal(t, 204, delResp.StatusCode)

	resp3 := callREST(t, p, "GET", "/applications/"+appID+"/configurationprofiles/"+profileID, "GetConfigurationProfile", "")
	assert.Equal(t, 404, resp3.StatusCode)
}

func TestDeploymentStrategyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/deploymentstrategies", "CreateDeploymentStrategy",
		`{"Name":"my-strategy","GrowthType":"LINEAR","GrowthFactor":20,"DeploymentDurationInMinutes":5,"FinalBakeTimeInMinutes":1,"ReplicateTo":"NONE"}`)
	assert.Equal(t, 201, resp.StatusCode)
	rb := parseBody(t, resp)
	stratID, ok := rb["Id"].(string)
	require.True(t, ok)
	assert.Equal(t, "my-strategy", rb["Name"])
	assert.Equal(t, float64(20), rb["GrowthFactor"])

	// Get
	resp2 := callREST(t, p, "GET", "/deploymentstrategies/"+stratID, "GetDeploymentStrategy", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Get non-existent
	resp3 := callREST(t, p, "GET", "/deploymentstrategies/nonexistent", "GetDeploymentStrategy", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List
	callREST(t, p, "POST", "/deploymentstrategies", "CreateDeploymentStrategy", `{"Name":"strat-b"}`)
	listResp := callREST(t, p, "GET", "/deploymentstrategies", "ListDeploymentStrategies", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	items := listBody["Items"].([]any)
	assert.Len(t, items, 2)

	// Update
	updResp := callREST(t, p, "PATCH", "/deploymentstrategies/"+stratID, "UpdateDeploymentStrategy",
		`{"GrowthFactor":50,"DeploymentDurationInMinutes":10}`)
	assert.Equal(t, 200, updResp.StatusCode)
	updBody := parseBody(t, updResp)
	assert.Equal(t, float64(50), updBody["GrowthFactor"])

	// Delete
	delResp := callREST(t, p, "DELETE", "/deployementstrategies/"+stratID, "DeleteDeploymentStrategy", "")
	assert.Equal(t, 204, delResp.StatusCode)

	resp4 := callREST(t, p, "GET", "/deploymentstrategies/"+stratID, "GetDeploymentStrategy", "")
	assert.Equal(t, 404, resp4.StatusCode)
}

func TestDeploymentFlow(t *testing.T) {
	p := newTestProvider(t)

	// Setup app, env, profile
	appResp := callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"deploy-app"}`)
	appID := parseBody(t, appResp)["Id"].(string)

	envResp := callREST(t, p, "POST", "/applications/"+appID+"/environments", "CreateEnvironment", `{"Name":"prod"}`)
	envID := parseBody(t, envResp)["Id"].(string)

	profileResp := callREST(t, p, "POST", "/applications/"+appID+"/configurationprofiles", "CreateConfigurationProfile",
		`{"Name":"my-config","LocationUri":"hosted"}`)
	profileID := parseBody(t, profileResp)["Id"].(string)

	stratResp := callREST(t, p, "POST", "/deploymentstrategies", "CreateDeploymentStrategy", `{"Name":"fast"}`)
	stratID := parseBody(t, stratResp)["Id"].(string)

	// Create hosted config version
	req := httptest.NewRequest("POST",
		"/applications/"+appID+"/configurationprofiles/"+profileID+"/hostedconfigurationversions",
		strings.NewReader(`{"key":"value"}`))
	req.Header.Set("Content-Type", "application/json")
	hcvResp, err := p.HandleRequest(context.Background(), "CreateHostedConfigurationVersion", req)
	require.NoError(t, err)
	assert.Equal(t, 201, hcvResp.StatusCode)
	hcvBody := parseBody(t, hcvResp)
	assert.Equal(t, float64(1), hcvBody["VersionNumber"])

	// Start deployment
	deployBody := map[string]any{
		"ConfigurationProfileId": profileID,
		"ConfigurationVersion":   "1",
		"DeploymentStrategyId":   stratID,
	}
	deployJSON, _ := json.Marshal(deployBody)
	deployResp := callREST(t, p, "POST", "/applications/"+appID+"/environments/"+envID+"/deployments",
		"StartDeployment", string(deployJSON))
	assert.Equal(t, 201, deployResp.StatusCode)
	deployRb := parseBody(t, deployResp)
	assert.Equal(t, float64(1), deployRb["DeploymentNumber"])
	assert.Equal(t, "COMPLETE", deployRb["State"])

	// Get deployment
	getDeployResp := callREST(t, p, "GET",
		"/applications/"+appID+"/environments/"+envID+"/deployments/1", "GetDeployment", "")
	assert.Equal(t, 200, getDeployResp.StatusCode)

	// List deployments
	listDeployResp := callREST(t, p, "GET",
		"/applications/"+appID+"/environments/"+envID+"/deployments", "ListDeployments", "")
	assert.Equal(t, 200, listDeployResp.StatusCode)
	listDeployBody := parseBody(t, listDeployResp)
	deployItems := listDeployBody["Items"].([]any)
	assert.Len(t, deployItems, 1)

	// Start second deployment then stop it
	deployResp2 := callREST(t, p, "POST", "/applications/"+appID+"/environments/"+envID+"/deployments",
		"StartDeployment", string(deployJSON))
	assert.Equal(t, 201, deployResp2.StatusCode)
	deployRb2 := parseBody(t, deployResp2)
	depNum2 := int(deployRb2["DeploymentNumber"].(float64))
	assert.Equal(t, 2, depNum2)

	stopResp := callREST(t, p, "DELETE",
		"/applications/"+appID+"/environments/"+envID+"/deployments/2", "StopDeployment", "")
	assert.Equal(t, 200, stopResp.StatusCode)
	stopBody := parseBody(t, stopResp)
	assert.Equal(t, "ROLLED_BACK", stopBody["State"])

	// GetHostedConfigVersion
	getHCVResp := callREST(t, p, "GET",
		"/applications/"+appID+"/configurationprofiles/"+profileID+"/hostedconfigurationversions/1",
		"GetHostedConfigurationVersion", "")
	assert.Equal(t, 200, getHCVResp.StatusCode)
	getHCVBody := parseBody(t, getHCVResp)
	assert.Equal(t, float64(1), getHCVBody["VersionNumber"])

	// ListHostedConfigVersions
	listHCVResp := callREST(t, p, "GET",
		"/applications/"+appID+"/configurationprofiles/"+profileID+"/hostedconfigurationversions",
		"ListHostedConfigurationVersions", "")
	assert.Equal(t, 200, listHCVResp.StatusCode)
	listHCVBody := parseBody(t, listHCVResp)
	hcvItems := listHCVBody["Items"].([]any)
	assert.Len(t, hcvItems, 1)

	// DeleteHostedConfigVersion
	delHCVResp := callREST(t, p, "DELETE",
		"/applications/"+appID+"/configurationprofiles/"+profileID+"/hostedconfigurationversions/1",
		"DeleteHostedConfigurationVersion", "")
	assert.Equal(t, 204, delHCVResp.StatusCode)

	// Get after delete
	getHCVResp2 := callREST(t, p, "GET",
		"/applications/"+appID+"/configurationprofiles/"+profileID+"/hostedconfigurationversions/1",
		"GetHostedConfigurationVersion", "")
	assert.Equal(t, 404, getHCVResp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create app
	appResp := callREST(t, p, "POST", "/applications", "CreateApplication", `{"Name":"tagged-app"}`)
	appBody := parseBody(t, appResp)
	appID := appBody["Id"].(string)
	arn := "arn:aws:appconfig:us-east-1:000000000000:application/" + appID

	// TagResource
	tagBody, _ := json.Marshal(map[string]any{
		"Tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	tagResp := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 204, tagResp.StatusCode)

	// ListTagsForResource
	listTagsResp := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, listTagsResp.StatusCode)
	listTagsBody := parseBody(t, listTagsResp)
	tags, ok := listTagsBody["Tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "platform", tags["Team"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 204, untagResp.StatusCode)

	// Verify 1 tag remains
	listTagsResp2 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	listTagsBody2 := parseBody(t, listTagsResp2)
	tags2, _ := listTagsBody2["Tags"].(map[string]any)
	assert.Len(t, tags2, 1)
	assert.Equal(t, "platform", tags2["Team"])
}
