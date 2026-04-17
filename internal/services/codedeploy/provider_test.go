// SPDX-License-Identifier: Apache-2.0

// internal/services/codedeploy/provider_test.go
package codedeploy

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

func call(t *testing.T, p *Provider, op string, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
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

	// CreateApplication
	resp := call(t, p, "CreateApplication", `{"applicationName": "my-app", "computePlatform": "Server"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	appID, _ := rb["applicationId"].(string)
	assert.NotEmpty(t, appID)

	// Create duplicate → error
	resp2 := call(t, p, "CreateApplication", `{"applicationName": "my-app"}`)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "ApplicationAlreadyExistsException", rb2["__type"])

	// GetApplication
	getResp := call(t, p, "GetApplication", `{"applicationName": "my-app"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	app, _ := gb["application"].(map[string]any)
	assert.Equal(t, "my-app", app["applicationName"])
	assert.Equal(t, "Server", app["computePlatform"])

	// GetApplication non-existent
	getResp2 := call(t, p, "GetApplication", `{"applicationName": "does-not-exist"}`)
	assert.Equal(t, 400, getResp2.StatusCode)
	eb := parseBody(t, getResp2)
	assert.Equal(t, "ApplicationDoesNotExistException", eb["__type"])

	// Create second application
	call(t, p, "CreateApplication", `{"applicationName": "second-app", "computePlatform": "Lambda"}`)

	// ListApplications
	listResp := call(t, p, "ListApplications", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	apps, _ := lb["applications"].([]any)
	assert.Len(t, apps, 2)

	// BatchGetApplications
	batchResp := call(t, p, "BatchGetApplications", `{"applicationNames": ["my-app", "second-app", "missing-app"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	infos, _ := bb["applicationsInfo"].([]any)
	assert.Len(t, infos, 2)

	// UpdateApplication
	updateResp := call(t, p, "UpdateApplication", `{"applicationName": "my-app", "newApplicationName": "renamed-app"}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	// GetApplication by new name
	getResp3 := call(t, p, "GetApplication", `{"applicationName": "renamed-app"}`)
	assert.Equal(t, 200, getResp3.StatusCode)

	// GetApplication by old name should fail
	getResp4 := call(t, p, "GetApplication", `{"applicationName": "my-app"}`)
	assert.Equal(t, 400, getResp4.StatusCode)

	// UpdateApplication non-existent
	updateResp2 := call(t, p, "UpdateApplication", `{"applicationName": "does-not-exist", "newApplicationName": "x"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)

	// DeleteApplication
	delResp := call(t, p, "DeleteApplication", `{"applicationName": "renamed-app"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// GetApplication after delete
	getResp5 := call(t, p, "GetApplication", `{"applicationName": "renamed-app"}`)
	assert.Equal(t, 400, getResp5.StatusCode)

	// DeleteApplication non-existent
	delResp2 := call(t, p, "DeleteApplication", `{"applicationName": "does-not-exist"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
}

func TestDeploymentGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup: create application
	call(t, p, "CreateApplication", `{"applicationName": "grp-app", "computePlatform": "Server"}`)

	// CreateDeploymentGroup
	createBody := `{"applicationName": "grp-app", "deploymentGroupName": "my-group", "serviceRoleArn": "arn:aws:iam::000000000000:role/role", "deploymentConfigName": "CodeDeployDefault.AllAtOnce"}`
	resp := call(t, p, "CreateDeploymentGroup", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	grpID, _ := rb["deploymentGroupId"].(string)
	assert.NotEmpty(t, grpID)

	// Create duplicate → error
	resp2 := call(t, p, "CreateDeploymentGroup", createBody)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "DeploymentGroupAlreadyExistsException", rb2["__type"])

	// CreateDeploymentGroup on non-existent app → error
	resp3 := call(t, p, "CreateDeploymentGroup", `{"applicationName": "no-app", "deploymentGroupName": "grp"}`)
	assert.Equal(t, 400, resp3.StatusCode)
	eb := parseBody(t, resp3)
	assert.Equal(t, "ApplicationDoesNotExistException", eb["__type"])

	// GetDeploymentGroup
	getResp := call(t, p, "GetDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "my-group"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	grp, _ := gb["deploymentGroupInfo"].(map[string]any)
	assert.Equal(t, "my-group", grp["deploymentGroupName"])
	assert.Equal(t, "grp-app", grp["applicationName"])
	assert.Equal(t, "CodeDeployDefault.AllAtOnce", grp["deploymentConfigName"])

	// GetDeploymentGroup non-existent
	getResp2 := call(t, p, "GetDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "no-group"}`)
	assert.Equal(t, 400, getResp2.StatusCode)

	// Create second group
	call(t, p, "CreateDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "second-group"}`)

	// ListDeploymentGroups
	listResp := call(t, p, "ListDeploymentGroups", `{"applicationName": "grp-app"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	groups, _ := lb["deploymentGroups"].([]any)
	assert.Len(t, groups, 2)

	// BatchGetDeploymentGroups
	batchResp := call(t, p, "BatchGetDeploymentGroups", `{"applicationName": "grp-app", "deploymentGroupNames": ["my-group", "second-group", "missing"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	infos, _ := bb["deploymentGroupsInfo"].([]any)
	assert.Len(t, infos, 2)

	// UpdateDeploymentGroup
	updateResp := call(t, p, "UpdateDeploymentGroup", `{"applicationName": "grp-app", "currentDeploymentGroupName": "my-group", "deploymentConfigName": "CodeDeployDefault.OneAtATime"}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	// Verify update
	getResp3 := call(t, p, "GetDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "my-group"}`)
	gb3 := parseBody(t, getResp3)
	grp3, _ := gb3["deploymentGroupInfo"].(map[string]any)
	assert.Equal(t, "CodeDeployDefault.OneAtATime", grp3["deploymentConfigName"])

	// UpdateDeploymentGroup non-existent
	updateResp2 := call(t, p, "UpdateDeploymentGroup", `{"applicationName": "grp-app", "currentDeploymentGroupName": "no-group"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)

	// DeleteDeploymentGroup
	delResp := call(t, p, "DeleteDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "my-group"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// GetDeploymentGroup after delete
	getResp4 := call(t, p, "GetDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "my-group"}`)
	assert.Equal(t, 400, getResp4.StatusCode)

	// DeleteDeploymentGroup non-existent
	delResp2 := call(t, p, "DeleteDeploymentGroup", `{"applicationName": "grp-app", "deploymentGroupName": "no-group"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
}

func TestDeploymentCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup
	call(t, p, "CreateApplication", `{"applicationName": "deploy-app", "computePlatform": "Server"}`)
	call(t, p, "CreateDeploymentGroup", `{"applicationName": "deploy-app", "deploymentGroupName": "deploy-group"}`)

	// CreateDeployment
	createBody := `{"applicationName": "deploy-app", "deploymentGroupName": "deploy-group", "description": "my deploy", "revision": {"revisionType": "S3", "s3Location": {"bucket": "my-bucket", "key": "app.zip"}}}`
	resp := call(t, p, "CreateDeployment", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	deployID, _ := rb["deploymentId"].(string)
	assert.NotEmpty(t, deployID)

	// Create second deployment
	resp2 := call(t, p, "CreateDeployment", `{"applicationName": "deploy-app", "deploymentGroupName": "deploy-group"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	deployID2, _ := rb2["deploymentId"].(string)
	assert.NotEmpty(t, deployID2)
	assert.NotEqual(t, deployID, deployID2)

	// CreateDeployment on non-existent app → error
	resp3 := call(t, p, "CreateDeployment", `{"applicationName": "no-app", "deploymentGroupName": "grp"}`)
	assert.Equal(t, 400, resp3.StatusCode)
	eb := parseBody(t, resp3)
	assert.Equal(t, "ApplicationDoesNotExistException", eb["__type"])

	// GetDeployment
	getResp := call(t, p, "GetDeployment", `{"deploymentId": "`+deployID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	d, _ := gb["deploymentInfo"].(map[string]any)
	assert.Equal(t, deployID, d["deploymentId"])
	assert.Equal(t, "deploy-app", d["applicationName"])
	assert.Equal(t, "deploy-group", d["deploymentGroupName"])
	assert.Equal(t, "Succeeded", d["status"])
	assert.Equal(t, "my deploy", d["description"])

	// GetDeployment non-existent
	getResp2 := call(t, p, "GetDeployment", `{"deploymentId": "d-NOTEXIST"}`)
	assert.Equal(t, 400, getResp2.StatusCode)
	eb2 := parseBody(t, getResp2)
	assert.Equal(t, "DeploymentDoesNotExistException", eb2["__type"])

	// ListDeployments
	listResp := call(t, p, "ListDeployments", `{"applicationName": "deploy-app", "deploymentGroupName": "deploy-group"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	ids, _ := lb["deployments"].([]any)
	assert.Len(t, ids, 2)

	// BatchGetDeployments
	batchResp := call(t, p, "BatchGetDeployments", `{"deploymentIds": ["`+deployID+`", "`+deployID2+`", "d-MISSING"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	infos, _ := bb["deploymentsInfo"].([]any)
	assert.Len(t, infos, 2)

	// StopDeployment
	stopResp := call(t, p, "StopDeployment", `{"deploymentId": "`+deployID+`"}`)
	assert.Equal(t, 200, stopResp.StatusCode)
	sb := parseBody(t, stopResp)
	assert.Equal(t, "Pending", sb["status"])

	// Verify status changed
	getResp3 := call(t, p, "GetDeployment", `{"deploymentId": "`+deployID+`"}`)
	gb3 := parseBody(t, getResp3)
	d3, _ := gb3["deploymentInfo"].(map[string]any)
	assert.Equal(t, "Stopped", d3["status"])

	// StopDeployment non-existent
	stopResp2 := call(t, p, "StopDeployment", `{"deploymentId": "d-NOTEXIST"}`)
	assert.Equal(t, 400, stopResp2.StatusCode)
}

func TestDeploymentConfigCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateDeploymentConfig
	createBody := `{"deploymentConfigName": "my-config", "computePlatform": "Server", "minimumHealthyHosts": {"type": "FLEET_PERCENT", "value": 75}}`
	resp := call(t, p, "CreateDeploymentConfig", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["deploymentConfigId"])

	// Create duplicate → error
	resp2 := call(t, p, "CreateDeploymentConfig", createBody)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "DeploymentConfigAlreadyExistsException", rb2["__type"])

	// Create second config
	call(t, p, "CreateDeploymentConfig", `{"deploymentConfigName": "second-config"}`)

	// GetDeploymentConfig
	getResp := call(t, p, "GetDeploymentConfig", `{"deploymentConfigName": "my-config"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gb := parseBody(t, getResp)
	cfg, _ := gb["deploymentConfigInfo"].(map[string]any)
	assert.Equal(t, "my-config", cfg["deploymentConfigName"])
	assert.Equal(t, "Server", cfg["computePlatform"])
	mh, _ := cfg["minimumHealthyHosts"].(map[string]any)
	assert.Equal(t, "FLEET_PERCENT", mh["type"])

	// GetDeploymentConfig non-existent
	getResp2 := call(t, p, "GetDeploymentConfig", `{"deploymentConfigName": "no-config"}`)
	assert.Equal(t, 400, getResp2.StatusCode)
	eb := parseBody(t, getResp2)
	assert.Equal(t, "DeploymentConfigDoesNotExistException", eb["__type"])

	// ListDeploymentConfigs
	listResp := call(t, p, "ListDeploymentConfigs", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	names, _ := lb["deploymentConfigsList"].([]any)
	assert.Len(t, names, 2)

	// DeleteDeploymentConfig
	delResp := call(t, p, "DeleteDeploymentConfig", `{"deploymentConfigName": "my-config"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// GetDeploymentConfig after delete
	getResp3 := call(t, p, "GetDeploymentConfig", `{"deploymentConfigName": "my-config"}`)
	assert.Equal(t, 400, getResp3.StatusCode)

	// DeleteDeploymentConfig non-existent
	delResp2 := call(t, p, "DeleteDeploymentConfig", `{"deploymentConfigName": "no-config"}`)
	assert.Equal(t, 400, delResp2.StatusCode)

	// ListDeploymentConfigs after delete
	listResp2 := call(t, p, "ListDeploymentConfigs", `{}`)
	lb2 := parseBody(t, listResp2)
	names2, _ := lb2["deploymentConfigsList"].([]any)
	assert.Len(t, names2, 1)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create application
	call(t, p, "CreateApplication", `{"applicationName": "tagged-app", "computePlatform": "Server", "tags": [{"key": "Env", "value": "prod"}]}`)

	arn := appARN("tagged-app")
	require.NotEmpty(t, arn)

	// ListTagsForResource
	lr := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	tagsList, ok := lrb["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList, 1)

	// TagResource
	tagBody := `{"resourceArn": "` + arn + `", "tags": [{"key": "Team", "value": "platform"}, {"key": "Owner", "value": "alice"}]}`
	tr := call(t, p, "TagResource", tagBody)
	assert.Equal(t, 200, tr.StatusCode)

	// Verify 3 tags
	lr2 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lrb2 := parseBody(t, lr2)
	tagsList2, _ := lrb2["tags"].([]any)
	assert.Len(t, tagsList2, 3)

	// UntagResource
	untagBody := `{"resourceArn": "` + arn + `", "tagKeys": ["Env", "Team"]}`
	utr := call(t, p, "UntagResource", untagBody)
	assert.Equal(t, 200, utr.StatusCode)

	// Verify only Owner remains
	lr3 := call(t, p, "ListTagsForResource", `{"resourceArn": "`+arn+`"}`)
	lrb3 := parseBody(t, lr3)
	tagsList3, _ := lrb3["tags"].([]any)
	assert.Len(t, tagsList3, 1)
	firstTag := tagsList3[0].(map[string]any)
	assert.Equal(t, "Owner", firstTag["key"])
	assert.Equal(t, "alice", firstTag["value"])

	// Unknown action returns empty success
	unknownResp := call(t, p, "AddTagsToOnPremisesInstances", `{}`)
	assert.Equal(t, 200, unknownResp.StatusCode)
}
