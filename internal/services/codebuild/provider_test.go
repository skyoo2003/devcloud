// SPDX-License-Identifier: Apache-2.0

// internal/services/codebuild/provider_test.go
package codebuild

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

func TestProjectCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateProject
	createBody := `{"name": "my-project", "description": "test project", "serviceRole": "arn:aws:iam::000000000000:role/role", "source": {"type": "GITHUB", "location": "https://github.com/test/repo"}, "artifacts": {"type": "NO_ARTIFACTS"}, "environment": {"type": "LINUX_CONTAINER", "computeType": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:5.0"}}`
	resp := call(t, p, "CreateProject", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	proj, _ := rb["project"].(map[string]any)
	assert.Equal(t, "my-project", proj["name"])
	assert.NotEmpty(t, proj["arn"])

	// Create duplicate → error
	resp2 := call(t, p, "CreateProject", `{"name": "my-project", "source": {"type": "NO_SOURCE"}, "artifacts": {"type": "NO_ARTIFACTS"}, "environment": {"type": "LINUX_CONTAINER", "computeType": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:5.0"}}`)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "ResourceAlreadyExistsException", rb2["__type"])

	// Create second project
	call(t, p, "CreateProject", `{"name": "second-project", "source": {"type": "NO_SOURCE"}, "artifacts": {"type": "NO_ARTIFACTS"}, "environment": {"type": "LINUX_CONTAINER", "computeType": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:5.0"}}`)

	// BatchGetProjects
	batchResp := call(t, p, "BatchGetProjects", `{"names": ["my-project", "second-project", "missing-project"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	projects, _ := bb["projects"].([]any)
	assert.Len(t, projects, 2)
	notFound, _ := bb["projectsNotFound"].([]any)
	assert.Len(t, notFound, 1)
	assert.Equal(t, "missing-project", notFound[0])

	// ListProjects
	listResp := call(t, p, "ListProjects", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	names, _ := lb["projects"].([]any)
	assert.Len(t, names, 2)

	// UpdateProject
	updateResp := call(t, p, "UpdateProject", `{"name": "my-project", "description": "updated description", "timeoutInMinutes": 90}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	ub := parseBody(t, updateResp)
	updatedProj, _ := ub["project"].(map[string]any)
	assert.Equal(t, "updated description", updatedProj["description"])
	assert.Equal(t, float64(90), updatedProj["timeoutInMinutes"])

	// UpdateProject non-existent
	updateResp2 := call(t, p, "UpdateProject", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)
	eb := parseBody(t, updateResp2)
	assert.Equal(t, "ResourceNotFoundException", eb["__type"])

	// InvalidateProjectCache
	cacheResp := call(t, p, "InvalidateProjectCache", `{"projectName": "my-project"}`)
	assert.Equal(t, 200, cacheResp.StatusCode)

	// InvalidateProjectCache non-existent
	cacheResp2 := call(t, p, "InvalidateProjectCache", `{"projectName": "does-not-exist"}`)
	assert.Equal(t, 400, cacheResp2.StatusCode)

	// DeleteProject
	delResp := call(t, p, "DeleteProject", `{"name": "my-project"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// BatchGetProjects after delete
	batchResp2 := call(t, p, "BatchGetProjects", `{"names": ["my-project"]}`)
	bb2 := parseBody(t, batchResp2)
	projects2, _ := bb2["projects"].([]any)
	assert.Len(t, projects2, 0)
	notFound2, _ := bb2["projectsNotFound"].([]any)
	assert.Len(t, notFound2, 1)

	// DeleteProject non-existent
	delResp2 := call(t, p, "DeleteProject", `{"name": "does-not-exist"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
	eb2 := parseBody(t, delResp2)
	assert.Equal(t, "ResourceNotFoundException", eb2["__type"])
}

func TestStartAndGetBuild(t *testing.T) {
	p := newTestProvider(t)

	// Setup: create project
	call(t, p, "CreateProject", `{"name": "build-project", "source": {"type": "NO_SOURCE"}, "artifacts": {"type": "NO_ARTIFACTS"}, "environment": {"type": "LINUX_CONTAINER", "computeType": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:5.0"}}`)

	// StartBuild on non-existent project
	resp0 := call(t, p, "StartBuild", `{"projectName": "no-project"}`)
	assert.Equal(t, 400, resp0.StatusCode)
	eb := parseBody(t, resp0)
	assert.Equal(t, "ResourceNotFoundException", eb["__type"])

	// StartBuild
	startResp := call(t, p, "StartBuild", `{"projectName": "build-project", "sourceVersion": "main"}`)
	assert.Equal(t, 200, startResp.StatusCode)
	sb := parseBody(t, startResp)
	build, _ := sb["build"].(map[string]any)
	buildID, _ := build["id"].(string)
	assert.NotEmpty(t, buildID)
	assert.Equal(t, "build-project", build["projectName"])
	assert.Equal(t, "SUCCEEDED", build["buildStatus"])
	assert.Equal(t, "main", build["sourceVersion"])

	// Start second build
	startResp2 := call(t, p, "StartBuild", `{"projectName": "build-project"}`)
	assert.Equal(t, 200, startResp2.StatusCode)
	sb2 := parseBody(t, startResp2)
	build2, _ := sb2["build"].(map[string]any)
	buildID2, _ := build2["id"].(string)
	assert.NotEmpty(t, buildID2)
	assert.NotEqual(t, buildID, buildID2)

	// BatchGetBuilds
	batchBody := `{"ids": ["` + buildID + `", "` + buildID2 + `", "missing-build:abc"]}`
	batchResp := call(t, p, "BatchGetBuilds", batchBody)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	builds, _ := bb["builds"].([]any)
	assert.Len(t, builds, 2)
	notFound, _ := bb["buildsNotFound"].([]any)
	assert.Len(t, notFound, 1)

	// ListBuilds
	listResp := call(t, p, "ListBuilds", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	ids, _ := lb["ids"].([]any)
	assert.Len(t, ids, 2)

	// ListBuildsForProject
	listProjResp := call(t, p, "ListBuildsForProject", `{"projectName": "build-project"}`)
	assert.Equal(t, 200, listProjResp.StatusCode)
	lpb := parseBody(t, listProjResp)
	projIDs, _ := lpb["ids"].([]any)
	assert.Len(t, projIDs, 2)

	// ListBuildsForProject non-existent
	listProjResp2 := call(t, p, "ListBuildsForProject", `{"projectName": "no-project"}`)
	assert.Equal(t, 400, listProjResp2.StatusCode)

	// StopBuild
	stopResp := call(t, p, "StopBuild", `{"id": "`+buildID+`"}`)
	assert.Equal(t, 200, stopResp.StatusCode)
	stb := parseBody(t, stopResp)
	stoppedBuild, _ := stb["build"].(map[string]any)
	assert.Equal(t, "STOPPED", stoppedBuild["buildStatus"])

	// StopBuild non-existent
	stopResp2 := call(t, p, "StopBuild", `{"id": "no-project:missing"}`)
	assert.Equal(t, 400, stopResp2.StatusCode)

	// RetryBuild
	retryResp := call(t, p, "RetryBuild", `{"id": "`+buildID+`"}`)
	assert.Equal(t, 200, retryResp.StatusCode)
	rrb := parseBody(t, retryResp)
	retriedBuild, _ := rrb["build"].(map[string]any)
	newID, _ := retriedBuild["id"].(string)
	assert.NotEmpty(t, newID)
	assert.NotEqual(t, buildID, newID)

	// RetryBuild non-existent
	retryResp2 := call(t, p, "RetryBuild", `{"id": "no-project:missing"}`)
	assert.Equal(t, 400, retryResp2.StatusCode)

	// BatchDeleteBuilds
	delBody := `{"ids": ["` + buildID + `", "missing-build:abc"]}`
	delResp := call(t, p, "BatchDeleteBuilds", delBody)
	assert.Equal(t, 200, delResp.StatusCode)
	db := parseBody(t, delResp)
	deleted, _ := db["buildsDeleted"].([]any)
	assert.Len(t, deleted, 1)
	notDeleted, _ := db["buildsNotDeleted"].([]any)
	assert.Len(t, notDeleted, 1)
}

func TestReportGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateReportGroup
	createBody := `{"name": "my-report-group", "type": "TEST", "exportConfig": {"exportConfigType": "NO_EXPORT"}}`
	resp := call(t, p, "CreateReportGroup", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	rg, _ := rb["reportGroup"].(map[string]any)
	arn, _ := rg["arn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "my-report-group", rg["name"])
	assert.Equal(t, "TEST", rg["type"])

	// Create duplicate → error
	resp2 := call(t, p, "CreateReportGroup", createBody)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "ResourceAlreadyExistsException", rb2["__type"])

	// Create second report group
	resp3 := call(t, p, "CreateReportGroup", `{"name": "second-group", "type": "CODE_COVERAGE"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	rg3, _ := rb3["reportGroup"].(map[string]any)
	arn3, _ := rg3["arn"].(string)

	// BatchGetReportGroups
	batchResp := call(t, p, "BatchGetReportGroups", `{"reportGroupArns": ["`+arn+`", "`+arn3+`", "arn:aws:codebuild:us-east-1:000000000000:report-group/missing"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	groups, _ := bb["reportGroups"].([]any)
	assert.Len(t, groups, 2)
	notFound, _ := bb["reportGroupsNotFound"].([]any)
	assert.Len(t, notFound, 1)

	// ListReportGroups
	listResp := call(t, p, "ListReportGroups", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	arns, _ := lb["reportGroups"].([]any)
	assert.Len(t, arns, 2)

	// UpdateReportGroup
	updateResp := call(t, p, "UpdateReportGroup", `{"arn": "`+arn+`", "exportConfig": {"exportConfigType": "S3"}}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	ub := parseBody(t, updateResp)
	updatedRG, _ := ub["reportGroup"].(map[string]any)
	ec, _ := updatedRG["exportConfig"].(map[string]any)
	assert.Equal(t, "S3", ec["exportConfigType"])

	// UpdateReportGroup non-existent
	updateResp2 := call(t, p, "UpdateReportGroup", `{"arn": "arn:aws:codebuild:us-east-1:000000000000:report-group/no-group"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)
	eb := parseBody(t, updateResp2)
	assert.Equal(t, "ResourceNotFoundException", eb["__type"])

	// ListReports
	reportsResp := call(t, p, "ListReports", `{}`)
	assert.Equal(t, 200, reportsResp.StatusCode)
	rrb := parseBody(t, reportsResp)
	reports, _ := rrb["reports"].([]any)
	assert.Len(t, reports, 0)

	// ListReportsForReportGroup
	reportsForGroupResp := call(t, p, "ListReportsForReportGroup", `{"reportGroupArn": "`+arn+`"}`)
	assert.Equal(t, 200, reportsForGroupResp.StatusCode)

	// ListReportsForReportGroup non-existent
	reportsForGroupResp2 := call(t, p, "ListReportsForReportGroup", `{"reportGroupArn": "arn:aws:codebuild:us-east-1:000000000000:report-group/no-group"}`)
	assert.Equal(t, 400, reportsForGroupResp2.StatusCode)

	// DeleteReportGroup
	delResp := call(t, p, "DeleteReportGroup", `{"arn": "`+arn+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// BatchGetReportGroups after delete
	batchResp2 := call(t, p, "BatchGetReportGroups", `{"reportGroupArns": ["`+arn+`"]}`)
	bb2 := parseBody(t, batchResp2)
	groups2, _ := bb2["reportGroups"].([]any)
	assert.Len(t, groups2, 0)

	// DeleteReportGroup non-existent
	delResp2 := call(t, p, "DeleteReportGroup", `{"arn": "arn:aws:codebuild:us-east-1:000000000000:report-group/no-group"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
	eb2 := parseBody(t, delResp2)
	assert.Equal(t, "ResourceNotFoundException", eb2["__type"])
}

func TestFleetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateFleet
	createBody := `{"name": "my-fleet", "baseCapacity": 2, "computeType": "BUILD_GENERAL1_MEDIUM", "environmentType": "LINUX_CONTAINER"}`
	resp := call(t, p, "CreateFleet", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	fleet, _ := rb["fleet"].(map[string]any)
	arn, _ := fleet["arn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "my-fleet", fleet["name"])
	assert.Equal(t, float64(2), fleet["baseCapacity"])
	assert.Equal(t, "BUILD_GENERAL1_MEDIUM", fleet["computeType"])

	// Create duplicate → error
	resp2 := call(t, p, "CreateFleet", createBody)
	assert.Equal(t, 400, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "ResourceAlreadyExistsException", rb2["__type"])

	// Create second fleet
	resp3 := call(t, p, "CreateFleet", `{"name": "second-fleet", "baseCapacity": 1}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	fleet3, _ := rb3["fleet"].(map[string]any)
	arn3, _ := fleet3["arn"].(string)

	// BatchGetFleets
	batchResp := call(t, p, "BatchGetFleets", `{"names": ["my-fleet", "second-fleet", "missing-fleet"]}`)
	assert.Equal(t, 200, batchResp.StatusCode)
	bb := parseBody(t, batchResp)
	fleets, _ := bb["fleets"].([]any)
	assert.Len(t, fleets, 2)
	notFound, _ := bb["fleetsNotFound"].([]any)
	assert.Len(t, notFound, 1)
	assert.Equal(t, "missing-fleet", notFound[0])

	// ListFleets
	listResp := call(t, p, "ListFleets", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lb := parseBody(t, listResp)
	arns, _ := lb["fleets"].([]any)
	assert.Len(t, arns, 2)

	// UpdateFleet
	updateResp := call(t, p, "UpdateFleet", `{"fleet": "`+arn+`", "baseCapacity": 5, "computeType": "BUILD_GENERAL1_LARGE"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	ub := parseBody(t, updateResp)
	updatedFleet, _ := ub["fleet"].(map[string]any)
	assert.Equal(t, float64(5), updatedFleet["baseCapacity"])
	assert.Equal(t, "BUILD_GENERAL1_LARGE", updatedFleet["computeType"])

	// UpdateFleet non-existent
	updateResp2 := call(t, p, "UpdateFleet", `{"fleet": "arn:aws:codebuild:us-east-1:000000000000:fleet/no-fleet"}`)
	assert.Equal(t, 400, updateResp2.StatusCode)
	eb := parseBody(t, updateResp2)
	assert.Equal(t, "ResourceNotFoundException", eb["__type"])

	// DeleteFleet
	delResp := call(t, p, "DeleteFleet", `{"fleet": "`+arn3+`"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// ListFleets after delete
	listResp2 := call(t, p, "ListFleets", `{}`)
	lb2 := parseBody(t, listResp2)
	arns2, _ := lb2["fleets"].([]any)
	assert.Len(t, arns2, 1)

	// DeleteFleet non-existent
	delResp2 := call(t, p, "DeleteFleet", `{"fleet": "arn:aws:codebuild:us-east-1:000000000000:fleet/no-fleet"}`)
	assert.Equal(t, 400, delResp2.StatusCode)
	eb2 := parseBody(t, delResp2)
	assert.Equal(t, "ResourceNotFoundException", eb2["__type"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create project with tags
	call(t, p, "CreateProject", `{"name": "tagged-project", "source": {"type": "NO_SOURCE"}, "artifacts": {"type": "NO_ARTIFACTS"}, "environment": {"type": "LINUX_CONTAINER", "computeType": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:5.0"}, "tags": [{"key": "Env", "value": "prod"}]}`)

	arn := projectARN("tagged-project")
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

	// Source credentials
	importResp := call(t, p, "ImportSourceCredentials", `{"serverType": "GITHUB", "authType": "PERSONAL_ACCESS_TOKEN", "token": "ghp_test123"}`)
	assert.Equal(t, 200, importResp.StatusCode)
	ib := parseBody(t, importResp)
	credARN, _ := ib["arn"].(string)
	assert.NotEmpty(t, credARN)

	listCredsResp := call(t, p, "ListSourceCredentials", `{}`)
	assert.Equal(t, 200, listCredsResp.StatusCode)
	lcb := parseBody(t, listCredsResp)
	creds, _ := lcb["credentialsInfos"].([]any)
	assert.Len(t, creds, 1)

	deleteCredResp := call(t, p, "DeleteSourceCredentials", `{"arn": "`+credARN+`"}`)
	assert.Equal(t, 200, deleteCredResp.StatusCode)

	listCredsResp2 := call(t, p, "ListSourceCredentials", `{}`)
	lcb2 := parseBody(t, listCredsResp2)
	creds2, _ := lcb2["credentialsInfos"].([]any)
	assert.Len(t, creds2, 0)

	// ListCuratedEnvironmentImages
	envImagesResp := call(t, p, "ListCuratedEnvironmentImages", `{}`)
	assert.Equal(t, 200, envImagesResp.StatusCode)
	eib := parseBody(t, envImagesResp)
	platforms, _ := eib["platforms"].([]any)
	assert.NotEmpty(t, platforms)

	// Unknown action returns empty success
	unknownResp := call(t, p, "StartBuildBatch", `{}`)
	assert.Equal(t, 200, unknownResp.StatusCode)
}
