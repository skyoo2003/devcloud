// SPDX-License-Identifier: Apache-2.0

// internal/services/emr/provider_test.go
package emr

import (
	"context"
	"encoding/json"
	"fmt"
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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, action, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", fmt.Sprintf("ElasticMapReduce.%s", action))
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

func TestRunJobFlowAndDescribe(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "RunJobFlow", `{
		"Name": "my-cluster",
		"ReleaseLabel": "emr-6.15.0",
		"Instances": {"MasterInstanceType": "m5.xlarge", "InstanceCount": 3},
		"LogUri": "s3://my-bucket/logs",
		"ServiceRole": "EMR_DefaultRole"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	clusterID, ok := m["JobFlowId"].(string)
	require.True(t, ok)
	assert.True(t, strings.HasPrefix(clusterID, "j-"), "cluster ID should start with j-")
	assert.Len(t, clusterID, 15) // "j-" + 13 chars

	// Describe the cluster
	desc := callJSON(t, p, "DescribeCluster", fmt.Sprintf(`{"ClusterId": "%s"}`, clusterID))
	assert.Equal(t, 200, desc.StatusCode)
	dm := parseJSON(t, desc)
	cluster := dm["Cluster"].(map[string]any)
	assert.Equal(t, "my-cluster", cluster["Name"])
	assert.Equal(t, clusterID, cluster["Id"])
	assert.Equal(t, "emr-6.15.0", cluster["ReleaseLabel"])
	status := cluster["Status"].(map[string]any)
	assert.Equal(t, "WAITING", status["State"])
}

func TestListClusters(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "RunJobFlow", `{"Name": "cluster-1"}`)
	callJSON(t, p, "RunJobFlow", `{"Name": "cluster-2"}`)
	callJSON(t, p, "RunJobFlow", `{"Name": "cluster-3"}`)

	resp := callJSON(t, p, "ListClusters", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	clusters := m["Clusters"].([]any)
	assert.Len(t, clusters, 3)

	// Filter by status — TERMINATED should return 0
	resp2 := callJSON(t, p, "ListClusters", `{"ClusterStates": ["TERMINATED"]}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	clusters2 := m2["Clusters"].([]any)
	assert.Len(t, clusters2, 0)
}

func TestTerminateJobFlows(t *testing.T) {
	p := newTestProvider(t)

	r1 := parseJSON(t, callJSON(t, p, "RunJobFlow", `{"Name": "cluster-1"}`))
	id1 := r1["JobFlowId"].(string)

	resp := callJSON(t, p, "TerminateJobFlows", fmt.Sprintf(`{"JobFlowIds": ["%s"]}`, id1))
	assert.Equal(t, 200, resp.StatusCode)

	desc := parseJSON(t, callJSON(t, p, "DescribeCluster", fmt.Sprintf(`{"ClusterId": "%s"}`, id1)))
	cluster := desc["Cluster"].(map[string]any)
	status := cluster["Status"].(map[string]any)
	assert.Equal(t, "TERMINATED", status["State"])
}

func TestAddAndListSteps(t *testing.T) {
	p := newTestProvider(t)

	r := parseJSON(t, callJSON(t, p, "RunJobFlow", `{"Name": "cluster-1"}`))
	clusterID := r["JobFlowId"].(string)

	addResp := callJSON(t, p, "AddJobFlowSteps", fmt.Sprintf(`{
		"JobFlowId": "%s",
		"Steps": [
			{"Name": "step-1", "ActionOnFailure": "CONTINUE", "HadoopJarStep": {"Jar": "command-runner.jar"}},
			{"Name": "step-2", "ActionOnFailure": "TERMINATE_CLUSTER"}
		]
	}`, clusterID))
	assert.Equal(t, 200, addResp.StatusCode)
	am := parseJSON(t, addResp)
	stepIDs := am["StepIds"].([]any)
	assert.Len(t, stepIDs, 2)

	// ListSteps
	listResp := callJSON(t, p, "ListSteps", fmt.Sprintf(`{"ClusterId": "%s"}`, clusterID))
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	steps := lm["Steps"].([]any)
	assert.Len(t, steps, 2)

	// DescribeStep
	stepID := stepIDs[0].(string)
	descResp := callJSON(t, p, "DescribeStep", fmt.Sprintf(`{"ClusterId": "%s", "StepId": "%s"}`, clusterID, stepID))
	assert.Equal(t, 200, descResp.StatusCode)
	sm := parseJSON(t, descResp)
	step := sm["Step"].(map[string]any)
	assert.Equal(t, stepID, step["Id"])
	assert.Equal(t, "step-1", step["Name"])

	// CancelSteps
	cancelResp := callJSON(t, p, "CancelSteps", fmt.Sprintf(`{"ClusterId": "%s", "StepIds": ["%s"]}`, clusterID, stepID))
	assert.Equal(t, 200, cancelResp.StatusCode)
	cm := parseJSON(t, cancelResp)
	cancelList := cm["CancelStepsInfoList"].([]any)
	assert.Len(t, cancelList, 1)
}

func TestSecurityConfigCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createResp := callJSON(t, p, "CreateSecurityConfiguration", `{
		"Name": "my-sec-config",
		"SecurityConfiguration": "{\"EncryptionConfiguration\":{}}"
	}`)
	assert.Equal(t, 200, createResp.StatusCode)
	cm := parseJSON(t, createResp)
	assert.Equal(t, "my-sec-config", cm["Name"])

	// Describe
	descResp := callJSON(t, p, "DescribeSecurityConfiguration", `{"Name": "my-sec-config"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	assert.Equal(t, "my-sec-config", dm["Name"])
	assert.NotEmpty(t, dm["SecurityConfiguration"])

	// List
	listResp := callJSON(t, p, "ListSecurityConfigurations", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	configs := lm["SecurityConfigurations"].([]any)
	assert.Len(t, configs, 1)

	// Delete
	delResp := callJSON(t, p, "DeleteSecurityConfiguration", `{"Name": "my-sec-config"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Describe after delete — should fail
	descAfter := callJSON(t, p, "DescribeSecurityConfiguration", `{"Name": "my-sec-config"}`)
	assert.Equal(t, 400, descAfter.StatusCode)
}

func TestStudioCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createResp := callJSON(t, p, "CreateStudio", `{
		"Name": "my-studio",
		"Description": "Test studio",
		"AuthMode": "SSO",
		"VpcId": "vpc-12345"
	}`)
	assert.Equal(t, 200, createResp.StatusCode)
	cm := parseJSON(t, createResp)
	studioID := cm["StudioId"].(string)
	assert.True(t, strings.HasPrefix(studioID, "es-"))
	assert.Contains(t, cm["Url"].(string), studioID)

	// Describe
	descResp := callJSON(t, p, "DescribeStudio", fmt.Sprintf(`{"StudioId": "%s"}`, studioID))
	assert.Equal(t, 200, descResp.StatusCode)
	dm := parseJSON(t, descResp)
	studio := dm["Studio"].(map[string]any)
	assert.Equal(t, "my-studio", studio["Name"])
	assert.Equal(t, "SSO", studio["AuthMode"])

	// List
	listResp := callJSON(t, p, "ListStudios", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	studios := lm["Studios"].([]any)
	assert.Len(t, studios, 1)

	// Update
	updateResp := callJSON(t, p, "UpdateStudio", fmt.Sprintf(`{"StudioId": "%s", "Name": "updated-studio"}`, studioID))
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete
	delResp := callJSON(t, p, "DeleteStudio", fmt.Sprintf(`{"StudioId": "%s"}`, studioID))
	assert.Equal(t, 200, delResp.StatusCode)

	// Describe after delete
	descAfter := callJSON(t, p, "DescribeStudio", fmt.Sprintf(`{"StudioId": "%s"}`, studioID))
	assert.Equal(t, 400, descAfter.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster with tags
	r := parseJSON(t, callJSON(t, p, "RunJobFlow", `{
		"Name": "tagged-cluster",
		"Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "data"}]
	}`))
	clusterID := r["JobFlowId"].(string)

	// Describe and verify tags
	desc := parseJSON(t, callJSON(t, p, "DescribeCluster", fmt.Sprintf(`{"ClusterId": "%s"}`, clusterID)))
	cluster := desc["Cluster"].(map[string]any)
	tags := cluster["Tags"].([]any)
	assert.Len(t, tags, 2)

	// AddTags
	addTagsResp := callJSON(t, p, "AddTags", fmt.Sprintf(`{
		"ResourceId": "%s",
		"Tags": [{"Key": "new-key", "Value": "new-val"}]
	}`, clusterID))
	assert.Equal(t, 200, addTagsResp.StatusCode)

	// Verify added tag
	desc2 := parseJSON(t, callJSON(t, p, "DescribeCluster", fmt.Sprintf(`{"ClusterId": "%s"}`, clusterID)))
	cluster2 := desc2["Cluster"].(map[string]any)
	tags2 := cluster2["Tags"].([]any)
	assert.Len(t, tags2, 3)

	// RemoveTags
	removeResp := callJSON(t, p, "RemoveTags", fmt.Sprintf(`{
		"ResourceId": "%s",
		"TagKeys": ["env"]
	}`, clusterID))
	assert.Equal(t, 200, removeResp.StatusCode)

	// Verify removal
	desc3 := parseJSON(t, callJSON(t, p, "DescribeCluster", fmt.Sprintf(`{"ClusterId": "%s"}`, clusterID)))
	cluster3 := desc3["Cluster"].(map[string]any)
	tags3 := cluster3["Tags"].([]any)
	assert.Len(t, tags3, 2)
}
