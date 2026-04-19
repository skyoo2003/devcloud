// SPDX-License-Identifier: Apache-2.0

// internal/services/eks/provider_test.go
package eks

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

func callEKS(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
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

// createCluster is a helper that creates a cluster and returns its name.
func createTestCluster(t *testing.T, p *Provider, name string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": name, "version": "1.29", "roleArn": "arn:aws:iam::000000000000:role/eks-role"})
	resp := callEKS(t, p, "POST", "/clusters", "CreateCluster", string(body))
	require.Equal(t, 200, resp.StatusCode)
}

func TestClusterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callEKS(t, p, "POST", "/clusters", "CreateCluster", `{"name":"test-cluster","version":"1.29","roleArn":"arn:aws:iam::000000000000:role/eks-role"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	cluster, ok := rb["cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-cluster", cluster["name"])
	assert.Equal(t, "1.29", cluster["version"])
	arn, ok := cluster["arn"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, arn)

	// Duplicate create should fail
	resp2 := callEKS(t, p, "POST", "/clusters", "CreateCluster", `{"name":"test-cluster","version":"1.29"}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := callEKS(t, p, "GET", "/clusters/test-cluster", "DescribeCluster", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	c3, ok := rb3["cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-cluster", c3["name"])
	assert.Equal(t, "ACTIVE", c3["status"])

	// List
	resp4 := callEKS(t, p, "GET", "/clusters", "ListClusters", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	names, ok := rb4["clusters"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 1)
	assert.Equal(t, "test-cluster", names[0])

	// UpdateClusterVersion
	resp5 := callEKS(t, p, "POST", "/clusters/test-cluster/updates", "UpdateClusterVersion", `{"version":"1.30"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Delete
	resp6 := callEKS(t, p, "DELETE", "/clusters/test-cluster", "DeleteCluster", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete
	resp7 := callEKS(t, p, "GET", "/clusters/test-cluster", "DescribeCluster", "")
	assert.Equal(t, 404, resp7.StatusCode)

	// List after delete — empty
	resp8 := callEKS(t, p, "GET", "/clusters", "ListClusters", "")
	rb8 := parseBody(t, resp8)
	names8 := rb8["clusters"].([]any)
	assert.Len(t, names8, 0)
}

func TestNodegroupCRUD(t *testing.T) {
	p := newTestProvider(t)
	createTestCluster(t, p, "my-cluster")

	// Create nodegroup
	body, _ := json.Marshal(map[string]any{
		"nodegroupName": "test-ng",
		"nodeRole":      "arn:aws:iam::000000000000:role/ng-role",
		"subnets":       []string{"subnet-1", "subnet-2"},
		"scalingConfig": map[string]any{"desiredSize": 2, "minSize": 1, "maxSize": 5},
	})
	resp := callEKS(t, p, "POST", "/clusters/my-cluster/node-groups", "CreateNodegroup", string(body))
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	ng, ok := rb["nodegroup"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-ng", ng["nodegroupName"])
	assert.Equal(t, "my-cluster", ng["clusterName"])

	// Duplicate create should fail
	resp2 := callEKS(t, p, "POST", "/clusters/my-cluster/node-groups", "CreateNodegroup", string(body))
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := callEKS(t, p, "GET", "/clusters/my-cluster/node-groups/test-ng", "DescribeNodegroup", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	ng3 := rb3["nodegroup"].(map[string]any)
	sc := ng3["scalingConfig"].(map[string]any)
	assert.Equal(t, float64(2), sc["desiredSize"])

	// List
	resp4 := callEKS(t, p, "GET", "/clusters/my-cluster/node-groups", "ListNodegroups", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	ngs := rb4["nodegroups"].([]any)
	assert.Len(t, ngs, 1)

	// UpdateNodegroupConfig
	updateBody, _ := json.Marshal(map[string]any{"scalingConfig": map[string]any{"desiredSize": 3, "minSize": 2, "maxSize": 6}})
	resp5 := callEKS(t, p, "POST", "/clusters/my-cluster/node-groups/test-ng/update-config", "UpdateNodegroupConfig", string(updateBody))
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify update
	resp6 := callEKS(t, p, "GET", "/clusters/my-cluster/node-groups/test-ng", "DescribeNodegroup", "")
	rb6 := parseBody(t, resp6)
	ng6 := rb6["nodegroup"].(map[string]any)
	sc6 := ng6["scalingConfig"].(map[string]any)
	assert.Equal(t, float64(3), sc6["desiredSize"])

	// Delete
	resp7 := callEKS(t, p, "DELETE", "/clusters/my-cluster/node-groups/test-ng", "DeleteNodegroup", "")
	assert.Equal(t, 200, resp7.StatusCode)

	// Describe after delete
	resp8 := callEKS(t, p, "GET", "/clusters/my-cluster/node-groups/test-ng", "DescribeNodegroup", "")
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestFargateProfileCRUD(t *testing.T) {
	p := newTestProvider(t)
	createTestCluster(t, p, "fp-cluster")

	// Create
	body, _ := json.Marshal(map[string]any{
		"fargateProfileName":  "test-fp",
		"podExecutionRoleArn": "arn:aws:iam::000000000000:role/fp-role",
		"selectors":           []map[string]any{{"namespace": "default"}},
		"subnets":             []string{"subnet-1"},
	})
	resp := callEKS(t, p, "POST", "/clusters/fp-cluster/fargate-profiles", "CreateFargateProfile", string(body))
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	fp, ok := rb["fargateProfile"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-fp", fp["fargateProfileName"])
	assert.Equal(t, "fp-cluster", fp["clusterName"])
	assert.NotEmpty(t, fp["fargateProfileArn"])

	// Duplicate create should fail
	resp2 := callEKS(t, p, "POST", "/clusters/fp-cluster/fargate-profiles", "CreateFargateProfile", string(body))
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := callEKS(t, p, "GET", "/clusters/fp-cluster/fargate-profiles/test-fp", "DescribeFargateProfile", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	fp3 := rb3["fargateProfile"].(map[string]any)
	assert.Equal(t, "ACTIVE", fp3["status"])

	// List
	resp4 := callEKS(t, p, "GET", "/clusters/fp-cluster/fargate-profiles", "ListFargateProfiles", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	names := rb4["fargateProfileNames"].([]any)
	assert.Len(t, names, 1)

	// Delete
	resp5 := callEKS(t, p, "DELETE", "/clusters/fp-cluster/fargate-profiles/test-fp", "DeleteFargateProfile", "")
	assert.Equal(t, 200, resp5.StatusCode)

	// Describe after delete
	resp6 := callEKS(t, p, "GET", "/clusters/fp-cluster/fargate-profiles/test-fp", "DescribeFargateProfile", "")
	assert.Equal(t, 404, resp6.StatusCode)

	// List after delete
	resp7 := callEKS(t, p, "GET", "/clusters/fp-cluster/fargate-profiles", "ListFargateProfiles", "")
	rb7 := parseBody(t, resp7)
	names7 := rb7["fargateProfileNames"].([]any)
	assert.Len(t, names7, 0)
}

func TestAddonCRUD(t *testing.T) {
	p := newTestProvider(t)
	createTestCluster(t, p, "addon-cluster")

	// Create
	body, _ := json.Marshal(map[string]any{
		"addonName":    "vpc-cni",
		"addonVersion": "v1.14.1-eksbuild.1",
	})
	resp := callEKS(t, p, "POST", "/clusters/addon-cluster/addons", "CreateAddon", string(body))
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	addon, ok := rb["addon"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "vpc-cni", addon["addonName"])
	assert.Equal(t, "addon-cluster", addon["clusterName"])
	assert.NotEmpty(t, addon["addonArn"])

	// Duplicate create
	resp2 := callEKS(t, p, "POST", "/clusters/addon-cluster/addons", "CreateAddon", string(body))
	assert.Equal(t, 409, resp2.StatusCode)

	// Describe
	resp3 := callEKS(t, p, "GET", "/clusters/addon-cluster/addons/vpc-cni", "DescribeAddon", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	a3 := rb3["addon"].(map[string]any)
	assert.Equal(t, "ACTIVE", a3["status"])
	assert.Equal(t, "v1.14.1-eksbuild.1", a3["addonVersion"])

	// List
	resp4 := callEKS(t, p, "GET", "/clusters/addon-cluster/addons", "ListAddons", "")
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	names := rb4["addons"].([]any)
	assert.Len(t, names, 1)

	// Update
	updateBody, _ := json.Marshal(map[string]any{"addonVersion": "v1.15.0-eksbuild.1"})
	resp5 := callEKS(t, p, "POST", "/clusters/addon-cluster/addons/vpc-cni/update", "UpdateAddon", string(updateBody))
	assert.Equal(t, 200, resp5.StatusCode)

	// Delete
	resp6 := callEKS(t, p, "DELETE", "/clusters/addon-cluster/addons/vpc-cni", "DeleteAddon", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Describe after delete
	resp7 := callEKS(t, p, "GET", "/clusters/addon-cluster/addons/vpc-cni", "DescribeAddon", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)
	createTestCluster(t, p, "tag-cluster")

	// Get cluster ARN
	resp := callEKS(t, p, "GET", "/clusters/tag-cluster", "DescribeCluster", "")
	rb := parseBody(t, resp)
	cluster := rb["cluster"].(map[string]any)
	arn := cluster["arn"].(string)
	require.NotEmpty(t, arn)

	// Tag resource
	tagBody, _ := json.Marshal(map[string]any{
		"tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	tagResp := callEKS(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, tagResp.StatusCode)

	// List tags
	listResp := callEKS(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, listResp.StatusCode)
	rb2 := parseBody(t, listResp)
	tags, ok := rb2["tags"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)
	assert.Equal(t, "prod", tags["Env"])
	assert.Equal(t, "platform", tags["Team"])

	// Untag resource
	untagReq := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", untagReq)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// List again — should have 1 tag
	listResp2 := callEKS(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	rb3 := parseBody(t, listResp2)
	tags3 := rb3["tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "platform", tags3["Team"])
}
