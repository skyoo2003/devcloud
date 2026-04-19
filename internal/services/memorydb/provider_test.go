// SPDX-License-Identifier: Apache-2.0

// internal/services/memorydb/provider_test.go
package memorydb

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
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

const svc = "AmazonMemoryDB"

func TestClusterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"test-cluster","NodeType":"db.r6g.large","ACLName":"open-access"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	cluster := m["Cluster"].(map[string]any)
	assert.Equal(t, "test-cluster", cluster["Name"])
	assert.Equal(t, "available", cluster["Status"])
	assert.Contains(t, cluster["ARN"], "arn:aws:memorydb:")

	// Duplicate create
	resp2 := callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"test-cluster","NodeType":"db.r6g.large","ACLName":"open-access"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe one
	resp3 := callJSON(t, p, svc+".DescribeClusters", `{"ClusterName":"test-cluster"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseBody(t, resp3)
	clusters := m3["Clusters"].([]any)
	assert.Len(t, clusters, 1)

	// Describe all
	callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"cluster2","NodeType":"db.r6g.large","ACLName":"open-access"}`)
	resp4 := callJSON(t, p, svc+".DescribeClusters", `{}`)
	m4 := parseBody(t, resp4)
	assert.Len(t, m4["Clusters"].([]any), 2)

	// Update
	resp5 := callJSON(t, p, svc+".UpdateCluster",
		`{"ClusterName":"test-cluster","NodeType":"db.r6g.xlarge"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseBody(t, resp5)
	assert.Equal(t, "db.r6g.xlarge", m5["Cluster"].(map[string]any)["NodeType"])

	// Delete
	resp6 := callJSON(t, p, svc+".DeleteCluster", `{"ClusterName":"test-cluster"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Not found
	resp7 := callJSON(t, p, svc+".DescribeClusters", `{"ClusterName":"test-cluster"}`)
	assert.Equal(t, 400, resp7.StatusCode)

	// BatchUpdateCluster
	callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"batch1","NodeType":"db.r6g.large","ACLName":"open-access"}`)
	resp8 := callJSON(t, p, svc+".BatchUpdateCluster",
		`{"ClusterNames":["batch1"]}`)
	assert.Equal(t, 200, resp8.StatusCode)
	m8 := parseBody(t, resp8)
	assert.Len(t, m8["ProcessedClusters"].([]any), 1)

	// FailoverShard
	resp9 := callJSON(t, p, svc+".FailoverShard",
		`{"ClusterName":"batch1","ShardName":"0001"}`)
	assert.Equal(t, 200, resp9.StatusCode)
}

func TestParameterGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, svc+".CreateParameterGroup",
		`{"ParameterGroupName":"my-pg","Family":"memorydb_redis7","Description":"test"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	pg := m["ParameterGroup"].(map[string]any)
	assert.Equal(t, "my-pg", pg["Name"])
	assert.Equal(t, "memorydb_redis7", pg["Family"])

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateParameterGroup",
		`{"ParameterGroupName":"my-pg","Family":"memorydb_redis7"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe one
	resp3 := callJSON(t, p, svc+".DescribeParameterGroups",
		`{"ParameterGroupName":"my-pg"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseBody(t, resp3)
	assert.Len(t, m3["ParameterGroups"].([]any), 1)

	// DescribeParameters (static)
	resp4 := callJSON(t, p, svc+".DescribeParameters",
		`{"ParameterGroupName":"my-pg"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// ResetParameterGroup
	resp5 := callJSON(t, p, svc+".ResetParameterGroup",
		`{"ParameterGroupName":"my-pg"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Update
	resp6 := callJSON(t, p, svc+".UpdateParameterGroup",
		`{"ParameterGroupName":"my-pg","Description":"updated"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Delete
	resp7 := callJSON(t, p, svc+".DeleteParameterGroup",
		`{"ParameterGroupName":"my-pg"}`)
	assert.Equal(t, 200, resp7.StatusCode)

	// Not found
	resp8 := callJSON(t, p, svc+".DescribeParameterGroups",
		`{"ParameterGroupName":"my-pg"}`)
	assert.Equal(t, 400, resp8.StatusCode)
}

func TestSubnetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, svc+".CreateSubnetGroup",
		`{"SubnetGroupName":"my-sg","Description":"test","VpcId":"vpc-123","SubnetIds":["subnet-a","subnet-b"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	sg := m["SubnetGroup"].(map[string]any)
	assert.Equal(t, "my-sg", sg["Name"])
	assert.Equal(t, "vpc-123", sg["VpcId"])
	subnets := sg["Subnets"].([]any)
	assert.Len(t, subnets, 2)

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateSubnetGroup",
		`{"SubnetGroupName":"my-sg","VpcId":"vpc-123","SubnetIds":[]}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	resp3 := callJSON(t, p, svc+".DescribeSubnetGroups",
		`{"SubnetGroupName":"my-sg"}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Update
	resp4 := callJSON(t, p, svc+".UpdateSubnetGroup",
		`{"SubnetGroupName":"my-sg","Description":"updated","SubnetIds":["subnet-c"]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseBody(t, resp4)
	sg4 := m4["SubnetGroup"].(map[string]any)
	assert.Len(t, sg4["Subnets"].([]any), 1)

	// Delete
	resp5 := callJSON(t, p, svc+".DeleteSubnetGroup",
		`{"SubnetGroupName":"my-sg"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Not found
	resp6 := callJSON(t, p, svc+".DescribeSubnetGroups",
		`{"SubnetGroupName":"my-sg"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestACLCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, svc+".CreateACL",
		`{"ACLName":"my-acl","UserNames":["alice"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	acl := m["ACL"].(map[string]any)
	assert.Equal(t, "my-acl", acl["Name"])
	assert.Equal(t, "active", acl["Status"])
	assert.Len(t, acl["UserNames"].([]any), 1)

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateACL", `{"ACLName":"my-acl"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	resp3 := callJSON(t, p, svc+".DescribeACLs", `{"ACLName":"my-acl"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseBody(t, resp3)
	assert.Len(t, m3["ACLs"].([]any), 1)

	// Update: add bob, remove alice
	resp4 := callJSON(t, p, svc+".UpdateACL",
		`{"ACLName":"my-acl","UserNamesToAdd":["bob"],"UserNamesToRemove":["alice"]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseBody(t, resp4)
	acl4 := m4["ACL"].(map[string]any)
	users4 := acl4["UserNames"].([]any)
	assert.Len(t, users4, 1)
	assert.Equal(t, "bob", users4[0])

	// Delete
	resp5 := callJSON(t, p, svc+".DeleteACL", `{"ACLName":"my-acl"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Not found
	resp6 := callJSON(t, p, svc+".DescribeACLs", `{"ACLName":"my-acl"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestUserCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, svc+".CreateUser",
		`{"UserName":"alice","AccessString":"on ~* +@all","AuthenticationMode":{"Type":"password","Passwords":["secret"]}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	user := m["User"].(map[string]any)
	assert.Equal(t, "alice", user["Name"])
	assert.Equal(t, "active", user["Status"])

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateUser",
		`{"UserName":"alice","AccessString":"on ~* +@all"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe one
	resp3 := callJSON(t, p, svc+".DescribeUsers", `{"UserName":"alice"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseBody(t, resp3)
	assert.Len(t, m3["Users"].([]any), 1)

	// Describe all
	callJSON(t, p, svc+".CreateUser", `{"UserName":"bob","AccessString":"on ~cache* +get"}`)
	resp4 := callJSON(t, p, svc+".DescribeUsers", `{}`)
	m4 := parseBody(t, resp4)
	assert.Len(t, m4["Users"].([]any), 2)

	// Update
	resp5 := callJSON(t, p, svc+".UpdateUser",
		`{"UserName":"alice","AccessString":"on ~new* +@all"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseBody(t, resp5)
	assert.Equal(t, "on ~new* +@all", m5["User"].(map[string]any)["AccessString"])

	// Delete
	resp6 := callJSON(t, p, svc+".DeleteUser", `{"UserName":"alice"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Not found
	resp7 := callJSON(t, p, svc+".DescribeUsers", `{"UserName":"alice"}`)
	assert.Equal(t, 400, resp7.StatusCode)
}

func TestSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup cluster
	callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"my-cluster","NodeType":"db.r6g.large","ACLName":"open-access"}`)

	// Create snapshot
	resp := callJSON(t, p, svc+".CreateSnapshot",
		`{"SnapshotName":"snap1","ClusterName":"my-cluster"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	snap := m["Snapshot"].(map[string]any)
	assert.Equal(t, "snap1", snap["Name"])
	assert.Equal(t, "my-cluster", snap["ClusterName"])
	assert.Equal(t, "available", snap["Status"])

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateSnapshot",
		`{"SnapshotName":"snap1","ClusterName":"my-cluster"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe one
	resp3 := callJSON(t, p, svc+".DescribeSnapshots", `{"SnapshotName":"snap1"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseBody(t, resp3)
	assert.Len(t, m3["Snapshots"].([]any), 1)

	// Copy
	resp4 := callJSON(t, p, svc+".CopySnapshot",
		`{"SourceSnapshotName":"snap1","TargetSnapshotName":"snap2"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseBody(t, resp4)
	assert.Equal(t, "snap2", m4["Snapshot"].(map[string]any)["Name"])

	// Describe all
	resp5 := callJSON(t, p, svc+".DescribeSnapshots", `{}`)
	m5 := parseBody(t, resp5)
	assert.Len(t, m5["Snapshots"].([]any), 2)

	// Delete
	resp6 := callJSON(t, p, svc+".DeleteSnapshot", `{"SnapshotName":"snap1"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// Not found
	resp7 := callJSON(t, p, svc+".DescribeSnapshots", `{"SnapshotName":"snap1"}`)
	assert.Equal(t, 400, resp7.StatusCode)

	// Missing cluster
	resp8 := callJSON(t, p, svc+".CreateSnapshot",
		`{"SnapshotName":"snap9","ClusterName":"no-cluster"}`)
	assert.Equal(t, 400, resp8.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster with tags
	callJSON(t, p, svc+".CreateCluster",
		`{"ClusterName":"tagged","NodeType":"db.r6g.large","ACLName":"open-access","Tags":[{"Key":"env","Value":"prod"}]}`)

	// Get ARN
	r := callJSON(t, p, svc+".DescribeClusters", `{"ClusterName":"tagged"}`)
	m := parseBody(t, r)
	arn := m["Clusters"].([]any)[0].(map[string]any)["ARN"].(string)

	// ListTags
	resp := callJSON(t, p, svc+".ListTags",
		`{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m2 := parseBody(t, resp)
	tags := m2["TagList"].([]any)
	assert.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])

	// TagResource
	resp3 := callJSON(t, p, svc+".TagResource",
		`{"ResourceArn":"`+arn+`","Tags":[{"Key":"team","Value":"infra"}]}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify 2 tags
	resp4 := callJSON(t, p, svc+".ListTags",
		`{"ResourceArn":"`+arn+`"}`)
	m4 := parseBody(t, resp4)
	assert.Len(t, m4["TagList"].([]any), 2)

	// UntagResource
	resp5 := callJSON(t, p, svc+".UntagResource",
		`{"ResourceArn":"`+arn+`","TagKeys":["env"]}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify 1 tag
	resp6 := callJSON(t, p, svc+".ListTags",
		`{"ResourceArn":"`+arn+`"}`)
	m6 := parseBody(t, resp6)
	assert.Len(t, m6["TagList"].([]any), 1)
}
