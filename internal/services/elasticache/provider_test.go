// SPDX-License-Identifier: Apache-2.0

package elasticache

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callQuery(t *testing.T, p *Provider, action string, params map[string]string) *plugin.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	for k, v := range params {
		form.Set(k, v)
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestCacheClusterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
		"Engine":         "redis",
		"EngineVersion":  "7.1",
		"CacheNodeType":  "cache.r6g.large",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		CacheCluster struct {
			CacheClusterId     string `xml:"CacheClusterId"`
			CacheClusterStatus string `xml:"CacheClusterStatus"`
			Engine             string `xml:"Engine"`
			ARN                string `xml:"ARN"`
		} `xml:"CreateCacheClusterResult>CacheCluster"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster", cr.CacheCluster.CacheClusterId)
	assert.Equal(t, "available", cr.CacheCluster.CacheClusterStatus)
	assert.Equal(t, "redis", cr.CacheCluster.Engine)
	assert.Contains(t, cr.CacheCluster.ARN, "arn:aws:elasticache")

	// Describe
	descResp := callQuery(t, p, "DescribeCacheClusters", map[string]string{
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		CacheClusters []struct {
			CacheClusterId string `xml:"CacheClusterId"`
		} `xml:"DescribeCacheClustersResult>CacheClusters>CacheCluster"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.CacheClusters, 1)
	assert.Equal(t, "my-cluster", dr.CacheClusters[0].CacheClusterId)

	// Duplicate create
	dupResp := callQuery(t, p, "CreateCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
		"EngineVersion":  "7.2",
	})
	assert.Equal(t, 200, modResp.StatusCode, string(modResp.Body))

	// Reboot
	rebootResp := callQuery(t, p, "RebootCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 200, rebootResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	type deleteResp struct {
		CacheCluster struct {
			CacheClusterId string `xml:"CacheClusterId"`
		} `xml:"DeleteCacheClusterResult>CacheCluster"`
	}
	var del deleteResp
	require.NoError(t, xml.Unmarshal(delResp.Body, &del))
	assert.Equal(t, "my-cluster", del.CacheCluster.CacheClusterId)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteCacheCluster", map[string]string{
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestReplicationGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateReplicationGroup", map[string]string{
		"ReplicationGroupId":          "my-rg",
		"ReplicationGroupDescription": "Test RG",
		"Engine":                      "redis",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		ReplicationGroup struct {
			ReplicationGroupId string `xml:"ReplicationGroupId"`
			Status             string `xml:"Status"`
			ARN                string `xml:"ARN"`
		} `xml:"CreateReplicationGroupResult>ReplicationGroup"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-rg", cr.ReplicationGroup.ReplicationGroupId)
	assert.Equal(t, "available", cr.ReplicationGroup.Status)
	assert.Contains(t, cr.ReplicationGroup.ARN, "arn:aws:elasticache")

	// Describe
	descResp := callQuery(t, p, "DescribeReplicationGroups", map[string]string{
		"ReplicationGroupId": "my-rg",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		ReplicationGroups []struct {
			ReplicationGroupId string `xml:"ReplicationGroupId"`
		} `xml:"DescribeReplicationGroupsResult>ReplicationGroups>ReplicationGroup"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.ReplicationGroups, 1)
	assert.Equal(t, "my-rg", dr.ReplicationGroups[0].ReplicationGroupId)

	// Duplicate
	dupResp := callQuery(t, p, "CreateReplicationGroup", map[string]string{
		"ReplicationGroupId":          "my-rg",
		"ReplicationGroupDescription": "dup",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyReplicationGroup", map[string]string{
		"ReplicationGroupId":          "my-rg",
		"ReplicationGroupDescription": "Updated",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// IncreaseReplicaCount
	incResp := callQuery(t, p, "IncreaseReplicaCount", map[string]string{
		"ReplicationGroupId": "my-rg",
	})
	assert.Equal(t, 200, incResp.StatusCode)

	// DecreaseReplicaCount
	decResp := callQuery(t, p, "DecreaseReplicaCount", map[string]string{
		"ReplicationGroupId": "my-rg",
	})
	assert.Equal(t, 200, decResp.StatusCode)

	// TestFailover
	tfResp := callQuery(t, p, "TestFailover", map[string]string{
		"ReplicationGroupId": "my-rg",
		"NodeGroupId":        "0001",
	})
	assert.Equal(t, 200, tfResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteReplicationGroup", map[string]string{
		"ReplicationGroupId": "my-rg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Not found
	notFoundResp := callQuery(t, p, "DeleteReplicationGroup", map[string]string{
		"ReplicationGroupId": "my-rg",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestParameterGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateCacheParameterGroup", map[string]string{
		"CacheParameterGroupName":   "my-pg",
		"CacheParameterGroupFamily": "redis7",
		"Description":               "Test PG",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		CacheParameterGroup struct {
			CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
			CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
			ARN                       string `xml:"ARN"`
		} `xml:"CreateCacheParameterGroupResult>CacheParameterGroup"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-pg", cr.CacheParameterGroup.CacheParameterGroupName)
	assert.Equal(t, "redis7", cr.CacheParameterGroup.CacheParameterGroupFamily)

	// Describe
	descResp := callQuery(t, p, "DescribeCacheParameterGroups", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		CacheParameterGroups []struct {
			CacheParameterGroupName string `xml:"CacheParameterGroupName"`
		} `xml:"DescribeCacheParameterGroupsResult>CacheParameterGroups>CacheParameterGroup"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.CacheParameterGroups, 1)
	assert.Equal(t, "my-pg", dr.CacheParameterGroups[0].CacheParameterGroupName)

	// Duplicate
	dupResp := callQuery(t, p, "CreateCacheParameterGroup", map[string]string{
		"CacheParameterGroupName":   "my-pg",
		"CacheParameterGroupFamily": "redis7",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyCacheParameterGroup", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Reset
	resetResp := callQuery(t, p, "ResetCacheParameterGroup", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, resetResp.StatusCode)

	// DescribeCacheParameters
	paramsResp := callQuery(t, p, "DescribeCacheParameters", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, paramsResp.StatusCode)

	// DescribeEngineDefaultParameters
	engDefaultResp := callQuery(t, p, "DescribeEngineDefaultParameters", map[string]string{
		"CacheParameterGroupFamily": "redis7",
	})
	assert.Equal(t, 200, engDefaultResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteCacheParameterGroup", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Not found
	notFoundResp := callQuery(t, p, "DeleteCacheParameterGroup", map[string]string{
		"CacheParameterGroupName": "my-pg",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestSubnetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName":        "my-sg",
		"CacheSubnetGroupDescription": "Test SG",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		CacheSubnetGroup struct {
			CacheSubnetGroupName        string `xml:"CacheSubnetGroupName"`
			CacheSubnetGroupDescription string `xml:"CacheSubnetGroupDescription"`
			ARN                         string `xml:"ARN"`
		} `xml:"CreateCacheSubnetGroupResult>CacheSubnetGroup"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-sg", cr.CacheSubnetGroup.CacheSubnetGroupName)

	// Describe
	descResp := callQuery(t, p, "DescribeCacheSubnetGroups", map[string]string{
		"CacheSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		CacheSubnetGroups []struct {
			CacheSubnetGroupName string `xml:"CacheSubnetGroupName"`
		} `xml:"DescribeCacheSubnetGroupsResult>CacheSubnetGroups>CacheSubnetGroup"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.CacheSubnetGroups, 1)
	assert.Equal(t, "my-sg", dr.CacheSubnetGroups[0].CacheSubnetGroupName)

	// Duplicate
	dupResp := callQuery(t, p, "CreateCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName":        "my-sg",
		"CacheSubnetGroupDescription": "dup",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName":        "my-sg",
		"CacheSubnetGroupDescription": "Updated",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Not found
	notFoundResp := callQuery(t, p, "DeleteCacheSubnetGroup", map[string]string{
		"CacheSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestUserAndUserGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create user
	resp := callQuery(t, p, "CreateUser", map[string]string{
		"UserId":       "my-user",
		"UserName":     "myuser",
		"Engine":       "redis",
		"AccessString": "on ~* +@all",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createUserResp struct {
		User struct {
			UserId   string `xml:"UserId"`
			UserName string `xml:"UserName"`
			Status   string `xml:"Status"`
			ARN      string `xml:"ARN"`
		} `xml:"CreateUserResult>User"`
	}
	var cur createUserResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cur))
	assert.Equal(t, "my-user", cur.User.UserId)
	assert.Equal(t, "myuser", cur.User.UserName)
	assert.Equal(t, "active", cur.User.Status)

	// Describe users
	descResp := callQuery(t, p, "DescribeUsers", map[string]string{
		"UserId": "my-user",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type describeUsersResp struct {
		Users []struct {
			UserId string `xml:"UserId"`
		} `xml:"DescribeUsersResult>Users>member"`
	}
	var dur describeUsersResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dur))
	require.Len(t, dur.Users, 1)
	assert.Equal(t, "my-user", dur.Users[0].UserId)

	// Duplicate user
	dupResp := callQuery(t, p, "CreateUser", map[string]string{
		"UserId":   "my-user",
		"UserName": "myuser",
		"Engine":   "redis",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify user
	modResp := callQuery(t, p, "ModifyUser", map[string]string{
		"UserId":       "my-user",
		"AccessString": "on ~* +@read",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Create user group
	ugResp := callQuery(t, p, "CreateUserGroup", map[string]string{
		"UserGroupId": "my-ug",
		"Engine":      "redis",
	})
	assert.Equal(t, 200, ugResp.StatusCode, string(ugResp.Body))

	type createUGResp struct {
		UserGroup struct {
			UserGroupId string `xml:"UserGroupId"`
			Status      string `xml:"Status"`
			ARN         string `xml:"ARN"`
		} `xml:"CreateUserGroupResult>UserGroup"`
	}
	var cugr createUGResp
	require.NoError(t, xml.Unmarshal(ugResp.Body, &cugr))
	assert.Equal(t, "my-ug", cugr.UserGroup.UserGroupId)
	assert.Equal(t, "active", cugr.UserGroup.Status)

	// Describe user groups
	descUGResp := callQuery(t, p, "DescribeUserGroups", map[string]string{
		"UserGroupId": "my-ug",
	})
	assert.Equal(t, 200, descUGResp.StatusCode)

	type describeUGResp struct {
		UserGroups []struct {
			UserGroupId string `xml:"UserGroupId"`
		} `xml:"DescribeUserGroupsResult>UserGroups>member"`
	}
	var dugr describeUGResp
	require.NoError(t, xml.Unmarshal(descUGResp.Body, &dugr))
	require.Len(t, dugr.UserGroups, 1)
	assert.Equal(t, "my-ug", dugr.UserGroups[0].UserGroupId)

	// Modify user group
	modUGResp := callQuery(t, p, "ModifyUserGroup", map[string]string{
		"UserGroupId": "my-ug",
	})
	assert.Equal(t, 200, modUGResp.StatusCode)

	// Delete user group
	delUGResp := callQuery(t, p, "DeleteUserGroup", map[string]string{
		"UserGroupId": "my-ug",
	})
	assert.Equal(t, 200, delUGResp.StatusCode)

	// Not found user group
	notFoundUGResp := callQuery(t, p, "DeleteUserGroup", map[string]string{
		"UserGroupId": "my-ug",
	})
	assert.Equal(t, 404, notFoundUGResp.StatusCode)

	// Delete user
	delResp := callQuery(t, p, "DeleteUser", map[string]string{
		"UserId": "my-user",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Not found user
	notFoundResp := callQuery(t, p, "DeleteUser", map[string]string{
		"UserId": "my-user",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateSnapshot", map[string]string{
		"SnapshotName":   "my-snap",
		"CacheClusterId": "my-cluster",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		Snapshot struct {
			SnapshotName   string `xml:"SnapshotName"`
			CacheClusterId string `xml:"CacheClusterId"`
			SnapshotStatus string `xml:"SnapshotStatus"`
			ARN            string `xml:"ARN"`
		} `xml:"CreateSnapshotResult>Snapshot"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-snap", cr.Snapshot.SnapshotName)
	assert.Equal(t, "my-cluster", cr.Snapshot.CacheClusterId)
	assert.Equal(t, "available", cr.Snapshot.SnapshotStatus)
	assert.Contains(t, cr.Snapshot.ARN, "arn:aws:elasticache")

	// Describe by name
	descResp := callQuery(t, p, "DescribeSnapshots", map[string]string{
		"SnapshotName": "my-snap",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		Snapshots []struct {
			SnapshotName string `xml:"SnapshotName"`
		} `xml:"DescribeSnapshotsResult>SnapShotList>Snapshot"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.Snapshots, 1)
	assert.Equal(t, "my-snap", dr.Snapshots[0].SnapshotName)

	// Duplicate
	dupResp := callQuery(t, p, "CreateSnapshot", map[string]string{
		"SnapshotName": "my-snap",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Copy
	copyResp := callQuery(t, p, "CopySnapshot", map[string]string{
		"SourceSnapshotName": "my-snap",
		"TargetSnapshotName": "my-snap-copy",
	})
	assert.Equal(t, 200, copyResp.StatusCode, string(copyResp.Body))

	// Delete
	delResp := callQuery(t, p, "DeleteSnapshot", map[string]string{
		"SnapshotName": "my-snap",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Not found
	notFoundResp := callQuery(t, p, "DeleteSnapshot", map[string]string{
		"SnapshotName": "my-snap",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a cluster to get its ARN
	resp := callQuery(t, p, "CreateCacheCluster", map[string]string{
		"CacheClusterId": "tag-cluster",
		"Engine":         "redis",
	})
	require.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		CacheCluster struct {
			ARN string `xml:"ARN"`
		} `xml:"CreateCacheClusterResult>CacheCluster"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	arn := cr.CacheCluster.ARN
	require.NotEmpty(t, arn)

	// AddTags
	addResp := callQuery(t, p, "AddTagsToResource", map[string]string{
		"ResourceName":        arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "test",
		"Tags.member.2.Key":   "project",
		"Tags.member.2.Value": "devcloud",
	})
	assert.Equal(t, 200, addResp.StatusCode, string(addResp.Body))

	// ListTags
	listResp := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, listResp.StatusCode, string(listResp.Body))

	type listTagsResp struct {
		TagList []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>TagList>Tag"`
	}
	var ltr listTagsResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &ltr))
	assert.Len(t, ltr.TagList, 2)

	// RemoveTags
	removeResp := callQuery(t, p, "RemoveTagsFromResource", map[string]string{
		"ResourceName":     arn,
		"TagKeys.member.1": "env",
	})
	assert.Equal(t, 200, removeResp.StatusCode, string(removeResp.Body))

	// ListTags again - should have 1 tag
	listResp2 := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, listResp2.StatusCode)
	var ltr2 listTagsResp
	require.NoError(t, xml.Unmarshal(listResp2.Body, &ltr2))
	assert.Len(t, ltr2.TagList, 1)
	assert.Equal(t, "project", ltr2.TagList[0].Key)
	assert.Equal(t, "devcloud", ltr2.TagList[0].Value)
}
