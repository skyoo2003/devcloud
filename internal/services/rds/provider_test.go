// SPDX-License-Identifier: Apache-2.0

package rds

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

func TestDBInstanceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
		"Engine":               "mysql",
		"DBInstanceClass":      "db.t3.medium",
		"MasterUsername":       "admin",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBInstance struct {
			DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
			DBInstanceStatus     string `xml:"DBInstanceStatus"`
			Engine               string `xml:"Engine"`
			DBInstanceArn        string `xml:"DBInstanceArn"`
		} `xml:"CreateDBInstanceResult>DBInstance"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-instance", cr.DBInstance.DBInstanceIdentifier)
	assert.Equal(t, "available", cr.DBInstance.DBInstanceStatus)
	assert.Equal(t, "mysql", cr.DBInstance.Engine)
	assert.Contains(t, cr.DBInstance.DBInstanceArn, "arn:aws:rds")

	// Describe
	descResp := callQuery(t, p, "DescribeDBInstances", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		DBInstances []struct {
			DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
		} `xml:"DescribeDBInstancesResult>DBInstances>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBInstances, 1)
	assert.Equal(t, "my-instance", dr.DBInstances[0].DBInstanceIdentifier)

	// Duplicate create
	dupResp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
		"DBInstanceClass":      "db.r6g.large",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Reboot
	rebootResp := callQuery(t, p, "RebootDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, rebootResp.StatusCode)

	// Stop / Start
	stopResp := callQuery(t, p, "StopDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, stopResp.StatusCode)

	startResp := callQuery(t, p, "StartDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, startResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone — should return 404 with DBInstanceNotFound
	goneResp := callQuery(t, p, "DescribeDBInstances", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestDBClusterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"Engine":              "aurora-mysql",
		"MasterUsername":      "admin",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBCluster struct {
			DBClusterIdentifier string `xml:"DBClusterIdentifier"`
			Status              string `xml:"Status"`
			Engine              string `xml:"Engine"`
			DBClusterArn        string `xml:"DBClusterArn"`
		} `xml:"CreateDBClusterResult>DBCluster"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster", cr.DBCluster.DBClusterIdentifier)
	assert.Equal(t, "available", cr.DBCluster.Status)
	assert.Equal(t, "aurora-mysql", cr.DBCluster.Engine)
	assert.Contains(t, cr.DBCluster.DBClusterArn, "arn:aws:rds")

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusters", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		DBClusters []struct {
			DBClusterIdentifier string `xml:"DBClusterIdentifier"`
		} `xml:"DescribeDBClustersResult>DBClusters>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusters, 1)
	assert.Equal(t, "my-cluster", dr.DBClusters[0].DBClusterIdentifier)

	// Duplicate
	dupResp := callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"EngineVersion":       "8.0.mysql_aurora.3.08.0",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Stop / Start / Failover / Reboot
	stopResp := callQuery(t, p, "StopDBCluster", map[string]string{"DBClusterIdentifier": "my-cluster"})
	assert.Equal(t, 200, stopResp.StatusCode)

	startResp := callQuery(t, p, "StartDBCluster", map[string]string{"DBClusterIdentifier": "my-cluster"})
	assert.Equal(t, 200, startResp.StatusCode)

	failoverResp := callQuery(t, p, "FailoverDBCluster", map[string]string{"DBClusterIdentifier": "my-cluster"})
	assert.Equal(t, 200, failoverResp.StatusCode)

	rebootResp := callQuery(t, p, "RebootDBCluster", map[string]string{"DBClusterIdentifier": "my-cluster"})
	assert.Equal(t, 200, rebootResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestDBSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance first
	instResp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "snap-inst",
		"Engine":               "mysql",
	})
	require.Equal(t, 200, instResp.StatusCode)

	// Create snapshot
	resp := callQuery(t, p, "CreateDBSnapshot", map[string]string{
		"DBSnapshotIdentifier": "my-snap",
		"DBInstanceIdentifier": "snap-inst",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBSnapshot struct {
			DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
			DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
			Status               string `xml:"Status"`
		} `xml:"CreateDBSnapshotResult>DBSnapshot"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-snap", cr.DBSnapshot.DBSnapshotIdentifier)
	assert.Equal(t, "snap-inst", cr.DBSnapshot.DBInstanceIdentifier)
	assert.Equal(t, "available", cr.DBSnapshot.Status)

	// Describe
	descResp := callQuery(t, p, "DescribeDBSnapshots", map[string]string{
		"DBInstanceIdentifier": "snap-inst",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		DBSnapshots []struct {
			DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
		} `xml:"DescribeDBSnapshotsResult>DBSnapshots>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBSnapshots, 1)

	// Copy
	copyResp := callQuery(t, p, "CopyDBSnapshot", map[string]string{
		"SourceDBSnapshotIdentifier": "my-snap",
		"TargetDBSnapshotIdentifier": "my-snap-copy",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBSnapshot", map[string]string{
		"DBSnapshotIdentifier": "my-snap",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteDBSnapshot", map[string]string{
		"DBSnapshotIdentifier": "my-snap",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestClusterSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster first
	clusterResp := callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "snap-cluster",
		"Engine":              "aurora-mysql",
	})
	require.Equal(t, 200, clusterResp.StatusCode)

	// Create cluster snapshot
	resp := callQuery(t, p, "CreateDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "my-cluster-snap",
		"DBClusterIdentifier":         "snap-cluster",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBClusterSnapshot struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
			DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
			Status                      string `xml:"Status"`
		} `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster-snap", cr.DBClusterSnapshot.DBClusterSnapshotIdentifier)
	assert.Equal(t, "snap-cluster", cr.DBClusterSnapshot.DBClusterIdentifier)
	assert.Equal(t, "available", cr.DBClusterSnapshot.Status)

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusterSnapshots", map[string]string{
		"DBClusterIdentifier": "snap-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		DBClusterSnapshots []struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
		} `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusterSnapshots, 1)

	// Copy
	copyResp := callQuery(t, p, "CopyDBClusterSnapshot", map[string]string{
		"SourceDBClusterSnapshotIdentifier": "my-cluster-snap",
		"TargetDBClusterSnapshotIdentifier": "my-cluster-snap-copy",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "my-cluster-snap",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "my-cluster-snap",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestParameterGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create DB parameter group
	resp := callQuery(t, p, "CreateDBParameterGroup", map[string]string{
		"DBParameterGroupName":   "my-pg",
		"DBParameterGroupFamily": "mysql8.0",
		"Description":            "test pg",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createPGResp struct {
		DBParameterGroup struct {
			DBParameterGroupName   string `xml:"DBParameterGroupName"`
			DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
		} `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
	}
	var cr createPGResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-pg", cr.DBParameterGroup.DBParameterGroupName)
	assert.Equal(t, "mysql8.0", cr.DBParameterGroup.DBParameterGroupFamily)

	// Describe
	descResp := callQuery(t, p, "DescribeDBParameterGroups", map[string]string{
		"DBParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Reset
	resetResp := callQuery(t, p, "ResetDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, resetResp.StatusCode)

	// Copy
	copyResp := callQuery(t, p, "CopyDBParameterGroup", map[string]string{
		"SourceDBParameterGroupIdentifier":  "my-pg",
		"TargetDBParameterGroupIdentifier":  "my-pg-copy",
		"TargetDBParameterGroupDescription": "copy",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-pg",
	})
	assert.Equal(t, 404, goneResp.StatusCode)

	// Cluster parameter group
	cResp := callQuery(t, p, "CreateDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cpg",
		"DBParameterGroupFamily":      "aurora-mysql8.0",
		"Description":                 "test cpg",
	})
	assert.Equal(t, 200, cResp.StatusCode)

	// Describe cluster param groups
	cdResp := callQuery(t, p, "DescribeDBClusterParameterGroups", map[string]string{
		"DBClusterParameterGroupName": "my-cpg",
	})
	assert.Equal(t, 200, cdResp.StatusCode)

	// Copy cluster param group
	ccResp := callQuery(t, p, "CopyDBClusterParameterGroup", map[string]string{
		"SourceDBClusterParameterGroupIdentifier":  "my-cpg",
		"TargetDBClusterParameterGroupIdentifier":  "my-cpg-copy",
		"TargetDBClusterParameterGroupDescription": "copy",
	})
	assert.Equal(t, 200, ccResp.StatusCode)

	// Delete cluster param group
	cdDelResp := callQuery(t, p, "DeleteDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cpg",
	})
	assert.Equal(t, 200, cdDelResp.StatusCode)
}

func TestSubnetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":        "my-sg",
		"DBSubnetGroupDescription": "test subnet group",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBSubnetGroup struct {
			DBSubnetGroupName        string `xml:"DBSubnetGroupName"`
			DBSubnetGroupDescription string `xml:"DBSubnetGroupDescription"`
		} `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-sg", cr.DBSubnetGroup.DBSubnetGroupName)

	// Describe
	descResp := callQuery(t, p, "DescribeDBSubnetGroups", map[string]string{
		"DBSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		DBSubnetGroups []struct {
			DBSubnetGroupName string `xml:"DBSubnetGroupName"`
		} `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBSubnetGroups, 1)
	assert.Equal(t, "my-sg", dr.DBSubnetGroups[0].DBSubnetGroupName)

	// Modify
	modResp := callQuery(t, p, "ModifyDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":        "my-sg",
		"DBSubnetGroupDescription": "updated description",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBSubnetGroup", map[string]string{
		"DBSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteDBSubnetGroup", map[string]string{
		"DBSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestOptionGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateOptionGroup", map[string]string{
		"OptionGroupName":        "my-og",
		"EngineName":             "mysql",
		"MajorEngineVersion":     "8.0",
		"OptionGroupDescription": "test option group",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		OptionGroup struct {
			OptionGroupName    string `xml:"OptionGroupName"`
			EngineName         string `xml:"EngineName"`
			MajorEngineVersion string `xml:"MajorEngineVersion"`
		} `xml:"CreateOptionGroupResult>OptionGroup"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-og", cr.OptionGroup.OptionGroupName)
	assert.Equal(t, "mysql", cr.OptionGroup.EngineName)
	assert.Equal(t, "8.0", cr.OptionGroup.MajorEngineVersion)

	// Describe
	descResp := callQuery(t, p, "DescribeOptionGroups", map[string]string{
		"OptionGroupName": "my-og",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descResp2 struct {
		OptionGroupsList []struct {
			OptionGroupName string `xml:"OptionGroupName"`
		} `xml:"DescribeOptionGroupsResult>OptionGroupsList>member"`
	}
	var dr descResp2
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.OptionGroupsList, 1)

	// Modify
	modResp := callQuery(t, p, "ModifyOptionGroup", map[string]string{
		"OptionGroupName": "my-og",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Copy
	copyResp := callQuery(t, p, "CopyOptionGroup", map[string]string{
		"SourceOptionGroupIdentifier":  "my-og",
		"TargetOptionGroupIdentifier":  "my-og-copy",
		"TargetOptionGroupDescription": "copy",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteOptionGroup", map[string]string{
		"OptionGroupName": "my-og",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	goneResp := callQuery(t, p, "DeleteOptionGroup", map[string]string{
		"OptionGroupName": "my-og",
	})
	assert.Equal(t, 404, goneResp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create an instance to tag
	instResp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "tag-inst",
		"Engine":               "mysql",
	})
	require.Equal(t, 200, instResp.StatusCode)

	type createResp struct {
		DBInstance struct {
			DBInstanceArn string `xml:"DBInstanceArn"`
		} `xml:"CreateDBInstanceResult>DBInstance"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(instResp.Body, &cr))
	arn := cr.DBInstance.DBInstanceArn
	require.NotEmpty(t, arn)

	// Add tags
	addResp := callQuery(t, p, "AddTagsToResource", map[string]string{
		"ResourceName":        arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "test",
		"Tags.member.2.Key":   "team",
		"Tags.member.2.Value": "platform",
	})
	assert.Equal(t, 200, addResp.StatusCode)

	// List tags
	listResp := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, listResp.StatusCode)

	type listTagsResp struct {
		TagList []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>TagList>member"`
	}
	var lr listTagsResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &lr))
	assert.Len(t, lr.TagList, 2)

	// Remove tag
	removeResp := callQuery(t, p, "RemoveTagsFromResource", map[string]string{
		"ResourceName":     arn,
		"TagKeys.member.1": "env",
	})
	assert.Equal(t, 200, removeResp.StatusCode)

	// Verify one tag left
	listResp2 := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, listResp2.StatusCode)
	var lr2 listTagsResp
	require.NoError(t, xml.Unmarshal(listResp2.Body, &lr2))
	assert.Len(t, lr2.TagList, 1)
	assert.Equal(t, "team", lr2.TagList[0].Key)
}
