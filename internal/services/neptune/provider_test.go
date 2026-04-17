// SPDX-License-Identifier: Apache-2.0

package neptune

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

func TestDBClusterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"Engine":              "neptune",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		DBCluster struct {
			DBClusterIdentifier string `xml:"DBClusterIdentifier"`
			Status              string `xml:"Status"`
			Engine              string `xml:"Engine"`
			DBClusterArn        string `xml:"DBClusterArn"`
			Port                int    `xml:"Port"`
		} `xml:"CreateDBClusterResult>DBCluster"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster", cr.DBCluster.DBClusterIdentifier)
	assert.Equal(t, "available", cr.DBCluster.Status)
	assert.Equal(t, "neptune", cr.DBCluster.Engine)
	assert.Equal(t, 8182, cr.DBCluster.Port)
	assert.Contains(t, cr.DBCluster.DBClusterArn, "arn:aws:rds")

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusters", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type describeResp struct {
		DBClusters []struct {
			DBClusterIdentifier string `xml:"DBClusterIdentifier"`
		} `xml:"DescribeDBClustersResult>DBClusters>member"`
	}
	var dr describeResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusters, 1)
	assert.Equal(t, "my-cluster", dr.DBClusters[0].DBClusterIdentifier)

	// Duplicate create
	dupResp := callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"EngineVersion":       "1.3.1.0",
	})
	assert.Equal(t, 200, modResp.StatusCode, string(modResp.Body))

	// Start / Stop
	stopResp := callQuery(t, p, "StopDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, stopResp.StatusCode)

	startResp := callQuery(t, p, "StartDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, startResp.StatusCode)

	// Failover
	failoverResp := callQuery(t, p, "FailoverDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, failoverResp.StatusCode)

	// PromoteReadReplica
	promoteResp := callQuery(t, p, "PromoteReadReplicaDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, promoteResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	type deleteResp struct {
		DBCluster struct {
			DBClusterIdentifier string `xml:"DBClusterIdentifier"`
		} `xml:"DeleteDBClusterResult>DBCluster"`
	}
	var del deleteResp
	require.NoError(t, xml.Unmarshal(delResp.Body, &del))
	assert.Equal(t, "my-cluster", del.DBCluster.DBClusterIdentifier)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestDBInstanceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create instance
	resp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
		"DBClusterIdentifier":  "my-cluster",
		"DBInstanceClass":      "db.r6g.large",
		"Engine":               "neptune",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createInstResp struct {
		DBInstance struct {
			DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
			DBInstanceStatus     string `xml:"DBInstanceStatus"`
			DBInstanceClass      string `xml:"DBInstanceClass"`
			DBInstanceArn        string `xml:"DBInstanceArn"`
			Endpoint             struct {
				Port int `xml:"Port"`
			} `xml:"Endpoint"`
		} `xml:"CreateDBInstanceResult>DBInstance"`
	}
	var cr createInstResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-instance", cr.DBInstance.DBInstanceIdentifier)
	assert.Equal(t, "available", cr.DBInstance.DBInstanceStatus)
	assert.Equal(t, "db.r6g.large", cr.DBInstance.DBInstanceClass)
	assert.Equal(t, 8182, cr.DBInstance.Endpoint.Port)
	assert.Contains(t, cr.DBInstance.DBInstanceArn, "arn:aws:rds")

	// Describe
	descResp := callQuery(t, p, "DescribeDBInstances", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descInstResp struct {
		DBInstances []struct {
			DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
		} `xml:"DescribeDBInstancesResult>DBInstances>member"`
	}
	var dr descInstResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBInstances, 1)
	assert.Equal(t, "my-instance", dr.DBInstances[0].DBInstanceIdentifier)

	// Duplicate
	dupResp := callQuery(t, p, "CreateDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Modify
	modResp := callQuery(t, p, "ModifyDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
		"DBInstanceClass":      "db.r6g.xlarge",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Reboot
	rebootResp := callQuery(t, p, "RebootDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, rebootResp.StatusCode)

	// DescribeValidDBInstanceModifications
	validModResp := callQuery(t, p, "DescribeValidDBInstanceModifications", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, validModResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBInstance", map[string]string{
		"DBInstanceIdentifier": "my-instance",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestClusterSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster first
	callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"Engine":              "neptune",
	})

	// Create snapshot
	resp := callQuery(t, p, "CreateDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-1",
		"DBClusterIdentifier":         "my-cluster",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createSnapResp struct {
		DBClusterSnapshot struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
			DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
			Status                      string `xml:"Status"`
			DBClusterSnapshotArn        string `xml:"DBClusterSnapshotArn"`
			Engine                      string `xml:"Engine"`
		} `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	var cr createSnapResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "snap-1", cr.DBClusterSnapshot.DBClusterSnapshotIdentifier)
	assert.Equal(t, "my-cluster", cr.DBClusterSnapshot.DBClusterIdentifier)
	assert.Equal(t, "available", cr.DBClusterSnapshot.Status)
	assert.Equal(t, "neptune", cr.DBClusterSnapshot.Engine)
	assert.Contains(t, cr.DBClusterSnapshot.DBClusterSnapshotArn, "arn:aws:rds")

	// Cluster not found
	noClusterResp := callQuery(t, p, "CreateDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-x",
		"DBClusterIdentifier":         "nonexistent",
	})
	assert.Equal(t, 404, noClusterResp.StatusCode)

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusterSnapshots", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descSnapResp struct {
		DBClusterSnapshots []struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
		} `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots>member"`
	}
	var dr descSnapResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusterSnapshots, 1)
	assert.Equal(t, "snap-1", dr.DBClusterSnapshots[0].DBClusterSnapshotIdentifier)

	// Copy
	copyResp := callQuery(t, p, "CopyDBClusterSnapshot", map[string]string{
		"SourceDBClusterSnapshotIdentifier": "snap-1",
		"TargetDBClusterSnapshotIdentifier": "snap-2",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Snapshot attributes
	attrResp := callQuery(t, p, "DescribeDBClusterSnapshotAttributes", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-1",
	})
	assert.Equal(t, 200, attrResp.StatusCode)

	// Modify snapshot attribute
	modAttrResp := callQuery(t, p, "ModifyDBClusterSnapshotAttribute", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-1",
		"AttributeName":               "restore",
	})
	assert.Equal(t, 200, modAttrResp.StatusCode)

	// Restore cluster from snapshot
	restoreResp := callQuery(t, p, "RestoreDBClusterFromSnapshot", map[string]string{
		"DBClusterIdentifier": "restored-cluster",
		"SnapshotIdentifier":  "snap-1",
		"Engine":              "neptune",
	})
	assert.Equal(t, 200, restoreResp.StatusCode)

	// Delete snapshot
	delResp := callQuery(t, p, "DeleteDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-1",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBClusterSnapshot", map[string]string{
		"DBClusterSnapshotIdentifier": "snap-1",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestSubnetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":        "my-subnet-group",
		"DBSubnetGroupDescription": "Test subnet group",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createSGResp struct {
		DBSubnetGroup struct {
			DBSubnetGroupName        string `xml:"DBSubnetGroupName"`
			DBSubnetGroupDescription string `xml:"DBSubnetGroupDescription"`
			DBSubnetGroupArn         string `xml:"DBSubnetGroupArn"`
		} `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
	}
	var cr createSGResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-subnet-group", cr.DBSubnetGroup.DBSubnetGroupName)
	assert.Equal(t, "Test subnet group", cr.DBSubnetGroup.DBSubnetGroupDescription)
	assert.Contains(t, cr.DBSubnetGroup.DBSubnetGroupArn, "arn:aws:rds")

	// Duplicate
	dupResp := callQuery(t, p, "CreateDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":        "my-subnet-group",
		"DBSubnetGroupDescription": "Dup",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Describe
	descResp := callQuery(t, p, "DescribeDBSubnetGroups", map[string]string{
		"DBSubnetGroupName": "my-subnet-group",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descSGResp struct {
		DBSubnetGroups []struct {
			DBSubnetGroupName string `xml:"DBSubnetGroupName"`
		} `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups>member"`
	}
	var dr descSGResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBSubnetGroups, 1)
	assert.Equal(t, "my-subnet-group", dr.DBSubnetGroups[0].DBSubnetGroupName)

	// Modify
	modResp := callQuery(t, p, "ModifyDBSubnetGroup", map[string]string{
		"DBSubnetGroupName":        "my-subnet-group",
		"DBSubnetGroupDescription": "Updated",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBSubnetGroup", map[string]string{
		"DBSubnetGroupName": "my-subnet-group",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBSubnetGroup", map[string]string{
		"DBSubnetGroupName": "my-subnet-group",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestParameterGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// --- DBClusterParameterGroup ---

	// Create
	resp := callQuery(t, p, "CreateDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
		"DBParameterGroupFamily":      "neptune1.3",
		"Description":                 "Test cluster param group",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createClusterPGResp struct {
		DBClusterParameterGroup struct {
			DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
			DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
			DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn"`
		} `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
	}
	var cr createClusterPGResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster-pg", cr.DBClusterParameterGroup.DBClusterParameterGroupName)
	assert.Equal(t, "neptune1.3", cr.DBClusterParameterGroup.DBParameterGroupFamily)
	assert.Contains(t, cr.DBClusterParameterGroup.DBClusterParameterGroupArn, "arn:aws:rds")

	// Duplicate
	dupResp := callQuery(t, p, "CreateDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
		"DBParameterGroupFamily":      "neptune1.3",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusterParameterGroups", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descClusterPGResp struct {
		DBClusterParameterGroups []struct {
			DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
		} `xml:"DescribeDBClusterParameterGroupsResult>DBClusterParameterGroups>member"`
	}
	var dr descClusterPGResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusterParameterGroups, 1)
	assert.Equal(t, "my-cluster-pg", dr.DBClusterParameterGroups[0].DBClusterParameterGroupName)

	// Modify
	modResp := callQuery(t, p, "ModifyDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Reset
	resetResp := callQuery(t, p, "ResetDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 200, resetResp.StatusCode)

	// Describe parameters
	descParamsResp := callQuery(t, p, "DescribeDBClusterParameters", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 200, descParamsResp.StatusCode)

	// Describe engine defaults
	descDefaultsResp := callQuery(t, p, "DescribeEngineDefaultClusterParameters", map[string]string{
		"DBParameterGroupFamily": "neptune1.3",
	})
	assert.Equal(t, 200, descDefaultsResp.StatusCode)

	// Copy
	copyResp := callQuery(t, p, "CopyDBClusterParameterGroup", map[string]string{
		"SourceDBClusterParameterGroupIdentifier":  "my-cluster-pg",
		"TargetDBClusterParameterGroupIdentifier":  "my-cluster-pg-copy",
		"TargetDBClusterParameterGroupDescription": "Copied group",
	})
	assert.Equal(t, 200, copyResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBClusterParameterGroup", map[string]string{
		"DBClusterParameterGroupName": "my-cluster-pg",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)

	// --- DBParameterGroup (Neptune-specific) ---

	// Create DB param group
	dbPGResp := callQuery(t, p, "CreateDBParameterGroup", map[string]string{
		"DBParameterGroupName":   "my-db-pg",
		"DBParameterGroupFamily": "neptune1.3",
		"Description":            "Test DB param group",
	})
	assert.Equal(t, 200, dbPGResp.StatusCode, string(dbPGResp.Body))

	type createDBPGResp struct {
		DBParameterGroup struct {
			DBParameterGroupName   string `xml:"DBParameterGroupName"`
			DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
			DBParameterGroupArn    string `xml:"DBParameterGroupArn"`
		} `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
	}
	var dbpg createDBPGResp
	require.NoError(t, xml.Unmarshal(dbPGResp.Body, &dbpg))
	assert.Equal(t, "my-db-pg", dbpg.DBParameterGroup.DBParameterGroupName)
	assert.Equal(t, "neptune1.3", dbpg.DBParameterGroup.DBParameterGroupFamily)
	assert.Contains(t, dbpg.DBParameterGroup.DBParameterGroupArn, "arn:aws:rds")

	// Duplicate DB param group
	dupDBPGResp := callQuery(t, p, "CreateDBParameterGroup", map[string]string{
		"DBParameterGroupName":   "my-db-pg",
		"DBParameterGroupFamily": "neptune1.3",
	})
	assert.Equal(t, 409, dupDBPGResp.StatusCode)

	// Describe DB param groups
	descDBPGQueryResp := callQuery(t, p, "DescribeDBParameterGroups", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 200, descDBPGQueryResp.StatusCode)

	type descDBPGResp struct {
		DBParameterGroups []struct {
			DBParameterGroupName string `xml:"DBParameterGroupName"`
		} `xml:"DescribeDBParameterGroupsResult>DBParameterGroups>member"`
	}
	var ddr descDBPGResp
	require.NoError(t, xml.Unmarshal(descDBPGQueryResp.Body, &ddr))
	require.Len(t, ddr.DBParameterGroups, 1)
	assert.Equal(t, "my-db-pg", ddr.DBParameterGroups[0].DBParameterGroupName)

	// Modify DB param group
	modDBPGResp := callQuery(t, p, "ModifyDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 200, modDBPGResp.StatusCode)

	// Reset DB param group
	resetDBPGResp := callQuery(t, p, "ResetDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 200, resetDBPGResp.StatusCode)

	// Describe DB parameters
	descDBParamsResp := callQuery(t, p, "DescribeDBParameters", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 200, descDBParamsResp.StatusCode)

	// Describe engine default parameters
	descEngDefaultsResp := callQuery(t, p, "DescribeEngineDefaultParameters", map[string]string{
		"DBParameterGroupFamily": "neptune1.3",
	})
	assert.Equal(t, 200, descEngDefaultsResp.StatusCode)

	// Copy DB param group
	copyDBPGResp := callQuery(t, p, "CopyDBParameterGroup", map[string]string{
		"SourceDBParameterGroupIdentifier":  "my-db-pg",
		"TargetDBParameterGroupIdentifier":  "my-db-pg-copy",
		"TargetDBParameterGroupDescription": "Copied DB param group",
	})
	assert.Equal(t, 200, copyDBPGResp.StatusCode)

	// Delete DB param group
	delDBPGResp := callQuery(t, p, "DeleteDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 200, delDBPGResp.StatusCode)

	// Not found DB param group
	notFoundDBPGResp := callQuery(t, p, "DeleteDBParameterGroup", map[string]string{
		"DBParameterGroupName": "my-db-pg",
	})
	assert.Equal(t, 404, notFoundDBPGResp.StatusCode)
}

func TestClusterEndpointCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create cluster first
	callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "my-cluster",
		"Engine":              "neptune",
	})

	// Create endpoint
	resp := callQuery(t, p, "CreateDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "my-endpoint",
		"DBClusterIdentifier":         "my-cluster",
		"EndpointType":                "READER",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createEPResp struct {
		Endpoint struct {
			DBClusterEndpointIdentifier string `xml:"DBClusterEndpointIdentifier"`
			DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
			Status                      string `xml:"Status"`
			EndpointType                string `xml:"EndpointType"`
			DBClusterEndpointArn        string `xml:"DBClusterEndpointArn"`
		} `xml:"CreateDBClusterEndpointResult"`
	}
	var cr createEPResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-endpoint", cr.Endpoint.DBClusterEndpointIdentifier)
	assert.Equal(t, "my-cluster", cr.Endpoint.DBClusterIdentifier)
	assert.Equal(t, "available", cr.Endpoint.Status)
	assert.Equal(t, "READER", cr.Endpoint.EndpointType)
	assert.Contains(t, cr.Endpoint.DBClusterEndpointArn, "arn:aws:rds")

	// Cluster not found
	noClusterResp := callQuery(t, p, "CreateDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "ep-x",
		"DBClusterIdentifier":         "nonexistent",
	})
	assert.Equal(t, 404, noClusterResp.StatusCode)

	// Duplicate endpoint
	dupResp := callQuery(t, p, "CreateDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "my-endpoint",
		"DBClusterIdentifier":         "my-cluster",
	})
	assert.Equal(t, 409, dupResp.StatusCode)

	// Describe
	descResp := callQuery(t, p, "DescribeDBClusterEndpoints", map[string]string{
		"DBClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descEPResp struct {
		DBClusterEndpoints []struct {
			DBClusterEndpointIdentifier string `xml:"DBClusterEndpointIdentifier"`
		} `xml:"DescribeDBClusterEndpointsResult>DBClusterEndpoints>member"`
	}
	var dr descEPResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dr))
	require.Len(t, dr.DBClusterEndpoints, 1)
	assert.Equal(t, "my-endpoint", dr.DBClusterEndpoints[0].DBClusterEndpointIdentifier)

	// Modify
	modResp := callQuery(t, p, "ModifyDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "my-endpoint",
	})
	assert.Equal(t, 200, modResp.StatusCode)

	// Delete
	delResp := callQuery(t, p, "DeleteDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "my-endpoint",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Not found
	notFoundResp := callQuery(t, p, "DeleteDBClusterEndpoint", map[string]string{
		"DBClusterEndpointIdentifier": "my-endpoint",
	})
	assert.Equal(t, 404, notFoundResp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a cluster to tag
	callQuery(t, p, "CreateDBCluster", map[string]string{
		"DBClusterIdentifier": "tagged-cluster",
		"Engine":              "neptune",
	})

	arn := "arn:aws:rds:us-east-1:000000000000:cluster/tagged-cluster"

	// Add tags
	addResp := callQuery(t, p, "AddTagsToResource", map[string]string{
		"ResourceName":        arn,
		"Tags.member.1.Key":   "Env",
		"Tags.member.1.Value": "test",
		"Tags.member.2.Key":   "Team",
		"Tags.member.2.Value": "platform",
	})
	assert.Equal(t, 200, addResp.StatusCode, string(addResp.Body))

	// List tags
	listResp := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, listResp.StatusCode, string(listResp.Body))

	type listTagsResp struct {
		TagList []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>TagList>member"`
	}
	var lr listTagsResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &lr))
	assert.Len(t, lr.TagList, 2)

	// Remove tags
	removeResp := callQuery(t, p, "RemoveTagsFromResource", map[string]string{
		"ResourceName":     arn,
		"TagKeys.member.1": "Env",
	})
	assert.Equal(t, 200, removeResp.StatusCode)

	// Verify one tag removed
	listResp2 := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceName": arn,
	})
	var lr2 listTagsResp
	require.NoError(t, xml.Unmarshal(listResp2.Body, &lr2))
	assert.Len(t, lr2.TagList, 1)
	assert.Equal(t, "Team", lr2.TagList[0].Key)
}
