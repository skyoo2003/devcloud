// SPDX-License-Identifier: Apache-2.0

package redshift

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

func TestCreateAndDescribeCluster(t *testing.T) {
	p := newTestProvider(t)

	resp := callQuery(t, p, "CreateCluster", map[string]string{
		"ClusterIdentifier": "my-cluster",
		"NodeType":          "dc2.large",
		"MasterUsername":    "admin",
		"DBName":            "mydb",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		Cluster struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
			ClusterStatus     string `xml:"ClusterStatus"`
			DBName            string `xml:"DBName"`
			ClusterArn        string `xml:"ClusterArn"`
		} `xml:"CreateClusterResult>Cluster"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-cluster", cr.Cluster.ClusterIdentifier)
	assert.Equal(t, "available", cr.Cluster.ClusterStatus)
	assert.Equal(t, "mydb", cr.Cluster.DBName)
	assert.Contains(t, cr.Cluster.ClusterArn, "arn:aws:redshift")

	// DescribeClusters
	descClustersResp := callQuery(t, p, "DescribeClusters", map[string]string{
		"ClusterIdentifier": "my-cluster",
	})
	assert.Equal(t, 200, descClustersResp.StatusCode, string(descClustersResp.Body))

	type describeClustersResult struct {
		Clusters []struct {
			ClusterIdentifier string `xml:"ClusterIdentifier"`
		} `xml:"DescribeClustersResult>Clusters>member"`
	}
	var dr describeClustersResult
	require.NoError(t, xml.Unmarshal(descClustersResp.Body, &dr))
	require.Len(t, dr.Clusters, 1)
	assert.Equal(t, "my-cluster", dr.Clusters[0].ClusterIdentifier)
}

func TestDeleteCluster(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateCluster", map[string]string{"ClusterIdentifier": "del-cluster"})

	resp := callQuery(t, p, "DeleteCluster", map[string]string{
		"ClusterIdentifier": "del-cluster",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	// Should be gone now — returns 404 with ClusterNotFound
	descAfterDel := callQuery(t, p, "DescribeClusters", map[string]string{
		"ClusterIdentifier": "del-cluster",
	})
	assert.Equal(t, 404, descAfterDel.StatusCode)
}

func TestPauseResumeCluster(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateCluster", map[string]string{"ClusterIdentifier": "pause-cluster"})

	// Pause
	pauseResp := callQuery(t, p, "PauseCluster", map[string]string{
		"ClusterIdentifier": "pause-cluster",
	})
	assert.Equal(t, 200, pauseResp.StatusCode, string(pauseResp.Body))

	type clusterResp struct {
		Cluster struct {
			ClusterStatus string `xml:"ClusterStatus"`
		} `xml:"PauseClusterResult>Cluster"`
	}
	var pr clusterResp
	require.NoError(t, xml.Unmarshal(pauseResp.Body, &pr))
	assert.Equal(t, "paused", pr.Cluster.ClusterStatus)

	// Resume
	resumeResp := callQuery(t, p, "ResumeCluster", map[string]string{
		"ClusterIdentifier": "pause-cluster",
	})
	assert.Equal(t, 200, resumeResp.StatusCode, string(resumeResp.Body))

	type resumeClusterResp struct {
		Cluster struct {
			ClusterStatus string `xml:"ClusterStatus"`
		} `xml:"ResumeClusterResult>Cluster"`
	}
	var rr resumeClusterResp
	require.NoError(t, xml.Unmarshal(resumeResp.Body, &rr))
	assert.Equal(t, "available", rr.Cluster.ClusterStatus)
}

func TestSnapshotCRUD(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateCluster", map[string]string{"ClusterIdentifier": "snap-cluster"})

	// Create snapshot
	createResp := callQuery(t, p, "CreateClusterSnapshot", map[string]string{
		"SnapshotIdentifier": "my-snap",
		"ClusterIdentifier":  "snap-cluster",
	})
	assert.Equal(t, 200, createResp.StatusCode, string(createResp.Body))

	type createSnapResp struct {
		Snapshot struct {
			SnapshotIdentifier string `xml:"SnapshotIdentifier"`
			ClusterIdentifier  string `xml:"ClusterIdentifier"`
			Status             string `xml:"Status"`
		} `xml:"CreateClusterSnapshotResult>Snapshot"`
	}
	var csr createSnapResp
	require.NoError(t, xml.Unmarshal(createResp.Body, &csr))
	assert.Equal(t, "my-snap", csr.Snapshot.SnapshotIdentifier)
	assert.Equal(t, "snap-cluster", csr.Snapshot.ClusterIdentifier)
	assert.Equal(t, "available", csr.Snapshot.Status)

	// Describe snapshots
	descResp := callQuery(t, p, "DescribeClusterSnapshots", map[string]string{
		"ClusterIdentifier": "snap-cluster",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descSnapsResp struct {
		Snapshots []struct {
			SnapshotIdentifier string `xml:"SnapshotIdentifier"`
		} `xml:"DescribeClusterSnapshotsResult>Snapshots>member"`
	}
	var dsr descSnapsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dsr))
	require.Len(t, dsr.Snapshots, 1)
	assert.Equal(t, "my-snap", dsr.Snapshots[0].SnapshotIdentifier)

	// Delete snapshot
	delResp := callQuery(t, p, "DeleteClusterSnapshot", map[string]string{
		"SnapshotIdentifier": "my-snap",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))
}

func TestParameterGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createResp := callQuery(t, p, "CreateClusterParameterGroup", map[string]string{
		"ParameterGroupName":   "my-pg",
		"ParameterGroupFamily": "redshift-1.0",
		"Description":          "test group",
	})
	assert.Equal(t, 200, createResp.StatusCode, string(createResp.Body))

	type createPGResp struct {
		ParameterGroup struct {
			ParameterGroupName   string `xml:"ParameterGroupName"`
			ParameterGroupFamily string `xml:"ParameterGroupFamily"`
		} `xml:"CreateClusterParameterGroupResult>ClusterParameterGroup"`
	}
	var cpgr createPGResp
	require.NoError(t, xml.Unmarshal(createResp.Body, &cpgr))
	assert.Equal(t, "my-pg", cpgr.ParameterGroup.ParameterGroupName)
	assert.Equal(t, "redshift-1.0", cpgr.ParameterGroup.ParameterGroupFamily)

	// Describe
	descResp := callQuery(t, p, "DescribeClusterParameterGroups", map[string]string{
		"ParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descPGResp struct {
		ParameterGroups []struct {
			ParameterGroupName string `xml:"ParameterGroupName"`
		} `xml:"DescribeClusterParameterGroupsResult>ParameterGroups>member"`
	}
	var dpgr descPGResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dpgr))
	require.Len(t, dpgr.ParameterGroups, 1)
	assert.Equal(t, "my-pg", dpgr.ParameterGroups[0].ParameterGroupName)

	// Delete
	delResp := callQuery(t, p, "DeleteClusterParameterGroup", map[string]string{
		"ParameterGroupName": "my-pg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))
}

func TestSubnetGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createResp := callQuery(t, p, "CreateClusterSubnetGroup", map[string]string{
		"ClusterSubnetGroupName": "my-sg",
		"Description":            "test subnet group",
	})
	assert.Equal(t, 200, createResp.StatusCode, string(createResp.Body))

	type createSGResp struct {
		SubnetGroup struct {
			ClusterSubnetGroupName string `xml:"ClusterSubnetGroupName"`
			Description            string `xml:"Description"`
		} `xml:"CreateClusterSubnetGroupResult>ClusterSubnetGroup"`
	}
	var csgr createSGResp
	require.NoError(t, xml.Unmarshal(createResp.Body, &csgr))
	assert.Equal(t, "my-sg", csgr.SubnetGroup.ClusterSubnetGroupName)

	// Describe
	descResp := callQuery(t, p, "DescribeClusterSubnetGroups", map[string]string{
		"ClusterSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descSGResp struct {
		SubnetGroups []struct {
			ClusterSubnetGroupName string `xml:"ClusterSubnetGroupName"`
		} `xml:"DescribeClusterSubnetGroupsResult>ClusterSubnetGroups>member"`
	}
	var dsgr descSGResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dsgr))
	require.Len(t, dsgr.SubnetGroups, 1)
	assert.Equal(t, "my-sg", dsgr.SubnetGroups[0].ClusterSubnetGroupName)

	// Modify
	modResp := callQuery(t, p, "ModifyClusterSubnetGroup", map[string]string{
		"ClusterSubnetGroupName": "my-sg",
		"Description":            "updated description",
	})
	assert.Equal(t, 200, modResp.StatusCode, string(modResp.Body))

	// Delete
	delResp := callQuery(t, p, "DeleteClusterSubnetGroup", map[string]string{
		"ClusterSubnetGroupName": "my-sg",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateCluster", map[string]string{"ClusterIdentifier": "tag-cluster"})
	arn := "arn:aws:redshift:us-east-1:000000000000:cluster/tag-cluster"

	// CreateTags
	createResp := callQuery(t, p, "CreateTags", map[string]string{
		"ResourceName":     arn,
		"Tags.Tag.1.Key":   "env",
		"Tags.Tag.1.Value": "test",
		"Tags.Tag.2.Key":   "team",
		"Tags.Tag.2.Value": "data",
	})
	assert.Equal(t, 200, createResp.StatusCode, string(createResp.Body))

	// DescribeTags
	descResp := callQuery(t, p, "DescribeTags", map[string]string{
		"ResourceName": arn,
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type descTagsResp struct {
		TaggedResources []struct {
			Tag struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Tag"`
		} `xml:"DescribeTagsResult>TaggedResources>member"`
	}
	var dtr descTagsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dtr))
	assert.Len(t, dtr.TaggedResources, 2)

	// DeleteTags
	delResp := callQuery(t, p, "DeleteTags", map[string]string{
		"ResourceName":     arn,
		"TagKeys.TagKey.1": "env",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Verify tag removed
	descResp2 := callQuery(t, p, "DescribeTags", map[string]string{
		"ResourceName": arn,
	})
	var dtr2 descTagsResp
	require.NoError(t, xml.Unmarshal(descResp2.Body, &dtr2))
	assert.Len(t, dtr2.TaggedResources, 1)
	assert.Equal(t, "team", dtr2.TaggedResources[0].Tag.Key)
}
