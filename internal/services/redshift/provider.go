// SPDX-License-Identifier: Apache-2.0

package redshift

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the RedshiftServiceVersion20121201 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "redshift" }
func (p *Provider) ServiceName() string           { return "RedshiftServiceVersion20121201" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init redshift: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return rsError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return rsError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// Cluster
	case "CreateCluster":
		return p.handleCreateCluster(form)
	case "DescribeClusters":
		return p.handleDescribeClusters(form)
	case "ModifyCluster":
		return p.handleModifyCluster(form)
	case "DeleteCluster":
		return p.handleDeleteCluster(form)
	case "RebootCluster":
		return p.handleRebootCluster(form)
	case "PauseCluster":
		return p.handlePauseCluster(form)
	case "ResumeCluster":
		return p.handleResumeCluster(form)

	// Snapshots
	case "CreateClusterSnapshot":
		return p.handleCreateClusterSnapshot(form)
	case "DescribeClusterSnapshots":
		return p.handleDescribeClusterSnapshots(form)
	case "DeleteClusterSnapshot":
		return p.handleDeleteClusterSnapshot(form)
	case "CopyClusterSnapshot":
		return p.handleCopyClusterSnapshot(form)
	case "RestoreFromClusterSnapshot":
		return p.handleRestoreFromClusterSnapshot(form)

	// Parameter Groups
	case "CreateClusterParameterGroup":
		return p.handleCreateClusterParameterGroup(form)
	case "DescribeClusterParameterGroups":
		return p.handleDescribeClusterParameterGroups(form)
	case "ModifyClusterParameterGroup":
		return p.handleModifyClusterParameterGroup(form)
	case "ResetClusterParameterGroup":
		return p.handleResetClusterParameterGroup(form)
	case "DeleteClusterParameterGroup":
		return p.handleDeleteClusterParameterGroup(form)
	case "DescribeClusterParameters":
		return p.handleDescribeClusterParameters(form)
	case "DescribeDefaultClusterParameters":
		return p.handleDescribeDefaultClusterParameters(form)

	// Subnet Groups
	case "CreateClusterSubnetGroup":
		return p.handleCreateClusterSubnetGroup(form)
	case "DescribeClusterSubnetGroups":
		return p.handleDescribeClusterSubnetGroups(form)
	case "ModifyClusterSubnetGroup":
		return p.handleModifyClusterSubnetGroup(form)
	case "DeleteClusterSubnetGroup":
		return p.handleDeleteClusterSubnetGroup(form)

	// Tags
	case "CreateTags":
		return p.handleCreateTags(form)
	case "DeleteTags":
		return p.handleDeleteTags(form)
	case "DescribeTags":
		return p.handleDescribeTags(form)

	// Logging
	case "EnableLogging":
		return p.handleEnableLogging(form)
	case "DisableLogging":
		return p.handleDisableLogging(form)
	case "DescribeLoggingStatus":
		return p.handleDescribeLoggingStatus(form)

	// Credentials
	case "GetClusterCredentials":
		return p.handleGetClusterCredentials(form)
	case "GetClusterCredentialsWithIAM":
		return p.handleGetClusterCredentialsWithIAM(form)

	default:
		type genericResult struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return shared.XMLResponse(http.StatusOK, genericResult{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListClusters(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, plugin.Resource{Type: "cluster", ID: c.ID, Name: c.ID})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func rsError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func rsXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type endpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type clusterXML struct {
	ClusterIdentifier string      `xml:"ClusterIdentifier"`
	ClusterStatus     string      `xml:"ClusterStatus"`
	NodeType          string      `xml:"NodeType"`
	NumberOfNodes     int         `xml:"NumberOfNodes"`
	DBName            string      `xml:"DBName"`
	MasterUsername    string      `xml:"MasterUsername"`
	Endpoint          endpointXML `xml:"Endpoint"`
	ClusterCreateTime string      `xml:"ClusterCreateTime"`
	ClusterArn        string      `xml:"ClusterArn"`
}

func clusterToXML(c *Cluster) clusterXML {
	return clusterXML{
		ClusterIdentifier: c.ID,
		ClusterStatus:     c.Status,
		NodeType:          c.NodeType,
		NumberOfNodes:     c.NumNodes,
		DBName:            c.DBName,
		MasterUsername:    c.MasterUser,
		Endpoint: endpointXML{
			Address: c.EndpointAddr,
			Port:    c.EndpointPort,
		},
		ClusterCreateTime: c.CreatedAt.UTC().Format(time.RFC3339),
		ClusterArn:        c.ARN,
	}
}

type snapshotXML struct {
	SnapshotIdentifier string `xml:"SnapshotIdentifier"`
	ClusterIdentifier  string `xml:"ClusterIdentifier"`
	SnapshotArn        string `xml:"SnapshotArn"`
	Status             string `xml:"Status"`
	SnapshotType       string `xml:"SnapshotType"`
	SnapshotCreateTime string `xml:"SnapshotCreateTime"`
}

func snapshotToXML(sn *ClusterSnapshot) snapshotXML {
	return snapshotXML{
		SnapshotIdentifier: sn.ID,
		ClusterIdentifier:  sn.ClusterID,
		SnapshotArn:        sn.ARN,
		Status:             sn.Status,
		SnapshotType:       sn.SnapshotType,
		SnapshotCreateTime: sn.CreatedAt.UTC().Format(time.RFC3339),
	}
}

type paramGroupXML struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description"`
}

func paramGroupToXML(pg *ParameterGroup) paramGroupXML {
	return paramGroupXML{
		ParameterGroupName:   pg.Name,
		ParameterGroupFamily: pg.Family,
		Description:          pg.Description,
	}
}

type subnetGroupXML struct {
	ClusterSubnetGroupName string `xml:"ClusterSubnetGroupName"`
	Description            string `xml:"Description"`
	VpcId                  string `xml:"VpcId"`
}

func subnetGroupToXML(sg *SubnetGroup) subnetGroupXML {
	return subnetGroupXML{
		ClusterSubnetGroupName: sg.Name,
		Description:            sg.Description,
		VpcId:                  sg.VpcID,
	}
}

// --- Cluster handlers ---

func (p *Provider) handleCreateCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	nodeType := form.Get("NodeType")
	if nodeType == "" {
		nodeType = "dc2.large"
	}
	dbName := form.Get("DBName")
	if dbName == "" {
		dbName = "dev"
	}
	masterUser := form.Get("MasterUsername")
	if masterUser == "" {
		masterUser = "admin"
	}
	numNodes := 1
	arn := shared.BuildARN("redshift", "cluster", id)
	c, err := p.store.CreateCluster(id, arn, nodeType, dbName, masterUser, numNodes, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rsError("ClusterAlreadyExists", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createClusterResult struct {
		XMLName xml.Name `xml:"CreateClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"CreateClusterResult"`
	}
	var resp createClusterResult
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeClusters(form url.Values) (*plugin.Response, error) {
	var ids []string
	requestedID := form.Get("ClusterIdentifier")
	if requestedID != "" {
		ids = append(ids, requestedID)
	}
	clusters, err := p.store.ListClusters(ids)
	if err != nil {
		return nil, err
	}
	if requestedID != "" && len(clusters) == 0 {
		return rsError("ClusterNotFound", "cluster not found: "+requestedID, http.StatusNotFound), nil
	}

	items := make([]clusterXML, 0, len(clusters))
	for i := range clusters {
		items = append(items, clusterToXML(&clusters[i]))
	}

	type describeClustersResponse struct {
		XMLName  xml.Name     `xml:"DescribeClustersResponse"`
		Clusters []clusterXML `xml:"DescribeClustersResult>Clusters>member"`
	}
	return rsXMLResponse(http.StatusOK, describeClustersResponse{Clusters: items})
}

func (p *Provider) handleModifyCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if nt := form.Get("NodeType"); nt != "" {
		c.NodeType = nt
	}

	type modifyClusterResponse struct {
		XMLName xml.Name `xml:"ModifyClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"ModifyClusterResult"`
	}
	var resp modifyClusterResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.DeleteCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteClusterResponse struct {
		XMLName xml.Name `xml:"DeleteClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"DeleteClusterResult"`
	}
	var resp deleteClusterResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleRebootCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "rebooting"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type rebootClusterResponse struct {
		XMLName xml.Name `xml:"RebootClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"RebootClusterResult"`
	}
	var resp rebootClusterResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handlePauseCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "paused"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type pauseClusterResponse struct {
		XMLName xml.Name `xml:"PauseClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"PauseClusterResult"`
	}
	var resp pauseClusterResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleResumeCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "available"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type resumeClusterResponse struct {
		XMLName xml.Name `xml:"ResumeClusterResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"ResumeClusterResult"`
	}
	var resp resumeClusterResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

// --- Snapshot handlers ---

func (p *Provider) handleCreateClusterSnapshot(form url.Values) (*plugin.Response, error) {
	snapshotID := form.Get("SnapshotIdentifier")
	clusterID := form.Get("ClusterIdentifier")
	if snapshotID == "" || clusterID == "" {
		return rsError("MissingParameter", "SnapshotIdentifier and ClusterIdentifier are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterID); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rsError("ClusterNotFound", "cluster not found: "+clusterID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("redshift", "snapshot", snapshotID)
	sn, err := p.store.CreateSnapshot(snapshotID, clusterID, arn, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rsError("ClusterSnapshotAlreadyExists", "snapshot already exists: "+snapshotID, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSnapshotResponse struct {
		XMLName  xml.Name    `xml:"CreateClusterSnapshotResponse"`
		Snapshot snapshotXML `xml:"CreateClusterSnapshotResult>Snapshot"`
	}
	return rsXMLResponse(http.StatusOK, createSnapshotResponse{Snapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDescribeClusterSnapshots(form url.Values) (*plugin.Response, error) {
	clusterID := form.Get("ClusterIdentifier")
	snaps, err := p.store.ListSnapshots(clusterID)
	if err != nil {
		return nil, err
	}

	items := make([]snapshotXML, 0, len(snaps))
	for i := range snaps {
		items = append(items, snapshotToXML(&snaps[i]))
	}

	type describeSnapshotsResponse struct {
		XMLName   xml.Name      `xml:"DescribeClusterSnapshotsResponse"`
		Snapshots []snapshotXML `xml:"DescribeClusterSnapshotsResult>Snapshots>member"`
	}
	return rsXMLResponse(http.StatusOK, describeSnapshotsResponse{Snapshots: items})
}

func (p *Provider) handleDeleteClusterSnapshot(form url.Values) (*plugin.Response, error) {
	id := form.Get("SnapshotIdentifier")
	if id == "" {
		return rsError("MissingParameter", "SnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	sn, err := p.store.DeleteSnapshot(id)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return rsError("ClusterSnapshotNotFound", "snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSnapshotResponse struct {
		XMLName  xml.Name    `xml:"DeleteClusterSnapshotResponse"`
		Snapshot snapshotXML `xml:"DeleteClusterSnapshotResult>Snapshot"`
	}
	return rsXMLResponse(http.StatusOK, deleteSnapshotResponse{Snapshot: snapshotToXML(sn)})
}

func (p *Provider) handleCopyClusterSnapshot(form url.Values) (*plugin.Response, error) {
	srcID := form.Get("SourceSnapshotIdentifier")
	destID := form.Get("TargetSnapshotIdentifier")
	if srcID == "" || destID == "" {
		return rsError("MissingParameter", "SourceSnapshotIdentifier and TargetSnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(srcID)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return rsError("ClusterSnapshotNotFound", "snapshot not found: "+srcID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("redshift", "snapshot", destID)
	sn, err := p.store.CreateSnapshot(destID, src.ClusterID, arn, "manual")
	if err != nil {
		return nil, err
	}

	type copySnapshotResponse struct {
		XMLName  xml.Name    `xml:"CopyClusterSnapshotResponse"`
		Snapshot snapshotXML `xml:"CopyClusterSnapshotResult>Snapshot"`
	}
	return rsXMLResponse(http.StatusOK, copySnapshotResponse{Snapshot: snapshotToXML(sn)})
}

func (p *Provider) handleRestoreFromClusterSnapshot(form url.Values) (*plugin.Response, error) {
	newID := form.Get("ClusterIdentifier")
	snapshotID := form.Get("SnapshotIdentifier")
	if newID == "" || snapshotID == "" {
		return rsError("MissingParameter", "ClusterIdentifier and SnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(snapshotID)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return rsError("ClusterSnapshotNotFound", "snapshot not found: "+snapshotID, http.StatusNotFound), nil
		}
		return nil, err
	}
	// Get original cluster info if it exists
	origCluster, _ := p.store.GetCluster(src.ClusterID)
	nodeType := "dc2.large"
	dbName := "dev"
	masterUser := "admin"
	if origCluster != nil {
		nodeType = origCluster.NodeType
		dbName = origCluster.DBName
		masterUser = origCluster.MasterUser
	}
	arn := shared.BuildARN("redshift", "cluster", newID)
	c, err := p.store.CreateCluster(newID, arn, nodeType, dbName, masterUser, 1, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rsError("ClusterAlreadyExists", "cluster already exists: "+newID, http.StatusConflict), nil
		}
		return nil, err
	}

	type restoreResponse struct {
		XMLName xml.Name `xml:"RestoreFromClusterSnapshotResponse"`
		Result  struct {
			Cluster clusterXML `xml:"Cluster"`
		} `xml:"RestoreFromClusterSnapshotResult"`
	}
	var resp restoreResponse
	resp.Result.Cluster = clusterToXML(c)
	return rsXMLResponse(http.StatusOK, resp)
}

// --- Parameter Group handlers ---

func (p *Provider) handleCreateClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ParameterGroupName")
	if name == "" {
		return rsError("MissingParameter", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	family := form.Get("ParameterGroupFamily")
	if family == "" {
		family = "redshift-1.0"
	}
	description := form.Get("Description")
	arn := shared.BuildARN("redshift", "parametergroup", name)
	pg, err := p.store.CreateParameterGroup(name, arn, family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rsError("ClusterParameterGroupAlreadyExists", "parameter group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createParamGroupResponse struct {
		XMLName        xml.Name      `xml:"CreateClusterParameterGroupResponse"`
		ParameterGroup paramGroupXML `xml:"CreateClusterParameterGroupResult>ClusterParameterGroup"`
	}
	return rsXMLResponse(http.StatusOK, createParamGroupResponse{ParameterGroup: paramGroupToXML(pg)})
}

func (p *Provider) handleDescribeClusterParameterGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if n := form.Get("ParameterGroupName"); n != "" {
		names = append(names, n)
	}
	groups, err := p.store.ListParameterGroups(names)
	if err != nil {
		return nil, err
	}

	items := make([]paramGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, paramGroupToXML(&groups[i]))
	}

	type describeParamGroupsResponse struct {
		XMLName         xml.Name        `xml:"DescribeClusterParameterGroupsResponse"`
		ParameterGroups []paramGroupXML `xml:"DescribeClusterParameterGroupsResult>ParameterGroups>member"`
	}
	return rsXMLResponse(http.StatusOK, describeParamGroupsResponse{ParameterGroups: items})
}

func (p *Provider) handleModifyClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ParameterGroupName")
	if name == "" {
		return rsError("MissingParameter", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetParameterGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rsError("ClusterParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyParamGroupResponse struct {
		XMLName              xml.Name `xml:"ModifyClusterParameterGroupResponse"`
		ParameterGroupName   string   `xml:"ModifyClusterParameterGroupResult>ParameterGroupName"`
		ParameterGroupStatus string   `xml:"ModifyClusterParameterGroupResult>ParameterGroupStatus"`
	}
	return rsXMLResponse(http.StatusOK, modifyParamGroupResponse{
		ParameterGroupName:   name,
		ParameterGroupStatus: "Your parameter group has been updated",
	})
}

func (p *Provider) handleResetClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ParameterGroupName")
	if name == "" {
		return rsError("MissingParameter", "ParameterGroupName is required", http.StatusBadRequest), nil
	}

	type resetParamGroupResponse struct {
		XMLName              xml.Name `xml:"ResetClusterParameterGroupResponse"`
		ParameterGroupName   string   `xml:"ResetClusterParameterGroupResult>ParameterGroupName"`
		ParameterGroupStatus string   `xml:"ResetClusterParameterGroupResult>ParameterGroupStatus"`
	}
	return rsXMLResponse(http.StatusOK, resetParamGroupResponse{
		ParameterGroupName:   name,
		ParameterGroupStatus: "Your parameter group has been updated",
	})
}

func (p *Provider) handleDeleteClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ParameterGroupName")
	if name == "" {
		return rsError("MissingParameter", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteParameterGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rsError("ClusterParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteParamGroupResponse struct {
		XMLName xml.Name `xml:"DeleteClusterParameterGroupResponse"`
	}
	return rsXMLResponse(http.StatusOK, deleteParamGroupResponse{})
}

func (p *Provider) handleDescribeClusterParameters(form url.Values) (*plugin.Response, error) {
	name := form.Get("ParameterGroupName")
	if name == "" {
		return rsError("MissingParameter", "ParameterGroupName is required", http.StatusBadRequest), nil
	}

	type describeParamsResponse struct {
		XMLName    xml.Name `xml:"DescribeClusterParametersResponse"`
		Parameters []any    `xml:"DescribeClusterParametersResult>Parameters>member"`
	}
	return rsXMLResponse(http.StatusOK, describeParamsResponse{})
}

func (p *Provider) handleDescribeDefaultClusterParameters(form url.Values) (*plugin.Response, error) {
	family := form.Get("ParameterGroupFamily")
	if family == "" {
		family = "redshift-1.0"
	}

	type describeDefaultParamsResponse struct {
		XMLName xml.Name `xml:"DescribeDefaultClusterParametersResponse"`
		Family  string   `xml:"DescribeDefaultClusterParametersResult>DefaultClusterParameters>ParameterGroupFamily"`
	}
	return rsXMLResponse(http.StatusOK, describeDefaultParamsResponse{Family: family})
}

// --- Subnet Group handlers ---

func (p *Provider) handleCreateClusterSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ClusterSubnetGroupName")
	if name == "" {
		return rsError("MissingParameter", "ClusterSubnetGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	arn := shared.BuildARN("redshift", "subnetgroup", name)
	sg, err := p.store.CreateSubnetGroup(name, arn, description, "", "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rsError("ClusterSubnetGroupAlreadyExists", "subnet group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSubnetGroupResponse struct {
		XMLName     xml.Name       `xml:"CreateClusterSubnetGroupResponse"`
		SubnetGroup subnetGroupXML `xml:"CreateClusterSubnetGroupResult>ClusterSubnetGroup"`
	}
	return rsXMLResponse(http.StatusOK, createSubnetGroupResponse{SubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDescribeClusterSubnetGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if n := form.Get("ClusterSubnetGroupName"); n != "" {
		names = append(names, n)
	}
	groups, err := p.store.ListSubnetGroups(names)
	if err != nil {
		return nil, err
	}

	items := make([]subnetGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, subnetGroupToXML(&groups[i]))
	}

	type describeSubnetGroupsResponse struct {
		XMLName      xml.Name         `xml:"DescribeClusterSubnetGroupsResponse"`
		SubnetGroups []subnetGroupXML `xml:"DescribeClusterSubnetGroupsResult>ClusterSubnetGroups>member"`
	}
	return rsXMLResponse(http.StatusOK, describeSubnetGroupsResponse{SubnetGroups: items})
}

func (p *Provider) handleModifyClusterSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ClusterSubnetGroupName")
	if name == "" {
		return rsError("MissingParameter", "ClusterSubnetGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	if err := p.store.UpdateSubnetGroup(name, description, "", "[]"); err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return rsError("ClusterSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}
	sg, _ := p.store.GetSubnetGroup(name)

	type modifySubnetGroupResponse struct {
		XMLName     xml.Name       `xml:"ModifyClusterSubnetGroupResponse"`
		SubnetGroup subnetGroupXML `xml:"ModifyClusterSubnetGroupResult>ClusterSubnetGroup"`
	}
	return rsXMLResponse(http.StatusOK, modifySubnetGroupResponse{SubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDeleteClusterSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("ClusterSubnetGroupName")
	if name == "" {
		return rsError("MissingParameter", "ClusterSubnetGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteSubnetGroup(name); err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return rsError("ClusterSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSubnetGroupResponse struct {
		XMLName xml.Name `xml:"DeleteClusterSubnetGroupResponse"`
	}
	return rsXMLResponse(http.StatusOK, deleteSubnetGroupResponse{})
}

// --- Tag handlers ---

func (p *Provider) handleCreateTags(form url.Values) (*plugin.Response, error) {
	resourceName := form.Get("ResourceName")
	if resourceName == "" {
		return rsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		v := form.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))
		if k == "" {
			break
		}
		tags[k] = v
	}
	if err := p.store.AddTags(resourceName, tags); err != nil {
		return nil, err
	}

	type createTagsResponse struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
	}
	return rsXMLResponse(http.StatusOK, createTagsResponse{})
}

func (p *Provider) handleDeleteTags(form url.Values) (*plugin.Response, error) {
	resourceName := form.Get("ResourceName")
	if resourceName == "" {
		return rsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	var keys []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.TagKey.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	if err := p.store.RemoveTags(resourceName, keys); err != nil {
		return nil, err
	}

	type deleteTagsResponse struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
	}
	return rsXMLResponse(http.StatusOK, deleteTagsResponse{})
}

func (p *Provider) handleDescribeTags(form url.Values) (*plugin.Response, error) {
	resourceName := form.Get("ResourceName")
	if resourceName == "" {
		return rsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tagsMap, err := p.store.ListTags(resourceName)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type taggedResourceXML struct {
		Tag          tagXML `xml:"Tag"`
		ResourceName string `xml:"ResourceName"`
		ResourceType string `xml:"ResourceType"`
	}

	items := make([]taggedResourceXML, 0, len(tagsMap))
	for k, v := range tagsMap {
		items = append(items, taggedResourceXML{
			Tag:          tagXML{Key: k, Value: v},
			ResourceName: resourceName,
			ResourceType: "cluster",
		})
	}

	type describeTagsResponse struct {
		XMLName         xml.Name            `xml:"DescribeTagsResponse"`
		TaggedResources []taggedResourceXML `xml:"DescribeTagsResult>TaggedResources>member"`
	}
	return rsXMLResponse(http.StatusOK, describeTagsResponse{TaggedResources: items})
}

// --- Logging handlers ---

func (p *Provider) handleEnableLogging(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}

	type loggingStatusXML struct {
		XMLName                    xml.Name `xml:"EnableLoggingResponse"`
		LoggingEnabled             bool     `xml:"EnableLoggingResult>LoggingEnabled"`
		BucketName                 string   `xml:"EnableLoggingResult>BucketName,omitempty"`
		S3KeyPrefix                string   `xml:"EnableLoggingResult>S3KeyPrefix,omitempty"`
		LastSuccessfulDeliveryTime string   `xml:"EnableLoggingResult>LastSuccessfulDeliveryTime,omitempty"`
	}
	return rsXMLResponse(http.StatusOK, loggingStatusXML{
		LoggingEnabled: true,
		BucketName:     form.Get("BucketName"),
	})
}

func (p *Provider) handleDisableLogging(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}

	type loggingStatusXML struct {
		XMLName        xml.Name `xml:"DisableLoggingResponse"`
		LoggingEnabled bool     `xml:"DisableLoggingResult>LoggingEnabled"`
	}
	return rsXMLResponse(http.StatusOK, loggingStatusXML{LoggingEnabled: false})
}

func (p *Provider) handleDescribeLoggingStatus(form url.Values) (*plugin.Response, error) {
	id := form.Get("ClusterIdentifier")
	if id == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}

	type loggingStatusXML struct {
		XMLName        xml.Name `xml:"DescribeLoggingStatusResponse"`
		LoggingEnabled bool     `xml:"DescribeLoggingStatusResult>LoggingEnabled"`
	}
	return rsXMLResponse(http.StatusOK, loggingStatusXML{LoggingEnabled: false})
}

// --- Credentials handlers ---

func (p *Provider) handleGetClusterCredentials(form url.Values) (*plugin.Response, error) {
	dbUser := form.Get("DbUser")
	clusterID := form.Get("ClusterIdentifier")
	if dbUser == "" || clusterID == "" {
		return rsError("MissingParameter", "DbUser and ClusterIdentifier are required", http.StatusBadRequest), nil
	}

	type credentialsXML struct {
		XMLName    xml.Name `xml:"GetClusterCredentialsResponse"`
		DbUser     string   `xml:"GetClusterCredentialsResult>DbUser"`
		DbPassword string   `xml:"GetClusterCredentialsResult>DbPassword"`
		Expiration string   `xml:"GetClusterCredentialsResult>Expiration"`
	}
	return rsXMLResponse(http.StatusOK, credentialsXML{
		DbUser:     dbUser,
		DbPassword: shared.GenerateID("", 32),
		Expiration: time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339),
	})
}

func (p *Provider) handleGetClusterCredentialsWithIAM(form url.Values) (*plugin.Response, error) {
	clusterID := form.Get("ClusterIdentifier")
	if clusterID == "" {
		return rsError("MissingParameter", "ClusterIdentifier is required", http.StatusBadRequest), nil
	}

	type credentialsXML struct {
		XMLName    xml.Name `xml:"GetClusterCredentialsWithIAMResponse"`
		DbUser     string   `xml:"GetClusterCredentialsWithIAMResult>DbUser"`
		DbPassword string   `xml:"GetClusterCredentialsWithIAMResult>DbPassword"`
		Expiration string   `xml:"GetClusterCredentialsWithIAMResult>Expiration"`
	}
	return rsXMLResponse(http.StatusOK, credentialsXML{
		DbUser:     "iamuser",
		DbPassword: shared.GenerateID("", 32),
		Expiration: time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339),
	})
}
