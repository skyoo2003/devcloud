// SPDX-License-Identifier: Apache-2.0

package docdb

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

// Provider implements the RDSv19 (DocumentDB) service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "docdb" }
func (p *Provider) ServiceName() string           { return "RDSv19" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init docdb: %w", err)
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
		return dbError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return dbError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// DBCluster
	case "CreateDBCluster":
		return p.handleCreateDBCluster(form)
	case "DescribeDBClusters":
		return p.handleDescribeDBClusters(form)
	case "ModifyDBCluster":
		return p.handleModifyDBCluster(form)
	case "DeleteDBCluster":
		return p.handleDeleteDBCluster(form)
	case "StartDBCluster":
		return p.handleStartDBCluster(form)
	case "StopDBCluster":
		return p.handleStopDBCluster(form)
	case "FailoverDBCluster":
		return p.handleFailoverDBCluster(form)
	case "RestoreDBClusterFromSnapshot":
		return p.handleRestoreDBClusterFromSnapshot(form)
	case "RestoreDBClusterToPointInTime":
		return p.handleRestoreDBClusterToPointInTime(form)

	// DBInstance
	case "CreateDBInstance":
		return p.handleCreateDBInstance(form)
	case "DescribeDBInstances":
		return p.handleDescribeDBInstances(form)
	case "ModifyDBInstance":
		return p.handleModifyDBInstance(form)
	case "DeleteDBInstance":
		return p.handleDeleteDBInstance(form)
	case "RebootDBInstance":
		return p.handleRebootDBInstance(form)

	// DBClusterSnapshot
	case "CreateDBClusterSnapshot":
		return p.handleCreateDBClusterSnapshot(form)
	case "DescribeDBClusterSnapshots":
		return p.handleDescribeDBClusterSnapshots(form)
	case "DeleteDBClusterSnapshot":
		return p.handleDeleteDBClusterSnapshot(form)
	case "CopyDBClusterSnapshot":
		return p.handleCopyDBClusterSnapshot(form)
	case "DescribeDBClusterSnapshotAttributes":
		return p.handleDescribeDBClusterSnapshotAttributes(form)
	case "ModifyDBClusterSnapshotAttribute":
		return p.handleModifyDBClusterSnapshotAttribute(form)

	// DBSubnetGroup
	case "CreateDBSubnetGroup":
		return p.handleCreateDBSubnetGroup(form)
	case "DescribeDBSubnetGroups":
		return p.handleDescribeDBSubnetGroups(form)
	case "ModifyDBSubnetGroup":
		return p.handleModifyDBSubnetGroup(form)
	case "DeleteDBSubnetGroup":
		return p.handleDeleteDBSubnetGroup(form)

	// DBClusterParameterGroup
	case "CreateDBClusterParameterGroup":
		return p.handleCreateDBClusterParameterGroup(form)
	case "DescribeDBClusterParameterGroups":
		return p.handleDescribeDBClusterParameterGroups(form)
	case "ModifyDBClusterParameterGroup":
		return p.handleModifyDBClusterParameterGroup(form)
	case "DeleteDBClusterParameterGroup":
		return p.handleDeleteDBClusterParameterGroup(form)
	case "ResetDBClusterParameterGroup":
		return p.handleResetDBClusterParameterGroup(form)
	case "DescribeDBClusterParameters":
		return p.handleDescribeDBClusterParameters(form)
	case "DescribeEngineDefaultClusterParameters":
		return p.handleDescribeEngineDefaultClusterParameters(form)
	case "CopyDBClusterParameterGroup":
		return p.handleCopyDBClusterParameterGroup(form)

	// Tags
	case "AddTagsToResource":
		return p.handleAddTagsToResource(form)
	case "RemoveTagsFromResource":
		return p.handleRemoveTagsFromResource(form)
	case "ListTagsForResource":
		return p.handleListTagsForResource(form)

	// Describe misc
	case "DescribeDBEngineVersions":
		return p.handleDescribeDBEngineVersions(form)
	case "DescribeOrderableDBInstanceOptions":
		return p.handleDescribeOrderableDBInstanceOptions(form)
	case "DescribeEvents":
		return p.handleDescribeEvents(form)
	case "DescribeEventCategories":
		return p.handleDescribeEventCategories(form)
	case "DescribePendingMaintenanceActions":
		return p.handleDescribePendingMaintenanceActions(form)
	case "ApplyPendingMaintenanceAction":
		return p.handleApplyPendingMaintenanceAction(form)
	case "DescribeCertificates":
		return p.handleDescribeCertificates(form)

	// EventSubscription
	case "CreateEventSubscription":
		return p.handleCreateEventSubscription(form)
	case "DescribeEventSubscriptions":
		return p.handleDescribeEventSubscriptions(form)
	case "ModifyEventSubscription":
		return p.handleModifyEventSubscription(form)
	case "DeleteEventSubscription":
		return p.handleDeleteEventSubscription(form)
	case "AddSourceIdentifierToSubscription":
		return p.handleAddSourceIdentifierToSubscription(form)
	case "RemoveSourceIdentifierFromSubscription":
		return p.handleRemoveSourceIdentifierFromSubscription(form)

	// GlobalCluster
	case "CreateGlobalCluster":
		return p.handleCreateGlobalCluster(form)
	case "DescribeGlobalClusters":
		return p.handleDescribeGlobalClusters(form)
	case "ModifyGlobalCluster":
		return p.handleModifyGlobalCluster(form)
	case "DeleteGlobalCluster":
		return p.handleDeleteGlobalCluster(form)
	case "RemoveFromGlobalCluster":
		return p.handleRemoveFromGlobalCluster(form)
	case "FailoverGlobalCluster":
		return p.handleFailoverGlobalCluster(form)
	case "SwitchoverGlobalCluster":
		return p.handleSwitchoverGlobalCluster(form)

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
		out = append(out, plugin.Resource{Type: "db-cluster", ID: c.ID, Name: c.ID})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func dbError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func dbXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type dbEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type dbClusterXML struct {
	DBClusterIdentifier string `xml:"DBClusterIdentifier"`
	Status              string `xml:"Status"`
	Engine              string `xml:"Engine"`
	EngineVersion       string `xml:"EngineVersion"`
	MasterUsername      string `xml:"MasterUsername"`
	Endpoint            string `xml:"Endpoint"`
	ReaderEndpoint      string `xml:"ReaderEndpoint"`
	Port                int    `xml:"Port"`
	StorageEncrypted    bool   `xml:"StorageEncrypted"`
	ClusterCreateTime   string `xml:"ClusterCreateTime"`
	DBClusterArn        string `xml:"DBClusterArn"`
}

func clusterToXML(c *DBCluster) dbClusterXML {
	return dbClusterXML{
		DBClusterIdentifier: c.ID,
		Status:              c.Status,
		Engine:              c.Engine,
		EngineVersion:       c.EngineVersion,
		MasterUsername:      c.MasterUser,
		Endpoint:            c.Endpoint,
		ReaderEndpoint:      c.ReaderEndpoint,
		Port:                c.Port,
		StorageEncrypted:    c.StorageEncrypted,
		ClusterCreateTime:   c.CreatedAt.UTC().Format(time.RFC3339),
		DBClusterArn:        c.ARN,
	}
}

type dbInstanceXML struct {
	DBInstanceIdentifier string        `xml:"DBInstanceIdentifier"`
	DBClusterIdentifier  string        `xml:"DBClusterIdentifier"`
	DBInstanceStatus     string        `xml:"DBInstanceStatus"`
	DBInstanceClass      string        `xml:"DBInstanceClass"`
	Engine               string        `xml:"Engine"`
	EngineVersion        string        `xml:"EngineVersion"`
	Endpoint             dbEndpointXML `xml:"Endpoint"`
	AvailabilityZone     string        `xml:"AvailabilityZone"`
	InstanceCreateTime   string        `xml:"InstanceCreateTime"`
	DBInstanceArn        string        `xml:"DBInstanceArn"`
}

func instanceToXML(inst *DBInstance) dbInstanceXML {
	return dbInstanceXML{
		DBInstanceIdentifier: inst.ID,
		DBClusterIdentifier:  inst.ClusterID,
		DBInstanceStatus:     inst.Status,
		DBInstanceClass:      inst.InstanceClass,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		Endpoint: dbEndpointXML{
			Address: inst.Endpoint,
			Port:    inst.Port,
		},
		AvailabilityZone:   inst.AZ,
		InstanceCreateTime: inst.CreatedAt.UTC().Format(time.RFC3339),
		DBInstanceArn:      inst.ARN,
	}
}

type clusterSnapshotXML struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	DBClusterSnapshotArn        string `xml:"DBClusterSnapshotArn"`
	Status                      string `xml:"Status"`
	Engine                      string `xml:"Engine"`
	SnapshotType                string `xml:"SnapshotType"`
	SnapshotCreateTime          string `xml:"SnapshotCreateTime"`
}

func snapshotToXML(sn *ClusterSnapshot) clusterSnapshotXML {
	return clusterSnapshotXML{
		DBClusterSnapshotIdentifier: sn.ID,
		DBClusterIdentifier:         sn.ClusterID,
		DBClusterSnapshotArn:        sn.ARN,
		Status:                      sn.Status,
		Engine:                      sn.Engine,
		SnapshotType:                sn.SnapshotType,
		SnapshotCreateTime:          sn.CreatedAt.UTC().Format(time.RFC3339),
	}
}

type subnetGroupXML struct {
	DBSubnetGroupName        string `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string `xml:"DBSubnetGroupDescription"`
	VpcId                    string `xml:"VpcId"`
	SubnetGroupStatus        string `xml:"SubnetGroupStatus"`
	DBSubnetGroupArn         string `xml:"DBSubnetGroupArn"`
}

func subnetGroupToXML(sg *SubnetGroup) subnetGroupXML {
	return subnetGroupXML{
		DBSubnetGroupName:        sg.Name,
		DBSubnetGroupDescription: sg.Description,
		VpcId:                    sg.VpcID,
		SubnetGroupStatus:        sg.Status,
		DBSubnetGroupArn:         sg.ARN,
	}
}

type paramGroupXML struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
	DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn"`
}

func paramGroupToXML(pg *ClusterParamGroup) paramGroupXML {
	return paramGroupXML{
		DBClusterParameterGroupName: pg.Name,
		DBParameterGroupFamily:      pg.Family,
		Description:                 pg.Description,
		DBClusterParameterGroupArn:  pg.ARN,
	}
}

// --- DBCluster handlers ---

func (p *Provider) handleCreateDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "docdb"
	}
	engineVersion := form.Get("EngineVersion")
	if engineVersion == "" {
		engineVersion = "5.0.0"
	}
	masterUser := form.Get("MasterUsername")
	if masterUser == "" {
		masterUser = "admin"
	}
	port := 27017
	arn := shared.BuildARN("rds", "cluster", id)
	c, err := p.store.CreateCluster(id, arn, engine, engineVersion, masterUser, port, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBClusterAlreadyExistsFault", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createDBClusterResult struct {
		XMLName   xml.Name     `xml:"CreateDBClusterResponse"`
		DBCluster dbClusterXML `xml:"CreateDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, createDBClusterResult{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleDescribeDBClusters(form url.Values) (*plugin.Response, error) {
	var ids []string
	requestedID := form.Get("DBClusterIdentifier")
	if requestedID != "" {
		ids = append(ids, requestedID)
	}
	clusters, err := p.store.ListClusters(ids)
	if err != nil {
		return nil, err
	}
	if requestedID != "" && len(clusters) == 0 {
		return dbError("DBClusterNotFoundFault", "cluster not found: "+requestedID, http.StatusNotFound), nil
	}
	items := make([]dbClusterXML, 0, len(clusters))
	for i := range clusters {
		items = append(items, clusterToXML(&clusters[i]))
	}

	type describeDBClustersResponse struct {
		XMLName    xml.Name       `xml:"DescribeDBClustersResponse"`
		DBClusters []dbClusterXML `xml:"DescribeDBClustersResult>DBClusters>member"`
	}
	return dbXMLResponse(http.StatusOK, describeDBClustersResponse{DBClusters: items})
}

func (p *Provider) handleModifyDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if ev := form.Get("EngineVersion"); ev != "" {
		c.EngineVersion = ev
	}

	type modifyDBClusterResponse struct {
		XMLName   xml.Name     `xml:"ModifyDBClusterResponse"`
		DBCluster dbClusterXML `xml:"ModifyDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, modifyDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleDeleteDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.DeleteCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteDBClusterResponse struct {
		XMLName   xml.Name     `xml:"DeleteDBClusterResponse"`
		DBCluster dbClusterXML `xml:"DeleteDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, deleteDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleStartDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "available"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type startDBClusterResponse struct {
		XMLName   xml.Name     `xml:"StartDBClusterResponse"`
		DBCluster dbClusterXML `xml:"StartDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, startDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleStopDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "stopped"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type stopDBClusterResponse struct {
		XMLName   xml.Name     `xml:"StopDBClusterResponse"`
		DBCluster dbClusterXML `xml:"StopDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, stopDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleFailoverDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type failoverDBClusterResponse struct {
		XMLName   xml.Name     `xml:"FailoverDBClusterResponse"`
		DBCluster dbClusterXML `xml:"FailoverDBClusterResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, failoverDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleRestoreDBClusterFromSnapshot(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	snapshotID := form.Get("SnapshotIdentifier")
	if id == "" || snapshotID == "" {
		return dbError("MissingParameter", "DBClusterIdentifier and SnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(snapshotID)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return dbError("DBClusterSnapshotNotFoundFault", "snapshot not found: "+snapshotID, http.StatusNotFound), nil
		}
		return nil, err
	}
	engine := src.Engine
	arn := shared.BuildARN("rds", "cluster", id)
	c, err := p.store.CreateCluster(id, arn, engine, "5.0.0", "admin", 27017, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBClusterAlreadyExistsFault", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type restoreResponse struct {
		XMLName   xml.Name     `xml:"RestoreDBClusterFromSnapshotResponse"`
		DBCluster dbClusterXML `xml:"RestoreDBClusterFromSnapshotResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, restoreResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleRestoreDBClusterToPointInTime(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	srcID := form.Get("SourceDBClusterIdentifier")
	if id == "" || srcID == "" {
		return dbError("MissingParameter", "DBClusterIdentifier and SourceDBClusterIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetCluster(srcID)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+srcID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "cluster", id)
	c, err := p.store.CreateCluster(id, arn, src.Engine, src.EngineVersion, src.MasterUser, src.Port, src.StorageEncrypted)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBClusterAlreadyExistsFault", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type restorePITResponse struct {
		XMLName   xml.Name     `xml:"RestoreDBClusterToPointInTimeResponse"`
		DBCluster dbClusterXML `xml:"RestoreDBClusterToPointInTimeResult>DBCluster"`
	}
	return dbXMLResponse(http.StatusOK, restorePITResponse{DBCluster: clusterToXML(c)})
}

// --- DBInstance handlers ---

func (p *Provider) handleCreateDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	clusterID := form.Get("DBClusterIdentifier")
	instanceClass := form.Get("DBInstanceClass")
	if instanceClass == "" {
		instanceClass = "db.r6g.large"
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "docdb"
	}
	az := form.Get("AvailabilityZone")
	if az == "" {
		az = "us-east-1a"
	}
	arn := shared.BuildARN("rds", "db", id)
	inst, err := p.store.CreateInstance(id, arn, clusterID, instanceClass, engine, "5.0.0", az, 27017)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBInstanceAlreadyExists", "instance already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createDBInstanceResult struct {
		XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"CreateDBInstanceResult>DBInstance"`
	}
	return dbXMLResponse(http.StatusOK, createDBInstanceResult{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleDescribeDBInstances(form url.Values) (*plugin.Response, error) {
	var ids []string
	if id := form.Get("DBInstanceIdentifier"); id != "" {
		ids = append(ids, id)
	}
	instances, err := p.store.ListInstances(ids)
	if err != nil {
		return nil, err
	}
	items := make([]dbInstanceXML, 0, len(instances))
	for i := range instances {
		items = append(items, instanceToXML(&instances[i]))
	}

	type describeDBInstancesResponse struct {
		XMLName     xml.Name        `xml:"DescribeDBInstancesResponse"`
		DBInstances []dbInstanceXML `xml:"DescribeDBInstancesResult>DBInstances>member"`
	}
	return dbXMLResponse(http.StatusOK, describeDBInstancesResponse{DBInstances: items})
}

func (p *Provider) handleModifyDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	inst, err := p.store.GetInstance(id)
	if err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return dbError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if ic := form.Get("DBInstanceClass"); ic != "" {
		inst.InstanceClass = ic
	}

	type modifyDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"ModifyDBInstanceResult>DBInstance"`
	}
	return dbXMLResponse(http.StatusOK, modifyDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleDeleteDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	inst, err := p.store.DeleteInstance(id)
	if err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return dbError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"DeleteDBInstanceResult>DBInstance"`
	}
	return dbXMLResponse(http.StatusOK, deleteDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleRebootDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateInstanceStatus(id, "rebooting"); err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return dbError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	inst, _ := p.store.GetInstance(id)

	type rebootDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"RebootDBInstanceResult>DBInstance"`
	}
	return dbXMLResponse(http.StatusOK, rebootDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

// --- DBClusterSnapshot handlers ---

func (p *Provider) handleCreateDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	snapshotID := form.Get("DBClusterSnapshotIdentifier")
	clusterID := form.Get("DBClusterIdentifier")
	if snapshotID == "" || clusterID == "" {
		return dbError("MissingParameter", "DBClusterSnapshotIdentifier and DBClusterIdentifier are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterID); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return dbError("DBClusterNotFoundFault", "cluster not found: "+clusterID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "cluster-snapshot", snapshotID)
	sn, err := p.store.CreateSnapshot(snapshotID, arn, clusterID, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBClusterSnapshotAlreadyExistsFault", "snapshot already exists: "+snapshotID, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSnapshotResponse struct {
		XMLName           xml.Name           `xml:"CreateDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return dbXMLResponse(http.StatusOK, createSnapshotResponse{DBClusterSnapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDescribeDBClusterSnapshots(form url.Values) (*plugin.Response, error) {
	clusterID := form.Get("DBClusterIdentifier")
	snaps, err := p.store.ListSnapshots(clusterID)
	if err != nil {
		return nil, err
	}
	items := make([]clusterSnapshotXML, 0, len(snaps))
	for i := range snaps {
		items = append(items, snapshotToXML(&snaps[i]))
	}

	type describeSnapshotsResponse struct {
		XMLName            xml.Name             `xml:"DescribeDBClusterSnapshotsResponse"`
		DBClusterSnapshots []clusterSnapshotXML `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots>member"`
	}
	return dbXMLResponse(http.StatusOK, describeSnapshotsResponse{DBClusterSnapshots: items})
}

func (p *Provider) handleDeleteDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterSnapshotIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterSnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	sn, err := p.store.DeleteSnapshot(id)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return dbError("DBClusterSnapshotNotFoundFault", "snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSnapshotResponse struct {
		XMLName           xml.Name           `xml:"DeleteDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return dbXMLResponse(http.StatusOK, deleteSnapshotResponse{DBClusterSnapshot: snapshotToXML(sn)})
}

func (p *Provider) handleCopyDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	srcID := form.Get("SourceDBClusterSnapshotIdentifier")
	destID := form.Get("TargetDBClusterSnapshotIdentifier")
	if srcID == "" || destID == "" {
		return dbError("MissingParameter", "SourceDBClusterSnapshotIdentifier and TargetDBClusterSnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(srcID)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return dbError("DBClusterSnapshotNotFoundFault", "snapshot not found: "+srcID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "cluster-snapshot", destID)
	sn, err := p.store.CreateSnapshot(destID, arn, src.ClusterID, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBClusterSnapshotAlreadyExistsFault", "snapshot already exists: "+destID, http.StatusConflict), nil
		}
		return nil, err
	}

	type copySnapshotResponse struct {
		XMLName           xml.Name           `xml:"CopyDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return dbXMLResponse(http.StatusOK, copySnapshotResponse{DBClusterSnapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDescribeDBClusterSnapshotAttributes(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterSnapshotIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterSnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetSnapshot(id); err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return dbError("DBClusterSnapshotNotFoundFault", "snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type attrResult struct {
		XMLName xml.Name `xml:"DescribeDBClusterSnapshotAttributesResponse"`
		Result  struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotAttributesResult>DBClusterSnapshotIdentifier"`
		} `xml:"DescribeDBClusterSnapshotAttributesResult"`
	}
	var resp attrResult
	resp.Result.DBClusterSnapshotIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleModifyDBClusterSnapshotAttribute(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterSnapshotIdentifier")
	if id == "" {
		return dbError("MissingParameter", "DBClusterSnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetSnapshot(id); err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return dbError("DBClusterSnapshotNotFoundFault", "snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyAttrResult struct {
		XMLName xml.Name `xml:"ModifyDBClusterSnapshotAttributeResponse"`
		Result  struct {
			DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotAttributesResult>DBClusterSnapshotIdentifier"`
		} `xml:"ModifyDBClusterSnapshotAttributeResult"`
	}
	var resp modifyAttrResult
	resp.Result.DBClusterSnapshotIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

// --- DBSubnetGroup handlers ---

func (p *Provider) handleCreateDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("DBSubnetGroupDescription")
	arn := shared.BuildARN("rds", "subgrp", name)
	sg, err := p.store.CreateSubnetGroup(name, arn, description, "", "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBSubnetGroupAlreadyExists", "subnet group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSubnetGroupResponse struct {
		XMLName       xml.Name       `xml:"CreateDBSubnetGroupResponse"`
		DBSubnetGroup subnetGroupXML `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
	}
	return dbXMLResponse(http.StatusOK, createSubnetGroupResponse{DBSubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDescribeDBSubnetGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("DBSubnetGroupName"); name != "" {
		names = append(names, name)
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
		XMLName        xml.Name         `xml:"DescribeDBSubnetGroupsResponse"`
		DBSubnetGroups []subnetGroupXML `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups>member"`
	}
	return dbXMLResponse(http.StatusOK, describeSubnetGroupsResponse{DBSubnetGroups: items})
}

func (p *Provider) handleModifyDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	sg, err := p.store.GetSubnetGroup(name)
	if err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return dbError("DBSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}
	description := form.Get("DBSubnetGroupDescription")
	if description == "" {
		description = sg.Description
	}
	if err := p.store.UpdateSubnetGroup(name, description, sg.VpcID, sg.Subnets); err != nil {
		return nil, err
	}
	sg.Description = description

	type modifySubnetGroupResponse struct {
		XMLName       xml.Name       `xml:"ModifyDBSubnetGroupResponse"`
		DBSubnetGroup subnetGroupXML `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
	}
	return dbXMLResponse(http.StatusOK, modifySubnetGroupResponse{DBSubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDeleteDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteSubnetGroup(name); err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return dbError("DBSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSubnetGroupResponse struct {
		XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	}
	return dbXMLResponse(http.StatusOK, deleteSubnetGroupResponse{})
}

// --- DBClusterParameterGroup handlers ---

func (p *Provider) handleCreateDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	family := form.Get("DBParameterGroupFamily")
	if family == "" {
		family = "docdb5.0"
	}
	description := form.Get("Description")
	arn := shared.BuildARN("rds", "cluster-pg", name)
	pg, err := p.store.CreateParamGroup(name, arn, family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBParameterGroupAlreadyExists", "parameter group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createParamGroupResponse struct {
		XMLName                 xml.Name      `xml:"CreateDBClusterParameterGroupResponse"`
		DBClusterParameterGroup paramGroupXML `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
	}
	return dbXMLResponse(http.StatusOK, createParamGroupResponse{DBClusterParameterGroup: paramGroupToXML(pg)})
}

func (p *Provider) handleDescribeDBClusterParameterGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("DBClusterParameterGroupName"); name != "" {
		names = append(names, name)
	}
	groups, err := p.store.ListParamGroups(names)
	if err != nil {
		return nil, err
	}
	items := make([]paramGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, paramGroupToXML(&groups[i]))
	}

	type describeParamGroupsResponse struct {
		XMLName                  xml.Name        `xml:"DescribeDBClusterParameterGroupsResponse"`
		DBClusterParameterGroups []paramGroupXML `xml:"DescribeDBClusterParameterGroupsResult>DBClusterParameterGroups>member"`
	}
	return dbXMLResponse(http.StatusOK, describeParamGroupsResponse{DBClusterParameterGroups: items})
}

func (p *Provider) handleModifyDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return dbError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyParamGroupResponse struct {
		XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
		DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
	}
	return dbXMLResponse(http.StatusOK, modifyParamGroupResponse{DBClusterParameterGroupName: name})
}

func (p *Provider) handleDeleteDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return dbError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteParamGroupResponse struct {
		XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	}
	return dbXMLResponse(http.StatusOK, deleteParamGroupResponse{})
}

func (p *Provider) handleResetDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return dbError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return dbError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type resetParamGroupResponse struct {
		XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
		DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
	}
	return dbXMLResponse(http.StatusOK, resetParamGroupResponse{DBClusterParameterGroupName: name})
}

func (p *Provider) handleDescribeDBClusterParameters(form url.Values) (*plugin.Response, error) {
	type describeParamsResponse struct {
		XMLName    xml.Name   `xml:"DescribeDBClusterParametersResponse"`
		Parameters []struct{} `xml:"DescribeDBClusterParametersResult>Parameters>member"`
	}
	return dbXMLResponse(http.StatusOK, describeParamsResponse{})
}

func (p *Provider) handleDescribeEngineDefaultClusterParameters(form url.Values) (*plugin.Response, error) {
	type describeDefaultParamsResponse struct {
		XMLName xml.Name `xml:"DescribeEngineDefaultClusterParametersResponse"`
		Result  struct {
			DBParameterGroupFamily string `xml:"EngineDefaults>DBParameterGroupFamily"`
		} `xml:"DescribeEngineDefaultClusterParametersResult"`
	}
	var resp describeDefaultParamsResponse
	resp.Result.DBParameterGroupFamily = form.Get("DBParameterGroupFamily")
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleCopyDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	srcName := form.Get("SourceDBClusterParameterGroupIdentifier")
	destName := form.Get("TargetDBClusterParameterGroupIdentifier")
	if srcName == "" || destName == "" {
		return dbError("MissingParameter", "SourceDBClusterParameterGroupIdentifier and TargetDBClusterParameterGroupIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetParamGroup(srcName)
	if err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return dbError("DBParameterGroupNotFound", "parameter group not found: "+srcName, http.StatusNotFound), nil
		}
		return nil, err
	}
	description := form.Get("TargetDBClusterParameterGroupDescription")
	arn := shared.BuildARN("rds", "cluster-pg", destName)
	pg, err := p.store.CreateParamGroup(destName, arn, src.Family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return dbError("DBParameterGroupAlreadyExists", "parameter group already exists: "+destName, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyParamGroupResponse struct {
		XMLName                 xml.Name      `xml:"CopyDBClusterParameterGroupResponse"`
		DBClusterParameterGroup paramGroupXML `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
	}
	return dbXMLResponse(http.StatusOK, copyParamGroupResponse{DBClusterParameterGroup: paramGroupToXML(pg)})
}

// --- Tag handlers ---

func (p *Provider) handleAddTagsToResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return dbError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tags := parseFormTags(form)
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, err
	}

	type addTagsResponse struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	}
	return dbXMLResponse(http.StatusOK, addTagsResponse{})
}

func (p *Provider) handleRemoveTagsFromResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return dbError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	var keys []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			break
		}
		keys = append(keys, k)
	}
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, err
	}

	type removeTagsResponse struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	}
	return dbXMLResponse(http.StatusOK, removeTagsResponse{})
}

func (p *Provider) handleListTagsForResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return dbError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tagMap, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var items []tagXML
	for k, v := range tagMap {
		items = append(items, tagXML{Key: k, Value: v})
	}

	type listTagsResponse struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		TagList []tagXML `xml:"ListTagsForResourceResult>TagList>member"`
	}
	return dbXMLResponse(http.StatusOK, listTagsResponse{TagList: items})
}

func parseFormTags(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		v := form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		if k == "" {
			break
		}
		tags[k] = v
	}
	return tags
}

// --- Misc describe handlers ---

func (p *Provider) handleDescribeDBEngineVersions(form url.Values) (*plugin.Response, error) {
	type engineVersionXML struct {
		Engine        string `xml:"Engine"`
		EngineVersion string `xml:"EngineVersion"`
	}
	versions := []engineVersionXML{
		{Engine: "docdb", EngineVersion: "5.0.0"},
		{Engine: "docdb", EngineVersion: "4.0.0"},
	}

	type describeEngineVersionsResponse struct {
		XMLName          xml.Name           `xml:"DescribeDBEngineVersionsResponse"`
		DBEngineVersions []engineVersionXML `xml:"DescribeDBEngineVersionsResult>DBEngineVersions>member"`
	}
	return dbXMLResponse(http.StatusOK, describeEngineVersionsResponse{DBEngineVersions: versions})
}

func (p *Provider) handleDescribeOrderableDBInstanceOptions(form url.Values) (*plugin.Response, error) {
	type orderableOptionXML struct {
		Engine          string `xml:"Engine"`
		EngineVersion   string `xml:"EngineVersion"`
		DBInstanceClass string `xml:"DBInstanceClass"`
	}
	options := []orderableOptionXML{
		{Engine: "docdb", EngineVersion: "5.0.0", DBInstanceClass: "db.r6g.large"},
		{Engine: "docdb", EngineVersion: "5.0.0", DBInstanceClass: "db.r6g.xlarge"},
	}

	type describeOrderableResponse struct {
		XMLName                    xml.Name             `xml:"DescribeOrderableDBInstanceOptionsResponse"`
		OrderableDBInstanceOptions []orderableOptionXML `xml:"DescribeOrderableDBInstanceOptionsResult>OrderableDBInstanceOptions>member"`
	}
	return dbXMLResponse(http.StatusOK, describeOrderableResponse{OrderableDBInstanceOptions: options})
}

func (p *Provider) handleDescribeEvents(form url.Values) (*plugin.Response, error) {
	type describeEventsResponse struct {
		XMLName xml.Name   `xml:"DescribeEventsResponse"`
		Events  []struct{} `xml:"DescribeEventsResult>Events>member"`
	}
	return dbXMLResponse(http.StatusOK, describeEventsResponse{})
}

func (p *Provider) handleDescribeEventCategories(form url.Values) (*plugin.Response, error) {
	type eventCategoryMapXML struct {
		SourceType      string   `xml:"SourceType"`
		EventCategories []string `xml:"EventCategories>member"`
	}
	categories := []eventCategoryMapXML{
		{SourceType: "db-cluster", EventCategories: []string{"creation", "deletion", "failover", "maintenance"}},
		{SourceType: "db-instance", EventCategories: []string{"availability", "backup", "creation", "deletion", "maintenance", "recovery"}},
	}

	type describeEventCategoriesResponse struct {
		XMLName                xml.Name              `xml:"DescribeEventCategoriesResponse"`
		EventCategoriesMapList []eventCategoryMapXML `xml:"DescribeEventCategoriesResult>EventCategoriesMapList>member"`
	}
	return dbXMLResponse(http.StatusOK, describeEventCategoriesResponse{EventCategoriesMapList: categories})
}

func (p *Provider) handleDescribePendingMaintenanceActions(form url.Values) (*plugin.Response, error) {
	type describePendingResponse struct {
		XMLName                   xml.Name   `xml:"DescribePendingMaintenanceActionsResponse"`
		PendingMaintenanceActions []struct{} `xml:"DescribePendingMaintenanceActionsResult>PendingMaintenanceActions>member"`
	}
	return dbXMLResponse(http.StatusOK, describePendingResponse{})
}

func (p *Provider) handleApplyPendingMaintenanceAction(form url.Values) (*plugin.Response, error) {
	resourceID := form.Get("ResourceIdentifier")

	type applyPendingResponse struct {
		XMLName xml.Name `xml:"ApplyPendingMaintenanceActionResponse"`
		Result  struct {
			ResourceIdentifier string `xml:"ResourcePendingMaintenanceActions>ResourceIdentifier"`
		} `xml:"ApplyPendingMaintenanceActionResult"`
	}
	var resp applyPendingResponse
	resp.Result.ResourceIdentifier = resourceID
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeCertificates(form url.Values) (*plugin.Response, error) {
	type certificateXML struct {
		CertificateIdentifier string `xml:"CertificateIdentifier"`
		CertificateType       string `xml:"CertificateType"`
	}
	certs := []certificateXML{
		{CertificateIdentifier: "rds-ca-2019", CertificateType: "CA"},
	}

	type describeCertificatesResponse struct {
		XMLName      xml.Name         `xml:"DescribeCertificatesResponse"`
		Certificates []certificateXML `xml:"DescribeCertificatesResult>Certificates>member"`
	}
	return dbXMLResponse(http.StatusOK, describeCertificatesResponse{Certificates: certs})
}

// --- EventSubscription handlers (stub) ---

func (p *Provider) handleCreateEventSubscription(form url.Values) (*plugin.Response, error) {
	name := form.Get("SubscriptionName")
	type createEventSubResponse struct {
		XMLName xml.Name `xml:"CreateEventSubscriptionResponse"`
		Result  struct {
			SubscriptionName string `xml:"EventSubscription>SubscriptionName"`
			Status           string `xml:"EventSubscription>Status"`
		} `xml:"CreateEventSubscriptionResult"`
	}
	var resp createEventSubResponse
	resp.Result.SubscriptionName = name
	resp.Result.Status = "active"
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeEventSubscriptions(form url.Values) (*plugin.Response, error) {
	type describeEventSubsResponse struct {
		XMLName                xml.Name   `xml:"DescribeEventSubscriptionsResponse"`
		EventSubscriptionsList []struct{} `xml:"DescribeEventSubscriptionsResult>EventSubscriptionsList>member"`
	}
	return dbXMLResponse(http.StatusOK, describeEventSubsResponse{})
}

func (p *Provider) handleModifyEventSubscription(form url.Values) (*plugin.Response, error) {
	name := form.Get("SubscriptionName")
	type modifyEventSubResponse struct {
		XMLName xml.Name `xml:"ModifyEventSubscriptionResponse"`
		Result  struct {
			SubscriptionName string `xml:"EventSubscription>SubscriptionName"`
		} `xml:"ModifyEventSubscriptionResult"`
	}
	var resp modifyEventSubResponse
	resp.Result.SubscriptionName = name
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteEventSubscription(form url.Values) (*plugin.Response, error) {
	name := form.Get("SubscriptionName")
	type deleteEventSubResponse struct {
		XMLName xml.Name `xml:"DeleteEventSubscriptionResponse"`
		Result  struct {
			SubscriptionName string `xml:"EventSubscription>SubscriptionName"`
		} `xml:"DeleteEventSubscriptionResult"`
	}
	var resp deleteEventSubResponse
	resp.Result.SubscriptionName = name
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleAddSourceIdentifierToSubscription(form url.Values) (*plugin.Response, error) {
	name := form.Get("SubscriptionName")
	type addSrcResponse struct {
		XMLName xml.Name `xml:"AddSourceIdentifierToSubscriptionResponse"`
		Result  struct {
			SubscriptionName string `xml:"EventSubscription>SubscriptionName"`
		} `xml:"AddSourceIdentifierToSubscriptionResult"`
	}
	var resp addSrcResponse
	resp.Result.SubscriptionName = name
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleRemoveSourceIdentifierFromSubscription(form url.Values) (*plugin.Response, error) {
	name := form.Get("SubscriptionName")
	type removeSrcResponse struct {
		XMLName xml.Name `xml:"RemoveSourceIdentifierFromSubscriptionResponse"`
		Result  struct {
			SubscriptionName string `xml:"EventSubscription>SubscriptionName"`
		} `xml:"RemoveSourceIdentifierFromSubscriptionResult"`
	}
	var resp removeSrcResponse
	resp.Result.SubscriptionName = name
	return dbXMLResponse(http.StatusOK, resp)
}

// --- GlobalCluster handlers (stub) ---

func (p *Provider) handleCreateGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type createGlobalClusterResponse struct {
		XMLName xml.Name `xml:"CreateGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
			Status                  string `xml:"GlobalCluster>Status"`
		} `xml:"CreateGlobalClusterResult"`
	}
	var resp createGlobalClusterResponse
	resp.Result.GlobalClusterIdentifier = id
	resp.Result.Status = "available"
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDescribeGlobalClusters(form url.Values) (*plugin.Response, error) {
	type describeGlobalClustersResponse struct {
		XMLName        xml.Name   `xml:"DescribeGlobalClustersResponse"`
		GlobalClusters []struct{} `xml:"DescribeGlobalClustersResult>GlobalClusters>member"`
	}
	return dbXMLResponse(http.StatusOK, describeGlobalClustersResponse{})
}

func (p *Provider) handleModifyGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type modifyGlobalClusterResponse struct {
		XMLName xml.Name `xml:"ModifyGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
		} `xml:"ModifyGlobalClusterResult"`
	}
	var resp modifyGlobalClusterResponse
	resp.Result.GlobalClusterIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleDeleteGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type deleteGlobalClusterResponse struct {
		XMLName xml.Name `xml:"DeleteGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
		} `xml:"DeleteGlobalClusterResult"`
	}
	var resp deleteGlobalClusterResponse
	resp.Result.GlobalClusterIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleRemoveFromGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type removeFromGlobalResponse struct {
		XMLName xml.Name `xml:"RemoveFromGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
		} `xml:"RemoveFromGlobalClusterResult"`
	}
	var resp removeFromGlobalResponse
	resp.Result.GlobalClusterIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleFailoverGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type failoverGlobalClusterResponse struct {
		XMLName xml.Name `xml:"FailoverGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
		} `xml:"FailoverGlobalClusterResult"`
	}
	var resp failoverGlobalClusterResponse
	resp.Result.GlobalClusterIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleSwitchoverGlobalCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("GlobalClusterIdentifier")
	type switchoverGlobalClusterResponse struct {
		XMLName xml.Name `xml:"SwitchoverGlobalClusterResponse"`
		Result  struct {
			GlobalClusterIdentifier string `xml:"GlobalCluster>GlobalClusterIdentifier"`
		} `xml:"SwitchoverGlobalClusterResult"`
	}
	var resp switchoverGlobalClusterResponse
	resp.Result.GlobalClusterIdentifier = id
	return dbXMLResponse(http.StatusOK, resp)
}
