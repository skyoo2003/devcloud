// SPDX-License-Identifier: Apache-2.0

package rds

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

// Provider implements the RDSv19 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "rds" }
func (p *Provider) ServiceName() string           { return "RDSv19" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init rds: %w", err)
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
		return rdsError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return rdsError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
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
	case "StartDBInstance":
		return p.handleStartDBInstance(form)
	case "StopDBInstance":
		return p.handleStopDBInstance(form)

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
	case "RebootDBCluster":
		return p.handleRebootDBCluster(form)

	// DBSnapshot
	case "CreateDBSnapshot":
		return p.handleCreateDBSnapshot(form)
	case "DescribeDBSnapshots":
		return p.handleDescribeDBSnapshots(form)
	case "DeleteDBSnapshot":
		return p.handleDeleteDBSnapshot(form)
	case "CopyDBSnapshot":
		return p.handleCopyDBSnapshot(form)

	// DBClusterSnapshot
	case "CreateDBClusterSnapshot":
		return p.handleCreateDBClusterSnapshot(form)
	case "DescribeDBClusterSnapshots":
		return p.handleDescribeDBClusterSnapshots(form)
	case "DeleteDBClusterSnapshot":
		return p.handleDeleteDBClusterSnapshot(form)
	case "CopyDBClusterSnapshot":
		return p.handleCopyDBClusterSnapshot(form)

	// DBParameterGroup
	case "CreateDBParameterGroup":
		return p.handleCreateDBParameterGroup(form)
	case "DescribeDBParameterGroups":
		return p.handleDescribeDBParameterGroups(form)
	case "ModifyDBParameterGroup":
		return p.handleModifyDBParameterGroup(form)
	case "DeleteDBParameterGroup":
		return p.handleDeleteDBParameterGroup(form)
	case "ResetDBParameterGroup":
		return p.handleResetDBParameterGroup(form)
	case "DescribeDBParameters":
		return p.handleDescribeDBParameters(form)
	case "DescribeEngineDefaultParameters":
		return p.handleDescribeEngineDefaultParameters(form)
	case "CopyDBParameterGroup":
		return p.handleCopyDBParameterGroup(form)

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

	// DBSubnetGroup
	case "CreateDBSubnetGroup":
		return p.handleCreateDBSubnetGroup(form)
	case "DescribeDBSubnetGroups":
		return p.handleDescribeDBSubnetGroups(form)
	case "ModifyDBSubnetGroup":
		return p.handleModifyDBSubnetGroup(form)
	case "DeleteDBSubnetGroup":
		return p.handleDeleteDBSubnetGroup(form)

	// OptionGroup
	case "CreateOptionGroup":
		return p.handleCreateOptionGroup(form)
	case "DescribeOptionGroups":
		return p.handleDescribeOptionGroups(form)
	case "ModifyOptionGroup":
		return p.handleModifyOptionGroup(form)
	case "DeleteOptionGroup":
		return p.handleDeleteOptionGroup(form)
	case "CopyOptionGroup":
		return p.handleCopyOptionGroup(form)
	case "DescribeOptionGroupOptions":
		return p.handleDescribeOptionGroupOptions(form)

	// Tags
	case "AddTagsToResource":
		return p.handleAddTagsToResource(form)
	case "RemoveTagsFromResource":
		return p.handleRemoveTagsFromResource(form)
	case "ListTagsForResource":
		return p.handleListTagsForResource(form)

	// Misc describe
	case "DescribeDBEngineVersions":
		return p.handleDescribeDBEngineVersions(form)
	case "DescribeOrderableDBInstanceOptions":
		return p.handleDescribeOrderableDBInstanceOptions(form)
	case "DescribeAccountAttributes":
		return p.handleDescribeAccountAttributes(form)
	case "DescribeEvents":
		return p.handleDescribeEvents(form)
	case "DescribeEventCategories":
		return p.handleDescribeEventCategories(form)
	case "DescribeSourceRegions":
		return p.handleDescribeSourceRegions(form)
	case "DescribeCertificates":
		return p.handleDescribeCertificates(form)
	case "DescribePendingMaintenanceActions":
		return p.handleDescribePendingMaintenanceActions(form)
	case "ApplyPendingMaintenanceAction":
		return p.handleApplyPendingMaintenanceAction(form)

	default:
		type genericResult struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return shared.XMLResponse(http.StatusOK, genericResult{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	instances, err := p.store.ListInstances(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(instances))
	for _, inst := range instances {
		out = append(out, plugin.Resource{Type: "db-instance", ID: inst.ID, Name: inst.ID})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func rdsError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func rdsXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type dbEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type dbInstanceXML struct {
	DBInstanceIdentifier string        `xml:"DBInstanceIdentifier"`
	DBClusterIdentifier  string        `xml:"DBClusterIdentifier"`
	DBInstanceStatus     string        `xml:"DBInstanceStatus"`
	DBInstanceClass      string        `xml:"DBInstanceClass"`
	Engine               string        `xml:"Engine"`
	EngineVersion        string        `xml:"EngineVersion"`
	MasterUsername       string        `xml:"MasterUsername"`
	DBName               string        `xml:"DBName"`
	Endpoint             dbEndpointXML `xml:"Endpoint"`
	AvailabilityZone     string        `xml:"AvailabilityZone"`
	MultiAZ              bool          `xml:"MultiAZ"`
	StorageType          string        `xml:"StorageType"`
	AllocatedStorage     int           `xml:"AllocatedStorage"`
	StorageEncrypted     bool          `xml:"StorageEncrypted"`
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
		MasterUsername:       inst.MasterUser,
		DBName:               inst.DBName,
		Endpoint: dbEndpointXML{
			Address: inst.Endpoint,
			Port:    inst.Port,
		},
		AvailabilityZone:   inst.AZ,
		MultiAZ:            inst.MultiAZ,
		StorageType:        inst.StorageType,
		AllocatedStorage:   inst.AllocatedStorage,
		StorageEncrypted:   inst.Encrypted,
		InstanceCreateTime: inst.CreatedAt.UTC().Format(time.RFC3339),
		DBInstanceArn:      inst.ARN,
	}
}

type dbClusterXML struct {
	DBClusterIdentifier string `xml:"DBClusterIdentifier"`
	Status              string `xml:"Status"`
	Engine              string `xml:"Engine"`
	EngineVersion       string `xml:"EngineVersion"`
	MasterUsername      string `xml:"MasterUsername"`
	DatabaseName        string `xml:"DatabaseName"`
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
		DatabaseName:        c.DBName,
		Endpoint:            c.Endpoint,
		ReaderEndpoint:      c.ReaderEndpoint,
		Port:                c.Port,
		StorageEncrypted:    c.StorageEncrypted,
		ClusterCreateTime:   c.CreatedAt.UTC().Format(time.RFC3339),
		DBClusterArn:        c.ARN,
	}
}

type dbSnapshotXML struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	DBSnapshotArn        string `xml:"DBSnapshotArn"`
	Status               string `xml:"Status"`
	Engine               string `xml:"Engine"`
	SnapshotType         string `xml:"SnapshotType"`
	SnapshotCreateTime   string `xml:"SnapshotCreateTime"`
}

func snapshotToXML(sn *DBSnapshot) dbSnapshotXML {
	return dbSnapshotXML{
		DBSnapshotIdentifier: sn.ID,
		DBInstanceIdentifier: sn.InstanceID,
		DBSnapshotArn:        sn.ARN,
		Status:               sn.Status,
		Engine:               sn.Engine,
		SnapshotType:         sn.SnapshotType,
		SnapshotCreateTime:   sn.CreatedAt.UTC().Format(time.RFC3339),
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

func clusterSnapshotToXML(sn *ClusterSnapshot) clusterSnapshotXML {
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

type dbParamGroupXML struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
	DBParameterGroupArn    string `xml:"DBParameterGroupArn"`
}

func paramGroupToXML(pg *DBParamGroup) dbParamGroupXML {
	return dbParamGroupXML{
		DBParameterGroupName:   pg.Name,
		DBParameterGroupFamily: pg.Family,
		Description:            pg.Description,
		DBParameterGroupArn:    pg.ARN,
	}
}

type clusterParamGroupXML struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
	DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn"`
}

func clusterParamGroupToXML(pg *ClusterParamGroup) clusterParamGroupXML {
	return clusterParamGroupXML{
		DBClusterParameterGroupName: pg.Name,
		DBParameterGroupFamily:      pg.Family,
		Description:                 pg.Description,
		DBClusterParameterGroupArn:  pg.ARN,
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

type optionGroupXML struct {
	OptionGroupName        string `xml:"OptionGroupName"`
	EngineName             string `xml:"EngineName"`
	MajorEngineVersion     string `xml:"MajorEngineVersion"`
	OptionGroupDescription string `xml:"OptionGroupDescription"`
	OptionGroupArn         string `xml:"OptionGroupArn"`
}

func optionGroupToXML(og *OptionGroup) optionGroupXML {
	return optionGroupXML{
		OptionGroupName:        og.Name,
		EngineName:             og.Engine,
		MajorEngineVersion:     og.MajorVersion,
		OptionGroupDescription: og.Description,
		OptionGroupArn:         og.ARN,
	}
}

// --- DBInstance handlers ---

func (p *Provider) handleCreateDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "mysql"
	}
	engineVersion := form.Get("EngineVersion")
	if engineVersion == "" {
		engineVersion = "8.0.35"
	}
	instanceClass := form.Get("DBInstanceClass")
	if instanceClass == "" {
		instanceClass = "db.t3.medium"
	}
	masterUser := form.Get("MasterUsername")
	if masterUser == "" {
		masterUser = "admin"
	}
	dbName := form.Get("DBName")
	az := form.Get("AvailabilityZone")
	if az == "" {
		az = "us-east-1a"
	}
	storageType := form.Get("StorageType")
	if storageType == "" {
		storageType = "gp3"
	}
	clusterID := form.Get("DBClusterIdentifier")
	port := 3306

	arn := shared.BuildARN("rds", "db", id)
	inst, err := p.store.CreateInstance(id, arn, clusterID, engine, engineVersion, instanceClass, masterUser, dbName, az, storageType, port, 20, false, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBInstanceAlreadyExists", "instance already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createDBInstanceResult struct {
		XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"CreateDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, createDBInstanceResult{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleDescribeDBInstances(form url.Values) (*plugin.Response, error) {
	var ids []string
	requestedID := form.Get("DBInstanceIdentifier")
	if requestedID != "" {
		ids = append(ids, requestedID)
	}
	instances, err := p.store.ListInstances(ids)
	if err != nil {
		return nil, err
	}
	if requestedID != "" && len(instances) == 0 {
		return rdsError("DBInstanceNotFound", "instance not found: "+requestedID, http.StatusNotFound), nil
	}
	items := make([]dbInstanceXML, 0, len(instances))
	for i := range instances {
		items = append(items, instanceToXML(&instances[i]))
	}

	type describeDBInstancesResponse struct {
		XMLName     xml.Name        `xml:"DescribeDBInstancesResponse"`
		DBInstances []dbInstanceXML `xml:"DescribeDBInstancesResult>DBInstances>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeDBInstancesResponse{DBInstances: items})
}

func (p *Provider) handleModifyDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	inst, err := p.store.GetInstance(id)
	if err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if ic := form.Get("DBInstanceClass"); ic != "" {
		if err := p.store.UpdateInstanceClass(id, ic); err != nil {
			return nil, err
		}
		inst.InstanceClass = ic
	}

	type modifyDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"ModifyDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, modifyDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleDeleteDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	inst, err := p.store.DeleteInstance(id)
	if err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"DeleteDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, deleteDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleRebootDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateInstanceStatus(id, "rebooting"); err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	inst, _ := p.store.GetInstance(id)

	type rebootDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"RebootDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, rebootDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleStartDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateInstanceStatus(id, "available"); err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	inst, _ := p.store.GetInstance(id)

	type startDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"StartDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"StartDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, startDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

func (p *Provider) handleStopDBInstance(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBInstanceIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateInstanceStatus(id, "stopped"); err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	inst, _ := p.store.GetInstance(id)

	type stopDBInstanceResponse struct {
		XMLName    xml.Name      `xml:"StopDBInstanceResponse"`
		DBInstance dbInstanceXML `xml:"StopDBInstanceResult>DBInstance"`
	}
	return rdsXMLResponse(http.StatusOK, stopDBInstanceResponse{DBInstance: instanceToXML(inst)})
}

// --- DBCluster handlers ---

func (p *Provider) handleCreateDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "aurora-mysql"
	}
	engineVersion := form.Get("EngineVersion")
	if engineVersion == "" {
		engineVersion = "8.0.mysql_aurora.3.07.0"
	}
	masterUser := form.Get("MasterUsername")
	if masterUser == "" {
		masterUser = "admin"
	}
	dbName := form.Get("DatabaseName")
	port := 3306
	arn := shared.BuildARN("rds", "cluster", id)
	c, err := p.store.CreateCluster(id, arn, engine, engineVersion, masterUser, dbName, port, false)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBClusterAlreadyExistsFault", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createDBClusterResult struct {
		XMLName   xml.Name     `xml:"CreateDBClusterResponse"`
		DBCluster dbClusterXML `xml:"CreateDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, createDBClusterResult{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleDescribeDBClusters(form url.Values) (*plugin.Response, error) {
	var ids []string
	if id := form.Get("DBClusterIdentifier"); id != "" {
		ids = append(ids, id)
	}
	clusters, err := p.store.ListClusters(ids)
	if err != nil {
		return nil, err
	}
	if form.Get("DBClusterIdentifier") != "" && len(clusters) == 0 {
		return rdsError("DBClusterNotFoundFault", "cluster not found: "+form.Get("DBClusterIdentifier"), http.StatusNotFound), nil
	}
	items := make([]dbClusterXML, 0, len(clusters))
	for i := range clusters {
		items = append(items, clusterToXML(&clusters[i]))
	}

	type describeDBClustersResponse struct {
		XMLName    xml.Name       `xml:"DescribeDBClustersResponse"`
		DBClusters []dbClusterXML `xml:"DescribeDBClustersResult>DBClusters>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeDBClustersResponse{DBClusters: items})
}

func (p *Provider) handleModifyDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
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
	return rdsXMLResponse(http.StatusOK, modifyDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleDeleteDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.DeleteCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteDBClusterResponse struct {
		XMLName   xml.Name     `xml:"DeleteDBClusterResponse"`
		DBCluster dbClusterXML `xml:"DeleteDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, deleteDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleStartDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "available"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type startDBClusterResponse struct {
		XMLName   xml.Name     `xml:"StartDBClusterResponse"`
		DBCluster dbClusterXML `xml:"StartDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, startDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleStopDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateClusterStatus(id, "stopped"); err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	c, _ := p.store.GetCluster(id)

	type stopDBClusterResponse struct {
		XMLName   xml.Name     `xml:"StopDBClusterResponse"`
		DBCluster dbClusterXML `xml:"StopDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, stopDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleFailoverDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type failoverDBClusterResponse struct {
		XMLName   xml.Name     `xml:"FailoverDBClusterResponse"`
		DBCluster dbClusterXML `xml:"FailoverDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, failoverDBClusterResponse{DBCluster: clusterToXML(c)})
}

func (p *Provider) handleRebootDBCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterIdentifier is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type rebootDBClusterResponse struct {
		XMLName   xml.Name     `xml:"RebootDBClusterResponse"`
		DBCluster dbClusterXML `xml:"RebootDBClusterResult>DBCluster"`
	}
	return rdsXMLResponse(http.StatusOK, rebootDBClusterResponse{DBCluster: clusterToXML(c)})
}

// --- DBSnapshot handlers ---

func (p *Provider) handleCreateDBSnapshot(form url.Values) (*plugin.Response, error) {
	snapshotID := form.Get("DBSnapshotIdentifier")
	instanceID := form.Get("DBInstanceIdentifier")
	if snapshotID == "" || instanceID == "" {
		return rdsError("MissingParameter", "DBSnapshotIdentifier and DBInstanceIdentifier are required", http.StatusBadRequest), nil
	}
	inst, err := p.store.GetInstance(instanceID)
	if err != nil {
		if errors.Is(err, errInstanceNotFound) {
			return rdsError("DBInstanceNotFound", "instance not found: "+instanceID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "snapshot", snapshotID)
	sn, err := p.store.CreateSnapshot(snapshotID, arn, instanceID, inst.Engine, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBSnapshotAlreadyExists", "snapshot already exists: "+snapshotID, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSnapshotResponse struct {
		XMLName    xml.Name      `xml:"CreateDBSnapshotResponse"`
		DBSnapshot dbSnapshotXML `xml:"CreateDBSnapshotResult>DBSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, createSnapshotResponse{DBSnapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDescribeDBSnapshots(form url.Values) (*plugin.Response, error) {
	instanceID := form.Get("DBInstanceIdentifier")
	snaps, err := p.store.ListSnapshots(instanceID)
	if err != nil {
		return nil, err
	}
	items := make([]dbSnapshotXML, 0, len(snaps))
	for i := range snaps {
		items = append(items, snapshotToXML(&snaps[i]))
	}

	type describeSnapshotsResponse struct {
		XMLName     xml.Name        `xml:"DescribeDBSnapshotsResponse"`
		DBSnapshots []dbSnapshotXML `xml:"DescribeDBSnapshotsResult>DBSnapshots>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeSnapshotsResponse{DBSnapshots: items})
}

func (p *Provider) handleDeleteDBSnapshot(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBSnapshotIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBSnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	sn, err := p.store.DeleteSnapshot(id)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return rdsError("DBSnapshotNotFound", "snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSnapshotResponse struct {
		XMLName    xml.Name      `xml:"DeleteDBSnapshotResponse"`
		DBSnapshot dbSnapshotXML `xml:"DeleteDBSnapshotResult>DBSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, deleteSnapshotResponse{DBSnapshot: snapshotToXML(sn)})
}

func (p *Provider) handleCopyDBSnapshot(form url.Values) (*plugin.Response, error) {
	srcID := form.Get("SourceDBSnapshotIdentifier")
	destID := form.Get("TargetDBSnapshotIdentifier")
	if srcID == "" || destID == "" {
		return rdsError("MissingParameter", "SourceDBSnapshotIdentifier and TargetDBSnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(srcID)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return rdsError("DBSnapshotNotFound", "snapshot not found: "+srcID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "snapshot", destID)
	sn, err := p.store.CreateSnapshot(destID, arn, src.InstanceID, src.Engine, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBSnapshotAlreadyExists", "snapshot already exists: "+destID, http.StatusConflict), nil
		}
		return nil, err
	}

	type copySnapshotResponse struct {
		XMLName    xml.Name      `xml:"CopyDBSnapshotResponse"`
		DBSnapshot dbSnapshotXML `xml:"CopyDBSnapshotResult>DBSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, copySnapshotResponse{DBSnapshot: snapshotToXML(sn)})
}

// --- DBClusterSnapshot handlers ---

func (p *Provider) handleCreateDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	snapshotID := form.Get("DBClusterSnapshotIdentifier")
	clusterID := form.Get("DBClusterIdentifier")
	if snapshotID == "" || clusterID == "" {
		return rdsError("MissingParameter", "DBClusterSnapshotIdentifier and DBClusterIdentifier are required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(clusterID)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return rdsError("DBClusterNotFoundFault", "cluster not found: "+clusterID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "cluster-snapshot", snapshotID)
	sn, err := p.store.CreateClusterSnapshot(snapshotID, arn, clusterID, c.Engine, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBClusterSnapshotAlreadyExistsFault", "cluster snapshot already exists: "+snapshotID, http.StatusConflict), nil
		}
		return nil, err
	}

	type createClusterSnapshotResponse struct {
		XMLName           xml.Name           `xml:"CreateDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, createClusterSnapshotResponse{DBClusterSnapshot: clusterSnapshotToXML(sn)})
}

func (p *Provider) handleDescribeDBClusterSnapshots(form url.Values) (*plugin.Response, error) {
	clusterID := form.Get("DBClusterIdentifier")
	snaps, err := p.store.ListClusterSnapshots(clusterID)
	if err != nil {
		return nil, err
	}
	items := make([]clusterSnapshotXML, 0, len(snaps))
	for i := range snaps {
		items = append(items, clusterSnapshotToXML(&snaps[i]))
	}

	type describeClusterSnapshotsResponse struct {
		XMLName            xml.Name             `xml:"DescribeDBClusterSnapshotsResponse"`
		DBClusterSnapshots []clusterSnapshotXML `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeClusterSnapshotsResponse{DBClusterSnapshots: items})
}

func (p *Provider) handleDeleteDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	id := form.Get("DBClusterSnapshotIdentifier")
	if id == "" {
		return rdsError("MissingParameter", "DBClusterSnapshotIdentifier is required", http.StatusBadRequest), nil
	}
	sn, err := p.store.DeleteClusterSnapshot(id)
	if err != nil {
		if errors.Is(err, errClusterSnapNotFound) {
			return rdsError("DBClusterSnapshotNotFoundFault", "cluster snapshot not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteClusterSnapshotResponse struct {
		XMLName           xml.Name           `xml:"DeleteDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, deleteClusterSnapshotResponse{DBClusterSnapshot: clusterSnapshotToXML(sn)})
}

func (p *Provider) handleCopyDBClusterSnapshot(form url.Values) (*plugin.Response, error) {
	srcID := form.Get("SourceDBClusterSnapshotIdentifier")
	destID := form.Get("TargetDBClusterSnapshotIdentifier")
	if srcID == "" || destID == "" {
		return rdsError("MissingParameter", "SourceDBClusterSnapshotIdentifier and TargetDBClusterSnapshotIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetClusterSnapshot(srcID)
	if err != nil {
		if errors.Is(err, errClusterSnapNotFound) {
			return rdsError("DBClusterSnapshotNotFoundFault", "cluster snapshot not found: "+srcID, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("rds", "cluster-snapshot", destID)
	sn, err := p.store.CreateClusterSnapshot(destID, arn, src.ClusterID, src.Engine, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBClusterSnapshotAlreadyExistsFault", "cluster snapshot already exists: "+destID, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyClusterSnapshotResponse struct {
		XMLName           xml.Name           `xml:"CopyDBClusterSnapshotResponse"`
		DBClusterSnapshot clusterSnapshotXML `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
	}
	return rdsXMLResponse(http.StatusOK, copyClusterSnapshotResponse{DBClusterSnapshot: clusterSnapshotToXML(sn)})
}

// --- DBParameterGroup handlers ---

func (p *Provider) handleCreateDBParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBParameterGroupName is required", http.StatusBadRequest), nil
	}
	family := form.Get("DBParameterGroupFamily")
	if family == "" {
		family = "mysql8.0"
	}
	description := form.Get("Description")
	arn := shared.BuildARN("rds", "pg", name)
	pg, err := p.store.CreateParamGroup(name, arn, family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBParameterGroupAlreadyExists", "parameter group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createParamGroupResponse struct {
		XMLName          xml.Name        `xml:"CreateDBParameterGroupResponse"`
		DBParameterGroup dbParamGroupXML `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
	}
	return rdsXMLResponse(http.StatusOK, createParamGroupResponse{DBParameterGroup: paramGroupToXML(pg)})
}

func (p *Provider) handleDescribeDBParameterGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("DBParameterGroupName"); name != "" {
		names = append(names, name)
	}
	groups, err := p.store.ListParamGroups(names)
	if err != nil {
		return nil, err
	}
	items := make([]dbParamGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, paramGroupToXML(&groups[i]))
	}

	type describeParamGroupsResponse struct {
		XMLName           xml.Name          `xml:"DescribeDBParameterGroupsResponse"`
		DBParameterGroups []dbParamGroupXML `xml:"DescribeDBParameterGroupsResult>DBParameterGroups>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeParamGroupsResponse{DBParameterGroups: items})
}

func (p *Provider) handleModifyDBParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rdsError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyParamGroupResponse struct {
		XMLName              xml.Name `xml:"ModifyDBParameterGroupResponse"`
		DBParameterGroupName string   `xml:"ModifyDBParameterGroupResult>DBParameterGroupName"`
	}
	return rdsXMLResponse(http.StatusOK, modifyParamGroupResponse{DBParameterGroupName: name})
}

func (p *Provider) handleDeleteDBParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rdsError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteParamGroupResponse struct {
		XMLName xml.Name `xml:"DeleteDBParameterGroupResponse"`
	}
	return rdsXMLResponse(http.StatusOK, deleteParamGroupResponse{})
}

func (p *Provider) handleResetDBParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetParamGroup(name); err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rdsError("DBParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type resetParamGroupResponse struct {
		XMLName              xml.Name `xml:"ResetDBParameterGroupResponse"`
		DBParameterGroupName string   `xml:"ResetDBParameterGroupResult>DBParameterGroupName"`
	}
	return rdsXMLResponse(http.StatusOK, resetParamGroupResponse{DBParameterGroupName: name})
}

func (p *Provider) handleDescribeDBParameters(form url.Values) (*plugin.Response, error) {
	type describeParamsResponse struct {
		XMLName    xml.Name   `xml:"DescribeDBParametersResponse"`
		Parameters []struct{} `xml:"DescribeDBParametersResult>Parameters>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeParamsResponse{})
}

func (p *Provider) handleDescribeEngineDefaultParameters(form url.Values) (*plugin.Response, error) {
	type describeDefaultParamsResponse struct {
		XMLName xml.Name `xml:"DescribeEngineDefaultParametersResponse"`
		Result  struct {
			DBParameterGroupFamily string `xml:"EngineDefaults>DBParameterGroupFamily"`
		} `xml:"DescribeEngineDefaultParametersResult"`
	}
	var resp describeDefaultParamsResponse
	resp.Result.DBParameterGroupFamily = form.Get("DBParameterGroupFamily")
	return rdsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleCopyDBParameterGroup(form url.Values) (*plugin.Response, error) {
	srcName := form.Get("SourceDBParameterGroupIdentifier")
	destName := form.Get("TargetDBParameterGroupIdentifier")
	if srcName == "" || destName == "" {
		return rdsError("MissingParameter", "SourceDBParameterGroupIdentifier and TargetDBParameterGroupIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetParamGroup(srcName)
	if err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return rdsError("DBParameterGroupNotFound", "parameter group not found: "+srcName, http.StatusNotFound), nil
		}
		return nil, err
	}
	description := form.Get("TargetDBParameterGroupDescription")
	arn := shared.BuildARN("rds", "pg", destName)
	pg, err := p.store.CreateParamGroup(destName, arn, src.Family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBParameterGroupAlreadyExists", "parameter group already exists: "+destName, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyParamGroupResponse struct {
		XMLName          xml.Name        `xml:"CopyDBParameterGroupResponse"`
		DBParameterGroup dbParamGroupXML `xml:"CopyDBParameterGroupResult>DBParameterGroup"`
	}
	return rdsXMLResponse(http.StatusOK, copyParamGroupResponse{DBParameterGroup: paramGroupToXML(pg)})
}

// --- DBClusterParameterGroup handlers ---

func (p *Provider) handleCreateDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	family := form.Get("DBParameterGroupFamily")
	if family == "" {
		family = "aurora-mysql8.0"
	}
	description := form.Get("Description")
	arn := shared.BuildARN("rds", "cluster-pg", name)
	pg, err := p.store.CreateClusterParamGroup(name, arn, family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBParameterGroupAlreadyExists", "cluster parameter group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createClusterParamGroupResponse struct {
		XMLName                 xml.Name             `xml:"CreateDBClusterParameterGroupResponse"`
		DBClusterParameterGroup clusterParamGroupXML `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
	}
	return rdsXMLResponse(http.StatusOK, createClusterParamGroupResponse{DBClusterParameterGroup: clusterParamGroupToXML(pg)})
}

func (p *Provider) handleDescribeDBClusterParameterGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("DBClusterParameterGroupName"); name != "" {
		names = append(names, name)
	}
	groups, err := p.store.ListClusterParamGroups(names)
	if err != nil {
		return nil, err
	}
	items := make([]clusterParamGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, clusterParamGroupToXML(&groups[i]))
	}

	type describeClusterParamGroupsResponse struct {
		XMLName                  xml.Name               `xml:"DescribeDBClusterParameterGroupsResponse"`
		DBClusterParameterGroups []clusterParamGroupXML `xml:"DescribeDBClusterParameterGroupsResult>DBClusterParameterGroups>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeClusterParamGroupsResponse{DBClusterParameterGroups: items})
}

func (p *Provider) handleModifyDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterParamGroup(name); err != nil {
		if errors.Is(err, errClusterPGNotFound) {
			return rdsError("DBParameterGroupNotFound", "cluster parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyClusterParamGroupResponse struct {
		XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
		DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
	}
	return rdsXMLResponse(http.StatusOK, modifyClusterParamGroupResponse{DBClusterParameterGroupName: name})
}

func (p *Provider) handleDeleteDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteClusterParamGroup(name); err != nil {
		if errors.Is(err, errClusterPGNotFound) {
			return rdsError("DBParameterGroupNotFound", "cluster parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteClusterParamGroupResponse struct {
		XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	}
	return rdsXMLResponse(http.StatusOK, deleteClusterParamGroupResponse{})
}

func (p *Provider) handleResetDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBClusterParameterGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBClusterParameterGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterParamGroup(name); err != nil {
		if errors.Is(err, errClusterPGNotFound) {
			return rdsError("DBParameterGroupNotFound", "cluster parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type resetClusterParamGroupResponse struct {
		XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
		DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
	}
	return rdsXMLResponse(http.StatusOK, resetClusterParamGroupResponse{DBClusterParameterGroupName: name})
}

func (p *Provider) handleDescribeDBClusterParameters(form url.Values) (*plugin.Response, error) {
	type describeClusterParamsResponse struct {
		XMLName    xml.Name   `xml:"DescribeDBClusterParametersResponse"`
		Parameters []struct{} `xml:"DescribeDBClusterParametersResult>Parameters>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeClusterParamsResponse{})
}

func (p *Provider) handleDescribeEngineDefaultClusterParameters(form url.Values) (*plugin.Response, error) {
	type describeDefaultClusterParamsResponse struct {
		XMLName xml.Name `xml:"DescribeEngineDefaultClusterParametersResponse"`
		Result  struct {
			DBParameterGroupFamily string `xml:"EngineDefaults>DBParameterGroupFamily"`
		} `xml:"DescribeEngineDefaultClusterParametersResult"`
	}
	var resp describeDefaultClusterParamsResponse
	resp.Result.DBParameterGroupFamily = form.Get("DBParameterGroupFamily")
	return rdsXMLResponse(http.StatusOK, resp)
}

func (p *Provider) handleCopyDBClusterParameterGroup(form url.Values) (*plugin.Response, error) {
	srcName := form.Get("SourceDBClusterParameterGroupIdentifier")
	destName := form.Get("TargetDBClusterParameterGroupIdentifier")
	if srcName == "" || destName == "" {
		return rdsError("MissingParameter", "SourceDBClusterParameterGroupIdentifier and TargetDBClusterParameterGroupIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetClusterParamGroup(srcName)
	if err != nil {
		if errors.Is(err, errClusterPGNotFound) {
			return rdsError("DBParameterGroupNotFound", "cluster parameter group not found: "+srcName, http.StatusNotFound), nil
		}
		return nil, err
	}
	description := form.Get("TargetDBClusterParameterGroupDescription")
	arn := shared.BuildARN("rds", "cluster-pg", destName)
	pg, err := p.store.CreateClusterParamGroup(destName, arn, src.Family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBParameterGroupAlreadyExists", "cluster parameter group already exists: "+destName, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyClusterParamGroupResponse struct {
		XMLName                 xml.Name             `xml:"CopyDBClusterParameterGroupResponse"`
		DBClusterParameterGroup clusterParamGroupXML `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
	}
	return rdsXMLResponse(http.StatusOK, copyClusterParamGroupResponse{DBClusterParameterGroup: clusterParamGroupToXML(pg)})
}

// --- DBSubnetGroup handlers ---

func (p *Provider) handleCreateDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("DBSubnetGroupDescription")
	arn := shared.BuildARN("rds", "subgrp", name)
	sg, err := p.store.CreateSubnetGroup(name, arn, description, "", "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("DBSubnetGroupAlreadyExists", "subnet group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createSubnetGroupResponse struct {
		XMLName       xml.Name       `xml:"CreateDBSubnetGroupResponse"`
		DBSubnetGroup subnetGroupXML `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
	}
	return rdsXMLResponse(http.StatusOK, createSubnetGroupResponse{DBSubnetGroup: subnetGroupToXML(sg)})
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
	return rdsXMLResponse(http.StatusOK, describeSubnetGroupsResponse{DBSubnetGroups: items})
}

func (p *Provider) handleModifyDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	sg, err := p.store.GetSubnetGroup(name)
	if err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return rdsError("DBSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
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
	return rdsXMLResponse(http.StatusOK, modifySubnetGroupResponse{DBSubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDeleteDBSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("DBSubnetGroupName")
	if name == "" {
		return rdsError("MissingParameter", "DBSubnetGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteSubnetGroup(name); err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return rdsError("DBSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteSubnetGroupResponse struct {
		XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	}
	return rdsXMLResponse(http.StatusOK, deleteSubnetGroupResponse{})
}

// --- OptionGroup handlers ---

func (p *Provider) handleCreateOptionGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("OptionGroupName")
	if name == "" {
		return rdsError("MissingParameter", "OptionGroupName is required", http.StatusBadRequest), nil
	}
	engine := form.Get("EngineName")
	if engine == "" {
		engine = "mysql"
	}
	majorVersion := form.Get("MajorEngineVersion")
	if majorVersion == "" {
		majorVersion = "8.0"
	}
	description := form.Get("OptionGroupDescription")
	arn := shared.BuildARN("rds", "og", name)
	og, err := p.store.CreateOptionGroup(name, arn, engine, majorVersion, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("OptionGroupAlreadyExistsFault", "option group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createOptionGroupResponse struct {
		XMLName     xml.Name       `xml:"CreateOptionGroupResponse"`
		OptionGroup optionGroupXML `xml:"CreateOptionGroupResult>OptionGroup"`
	}
	return rdsXMLResponse(http.StatusOK, createOptionGroupResponse{OptionGroup: optionGroupToXML(og)})
}

func (p *Provider) handleDescribeOptionGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("OptionGroupName"); name != "" {
		names = append(names, name)
	}
	groups, err := p.store.ListOptionGroups(names)
	if err != nil {
		return nil, err
	}
	items := make([]optionGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, optionGroupToXML(&groups[i]))
	}

	type describeOptionGroupsResponse struct {
		XMLName          xml.Name         `xml:"DescribeOptionGroupsResponse"`
		OptionGroupsList []optionGroupXML `xml:"DescribeOptionGroupsResult>OptionGroupsList>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeOptionGroupsResponse{OptionGroupsList: items})
}

func (p *Provider) handleModifyOptionGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("OptionGroupName")
	if name == "" {
		return rdsError("MissingParameter", "OptionGroupName is required", http.StatusBadRequest), nil
	}
	og, err := p.store.GetOptionGroup(name)
	if err != nil {
		if errors.Is(err, errOptionGroupNotFound) {
			return rdsError("OptionGroupNotFoundFault", "option group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyOptionGroupResponse struct {
		XMLName     xml.Name       `xml:"ModifyOptionGroupResponse"`
		OptionGroup optionGroupXML `xml:"ModifyOptionGroupResult>OptionGroup"`
	}
	return rdsXMLResponse(http.StatusOK, modifyOptionGroupResponse{OptionGroup: optionGroupToXML(og)})
}

func (p *Provider) handleDeleteOptionGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("OptionGroupName")
	if name == "" {
		return rdsError("MissingParameter", "OptionGroupName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteOptionGroup(name); err != nil {
		if errors.Is(err, errOptionGroupNotFound) {
			return rdsError("OptionGroupNotFoundFault", "option group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteOptionGroupResponse struct {
		XMLName xml.Name `xml:"DeleteOptionGroupResponse"`
	}
	return rdsXMLResponse(http.StatusOK, deleteOptionGroupResponse{})
}

func (p *Provider) handleCopyOptionGroup(form url.Values) (*plugin.Response, error) {
	srcName := form.Get("SourceOptionGroupIdentifier")
	destName := form.Get("TargetOptionGroupIdentifier")
	if srcName == "" || destName == "" {
		return rdsError("MissingParameter", "SourceOptionGroupIdentifier and TargetOptionGroupIdentifier are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetOptionGroup(srcName)
	if err != nil {
		if errors.Is(err, errOptionGroupNotFound) {
			return rdsError("OptionGroupNotFoundFault", "option group not found: "+srcName, http.StatusNotFound), nil
		}
		return nil, err
	}
	description := form.Get("TargetOptionGroupDescription")
	arn := shared.BuildARN("rds", "og", destName)
	og, err := p.store.CreateOptionGroup(destName, arn, src.Engine, src.MajorVersion, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return rdsError("OptionGroupAlreadyExistsFault", "option group already exists: "+destName, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyOptionGroupResponse struct {
		XMLName     xml.Name       `xml:"CopyOptionGroupResponse"`
		OptionGroup optionGroupXML `xml:"CopyOptionGroupResult>OptionGroup"`
	}
	return rdsXMLResponse(http.StatusOK, copyOptionGroupResponse{OptionGroup: optionGroupToXML(og)})
}

func (p *Provider) handleDescribeOptionGroupOptions(form url.Values) (*plugin.Response, error) {
	type describeOptionGroupOptionsResponse struct {
		XMLName            xml.Name   `xml:"DescribeOptionGroupOptionsResponse"`
		OptionGroupOptions []struct{} `xml:"DescribeOptionGroupOptionsResult>OptionGroupOptions>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeOptionGroupOptionsResponse{})
}

// --- Tag handlers ---

func (p *Provider) handleAddTagsToResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return rdsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tags := parseFormTags(form)
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, err
	}

	type addTagsResponse struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	}
	return rdsXMLResponse(http.StatusOK, addTagsResponse{})
}

func (p *Provider) handleRemoveTagsFromResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return rdsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
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
	return rdsXMLResponse(http.StatusOK, removeTagsResponse{})
}

func (p *Provider) handleListTagsForResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return rdsError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
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
	return rdsXMLResponse(http.StatusOK, listTagsResponse{TagList: items})
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
		{Engine: "mysql", EngineVersion: "8.0.35"},
		{Engine: "mysql", EngineVersion: "8.0.28"},
		{Engine: "postgres", EngineVersion: "15.4"},
		{Engine: "postgres", EngineVersion: "14.9"},
		{Engine: "aurora-mysql", EngineVersion: "8.0.mysql_aurora.3.07.0"},
		{Engine: "aurora-postgresql", EngineVersion: "15.4"},
		{Engine: "mariadb", EngineVersion: "10.6.14"},
		{Engine: "oracle-ee", EngineVersion: "19.0.0.0.ru-2023-07.rur-2023-07.r1"},
		{Engine: "sqlserver-ee", EngineVersion: "15.00.4316.3.v1"},
	}

	type describeEngineVersionsResponse struct {
		XMLName          xml.Name           `xml:"DescribeDBEngineVersionsResponse"`
		DBEngineVersions []engineVersionXML `xml:"DescribeDBEngineVersionsResult>DBEngineVersions>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeEngineVersionsResponse{DBEngineVersions: versions})
}

func (p *Provider) handleDescribeOrderableDBInstanceOptions(form url.Values) (*plugin.Response, error) {
	type orderableOptionXML struct {
		Engine          string `xml:"Engine"`
		EngineVersion   string `xml:"EngineVersion"`
		DBInstanceClass string `xml:"DBInstanceClass"`
	}
	options := []orderableOptionXML{
		{Engine: "mysql", EngineVersion: "8.0.35", DBInstanceClass: "db.t3.micro"},
		{Engine: "mysql", EngineVersion: "8.0.35", DBInstanceClass: "db.t3.medium"},
		{Engine: "mysql", EngineVersion: "8.0.35", DBInstanceClass: "db.r6g.large"},
		{Engine: "postgres", EngineVersion: "15.4", DBInstanceClass: "db.t3.medium"},
		{Engine: "aurora-mysql", EngineVersion: "8.0.mysql_aurora.3.07.0", DBInstanceClass: "db.r6g.large"},
	}

	type describeOrderableResponse struct {
		XMLName                    xml.Name             `xml:"DescribeOrderableDBInstanceOptionsResponse"`
		OrderableDBInstanceOptions []orderableOptionXML `xml:"DescribeOrderableDBInstanceOptionsResult>OrderableDBInstanceOptions>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeOrderableResponse{OrderableDBInstanceOptions: options})
}

func (p *Provider) handleDescribeAccountAttributes(form url.Values) (*plugin.Response, error) {
	type accountQuotaXML struct {
		AccountQuotaName string `xml:"AccountQuotaName"`
		Used             int    `xml:"Used"`
		Max              int    `xml:"Max"`
	}
	quotas := []accountQuotaXML{
		{AccountQuotaName: "DBInstances", Used: 0, Max: 40},
		{AccountQuotaName: "DBClusters", Used: 0, Max: 40},
		{AccountQuotaName: "DBParameterGroups", Used: 0, Max: 50},
	}

	type describeAccountAttributesResponse struct {
		XMLName       xml.Name          `xml:"DescribeAccountAttributesResponse"`
		AccountQuotas []accountQuotaXML `xml:"DescribeAccountAttributesResult>AccountQuotas>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeAccountAttributesResponse{AccountQuotas: quotas})
}

func (p *Provider) handleDescribeEvents(form url.Values) (*plugin.Response, error) {
	type describeEventsResponse struct {
		XMLName xml.Name   `xml:"DescribeEventsResponse"`
		Events  []struct{} `xml:"DescribeEventsResult>Events>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeEventsResponse{})
}

func (p *Provider) handleDescribeEventCategories(form url.Values) (*plugin.Response, error) {
	type eventCategoryMapXML struct {
		SourceType      string   `xml:"SourceType"`
		EventCategories []string `xml:"EventCategories>member"`
	}
	categories := []eventCategoryMapXML{
		{SourceType: "db-instance", EventCategories: []string{"availability", "backup", "creation", "deletion", "failover", "maintenance", "notification", "recovery", "restoration"}},
		{SourceType: "db-cluster", EventCategories: []string{"creation", "deletion", "failover", "maintenance", "notification"}},
		{SourceType: "db-snapshot", EventCategories: []string{"creation", "deletion", "restoration"}},
	}

	type describeEventCategoriesResponse struct {
		XMLName                xml.Name              `xml:"DescribeEventCategoriesResponse"`
		EventCategoriesMapList []eventCategoryMapXML `xml:"DescribeEventCategoriesResult>EventCategoriesMapList>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeEventCategoriesResponse{EventCategoriesMapList: categories})
}

func (p *Provider) handleDescribeSourceRegions(form url.Values) (*plugin.Response, error) {
	type sourceRegionXML struct {
		RegionName string `xml:"RegionName"`
		Status     string `xml:"Status"`
	}
	regions := []sourceRegionXML{
		{RegionName: "us-east-1", Status: "available"},
		{RegionName: "us-west-2", Status: "available"},
		{RegionName: "eu-west-1", Status: "available"},
	}

	type describeSourceRegionsResponse struct {
		XMLName       xml.Name          `xml:"DescribeSourceRegionsResponse"`
		SourceRegions []sourceRegionXML `xml:"DescribeSourceRegionsResult>SourceRegions>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeSourceRegionsResponse{SourceRegions: regions})
}

func (p *Provider) handleDescribeCertificates(form url.Values) (*plugin.Response, error) {
	type certificateXML struct {
		CertificateIdentifier string `xml:"CertificateIdentifier"`
		CertificateType       string `xml:"CertificateType"`
	}
	certs := []certificateXML{
		{CertificateIdentifier: "rds-ca-2019", CertificateType: "CA"},
		{CertificateIdentifier: "rds-ca-rsa2048-g1", CertificateType: "CA"},
	}

	type describeCertificatesResponse struct {
		XMLName      xml.Name         `xml:"DescribeCertificatesResponse"`
		Certificates []certificateXML `xml:"DescribeCertificatesResult>Certificates>member"`
	}
	return rdsXMLResponse(http.StatusOK, describeCertificatesResponse{Certificates: certs})
}

func (p *Provider) handleDescribePendingMaintenanceActions(form url.Values) (*plugin.Response, error) {
	type describePendingResponse struct {
		XMLName                   xml.Name   `xml:"DescribePendingMaintenanceActionsResponse"`
		PendingMaintenanceActions []struct{} `xml:"DescribePendingMaintenanceActionsResult>PendingMaintenanceActions>member"`
	}
	return rdsXMLResponse(http.StatusOK, describePendingResponse{})
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
	return rdsXMLResponse(http.StatusOK, resp)
}
