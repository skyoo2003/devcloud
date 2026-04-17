// SPDX-License-Identifier: Apache-2.0

package elasticache

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

// Provider implements the ElastiCacheV9 service (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "elasticache" }
func (p *Provider) ServiceName() string           { return "ElastiCacheV9" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init elasticache: %w", err)
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
		return ecError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return ecError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// CacheCluster
	case "CreateCacheCluster":
		return p.handleCreateCacheCluster(form)
	case "DescribeCacheClusters":
		return p.handleDescribeCacheClusters(form)
	case "ModifyCacheCluster":
		return p.handleModifyCacheCluster(form)
	case "DeleteCacheCluster":
		return p.handleDeleteCacheCluster(form)
	case "RebootCacheCluster":
		return p.handleRebootCacheCluster(form)

	// ReplicationGroup
	case "CreateReplicationGroup":
		return p.handleCreateReplicationGroup(form)
	case "DescribeReplicationGroups":
		return p.handleDescribeReplicationGroups(form)
	case "ModifyReplicationGroup":
		return p.handleModifyReplicationGroup(form)
	case "DeleteReplicationGroup":
		return p.handleDeleteReplicationGroup(form)
	case "IncreaseReplicaCount":
		return p.handleIncreaseReplicaCount(form)
	case "DecreaseReplicaCount":
		return p.handleDecreaseReplicaCount(form)
	case "ModifyReplicationGroupShardConfiguration":
		return p.handleModifyReplicationGroupShardConfiguration(form)
	case "TestFailover":
		return p.handleTestFailover(form)

	// CacheParameterGroup
	case "CreateCacheParameterGroup":
		return p.handleCreateCacheParameterGroup(form)
	case "DescribeCacheParameterGroups":
		return p.handleDescribeCacheParameterGroups(form)
	case "ModifyCacheParameterGroup":
		return p.handleModifyCacheParameterGroup(form)
	case "ResetCacheParameterGroup":
		return p.handleResetCacheParameterGroup(form)
	case "DeleteCacheParameterGroup":
		return p.handleDeleteCacheParameterGroup(form)
	case "DescribeCacheParameters":
		return p.handleDescribeCacheParameters(form)
	case "DescribeEngineDefaultParameters":
		return p.handleDescribeEngineDefaultParameters(form)

	// CacheSubnetGroup
	case "CreateCacheSubnetGroup":
		return p.handleCreateCacheSubnetGroup(form)
	case "DescribeCacheSubnetGroups":
		return p.handleDescribeCacheSubnetGroups(form)
	case "ModifyCacheSubnetGroup":
		return p.handleModifyCacheSubnetGroup(form)
	case "DeleteCacheSubnetGroup":
		return p.handleDeleteCacheSubnetGroup(form)

	// CacheSecurityGroup
	case "CreateCacheSecurityGroup":
		return p.handleCreateCacheSecurityGroup(form)
	case "DescribeCacheSecurityGroups":
		return p.handleDescribeCacheSecurityGroups(form)
	case "DeleteCacheSecurityGroup":
		return p.handleDeleteCacheSecurityGroup(form)
	case "AuthorizeCacheSecurityGroupIngress":
		return p.handleAuthorizeCacheSecurityGroupIngress(form)
	case "RevokeCacheSecurityGroupIngress":
		return p.handleRevokeCacheSecurityGroupIngress(form)

	// User
	case "CreateUser":
		return p.handleCreateUser(form)
	case "DescribeUsers":
		return p.handleDescribeUsers(form)
	case "ModifyUser":
		return p.handleModifyUser(form)
	case "DeleteUser":
		return p.handleDeleteUser(form)

	// UserGroup
	case "CreateUserGroup":
		return p.handleCreateUserGroup(form)
	case "DescribeUserGroups":
		return p.handleDescribeUserGroups(form)
	case "ModifyUserGroup":
		return p.handleModifyUserGroup(form)
	case "DeleteUserGroup":
		return p.handleDeleteUserGroup(form)

	// Snapshot
	case "CreateSnapshot":
		return p.handleCreateSnapshot(form)
	case "DescribeSnapshots":
		return p.handleDescribeSnapshots(form)
	case "CopySnapshot":
		return p.handleCopySnapshot(form)
	case "DeleteSnapshot":
		return p.handleDeleteSnapshot(form)

	// ServerlessCache
	case "CreateServerlessCache":
		return p.handleCreateServerlessCache(form)
	case "DescribeServerlessCaches":
		return p.handleDescribeServerlessCaches(form)
	case "ModifyServerlessCache":
		return p.handleModifyServerlessCache(form)
	case "DeleteServerlessCache":
		return p.handleDeleteServerlessCache(form)

	// Tags
	case "AddTagsToResource":
		return p.handleAddTagsToResource(form)
	case "RemoveTagsFromResource":
		return p.handleRemoveTagsFromResource(form)
	case "ListTagsForResource":
		return p.handleListTagsForResource(form)

	default:
		type genericResponse struct {
			XMLName xml.Name `xml:"GenericResponse"`
		}
		return ecXMLResponse(http.StatusOK, genericResponse{XMLName: xml.Name{Local: action + "Response"}})
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListCacheClusters(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, plugin.Resource{Type: "cache-cluster", ID: c.ID, Name: c.ID})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func ecError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func ecXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type endpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type cacheClusterXML struct {
	CacheClusterId            string      `xml:"CacheClusterId"`
	CacheClusterStatus        string      `xml:"CacheClusterStatus"`
	Engine                    string      `xml:"Engine"`
	EngineVersion             string      `xml:"EngineVersion"`
	CacheNodeType             string      `xml:"CacheNodeType"`
	NumCacheNodes             int         `xml:"NumCacheNodes"`
	ConfigurationEndpoint     endpointXML `xml:"ConfigurationEndpoint"`
	PreferredAvailabilityZone string      `xml:"PreferredAvailabilityZone"`
	CacheSubnetGroupName      string      `xml:"CacheSubnetGroupName"`
	CacheParameterGroupName   string      `xml:"CacheParameterGroup>CacheParameterGroupName"`
	ARN                       string      `xml:"ARN"`
	CacheClusterCreateTime    string      `xml:"CacheClusterCreateTime"`
}

func cacheClusterToXML(c *CacheCluster) cacheClusterXML {
	return cacheClusterXML{
		CacheClusterId:            c.ID,
		CacheClusterStatus:        c.Status,
		Engine:                    c.Engine,
		EngineVersion:             c.EngineVersion,
		CacheNodeType:             c.NodeType,
		NumCacheNodes:             c.NumNodes,
		ConfigurationEndpoint:     endpointXML{Address: c.Endpoint, Port: c.Port},
		PreferredAvailabilityZone: c.AZ,
		CacheSubnetGroupName:      c.SubnetGroup,
		CacheParameterGroupName:   c.ParamGroup,
		ARN:                       c.ARN,
		CacheClusterCreateTime:    c.CreatedAt.UTC().Format(time.RFC3339),
	}
}

type replicationGroupXML struct {
	ReplicationGroupId    string      `xml:"ReplicationGroupId"`
	Description           string      `xml:"Description"`
	Status                string      `xml:"Status"`
	ConfigurationEndpoint endpointXML `xml:"ConfigurationEndpoint"`
	ARN                   string      `xml:"ARN"`
}

func replicationGroupToXML(rg *ReplicationGroup) replicationGroupXML {
	return replicationGroupXML{
		ReplicationGroupId:    rg.ID,
		Description:           rg.Description,
		Status:                rg.Status,
		ConfigurationEndpoint: endpointXML{Address: rg.Endpoint, Port: rg.Port},
		ARN:                   rg.ARN,
	}
}

type paramGroupXML struct {
	CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
	CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
	Description               string `xml:"Description"`
	ARN                       string `xml:"ARN"`
}

func paramGroupToXML(pg *ParamGroup) paramGroupXML {
	return paramGroupXML{
		CacheParameterGroupName:   pg.Name,
		CacheParameterGroupFamily: pg.Family,
		Description:               pg.Description,
		ARN:                       pg.ARN,
	}
}

type subnetGroupXML struct {
	CacheSubnetGroupName        string `xml:"CacheSubnetGroupName"`
	CacheSubnetGroupDescription string `xml:"CacheSubnetGroupDescription"`
	VpcId                       string `xml:"VpcId"`
	ARN                         string `xml:"ARN"`
}

func subnetGroupToXML(sg *SubnetGroup) subnetGroupXML {
	return subnetGroupXML{
		CacheSubnetGroupName:        sg.Name,
		CacheSubnetGroupDescription: sg.Description,
		VpcId:                       sg.VpcID,
		ARN:                         sg.ARN,
	}
}

type userXML struct {
	UserId       string `xml:"UserId"`
	UserName     string `xml:"UserName"`
	Status       string `xml:"Status"`
	Engine       string `xml:"Engine"`
	AccessString string `xml:"AccessString"`
	ARN          string `xml:"ARN"`
}

func userToXML(u *User) userXML {
	return userXML{
		UserId:       u.ID,
		UserName:     u.UserName,
		Status:       u.Status,
		Engine:       u.Engine,
		AccessString: u.AccessString,
		ARN:          u.ARN,
	}
}

type userGroupXML struct {
	UserGroupId string `xml:"UserGroupId"`
	Status      string `xml:"Status"`
	Engine      string `xml:"Engine"`
	ARN         string `xml:"ARN"`
}

func userGroupToXML(ug *UserGroup) userGroupXML {
	return userGroupXML{
		UserGroupId: ug.ID,
		Status:      ug.Status,
		Engine:      ug.Engine,
		ARN:         ug.ARN,
	}
}

type snapshotXML struct {
	SnapshotName           string `xml:"SnapshotName"`
	CacheClusterId         string `xml:"CacheClusterId"`
	ReplicationGroupId     string `xml:"ReplicationGroupId"`
	SnapshotStatus         string `xml:"SnapshotStatus"`
	SnapshotSource         string `xml:"SnapshotSource"`
	ARN                    string `xml:"ARN"`
	NodeSnapshotsCreatedAt string `xml:"NodeSnapshots>NodeSnapshot>SnapshotCreateTime"`
}

func snapshotToXML(sn *Snapshot) snapshotXML {
	return snapshotXML{
		SnapshotName:           sn.Name,
		CacheClusterId:         sn.ClusterID,
		ReplicationGroupId:     sn.ReplGroupID,
		SnapshotStatus:         sn.Status,
		SnapshotSource:         sn.Source,
		ARN:                    sn.ARN,
		NodeSnapshotsCreatedAt: sn.CreatedAt.UTC().Format(time.RFC3339),
	}
}

type serverlessCacheXML struct {
	ServerlessCacheName string      `xml:"ServerlessCacheName"`
	Status              string      `xml:"Status"`
	Engine              string      `xml:"Engine"`
	Endpoint            endpointXML `xml:"Endpoint"`
	ARN                 string      `xml:"ARN"`
	CreateTime          string      `xml:"CreateTime"`
}

func serverlessCacheToXML(sc *ServerlessCache) serverlessCacheXML {
	return serverlessCacheXML{
		ServerlessCacheName: sc.Name,
		Status:              sc.Status,
		Engine:              sc.Engine,
		Endpoint:            endpointXML{Address: sc.Endpoint, Port: 6379},
		ARN:                 sc.ARN,
		CreateTime:          sc.CreatedAt.UTC().Format(time.RFC3339),
	}
}

// --- CacheCluster handlers ---

func (p *Provider) handleCreateCacheCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("CacheClusterId")
	if id == "" {
		return ecError("MissingParameter", "CacheClusterId is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "redis"
	}
	engineVersion := form.Get("EngineVersion")
	if engineVersion == "" {
		engineVersion = "7.1"
	}
	nodeType := form.Get("CacheNodeType")
	if nodeType == "" {
		nodeType = "cache.r6g.large"
	}
	az := form.Get("PreferredAvailabilityZone")
	if az == "" {
		az = "us-east-1a"
	}
	subnetGroup := form.Get("CacheSubnetGroupName")
	paramGroup := form.Get("CacheParameterGroupName")
	numNodes := 1
	port := 6379
	if engine == "memcached" {
		port = 11211
	}
	arn := shared.BuildARN("elasticache", "cluster", id)
	c, err := p.store.CreateCacheCluster(id, arn, engine, engineVersion, nodeType, az, subnetGroup, paramGroup, numNodes, port)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("CacheClusterAlreadyExists", "cluster already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName      xml.Name        `xml:"CreateCacheClusterResponse"`
		CacheCluster cacheClusterXML `xml:"CreateCacheClusterResult>CacheCluster"`
	}
	return ecXMLResponse(http.StatusOK, createResult{CacheCluster: cacheClusterToXML(c)})
}

func (p *Provider) handleDescribeCacheClusters(form url.Values) (*plugin.Response, error) {
	var ids []string
	requestedID := form.Get("CacheClusterId")
	if requestedID != "" {
		ids = append(ids, requestedID)
	}
	clusters, err := p.store.ListCacheClusters(ids)
	if err != nil {
		return nil, err
	}
	if requestedID != "" && len(clusters) == 0 {
		return ecError("CacheClusterNotFound", "cluster not found: "+requestedID, http.StatusNotFound), nil
	}
	items := make([]cacheClusterXML, 0, len(clusters))
	for i := range clusters {
		items = append(items, cacheClusterToXML(&clusters[i]))
	}

	type describeResult struct {
		XMLName       xml.Name          `xml:"DescribeCacheClustersResponse"`
		CacheClusters []cacheClusterXML `xml:"DescribeCacheClustersResult>CacheClusters>CacheCluster"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{CacheClusters: items})
}

func (p *Provider) handleModifyCacheCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("CacheClusterId")
	if id == "" {
		return ecError("MissingParameter", "CacheClusterId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCacheCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return ecError("CacheClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if ev := form.Get("EngineVersion"); ev != "" {
		c.EngineVersion = ev
	}

	type modifyResult struct {
		XMLName      xml.Name        `xml:"ModifyCacheClusterResponse"`
		CacheCluster cacheClusterXML `xml:"ModifyCacheClusterResult>CacheCluster"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{CacheCluster: cacheClusterToXML(c)})
}

func (p *Provider) handleDeleteCacheCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("CacheClusterId")
	if id == "" {
		return ecError("MissingParameter", "CacheClusterId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.DeleteCacheCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return ecError("CacheClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName      xml.Name        `xml:"DeleteCacheClusterResponse"`
		CacheCluster cacheClusterXML `xml:"DeleteCacheClusterResult>CacheCluster"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{CacheCluster: cacheClusterToXML(c)})
}

func (p *Provider) handleRebootCacheCluster(form url.Values) (*plugin.Response, error) {
	id := form.Get("CacheClusterId")
	if id == "" {
		return ecError("MissingParameter", "CacheClusterId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCacheCluster(id)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return ecError("CacheClusterNotFound", "cluster not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type rebootResult struct {
		XMLName      xml.Name        `xml:"RebootCacheClusterResponse"`
		CacheCluster cacheClusterXML `xml:"RebootCacheClusterResult>CacheCluster"`
	}
	return ecXMLResponse(http.StatusOK, rebootResult{CacheCluster: cacheClusterToXML(c)})
}

// --- ReplicationGroup handlers ---

func (p *Provider) handleCreateReplicationGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	description := form.Get("ReplicationGroupDescription")
	engine := form.Get("Engine")
	if engine == "" {
		engine = "redis"
	}
	engineVersion := form.Get("EngineVersion")
	if engineVersion == "" {
		engineVersion = "7.1"
	}
	nodeType := form.Get("CacheNodeType")
	if nodeType == "" {
		nodeType = "cache.r6g.large"
	}
	arn := shared.BuildARN("elasticache", "replicationgroup", id)
	rg, err := p.store.CreateReplicationGroup(id, arn, description, engine, engineVersion, nodeType, 1, 1, 6379)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("ReplicationGroupAlreadyExists", "replication group already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName          xml.Name            `xml:"CreateReplicationGroupResponse"`
		ReplicationGroup replicationGroupXML `xml:"CreateReplicationGroupResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, createResult{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleDescribeReplicationGroups(form url.Values) (*plugin.Response, error) {
	var ids []string
	if id := form.Get("ReplicationGroupId"); id != "" {
		ids = append(ids, id)
	}
	groups, err := p.store.ListReplicationGroups(ids)
	if err != nil {
		return nil, err
	}
	items := make([]replicationGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, replicationGroupToXML(&groups[i]))
	}

	type describeResult struct {
		XMLName           xml.Name              `xml:"DescribeReplicationGroupsResponse"`
		ReplicationGroups []replicationGroupXML `xml:"DescribeReplicationGroupsResult>ReplicationGroups>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{ReplicationGroups: items})
}

func (p *Provider) handleModifyReplicationGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if d := form.Get("ReplicationGroupDescription"); d != "" {
		rg.Description = d
	}

	type modifyResult struct {
		XMLName          xml.Name            `xml:"ModifyReplicationGroupResponse"`
		ReplicationGroup replicationGroupXML `xml:"ModifyReplicationGroupResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleDeleteReplicationGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.DeleteReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName          xml.Name            `xml:"DeleteReplicationGroupResponse"`
		ReplicationGroup replicationGroupXML `xml:"DeleteReplicationGroupResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleIncreaseReplicaCount(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName          xml.Name            `xml:"IncreaseReplicaCountResponse"`
		ReplicationGroup replicationGroupXML `xml:"IncreaseReplicaCountResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleDecreaseReplicaCount(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName          xml.Name            `xml:"DecreaseReplicaCountResponse"`
		ReplicationGroup replicationGroupXML `xml:"DecreaseReplicaCountResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleModifyReplicationGroupShardConfiguration(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName          xml.Name            `xml:"ModifyReplicationGroupShardConfigurationResponse"`
		ReplicationGroup replicationGroupXML `xml:"ModifyReplicationGroupShardConfigurationResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *Provider) handleTestFailover(form url.Values) (*plugin.Response, error) {
	id := form.Get("ReplicationGroupId")
	if id == "" {
		return ecError("MissingParameter", "ReplicationGroupId is required", http.StatusBadRequest), nil
	}
	rg, err := p.store.GetReplicationGroup(id)
	if err != nil {
		if errors.Is(err, errReplGroupNotFound) {
			return ecError("ReplicationGroupNotFoundFault", "replication group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName          xml.Name            `xml:"TestFailoverResponse"`
		ReplicationGroup replicationGroupXML `xml:"TestFailoverResult>ReplicationGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{ReplicationGroup: replicationGroupToXML(rg)})
}

// --- CacheParameterGroup handlers ---

func (p *Provider) handleCreateCacheParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheParameterGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheParameterGroupName is required", http.StatusBadRequest), nil
	}
	family := form.Get("CacheParameterGroupFamily")
	if family == "" {
		family = "redis7"
	}
	description := form.Get("Description")
	arn := shared.BuildARN("elasticache", "parametergroup", name)
	pg, err := p.store.CreateParamGroup(name, arn, family, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("CacheParameterGroupAlreadyExists", "parameter group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName             xml.Name      `xml:"CreateCacheParameterGroupResponse"`
		CacheParameterGroup paramGroupXML `xml:"CreateCacheParameterGroupResult>CacheParameterGroup"`
	}
	return ecXMLResponse(http.StatusOK, createResult{CacheParameterGroup: paramGroupToXML(pg)})
}

func (p *Provider) handleDescribeCacheParameterGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("CacheParameterGroupName"); name != "" {
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

	type describeResult struct {
		XMLName              xml.Name        `xml:"DescribeCacheParameterGroupsResponse"`
		CacheParameterGroups []paramGroupXML `xml:"DescribeCacheParameterGroupsResult>CacheParameterGroups>CacheParameterGroup"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{CacheParameterGroups: items})
}

func (p *Provider) handleModifyCacheParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheParameterGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheParameterGroupName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.GetParamGroup(name)
	if err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return ecError("CacheParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName                 xml.Name `xml:"ModifyCacheParameterGroupResponse"`
		CacheParameterGroupName string   `xml:"ModifyCacheParameterGroupResult>CacheParameterGroupName"`
	}
	return ecXMLResponse(http.StatusOK, result{CacheParameterGroupName: name})
}

func (p *Provider) handleResetCacheParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheParameterGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheParameterGroupName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.GetParamGroup(name)
	if err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return ecError("CacheParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName                 xml.Name `xml:"ResetCacheParameterGroupResponse"`
		CacheParameterGroupName string   `xml:"ResetCacheParameterGroupResult>CacheParameterGroupName"`
	}
	return ecXMLResponse(http.StatusOK, result{CacheParameterGroupName: name})
}

func (p *Provider) handleDeleteCacheParameterGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheParameterGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheParameterGroupName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteParamGroup(name)
	if err != nil {
		if errors.Is(err, errParamGroupNotFound) {
			return ecError("CacheParameterGroupNotFound", "parameter group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"DeleteCacheParameterGroupResponse"`
	}
	return ecXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDescribeCacheParameters(form url.Values) (*plugin.Response, error) {
	type result struct {
		XMLName    xml.Name `xml:"DescribeCacheParametersResponse"`
		Parameters []string `xml:"DescribeCacheParametersResult>Parameters"`
	}
	return ecXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleDescribeEngineDefaultParameters(form url.Values) (*plugin.Response, error) {
	cacheParamGroupFamily := form.Get("CacheParameterGroupFamily")
	type result struct {
		XMLName               xml.Name `xml:"DescribeEngineDefaultParametersResponse"`
		CacheParamGroupFamily string   `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>CacheParameterGroupFamily"`
	}
	return ecXMLResponse(http.StatusOK, result{CacheParamGroupFamily: cacheParamGroupFamily})
}

// --- CacheSubnetGroup handlers ---

func (p *Provider) handleCreateCacheSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSubnetGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSubnetGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("CacheSubnetGroupDescription")
	arn := shared.BuildARN("elasticache", "subnetgroup", name)
	sg, err := p.store.CreateSubnetGroup(name, arn, description, "", "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("CacheSubnetGroupAlreadyExists", "subnet group already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName          xml.Name       `xml:"CreateCacheSubnetGroupResponse"`
		CacheSubnetGroup subnetGroupXML `xml:"CreateCacheSubnetGroupResult>CacheSubnetGroup"`
	}
	return ecXMLResponse(http.StatusOK, createResult{CacheSubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDescribeCacheSubnetGroups(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("CacheSubnetGroupName"); name != "" {
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

	type describeResult struct {
		XMLName           xml.Name         `xml:"DescribeCacheSubnetGroupsResponse"`
		CacheSubnetGroups []subnetGroupXML `xml:"DescribeCacheSubnetGroupsResult>CacheSubnetGroups>CacheSubnetGroup"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{CacheSubnetGroups: items})
}

func (p *Provider) handleModifyCacheSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSubnetGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSubnetGroupName is required", http.StatusBadRequest), nil
	}
	sg, err := p.store.GetSubnetGroup(name)
	if err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return ecError("CacheSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}
	if d := form.Get("CacheSubnetGroupDescription"); d != "" {
		sg.Description = d
		_ = p.store.UpdateSubnetGroup(name, d, sg.VpcID, sg.Subnets)
	}

	type modifyResult struct {
		XMLName          xml.Name       `xml:"ModifyCacheSubnetGroupResponse"`
		CacheSubnetGroup subnetGroupXML `xml:"ModifyCacheSubnetGroupResult>CacheSubnetGroup"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{CacheSubnetGroup: subnetGroupToXML(sg)})
}

func (p *Provider) handleDeleteCacheSubnetGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSubnetGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSubnetGroupName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteSubnetGroup(name)
	if err != nil {
		if errors.Is(err, errSubnetGroupNotFound) {
			return ecError("CacheSubnetGroupNotFoundFault", "subnet group not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type result struct {
		XMLName xml.Name `xml:"DeleteCacheSubnetGroupResponse"`
	}
	return ecXMLResponse(http.StatusOK, result{})
}

// --- CacheSecurityGroup handlers (in-memory stubs) ---

var cacheSecurityGroups = map[string]struct{ Name, Description, ARN string }{}

func (p *Provider) handleCreateCacheSecurityGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSecurityGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSecurityGroupName is required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	arn := shared.BuildARN("elasticache", "securitygroup", name)
	if _, exists := cacheSecurityGroups[name]; exists {
		return ecError("CacheSecurityGroupAlreadyExists", "security group already exists: "+name, http.StatusConflict), nil
	}
	cacheSecurityGroups[name] = struct{ Name, Description, ARN string }{Name: name, Description: description, ARN: arn}

	type secGrpXML struct {
		CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
		Description            string `xml:"Description"`
		ARN                    string `xml:"ARN"`
	}
	type createResult struct {
		XMLName            xml.Name  `xml:"CreateCacheSecurityGroupResponse"`
		CacheSecurityGroup secGrpXML `xml:"CreateCacheSecurityGroupResult>CacheSecurityGroup"`
	}
	return ecXMLResponse(http.StatusOK, createResult{CacheSecurityGroup: secGrpXML{CacheSecurityGroupName: name, Description: description, ARN: arn}})
}

func (p *Provider) handleDescribeCacheSecurityGroups(form url.Values) (*plugin.Response, error) {
	type secGrpXML struct {
		CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
		Description            string `xml:"Description"`
		ARN                    string `xml:"ARN"`
	}
	var items []secGrpXML
	filterName := form.Get("CacheSecurityGroupName")
	for _, sg := range cacheSecurityGroups {
		if filterName == "" || sg.Name == filterName {
			items = append(items, secGrpXML{CacheSecurityGroupName: sg.Name, Description: sg.Description, ARN: sg.ARN})
		}
	}

	type describeResult struct {
		XMLName             xml.Name    `xml:"DescribeCacheSecurityGroupsResponse"`
		CacheSecurityGroups []secGrpXML `xml:"DescribeCacheSecurityGroupsResult>CacheSecurityGroups>CacheSecurityGroup"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{CacheSecurityGroups: items})
}

func (p *Provider) handleDeleteCacheSecurityGroup(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSecurityGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSecurityGroupName is required", http.StatusBadRequest), nil
	}
	if _, exists := cacheSecurityGroups[name]; !exists {
		return ecError("CacheSecurityGroupNotFound", "security group not found: "+name, http.StatusNotFound), nil
	}
	delete(cacheSecurityGroups, name)

	type result struct {
		XMLName xml.Name `xml:"DeleteCacheSecurityGroupResponse"`
	}
	return ecXMLResponse(http.StatusOK, result{})
}

func (p *Provider) handleAuthorizeCacheSecurityGroupIngress(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSecurityGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSecurityGroupName is required", http.StatusBadRequest), nil
	}
	sg, exists := cacheSecurityGroups[name]
	if !exists {
		return ecError("CacheSecurityGroupNotFound", "security group not found: "+name, http.StatusNotFound), nil
	}

	type secGrpXML struct {
		CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
		Description            string `xml:"Description"`
	}
	type result struct {
		XMLName            xml.Name  `xml:"AuthorizeCacheSecurityGroupIngressResponse"`
		CacheSecurityGroup secGrpXML `xml:"AuthorizeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{CacheSecurityGroup: secGrpXML{CacheSecurityGroupName: sg.Name, Description: sg.Description}})
}

func (p *Provider) handleRevokeCacheSecurityGroupIngress(form url.Values) (*plugin.Response, error) {
	name := form.Get("CacheSecurityGroupName")
	if name == "" {
		return ecError("MissingParameter", "CacheSecurityGroupName is required", http.StatusBadRequest), nil
	}
	sg, exists := cacheSecurityGroups[name]
	if !exists {
		return ecError("CacheSecurityGroupNotFound", "security group not found: "+name, http.StatusNotFound), nil
	}

	type secGrpXML struct {
		CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
		Description            string `xml:"Description"`
	}
	type result struct {
		XMLName            xml.Name  `xml:"RevokeCacheSecurityGroupIngressResponse"`
		CacheSecurityGroup secGrpXML `xml:"RevokeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}
	return ecXMLResponse(http.StatusOK, result{CacheSecurityGroup: secGrpXML{CacheSecurityGroupName: sg.Name, Description: sg.Description}})
}

// --- User handlers ---

func (p *Provider) handleCreateUser(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserId")
	if id == "" {
		return ecError("MissingParameter", "UserId is required", http.StatusBadRequest), nil
	}
	userName := form.Get("UserName")
	if userName == "" {
		userName = id
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "redis"
	}
	accessString := form.Get("AccessString")
	if accessString == "" {
		accessString = "on ~* +@all"
	}
	arn := shared.BuildARN("elasticache", "user", id)
	u, err := p.store.CreateUser(id, arn, userName, engine, accessString)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("UserAlreadyExists", "user already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName xml.Name `xml:"CreateUserResponse"`
		User    userXML  `xml:"CreateUserResult>User"`
	}
	return ecXMLResponse(http.StatusOK, createResult{User: userToXML(u)})
}

func (p *Provider) handleDescribeUsers(form url.Values) (*plugin.Response, error) {
	var ids []string
	if id := form.Get("UserId"); id != "" {
		ids = append(ids, id)
	}
	users, err := p.store.ListUsers(ids)
	if err != nil {
		return nil, err
	}
	items := make([]userXML, 0, len(users))
	for i := range users {
		items = append(items, userToXML(&users[i]))
	}

	type describeResult struct {
		XMLName xml.Name  `xml:"DescribeUsersResponse"`
		Users   []userXML `xml:"DescribeUsersResult>Users>member"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{Users: items})
}

func (p *Provider) handleModifyUser(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserId")
	if id == "" {
		return ecError("MissingParameter", "UserId is required", http.StatusBadRequest), nil
	}
	u, err := p.store.GetUser(id)
	if err != nil {
		if errors.Is(err, errUserNotFound) {
			return ecError("UserNotFound", "user not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}
	if as := form.Get("AccessString"); as != "" {
		u.AccessString = as
		_ = p.store.UpdateUserAccessString(id, as)
	}

	type modifyResult struct {
		XMLName xml.Name `xml:"ModifyUserResponse"`
		User    userXML  `xml:"ModifyUserResult>User"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{User: userToXML(u)})
}

func (p *Provider) handleDeleteUser(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserId")
	if id == "" {
		return ecError("MissingParameter", "UserId is required", http.StatusBadRequest), nil
	}
	u, err := p.store.DeleteUser(id)
	if err != nil {
		if errors.Is(err, errUserNotFound) {
			return ecError("UserNotFound", "user not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName xml.Name `xml:"DeleteUserResponse"`
		User    userXML  `xml:"DeleteUserResult>User"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{User: userToXML(u)})
}

// --- UserGroup handlers ---

func (p *Provider) handleCreateUserGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserGroupId")
	if id == "" {
		return ecError("MissingParameter", "UserGroupId is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "redis"
	}
	arn := shared.BuildARN("elasticache", "usergroup", id)
	ug, err := p.store.CreateUserGroup(id, arn, engine, "[]")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("UserGroupAlreadyExists", "user group already exists: "+id, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName   xml.Name     `xml:"CreateUserGroupResponse"`
		UserGroup userGroupXML `xml:"CreateUserGroupResult>UserGroup"`
	}
	return ecXMLResponse(http.StatusOK, createResult{UserGroup: userGroupToXML(ug)})
}

func (p *Provider) handleDescribeUserGroups(form url.Values) (*plugin.Response, error) {
	var ids []string
	if id := form.Get("UserGroupId"); id != "" {
		ids = append(ids, id)
	}
	groups, err := p.store.ListUserGroups(ids)
	if err != nil {
		return nil, err
	}
	items := make([]userGroupXML, 0, len(groups))
	for i := range groups {
		items = append(items, userGroupToXML(&groups[i]))
	}

	type describeResult struct {
		XMLName    xml.Name       `xml:"DescribeUserGroupsResponse"`
		UserGroups []userGroupXML `xml:"DescribeUserGroupsResult>UserGroups>member"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{UserGroups: items})
}

func (p *Provider) handleModifyUserGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserGroupId")
	if id == "" {
		return ecError("MissingParameter", "UserGroupId is required", http.StatusBadRequest), nil
	}
	ug, err := p.store.GetUserGroup(id)
	if err != nil {
		if errors.Is(err, errUserGroupNotFound) {
			return ecError("UserGroupNotFound", "user group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyResult struct {
		XMLName   xml.Name     `xml:"ModifyUserGroupResponse"`
		UserGroup userGroupXML `xml:"ModifyUserGroupResult>UserGroup"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{UserGroup: userGroupToXML(ug)})
}

func (p *Provider) handleDeleteUserGroup(form url.Values) (*plugin.Response, error) {
	id := form.Get("UserGroupId")
	if id == "" {
		return ecError("MissingParameter", "UserGroupId is required", http.StatusBadRequest), nil
	}
	ug, err := p.store.DeleteUserGroup(id)
	if err != nil {
		if errors.Is(err, errUserGroupNotFound) {
			return ecError("UserGroupNotFound", "user group not found: "+id, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName   xml.Name     `xml:"DeleteUserGroupResponse"`
		UserGroup userGroupXML `xml:"DeleteUserGroupResult>UserGroup"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{UserGroup: userGroupToXML(ug)})
}

// --- Snapshot handlers ---

func (p *Provider) handleCreateSnapshot(form url.Values) (*plugin.Response, error) {
	name := form.Get("SnapshotName")
	if name == "" {
		return ecError("MissingParameter", "SnapshotName is required", http.StatusBadRequest), nil
	}
	clusterID := form.Get("CacheClusterId")
	replGroupID := form.Get("ReplicationGroupId")
	arn := shared.BuildARN("elasticache", "snapshot", name)
	sn, err := p.store.CreateSnapshot(name, arn, clusterID, replGroupID, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("SnapshotAlreadyExistsFault", "snapshot already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName  xml.Name    `xml:"CreateSnapshotResponse"`
		Snapshot snapshotXML `xml:"CreateSnapshotResult>Snapshot"`
	}
	return ecXMLResponse(http.StatusOK, createResult{Snapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDescribeSnapshots(form url.Values) (*plugin.Response, error) {
	clusterID := form.Get("CacheClusterId")
	replGroupID := form.Get("ReplicationGroupId")
	// If SnapshotName is set, filter by name
	if name := form.Get("SnapshotName"); name != "" {
		sn, err := p.store.GetSnapshot(name)
		if err != nil {
			if errors.Is(err, errSnapshotNotFound) {
				return ecError("SnapshotNotFoundFault", "snapshot not found: "+name, http.StatusNotFound), nil
			}
			return nil, err
		}
		type describeResult struct {
			XMLName   xml.Name      `xml:"DescribeSnapshotsResponse"`
			Snapshots []snapshotXML `xml:"DescribeSnapshotsResult>SnapShotList>Snapshot"`
		}
		return ecXMLResponse(http.StatusOK, describeResult{Snapshots: []snapshotXML{snapshotToXML(sn)}})
	}
	snaps, err := p.store.ListSnapshots(clusterID, replGroupID)
	if err != nil {
		return nil, err
	}
	items := make([]snapshotXML, 0, len(snaps))
	for i := range snaps {
		items = append(items, snapshotToXML(&snaps[i]))
	}

	type describeResult struct {
		XMLName   xml.Name      `xml:"DescribeSnapshotsResponse"`
		Snapshots []snapshotXML `xml:"DescribeSnapshotsResult>SnapShotList>Snapshot"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{Snapshots: items})
}

func (p *Provider) handleCopySnapshot(form url.Values) (*plugin.Response, error) {
	srcName := form.Get("SourceSnapshotName")
	targetName := form.Get("TargetSnapshotName")
	if srcName == "" || targetName == "" {
		return ecError("MissingParameter", "SourceSnapshotName and TargetSnapshotName are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(srcName)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return ecError("SnapshotNotFoundFault", "snapshot not found: "+srcName, http.StatusNotFound), nil
		}
		return nil, err
	}
	arn := shared.BuildARN("elasticache", "snapshot", targetName)
	sn, err := p.store.CreateSnapshot(targetName, arn, src.ClusterID, src.ReplGroupID, "manual")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("SnapshotAlreadyExistsFault", "snapshot already exists: "+targetName, http.StatusConflict), nil
		}
		return nil, err
	}

	type copyResult struct {
		XMLName  xml.Name    `xml:"CopySnapshotResponse"`
		Snapshot snapshotXML `xml:"CopySnapshotResult>Snapshot"`
	}
	return ecXMLResponse(http.StatusOK, copyResult{Snapshot: snapshotToXML(sn)})
}

func (p *Provider) handleDeleteSnapshot(form url.Values) (*plugin.Response, error) {
	name := form.Get("SnapshotName")
	if name == "" {
		return ecError("MissingParameter", "SnapshotName is required", http.StatusBadRequest), nil
	}
	sn, err := p.store.DeleteSnapshot(name)
	if err != nil {
		if errors.Is(err, errSnapshotNotFound) {
			return ecError("SnapshotNotFoundFault", "snapshot not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName  xml.Name    `xml:"DeleteSnapshotResponse"`
		Snapshot snapshotXML `xml:"DeleteSnapshotResult>Snapshot"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{Snapshot: snapshotToXML(sn)})
}

// --- ServerlessCache handlers ---

func (p *Provider) handleCreateServerlessCache(form url.Values) (*plugin.Response, error) {
	name := form.Get("ServerlessCacheName")
	if name == "" {
		return ecError("MissingParameter", "ServerlessCacheName is required", http.StatusBadRequest), nil
	}
	engine := form.Get("Engine")
	if engine == "" {
		engine = "redis"
	}
	arn := shared.BuildARN("elasticache", "serverlesscache", name)
	sc, err := p.store.CreateServerlessCache(name, arn, engine)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ecError("ServerlessCacheAlreadyExists", "serverless cache already exists: "+name, http.StatusConflict), nil
		}
		return nil, err
	}

	type createResult struct {
		XMLName         xml.Name           `xml:"CreateServerlessCacheResponse"`
		ServerlessCache serverlessCacheXML `xml:"CreateServerlessCacheResult>ServerlessCache"`
	}
	return ecXMLResponse(http.StatusOK, createResult{ServerlessCache: serverlessCacheToXML(sc)})
}

func (p *Provider) handleDescribeServerlessCaches(form url.Values) (*plugin.Response, error) {
	var names []string
	if name := form.Get("ServerlessCacheName"); name != "" {
		names = append(names, name)
	}
	caches, err := p.store.ListServerlessCaches(names)
	if err != nil {
		return nil, err
	}
	items := make([]serverlessCacheXML, 0, len(caches))
	for i := range caches {
		items = append(items, serverlessCacheToXML(&caches[i]))
	}

	type describeResult struct {
		XMLName          xml.Name             `xml:"DescribeServerlessCachesResponse"`
		ServerlessCaches []serverlessCacheXML `xml:"DescribeServerlessCachesResult>ServerlessCaches>member"`
	}
	return ecXMLResponse(http.StatusOK, describeResult{ServerlessCaches: items})
}

func (p *Provider) handleModifyServerlessCache(form url.Values) (*plugin.Response, error) {
	name := form.Get("ServerlessCacheName")
	if name == "" {
		return ecError("MissingParameter", "ServerlessCacheName is required", http.StatusBadRequest), nil
	}
	sc, err := p.store.GetServerlessCache(name)
	if err != nil {
		if errors.Is(err, errServerlessNotFound) {
			return ecError("ServerlessCacheNotFoundFault", "serverless cache not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type modifyResult struct {
		XMLName         xml.Name           `xml:"ModifyServerlessCacheResponse"`
		ServerlessCache serverlessCacheXML `xml:"ModifyServerlessCacheResult>ServerlessCache"`
	}
	return ecXMLResponse(http.StatusOK, modifyResult{ServerlessCache: serverlessCacheToXML(sc)})
}

func (p *Provider) handleDeleteServerlessCache(form url.Values) (*plugin.Response, error) {
	name := form.Get("ServerlessCacheName")
	if name == "" {
		return ecError("MissingParameter", "ServerlessCacheName is required", http.StatusBadRequest), nil
	}
	sc, err := p.store.DeleteServerlessCache(name)
	if err != nil {
		if errors.Is(err, errServerlessNotFound) {
			return ecError("ServerlessCacheNotFoundFault", "serverless cache not found: "+name, http.StatusNotFound), nil
		}
		return nil, err
	}

	type deleteResult struct {
		XMLName         xml.Name           `xml:"DeleteServerlessCacheResponse"`
		ServerlessCache serverlessCacheXML `xml:"DeleteServerlessCacheResult>ServerlessCache"`
	}
	return ecXMLResponse(http.StatusOK, deleteResult{ServerlessCache: serverlessCacheToXML(sc)})
}

// --- Tags handlers ---

func (p *Provider) handleAddTagsToResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return ecError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tags := parseFormTags(form)
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, err
	}

	storedTags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var tagList []tagXML
	for k, v := range storedTags {
		tagList = append(tagList, tagXML{Key: k, Value: v})
	}

	type result struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		TagList []tagXML `xml:"AddTagsToResourceResult>TagList>Tag"`
	}
	return ecXMLResponse(http.StatusOK, result{TagList: tagList})
}

func (p *Provider) handleRemoveTagsFromResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return ecError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
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

	storedTags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var tagList []tagXML
	for k, v := range storedTags {
		tagList = append(tagList, tagXML{Key: k, Value: v})
	}

	type result struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
		TagList []tagXML `xml:"RemoveTagsFromResourceResult>TagList>Tag"`
	}
	return ecXMLResponse(http.StatusOK, result{TagList: tagList})
}

func (p *Provider) handleListTagsForResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceName")
	if arn == "" {
		return ecError("MissingParameter", "ResourceName is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	var tagList []tagXML
	for k, v := range tags {
		tagList = append(tagList, tagXML{Key: k, Value: v})
	}

	type result struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		TagList []tagXML `xml:"ListTagsForResourceResult>TagList>Tag"`
	}
	return ecXMLResponse(http.StatusOK, result{TagList: tagList})
}

// parseFormTags parses Tags.member.N.Key / Tags.member.N.Value from form values.
func parseFormTags(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			break
		}
		v := form.Get(fmt.Sprintf("Tags.member.%d.Value", i))
		tags[k] = v
	}
	return tags
}
