// SPDX-License-Identifier: Apache-2.0

// internal/services/memorydb/provider.go
package memorydb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "memorydb" }
func (p *Provider) ServiceName() string           { return "MemoryDB" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "memorydb"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// Cluster
	case "CreateCluster":
		return p.createCluster(params)
	case "DescribeClusters":
		return p.describeClusters(params)
	case "UpdateCluster":
		return p.updateCluster(params)
	case "DeleteCluster":
		return p.deleteCluster(params)
	case "BatchUpdateCluster":
		return p.batchUpdateCluster(params)
	case "FailoverShard":
		return p.failoverShard(params)

	// ParameterGroup
	case "CreateParameterGroup":
		return p.createParameterGroup(params)
	case "DescribeParameterGroups":
		return p.describeParameterGroups(params)
	case "UpdateParameterGroup":
		return p.updateParameterGroup(params)
	case "DeleteParameterGroup":
		return p.deleteParameterGroup(params)
	case "DescribeParameters":
		return p.describeParameters(params)
	case "ResetParameterGroup":
		return p.resetParameterGroup(params)

	// SubnetGroup
	case "CreateSubnetGroup":
		return p.createSubnetGroup(params)
	case "DescribeSubnetGroups":
		return p.describeSubnetGroups(params)
	case "UpdateSubnetGroup":
		return p.updateSubnetGroup(params)
	case "DeleteSubnetGroup":
		return p.deleteSubnetGroup(params)

	// ACL
	case "CreateACL":
		return p.createACL(params)
	case "DescribeACLs":
		return p.describeACLs(params)
	case "UpdateACL":
		return p.updateACL(params)
	case "DeleteACL":
		return p.deleteACL(params)

	// User
	case "CreateUser":
		return p.createUser(params)
	case "DescribeUsers":
		return p.describeUsers(params)
	case "UpdateUser":
		return p.updateUser(params)
	case "DeleteUser":
		return p.deleteUser(params)

	// Snapshot
	case "CreateSnapshot":
		return p.createSnapshot(params)
	case "DescribeSnapshots":
		return p.describeSnapshots(params)
	case "CopySnapshot":
		return p.copySnapshot(params)
	case "DeleteSnapshot":
		return p.deleteSnapshot(params)

	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTags":
		return p.listTags(params)

	// Static/empty responses
	case "DescribeEngineVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EngineVersions": []any{}})
	case "DescribeEvents":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Events": []any{}})
	case "DescribeServiceUpdates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceUpdates": []any{}})
	case "DescribeReservedNodes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedNodes": []any{}})
	case "DescribeReservedNodesOfferings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedNodesOfferings": []any{}})
	case "ListAllowedNodeTypeUpdates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ScaleUpNodeTypes": []any{}, "ScaleDownNodeTypes": []any{}})
	case "ListAllowedMultiRegionClusterUpdates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ScaleUpNodeTypes": []any{}, "ScaleDownNodeTypes": []any{}})
	case "CreateMultiRegionCluster":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionCluster": map[string]any{}})
	case "DescribeMultiRegionClusters":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionClusters": []any{}})
	case "UpdateMultiRegionCluster":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionCluster": map[string]any{}})
	case "DeleteMultiRegionCluster":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionCluster": map[string]any{}})
	case "DescribeMultiRegionParameterGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MultiRegionParameterGroups": []any{}})
	case "DescribeMultiRegionParameters":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Parameters": []any{}})
	case "PurchaseReservedNodesOffering":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedNode": map[string]any{}})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		res = append(res, plugin.Resource{Type: "cluster", ID: c.Name, Name: c.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Cluster handlers ----

func (p *Provider) createCluster(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	nodeType, _ := params["NodeType"].(string)
	if nodeType == "" {
		nodeType = "db.r6g.large"
	}
	aclName, _ := params["ACLName"].(string)
	if aclName == "" {
		aclName = "open-access"
	}
	engineVersion, _ := params["EngineVersion"].(string)
	if engineVersion == "" {
		engineVersion = "7.1"
	}
	subnetGroup, _ := params["SubnetGroupName"].(string)
	numShards := 1
	if v, ok := params["NumShards"].(float64); ok {
		numShards = int(v)
	}
	numReplicas := 1
	if v, ok := params["NumReplicasPerShard"].(float64); ok {
		numReplicas = int(v)
	}
	arn := shared.BuildARN("memorydb", "cluster", name)
	endpoint := fmt.Sprintf("clustercfg.%s.%s.memorydb.us-east-1.amazonaws.com", name, shared.GenerateID("", 6))
	c := &Cluster{
		Name:          name,
		ARN:           arn,
		Status:        "available",
		NodeType:      nodeType,
		NumShards:     numShards,
		NumReplicas:   numReplicas,
		EngineVersion: engineVersion,
		SubnetGroup:   subnetGroup,
		ACLName:       aclName,
		Endpoint:      endpoint,
		Port:          6379,
		CreatedAt:     nowUnix(),
	}
	if err := p.store.CreateCluster(c); err != nil {
		if isUnique(err) {
			return shared.JSONError("ClusterAlreadyExistsFault", "cluster already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Cluster": clusterToMap(c)})
}

func (p *Provider) describeClusters(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name != "" {
		c, err := p.store.GetCluster(name)
		if err != nil {
			return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"Clusters": []any{clusterToMap(c)}})
	}
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(clusters))
	for i := range clusters {
		list = append(list, clusterToMap(&clusters[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Clusters": list})
}

func (p *Provider) updateCluster(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if v, ok := params["NodeType"].(string); ok && v != "" {
		fields["node_type"] = v
	}
	if v, ok := params["EngineVersion"].(string); ok && v != "" {
		fields["engine_version"] = v
	}
	if v, ok := params["ACLName"].(string); ok && v != "" {
		fields["acl_name"] = v
	}
	if v, ok := params["ReplicaConfiguration"].(map[string]any); ok {
		if cnt, ok := v["ReplicaCount"].(float64); ok {
			fields["num_replicas"] = int(cnt)
		}
	}
	if v, ok := params["ShardConfiguration"].(map[string]any); ok {
		if cnt, ok := v["ShardCount"].(float64); ok {
			fields["num_shards"] = int(cnt)
		}
	}
	if err := p.store.UpdateCluster(name, fields); err != nil {
		return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
	}
	c, _ := p.store.GetCluster(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"Cluster": clusterToMap(c)})
}

func (p *Provider) deleteCluster(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(c.ARN)
	if err := p.store.DeleteCluster(name); err != nil {
		return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Cluster": clusterToMap(c)})
}

func (p *Provider) batchUpdateCluster(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["ClusterNames"].([]any)
	processed := make([]any, 0, len(rawNames))
	for _, n := range rawNames {
		name, _ := n.(string)
		if name == "" {
			continue
		}
		c, err := p.store.GetCluster(name)
		if err != nil {
			continue
		}
		processed = append(processed, clusterToMap(c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ProcessedClusters":   processed,
		"UnprocessedClusters": []any{},
	})
}

func (p *Provider) failoverShard(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCluster(name)
	if err != nil {
		return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
	}
	p.store.UpdateCluster(name, map[string]any{"status": "available"})
	return shared.JSONResponse(http.StatusOK, map[string]any{"Cluster": clusterToMap(c)})
}

// ---- ParameterGroup handlers ----

func (p *Provider) createParameterGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ParameterGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	family, _ := params["Family"].(string)
	if family == "" {
		family = "memorydb_redis7"
	}
	description, _ := params["Description"].(string)
	arn := shared.BuildARN("memorydb", "parametergroup", name)
	pg := &ParameterGroup{Name: name, ARN: arn, Family: family, Description: description}
	if err := p.store.CreateParameterGroup(pg); err != nil {
		if isUnique(err) {
			return shared.JSONError("ParameterGroupAlreadyExistsFault", "parameter group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroup": pgToMap(pg)})
}

func (p *Provider) describeParameterGroups(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ParameterGroupName"].(string)
	if name != "" {
		pg, err := p.store.GetParameterGroup(name)
		if err != nil {
			return shared.JSONError("ParameterGroupNotFoundFault", "parameter group not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroups": []any{pgToMap(pg)}})
	}
	pgs, err := p.store.ListParameterGroups()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(pgs))
	for i := range pgs {
		list = append(list, pgToMap(&pgs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroups": list})
}

func (p *Provider) updateParameterGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ParameterGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	if err := p.store.UpdateParameterGroup(name, description); err != nil {
		return shared.JSONError("ParameterGroupNotFoundFault", "parameter group not found", http.StatusBadRequest), nil
	}
	pg, _ := p.store.GetParameterGroup(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroup": pgToMap(pg)})
}

func (p *Provider) deleteParameterGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ParameterGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetParameterGroup(name)
	if err != nil {
		return shared.JSONError("ParameterGroupNotFoundFault", "parameter group not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(pg.ARN)
	if err := p.store.DeleteParameterGroup(name); err != nil {
		return shared.JSONError("ParameterGroupNotFoundFault", "parameter group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroup": pgToMap(pg)})
}

func (p *Provider) describeParameters(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"Parameters": []any{}})
}

func (p *Provider) resetParameterGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ParameterGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ParameterGroupName is required", http.StatusBadRequest), nil
	}
	pg, err := p.store.GetParameterGroup(name)
	if err != nil {
		return shared.JSONError("ParameterGroupNotFoundFault", "parameter group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ParameterGroup": pgToMap(pg)})
}

// ---- SubnetGroup handlers ----

func (p *Provider) createSubnetGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubnetGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "SubnetGroupName is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	vpcID, _ := params["VpcId"].(string)
	rawSubnets, _ := params["SubnetIds"].([]any)
	subnets := marshalStringSlice(toStringSlice(rawSubnets))
	arn := shared.BuildARN("memorydb", "subnetgroup", name)
	sg := &SubnetGroup{
		Name:        name,
		ARN:         arn,
		Description: description,
		VPCID:       vpcID,
		Subnets:     subnets,
	}
	if err := p.store.CreateSubnetGroup(sg); err != nil {
		if isUnique(err) {
			return shared.JSONError("SubnetGroupAlreadyExistsFault", "subnet group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SubnetGroup": sgToMap(sg)})
}

func (p *Provider) describeSubnetGroups(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubnetGroupName"].(string)
	if name != "" {
		sg, err := p.store.GetSubnetGroup(name)
		if err != nil {
			return shared.JSONError("SubnetGroupNotFoundFault", "subnet group not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"SubnetGroups": []any{sgToMap(sg)}})
	}
	sgs, err := p.store.ListSubnetGroups()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(sgs))
	for i := range sgs {
		list = append(list, sgToMap(&sgs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SubnetGroups": list})
}

func (p *Provider) updateSubnetGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubnetGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "SubnetGroupName is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	vpcID, _ := params["VpcId"].(string)
	var subnetStr string
	if rawSubnets, ok := params["SubnetIds"].([]any); ok {
		subnetStr = marshalStringSlice(toStringSlice(rawSubnets))
	}
	if err := p.store.UpdateSubnetGroup(name, description, vpcID, subnetStr); err != nil {
		return shared.JSONError("SubnetGroupNotFoundFault", "subnet group not found", http.StatusBadRequest), nil
	}
	sg, _ := p.store.GetSubnetGroup(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"SubnetGroup": sgToMap(sg)})
}

func (p *Provider) deleteSubnetGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubnetGroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "SubnetGroupName is required", http.StatusBadRequest), nil
	}
	sg, err := p.store.GetSubnetGroup(name)
	if err != nil {
		return shared.JSONError("SubnetGroupNotFoundFault", "subnet group not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(sg.ARN)
	if err := p.store.DeleteSubnetGroup(name); err != nil {
		return shared.JSONError("SubnetGroupNotFoundFault", "subnet group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SubnetGroup": sgToMap(sg)})
}

// ---- ACL handlers ----

func (p *Provider) createACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ACLName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ACLName is required", http.StatusBadRequest), nil
	}
	rawUsers, _ := params["UserNames"].([]any)
	userNames := marshalStringSlice(toStringSlice(rawUsers))
	arn := shared.BuildARN("memorydb", "acl", name)
	a := &ACL{
		Name:      name,
		ARN:       arn,
		Status:    "active",
		UserNames: userNames,
		MinEngine: "6.2",
	}
	if err := p.store.CreateACL(a); err != nil {
		if isUnique(err) {
			return shared.JSONError("ACLAlreadyExistsFault", "ACL already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ACL": aclToMap(a)})
}

func (p *Provider) describeACLs(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ACLName"].(string)
	if name != "" {
		a, err := p.store.GetACL(name)
		if err != nil {
			return shared.JSONError("ACLNotFoundFault", "ACL not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"ACLs": []any{aclToMap(a)}})
	}
	acls, err := p.store.ListACLs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(acls))
	for i := range acls {
		list = append(list, aclToMap(&acls[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ACLs": list})
}

func (p *Provider) updateACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ACLName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ACLName is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetACL(name)
	if err != nil {
		return shared.JSONError("ACLNotFoundFault", "ACL not found", http.StatusBadRequest), nil
	}
	users := unmarshalStringSlice(a.UserNames)
	if rawAdd, ok := params["UserNamesToAdd"].([]any); ok {
		for _, u := range toStringSlice(rawAdd) {
			users = appendUnique(users, u)
		}
	}
	if rawRm, ok := params["UserNamesToRemove"].([]any); ok {
		for _, u := range toStringSlice(rawRm) {
			users = removeStr(users, u)
		}
	}
	newUserNames := marshalStringSlice(users)
	if err := p.store.UpdateACL(name, newUserNames); err != nil {
		return nil, err
	}
	a, _ = p.store.GetACL(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ACL": aclToMap(a)})
}

func (p *Provider) deleteACL(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ACLName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ACLName is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetACL(name)
	if err != nil {
		return shared.JSONError("ACLNotFoundFault", "ACL not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(a.ARN)
	if err := p.store.DeleteACL(name); err != nil {
		return shared.JSONError("ACLNotFoundFault", "ACL not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ACL": aclToMap(a)})
}

// ---- User handlers ----

func (p *Provider) createUser(params map[string]any) (*plugin.Response, error) {
	name, _ := params["UserName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "UserName is required", http.StatusBadRequest), nil
	}
	accessString, _ := params["AccessString"].(string)
	if accessString == "" {
		accessString = "on ~* +@all"
	}
	authJSON := "{}"
	if auth, ok := params["AuthenticationMode"]; ok {
		b, _ := json.Marshal(auth)
		authJSON = string(b)
	}
	arn := shared.BuildARN("memorydb", "user", name)
	u := &User{
		Name:         name,
		ARN:          arn,
		Status:       "active",
		AccessString: accessString,
		Auth:         authJSON,
	}
	if err := p.store.CreateUser(u); err != nil {
		if isUnique(err) {
			return shared.JSONError("UserAlreadyExistsFault", "user already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"User": userToMap(u)})
}

func (p *Provider) describeUsers(params map[string]any) (*plugin.Response, error) {
	name, _ := params["UserName"].(string)
	if name != "" {
		u, err := p.store.GetUser(name)
		if err != nil {
			return shared.JSONError("UserNotFoundFault", "user not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"Users": []any{userToMap(u)}})
	}
	users, err := p.store.ListUsers()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(users))
	for i := range users {
		list = append(list, userToMap(&users[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Users": list})
}

func (p *Provider) updateUser(params map[string]any) (*plugin.Response, error) {
	name, _ := params["UserName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "UserName is required", http.StatusBadRequest), nil
	}
	accessString, _ := params["AccessString"].(string)
	authJSON := ""
	if auth, ok := params["AuthenticationMode"]; ok {
		b, _ := json.Marshal(auth)
		authJSON = string(b)
	}
	if err := p.store.UpdateUser(name, accessString, authJSON); err != nil {
		return shared.JSONError("UserNotFoundFault", "user not found", http.StatusBadRequest), nil
	}
	u, _ := p.store.GetUser(name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"User": userToMap(u)})
}

func (p *Provider) deleteUser(params map[string]any) (*plugin.Response, error) {
	name, _ := params["UserName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "UserName is required", http.StatusBadRequest), nil
	}
	u, err := p.store.GetUser(name)
	if err != nil {
		return shared.JSONError("UserNotFoundFault", "user not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(u.ARN)
	if err := p.store.DeleteUser(name); err != nil {
		return shared.JSONError("UserNotFoundFault", "user not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"User": userToMap(u)})
}

// ---- Snapshot handlers ----

func (p *Provider) createSnapshot(params map[string]any) (*plugin.Response, error) {
	snapName, _ := params["SnapshotName"].(string)
	if snapName == "" {
		return shared.JSONError("ValidationException", "SnapshotName is required", http.StatusBadRequest), nil
	}
	clusterName, _ := params["ClusterName"].(string)
	if clusterName == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetCluster(clusterName); err != nil {
		return shared.JSONError("ClusterNotFoundFault", "cluster not found", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("memorydb", "snapshot", snapName)
	snap := &Snapshot{
		Name:        snapName,
		ARN:         arn,
		ClusterName: clusterName,
		Status:      "available",
		Source:      "manual",
		CreatedAt:   nowUnix(),
	}
	if err := p.store.CreateSnapshot(snap); err != nil {
		if isUnique(err) {
			return shared.JSONError("SnapshotAlreadyExistsFault", "snapshot already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Snapshot": snapToMap(snap)})
}

func (p *Provider) describeSnapshots(params map[string]any) (*plugin.Response, error) {
	snapName, _ := params["SnapshotName"].(string)
	clusterName, _ := params["ClusterName"].(string)
	if snapName != "" {
		snap, err := p.store.GetSnapshot(snapName)
		if err != nil {
			return shared.JSONError("SnapshotNotFoundFault", "snapshot not found", http.StatusBadRequest), nil
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{"Snapshots": []any{snapToMap(snap)}})
	}
	snaps, err := p.store.ListSnapshots(clusterName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(snaps))
	for i := range snaps {
		list = append(list, snapToMap(&snaps[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Snapshots": list})
}

func (p *Provider) copySnapshot(params map[string]any) (*plugin.Response, error) {
	srcName, _ := params["SourceSnapshotName"].(string)
	dstName, _ := params["TargetSnapshotName"].(string)
	if srcName == "" || dstName == "" {
		return shared.JSONError("ValidationException", "SourceSnapshotName and TargetSnapshotName are required", http.StatusBadRequest), nil
	}
	src, err := p.store.GetSnapshot(srcName)
	if err != nil {
		return shared.JSONError("SnapshotNotFoundFault", "source snapshot not found", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("memorydb", "snapshot", dstName)
	snap := &Snapshot{
		Name:        dstName,
		ARN:         arn,
		ClusterName: src.ClusterName,
		Status:      "available",
		Source:      "manual",
		CreatedAt:   nowUnix(),
	}
	if err := p.store.CreateSnapshot(snap); err != nil {
		if isUnique(err) {
			return shared.JSONError("SnapshotAlreadyExistsFault", "snapshot already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Snapshot": snapToMap(snap)})
}

func (p *Provider) deleteSnapshot(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SnapshotName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "SnapshotName is required", http.StatusBadRequest), nil
	}
	snap, err := p.store.GetSnapshot(name)
	if err != nil {
		return shared.JSONError("SnapshotNotFoundFault", "snapshot not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(snap.ARN)
	if err := p.store.DeleteSnapshot(name); err != nil {
		return shared.JSONError("SnapshotNotFoundFault", "snapshot not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Snapshot": snapToMap(snap)})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidARNFault", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TagList": tagsToList(parseTags(rawTags))})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidARNFault", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := toStringSlice(rawKeys)
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{"TagList": tagsToList(tags)})
}

func (p *Provider) listTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidARNFault", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TagList": tagsToList(tags)})
}

// ---- serialization helpers ----

func clusterToMap(c *Cluster) map[string]any {
	return map[string]any{
		"Name":                    c.Name,
		"ARN":                     c.ARN,
		"Status":                  c.Status,
		"NodeType":                c.NodeType,
		"NumberOfShards":          c.NumShards,
		"EngineVersion":           c.EngineVersion,
		"SubnetGroupName":         c.SubnetGroup,
		"ACLName":                 c.ACLName,
		"ClusterEndpoint":         map[string]any{"Address": c.Endpoint, "Port": c.Port},
		"AvailabilityMode":        "MultiAZ",
		"AutoMinorVersionUpgrade": true,
	}
}

func pgToMap(pg *ParameterGroup) map[string]any {
	return map[string]any{
		"Name":        pg.Name,
		"ARN":         pg.ARN,
		"Family":      pg.Family,
		"Description": pg.Description,
	}
}

func sgToMap(sg *SubnetGroup) map[string]any {
	subnets := unmarshalStringSlice(sg.Subnets)
	subnetList := make([]any, 0, len(subnets))
	for _, s := range subnets {
		subnetList = append(subnetList, map[string]any{"Identifier": s})
	}
	return map[string]any{
		"Name":        sg.Name,
		"ARN":         sg.ARN,
		"Description": sg.Description,
		"VpcId":       sg.VPCID,
		"Subnets":     subnetList,
	}
}

func aclToMap(a *ACL) map[string]any {
	return map[string]any{
		"Name":                 a.Name,
		"ARN":                  a.ARN,
		"Status":               a.Status,
		"UserNames":            unmarshalStringSlice(a.UserNames),
		"MinimumEngineVersion": a.MinEngine,
	}
}

func userToMap(u *User) map[string]any {
	var auth any
	json.Unmarshal([]byte(u.Auth), &auth)
	return map[string]any{
		"Name":           u.Name,
		"ARN":            u.ARN,
		"Status":         u.Status,
		"AccessString":   u.AccessString,
		"Authentication": auth,
	}
}

func snapToMap(s *Snapshot) map[string]any {
	return map[string]any{
		"Name":        s.Name,
		"ARN":         s.ARN,
		"ClusterName": s.ClusterName,
		"Status":      s.Status,
		"Source":      s.Source,
	}
}

func tagsToList(tags map[string]string) []map[string]string {
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return list
}

func appendUnique(ss []string, s string) []string {
	for _, v := range ss {
		if v == s {
			return ss
		}
	}
	return append(ss, s)
}

func removeStr(ss []string, s string) []string {
	out := ss[:0]
	for _, v := range ss {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}
