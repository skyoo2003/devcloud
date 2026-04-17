// SPDX-License-Identifier: Apache-2.0

// internal/services/kafka/provider.go
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the Kafka (MSK) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "kafka" }
func (p *Provider) ServiceName() string           { return "Kafka" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "kafka"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	resp, err := p.handleRequest(ctx, op, req)
	if err != nil {
		return resp, err
	}
	// Kafka REST-JSON API uses camelCase wire format; convert response keys.
	if resp != nil && resp.StatusCode < 300 && len(resp.Body) > 0 {
		var data any
		if json.Unmarshal(resp.Body, &data) == nil {
			converted := shared.CamelCaseKeys(data)
			if b, err := json.Marshal(converted); err == nil {
				resp.Body = b
			}
		}
	}
	return resp, err
}

func (p *Provider) handleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
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

	// Normalize camelCase input params to PascalCase for internal consistency.
	params = shared.PascalCaseKeys(params)

	// Extract path parameters from URL path
	path := req.URL.Path

	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// ── Cluster CRUD ──────────────────────────────────────────────────────────
	case "CreateCluster":
		return p.createCluster(params)
	case "CreateClusterV2":
		return p.createClusterV2(params)
	case "DescribeCluster":
		clusterArn := extractClusterArnTopLevel(path) // /v1/clusters/{ClusterArn}
		return p.describeCluster(clusterArn)
	case "DescribeClusterV2":
		clusterArn := extractClusterArnTopLevel(path) // /api/v2/clusters/{ClusterArn}
		return p.describeClusterV2(clusterArn)
	case "ListClusters":
		return p.listClusters(req)
	case "ListClustersV2":
		return p.listClustersV2(req)
	case "DeleteCluster":
		clusterArn := extractClusterArnTopLevel(path)
		return p.deleteCluster(clusterArn)

	// ── Configuration CRUD ────────────────────────────────────────────────────
	case "CreateConfiguration":
		return p.createConfiguration(params)
	case "DescribeConfiguration":
		cfgArn := extractConfigurationArn(path)
		return p.describeConfiguration(cfgArn)
	case "DescribeConfigurationRevision":
		cfgArn := extractConfigurationArn(path)
		revStr := extractLastSegment(path)
		return p.describeConfigurationRevision(cfgArn, revStr)
	case "ListConfigurations":
		return p.listConfigurations()
	case "ListConfigurationRevisions":
		cfgArn := extractConfigurationArn(path)
		return p.listConfigurationRevisions(cfgArn)
	case "UpdateConfiguration":
		cfgArn := extractConfigurationArn(path)
		return p.updateConfiguration(cfgArn, params)
	case "DeleteConfiguration":
		cfgArn := extractConfigurationArn(path)
		return p.deleteConfiguration(cfgArn)

	// ── Topic CRUD ────────────────────────────────────────────────────────────
	case "CreateTopic":
		clusterArn := extractClusterArnFromTopicsPath(path)
		return p.createTopic(clusterArn, params)
	case "DescribeTopic":
		clusterArn, topicName := extractClusterAndTopic(path)
		return p.describeTopic(clusterArn, topicName)
	case "DescribeTopicPartitions":
		clusterArn, topicName := extractClusterAndTopicPartitions(path)
		return p.describeTopicPartitions(clusterArn, topicName)
	case "ListTopics":
		clusterArn := extractClusterArnFromTopicsPath(path)
		return p.listTopics(clusterArn)
	case "DeleteTopic":
		clusterArn, topicName := extractClusterAndTopic(path)
		return p.deleteTopic(clusterArn, topicName)
	case "UpdateTopic":
		clusterArn, topicName := extractClusterAndTopic(path)
		return p.updateTopic(clusterArn, topicName, params)

	// ── Tags ──────────────────────────────────────────────────────────────────
	case "TagResource":
		resourceArn := extractResourceArnFromTagPath(path)
		return p.tagResource(resourceArn, params)
	case "UntagResource":
		resourceArn := extractResourceArnFromTagPath(path)
		tagKeys := req.URL.Query()["tagKeys"]
		return p.untagResource(resourceArn, tagKeys)
	case "ListTagsForResource":
		resourceArn := extractResourceArnFromTagPath(path)
		return p.listTagsForResource(resourceArn)

	// ── Bootstrap Brokers ─────────────────────────────────────────────────────
	case "GetBootstrapBrokers":
		clusterArn := extractClusterArnFromSubpath(path, "bootstrap-brokers")
		return p.getBootstrapBrokers(clusterArn)

	// ── Cluster Updates ───────────────────────────────────────────────────────
	case "UpdateBrokerCount":
		clusterArn := extractClusterArnFromSubpath(path, "nodes")
		return p.updateBrokerCount(clusterArn, params)
	case "UpdateBrokerStorage":
		clusterArn := extractClusterArnFromSubpath(path, "nodes")
		return p.updateBrokerStorage(clusterArn, params)
	case "UpdateBrokerType":
		clusterArn := extractClusterArnFromSubpath(path, "nodes")
		return p.updateBrokerType(clusterArn, params)
	case "UpdateClusterConfiguration":
		clusterArn := extractClusterArnFromSubpath(path, "configuration")
		return p.updateClusterConfiguration(clusterArn, params)
	case "UpdateClusterKafkaVersion":
		clusterArn := extractClusterArnFromSubpath(path, "version")
		return p.updateClusterKafkaVersion(clusterArn, params)
	case "UpdateConnectivity":
		clusterArn := extractClusterArnFromSubpath(path, "connectivity")
		return p.updateClusterField(clusterArn)
	case "UpdateMonitoring":
		clusterArn := extractClusterArnFromSubpath(path, "monitoring")
		return p.updateClusterField(clusterArn)
	case "UpdateSecurity":
		clusterArn := extractClusterArnFromSubpath(path, "security")
		return p.updateClusterField(clusterArn)
	case "UpdateStorage":
		clusterArn := extractClusterArnFromSubpath(path, "storage")
		return p.updateClusterField(clusterArn)
	case "UpdateRebalancing":
		clusterArn := extractClusterArnFromSubpath(path, "rebalancing")
		return p.updateClusterField(clusterArn)

	// ── Cluster Policy ────────────────────────────────────────────────────────
	case "GetClusterPolicy":
		clusterArn := extractClusterArnFromSubpath(path, "policy")
		return p.getClusterPolicy(clusterArn)
	case "PutClusterPolicy":
		clusterArn := extractClusterArnFromSubpath(path, "policy")
		return p.putClusterPolicy(clusterArn, params)
	case "DeleteClusterPolicy":
		clusterArn := extractClusterArnFromSubpath(path, "policy")
		return p.deleteClusterPolicy(clusterArn)

	// ── Static Info ───────────────────────────────────────────────────────────
	case "GetCompatibleKafkaVersions":
		return p.getCompatibleKafkaVersions()
	case "ListKafkaVersions":
		return p.listKafkaVersions()
	case "ListNodes":
		clusterArn := extractClusterArnFromSubpath(path, "nodes")
		return p.listNodes(clusterArn)

	// ── Stub operations ───────────────────────────────────────────────────────
	case "BatchAssociateScramSecret":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterArn": "", "UnprocessedScramSecrets": []any{}})
	case "BatchDisassociateScramSecret":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterArn": "", "UnprocessedScramSecrets": []any{}})
	case "CreateReplicator":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReplicatorArn": shared.BuildARN("kafka", "replicator", shared.GenerateUUID()), "ReplicatorState": "RUNNING"})
	case "CreateVpcConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcConnectionArn": shared.BuildARN("kafka", "vpc-connection", shared.GenerateUUID()), "State": "AVAILABLE"})
	case "DeleteReplicator":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReplicatorArn": "", "ReplicatorState": "DELETING"})
	case "DeleteVpcConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcConnectionArn": "", "State": "DELETING"})
	case "DescribeClusterOperation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterOperationInfo": map[string]any{}})
	case "DescribeClusterOperationV2":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterOperationInfo": map[string]any{}})
	case "DescribeReplicator":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReplicatorArn": "", "ReplicatorState": "RUNNING"})
	case "DescribeVpcConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcConnectionArn": "", "State": "AVAILABLE"})
	case "ListClientVpcConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClientVpcConnections": []any{}, "NextToken": ""})
	case "ListClusterOperations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterOperationInfoList": []any{}, "NextToken": ""})
	case "ListClusterOperationsV2":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterOperationInfoList": []any{}, "NextToken": ""})
	case "ListReplicators":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Replicators": []any{}, "NextToken": ""})
	case "ListScramSecrets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SecretArnList": []any{}, "NextToken": ""})
	case "ListVpcConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcConnections": []any{}, "NextToken": ""})
	case "RebootBroker":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ClusterArn": "", "ClusterOperationArn": ""})
	case "RejectClientVpcConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcConnectionArn": "", "State": "REJECTED"})
	case "UpdateReplicationInfo":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReplicatorArn": "", "ReplicatorState": "RUNNING"})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	// Kafka/MSK paths are complex with embedded ARNs containing slashes.
	// We match prefix patterns to identify the operation.

	switch {
	// Tags: /v1/tags/{arn}
	case strings.HasPrefix(path, "/v1/tags"):
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// V2 APIs
	case strings.HasPrefix(path, "/api/v2/clusters"):
		rest := strings.TrimPrefix(path, "/api/v2/clusters")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case http.MethodPost:
				return "CreateClusterV2"
			case http.MethodGet:
				return "ListClustersV2"
			}
		}
		if rest != "" && method == http.MethodGet {
			return "DescribeClusterV2"
		}

	// Configurations: /v1/configurations[/{arn}[/revisions[/{rev}]]]
	case strings.HasPrefix(path, "/v1/configurations"):
		rest := strings.TrimPrefix(path, "/v1/configurations")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case http.MethodPost:
				return "CreateConfiguration"
			case http.MethodGet:
				return "ListConfigurations"
			}
		}
		if strings.Contains(rest, "/revisions/") {
			return "DescribeConfigurationRevision"
		}
		if strings.HasSuffix(rest, "/revisions") {
			return "ListConfigurationRevisions"
		}
		switch method {
		case http.MethodGet:
			return "DescribeConfiguration"
		case http.MethodPut:
			return "UpdateConfiguration"
		case http.MethodDelete:
			return "DeleteConfiguration"
		}

	// Compatible Kafka versions: /v1/compatible-kafka-versions
	case strings.HasPrefix(path, "/v1/compatible-kafka-versions"):
		return "GetCompatibleKafkaVersions"
	case strings.HasPrefix(path, "/v1/kafka-versions"):
		return "ListKafkaVersions"

	// Clusters: /v1/clusters[/{arn}[/subpath]]
	case strings.HasPrefix(path, "/v1/clusters"):
		rest := strings.TrimPrefix(path, "/v1/clusters")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			switch method {
			case http.MethodPost:
				return "CreateCluster"
			case http.MethodGet:
				return "ListClusters"
			}
		}
		// Check for subpath operations by looking for known keywords
		if strings.Contains(rest, "/topics/") && strings.HasSuffix(rest, "/partitions") {
			return "DescribeTopicPartitions"
		}
		if strings.Contains(rest, "/topics/") {
			switch method {
			case http.MethodGet:
				return "DescribeTopic"
			case http.MethodPut:
				return "UpdateTopic"
			case http.MethodDelete:
				return "DeleteTopic"
			}
		}
		if strings.HasSuffix(rest, "/topics") {
			switch method {
			case http.MethodPost:
				return "CreateTopic"
			case http.MethodGet:
				return "ListTopics"
			}
		}
		if strings.HasSuffix(rest, "/bootstrap-brokers") {
			return "GetBootstrapBrokers"
		}
		if strings.HasSuffix(rest, "/policy") {
			switch method {
			case http.MethodGet:
				return "GetClusterPolicy"
			case http.MethodPut:
				return "PutClusterPolicy"
			case http.MethodDelete:
				return "DeleteClusterPolicy"
			}
		}
		if strings.HasSuffix(rest, "/nodes") {
			switch method {
			case http.MethodPut:
				return "UpdateBrokerCount"
			case http.MethodGet:
				return "ListNodes"
			}
		}
		if strings.HasSuffix(rest, "/configuration") {
			return "UpdateClusterConfiguration"
		}
		if strings.HasSuffix(rest, "/version") {
			return "UpdateClusterKafkaVersion"
		}
		if strings.HasSuffix(rest, "/connectivity") {
			return "UpdateConnectivity"
		}
		if strings.HasSuffix(rest, "/monitoring") {
			return "UpdateMonitoring"
		}
		if strings.HasSuffix(rest, "/security") {
			return "UpdateSecurity"
		}
		if strings.HasSuffix(rest, "/storage") {
			return "UpdateStorage"
		}
		if strings.HasSuffix(rest, "/rebalancing") {
			return "UpdateRebalancing"
		}
		// Default: cluster-level operation
		switch method {
		case http.MethodGet:
			return "DescribeCluster"
		case http.MethodDelete:
			return "DeleteCluster"
		}

	// Replicators, VPC connections, etc.
	case strings.HasPrefix(path, "/replication/v1/replicators"):
		rest := strings.TrimPrefix(path, "/replication/v1/replicators")
		if rest == "" || rest == "/" {
			switch method {
			case http.MethodPost:
				return "CreateReplicator"
			case http.MethodGet:
				return "ListReplicators"
			}
		}
		switch method {
		case http.MethodGet:
			return "DescribeReplicator"
		case http.MethodDelete:
			return "DeleteReplicator"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(clusters))
	for _, c := range clusters {
		res = append(res, plugin.Resource{Type: "kafka-cluster", ID: c.ARN, Name: c.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ── Cluster operations ─────────────────────────────────────────────────────

func (p *Provider) createCluster(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	kafkaVersion, _ := params["KafkaVersion"].(string)
	if kafkaVersion == "" {
		kafkaVersion = "3.5.1"
	}
	brokerType := "kafka.m5.large"
	brokerCount := 3
	if bni, ok := params["BrokerNodeGroupInfo"].(map[string]any); ok {
		if it, ok := bni["InstanceType"].(string); ok && it != "" {
			brokerType = it
		}
	}
	if nc, ok := params["NumberOfBrokerNodes"].(float64); ok && nc > 0 {
		brokerCount = int(nc)
	}

	arn := shared.BuildARN("kafka", "cluster", name+"/"+shared.GenerateID("", 32))
	c, err := p.store.CreateCluster(arn, name, kafkaVersion, brokerType, brokerCount)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "cluster already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := make(map[string]string)
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		p.store.tags.AddTags(c.ARN, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":  c.ARN,
		"ClusterName": c.Name,
		"State":       c.Status,
	})
}

func (p *Provider) createClusterV2(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ClusterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ClusterName is required", http.StatusBadRequest), nil
	}
	kafkaVersion := "3.5.1"
	brokerType := "kafka.m5.large"
	brokerCount := 3

	if prov, ok := params["Provisioned"].(map[string]any); ok {
		if kv, ok := prov["KafkaVersion"].(string); ok && kv != "" {
			kafkaVersion = kv
		}
		if nc, ok := prov["NumberOfBrokerNodes"].(float64); ok && nc > 0 {
			brokerCount = int(nc)
		}
		if bni, ok := prov["BrokerNodeGroupInfo"].(map[string]any); ok {
			if it, ok := bni["InstanceType"].(string); ok && it != "" {
				brokerType = it
			}
		}
	}

	arn := shared.BuildARN("kafka", "cluster", name+"/"+shared.GenerateID("", 32))
	c, err := p.store.CreateCluster(arn, name, kafkaVersion, brokerType, brokerCount)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "cluster already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := make(map[string]string)
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		p.store.tags.AddTags(c.ARN, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":  c.ARN,
		"ClusterName": c.Name,
		"ClusterType": "PROVISIONED",
		"State":       c.Status,
	})
}

func (p *Provider) describeCluster(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetClusterByARN(clusterArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterInfo": clusterToInfo(c),
	})
}

func (p *Provider) describeClusterV2(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetClusterByARN(clusterArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterInfo": clusterToInfoV2(c),
	})
}

func (p *Provider) listClusters(req *http.Request) (*plugin.Response, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	filter := req.URL.Query().Get("clusterNameFilter")
	list := make([]map[string]any, 0, len(clusters))
	for _, c := range clusters {
		if filter != "" && !strings.Contains(c.Name, filter) {
			continue
		}
		list = append(list, clusterToInfo(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterInfoList": list,
		"NextToken":       "",
	})
}

func (p *Provider) listClustersV2(req *http.Request) (*plugin.Response, error) {
	clusters, err := p.store.ListClusters()
	if err != nil {
		return nil, err
	}
	filter := req.URL.Query().Get("clusterNameFilter")
	list := make([]map[string]any, 0, len(clusters))
	for _, c := range clusters {
		if filter != "" && !strings.Contains(c.Name, filter) {
			continue
		}
		list = append(list, clusterToInfoV2(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Clusters":  list,
		"NextToken": "",
	})
}

func (p *Provider) deleteCluster(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetClusterByARN(clusterArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(clusterArn)
	if err := p.store.DeleteCluster(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn": c.ARN,
		"State":      "DELETING",
	})
}

// ── Configuration operations ───────────────────────────────────────────────

func (p *Provider) createConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	serverProps, _ := params["ServerProperties"].(string)

	kafkaVersions := "[]"
	if rawVersions, ok := params["KafkaVersions"].([]any); ok {
		b, _ := json.Marshal(rawVersions)
		kafkaVersions = string(b)
	}

	arn := shared.BuildARN("kafka", "configuration", name+"/"+shared.GenerateID("", 16))
	cfg, err := p.store.CreateConfiguration(arn, name, kafkaVersions, serverProps)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "configuration already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, configurationToInfo(cfg))
}

func (p *Provider) describeConfiguration(cfgArn string) (*plugin.Response, error) {
	if cfgArn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	cfg, err := p.store.GetConfigurationByARN(cfgArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, configurationToInfo(cfg))
}

func (p *Provider) describeConfigurationRevision(cfgArn, _ string) (*plugin.Response, error) {
	if cfgArn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	cfg, err := p.store.GetConfigurationByARN(cfgArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn":              cfg.ARN,
		"CreationTime":     cfg.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"Revision":         cfg.Revision,
		"ServerProperties": cfg.ServerProps,
	})
}

func (p *Provider) listConfigurations() (*plugin.Response, error) {
	cfgs, err := p.store.ListConfigurations()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(cfgs))
	for _, c := range cfgs {
		list = append(list, configurationToInfo(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Configurations": list,
		"NextToken":      "",
	})
}

func (p *Provider) listConfigurationRevisions(cfgArn string) (*plugin.Response, error) {
	if cfgArn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	cfg, err := p.store.GetConfigurationByARN(cfgArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	revisions := make([]map[string]any, 0, cfg.Revision)
	for i := 1; i <= cfg.Revision; i++ {
		revisions = append(revisions, map[string]any{
			"Revision":     i,
			"CreationTime": cfg.CreatedAt.Format("2006-01-02T15:04:05Z"),
			"Description":  "",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Revisions": revisions,
		"NextToken": "",
	})
}

func (p *Provider) updateConfiguration(cfgArn string, params map[string]any) (*plugin.Response, error) {
	if cfgArn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	serverProps, _ := params["ServerProperties"].(string)
	cfg, err := p.store.UpdateConfiguration(cfgArn, serverProps)
	if err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn": cfg.ARN,
		"LatestRevision": map[string]any{
			"Revision":     cfg.Revision,
			"CreationTime": cfg.CreatedAt.Format("2006-01-02T15:04:05Z"),
		},
	})
}

func (p *Provider) deleteConfiguration(cfgArn string) (*plugin.Response, error) {
	if cfgArn == "" {
		return shared.JSONError("ValidationException", "Arn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteConfiguration(cfgArn); err != nil {
		return shared.JSONError("NotFoundException", "configuration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Arn": cfgArn, "State": "DELETE_FAILED"})
}

// ── Topic operations ──────────────────────────────────────────────────────

func (p *Provider) createTopic(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	// ensure cluster exists
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}

	topicName, _ := params["TopicName"].(string)
	if topicName == "" {
		return shared.JSONError("ValidationException", "TopicName is required", http.StatusBadRequest), nil
	}

	partitions := 1
	if n, ok := params["NumPartitions"].(float64); ok && n > 0 {
		partitions = int(n)
	}
	replication := 3
	if r, ok := params["ReplicationFactor"].(float64); ok && r > 0 {
		replication = int(r)
	}
	config := "{}"
	if cfg, ok := params["ConfigEntries"].([]any); ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}

	arn := shared.BuildARN("kafka", "topic", shared.GenerateID("", 32))
	t, err := p.store.CreateTopic(arn, topicName, clusterArn, partitions, replication, config)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "topic already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, topicToInfo(t))
}

func (p *Provider) describeTopic(clusterArn, topicName string) (*plugin.Response, error) {
	if clusterArn == "" || topicName == "" {
		return shared.JSONError("ValidationException", "ClusterArn and TopicName are required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetTopic(clusterArn, topicName)
	if err != nil {
		return shared.JSONError("NotFoundException", "topic not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, topicToInfo(t))
}

func (p *Provider) describeTopicPartitions(clusterArn, topicName string) (*plugin.Response, error) {
	if clusterArn == "" || topicName == "" {
		return shared.JSONError("ValidationException", "ClusterArn and TopicName are required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetTopic(clusterArn, topicName)
	if err != nil {
		return shared.JSONError("NotFoundException", "topic not found", http.StatusNotFound), nil
	}
	partitions := make([]map[string]any, 0, t.Partitions)
	for i := 0; i < t.Partitions; i++ {
		partitions = append(partitions, map[string]any{
			"Partition": i,
			"Leader":    0,
			"Replicas":  []int{0},
			"Isr":       []int{0},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TopicName":  topicName,
		"Partitions": partitions,
		"NextToken":  "",
	})
}

func (p *Provider) listTopics(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	topics, err := p.store.ListTopics(clusterArn)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(topics))
	for _, t := range topics {
		list = append(list, topicToInfo(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TopicsList": list,
		"NextToken":  "",
	})
}

func (p *Provider) deleteTopic(clusterArn, topicName string) (*plugin.Response, error) {
	if clusterArn == "" || topicName == "" {
		return shared.JSONError("ValidationException", "ClusterArn and TopicName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTopic(clusterArn, topicName); err != nil {
		return shared.JSONError("NotFoundException", "topic not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateTopic(clusterArn, topicName string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" || topicName == "" {
		return shared.JSONError("ValidationException", "ClusterArn and TopicName are required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetTopic(clusterArn, topicName)
	if err != nil {
		return shared.JSONError("NotFoundException", "topic not found", http.StatusNotFound), nil
	}
	partitions := t.Partitions
	if n, ok := params["NumPartitions"].(float64); ok && n > 0 {
		partitions = int(n)
	}
	config := t.Config
	if cfg, ok := params["ConfigEntries"].([]any); ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	updated, err := p.store.UpdateTopic(clusterArn, topicName, partitions, config)
	if err != nil {
		return shared.JSONError("NotFoundException", "topic not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, topicToInfo(updated))
}

// ── Tag operations ─────────────────────────────────────────────────────────

func (p *Provider) tagResource(resourceArn string, params map[string]any) (*plugin.Response, error) {
	if resourceArn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string)
	for k, v := range rawTags {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}
	if err := p.store.tags.AddTags(resourceArn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(resourceArn string, tagKeys []string) (*plugin.Response, error) {
	if resourceArn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.tags.RemoveTags(resourceArn, tagKeys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(resourceArn string) (*plugin.Response, error) {
	if resourceArn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceArn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// ── Bootstrap Brokers ─────────────────────────────────────────────────────

func (p *Provider) getBootstrapBrokers(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetClusterByARN(clusterArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	brokers := make([]string, 0, c.BrokerCount)
	for i := 0; i < c.BrokerCount; i++ {
		brokers = append(brokers, fmt.Sprintf("b-%d.devcloud.kafka.us-east-1.amazonaws.com:9092", i+1))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"BootstrapBrokerString":                         strings.Join(brokers, ","),
		"BootstrapBrokerStringSaslScram":                "",
		"BootstrapBrokerStringSaslIam":                  "",
		"BootstrapBrokerStringTls":                      "",
		"BootstrapBrokerStringPublicSaslScram":          "",
		"BootstrapBrokerStringPublicSaslIam":            "",
		"BootstrapBrokerStringPublicTls":                "",
		"BootstrapBrokerStringVpcConnectivityTls":       "",
		"BootstrapBrokerStringVpcConnectivitySaslScram": "",
		"BootstrapBrokerStringVpcConnectivitySaslIam":   "",
	})
}

// ── Cluster field updates ─────────────────────────────────────────────────

func (p *Provider) updateBrokerCount(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	count := 3
	if n, ok := params["TargetNumberOfBrokerNodes"].(float64); ok && n > 0 {
		count = int(n)
	}
	p.store.UpdateCluster(clusterArn, map[string]any{"broker_count": count})
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

func (p *Provider) updateBrokerStorage(clusterArn string, _ map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

func (p *Provider) updateBrokerType(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	if bt, ok := params["TargetInstanceType"].(string); ok && bt != "" {
		p.store.UpdateCluster(clusterArn, map[string]any{"broker_type": bt})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

func (p *Provider) updateClusterConfiguration(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	if cfg, ok := params["ConfigurationInfo"].(map[string]any); ok {
		if b, err := json.Marshal(cfg); err == nil {
			p.store.UpdateCluster(clusterArn, map[string]any{"config": string(b)})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

func (p *Provider) updateClusterKafkaVersion(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	if kv, ok := params["TargetKafkaVersion"].(string); ok && kv != "" {
		p.store.UpdateCluster(clusterArn, map[string]any{"kafka_version": kv})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

// updateClusterField validates the cluster exists and returns success.
func (p *Provider) updateClusterField(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ClusterArn":          clusterArn,
		"ClusterOperationArn": shared.BuildARN("kafka", "cluster-operation", shared.GenerateUUID()),
	})
}

// ── Cluster Policy ────────────────────────────────────────────────────────

func (p *Provider) getClusterPolicy(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	// ensure cluster exists
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	policy, version, err := p.store.GetClusterPolicy(clusterArn)
	if err != nil {
		if errors.Is(err, errClusterNotFound) {
			return shared.JSONResponse(http.StatusOK, map[string]any{"Policy": "", "CurrentVersion": "1"})
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Policy":         policy,
		"CurrentVersion": version,
	})
}

func (p *Provider) putClusterPolicy(clusterArn string, params map[string]any) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	policy, _ := params["Policy"].(string)
	version, err := p.store.PutClusterPolicy(clusterArn, policy)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"CurrentVersion": version})
}

func (p *Provider) deleteClusterPolicy(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetClusterByARN(clusterArn); err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteClusterPolicy(clusterArn); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ── Static Info ───────────────────────────────────────────────────────────

func (p *Provider) getCompatibleKafkaVersions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"KafkaVersions": []map[string]any{
			{"SourceVersion": "3.5.1", "TargetVersions": []string{"3.5.1"}},
			{"SourceVersion": "3.4.0", "TargetVersions": []string{"3.5.1"}},
			{"SourceVersion": "3.3.2", "TargetVersions": []string{"3.4.0", "3.5.1"}},
		},
	})
}

func (p *Provider) listKafkaVersions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"KafkaVersions": []map[string]any{
			{"Version": "3.5.1", "Status": "ACTIVE"},
			{"Version": "3.4.0", "Status": "ACTIVE"},
			{"Version": "3.3.2", "Status": "ACTIVE"},
			{"Version": "3.2.0", "Status": "ACTIVE"},
			{"Version": "2.8.1", "Status": "ACTIVE"},
			{"Version": "2.6.0", "Status": "DEPRECATED"},
		},
	})
}

func (p *Provider) listNodes(clusterArn string) (*plugin.Response, error) {
	if clusterArn == "" {
		return shared.JSONError("ValidationException", "ClusterArn is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetClusterByARN(clusterArn)
	if err != nil {
		return shared.JSONError("NotFoundException", "cluster not found", http.StatusNotFound), nil
	}
	nodes := make([]map[string]any, 0, c.BrokerCount)
	for i := 0; i < c.BrokerCount; i++ {
		nodes = append(nodes, map[string]any{
			"AddedToClusterTime": c.CreatedAt.Format("2006-01-02T15:04:05Z"),
			"BrokerNodeInfo": map[string]any{
				"AttachedENIId":      fmt.Sprintf("eni-%016x", i),
				"BrokerId":           float64(i + 1),
				"ClientSubnet":       "subnet-00000001",
				"ClientVpcIpAddress": fmt.Sprintf("10.0.0.%d", i+10),
				"Endpoints":          []string{fmt.Sprintf("b-%d.devcloud.kafka.us-east-1.amazonaws.com", i+1)},
			},
			"InstanceType": c.BrokerType,
			"NodeARN":      shared.BuildARN("kafka", "broker", fmt.Sprintf("%s/%d", c.Name, i+1)),
			"NodeType":     "BROKER",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"NodeInfoList": nodes,
		"NextToken":    "",
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────

func clusterToInfo(c *Cluster) map[string]any {
	return map[string]any{
		"ClusterArn":          c.ARN,
		"ClusterName":         c.Name,
		"State":               c.Status,
		"CreationTime":        c.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"CurrentVersion":      "1",
		"NumberOfBrokerNodes": c.BrokerCount,
		"BrokerNodeGroupInfo": map[string]any{
			"InstanceType": c.BrokerType,
		},
		"CurrentBrokerSoftwareInfo": map[string]any{
			"KafkaVersion": c.KafkaVersion,
		},
	}
}

func clusterToInfoV2(c *Cluster) map[string]any {
	return map[string]any{
		"ClusterArn":   c.ARN,
		"ClusterName":  c.Name,
		"ClusterType":  "PROVISIONED",
		"State":        c.Status,
		"CreationTime": c.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"Provisioned": map[string]any{
			"NumberOfBrokerNodes": c.BrokerCount,
			"BrokerNodeGroupInfo": map[string]any{
				"InstanceType": c.BrokerType,
			},
			"CurrentBrokerSoftwareInfo": map[string]any{
				"KafkaVersion": c.KafkaVersion,
			},
		},
	}
}

func configurationToInfo(c *Configuration) map[string]any {
	var kafkaVersions []string
	json.Unmarshal([]byte(c.KafkaVersions), &kafkaVersions)
	if kafkaVersions == nil {
		kafkaVersions = []string{}
	}
	return map[string]any{
		"Arn":  c.ARN,
		"Name": c.Name,
		"LatestRevision": map[string]any{
			"Revision":     c.Revision,
			"CreationTime": c.CreatedAt.Format("2006-01-02T15:04:05Z"),
		},
		"KafkaVersions":    kafkaVersions,
		"ServerProperties": c.ServerProps,
		"State":            "ACTIVE",
	}
}

func topicToInfo(t *Topic) map[string]any {
	var config any
	if err := json.Unmarshal([]byte(t.Config), &config); err != nil {
		config = t.Config
	}
	return map[string]any{
		"TopicArn":          t.ARN,
		"TopicName":         t.Name,
		"ClusterArn":        t.ClusterARN,
		"NumPartitions":     t.Partitions,
		"ReplicationFactor": t.Replication,
		"ConfigEntries":     config,
	}
}

// extractLastSegment returns the last path segment (used for ClusterArn, ConfigArn at end of path).
func extractLastSegment(path string) string {
	parts := strings.Split(strings.TrimSuffix(path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// extractConfigurationArn returns the ARN from paths like /v1/configurations/{Arn}[/...].
// The ARN is a full AWS ARN so it will span multiple "segments"; we reconstruct it from
// path position. Specifically, routes are /v1/configurations/<arn> where the arn is
// base64-URL-encoded or the raw arn with slashes. We use the generated router's PathParams
// but since we receive op+path without params, we search for "configurations" anchor.
func extractConfigurationArn(path string) string {
	return extractPathParamAfter(path, "configurations")
}

// extractPathParamAfter returns the value immediately after the given segment in the path.
func extractPathParamAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == segment && i+1 < len(parts) {
			// The arn may contain slashes; join remaining up to "revisions" keyword
			remaining := parts[i+1:]
			result := []string{}
			for _, seg := range remaining {
				if seg == "revisions" {
					break
				}
				result = append(result, seg)
			}
			return strings.Join(result, "/")
		}
	}
	return ""
}

// extractClusterArnFromTopicsPath returns cluster ARN from /v1/clusters/{ClusterArn}/topics
func extractClusterArnFromTopicsPath(path string) string {
	return extractClusterArnFromSubpath(path, "topics")
}

// extractClusterArnFromSubpath returns the cluster ARN from a path like
// /v1/clusters/{ClusterArn}/subpath[/...].
// ClusterArns contain "/" so we grab everything between "clusters" and the subpath keyword.
func extractClusterArnFromSubpath(path, subpath string) string {
	parts := strings.Split(path, "/")
	start := -1
	end := -1
	for i, p := range parts {
		if p == "clusters" && start == -1 {
			start = i + 1
		}
		if start > 0 && i > start && p == subpath {
			end = i
			break
		}
	}
	if start < 0 || end < 0 {
		return ""
	}
	return strings.Join(parts[start:end], "/")
}

// extractClusterAndTopic parses /v1/clusters/{ClusterArn}/topics/{TopicName}
func extractClusterAndTopic(path string) (clusterArn, topicName string) {
	parts := strings.Split(path, "/")
	topicIdx := -1
	for i, p := range parts {
		if p == "topics" {
			topicIdx = i
			break
		}
	}
	if topicIdx < 0 || topicIdx+1 >= len(parts) {
		return "", ""
	}
	// find "clusters" anchor
	clustersIdx := -1
	for i, p := range parts {
		if p == "clusters" {
			clustersIdx = i
			break
		}
	}
	if clustersIdx < 0 {
		return "", ""
	}
	clusterArn = strings.Join(parts[clustersIdx+1:topicIdx], "/")
	topicName = strings.Join(parts[topicIdx+1:], "/")
	// strip /partitions suffix if present
	topicName = strings.TrimSuffix(topicName, "/partitions")
	return clusterArn, topicName
}

// extractClusterAndTopicPartitions parses /v1/clusters/{ClusterArn}/topics/{TopicName}/partitions
func extractClusterAndTopicPartitions(path string) (clusterArn, topicName string) {
	// strip trailing /partitions and delegate
	trimmed := strings.TrimSuffix(path, "/partitions")
	return extractClusterAndTopic(trimmed)
}

// extractResourceArnFromTagPath returns ARN from /v1/tags/{ResourceArn}
func extractResourceArnFromTagPath(path string) string {
	return extractPathParamAfter(path, "tags")
}

// extractClusterArnTopLevel returns the cluster ARN from paths like /v1/clusters/{ClusterArn}
// where the ClusterArn may contain slashes (it's a full AWS ARN).
// It grabs everything after the "clusters" segment.
func extractClusterArnTopLevel(path string) string {
	const anchor = "clusters"
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == anchor && i+1 < len(parts) {
			return strings.Join(parts[i+1:], "/")
		}
	}
	return ""
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
