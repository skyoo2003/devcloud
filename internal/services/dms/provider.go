// SPDX-License-Identifier: Apache-2.0

// Package dms implements AWS Database Migration Service.
package dms

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

const defaultAccountID = plugin.DefaultAccountID

// DMSProvider implements plugin.ServicePlugin for DMS.
type DMSProvider struct {
	store *Store
}

// ServiceID returns the unique identifier for this plugin.
func (p *DMSProvider) ServiceID() string { return "dms" }

// ServiceName returns the human-readable name for this plugin.
func (p *DMSProvider) ServiceName() string { return "AmazonDMSv20160101" }

// Protocol returns the wire protocol used by this plugin.
func (p *DMSProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

// Init initialises the DMSProvider from cfg.
func (p *DMSProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "dms"))
	return err
}

// Shutdown closes the DMSProvider.
func (p *DMSProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate DMS operation.
func (p *DMSProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			op = target[idx+1:]
		} else {
			op = target
		}
	}
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

	switch op {
	// Replication Instances
	case "CreateReplicationInstance":
		return p.createReplicationInstance(params)
	case "DeleteReplicationInstance":
		return p.deleteReplicationInstance(params)
	case "DescribeReplicationInstances":
		return p.describeReplicationInstances(params)
	case "ModifyReplicationInstance":
		return p.modifyReplicationInstance(params)
	case "RebootReplicationInstance":
		return p.rebootReplicationInstance(params)

	// Replication Tasks
	case "CreateReplicationTask":
		return p.createReplicationTask(params)
	case "DeleteReplicationTask":
		return p.deleteReplicationTask(params)
	case "DescribeReplicationTasks":
		return p.describeReplicationTasks(params)
	case "ModifyReplicationTask":
		return p.modifyReplicationTask(params)
	case "StartReplicationTask":
		return p.startReplicationTask(params)
	case "StopReplicationTask":
		return p.stopReplicationTask(params)
	case "ReloadTables":
		return p.reloadTables(params)
	case "DescribeTableStatistics":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ReplicationTaskArn": getString(params, "ReplicationTaskArn"),
			"TableStatistics":    []any{},
		})
	case "DescribeReplicationTaskAssessmentResults":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ReplicationTaskAssessmentResults": []any{},
		})

	// Endpoints
	case "CreateEndpoint":
		return p.createEndpoint(params)
	case "DeleteEndpoint":
		return p.deleteEndpoint(params)
	case "DescribeEndpoints":
		return p.describeEndpoints(params)
	case "ModifyEndpoint":
		return p.modifyEndpoint(params)
	case "TestConnection":
		return p.testConnection(params)
	case "DescribeConnections":
		return p.describeConnections(params)
	case "DescribeEndpointTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SupportedEndpointTypes": []any{
				map[string]any{"EngineName": "mysql", "SupportsCDC": true, "EndpointType": "source"},
				map[string]any{"EngineName": "postgres", "SupportsCDC": true, "EndpointType": "source"},
				map[string]any{"EngineName": "aurora", "SupportsCDC": true, "EndpointType": "target"},
				map[string]any{"EngineName": "oracle", "SupportsCDC": true, "EndpointType": "source"},
			},
		})

	// Tags
	case "AddTagsToResource":
		return p.addTags(params)
	case "RemoveTagsFromResource":
		return p.removeTags(params)
	case "ListTagsForResource":
		return p.listTags(params)

	// Event Subscriptions
	case "CreateEventSubscription":
		return p.createEventSubscription(params)
	case "DescribeEventSubscriptions":
		return p.describeEventSubscriptions(params)
	case "DeleteEventSubscription":
		return p.deleteEventSubscription(params)
	case "DescribeEvents":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Events": []any{}})
	case "DescribeEventCategories":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EventCategoryGroupList": []any{},
		})
	case "DescribeAccountAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"AccountQuotas":           []any{},
			"UniqueAccountIdentifier": defaultAccountID,
		})
	case "DescribeCertificates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Certificates": []any{}})
	case "DescribeOrderableReplicationInstances":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"OrderableReplicationInstances": []any{
				map[string]any{
					"EngineVersion":            "3.5.1",
					"ReplicationInstanceClass": "dms.t2.micro",
					"StorageType":              "gp2",
					"MinAllocatedStorage":      5,
					"MaxAllocatedStorage":      6144,
					"DefaultAllocatedStorage":  50,
					"IncludedAllocatedStorage": 50,
				},
			},
		})
	case "DescribeReplicationSubnetGroups":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ReplicationSubnetGroups": []any{},
		})
	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation not supported: %s", op), http.StatusBadRequest), nil
	}
}

// ListResources returns instances as plugin resources.
func (p *DMSProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	instances, err := p.store.ListReplicationInstances()
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(instances))
	for _, r := range instances {
		out = append(out, plugin.Resource{Type: "replication-instance", ID: r.ARN, Name: r.Identifier})
	}
	return out, nil
}

// GetMetrics returns empty metrics.
func (p *DMSProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Replication Instance handlers ---

func (p *DMSProvider) createReplicationInstance(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationInstanceIdentifier"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationInstanceIdentifier is required", http.StatusBadRequest), nil
	}
	instClass, _ := params["ReplicationInstanceClass"].(string)
	if instClass == "" {
		instClass = "dms.t2.micro"
	}
	engineVersion, _ := params["EngineVersion"].(string)
	if engineVersion == "" {
		engineVersion = "3.5.1"
	}
	multiAZ, _ := params["MultiAZ"].(bool)
	allocStorage := 20
	if v, ok := params["AllocatedStorage"].(float64); ok {
		allocStorage = int(v)
	}
	pubAccess, _ := params["PubliclyAccessible"].(bool)

	arn := fmt.Sprintf("arn:aws:dms:%s:%s:rep:%s", shared.DefaultRegion, defaultAccountID, id)
	ri := &ReplicationInstance{
		ARN:                arn,
		Identifier:         id,
		InstanceClass:      instClass,
		EngineVersion:      engineVersion,
		Status:             "available",
		MultiAZ:            multiAZ,
		AllocatedStorage:   allocStorage,
		PubliclyAccessible: pubAccess,
		AccountID:          defaultAccountID,
		CreatedAt:          time.Now(),
	}
	if err := p.store.CreateReplicationInstance(ri); err != nil {
		return shared.JSONError("ResourceAlreadyExistsFault", "instance already exists", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationInstance": instanceToMap(ri),
	})
}

func (p *DMSProvider) deleteReplicationInstance(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationInstanceArn"].(string)
	if id == "" {
		id, _ = params["ReplicationInstanceIdentifier"].(string)
	}
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationInstanceArn is required", http.StatusBadRequest), nil
	}
	ri, err := p.store.GetReplicationInstance(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "instance not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteReplicationInstance(id); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "instance not found", http.StatusNotFound), nil
	}
	ri.Status = "deleting"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationInstance": instanceToMap(ri),
	})
}

func (p *DMSProvider) describeReplicationInstances(_ map[string]any) (*plugin.Response, error) {
	instances, err := p.store.ListReplicationInstances()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(instances))
	for _, r := range instances {
		items = append(items, instanceToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationInstances": items,
	})
}

func (p *DMSProvider) modifyReplicationInstance(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationInstanceArn"].(string)
	if id == "" {
		id, _ = params["ReplicationInstanceIdentifier"].(string)
	}
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationInstanceArn is required", http.StatusBadRequest), nil
	}
	instClass, _ := params["ReplicationInstanceClass"].(string)
	engineVersion, _ := params["EngineVersion"].(string)
	if err := p.store.ModifyReplicationInstance(id, instClass, engineVersion); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "instance not found", http.StatusNotFound), nil
	}
	ri, err := p.store.GetReplicationInstance(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "instance not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationInstance": instanceToMap(ri),
	})
}

func (p *DMSProvider) rebootReplicationInstance(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationInstanceArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationInstanceArn is required", http.StatusBadRequest), nil
	}
	ri, err := p.store.GetReplicationInstance(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "instance not found", http.StatusNotFound), nil
	}
	ri.Status = "rebooting"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationInstance": instanceToMap(ri),
	})
}

// --- Replication Task handlers ---

func (p *DMSProvider) createReplicationTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskIdentifier"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskIdentifier is required", http.StatusBadRequest), nil
	}
	instArn, _ := params["ReplicationInstanceArn"].(string)
	sourceArn, _ := params["SourceEndpointArn"].(string)
	targetArn, _ := params["TargetEndpointArn"].(string)
	migrationType, _ := params["MigrationType"].(string)
	if migrationType == "" {
		migrationType = "full-load"
	}
	tableMappings, _ := params["TableMappings"].(string)
	if tableMappings == "" {
		tableMappings = "{}"
	}
	taskSettings, _ := params["ReplicationTaskSettings"].(string)
	if taskSettings == "" {
		taskSettings = "{}"
	}
	arn := fmt.Sprintf("arn:aws:dms:%s:%s:task:%s", shared.DefaultRegion, defaultAccountID, id)
	rt := &ReplicationTask{
		ARN:           arn,
		Identifier:    id,
		InstanceARN:   instArn,
		SourceARN:     sourceArn,
		TargetARN:     targetArn,
		MigrationType: migrationType,
		TableMappings: tableMappings,
		TaskSettings:  taskSettings,
		Status:        "ready",
		AccountID:     defaultAccountID,
		CreatedAt:     time.Now(),
	}
	if err := p.store.CreateReplicationTask(rt); err != nil {
		return shared.JSONError("ResourceAlreadyExistsFault", "task already exists", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTask": taskToMap(rt),
	})
}

func (p *DMSProvider) deleteReplicationTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskArn is required", http.StatusBadRequest), nil
	}
	rt, err := p.store.GetReplicationTask(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteReplicationTask(id); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	rt.Status = "deleting"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTask": taskToMap(rt),
	})
}

func (p *DMSProvider) describeReplicationTasks(_ map[string]any) (*plugin.Response, error) {
	tasks, err := p.store.ListReplicationTasks()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		items = append(items, taskToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTasks": items,
	})
}

func (p *DMSProvider) modifyReplicationTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskArn is required", http.StatusBadRequest), nil
	}
	migrationType, _ := params["MigrationType"].(string)
	tableMappings, _ := params["TableMappings"].(string)
	taskSettings, _ := params["ReplicationTaskSettings"].(string)
	if err := p.store.ModifyReplicationTask(id, migrationType, tableMappings, taskSettings); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	rt, err := p.store.GetReplicationTask(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTask": taskToMap(rt),
	})
}

func (p *DMSProvider) startReplicationTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateTaskStatus(id, "running"); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	rt, _ := p.store.GetReplicationTask(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTask": taskToMap(rt),
	})
}

func (p *DMSProvider) stopReplicationTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateTaskStatus(id, "stopped"); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "task not found", http.StatusNotFound), nil
	}
	rt, _ := p.store.GetReplicationTask(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTask": taskToMap(rt),
	})
}

func (p *DMSProvider) reloadTables(params map[string]any) (*plugin.Response, error) {
	id, _ := params["ReplicationTaskArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "ReplicationTaskArn is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ReplicationTaskArn": id,
	})
}

// --- Endpoint handlers ---

func (p *DMSProvider) createEndpoint(params map[string]any) (*plugin.Response, error) {
	id, _ := params["EndpointIdentifier"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "EndpointIdentifier is required", http.StatusBadRequest), nil
	}
	endpointType, _ := params["EndpointType"].(string)
	if endpointType == "" {
		endpointType = "source"
	}
	engineName, _ := params["EngineName"].(string)
	if engineName == "" {
		engineName = "mysql"
	}
	serverName, _ := params["ServerName"].(string)
	databaseName, _ := params["DatabaseName"].(string)
	username, _ := params["Username"].(string)
	port := 3306
	if v, ok := params["Port"].(float64); ok {
		port = int(v)
	}
	sslMode, _ := params["SslMode"].(string)
	if sslMode == "" {
		sslMode = "none"
	}
	arn := fmt.Sprintf("arn:aws:dms:%s:%s:endpoint:%s", shared.DefaultRegion, defaultAccountID, id)
	ep := &DMSEndpoint{
		ARN:          arn,
		Identifier:   id,
		EndpointType: endpointType,
		EngineName:   engineName,
		ServerName:   serverName,
		Port:         port,
		DatabaseName: databaseName,
		Username:     username,
		Status:       "active",
		SSLMode:      sslMode,
		AccountID:    defaultAccountID,
		CreatedAt:    time.Now(),
	}
	if err := p.store.CreateEndpoint(ep); err != nil {
		return shared.JSONError("ResourceAlreadyExistsFault", "endpoint already exists", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Endpoint": endpointToMap(ep),
	})
}

func (p *DMSProvider) deleteEndpoint(params map[string]any) (*plugin.Response, error) {
	id, _ := params["EndpointArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "EndpointArn is required", http.StatusBadRequest), nil
	}
	ep, err := p.store.GetEndpoint(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "endpoint not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteEndpoint(id); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "endpoint not found", http.StatusNotFound), nil
	}
	ep.Status = "deleted"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Endpoint": endpointToMap(ep),
	})
}

func (p *DMSProvider) describeEndpoints(_ map[string]any) (*plugin.Response, error) {
	endpoints, err := p.store.ListEndpoints()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(endpoints))
	for _, e := range endpoints {
		items = append(items, endpointToMap(&e))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Endpoints": items,
	})
}

func (p *DMSProvider) modifyEndpoint(params map[string]any) (*plugin.Response, error) {
	id, _ := params["EndpointArn"].(string)
	if id == "" {
		return shared.JSONError("InvalidParameterValueException", "EndpointArn is required", http.StatusBadRequest), nil
	}
	engineName, _ := params["EngineName"].(string)
	serverName, _ := params["ServerName"].(string)
	databaseName, _ := params["DatabaseName"].(string)
	username, _ := params["Username"].(string)
	port := 0
	if v, ok := params["Port"].(float64); ok {
		port = int(v)
	}
	if err := p.store.ModifyEndpoint(id, engineName, serverName, databaseName, username, port); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "endpoint not found", http.StatusNotFound), nil
	}
	ep, _ := p.store.GetEndpoint(id)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Endpoint": endpointToMap(ep),
	})
}

func (p *DMSProvider) testConnection(params map[string]any) (*plugin.Response, error) {
	instArn, _ := params["ReplicationInstanceArn"].(string)
	endpointArn, _ := params["EndpointArn"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Connection": map[string]any{
			"ReplicationInstanceArn": instArn,
			"EndpointArn":            endpointArn,
			"Status":                 "successful",
			"LastFailureMessage":     "",
		},
	})
}

func (p *DMSProvider) describeConnections(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Connections": []any{},
	})
}

// --- Event Subscription handlers ---

func (p *DMSProvider) createEventSubscription(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubscriptionName"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "SubscriptionName is required", http.StatusBadRequest), nil
	}
	snsArn, _ := params["SnsTopicArn"].(string)
	sourceType, _ := params["SourceType"].(string)
	enabled := true
	if v, ok := params["Enabled"].(bool); ok {
		enabled = v
	}
	categoriesJSON := "[]"
	if v, ok := params["EventCategories"]; ok {
		b, _ := json.Marshal(v)
		categoriesJSON = string(b)
	}
	es := &EventSubscription{
		Name:            name,
		AccountID:       defaultAccountID,
		SnsTopicARN:     snsArn,
		SourceType:      sourceType,
		EventCategories: categoriesJSON,
		Enabled:         enabled,
		Status:          "active",
		CreatedAt:       time.Now(),
	}
	if err := p.store.CreateEventSubscription(es); err != nil {
		return shared.JSONError("ResourceAlreadyExistsFault", "subscription already exists", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EventSubscription": eventSubToMap(es),
	})
}

func (p *DMSProvider) describeEventSubscriptions(_ map[string]any) (*plugin.Response, error) {
	subs, err := p.store.ListEventSubscriptions(defaultAccountID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(subs))
	for _, s := range subs {
		items = append(items, eventSubToMap(&s))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EventSubscriptionsList": items,
	})
}

func (p *DMSProvider) deleteEventSubscription(params map[string]any) (*plugin.Response, error) {
	name, _ := params["SubscriptionName"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "SubscriptionName is required", http.StatusBadRequest), nil
	}
	es, err := p.store.GetEventSubscription(name, defaultAccountID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundFault", "subscription not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteEventSubscription(name, defaultAccountID); err != nil {
		return shared.JSONError("ResourceNotFoundFault", "subscription not found", http.StatusNotFound), nil
	}
	es.Status = "deleted"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"EventSubscription": eventSubToMap(es),
	})
}

// --- Tag handlers ---

func (p *DMSProvider) addTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	tags := parseDMSTags(rawTags)
	if err := p.store.PutTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *DMSProvider) removeTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.DeleteTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *DMSProvider) listTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.GetTags(arn)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TagList": list,
	})
}

// --- helpers ---

func instanceToMap(r *ReplicationInstance) map[string]any {
	return map[string]any{
		"ReplicationInstanceArn":        r.ARN,
		"ReplicationInstanceIdentifier": r.Identifier,
		"ReplicationInstanceClass":      r.InstanceClass,
		"EngineVersion":                 r.EngineVersion,
		"ReplicationInstanceStatus":     r.Status,
		"Status":                        r.Status,
		"MultiAZ":                       r.MultiAZ,
		"AllocatedStorage":              r.AllocatedStorage,
		"PubliclyAccessible":            r.PubliclyAccessible,
		"AutoMinorVersionUpgrade":       true,
		"PreferredMaintenanceWindow":    "sun:05:00-sun:06:00",
		"StorageType":                   "gp2",
		"InstanceCreateTime":            r.CreatedAt.Unix(),
	}
}

func taskToMap(t *ReplicationTask) map[string]any {
	var tableMappings, taskSettings any
	_ = json.Unmarshal([]byte(t.TableMappings), &tableMappings)
	_ = json.Unmarshal([]byte(t.TaskSettings), &taskSettings)
	return map[string]any{
		"ReplicationTaskArn":          t.ARN,
		"ReplicationTaskIdentifier":   t.Identifier,
		"ReplicationInstanceArn":      t.InstanceARN,
		"SourceEndpointArn":           t.SourceARN,
		"TargetEndpointArn":           t.TargetARN,
		"MigrationType":               t.MigrationType,
		"TableMappings":               tableMappings,
		"ReplicationTaskSettings":     taskSettings,
		"Status":                      t.Status,
		"TaskStatus":                  t.Status,
		"ReplicationTaskCreationDate": t.CreatedAt.Unix(),
	}
}

func endpointToMap(e *DMSEndpoint) map[string]any {
	return map[string]any{
		"EndpointArn":        e.ARN,
		"EndpointIdentifier": e.Identifier,
		"EndpointType":       e.EndpointType,
		"EngineName":         e.EngineName,
		"ServerName":         e.ServerName,
		"Port":               e.Port,
		"DatabaseName":       e.DatabaseName,
		"Username":           e.Username,
		"Status":             e.Status,
		"SslMode":            e.SSLMode,
	}
}

func eventSubToMap(es *EventSubscription) map[string]any {
	var categories any
	_ = json.Unmarshal([]byte(es.EventCategories), &categories)
	return map[string]any{
		"CustomerAwsId":            es.AccountID,
		"CustSubscriptionId":       es.Name,
		"SnsTopicArn":              es.SnsTopicARN,
		"SourceType":               es.SourceType,
		"EventCategoriesList":      categories,
		"Enabled":                  es.Enabled,
		"Status":                   es.Status,
		"SubscriptionCreationTime": es.CreatedAt.Format(time.RFC3339),
	}
}

func parseDMSTags(raw []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range raw {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		k, _ := tm["Key"].(string)
		v, _ := tm["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func getString(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}

func init() {
	plugin.DefaultRegistry.Register("dms", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &DMSProvider{}
	})
}
