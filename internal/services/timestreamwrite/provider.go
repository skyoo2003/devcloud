// SPDX-License-Identifier: Apache-2.0

// internal/services/timestreamwrite/provider.go
package timestreamwrite

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
)

// Provider implements the Timestream_20181101 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "timestreamwrite" }
func (p *Provider) ServiceName() string           { return "Timestream_20181101" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "timestreamwrite"))
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
		return json10Err("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return json10Err("SerializationException", "invalid JSON", http.StatusBadRequest), nil
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
	case "CreateDatabase":
		return p.createDatabase(params)
	case "DescribeDatabase":
		return p.describeDatabase(params)
	case "ListDatabases":
		return p.listDatabases(params)
	case "UpdateDatabase":
		return p.updateDatabase(params)
	case "DeleteDatabase":
		return p.deleteDatabase(params)
	case "CreateTable":
		return p.createTable(params)
	case "DescribeTable":
		return p.describeTable(params)
	case "ListTables":
		return p.listTables(params)
	case "UpdateTable":
		return p.updateTable(params)
	case "DeleteTable":
		return p.deleteTable(params)
	case "WriteRecords":
		return p.writeRecords(params)
	case "CreateBatchLoadTask":
		return p.createBatchLoadTask(params)
	case "DescribeBatchLoadTask":
		return p.describeBatchLoadTask(params)
	case "ListBatchLoadTasks":
		return p.listBatchLoadTasks(params)
	case "ResumeBatchLoadTask":
		return p.resumeBatchLoadTask(params)
	case "DescribeEndpoints":
		return p.describeEndpoints()
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return json10Err("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	dbs, err := p.store.ListDatabases()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(dbs))
	for _, db := range dbs {
		res = append(res, plugin.Resource{Type: "database", ID: db.ARN, Name: db.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Database handlers ----

func (p *Provider) createDatabase(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DatabaseName"].(string)
	if name == "" {
		return json10Err("ValidationException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	kmsKeyID, _ := params["KmsKeyId"].(string)
	db, err := p.store.CreateDatabase(name, kmsKeyID)
	if err != nil {
		if errors.Is(err, errDatabaseExists) {
			return json10Err("ConflictException", "database already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	// Handle initial tags if provided.
	if tags, ok := params["Tags"].([]any); ok {
		tagMap := tagsFromList(tags)
		if len(tagMap) > 0 {
			_ = p.store.AddTags(db.ARN, tagMap)
		}
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Database": dbRowToMap(db),
	})
}

func (p *Provider) describeDatabase(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DatabaseName"].(string)
	if name == "" {
		return json10Err("ValidationException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	db, err := p.store.GetDatabase(name)
	if err != nil {
		if errors.Is(err, errDatabaseNotFound) {
			return json10Err("ResourceNotFoundException", "database not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Database": dbRowToMap(db),
	})
}

func (p *Provider) listDatabases(params map[string]any) (*plugin.Response, error) {
	dbs, err := p.store.ListDatabases()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(dbs))
	for _, db := range dbs {
		list = append(list, dbRowToMap(db))
	}
	resp := map[string]any{"Databases": list}
	return json10Resp(http.StatusOK, resp)
}

func (p *Provider) updateDatabase(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DatabaseName"].(string)
	if name == "" {
		return json10Err("ValidationException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	kmsKeyID, _ := params["KmsKeyId"].(string)
	db, err := p.store.UpdateDatabase(name, kmsKeyID)
	if err != nil {
		if errors.Is(err, errDatabaseNotFound) {
			return json10Err("ResourceNotFoundException", "database not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Database": dbRowToMap(db),
	})
}

func (p *Provider) deleteDatabase(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DatabaseName"].(string)
	if name == "" {
		return json10Err("ValidationException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDatabase(name); err != nil {
		if errors.Is(err, errDatabaseNotFound) {
			return json10Err("ResourceNotFoundException", "database not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- Table handlers ----

func (p *Provider) createTable(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	// Verify database exists.
	if _, err := p.store.GetDatabase(dbName); err != nil {
		if errors.Is(err, errDatabaseNotFound) {
			return json10Err("ResourceNotFoundException", "database not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	t, err := p.store.CreateTable(dbName, tableName)
	if err != nil {
		if errors.Is(err, errTableExists) {
			return json10Err("ConflictException", "table already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if tags, ok := params["Tags"].([]any); ok {
		tagMap := tagsFromList(tags)
		if len(tagMap) > 0 {
			_ = p.store.AddTags(t.ARN, tagMap)
		}
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Table": tableRowToMap(t),
	})
}

func (p *Provider) describeTable(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetTable(dbName, tableName)
	if err != nil {
		if errors.Is(err, errTableNotFound) {
			return json10Err("ResourceNotFoundException", "table not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Table": tableRowToMap(t),
	})
}

func (p *Provider) listTables(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tables, err := p.store.ListTables(dbName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(tables))
	for _, t := range tables {
		list = append(list, tableRowToMap(t))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Tables": list,
	})
}

func (p *Provider) updateTable(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	retentionMem := "6"
	retentionMag := "73000"
	if rp, ok := params["RetentionProperties"].(map[string]any); ok {
		if v, ok := rp["MemoryStoreRetentionPeriodInHours"].(float64); ok {
			retentionMem = fmt.Sprintf("%d", int64(v))
		}
		if v, ok := rp["MagneticStoreRetentionPeriodInDays"].(float64); ok {
			retentionMag = fmt.Sprintf("%d", int64(v))
		}
	}
	t, err := p.store.UpdateTable(dbName, tableName, retentionMem, retentionMag)
	if err != nil {
		if errors.Is(err, errTableNotFound) {
			return json10Err("ResourceNotFoundException", "table not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Table": tableRowToMap(t),
	})
}

func (p *Provider) deleteTable(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTable(dbName, tableName); err != nil {
		if errors.Is(err, errTableNotFound) {
			return json10Err("ResourceNotFoundException", "table not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- WriteRecords handler ----

func (p *Provider) writeRecords(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	// No-op: just return success with RecordsIngested.
	var total int32
	if records, ok := params["Records"].([]any); ok {
		total = int32(len(records))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"RecordsIngested": map[string]any{
			"Total":         total,
			"MemoryStore":   total,
			"MagneticStore": 0,
		},
	})
}

// ---- BatchLoadTask handlers ----

func (p *Provider) createBatchLoadTask(params map[string]any) (*plugin.Response, error) {
	dbName, _ := params["TargetDatabaseName"].(string)
	tableName, _ := params["TargetTableName"].(string)
	if dbName == "" || tableName == "" {
		return json10Err("ValidationException", "TargetDatabaseName and TargetTableName are required", http.StatusBadRequest), nil
	}
	dataSource := "{}"
	if ds, ok := params["DataSourceConfiguration"]; ok {
		if b, err := json.Marshal(ds); err == nil {
			dataSource = string(b)
		}
	}
	task, err := p.store.CreateBatchLoadTask(dbName, tableName, dataSource)
	if err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"TaskId": task.ID,
	})
}

func (p *Provider) describeBatchLoadTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["TaskId"].(string)
	if id == "" {
		return json10Err("ValidationException", "TaskId is required", http.StatusBadRequest), nil
	}
	task, err := p.store.GetBatchLoadTask(id)
	if err != nil {
		if errors.Is(err, errBatchLoadNotFound) {
			return json10Err("ResourceNotFoundException", "batch load task not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{
		"BatchLoadTaskDescription": map[string]any{
			"TaskId":             task.ID,
			"TargetDatabaseName": task.DatabaseName,
			"TargetTableName":    task.TableName,
			"TaskStatus":         task.Status,
			"CreationTime":       task.CreatedAt.Unix(),
		},
	})
}

func (p *Provider) listBatchLoadTasks(params map[string]any) (*plugin.Response, error) {
	statusFilter, _ := params["TaskStatus"].(string)
	tasks, err := p.store.ListBatchLoadTasks(statusFilter)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(tasks))
	for _, t := range tasks {
		list = append(list, batchTaskToMap(t))
	}
	return json10Resp(http.StatusOK, map[string]any{
		"BatchLoadTasks": list,
	})
}

func (p *Provider) resumeBatchLoadTask(params map[string]any) (*plugin.Response, error) {
	id, _ := params["TaskId"].(string)
	if id == "" {
		return json10Err("ValidationException", "TaskId is required", http.StatusBadRequest), nil
	}
	if err := p.store.ResumeBatchLoadTask(id); err != nil {
		if errors.Is(err, errBatchLoadNotFound) {
			return json10Err("ResourceNotFoundException", "batch load task not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

// ---- DescribeEndpoints ----

func (p *Provider) describeEndpoints() (*plugin.Response, error) {
	return json10Resp(http.StatusOK, map[string]any{
		"Endpoints": []any{
			map[string]any{
				"Address":              "ingest.timestream.us-east-1.amazonaws.com",
				"CachePeriodInMinutes": 1440,
			},
		},
	})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return json10Err("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, _ := params["Tags"].([]any)
	tagMap := tagsFromList(tags)
	if err := p.store.AddTags(arn, tagMap); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return json10Err("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	var keys []string
	if ks, ok := params["TagKeys"].([]any); ok {
		for _, k := range ks {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
	}
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return json10Resp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return json10Err("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tagMap, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(tagMap))
	for k, v := range tagMap {
		list = append(list, map[string]any{"Key": k, "Value": v})
	}
	return json10Resp(http.StatusOK, map[string]any{
		"Tags": list,
	})
}

// ---- JSON 1.0 helpers ----

func json10Resp(status int, v any) (*plugin.Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}, nil
}

func json10Err(code, message string, status int) *plugin.Response {
	b, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, Body: b, ContentType: "application/x-amz-json-1.0"}
}

// tagsFromList converts a JSON tags list [{Key: ..., Value: ...}] to a map.
func tagsFromList(tags []any) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		if entry, ok := t.(map[string]any); ok {
			k, _ := entry["Key"].(string)
			v, _ := entry["Value"].(string)
			if k != "" {
				m[k] = v
			}
		}
	}
	return m
}
