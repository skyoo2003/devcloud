// SPDX-License-Identifier: Apache-2.0

// internal/services/glue/provider.go
package glue

import (
	"context"
	"encoding/json"
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

func (p *Provider) ServiceID() string             { return "glue" }
func (p *Provider) ServiceName() string           { return "Glue" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "glue"))
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
	// ---- Database ----
	case "CreateDatabase":
		return p.createDatabase(params)
	case "GetDatabase":
		return p.getDatabase(params)
	case "GetDatabases":
		return p.getDatabases(params)
	case "UpdateDatabase":
		return p.updateDatabase(params)
	case "DeleteDatabase":
		return p.deleteDatabase(params)

	// ---- Table ----
	case "CreateTable":
		return p.createTable(params)
	case "GetTable":
		return p.getTable(params)
	case "GetTables":
		return p.getTables(params)
	case "UpdateTable":
		return p.updateTable(params)
	case "DeleteTable":
		return p.deleteTable(params)
	case "BatchDeleteTable":
		return p.batchDeleteTable(params)
	case "GetTableVersion":
		return p.getTableVersion(params)
	case "GetTableVersions":
		return p.getTableVersions(params)
	case "DeleteTableVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "BatchDeleteTableVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": []any{}})

	// ---- Partition ----
	case "CreatePartition":
		return p.createPartition(params)
	case "GetPartition":
		return p.getPartition(params)
	case "GetPartitions":
		return p.getPartitions(params)
	case "UpdatePartition":
		return p.updatePartition(params)
	case "BatchCreatePartition":
		return p.batchCreatePartition(params)
	case "BatchDeletePartition":
		return p.batchDeletePartition(params)
	case "BatchGetPartition":
		return p.batchGetPartition(params)
	case "BatchUpdatePartition":
		return p.batchUpdatePartition(params)

	// ---- Crawler ----
	case "CreateCrawler":
		return p.createCrawler(params)
	case "GetCrawler":
		return p.getCrawler(params)
	case "GetCrawlers":
		return p.getCrawlers(params)
	case "UpdateCrawler":
		return p.updateCrawler(params)
	case "DeleteCrawler":
		return p.deleteCrawler(params)
	case "StartCrawler":
		return p.startCrawler(params)
	case "StopCrawler":
		return p.stopCrawler(params)
	case "BatchGetCrawlers":
		return p.batchGetCrawlers(params)
	case "GetCrawlerMetrics":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrawlerMetricsList": []any{}})

	// ---- Job ----
	case "CreateJob":
		return p.createJob(params)
	case "GetJob":
		return p.getJob(params)
	case "GetJobs":
		return p.getJobs(params)
	case "UpdateJob":
		return p.updateJob(params)
	case "DeleteJob":
		return p.deleteJob(params)
	case "BatchGetJobs":
		return p.batchGetJobs(params)
	case "StartJobRun":
		return p.startJobRun(params)
	case "GetJobRun":
		return p.getJobRun(params)
	case "GetJobRuns":
		return p.getJobRuns(params)
	case "BatchStopJobRun":
		return p.batchStopJobRun(params)
	case "GetJobBookmark":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobBookmarkEntry": map[string]any{}})
	case "ResetJobBookmark":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobBookmarkEntry": map[string]any{}})

	// ---- Connection ----
	case "CreateConnection":
		return p.createConnection(params)
	case "GetConnection":
		return p.getConnection(params)
	case "GetConnections":
		return p.getConnections(params)
	case "UpdateConnection":
		return p.updateConnection(params)
	case "DeleteConnection":
		return p.deleteConnection(params)
	case "BatchDeleteConnection":
		return p.batchDeleteConnection(params)

	// ---- Trigger ----
	case "CreateTrigger":
		return p.createTrigger(params)
	case "GetTrigger":
		return p.getTrigger(params)
	case "GetTriggers":
		return p.getTriggers(params)
	case "UpdateTrigger":
		return p.updateTrigger(params)
	case "DeleteTrigger":
		return p.deleteTrigger(params)
	case "StartTrigger":
		return p.startTrigger(params)
	case "StopTrigger":
		return p.stopTrigger(params)
	case "BatchGetTriggers":
		return p.batchGetTriggers(params)

	// ---- SecurityConfiguration ----
	case "CreateSecurityConfiguration":
		return p.createSecurityConfiguration(params)
	case "GetSecurityConfiguration":
		return p.getSecurityConfiguration(params)
	case "GetSecurityConfigurations":
		return p.getSecurityConfigurations(params)
	case "DeleteSecurityConfiguration":
		return p.deleteSecurityConfiguration(params)

	// ---- Tags ----
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "GetTags":
		return p.getTags(params)

	// ---- ~200 stub operations ----
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	dbs, err := p.store.ListDatabases(shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(dbs))
	for _, db := range dbs {
		res = append(res, plugin.Resource{Type: "database", ID: db.Name, Name: db.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- helpers ----

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return def
}

func marshalParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "{}"
}

func marshalParamArray(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "[]"
}

func catalogID(params map[string]any) string {
	if id := strParam(params, "CatalogId"); id != "" {
		return id
	}
	return shared.DefaultAccountID
}

func sqliteIsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func stringsParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ---- Database handlers ----

func (p *Provider) createDatabase(params map[string]any) (*plugin.Response, error) {
	input, _ := params["DatabaseInput"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidInputException", "DatabaseInput is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	description := strParam(input, "Description")
	locationURI := strParam(input, "LocationUri")
	parameters := marshalParam(input, "Parameters")
	_, err := p.store.CreateDatabase(catID, name, description, locationURI, parameters)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Database already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getDatabase(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	db, err := p.store.GetDatabase(catID, name)
	if err != nil {
		if err == errDatabaseNotFound {
			return shared.JSONError("EntityNotFoundException", "Database not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Database": databaseToMap(db)})
}

func (p *Provider) getDatabases(params map[string]any) (*plugin.Response, error) {
	catID := catalogID(params)
	dbs, err := p.store.ListDatabases(catID)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(dbs))
	for _, db := range dbs {
		list = append(list, databaseToMap(&db))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DatabaseList": list})
}

func (p *Provider) updateDatabase(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	input, _ := params["DatabaseInput"].(map[string]any)
	if input == nil {
		input = map[string]any{}
	}
	description := strParam(input, "Description")
	locationURI := strParam(input, "LocationUri")
	parameters := marshalParam(input, "Parameters")
	if err := p.store.UpdateDatabase(catID, name, description, locationURI, parameters); err != nil {
		if err == errDatabaseNotFound {
			return shared.JSONError("EntityNotFoundException", "Database not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteDatabase(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	if err := p.store.DeleteDatabase(catID, name); err != nil {
		if err == errDatabaseNotFound {
			return shared.JSONError("EntityNotFoundException", "Database not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func databaseToMap(db *Database) map[string]any {
	var params any
	json.Unmarshal([]byte(db.Parameters), &params)
	return map[string]any{
		"Name":        db.Name,
		"Description": db.Description,
		"LocationUri": db.LocationURI,
		"Parameters":  params,
		"CreateTime":  db.CreatedAt.Unix(),
		"CatalogId":   db.CatalogID,
	}
}

// ---- Table handlers ----

func (p *Provider) createTable(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	if dbName == "" {
		return shared.JSONError("InvalidInputException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	input, _ := params["TableInput"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidInputException", "TableInput is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	description := strParam(input, "Description")
	tableType := strParam(input, "TableType")
	if tableType == "" {
		tableType = "EXTERNAL_TABLE"
	}
	parameters := marshalParam(input, "Parameters")
	storageDesc := marshalParam(input, "StorageDescriptor")
	columns := marshalParamArray(input, "PartitionKeys")
	storageColumns := "[]"
	if sd, ok := input["StorageDescriptor"].(map[string]any); ok {
		storageColumns = marshalParamArray(sd, "Columns")
	}
	_, err := p.store.CreateTable(catID, dbName, name, description, tableType, parameters, storageDesc, storageColumns, columns)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Table already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getTable(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	name := strParam(params, "Name")
	if dbName == "" || name == "" {
		return shared.JSONError("InvalidInputException", "DatabaseName and Name are required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	t, err := p.store.GetTable(catID, dbName, name)
	if err != nil {
		if err == errTableNotFound {
			return shared.JSONError("EntityNotFoundException", "Table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Table": tableToMap(t)})
}

func (p *Provider) getTables(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	if dbName == "" {
		return shared.JSONError("InvalidInputException", "DatabaseName is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	tables, err := p.store.ListTables(catID, dbName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(tables))
	for _, t := range tables {
		list = append(list, tableToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"TableList": list})
}

func (p *Provider) updateTable(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	input, _ := params["TableInput"].(map[string]any)
	if dbName == "" || input == nil {
		return shared.JSONError("InvalidInputException", "DatabaseName and TableInput are required", http.StatusBadRequest), nil
	}
	name := strParam(input, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	description := strParam(input, "Description")
	tableType := strParam(input, "TableType")
	if tableType == "" {
		tableType = "EXTERNAL_TABLE"
	}
	parameters := marshalParam(input, "Parameters")
	storageDesc := marshalParam(input, "StorageDescriptor")
	columns := "[]"
	if sd, ok := input["StorageDescriptor"].(map[string]any); ok {
		columns = marshalParamArray(sd, "Columns")
	}
	partitionKeys := marshalParamArray(input, "PartitionKeys")
	if err := p.store.UpdateTable(catID, dbName, name, description, tableType, parameters, storageDesc, columns, partitionKeys); err != nil {
		if err == errTableNotFound {
			return shared.JSONError("EntityNotFoundException", "Table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteTable(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	name := strParam(params, "Name")
	if dbName == "" || name == "" {
		return shared.JSONError("InvalidInputException", "DatabaseName and Name are required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	if err := p.store.DeleteTable(catID, dbName, name); err != nil {
		if err == errTableNotFound {
			return shared.JSONError("EntityNotFoundException", "Table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchDeleteTable(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	names := stringsParam(params, "TablesToDelete")
	catID := catalogID(params)
	var errs []any
	for _, name := range names {
		if err := p.store.DeleteTable(catID, dbName, name); err != nil {
			errs = append(errs, map[string]any{"TableName": name, "ErrorDetail": map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": err.Error()}})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": errs})
}

func (p *Provider) getTableVersion(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	t, err := p.store.GetTable(catalogID(params), dbName, tableName)
	if err != nil {
		if err == errTableNotFound {
			return shared.JSONError("EntityNotFoundException", "Table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TableVersion": map[string]any{
			"Table":     tableToMap(t),
			"VersionId": "0",
		},
	})
}

func (p *Provider) getTableVersions(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	t, err := p.store.GetTable(catalogID(params), dbName, tableName)
	if err != nil {
		if err == errTableNotFound {
			return shared.JSONError("EntityNotFoundException", "Table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TableVersions": []any{
			map[string]any{"Table": tableToMap(t), "VersionId": "0"},
		},
	})
}

func tableToMap(t *Table) map[string]any {
	var params, sd, cols, pk any
	json.Unmarshal([]byte(t.Parameters), &params)
	json.Unmarshal([]byte(t.StorageDesc), &sd)
	json.Unmarshal([]byte(t.Columns), &cols)
	json.Unmarshal([]byte(t.PartitionKeys), &pk)
	return map[string]any{
		"Name":              t.Name,
		"DatabaseName":      t.DatabaseName,
		"Description":       t.Description,
		"TableType":         t.TableType,
		"Parameters":        params,
		"StorageDescriptor": sd,
		"PartitionKeys":     pk,
		"CreateTime":        t.CreatedAt.Unix(),
		"UpdateTime":        t.UpdatedAt.Unix(),
		"CatalogId":         t.CatalogID,
	}
}

// ---- Partition handlers ----

func (p *Provider) createPartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	if dbName == "" || tableName == "" {
		return shared.JSONError("InvalidInputException", "DatabaseName and TableName are required", http.StatusBadRequest), nil
	}
	input, _ := params["PartitionInput"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidInputException", "PartitionInput is required", http.StatusBadRequest), nil
	}
	values := stringsParam(input, "Values")
	catID := catalogID(params)
	parameters := marshalParam(input, "Parameters")
	storageDesc := marshalParam(input, "StorageDescriptor")
	_, err := p.store.CreatePartition(catID, dbName, tableName, values, parameters, storageDesc)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Partition already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getPartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	values := stringsParam(params, "PartitionValues")
	catID := catalogID(params)
	part, err := p.store.GetPartition(catID, dbName, tableName, values)
	if err != nil {
		if err == errPartitionNotFound {
			return shared.JSONError("EntityNotFoundException", "Partition not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Partition": partitionToMap(part)})
}

func (p *Provider) getPartitions(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	catID := catalogID(params)
	parts, err := p.store.ListPartitions(catID, dbName, tableName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(parts))
	for _, part := range parts {
		list = append(list, partitionToMap(&part))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Partitions": list})
}

func (p *Provider) updatePartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	values := stringsParam(params, "PartitionValueList")
	input, _ := params["PartitionInput"].(map[string]any)
	if input == nil {
		input = map[string]any{}
	}
	catID := catalogID(params)
	parameters := marshalParam(input, "Parameters")
	storageDesc := marshalParam(input, "StorageDescriptor")
	if err := p.store.UpdatePartition(catID, dbName, tableName, values, parameters, storageDesc); err != nil {
		if err == errPartitionNotFound {
			return shared.JSONError("EntityNotFoundException", "Partition not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchCreatePartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	catID := catalogID(params)
	inputs, _ := params["PartitionInputList"].([]any)
	var errs []any
	for _, item := range inputs {
		input, _ := item.(map[string]any)
		if input == nil {
			continue
		}
		values := stringsParam(input, "Values")
		parameters := marshalParam(input, "Parameters")
		storageDesc := marshalParam(input, "StorageDescriptor")
		if _, err := p.store.CreatePartition(catID, dbName, tableName, values, parameters, storageDesc); err != nil {
			errs = append(errs, map[string]any{
				"PartitionValues": values,
				"ErrorDetail":     map[string]any{"ErrorCode": "AlreadyExistsException", "ErrorMessage": err.Error()},
			})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": errs})
}

func (p *Provider) batchDeletePartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	catID := catalogID(params)
	parts, _ := params["PartitionsToDelete"].([]any)
	var errs []any
	for _, item := range parts {
		pv, _ := item.(map[string]any)
		if pv == nil {
			continue
		}
		values := stringsParam(pv, "Values")
		if err := p.store.DeletePartition(catID, dbName, tableName, values); err != nil {
			errs = append(errs, map[string]any{
				"PartitionValues": values,
				"ErrorDetail":     map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": err.Error()},
			})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": errs})
}

func (p *Provider) batchGetPartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	catID := catalogID(params)
	partitionsToGet, _ := params["PartitionsToGet"].([]any)
	var found []any
	var unfound []any
	for _, item := range partitionsToGet {
		pv, _ := item.(map[string]any)
		if pv == nil {
			continue
		}
		values := stringsParam(pv, "Values")
		part, err := p.store.GetPartition(catID, dbName, tableName, values)
		if err != nil {
			unfound = append(unfound, map[string]any{"Values": values})
		} else {
			found = append(found, partitionToMap(part))
		}
	}
	if found == nil {
		found = []any{}
	}
	if unfound == nil {
		unfound = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Partitions": found, "UnprocessedKeys": unfound})
}

func (p *Provider) batchUpdatePartition(params map[string]any) (*plugin.Response, error) {
	dbName := strParam(params, "DatabaseName")
	tableName := strParam(params, "TableName")
	catID := catalogID(params)
	entries, _ := params["Entries"].([]any)
	var errs []any
	for _, item := range entries {
		entry, _ := item.(map[string]any)
		if entry == nil {
			continue
		}
		values := stringsParam(entry, "PartitionValueList")
		input, _ := entry["PartitionInput"].(map[string]any)
		if input == nil {
			input = map[string]any{}
		}
		parameters := marshalParam(input, "Parameters")
		storageDesc := marshalParam(input, "StorageDescriptor")
		if err := p.store.UpdatePartition(catID, dbName, tableName, values, parameters, storageDesc); err != nil {
			errs = append(errs, map[string]any{
				"PartitionValueList": values,
				"ErrorDetail":        map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": err.Error()},
			})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": errs})
}

func partitionToMap(p *Partition) map[string]any {
	var params, sd any
	json.Unmarshal([]byte(p.Parameters), &params)
	json.Unmarshal([]byte(p.StorageDesc), &sd)
	values := strings.Split(p.ValuesKey, "\x00")
	if len(values) == 1 && values[0] == "" {
		values = []string{}
	}
	return map[string]any{
		"Values":            values,
		"DatabaseName":      p.DatabaseName,
		"TableName":         p.TableName,
		"Parameters":        params,
		"StorageDescriptor": sd,
		"CreationTime":      p.CreatedAt.Unix(),
		"CatalogId":         p.CatalogID,
	}
}

// ---- Crawler handlers ----

func (p *Provider) createCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	role := strParam(params, "Role")
	dbName := strParam(params, "DatabaseName")
	targets := marshalParam(params, "Targets")
	schedule := ""
	if s, ok := params["Schedule"].(map[string]any); ok {
		schedule = strParam(s, "ScheduleExpression")
	} else if s, ok := params["Schedule"].(string); ok {
		schedule = s
	}
	config := marshalParam(params, "Configuration")
	_, err := p.store.CreateCrawler(name, role, dbName, targets, schedule, config)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Crawler already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	c, err := p.store.GetCrawler(name)
	if err != nil {
		if err == errCrawlerNotFound {
			return shared.JSONError("EntityNotFoundException", "Crawler not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Crawler": crawlerToMap(c)})
}

func (p *Provider) getCrawlers(params map[string]any) (*plugin.Response, error) {
	crawlers, err := p.store.ListCrawlers()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(crawlers))
	for _, c := range crawlers {
		list = append(list, crawlerToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Crawlers": list})
}

func (p *Provider) updateCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	role := strParam(params, "Role")
	dbName := strParam(params, "DatabaseName")
	targets := marshalParam(params, "Targets")
	schedule := ""
	if s, ok := params["Schedule"].(string); ok {
		schedule = s
	}
	config := marshalParam(params, "Configuration")
	if err := p.store.UpdateCrawler(name, role, dbName, targets, schedule, config); err != nil {
		if err == errCrawlerNotFound {
			return shared.JSONError("EntityNotFoundException", "Crawler not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.DeleteCrawler(name); err != nil {
		if err == errCrawlerNotFound {
			return shared.JSONError("EntityNotFoundException", "Crawler not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.UpdateCrawlerStatus(name, "RUNNING"); err != nil {
		if err == errCrawlerNotFound {
			return shared.JSONError("EntityNotFoundException", "Crawler not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopCrawler(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.UpdateCrawlerStatus(name, "READY"); err != nil {
		if err == errCrawlerNotFound {
			return shared.JSONError("EntityNotFoundException", "Crawler not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchGetCrawlers(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "CrawlerNames")
	var found []any
	var missing []string
	for _, name := range names {
		c, err := p.store.GetCrawler(name)
		if err != nil {
			missing = append(missing, name)
		} else {
			found = append(found, crawlerToMap(c))
		}
	}
	if found == nil {
		found = []any{}
	}
	if missing == nil {
		missing = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Crawlers": found, "CrawlersNotFound": missing})
}

func crawlerToMap(c *Crawler) map[string]any {
	var targets, config any
	json.Unmarshal([]byte(c.Targets), &targets)
	json.Unmarshal([]byte(c.Config), &config)
	return map[string]any{
		"Name":          c.Name,
		"Role":          c.Role,
		"DatabaseName":  c.DatabaseName,
		"Targets":       targets,
		"State":         c.Status,
		"Schedule":      map[string]any{"ScheduleExpression": c.Schedule, "State": "NOT_SCHEDULED"},
		"Configuration": config,
		"CreationTime":  c.CreatedAt.Unix(),
		"LastUpdated":   c.UpdatedAt.Unix(),
	}
}

// ---- Job handlers ----

func (p *Provider) createJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	role := strParam(params, "Role")
	command := marshalParam(params, "Command")
	maxRetries := intParam(params, "MaxRetries", 0)
	timeout := intParam(params, "Timeout", 2880)
	config := marshalParam(params, "DefaultArguments")
	_, err := p.store.CreateJob(name, role, command, maxRetries, timeout, config)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("IdempotentParameterMismatchException", "Job already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) getJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "JobName")
	j, err := p.store.GetJob(name)
	if err != nil {
		if err == errJobNotFound {
			return shared.JSONError("EntityNotFoundException", "Job not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Job": jobToMap(j)})
}

func (p *Provider) getJobs(params map[string]any) (*plugin.Response, error) {
	jobs, err := p.store.ListJobs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(jobs))
	for _, j := range jobs {
		list = append(list, jobToMap(&j))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Jobs": list})
}

func (p *Provider) updateJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "JobName")
	if name == "" {
		return shared.JSONError("InvalidInputException", "JobName is required", http.StatusBadRequest), nil
	}
	input, _ := params["JobUpdate"].(map[string]any)
	if input == nil {
		input = map[string]any{}
	}
	role := strParam(input, "Role")
	command := marshalParam(input, "Command")
	maxRetries := intParam(input, "MaxRetries", 0)
	timeout := intParam(input, "Timeout", 2880)
	config := marshalParam(input, "DefaultArguments")
	if err := p.store.UpdateJob(name, role, command, maxRetries, timeout, config); err != nil {
		if err == errJobNotFound {
			return shared.JSONError("EntityNotFoundException", "Job not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobName": name})
}

func (p *Provider) deleteJob(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "JobName")
	if err := p.store.DeleteJob(name); err != nil {
		if err == errJobNotFound {
			return shared.JSONError("EntityNotFoundException", "Job not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobName": name})
}

func (p *Provider) batchGetJobs(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "JobNames")
	var found []any
	var missing []string
	for _, name := range names {
		j, err := p.store.GetJob(name)
		if err != nil {
			missing = append(missing, name)
		} else {
			found = append(found, jobToMap(j))
		}
	}
	if found == nil {
		found = []any{}
	}
	if missing == nil {
		missing = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Jobs": found, "JobsNotFound": missing})
}

func (p *Provider) startJobRun(params map[string]any) (*plugin.Response, error) {
	jobName := strParam(params, "JobName")
	if jobName == "" {
		return shared.JSONError("InvalidInputException", "JobName is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("jr_", 24)
	_, err := p.store.CreateJobRun(id, jobName)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobRunId": id})
}

func (p *Provider) getJobRun(params map[string]any) (*plugin.Response, error) {
	runID := strParam(params, "RunId")
	r, err := p.store.GetJobRun(runID)
	if err != nil {
		if err == errJobRunNotFound {
			return shared.JSONError("EntityNotFoundException", "JobRun not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobRun": jobRunToMap(r)})
}

func (p *Provider) getJobRuns(params map[string]any) (*plugin.Response, error) {
	jobName := strParam(params, "JobName")
	runs, err := p.store.ListJobRuns(jobName)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(runs))
	for _, r := range runs {
		list = append(list, jobRunToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobRuns": list})
}

func (p *Provider) batchStopJobRun(params map[string]any) (*plugin.Response, error) {
	jobName := strParam(params, "JobName")
	runIDs := stringsParam(params, "JobRunIds")
	var errs []any
	for _, id := range runIDs {
		if err := p.store.UpdateJobRunStatus(id, "STOPPED"); err != nil {
			errs = append(errs, map[string]any{
				"JobName":     jobName,
				"JobRunId":    id,
				"ErrorDetail": map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": err.Error()},
			})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SuccessfulSubmissions": []any{}, "Errors": errs})
}

func jobToMap(j *Job) map[string]any {
	var command, config any
	json.Unmarshal([]byte(j.Command), &command)
	json.Unmarshal([]byte(j.Config), &config)
	return map[string]any{
		"Name":             j.Name,
		"Role":             j.Role,
		"Command":          command,
		"MaxRetries":       j.MaxRetries,
		"Timeout":          j.Timeout,
		"DefaultArguments": config,
		"CreatedOn":        j.CreatedAt.Unix(),
	}
}

func jobRunToMap(r *JobRun) map[string]any {
	return map[string]any{
		"Id":          r.ID,
		"JobName":     r.JobName,
		"JobRunState": r.Status,
		"StartedOn":   r.StartedAt.Unix(),
		"CompletedOn": r.CompletedAt.Unix(),
	}
}

// ---- Connection handlers ----

func (p *Provider) createConnection(params map[string]any) (*plugin.Response, error) {
	input, _ := params["ConnectionInput"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidInputException", "ConnectionInput is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	catID := catalogID(params)
	connType := strParam(input, "ConnectionType")
	if connType == "" {
		connType = "JDBC"
	}
	properties := marshalParam(input, "ConnectionProperties")
	_, err := p.store.CreateConnection(catID, name, connType, properties)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Connection already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getConnection(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	catID := catalogID(params)
	c, err := p.store.GetConnection(catID, name)
	if err != nil {
		if err == errConnectionNotFound {
			return shared.JSONError("EntityNotFoundException", "Connection not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Connection": connectionToMap(c)})
}

func (p *Provider) getConnections(params map[string]any) (*plugin.Response, error) {
	catID := catalogID(params)
	conns, err := p.store.ListConnections(catID)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(conns))
	for _, c := range conns {
		list = append(list, connectionToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConnectionList": list})
}

func (p *Provider) updateConnection(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	catID := catalogID(params)
	input, _ := params["ConnectionInput"].(map[string]any)
	if input == nil {
		input = map[string]any{}
	}
	connType := strParam(input, "ConnectionType")
	if connType == "" {
		connType = "JDBC"
	}
	properties := marshalParam(input, "ConnectionProperties")
	if err := p.store.UpdateConnection(catID, name, connType, properties); err != nil {
		if err == errConnectionNotFound {
			return shared.JSONError("EntityNotFoundException", "Connection not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteConnection(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConnectionName")
	catID := catalogID(params)
	if err := p.store.DeleteConnection(catID, name); err != nil {
		if err == errConnectionNotFound {
			return shared.JSONError("EntityNotFoundException", "Connection not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchDeleteConnection(params map[string]any) (*plugin.Response, error) {
	catID := catalogID(params)
	names := stringsParam(params, "ConnectionNameList")
	var errs []any
	for _, name := range names {
		if err := p.store.DeleteConnection(catID, name); err != nil {
			errs = append(errs, map[string]any{
				"Key":         name,
				"ErrorDetail": map[string]any{"ErrorCode": "EntityNotFoundException", "ErrorMessage": err.Error()},
			})
		}
	}
	if errs == nil {
		errs = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Succeeded": []any{}, "Errors": errs})
}

func connectionToMap(c *Connection) map[string]any {
	var props any
	json.Unmarshal([]byte(c.Properties), &props)
	return map[string]any{
		"Name":                 c.Name,
		"ConnectionType":       c.Type,
		"ConnectionProperties": props,
		"CreationTime":         c.CreatedAt.Unix(),
		"CatalogId":            c.CatalogID,
	}
}

// ---- Trigger handlers ----

func (p *Provider) createTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	trigType := strParam(params, "Type")
	if trigType == "" {
		trigType = "ON_DEMAND"
	}
	actions := marshalParamArray(params, "Actions")
	predicate := marshalParam(params, "Predicate")
	schedule := strParam(params, "Schedule")
	_, err := p.store.CreateTrigger(name, trigType, actions, predicate, schedule)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "Trigger already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) getTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	t, err := p.store.GetTrigger(name)
	if err != nil {
		if err == errTriggerNotFound {
			return shared.JSONError("EntityNotFoundException", "Trigger not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Trigger": triggerToMap(t)})
}

func (p *Provider) getTriggers(params map[string]any) (*plugin.Response, error) {
	triggers, err := p.store.ListTriggers()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(triggers))
	for _, t := range triggers {
		list = append(list, triggerToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Triggers": list})
}

func (p *Provider) updateTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	input, _ := params["TriggerUpdate"].(map[string]any)
	if input == nil {
		input = map[string]any{}
	}
	trigType := strParam(input, "Type")
	if trigType == "" {
		trigType = "ON_DEMAND"
	}
	actions := marshalParamArray(input, "Actions")
	predicate := marshalParam(input, "Predicate")
	schedule := strParam(input, "Schedule")
	if err := p.store.UpdateTrigger(name, trigType, actions, predicate, schedule); err != nil {
		if err == errTriggerNotFound {
			return shared.JSONError("EntityNotFoundException", "Trigger not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Trigger": map[string]any{"Name": name}})
}

func (p *Provider) deleteTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.DeleteTrigger(name); err != nil {
		if err == errTriggerNotFound {
			return shared.JSONError("EntityNotFoundException", "Trigger not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) startTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.UpdateTriggerState(name, "ACTIVATED"); err != nil {
		if err == errTriggerNotFound {
			return shared.JSONError("EntityNotFoundException", "Trigger not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) stopTrigger(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.UpdateTriggerState(name, "DEACTIVATED"); err != nil {
		if err == errTriggerNotFound {
			return shared.JSONError("EntityNotFoundException", "Trigger not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) batchGetTriggers(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "TriggerNames")
	var found []any
	var missing []string
	for _, name := range names {
		t, err := p.store.GetTrigger(name)
		if err != nil {
			missing = append(missing, name)
		} else {
			found = append(found, triggerToMap(t))
		}
	}
	if found == nil {
		found = []any{}
	}
	if missing == nil {
		missing = []string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Triggers": found, "TriggersNotFound": missing})
}

func triggerToMap(t *Trigger) map[string]any {
	var actions, predicate any
	json.Unmarshal([]byte(t.Actions), &actions)
	json.Unmarshal([]byte(t.Predicate), &predicate)
	return map[string]any{
		"Name":      t.Name,
		"Type":      t.Type,
		"State":     t.State,
		"Actions":   actions,
		"Predicate": predicate,
		"Schedule":  t.Schedule,
	}
}

// ---- SecurityConfiguration handlers ----

func (p *Provider) createSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidInputException", "Name is required", http.StatusBadRequest), nil
	}
	config := marshalParam(params, "EncryptionConfiguration")
	_, err := p.store.CreateSecurityConfig(name, config)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("AlreadyExistsException", "SecurityConfiguration already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Name": name})
}

func (p *Provider) getSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	c, err := p.store.GetSecurityConfig(name)
	if err != nil {
		if err == errSecurityConfigNotFound {
			return shared.JSONError("EntityNotFoundException", "SecurityConfiguration not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SecurityConfiguration": securityConfigToMap(c)})
}

func (p *Provider) getSecurityConfigurations(params map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListSecurityConfigs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(configs))
	for _, c := range configs {
		list = append(list, securityConfigToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"SecurityConfigurations": list})
}

func (p *Provider) deleteSecurityConfiguration(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if err := p.store.DeleteSecurityConfig(name); err != nil {
		if err == errSecurityConfigNotFound {
			return shared.JSONError("EntityNotFoundException", "SecurityConfiguration not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func securityConfigToMap(c *SecurityConfig) map[string]any {
	var config any
	json.Unmarshal([]byte(c.Config), &config)
	return map[string]any{
		"Name":                    c.Name,
		"EncryptionConfiguration": config,
		"CreatedTimeStamp":        c.CreatedAt.Unix(),
	}
}

// ---- Tags handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	tagsRaw, _ := params["TagsToAdd"].(map[string]any)
	tags := make(map[string]string)
	for k, v := range tagsRaw {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	keys := stringsParam(params, "TagsToRemove")
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getTags(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}
