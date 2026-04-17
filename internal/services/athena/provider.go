// SPDX-License-Identifier: Apache-2.0

// internal/services/athena/provider.go
package athena

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

func (p *Provider) ServiceID() string             { return "athena" }
func (p *Provider) ServiceName() string           { return "AmazonAthena" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "athena"))
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
	// WorkGroup
	case "CreateWorkGroup":
		return p.createWorkGroup(params)
	case "GetWorkGroup":
		return p.getWorkGroup(params)
	case "ListWorkGroups":
		return p.listWorkGroups(params)
	case "DeleteWorkGroup":
		return p.deleteWorkGroup(params)
	case "UpdateWorkGroup":
		return p.updateWorkGroup(params)
	// NamedQuery
	case "CreateNamedQuery":
		return p.createNamedQuery(params)
	case "GetNamedQuery":
		return p.getNamedQuery(params)
	case "ListNamedQueries":
		return p.listNamedQueries(params)
	case "DeleteNamedQuery":
		return p.deleteNamedQuery(params)
	case "BatchGetNamedQuery":
		return p.batchGetNamedQuery(params)
	// QueryExecution
	case "StartQueryExecution":
		return p.startQueryExecution(params)
	case "GetQueryExecution":
		return p.getQueryExecution(params)
	case "GetQueryResults":
		return p.getQueryResults(params)
	case "ListQueryExecutions":
		return p.listQueryExecutions(params)
	case "StopQueryExecution":
		return p.stopQueryExecution(params)
	case "BatchGetQueryExecution":
		return p.batchGetQueryExecution(params)
	case "GetQueryRuntimeStatistics":
		return p.getQueryRuntimeStatistics(params)
	// DataCatalog
	case "CreateDataCatalog":
		return p.createDataCatalog(params)
	case "GetDataCatalog":
		return p.getDataCatalog(params)
	case "ListDataCatalogs":
		return p.listDataCatalogs(params)
	case "UpdateDataCatalog":
		return p.updateDataCatalog(params)
	case "DeleteDataCatalog":
		return p.deleteDataCatalog(params)
	// PreparedStatement
	case "CreatePreparedStatement":
		return p.createPreparedStatement(params)
	case "GetPreparedStatement":
		return p.getPreparedStatement(params)
	case "ListPreparedStatements":
		return p.listPreparedStatements(params)
	case "UpdatePreparedStatement":
		return p.updatePreparedStatement(params)
	case "DeletePreparedStatement":
		return p.deletePreparedStatement(params)
	case "BatchGetPreparedStatement":
		return p.batchGetPreparedStatement(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Stub operations (CapacityReservation, Notebook, Session, Calculator, etc.)
	case "CreateCapacityReservation", "UpdateCapacityReservation", "CancelCapacityReservation",
		"DeleteCapacityReservation", "PutCapacityAssignmentConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetCapacityReservation":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"CapacityReservation": map[string]any{"Name": strParam(params, "Name"), "Status": "ACTIVE", "TargetDpus": 24},
		})
	case "ListCapacityReservations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CapacityReservations": []any{}})
	case "GetCapacityAssignmentConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"CapacityAssignmentConfiguration": map[string]any{
				"CapacityReservationName": strParam(params, "CapacityReservationName"),
				"CapacityAssignments":     []any{},
			},
		})
	case "CreateNotebook", "DeleteNotebook", "UpdateNotebook", "UpdateNotebookMetadata", "ImportNotebook":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookId": shared.GenerateUUID()})
	case "ExportNotebook":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookMetadata": map[string]any{}, "Payload": ""})
	case "GetNotebookMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookMetadata": map[string]any{"NotebookId": strParam(params, "NotebookId")}})
	case "ListNotebookMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookMetadataList": []any{}})
	case "CreatePresignedNotebookUrl":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AuthToken": "token", "AuthTokenExpirationTime": 0, "NotebookUrl": "https://example.com"})
	case "ListNotebookSessions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NotebookSessionsList": []any{}})
	case "StartSession":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionId": shared.GenerateUUID(), "State": "CREATING"})
	case "GetSession":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionId": strParam(params, "SessionId"), "Status": map[string]any{"State": "IDLE"}})
	case "GetSessionStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SessionId": strParam(params, "SessionId"), "Status": map[string]any{"State": "IDLE"}})
	case "GetSessionEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Endpoint": ""})
	case "ListSessions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Sessions": []any{}})
	case "TerminateSession":
		return shared.JSONResponse(http.StatusOK, map[string]any{"State": "TERMINATING"})
	case "StartCalculationExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CalculationExecutionId": shared.GenerateUUID(), "State": "CREATING"})
	case "StopCalculationExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"State": "CANCELING"})
	case "GetCalculationExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CalculationExecutionId": strParam(params, "CalculationExecutionId"), "Status": map[string]any{"State": "COMPLETED"}})
	case "GetCalculationExecutionCode":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CodeBlock": ""})
	case "GetCalculationExecutionStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Status": map[string]any{"State": "COMPLETED"}})
	case "ListCalculationExecutions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Calculations": []any{}})
	case "ListExecutors":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Executors": []any{}})
	case "GetDatabase":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Database": map[string]any{"Name": strParam(params, "Database"), "Description": ""},
		})
	case "ListDatabases":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DatabaseList": []any{}})
	case "GetTableMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TableMetadata": map[string]any{"Name": strParam(params, "TableName"), "Columns": []any{}},
		})
	case "ListTableMetadata":
		return shared.JSONResponse(http.StatusOK, map[string]any{"TableMetadataList": []any{}})
	case "ListApplicationDPUSizes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationDPUSizes": []any{}})
	case "ListEngineVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EngineVersions": []any{}})
	case "GetResourceDashboard":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ResourceDashboard": map[string]any{}})
	case "UpdateNamedQuery":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	wgs, err := p.store.ListWorkGroups()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(wgs))
	for _, wg := range wgs {
		res = append(res, plugin.Resource{Type: "workgroup", ID: wg.Name, Name: wg.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- WorkGroup handlers ----

func (p *Provider) createWorkGroup(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	configJSON := "{}"
	if cfg, ok := params["Configuration"]; ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	}
	arn := shared.BuildARN("athena", "workgroup", name)
	wg, err := p.store.CreateWorkGroup(name, arn, description, configJSON)
	if err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("InvalidRequestException", "WorkGroup already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(wg.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getWorkGroup(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "WorkGroup")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "WorkGroup is required", http.StatusBadRequest), nil
	}
	wg, err := p.store.GetWorkGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "WorkGroup not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WorkGroup": workGroupToMap(wg),
	})
}

func (p *Provider) listWorkGroups(_ map[string]any) (*plugin.Response, error) {
	wgs, err := p.store.ListWorkGroups()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(wgs))
	for _, wg := range wgs {
		items = append(items, map[string]any{
			"Name":         wg.Name,
			"State":        wg.State,
			"Description":  wg.Description,
			"CreationTime": wg.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"WorkGroups": items})
}

func (p *Provider) deleteWorkGroup(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "WorkGroup")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "WorkGroup is required", http.StatusBadRequest), nil
	}
	wg, err := p.store.GetWorkGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "WorkGroup not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(wg.ARN)
	if err := p.store.DeleteWorkGroup(name); err != nil {
		return shared.JSONError("InvalidRequestException", "WorkGroup not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateWorkGroup(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "WorkGroup")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "WorkGroup is required", http.StatusBadRequest), nil
	}
	wg, err := p.store.GetWorkGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "WorkGroup not found", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	if description == "" {
		description = wg.Description
	}
	state := strParam(params, "State")
	if state == "" {
		state = wg.State
	}
	configJSON := wg.Config
	if cfg, ok := params["ConfigurationUpdates"]; ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	}
	if err := p.store.UpdateWorkGroup(name, description, state, configJSON); err != nil {
		return shared.JSONError("InvalidRequestException", "WorkGroup not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func workGroupToMap(wg *WorkGroup) map[string]any {
	var cfg any
	json.Unmarshal([]byte(wg.Config), &cfg)
	return map[string]any{
		"Name":          wg.Name,
		"State":         wg.State,
		"Description":   wg.Description,
		"CreationTime":  wg.CreatedAt.Unix(),
		"Configuration": cfg,
	}
}

// ---- NamedQuery handlers ----

func (p *Provider) createNamedQuery(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	queryString := strParam(params, "QueryString")
	if name == "" || queryString == "" {
		return shared.JSONError("InvalidRequestException", "Name and QueryString are required", http.StatusBadRequest), nil
	}
	workgroup := strParam(params, "WorkGroup")
	if workgroup == "" {
		workgroup = "primary"
	}
	database := strParam(params, "Database")
	description := strParam(params, "Description")
	id := shared.GenerateUUID()
	nq, err := p.store.CreateNamedQuery(id, name, workgroup, database, queryString, description)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NamedQueryId": nq.ID})
}

func (p *Provider) getNamedQuery(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "NamedQueryId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "NamedQueryId is required", http.StatusBadRequest), nil
	}
	nq, err := p.store.GetNamedQuery(id)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "NamedQuery not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"NamedQuery": namedQueryToMap(nq),
	})
}

func (p *Provider) listNamedQueries(params map[string]any) (*plugin.Response, error) {
	workgroup := strParam(params, "WorkGroup")
	queries, err := p.store.ListNamedQueries(workgroup)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(queries))
	for _, q := range queries {
		ids = append(ids, q.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"NamedQueryIds": ids})
}

func (p *Provider) deleteNamedQuery(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "NamedQueryId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "NamedQueryId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteNamedQuery(id); err != nil {
		return shared.JSONError("InvalidRequestException", "NamedQuery not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchGetNamedQuery(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["NamedQueryIds"].([]any)
	var found []map[string]any
	var unprocessed []map[string]any
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		nq, err := p.store.GetNamedQuery(id)
		if err != nil {
			unprocessed = append(unprocessed, map[string]any{"NamedQueryId": id, "ErrorCode": "404", "ErrorMessage": "not found"})
		} else {
			found = append(found, namedQueryToMap(nq))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"NamedQueries":             found,
		"UnprocessedNamedQueryIds": unprocessed,
	})
}

func namedQueryToMap(nq *NamedQuery) map[string]any {
	return map[string]any{
		"NamedQueryId": nq.ID,
		"Name":         nq.Name,
		"WorkGroup":    nq.WorkGroup,
		"Database":     nq.DatabaseName,
		"QueryString":  nq.QueryString,
		"Description":  nq.Description,
	}
}

// ---- QueryExecution handlers ----

func (p *Provider) startQueryExecution(params map[string]any) (*plugin.Response, error) {
	query := strParam(params, "QueryString")
	if query == "" {
		return shared.JSONError("InvalidRequestException", "QueryString is required", http.StatusBadRequest), nil
	}
	workgroup := strParam(params, "WorkGroup")
	if workgroup == "" {
		workgroup = "primary"
	}
	database := ""
	if ctx, ok := params["QueryExecutionContext"].(map[string]any); ok {
		database, _ = ctx["Database"].(string)
	}
	id := shared.GenerateUUID()
	qe, err := p.store.CreateQueryExecution(id, workgroup, query, database)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"QueryExecutionId": qe.ID})
}

func (p *Provider) getQueryExecution(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "QueryExecutionId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "QueryExecutionId is required", http.StatusBadRequest), nil
	}
	qe, err := p.store.GetQueryExecution(id)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "QueryExecution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryExecution": queryExecutionToMap(qe),
	})
}

func (p *Provider) getQueryResults(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "QueryExecutionId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "QueryExecutionId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetQueryExecution(id); err != nil {
		return shared.JSONError("InvalidRequestException", "QueryExecution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResultSet": map[string]any{
			"Rows": []any{},
			"ResultSetMetadata": map[string]any{
				"ColumnInfo": []any{
					map[string]any{"Name": "result", "Type": "varchar"},
				},
			},
		},
		"UpdateCount": 0,
	})
}

func (p *Provider) listQueryExecutions(params map[string]any) (*plugin.Response, error) {
	workgroup := strParam(params, "WorkGroup")
	execs, err := p.store.ListQueryExecutions(workgroup)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(execs))
	for _, qe := range execs {
		ids = append(ids, qe.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"QueryExecutionIds": ids})
}

func (p *Provider) stopQueryExecution(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "QueryExecutionId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "QueryExecutionId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateQueryExecutionStatus(id, "CANCELLED"); err != nil {
		return shared.JSONError("InvalidRequestException", "QueryExecution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchGetQueryExecution(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["QueryExecutionIds"].([]any)
	var found []map[string]any
	var unprocessed []map[string]any
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		qe, err := p.store.GetQueryExecution(id)
		if err != nil {
			unprocessed = append(unprocessed, map[string]any{"QueryExecutionId": id, "ErrorCode": "404", "ErrorMessage": "not found"})
		} else {
			found = append(found, queryExecutionToMap(qe))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryExecutions":              found,
		"UnprocessedQueryExecutionIds": unprocessed,
	})
}

func (p *Provider) getQueryRuntimeStatistics(params map[string]any) (*plugin.Response, error) {
	id := strParam(params, "QueryExecutionId")
	if id == "" {
		return shared.JSONError("InvalidRequestException", "QueryExecutionId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetQueryExecution(id); err != nil {
		return shared.JSONError("InvalidRequestException", "QueryExecution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryRuntimeStatistics": map[string]any{
			"Timeline": map[string]any{
				"QueryQueueTimeInMillis":        0,
				"QueryPlanningTimeInMillis":     0,
				"EngineExecutionTimeInMillis":   0,
				"ServiceProcessingTimeInMillis": 0,
				"TotalExecutionTimeInMillis":    0,
			},
		},
	})
}

func queryExecutionToMap(qe *QueryExecution) map[string]any {
	return map[string]any{
		"QueryExecutionId": qe.ID,
		"WorkGroup":        qe.WorkGroup,
		"Query":            qe.Query,
		"QueryExecutionContext": map[string]any{
			"Database": qe.DatabaseName,
		},
		"Status": map[string]any{
			"State":              qe.Status,
			"SubmissionDateTime": qe.SubmittedAt.Unix(),
		},
		"Statistics": map[string]any{
			"EngineExecutionTimeInMillis": 0,
			"DataScannedInBytes":          0,
		},
	}
}

// ---- DataCatalog handlers ----

func (p *Provider) createDataCatalog(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "Name is required", http.StatusBadRequest), nil
	}
	catalogType := strParam(params, "Type")
	if catalogType == "" {
		catalogType = "HIVE"
	}
	description := strParam(params, "Description")
	parametersJSON := "{}"
	if p2, ok := params["Parameters"]; ok {
		b, _ := json.Marshal(p2)
		parametersJSON = string(b)
	}
	arn := shared.BuildARN("athena", "datacatalog", name)
	if _, err := p.store.CreateDataCatalog(name, arn, catalogType, description, parametersJSON); err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("InvalidRequestException", "DataCatalog already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getDataCatalog(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "Name is required", http.StatusBadRequest), nil
	}
	dc, err := p.store.GetDataCatalog(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "DataCatalog not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DataCatalog": dataCatalogToMap(dc),
	})
}

func (p *Provider) listDataCatalogs(_ map[string]any) (*plugin.Response, error) {
	catalogs, err := p.store.ListDataCatalogs()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(catalogs))
	for _, dc := range catalogs {
		items = append(items, map[string]any{
			"CatalogName": dc.Name,
			"Type":        dc.Type,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DataCatalogsSummary": items})
}

func (p *Provider) updateDataCatalog(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "Name is required", http.StatusBadRequest), nil
	}
	dc, err := p.store.GetDataCatalog(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "DataCatalog not found", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	if description == "" {
		description = dc.Description
	}
	parametersJSON := dc.Parameters
	if p2, ok := params["Parameters"]; ok {
		b, _ := json.Marshal(p2)
		parametersJSON = string(b)
	}
	if err := p.store.UpdateDataCatalog(name, description, parametersJSON); err != nil {
		return shared.JSONError("InvalidRequestException", "DataCatalog not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteDataCatalog(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("InvalidRequestException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDataCatalog(name); err != nil {
		return shared.JSONError("InvalidRequestException", "DataCatalog not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func dataCatalogToMap(dc *DataCatalog) map[string]any {
	var params any
	json.Unmarshal([]byte(dc.Parameters), &params)
	return map[string]any{
		"Name":        dc.Name,
		"Type":        dc.Type,
		"Description": dc.Description,
		"Parameters":  params,
	}
}

// ---- PreparedStatement handlers ----

func (p *Provider) createPreparedStatement(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "StatementName")
	workgroup := strParam(params, "WorkGroup")
	queryStatement := strParam(params, "QueryStatement")
	if name == "" || workgroup == "" || queryStatement == "" {
		return shared.JSONError("InvalidRequestException", "StatementName, WorkGroup, and QueryStatement are required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	if _, err := p.store.CreatePreparedStatement(name, workgroup, queryStatement, description); err != nil {
		if sqliteIsUnique(err) {
			return shared.JSONError("InvalidRequestException", "PreparedStatement already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getPreparedStatement(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "StatementName")
	workgroup := strParam(params, "WorkGroup")
	if name == "" || workgroup == "" {
		return shared.JSONError("InvalidRequestException", "StatementName and WorkGroup are required", http.StatusBadRequest), nil
	}
	ps, err := p.store.GetPreparedStatement(name, workgroup)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "PreparedStatement not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PreparedStatement": preparedStatementToMap(ps),
	})
}

func (p *Provider) listPreparedStatements(params map[string]any) (*plugin.Response, error) {
	workgroup := strParam(params, "WorkGroup")
	if workgroup == "" {
		return shared.JSONError("InvalidRequestException", "WorkGroup is required", http.StatusBadRequest), nil
	}
	stmts, err := p.store.ListPreparedStatements(workgroup)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(stmts))
	for _, ps := range stmts {
		items = append(items, map[string]any{
			"StatementName":    ps.Name,
			"LastModifiedTime": ps.UpdatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"PreparedStatements": items})
}

func (p *Provider) updatePreparedStatement(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "StatementName")
	workgroup := strParam(params, "WorkGroup")
	queryStatement := strParam(params, "QueryStatement")
	if name == "" || workgroup == "" || queryStatement == "" {
		return shared.JSONError("InvalidRequestException", "StatementName, WorkGroup, and QueryStatement are required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	if err := p.store.UpdatePreparedStatement(name, workgroup, queryStatement, description); err != nil {
		return shared.JSONError("InvalidRequestException", "PreparedStatement not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deletePreparedStatement(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "StatementName")
	workgroup := strParam(params, "WorkGroup")
	if name == "" || workgroup == "" {
		return shared.JSONError("InvalidRequestException", "StatementName and WorkGroup are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePreparedStatement(name, workgroup); err != nil {
		return shared.JSONError("InvalidRequestException", "PreparedStatement not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchGetPreparedStatement(params map[string]any) (*plugin.Response, error) {
	workgroup := strParam(params, "WorkGroup")
	rawNames, _ := params["PreparedStatementNames"].([]any)
	var found []map[string]any
	var unprocessed []map[string]any
	for _, raw := range rawNames {
		name, _ := raw.(string)
		ps, err := p.store.GetPreparedStatement(name, workgroup)
		if err != nil {
			unprocessed = append(unprocessed, map[string]any{"StatementName": name, "ErrorCode": "404", "ErrorMessage": "not found"})
		} else {
			found = append(found, preparedStatementToMap(ps))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PreparedStatements":                found,
		"UnprocessedPreparedStatementNames": unprocessed,
	})
}

func preparedStatementToMap(ps *PreparedStatement) map[string]any {
	return map[string]any{
		"StatementName":    ps.Name,
		"WorkGroupName":    ps.WorkGroup,
		"QueryStatement":   ps.QueryStatement,
		"Description":      ps.Description,
		"LastModifiedTime": ps.UpdatedAt.Unix(),
	}
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceARN")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// ---- helpers ----

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func sqliteIsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
