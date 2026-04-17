// SPDX-License-Identifier: Apache-2.0

// internal/services/appsync/provider.go
package appsync

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

// Provider implements the DeepdishControlPlaneService (AppSync) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "appsync" }
func (p *Provider) ServiceName() string           { return "DeepdishControlPlaneService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "appsync"))
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

	path := req.URL.Path

	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// GraphqlApi
	case "CreateGraphqlApi":
		return p.createGraphqlApi(params)
	case "GetGraphqlApi":
		apiID := extractPathParam(path, "apis")
		return p.getGraphqlApi(apiID)
	case "ListGraphqlApis":
		return p.listGraphqlApis()
	case "UpdateGraphqlApi":
		apiID := extractPathParam(path, "apis")
		return p.updateGraphqlApi(apiID, params)
	case "DeleteGraphqlApi":
		apiID := extractPathParam(path, "apis")
		return p.deleteGraphqlApi(apiID)

	// DataSource
	case "CreateDataSource":
		apiID := extractPathParam(path, "apis")
		return p.createDataSource(apiID, params)
	case "GetDataSource":
		apiID := extractPathParam(path, "apis")
		name := extractPathParam(path, "datasources")
		return p.getDataSource(apiID, name)
	case "ListDataSources":
		apiID := extractPathParam(path, "apis")
		return p.listDataSources(apiID)
	case "UpdateDataSource":
		apiID := extractPathParam(path, "apis")
		name := extractPathParam(path, "datasources")
		return p.updateDataSource(apiID, name, params)
	case "DeleteDataSource":
		apiID := extractPathParam(path, "apis")
		name := extractPathParam(path, "datasources")
		return p.deleteDataSource(apiID, name)

	// Resolver
	case "CreateResolver":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		return p.createResolver(apiID, typeName, params)
	case "GetResolver":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		fieldName := extractPathParam(path, "resolvers")
		return p.getResolver(apiID, typeName, fieldName)
	case "ListResolvers":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		return p.listResolvers(apiID, typeName)
	case "UpdateResolver":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		fieldName := extractPathParam(path, "resolvers")
		return p.updateResolver(apiID, typeName, fieldName, params)
	case "DeleteResolver":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		fieldName := extractPathParam(path, "resolvers")
		return p.deleteResolver(apiID, typeName, fieldName)
	case "ListResolversByFunction":
		apiID := extractPathParam(path, "apis")
		funcID := extractPathParam(path, "functions")
		return p.listResolversByFunction(apiID, funcID)

	// Function
	case "CreateFunction":
		apiID := extractPathParam(path, "apis")
		return p.createFunction(apiID, params)
	case "GetFunction":
		apiID := extractPathParam(path, "apis")
		funcID := extractPathParam(path, "functions")
		return p.getFunction(apiID, funcID)
	case "ListFunctions":
		apiID := extractPathParam(path, "apis")
		return p.listFunctions(apiID)
	case "UpdateFunction":
		apiID := extractPathParam(path, "apis")
		funcID := extractPathParam(path, "functions")
		return p.updateFunction(apiID, funcID, params)
	case "DeleteFunction":
		apiID := extractPathParam(path, "apis")
		funcID := extractPathParam(path, "functions")
		return p.deleteFunction(apiID, funcID)

	// ApiKey
	case "CreateApiKey":
		apiID := extractPathParam(path, "apis")
		return p.createApiKey(apiID, params)
	case "ListApiKeys":
		apiID := extractPathParam(path, "apis")
		return p.listApiKeys(apiID)
	case "UpdateApiKey":
		apiID := extractPathParam(path, "apis")
		keyID := extractPathParam(path, "apikeys")
		return p.updateApiKey(apiID, keyID, params)
	case "DeleteApiKey":
		apiID := extractPathParam(path, "apis")
		keyID := extractPathParam(path, "apikeys")
		return p.deleteApiKey(apiID, keyID)

	// Type
	case "CreateType":
		apiID := extractPathParam(path, "apis")
		return p.createType(apiID, params)
	case "GetType":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		format := req.URL.Query().Get("format")
		return p.getType(apiID, typeName, format)
	case "ListTypes":
		apiID := extractPathParam(path, "apis")
		format := req.URL.Query().Get("format")
		return p.listTypes(apiID, format)
	case "UpdateType":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		return p.updateType(apiID, typeName, params)
	case "DeleteType":
		apiID := extractPathParam(path, "apis")
		typeName := extractPathParam(path, "types")
		return p.deleteType(apiID, typeName)

	// Schema
	case "StartSchemaCreation":
		apiID := extractPathParam(path, "apis")
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"status": "PROCESSING",
			"apiId":  apiID,
		})
	case "GetSchemaCreationStatus":
		apiID := extractPathParam(path, "apis")
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"status": "SUCCESS",
			"apiId":  apiID,
		})
	case "GetIntrospectionSchema":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"schema": "",
		})

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Stubs for remaining operations
	default:
		return p.handleStub(op)
	}
}

func (p *Provider) handleStub(op string) (*plugin.Response, error) {
	switch op {
	// Api (AppSync Events API)
	case "CreateApi", "GetApi", "UpdateApi", "DeleteApi", "ListApis":
		return shared.JSONResponse(http.StatusOK, map[string]any{"api": map[string]any{}, "apis": []any{}})
	// ChannelNamespace
	case "CreateChannelNamespace", "GetChannelNamespace", "UpdateChannelNamespace", "DeleteChannelNamespace", "ListChannelNamespaces":
		return shared.JSONResponse(http.StatusOK, map[string]any{"channelNamespace": map[string]any{}, "channelNamespaces": []any{}})
	// DomainName
	case "CreateDomainName", "GetDomainName", "UpdateDomainName", "DeleteDomainName", "ListDomainNames":
		return shared.JSONResponse(http.StatusOK, map[string]any{"domainNameConfig": map[string]any{}, "domainNameConfigs": []any{}})
	case "AssociateApi", "DisassociateApi", "GetApiAssociation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"apiAssociation": map[string]any{}})
	// SourceApiAssociation
	case "AssociateMergedGraphqlApi", "AssociateSourceGraphqlApi", "DisassociateMergedGraphqlApi", "DisassociateSourceGraphqlApi",
		"GetSourceApiAssociation", "ListSourceApiAssociations", "UpdateSourceApiAssociation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"sourceApiAssociation": map[string]any{}})
	// Cache
	case "CreateApiCache", "GetApiCache", "UpdateApiCache", "DeleteApiCache", "FlushApiCache":
		return shared.JSONResponse(http.StatusOK, map[string]any{"apiCache": map[string]any{}})
	// DataSourceIntrospection
	case "StartDataSourceIntrospection", "GetDataSourceIntrospection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"introspectionId": shared.GenerateID("", 16)})
	// Evaluate
	case "EvaluateCode":
		return shared.JSONResponse(http.StatusOK, map[string]any{"evaluationResult": ""})
	case "EvaluateMappingTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"evaluationResult": ""})
	// EnvironmentVariables
	case "PutGraphqlApiEnvironmentVariables", "GetGraphqlApiEnvironmentVariables":
		return shared.JSONResponse(http.StatusOK, map[string]any{"environmentVariables": map[string]any{}})
	// Merged API
	case "CreateMergedApiAssociation", "DeleteMergedApiAssociation", "ListMergedApiAssociations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"mergedApiAssociation": map[string]any{}})
	// Introspection
	case "StartSchemaMerge", "GetSchemaError":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.TrimPrefix(path, "/v1")
	p = strings.Trim(p, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /v1/tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// APIs: /v1/apis
	case n >= 1 && seg[0] == "apis":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateGraphqlApi"
			case http.MethodGet:
				return "ListGraphqlApis"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetGraphqlApi"
			case http.MethodPost:
				return "UpdateGraphqlApi"
			case http.MethodDelete:
				return "DeleteGraphqlApi"
			}
		}
		// /apis/{id}/datasources
		if n >= 3 && seg[2] == "datasources" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateDataSource"
				case http.MethodGet:
					return "ListDataSources"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetDataSource"
				case http.MethodPost:
					return "UpdateDataSource"
				case http.MethodDelete:
					return "DeleteDataSource"
				}
			}
		}
		// /apis/{id}/types/{type}/resolvers
		if n >= 3 && seg[2] == "types" {
			if n == 3 && method == http.MethodGet {
				return "ListTypes"
			}
			if n == 4 {
				switch method {
				case http.MethodPost:
					return "CreateType"
				case http.MethodGet:
					return "GetType"
				case http.MethodPut:
					return "UpdateType"
				case http.MethodDelete:
					return "DeleteType"
				}
			}
			if n >= 5 && seg[4] == "resolvers" {
				if n == 5 {
					switch method {
					case http.MethodPost:
						return "CreateResolver"
					case http.MethodGet:
						return "ListResolvers"
					}
				}
				if n == 6 {
					switch method {
					case http.MethodGet:
						return "GetResolver"
					case http.MethodPost:
						return "UpdateResolver"
					case http.MethodDelete:
						return "DeleteResolver"
					}
				}
			}
		}
		// /apis/{id}/functions
		if n >= 3 && seg[2] == "functions" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateFunction"
				case http.MethodGet:
					return "ListFunctions"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetFunction"
				case http.MethodPost:
					return "UpdateFunction"
				case http.MethodDelete:
					return "DeleteFunction"
				}
			}
			// /apis/{id}/functions/{fid}/resolvers
			if n >= 5 && seg[4] == "resolvers" && n == 5 && method == http.MethodGet {
				return "ListResolversByFunction"
			}
		}
		// /apis/{id}/apikeys
		if n >= 3 && seg[2] == "apikeys" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateApiKey"
				case http.MethodGet:
					return "ListApiKeys"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodPost:
					return "UpdateApiKey"
				case http.MethodDelete:
					return "DeleteApiKey"
				}
			}
		}
		// /apis/{id}/schemacreation
		if n >= 3 && seg[2] == "schemacreation" {
			switch method {
			case http.MethodPost:
				return "StartSchemaCreation"
			case http.MethodGet:
				return "GetSchemaCreationStatus"
			}
		}
		// /apis/{id}/schema
		if n >= 3 && seg[2] == "schema" && method == http.MethodGet {
			return "GetIntrospectionSchema"
		}
		// /apis/{id}/environmentVariables
		if n >= 3 && seg[2] == "environmentVariables" {
			switch method {
			case http.MethodPut:
				return "PutGraphqlApiEnvironmentVariables"
			case http.MethodGet:
				return "GetGraphqlApiEnvironmentVariables"
			}
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apis, err := p.store.ListGraphqlApis()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apis))
	for _, a := range apis {
		res = append(res, plugin.Resource{Type: "appsync-graphql-api", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- GraphqlApi CRUD ---

func (p *Provider) createGraphqlApi(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("", 26)
	arn := shared.BuildARN("appsync", "apis", id)
	urisMap := map[string]string{
		"GRAPHQL":  fmt.Sprintf("https://%s.appsync-api.us-east-1.amazonaws.com/graphql", id),
		"REALTIME": fmt.Sprintf("wss://%s.appsync-realtime-api.us-east-1.amazonaws.com/graphql", id),
	}
	urisJSON, _ := json.Marshal(urisMap)
	a := &GraphqlApi{
		ID:        id,
		ARN:       arn,
		Name:      name,
		AuthType:  strParamDefault(params, "authenticationType", "API_KEY"),
		LogConfig: "{}",
		Uris:      string(urisJSON),
	}
	if err := p.store.CreateGraphqlApi(a); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "api already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{"graphqlApi": graphqlApiToMap(a, tags)})
}

func (p *Provider) getGraphqlApi(apiID string) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetGraphqlApi(apiID)
	if err != nil {
		return shared.JSONError("NotFoundException", "GraphQL API not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"graphqlApi": graphqlApiToMap(a, tags)})
}

func (p *Provider) listGraphqlApis() (*plugin.Response, error) {
	apis, err := p.store.ListGraphqlApis()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(apis))
	for i := range apis {
		tags, _ := p.store.tags.ListTags(apis[i].ARN)
		result = append(result, graphqlApiToMap(&apis[i], tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"graphqlApis": result})
}

func (p *Provider) updateGraphqlApi(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateGraphqlApi(apiID, params); err != nil {
		return shared.JSONError("NotFoundException", "GraphQL API not found", http.StatusNotFound), nil
	}
	a, _ := p.store.GetGraphqlApi(apiID)
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"graphqlApi": graphqlApiToMap(a, tags)})
}

func (p *Provider) deleteGraphqlApi(apiID string) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.DeleteGraphqlApi(apiID)
	if err != nil {
		return shared.JSONError("NotFoundException", "GraphQL API not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- DataSource CRUD ---

func (p *Provider) createDataSource(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("appsync", fmt.Sprintf("apis/%s/datasources", apiID), name)
	ds := &DataSource{
		ApiID:       apiID,
		Name:        name,
		ARN:         arn,
		Type:        strParamDefault(params, "type", "NONE"),
		Config:      "{}",
		ServiceRole: strParam(params, "serviceRoleArn"),
	}
	if err := p.store.CreateDataSource(ds); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "data source already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"dataSource": dataSourceToMap(ds)})
}

func (p *Provider) getDataSource(apiID, name string) (*plugin.Response, error) {
	if apiID == "" || name == "" {
		return shared.JSONError("BadRequestException", "apiId and name are required", http.StatusBadRequest), nil
	}
	ds, err := p.store.GetDataSource(apiID, name)
	if err != nil {
		return shared.JSONError("NotFoundException", "data source not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"dataSource": dataSourceToMap(ds)})
}

func (p *Provider) listDataSources(apiID string) (*plugin.Response, error) {
	dss, err := p.store.ListDataSources(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(dss))
	for i := range dss {
		result = append(result, dataSourceToMap(&dss[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"dataSources": result})
}

func (p *Provider) updateDataSource(apiID, name string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || name == "" {
		return shared.JSONError("BadRequestException", "apiId and name are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDataSource(apiID, name, params); err != nil {
		return shared.JSONError("NotFoundException", "data source not found", http.StatusNotFound), nil
	}
	ds, _ := p.store.GetDataSource(apiID, name)
	return shared.JSONResponse(http.StatusOK, map[string]any{"dataSource": dataSourceToMap(ds)})
}

func (p *Provider) deleteDataSource(apiID, name string) (*plugin.Response, error) {
	if apiID == "" || name == "" {
		return shared.JSONError("BadRequestException", "apiId and name are required", http.StatusBadRequest), nil
	}
	ds, err := p.store.DeleteDataSource(apiID, name)
	if err != nil {
		return shared.JSONError("NotFoundException", "data source not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"dataSource": dataSourceToMap(ds)})
}

// --- Resolver CRUD ---

func (p *Provider) createResolver(apiID, typeName string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || typeName == "" {
		return shared.JSONError("BadRequestException", "apiId and typeName are required", http.StatusBadRequest), nil
	}
	fieldName, _ := params["fieldName"].(string)
	if fieldName == "" {
		return shared.JSONError("BadRequestException", "fieldName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("appsync", fmt.Sprintf("apis/%s/types/%s/resolvers", apiID, typeName), fieldName)
	r := &Resolver{
		ApiID:            apiID,
		TypeName:         typeName,
		FieldName:        fieldName,
		ARN:              arn,
		DataSource:       strParam(params, "dataSourceName"),
		RequestTemplate:  strParam(params, "requestMappingTemplate"),
		ResponseTemplate: strParam(params, "responseMappingTemplate"),
		Kind:             strParamDefault(params, "kind", "UNIT"),
	}
	if err := p.store.CreateResolver(r); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "resolver already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolver": resolverToMap(r)})
}

func (p *Provider) getResolver(apiID, typeName, fieldName string) (*plugin.Response, error) {
	if apiID == "" || typeName == "" || fieldName == "" {
		return shared.JSONError("BadRequestException", "apiId, typeName and fieldName are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return shared.JSONError("NotFoundException", "resolver not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolver": resolverToMap(r)})
}

func (p *Provider) listResolvers(apiID, typeName string) (*plugin.Response, error) {
	resolvers, err := p.store.ListResolvers(apiID, typeName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(resolvers))
	for i := range resolvers {
		result = append(result, resolverToMap(&resolvers[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolvers": result})
}

func (p *Provider) updateResolver(apiID, typeName, fieldName string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || typeName == "" || fieldName == "" {
		return shared.JSONError("BadRequestException", "apiId, typeName and fieldName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateResolver(apiID, typeName, fieldName, params); err != nil {
		return shared.JSONError("NotFoundException", "resolver not found", http.StatusNotFound), nil
	}
	r, _ := p.store.GetResolver(apiID, typeName, fieldName)
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolver": resolverToMap(r)})
}

func (p *Provider) deleteResolver(apiID, typeName, fieldName string) (*plugin.Response, error) {
	if apiID == "" || typeName == "" || fieldName == "" {
		return shared.JSONError("BadRequestException", "apiId, typeName and fieldName are required", http.StatusBadRequest), nil
	}
	r, err := p.store.DeleteResolver(apiID, typeName, fieldName)
	if err != nil {
		return shared.JSONError("NotFoundException", "resolver not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolver": resolverToMap(r)})
}

func (p *Provider) listResolversByFunction(apiID, funcID string) (*plugin.Response, error) {
	resolvers, err := p.store.ListResolversByFunction(apiID, funcID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(resolvers))
	for i := range resolvers {
		result = append(result, resolverToMap(&resolvers[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resolvers": result})
}

// --- Function CRUD ---

func (p *Provider) createFunction(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("", 26)
	arn := shared.BuildARN("appsync", fmt.Sprintf("apis/%s/functions", apiID), id)
	f := &Function{
		ApiID:            apiID,
		ID:               id,
		ARN:              arn,
		Name:             name,
		DataSource:       strParam(params, "dataSourceName"),
		RequestTemplate:  strParam(params, "requestMappingTemplate"),
		ResponseTemplate: strParam(params, "responseMappingTemplate"),
	}
	if err := p.store.CreateFunction(f); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"functionConfiguration": functionToMap(f)})
}

func (p *Provider) getFunction(apiID, funcID string) (*plugin.Response, error) {
	if apiID == "" || funcID == "" {
		return shared.JSONError("BadRequestException", "apiId and functionId are required", http.StatusBadRequest), nil
	}
	f, err := p.store.GetFunction(apiID, funcID)
	if err != nil {
		return shared.JSONError("NotFoundException", "function not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"functionConfiguration": functionToMap(f)})
}

func (p *Provider) listFunctions(apiID string) (*plugin.Response, error) {
	fns, err := p.store.ListFunctions(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(fns))
	for i := range fns {
		result = append(result, functionToMap(&fns[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"functions": result})
}

func (p *Provider) updateFunction(apiID, funcID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || funcID == "" {
		return shared.JSONError("BadRequestException", "apiId and functionId are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateFunction(apiID, funcID, params); err != nil {
		return shared.JSONError("NotFoundException", "function not found", http.StatusNotFound), nil
	}
	f, _ := p.store.GetFunction(apiID, funcID)
	return shared.JSONResponse(http.StatusOK, map[string]any{"functionConfiguration": functionToMap(f)})
}

func (p *Provider) deleteFunction(apiID, funcID string) (*plugin.Response, error) {
	if apiID == "" || funcID == "" {
		return shared.JSONError("BadRequestException", "apiId and functionId are required", http.StatusBadRequest), nil
	}
	f, err := p.store.DeleteFunction(apiID, funcID)
	if err != nil {
		return shared.JSONError("NotFoundException", "function not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"functionConfiguration": functionToMap(f)})
}

// --- ApiKey CRUD ---

func (p *Provider) createApiKey(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	id := "da2-" + shared.GenerateID("", 26)
	expires := time.Now().Add(7 * 24 * time.Hour).Unix()
	if v, ok := params["expires"].(float64); ok && v > 0 {
		expires = int64(v)
	}
	k := &ApiKey{
		ApiID:       apiID,
		ID:          id,
		Expires:     expires,
		Description: strParam(params, "description"),
	}
	if err := p.store.CreateApiKey(k); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"apiKey": apiKeyToMap(k)})
}

func (p *Provider) listApiKeys(apiID string) (*plugin.Response, error) {
	keys, err := p.store.ListApiKeys(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(keys))
	for i := range keys {
		result = append(result, apiKeyToMap(&keys[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"apiKeys": result})
}

func (p *Provider) updateApiKey(apiID, keyID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || keyID == "" {
		return shared.JSONError("BadRequestException", "apiId and id are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateApiKey(apiID, keyID, params); err != nil {
		return shared.JSONError("NotFoundException", "api key not found", http.StatusNotFound), nil
	}
	k, _ := p.store.GetApiKey(apiID, keyID)
	return shared.JSONResponse(http.StatusOK, map[string]any{"apiKey": apiKeyToMap(k)})
}

func (p *Provider) deleteApiKey(apiID, keyID string) (*plugin.Response, error) {
	if apiID == "" || keyID == "" {
		return shared.JSONError("BadRequestException", "apiId and id are required", http.StatusBadRequest), nil
	}
	k, err := p.store.DeleteApiKey(apiID, keyID)
	if err != nil {
		return shared.JSONError("NotFoundException", "api key not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"apiKey": apiKeyToMap(k)})
}

// --- Type CRUD ---

func (p *Provider) createType(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	definition, _ := params["definition"].(string)
	if definition == "" {
		return shared.JSONError("BadRequestException", "definition is required", http.StatusBadRequest), nil
	}
	// Try to extract type name from SDL definition
	name := extractTypeName(definition)
	if name == "" {
		name = shared.GenerateID("Type", 8)
	}
	format := strParamDefault(params, "format", "SDL")
	tp := &Type{
		ApiID:      apiID,
		Name:       name,
		Definition: definition,
		Format:     format,
	}
	if err := p.store.CreateType(tp); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "type already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"type": typeToMap(tp)})
}

func (p *Provider) getType(apiID, typeName, format string) (*plugin.Response, error) {
	if apiID == "" || typeName == "" {
		return shared.JSONError("BadRequestException", "apiId and typeName are required", http.StatusBadRequest), nil
	}
	tp, err := p.store.GetType(apiID, typeName)
	if err != nil {
		return shared.JSONError("NotFoundException", "type not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"type": typeToMap(tp)})
}

func (p *Provider) listTypes(apiID, format string) (*plugin.Response, error) {
	types, err := p.store.ListTypes(apiID, format)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(types))
	for i := range types {
		result = append(result, typeToMap(&types[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"types": result})
}

func (p *Provider) updateType(apiID, typeName string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || typeName == "" {
		return shared.JSONError("BadRequestException", "apiId and typeName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateType(apiID, typeName, params); err != nil {
		return shared.JSONError("NotFoundException", "type not found", http.StatusNotFound), nil
	}
	tp, _ := p.store.GetType(apiID, typeName)
	return shared.JSONResponse(http.StatusOK, map[string]any{"type": typeToMap(tp)})
}

func (p *Provider) deleteType(apiID, typeName string) (*plugin.Response, error) {
	if apiID == "" || typeName == "" {
		return shared.JSONError("BadRequestException", "apiId and typeName are required", http.StatusBadRequest), nil
	}
	tp, err := p.store.DeleteType(apiID, typeName)
	if err != nil {
		return shared.JSONError("NotFoundException", "type not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"type": typeToMap(tp)})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractTagARN(req.URL.Path)
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractTagARN(req.URL.Path)
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractTagARN(req.URL.Path)
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// --- Map builders ---

func graphqlApiToMap(a *GraphqlApi, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var uris map[string]string
	json.Unmarshal([]byte(a.Uris), &uris)
	if uris == nil {
		uris = map[string]string{}
	}
	return map[string]any{
		"apiId":              a.ID,
		"arn":                a.ARN,
		"name":               a.Name,
		"authenticationType": a.AuthType,
		"uris":               uris,
		"tags":               tags,
		"createdAt":          a.CreatedAt.Format(time.RFC3339),
		"xrayEnabled":        false,
	}
}

func dataSourceToMap(ds *DataSource) map[string]any {
	return map[string]any{
		"dataSourceArn":  ds.ARN,
		"name":           ds.Name,
		"type":           ds.Type,
		"serviceRoleArn": ds.ServiceRole,
		"apiId":          ds.ApiID,
	}
}

func resolverToMap(r *Resolver) map[string]any {
	return map[string]any{
		"resolverArn":             r.ARN,
		"typeName":                r.TypeName,
		"fieldName":               r.FieldName,
		"dataSourceName":          r.DataSource,
		"requestMappingTemplate":  r.RequestTemplate,
		"responseMappingTemplate": r.ResponseTemplate,
		"kind":                    r.Kind,
	}
}

func functionToMap(f *Function) map[string]any {
	return map[string]any{
		"functionArn":             f.ARN,
		"functionId":              f.ID,
		"name":                    f.Name,
		"dataSourceName":          f.DataSource,
		"requestMappingTemplate":  f.RequestTemplate,
		"responseMappingTemplate": f.ResponseTemplate,
		"apiId":                   f.ApiID,
	}
}

func apiKeyToMap(k *ApiKey) map[string]any {
	return map[string]any{
		"id":          k.ID,
		"description": k.Description,
		"expires":     k.Expires,
	}
}

func typeToMap(tp *Type) map[string]any {
	return map[string]any{
		"name":       tp.Name,
		"definition": tp.Definition,
		"format":     tp.Format,
		"apiId":      tp.ApiID,
	}
}

// --- Helpers ---

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractTagARN extracts ARN from tag resource paths like /v1/tags/{arn}
func extractTagARN(path string) string {
	// Path is /v1/tags/{resourceArn}
	idx := strings.Index(path, "/tags/")
	if idx < 0 {
		return ""
	}
	return path[idx+len("/tags/"):]
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func strParamDefault(params map[string]any, key, def string) string {
	if v, ok := params[key].(string); ok && v != "" {
		return v
	}
	return def
}

func toStringMap(m map[string]any) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}

// extractTypeName tries to parse the type name from a GraphQL SDL definition like "type Foo { ... }"
func extractTypeName(definition string) string {
	definition = strings.TrimSpace(definition)
	// Look for: type <Name> or input <Name> or interface <Name> or enum <Name>
	for _, prefix := range []string{"type ", "input ", "interface ", "enum ", "union "} {
		if strings.HasPrefix(definition, prefix) {
			rest := strings.TrimPrefix(definition, prefix)
			rest = strings.TrimSpace(rest)
			end := strings.IndexAny(rest, " \t\n{")
			if end < 0 {
				return rest
			}
			return rest[:end]
		}
	}
	return ""
}
