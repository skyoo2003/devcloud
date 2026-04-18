// SPDX-License-Identifier: Apache-2.0

// internal/services/apigatewayv2/provider.go
package apigatewayv2

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

// Provider implements the ApiGatewayV2 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "apigatewayv2" }
func (p *Provider) ServiceName() string           { return "ApiGatewayV2" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "apigatewayv2"))
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
		return shared.JSONError("BadRequestException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("BadRequestException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	path := req.URL.Path

	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// APIs
	case "CreateApi":
		return p.createAPI(params)
	case "GetApi":
		apiID := extractPathParam(path, "apis")
		return p.getAPI(apiID, nil)
	case "GetApis":
		return p.listAPIs()
	case "UpdateApi":
		apiID := extractPathParam(path, "apis")
		return p.updateAPI(apiID, params)
	case "DeleteApi":
		apiID := extractPathParam(path, "apis")
		return p.deleteAPI(apiID)
	case "ImportApi":
		return p.importAPI(params)
	case "ReimportApi":
		apiID := extractPathParam(path, "apis")
		return p.reimportAPI(apiID, params)
	case "ExportApi":
		apiID := extractPathParam(path, "apis")
		return p.exportAPI(apiID)

	// Routes
	case "CreateRoute":
		apiID := extractPathParam(path, "apis")
		return p.createRoute(apiID, params)
	case "GetRoute":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		return p.getRoute(apiID, routeID)
	case "GetRoutes":
		apiID := extractPathParam(path, "apis")
		return p.listRoutes(apiID)
	case "UpdateRoute":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		return p.updateRoute(apiID, routeID, params)
	case "DeleteRoute":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		return p.deleteRoute(apiID, routeID)

	// Route Responses
	case "CreateRouteResponse":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		return p.createRouteResponse(apiID, routeID, params)
	case "GetRouteResponse":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		responseID := extractPathParam(path, "routeresponses")
		return p.getRouteResponse(apiID, routeID, responseID)
	case "GetRouteResponses":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		return p.listRouteResponses(apiID, routeID)
	case "DeleteRouteResponse":
		apiID := extractPathParam(path, "apis")
		routeID := extractPathParam(path, "routes")
		responseID := extractPathParam(path, "routeresponses")
		return p.deleteRouteResponse(apiID, routeID, responseID)

	// Integrations
	case "CreateIntegration":
		apiID := extractPathParam(path, "apis")
		return p.createIntegration(apiID, params)
	case "GetIntegration":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		return p.getIntegration(apiID, integrationID)
	case "GetIntegrations":
		apiID := extractPathParam(path, "apis")
		return p.listIntegrations(apiID)
	case "UpdateIntegration":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		return p.updateIntegration(apiID, integrationID, params)
	case "DeleteIntegration":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		return p.deleteIntegration(apiID, integrationID)

	// Integration Responses
	case "CreateIntegrationResponse":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		return p.createIntegrationResponse(apiID, integrationID, params)
	case "GetIntegrationResponse":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		responseID := extractPathParam(path, "integrationresponses")
		return p.getIntegrationResponse(apiID, integrationID, responseID)
	case "GetIntegrationResponses":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		return p.listIntegrationResponses(apiID, integrationID)
	case "UpdateIntegrationResponse":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		responseID := extractPathParam(path, "integrationresponses")
		return p.updateIntegrationResponse(apiID, integrationID, responseID, params)
	case "DeleteIntegrationResponse":
		apiID := extractPathParam(path, "apis")
		integrationID := extractPathParam(path, "integrations")
		responseID := extractPathParam(path, "integrationresponses")
		return p.deleteIntegrationResponse(apiID, integrationID, responseID)

	// Authorizers
	case "CreateAuthorizer":
		apiID := extractPathParam(path, "apis")
		return p.createAuthorizer(apiID, params)
	case "GetAuthorizer":
		apiID := extractPathParam(path, "apis")
		authorizerID := extractPathParam(path, "authorizers")
		return p.getAuthorizer(apiID, authorizerID)
	case "GetAuthorizers":
		apiID := extractPathParam(path, "apis")
		return p.listAuthorizers(apiID)
	case "UpdateAuthorizer":
		apiID := extractPathParam(path, "apis")
		authorizerID := extractPathParam(path, "authorizers")
		return p.updateAuthorizer(apiID, authorizerID, params)
	case "DeleteAuthorizer":
		apiID := extractPathParam(path, "apis")
		authorizerID := extractPathParam(path, "authorizers")
		return p.deleteAuthorizer(apiID, authorizerID)
	case "ResetAuthorizersCache":
		return shared.JSONResponse(http.StatusNoContent, nil)

	// Deployments
	case "CreateDeployment":
		apiID := extractPathParam(path, "apis")
		return p.createDeployment(apiID, params)
	case "GetDeployment":
		apiID := extractPathParam(path, "apis")
		deploymentID := extractPathParam(path, "deployments")
		return p.getDeployment(apiID, deploymentID)
	case "GetDeployments":
		apiID := extractPathParam(path, "apis")
		return p.listDeployments(apiID)
	case "UpdateDeployment":
		apiID := extractPathParam(path, "apis")
		deploymentID := extractPathParam(path, "deployments")
		return p.updateDeployment(apiID, deploymentID, params)
	case "DeleteDeployment":
		apiID := extractPathParam(path, "apis")
		deploymentID := extractPathParam(path, "deployments")
		return p.deleteDeployment(apiID, deploymentID)

	// Stages
	case "CreateStage":
		apiID := extractPathParam(path, "apis")
		return p.createStage(apiID, params)
	case "GetStage":
		apiID := extractPathParam(path, "apis")
		stageName := extractPathParam(path, "stages")
		return p.getStage(apiID, stageName)
	case "GetStages":
		apiID := extractPathParam(path, "apis")
		return p.listStages(apiID)
	case "UpdateStage":
		apiID := extractPathParam(path, "apis")
		stageName := extractPathParam(path, "stages")
		return p.updateStage(apiID, stageName, params)
	case "DeleteStage":
		apiID := extractPathParam(path, "apis")
		stageName := extractPathParam(path, "stages")
		return p.deleteStage(apiID, stageName)
	case "DeleteAccessLogSettings":
		return shared.JSONResponse(http.StatusNoContent, nil)
	case "DeleteRouteSettings":
		return shared.JSONResponse(http.StatusNoContent, nil)
	case "DeleteRouteRequestParameter":
		return shared.JSONResponse(http.StatusNoContent, nil)

	// Models
	case "CreateModel":
		apiID := extractPathParam(path, "apis")
		return p.createModel(apiID, params)
	case "GetModel":
		apiID := extractPathParam(path, "apis")
		modelID := extractPathParam(path, "models")
		return p.getModel(apiID, modelID)
	case "GetModels":
		apiID := extractPathParam(path, "apis")
		return p.listModels(apiID)
	case "GetModelTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"value": "{}"})
	case "UpdateModel":
		apiID := extractPathParam(path, "apis")
		modelID := extractPathParam(path, "models")
		return p.updateModel(apiID, modelID, params)
	case "DeleteModel":
		apiID := extractPathParam(path, "apis")
		modelID := extractPathParam(path, "models")
		return p.deleteModel(apiID, modelID)

	// Domain Names
	case "CreateDomainName":
		return p.createDomainName(params)
	case "GetDomainName":
		domainName := extractPathParam(path, "domainnames")
		return p.getDomainName(domainName)
	case "GetDomainNames":
		return p.listDomainNames()
	case "UpdateDomainName":
		domainName := extractPathParam(path, "domainnames")
		return p.updateDomainName(domainName, params)
	case "DeleteDomainName":
		domainName := extractPathParam(path, "domainnames")
		return p.deleteDomainName(domainName)

	// API Mappings
	case "CreateApiMapping":
		domainName := extractPathParam(path, "domainnames")
		return p.createAPIMapping(domainName, params)
	case "GetApiMapping":
		domainName := extractPathParam(path, "domainnames")
		mappingID := extractPathParam(path, "apimappings")
		return p.getAPIMapping(domainName, mappingID)
	case "GetApiMappings":
		domainName := extractPathParam(path, "domainnames")
		return p.listAPIMappings(domainName)
	case "UpdateApiMapping":
		domainName := extractPathParam(path, "domainnames")
		mappingID := extractPathParam(path, "apimappings")
		return p.updateAPIMapping(domainName, mappingID, params)
	case "DeleteApiMapping":
		domainName := extractPathParam(path, "domainnames")
		mappingID := extractPathParam(path, "apimappings")
		return p.deleteAPIMapping(domainName, mappingID)

	// VPC Links
	case "CreateVpcLink":
		return p.createVpcLink(params)
	case "GetVpcLink":
		vpcLinkID := extractPathParam(path, "vpclinks")
		return p.getVpcLink(vpcLinkID)
	case "GetVpcLinks":
		return p.listVpcLinks()
	case "UpdateVpcLink":
		vpcLinkID := extractPathParam(path, "vpclinks")
		return p.updateVpcLink(vpcLinkID, params)
	case "DeleteVpcLink":
		vpcLinkID := extractPathParam(path, "vpclinks")
		return p.deleteVpcLink(vpcLinkID)

	// Tags
	case "GetTags":
		return p.getTags(req)
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)

	// CORS
	case "DeleteCorsConfiguration":
		return shared.JSONResponse(http.StatusNoContent, nil)

	// Stub ops (Portal, PortalProduct, RoutingRule, etc.)
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apis, err := p.store.ListAPIs()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apis))
	for _, a := range apis {
		res = append(res, plugin.Resource{Type: "apigatewayv2-api", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- API CRUD ---

func (p *Provider) createAPI(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("", 10)
	arn := shared.BuildARN("apigateway", "restapis", id)
	apiEndpoint := fmt.Sprintf("https://%s.execute-api.us-east-1.amazonaws.com", id)

	a := &API{
		ID:             id,
		ARN:            arn,
		Name:           name,
		ProtocolType:   strParamDefault(params, "protocolType", "HTTP"),
		Description:    strParam(params, "description"),
		RouteSelection: strParamDefault(params, "routeSelectionExpression", "$request.method $request.path"),
		APIEndpoint:    apiEndpoint,
	}

	if err := p.store.CreateAPI(a); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "api already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}

	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusCreated, apiToMap(a, tags))
}

func (p *Provider) getAPI(apiID string, tags map[string]string) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAPI(apiID)
	if err != nil {
		return shared.JSONError("NotFoundException", "api not found", http.StatusNotFound), nil
	}
	if tags == nil {
		tags, _ = p.store.tags.ListTags(a.ARN)
	}
	return shared.JSONResponse(http.StatusOK, apiToMap(a, tags))
}

func (p *Provider) listAPIs() (*plugin.Response, error) {
	apis, err := p.store.ListAPIs()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(apis))
	for i := range apis {
		tags, _ := p.store.tags.ListTags(apis[i].ARN)
		result = append(result, apiToMap(&apis[i], tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateAPI(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateAPI(apiID, params); err != nil {
		return shared.JSONError("NotFoundException", "api not found", http.StatusNotFound), nil
	}
	a, _ := p.store.GetAPI(apiID)
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, apiToMap(a, tags))
}

func (p *Provider) deleteAPI(apiID string) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.DeleteAPI(apiID)
	if err != nil {
		return shared.JSONError("NotFoundException", "api not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(a.ARN)
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *Provider) importAPI(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		name = "imported-api"
	}
	return p.createAPI(map[string]any{
		"name":         name,
		"protocolType": "HTTP",
	})
}

func (p *Provider) reimportAPI(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAPI(apiID)
	if err != nil {
		return shared.JSONError("NotFoundException", "api not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, apiToMap(a, tags))
}

func (p *Provider) exportAPI(apiID string) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAPI(apiID); err != nil {
		return shared.JSONError("NotFoundException", "api not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"body": "{}"})
}

// --- Route CRUD ---

func (p *Provider) createRoute(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	routeKey, _ := params["routeKey"].(string)
	if routeKey == "" {
		return shared.JSONError("BadRequestException", "routeKey is required", http.StatusBadRequest), nil
	}
	r := &Route{
		ID:                shared.GenerateID("", 10),
		APIID:             apiID,
		RouteKey:          routeKey,
		Target:            strParam(params, "target"),
		AuthorizationType: strParamDefault(params, "authorizationType", "NONE"),
		AuthorizerID:      strParam(params, "authorizerId"),
	}
	if err := p.store.CreateRoute(r); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, routeToMap(r))
}

func (p *Provider) getRoute(apiID, routeID string) (*plugin.Response, error) {
	if apiID == "" || routeID == "" {
		return shared.JSONError("BadRequestException", "apiId and routeId are required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRoute(apiID, routeID)
	if err != nil {
		return shared.JSONError("NotFoundException", "route not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, routeToMap(r))
}

func (p *Provider) listRoutes(apiID string) (*plugin.Response, error) {
	routes, err := p.store.ListRoutes(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(routes))
	for i := range routes {
		result = append(result, routeToMap(&routes[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateRoute(apiID, routeID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || routeID == "" {
		return shared.JSONError("BadRequestException", "apiId and routeId are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRoute(apiID, routeID, params); err != nil {
		return shared.JSONError("NotFoundException", "route not found", http.StatusNotFound), nil
	}
	r, _ := p.store.GetRoute(apiID, routeID)
	return shared.JSONResponse(http.StatusOK, routeToMap(r))
}

func (p *Provider) deleteRoute(apiID, routeID string) (*plugin.Response, error) {
	if apiID == "" || routeID == "" {
		return shared.JSONError("BadRequestException", "apiId and routeId are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.DeleteRoute(apiID, routeID); err != nil {
		return shared.JSONError("NotFoundException", "route not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Route Response CRUD ---

func (p *Provider) createRouteResponse(apiID, routeID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" || routeID == "" {
		return shared.JSONError("BadRequestException", "apiId and routeId are required", http.StatusBadRequest), nil
	}
	rr := &RouteResponse{
		ID:                 shared.GenerateID("", 10),
		APIID:              apiID,
		RouteID:            routeID,
		RouteResponseKey:   strParamDefault(params, "routeResponseKey", "default"),
		ModelSelectionExpr: strParam(params, "modelSelectionExpression"),
	}
	if err := p.store.CreateRouteResponse(rr); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, routeResponseToMap(rr))
}

func (p *Provider) getRouteResponse(apiID, routeID, responseID string) (*plugin.Response, error) {
	rr, err := p.store.GetRouteResponse(apiID, routeID, responseID)
	if err != nil {
		return shared.JSONError("NotFoundException", "route response not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, routeResponseToMap(rr))
}

func (p *Provider) listRouteResponses(apiID, routeID string) (*plugin.Response, error) {
	items, err := p.store.ListRouteResponses(apiID, routeID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, routeResponseToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) deleteRouteResponse(apiID, routeID, responseID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteRouteResponse(apiID, routeID, responseID); err != nil {
		return shared.JSONError("NotFoundException", "route response not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Integration CRUD ---

func (p *Provider) createIntegration(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	i := &Integration{
		ID:                shared.GenerateID("", 10),
		APIID:             apiID,
		Type:              strParamDefault(params, "integrationType", "HTTP_PROXY"),
		IntegrationURI:    strParam(params, "integrationUri"),
		IntegrationMethod: strParam(params, "integrationMethod"),
		PayloadFormat:     strParamDefault(params, "payloadFormatVersion", "2.0"),
	}
	if err := p.store.CreateIntegration(i); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, integrationToMap(i))
}

func (p *Provider) getIntegration(apiID, integrationID string) (*plugin.Response, error) {
	i, err := p.store.GetIntegration(apiID, integrationID)
	if err != nil {
		return shared.JSONError("NotFoundException", "integration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, integrationToMap(i))
}

func (p *Provider) listIntegrations(apiID string) (*plugin.Response, error) {
	items, err := p.store.ListIntegrations(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, integrationToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateIntegration(apiID, integrationID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateIntegration(apiID, integrationID, params); err != nil {
		return shared.JSONError("NotFoundException", "integration not found", http.StatusNotFound), nil
	}
	i, _ := p.store.GetIntegration(apiID, integrationID)
	return shared.JSONResponse(http.StatusOK, integrationToMap(i))
}

func (p *Provider) deleteIntegration(apiID, integrationID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteIntegration(apiID, integrationID); err != nil {
		return shared.JSONError("NotFoundException", "integration not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Integration Response CRUD ---

func (p *Provider) createIntegrationResponse(apiID, integrationID string, params map[string]any) (*plugin.Response, error) {
	ir := &IntegrationResponse{
		ID:                shared.GenerateID("", 10),
		APIID:             apiID,
		IntegrationID:     integrationID,
		ResponseKey:       strParamDefault(params, "integrationResponseKey", "default"),
		TemplateSelection: strParam(params, "templateSelectionExpression"),
	}
	if err := p.store.CreateIntegrationResponse(ir); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, integrationResponseToMap(ir))
}

func (p *Provider) getIntegrationResponse(apiID, integrationID, responseID string) (*plugin.Response, error) {
	ir, err := p.store.GetIntegrationResponse(apiID, integrationID, responseID)
	if err != nil {
		return shared.JSONError("NotFoundException", "integration response not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, integrationResponseToMap(ir))
}

func (p *Provider) listIntegrationResponses(apiID, integrationID string) (*plugin.Response, error) {
	items, err := p.store.ListIntegrationResponses(apiID, integrationID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, integrationResponseToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateIntegrationResponse(apiID, integrationID, responseID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateIntegrationResponse(apiID, integrationID, responseID, params); err != nil {
		return shared.JSONError("NotFoundException", "integration response not found", http.StatusNotFound), nil
	}
	ir, _ := p.store.GetIntegrationResponse(apiID, integrationID, responseID)
	return shared.JSONResponse(http.StatusOK, integrationResponseToMap(ir))
}

func (p *Provider) deleteIntegrationResponse(apiID, integrationID, responseID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteIntegrationResponse(apiID, integrationID, responseID); err != nil {
		return shared.JSONError("NotFoundException", "integration response not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Authorizer CRUD ---

func (p *Provider) createAuthorizer(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	jwtConfig := "{}"
	if jc, ok := params["jwtConfiguration"].(map[string]any); ok {
		if b, err := json.Marshal(jc); err == nil {
			jwtConfig = string(b)
		}
	}
	a := &Authorizer{
		ID:             shared.GenerateID("", 10),
		APIID:          apiID,
		Name:           name,
		Type:           strParamDefault(params, "authorizerType", "JWT"),
		IdentitySource: strParam(params, "identitySource"),
		JWTConfig:      jwtConfig,
	}
	if err := p.store.CreateAuthorizer(a); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, authorizerToMap(a))
}

func (p *Provider) getAuthorizer(apiID, authorizerID string) (*plugin.Response, error) {
	a, err := p.store.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		return shared.JSONError("NotFoundException", "authorizer not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, authorizerToMap(a))
}

func (p *Provider) listAuthorizers(apiID string) (*plugin.Response, error) {
	items, err := p.store.ListAuthorizers(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, authorizerToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateAuthorizer(apiID, authorizerID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateAuthorizer(apiID, authorizerID, params); err != nil {
		return shared.JSONError("NotFoundException", "authorizer not found", http.StatusNotFound), nil
	}
	a, _ := p.store.GetAuthorizer(apiID, authorizerID)
	return shared.JSONResponse(http.StatusOK, authorizerToMap(a))
}

func (p *Provider) deleteAuthorizer(apiID, authorizerID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteAuthorizer(apiID, authorizerID); err != nil {
		return shared.JSONError("NotFoundException", "authorizer not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Deployment CRUD ---

func (p *Provider) createDeployment(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	d := &Deployment{
		ID:          shared.GenerateID("", 10),
		APIID:       apiID,
		Description: strParam(params, "description"),
	}
	if err := p.store.CreateDeployment(d); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, deploymentToMap(d))
}

func (p *Provider) getDeployment(apiID, deploymentID string) (*plugin.Response, error) {
	d, err := p.store.GetDeployment(apiID, deploymentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "deployment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, deploymentToMap(d))
}

func (p *Provider) listDeployments(apiID string) (*plugin.Response, error) {
	items, err := p.store.ListDeployments(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, deploymentToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateDeployment(apiID, deploymentID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateDeployment(apiID, deploymentID, params); err != nil {
		return shared.JSONError("NotFoundException", "deployment not found", http.StatusNotFound), nil
	}
	d, _ := p.store.GetDeployment(apiID, deploymentID)
	return shared.JSONResponse(http.StatusOK, deploymentToMap(d))
}

func (p *Provider) deleteDeployment(apiID, deploymentID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteDeployment(apiID, deploymentID); err != nil {
		return shared.JSONError("NotFoundException", "deployment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Stage CRUD ---

func (p *Provider) createStage(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	stageName, _ := params["stageName"].(string)
	if stageName == "" {
		return shared.JSONError("BadRequestException", "stageName is required", http.StatusBadRequest), nil
	}
	stageVars := "{}"
	if sv, ok := params["stageVariables"].(map[string]any); ok {
		if b, err := json.Marshal(sv); err == nil {
			stageVars = string(b)
		}
	}
	st := &Stage{
		Name:           stageName,
		APIID:          apiID,
		Description:    strParam(params, "description"),
		DeploymentID:   strParam(params, "deploymentId"),
		AutoDeploy:     boolParamDefault(params, "autoDeploy", false),
		StageVariables: stageVars,
		AccessLog:      "{}",
	}
	if err := p.store.CreateStage(st); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "stage already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, stageToMap(st))
}

func (p *Provider) getStage(apiID, stageName string) (*plugin.Response, error) {
	st, err := p.store.GetStage(apiID, stageName)
	if err != nil {
		return shared.JSONError("NotFoundException", "stage not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, stageToMap(st))
}

func (p *Provider) listStages(apiID string) (*plugin.Response, error) {
	items, err := p.store.ListStages(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, stageToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateStage(apiID, stageName string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateStage(apiID, stageName, params); err != nil {
		return shared.JSONError("NotFoundException", "stage not found", http.StatusNotFound), nil
	}
	st, _ := p.store.GetStage(apiID, stageName)
	return shared.JSONResponse(http.StatusOK, stageToMap(st))
}

func (p *Provider) deleteStage(apiID, stageName string) (*plugin.Response, error) {
	if _, err := p.store.DeleteStage(apiID, stageName); err != nil {
		return shared.JSONError("NotFoundException", "stage not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Model CRUD ---

func (p *Provider) createModel(apiID string, params map[string]any) (*plugin.Response, error) {
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	schemaDef := "{}"
	if s, ok := params["schema"].(string); ok && s != "" {
		schemaDef = s
	}
	m := &Model{
		ID:          shared.GenerateID("", 10),
		APIID:       apiID,
		Name:        name,
		ContentType: strParamDefault(params, "contentType", "application/json"),
		SchemaDef:   schemaDef,
	}
	if err := p.store.CreateModel(m); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, modelToMap(m))
}

func (p *Provider) getModel(apiID, modelID string) (*plugin.Response, error) {
	m, err := p.store.GetModel(apiID, modelID)
	if err != nil {
		return shared.JSONError("NotFoundException", "model not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, modelToMap(m))
}

func (p *Provider) listModels(apiID string) (*plugin.Response, error) {
	items, err := p.store.ListModels(apiID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, modelToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateModel(apiID, modelID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateModel(apiID, modelID, params); err != nil {
		return shared.JSONError("NotFoundException", "model not found", http.StatusNotFound), nil
	}
	m, _ := p.store.GetModel(apiID, modelID)
	return shared.JSONResponse(http.StatusOK, modelToMap(m))
}

func (p *Provider) deleteModel(apiID, modelID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteModel(apiID, modelID); err != nil {
		return shared.JSONError("NotFoundException", "model not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Domain Name CRUD ---

func (p *Provider) createDomainName(params map[string]any) (*plugin.Response, error) {
	name, _ := params["domainName"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "domainName is required", http.StatusBadRequest), nil
	}
	config := "[]"
	if cfg, ok := params["domainNameConfigurations"].([]any); ok {
		if b, err := json.Marshal(cfg); err == nil {
			config = string(b)
		}
	}
	d := &DomainName{
		Name:      name,
		Config:    config,
		MutualTLS: "{}",
	}
	if err := p.store.CreateDomainName(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "domain name already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		arn := shared.BuildARN("apigateway", "domainnames", name)
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, domainNameToMap(d))
}

func (p *Provider) getDomainName(name string) (*plugin.Response, error) {
	d, err := p.store.GetDomainName(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "domain name not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, domainNameToMap(d))
}

func (p *Provider) listDomainNames() (*plugin.Response, error) {
	items, err := p.store.ListDomainNames()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, domainNameToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateDomainName(name string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateDomainName(name, params); err != nil {
		return shared.JSONError("NotFoundException", "domain name not found", http.StatusNotFound), nil
	}
	d, _ := p.store.GetDomainName(name)
	return shared.JSONResponse(http.StatusOK, domainNameToMap(d))
}

func (p *Provider) deleteDomainName(name string) (*plugin.Response, error) {
	if _, err := p.store.DeleteDomainName(name); err != nil {
		return shared.JSONError("NotFoundException", "domain name not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- API Mapping CRUD ---

func (p *Provider) createAPIMapping(domainName string, params map[string]any) (*plugin.Response, error) {
	if domainName == "" {
		return shared.JSONError("BadRequestException", "domainName is required", http.StatusBadRequest), nil
	}
	apiID, _ := params["apiId"].(string)
	if apiID == "" {
		return shared.JSONError("BadRequestException", "apiId is required", http.StatusBadRequest), nil
	}
	m := &APIMapping{
		ID:            shared.GenerateID("", 10),
		APIID:         apiID,
		DomainName:    domainName,
		Stage:         strParam(params, "stage"),
		APIMappingKey: strParam(params, "apiMappingKey"),
	}
	if err := p.store.CreateAPIMapping(m); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, apiMappingToMap(m))
}

func (p *Provider) getAPIMapping(domainName, mappingID string) (*plugin.Response, error) {
	m, err := p.store.GetAPIMapping(domainName, mappingID)
	if err != nil {
		return shared.JSONError("NotFoundException", "api mapping not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, apiMappingToMap(m))
}

func (p *Provider) listAPIMappings(domainName string) (*plugin.Response, error) {
	items, err := p.store.ListAPIMappings(domainName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, apiMappingToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateAPIMapping(domainName, mappingID string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateAPIMapping(domainName, mappingID, params); err != nil {
		return shared.JSONError("NotFoundException", "api mapping not found", http.StatusNotFound), nil
	}
	m, _ := p.store.GetAPIMapping(domainName, mappingID)
	return shared.JSONResponse(http.StatusOK, apiMappingToMap(m))
}

func (p *Provider) deleteAPIMapping(domainName, mappingID string) (*plugin.Response, error) {
	if _, err := p.store.DeleteAPIMapping(domainName, mappingID); err != nil {
		return shared.JSONError("NotFoundException", "api mapping not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- VPC Link CRUD ---

func (p *Provider) createVpcLink(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "name is required", http.StatusBadRequest), nil
	}
	secGroups := "[]"
	if sg, ok := params["securityGroupIds"].([]any); ok {
		if b, err := json.Marshal(sg); err == nil {
			secGroups = string(b)
		}
	}
	subnetIDs := "[]"
	if sn, ok := params["subnetIds"].([]any); ok {
		if b, err := json.Marshal(sn); err == nil {
			subnetIDs = string(b)
		}
	}
	v := &VpcLink{
		ID:             shared.GenerateID("", 10),
		Name:           name,
		SecurityGroups: secGroups,
		SubnetIDs:      subnetIDs,
		Status:         "AVAILABLE",
	}
	if err := p.store.CreateVpcLink(v); err != nil {
		return nil, err
	}
	if rawTags, ok := params["tags"].(map[string]any); ok {
		arn := shared.BuildARN("apigateway", "vpclinks", v.ID)
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, vpcLinkToMap(v))
}

func (p *Provider) getVpcLink(id string) (*plugin.Response, error) {
	v, err := p.store.GetVpcLink(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "vpc link not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, vpcLinkToMap(v))
}

func (p *Provider) listVpcLinks() (*plugin.Response, error) {
	items, err := p.store.ListVpcLinks()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(items))
	for i := range items {
		result = append(result, vpcLinkToMap(&items[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"items": result})
}

func (p *Provider) updateVpcLink(id string, params map[string]any) (*plugin.Response, error) {
	if err := p.store.UpdateVpcLink(id, params); err != nil {
		return shared.JSONError("NotFoundException", "vpc link not found", http.StatusNotFound), nil
	}
	v, _ := p.store.GetVpcLink(id)
	return shared.JSONResponse(http.StatusOK, vpcLinkToMap(v))
}

func (p *Provider) deleteVpcLink(id string) (*plugin.Response, error) {
	if _, err := p.store.DeleteVpcLink(id); err != nil {
		return shared.JSONError("NotFoundException", "vpc link not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Tags ---

func (p *Provider) getTags(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

// --- Map builders ---

func apiToMap(a *API, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"apiId":                    a.ID,
		"name":                     a.Name,
		"protocolType":             a.ProtocolType,
		"description":              a.Description,
		"routeSelectionExpression": a.RouteSelection,
		"apiEndpoint":              a.APIEndpoint,
		"createdDate":              a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"tags":                     tags,
	}
}

func routeToMap(r *Route) map[string]any {
	return map[string]any{
		"routeId":           r.ID,
		"apiId":             r.APIID,
		"routeKey":          r.RouteKey,
		"target":            r.Target,
		"authorizationType": r.AuthorizationType,
		"authorizerId":      r.AuthorizerID,
	}
}

func routeResponseToMap(rr *RouteResponse) map[string]any {
	return map[string]any{
		"routeResponseId":          rr.ID,
		"routeResponseKey":         rr.RouteResponseKey,
		"modelSelectionExpression": rr.ModelSelectionExpr,
	}
}

func integrationToMap(i *Integration) map[string]any {
	return map[string]any{
		"integrationId":        i.ID,
		"apiId":                i.APIID,
		"integrationType":      i.Type,
		"integrationUri":       i.IntegrationURI,
		"integrationMethod":    i.IntegrationMethod,
		"payloadFormatVersion": i.PayloadFormat,
	}
}

func integrationResponseToMap(ir *IntegrationResponse) map[string]any {
	return map[string]any{
		"integrationResponseId":       ir.ID,
		"integrationId":               ir.IntegrationID,
		"integrationResponseKey":      ir.ResponseKey,
		"templateSelectionExpression": ir.TemplateSelection,
	}
}

func authorizerToMap(a *Authorizer) map[string]any {
	var jwtConfig any
	_ = json.Unmarshal([]byte(a.JWTConfig), &jwtConfig)
	if jwtConfig == nil {
		jwtConfig = map[string]any{}
	}
	return map[string]any{
		"authorizerId":     a.ID,
		"name":             a.Name,
		"authorizerType":   a.Type,
		"identitySource":   a.IdentitySource,
		"jwtConfiguration": jwtConfig,
	}
}

func deploymentToMap(d *Deployment) map[string]any {
	return map[string]any{
		"deploymentId":     d.ID,
		"description":      d.Description,
		"autoDeployed":     d.AutoDeployed,
		"deploymentStatus": "DEPLOYED",
		"createdDate":      d.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func stageToMap(st *Stage) map[string]any {
	var stageVars any
	_ = json.Unmarshal([]byte(st.StageVariables), &stageVars)
	if stageVars == nil {
		stageVars = map[string]any{}
	}
	return map[string]any{
		"stageName":      st.Name,
		"apiId":          st.APIID,
		"description":    st.Description,
		"deploymentId":   st.DeploymentID,
		"autoDeploy":     st.AutoDeploy,
		"stageVariables": stageVars,
		"createdDate":    st.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func modelToMap(m *Model) map[string]any {
	return map[string]any{
		"modelId":     m.ID,
		"name":        m.Name,
		"contentType": m.ContentType,
		"schema":      m.SchemaDef,
	}
}

func domainNameToMap(d *DomainName) map[string]any {
	var config any
	_ = json.Unmarshal([]byte(d.Config), &config)
	if config == nil {
		config = []any{}
	}
	return map[string]any{
		"domainName":                    d.Name,
		"domainNameConfigurations":      config,
		"apiMappingSelectionExpression": "$request.basepath",
	}
}

func apiMappingToMap(m *APIMapping) map[string]any {
	return map[string]any{
		"apiMappingId":  m.ID,
		"apiId":         m.APIID,
		"domainName":    m.DomainName,
		"stage":         m.Stage,
		"apiMappingKey": m.APIMappingKey,
	}
}

func vpcLinkToMap(v *VpcLink) map[string]any {
	var secGroups any
	_ = json.Unmarshal([]byte(v.SecurityGroups), &secGroups)
	if secGroups == nil {
		secGroups = []any{}
	}
	var subnetIDs any
	_ = json.Unmarshal([]byte(v.SubnetIDs), &subnetIDs)
	if subnetIDs == nil {
		subnetIDs = []any{}
	}
	return map[string]any{
		"vpcLinkId":        v.ID,
		"name":             v.Name,
		"securityGroupIds": secGroups,
		"subnetIds":        subnetIDs,
		"vpcLinkStatus":    v.Status,
		"createdDate":      v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

// --- Helpers ---

// resolveOp maps an HTTP method + URL path to an API Gateway V2 operation name.
func resolveOp(method, path string) string {
	// Strip common prefixes like /v2/ and trailing slashes.
	p := strings.TrimPrefix(path, "/v2")
	p = strings.Trim(p, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /v2/tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "GetTags"
		case http.MethodDelete:
			return "UntagResource"
		}

	// Domain names
	case n >= 1 && seg[0] == "domainnames":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateDomainName"
			case http.MethodGet:
				return "GetDomainNames"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetDomainName"
			case http.MethodPatch:
				return "UpdateDomainName"
			case http.MethodDelete:
				return "DeleteDomainName"
			}
		}
		// /domainnames/{name}/apimappings
		if n >= 3 && seg[2] == "apimappings" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateApiMapping"
				case http.MethodGet:
					return "GetApiMappings"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetApiMapping"
				case http.MethodPatch:
					return "UpdateApiMapping"
				case http.MethodDelete:
					return "DeleteApiMapping"
				}
			}
		}

	// VPC Links
	case n >= 1 && seg[0] == "vpclinks":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateVpcLink"
			case http.MethodGet:
				return "GetVpcLinks"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetVpcLink"
			case http.MethodPatch:
				return "UpdateVpcLink"
			case http.MethodDelete:
				return "DeleteVpcLink"
			}
		}

	// APIs
	case n >= 1 && seg[0] == "apis":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateApi"
			case http.MethodGet:
				return "GetApis"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetApi"
			case http.MethodPatch:
				return "UpdateApi"
			case http.MethodDelete:
				return "DeleteApi"
			}
		}
		if n >= 3 {
			resource := seg[2]
			switch resource {
			case "routes":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateRoute"
					case http.MethodGet:
						return "GetRoutes"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetRoute"
					case http.MethodPatch:
						return "UpdateRoute"
					case http.MethodDelete:
						return "DeleteRoute"
					}
				}
				// /apis/{id}/routes/{id}/routeresponses
				if n >= 5 && seg[4] == "routeresponses" {
					if n == 5 {
						switch method {
						case http.MethodPost:
							return "CreateRouteResponse"
						case http.MethodGet:
							return "GetRouteResponses"
						}
					}
					if n == 6 {
						switch method {
						case http.MethodGet:
							return "GetRouteResponse"
						case http.MethodDelete:
							return "DeleteRouteResponse"
						}
					}
				}
				// /apis/{id}/routes/{id}/requestparameters/{key}
				if n >= 5 && seg[4] == "requestparameters" {
					return "DeleteRouteRequestParameter"
				}
			case "integrations":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateIntegration"
					case http.MethodGet:
						return "GetIntegrations"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetIntegration"
					case http.MethodPatch:
						return "UpdateIntegration"
					case http.MethodDelete:
						return "DeleteIntegration"
					}
				}
				// /apis/{id}/integrations/{id}/integrationresponses
				if n >= 5 && seg[4] == "integrationresponses" {
					if n == 5 {
						switch method {
						case http.MethodPost:
							return "CreateIntegrationResponse"
						case http.MethodGet:
							return "GetIntegrationResponses"
						}
					}
					if n == 6 {
						switch method {
						case http.MethodGet:
							return "GetIntegrationResponse"
						case http.MethodPatch:
							return "UpdateIntegrationResponse"
						case http.MethodDelete:
							return "DeleteIntegrationResponse"
						}
					}
				}
			case "authorizers":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateAuthorizer"
					case http.MethodGet:
						return "GetAuthorizers"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetAuthorizer"
					case http.MethodPatch:
						return "UpdateAuthorizer"
					case http.MethodDelete:
						return "DeleteAuthorizer"
					}
				}
			case "deployments":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateDeployment"
					case http.MethodGet:
						return "GetDeployments"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetDeployment"
					case http.MethodPatch:
						return "UpdateDeployment"
					case http.MethodDelete:
						return "DeleteDeployment"
					}
				}
			case "stages":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateStage"
					case http.MethodGet:
						return "GetStages"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetStage"
					case http.MethodPatch:
						return "UpdateStage"
					case http.MethodDelete:
						return "DeleteStage"
					}
				}
			case "models":
				if n == 3 {
					switch method {
					case http.MethodPost:
						return "CreateModel"
					case http.MethodGet:
						return "GetModels"
					}
				}
				if n == 4 {
					switch method {
					case http.MethodGet:
						return "GetModel"
					case http.MethodPatch:
						return "UpdateModel"
					case http.MethodDelete:
						return "DeleteModel"
					}
				}
			case "cors":
				if method == http.MethodDelete {
					return "DeleteCorsConfiguration"
				}
			case "exports":
				return "ExportApi"
			}
		}
	}

	return ""
}

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
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

func boolParamDefault(params map[string]any, key string, def bool) bool {
	if v, ok := params[key].(bool); ok {
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
