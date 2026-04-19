// SPDX-License-Identifier: Apache-2.0

// internal/services/opensearch/provider.go
package opensearch

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

// Provider implements the OpenSearchService service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "opensearch" }
func (p *Provider) ServiceName() string           { return "OpenSearchService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "opensearch"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
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
	// --- Domain CRUD ---
	case "CreateDomain":
		return p.createDomain(params)
	case "DeleteDomain":
		name := extractPathParam(req.URL.Path, "domain")
		return p.deleteDomain(name)
	case "DescribeDomain":
		name := extractPathParam(req.URL.Path, "domain")
		return p.describeDomain(name)
	case "DescribeDomains":
		return p.describeDomains(params)
	case "ListDomainNames":
		return p.listDomainNames()
	case "UpdateDomainConfig":
		name := extractPathParam(req.URL.Path, "domain")
		return p.updateDomainConfig(name, params)
	case "DescribeDomainConfig":
		name := extractPathParam(req.URL.Path, "domain")
		return p.describeDomainConfig(name)

	// --- Application CRUD ---
	case "CreateApplication":
		return p.createApplication(params)
	case "GetApplication":
		id := extractPathParam(req.URL.Path, "application")
		return p.getApplication(id)
	case "ListApplications":
		return p.listApplications()
	case "DeleteApplication":
		id := extractPathParam(req.URL.Path, "application")
		return p.deleteApplication(id)
	case "UpdateApplication":
		id := extractPathParam(req.URL.Path, "application")
		return p.updateApplication(id, params)

	// --- Tags ---
	case "AddTags":
		return p.addTags(params)
	case "RemoveTags":
		return p.removeTags(params)
	case "ListTags":
		return p.listTags(req)

	// --- Static Info ---
	case "GetCompatibleVersions":
		return p.getCompatibleVersions()
	case "ListVersions":
		return p.listVersions()
	case "ListInstanceTypeDetails":
		return p.listInstanceTypeDetails()
	case "DescribeInstanceTypeLimits":
		return p.describeInstanceTypeLimits()

	// --- Stub operations ---
	case "AcceptInboundConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"connection": map[string]any{}})
	case "AddDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Message": "DataSource added"})
	case "AddDirectQueryDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DataSourceArn": shared.BuildARN("opensearch", "datasource", "stub")})
	case "AssociatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetails": map[string]any{}})
	case "AssociatePackages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}})
	case "AuthorizeVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AuthorizedPrincipal": map[string]any{}})
	case "CancelDomainConfigChange":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DryRun": false, "CancelledChangeIds": []any{}, "CancelledChangeProperties": []any{}})
	case "CancelServiceSoftwareUpdate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceSoftwareOptions": map[string]any{}})
	case "CreateIndex":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CreateOutboundConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ConnectionId": shared.GenerateUUID(), "ConnectionStatus": map[string]any{"StatusCode": "PENDING_ACCEPTANCE"}})
	case "CreatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "CreateVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoint": map[string]any{}})
	case "DeleteDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Message": "DataSource deleted"})
	case "DeleteDirectQueryDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteInboundConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Connection": map[string]any{}})
	case "DeleteIndex":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteOutboundConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Connection": map[string]any{}})
	case "DeletePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "DeleteVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummary": map[string]any{}})
	case "DeregisterCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeDomainAutoTunes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AutoTunes": []any{}, "NextToken": ""})
	case "DescribeDomainChangeProgress":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ChangeProgressStatus": map[string]any{"ChangeProgressStages": []any{}, "CompletedProperties": []any{}, "PendingProperties": []any{}}})
	case "DescribeDomainHealth":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainState": "Active", "EnvironmentInformation": []any{}})
	case "DescribeDomainNodes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainNodesStatusList": []any{}})
	case "DescribeDryRunProgress":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DryRunProgressStatus": map[string]any{"DryRunId": "", "DryRunStatus": "succeeded", "CreationDate": "", "UpdateDate": ""}, "DryRunResults": map[string]any{}})
	case "DescribeInboundConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Connections": []any{}, "NextToken": ""})
	case "DescribeInsightDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InsightDetails": map[string]any{}})
	case "DescribeOutboundConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Connections": []any{}, "NextToken": ""})
	case "DescribePackages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetailsList": []any{}, "NextToken": ""})
	case "DescribeReservedInstanceOfferings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedInstanceOfferings": []any{}, "NextToken": ""})
	case "DescribeReservedInstances":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedInstances": []any{}, "NextToken": ""})
	case "DescribeVpcEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoints": []any{}, "VpcEndpointErrorList": []any{}})
	case "DissociatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetails": map[string]any{}})
	case "DissociatePackages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}})
	case "GetCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Capability": map[string]any{}})
	case "GetDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DataSourceType": map[string]any{}, "Description": "", "Name": ""})
	case "GetDefaultApplicationSetting":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationSettings": []any{}, "NextToken": ""})
	case "GetDirectQueryDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DataSourceName": "", "DataSourceType": map[string]any{}, "OpenSearchArns": []any{}})
	case "GetDomainMaintenanceStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "COMPLETED", "Action": ""})
	case "GetIndex":
		return shared.JSONResponse(http.StatusOK, map[string]any{"IndexName": "", "IndexStatus": map[string]any{}})
	case "GetPackageVersionHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageVersionHistoryList": []any{}, "NextToken": ""})
	case "GetUpgradeHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"UpgradeHistories": []any{}, "NextToken": ""})
	case "GetUpgradeStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"UpgradeName": "", "StepStatus": "SUCCEEDED", "UpgradeStep": "UPGRADE"})
	case "ListDataSources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DataSources": []any{}})
	case "ListDirectQueryDataSources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DirectQueryDataSources": []any{}, "NextToken": ""})
	case "ListDomainMaintenances":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainMaintenances": []any{}, "NextToken": ""})
	case "ListDomainsForPackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}, "NextToken": ""})
	case "ListInsights":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InsightsSummaries": []any{}, "NextToken": ""})
	case "ListPackagesForDomain":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}, "NextToken": ""})
	case "ListScheduledActions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ScheduledActions": []any{}, "NextToken": ""})
	case "ListVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AuthorizedPrincipalList": []any{}, "NextToken": ""})
	case "ListVpcEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummaryList": []any{}, "NextToken": ""})
	case "ListVpcEndpointsForDomain":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummaryList": []any{}, "NextToken": ""})
	case "PurchaseReservedInstanceOffering":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservationName": "", "ReservedInstanceId": shared.GenerateUUID()})
	case "PutDefaultApplicationSetting":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RegisterCapability":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RejectInboundConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Connection": map[string]any{}})
	case "RevokeVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "StartDomainMaintenance":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MaintenanceId": shared.GenerateUUID()})
	case "StartServiceSoftwareUpdate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceSoftwareOptions": map[string]any{}})
	case "UpdateDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Message": "DataSource updated"})
	case "UpdateDirectQueryDataSource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DataSourceArn": ""})
	case "UpdateIndex":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "UpdatePackageScope":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageID": "", "Operation": "", "PackageScopeOperationStatus": "COMPLETED"})
	case "UpdateScheduledAction":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ScheduledAction": map[string]any{}})
	case "UpdateVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoint": map[string]any{}})
	case "UpgradeDomain":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainName": "", "PerformCheckOnly": false, "TargetVersion": ""})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	domains, err := p.store.ListDomains()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(domains))
	for _, d := range domains {
		res = append(res, plugin.Resource{Type: "opensearch-domain", ID: d.Name, Name: d.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Domain CRUD ---

func (p *Provider) createDomain(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DomainName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	engineVersion, _ := params["EngineVersion"].(string)
	if engineVersion == "" {
		engineVersion = "OpenSearch_2.11"
	}

	arn := shared.BuildARN("es", "domain", name)
	domainID := shared.GenerateID("", 32)

	d, err := p.store.CreateDomain(name, arn, domainID, engineVersion)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "domain already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["TagList"].([]any); ok {
		_ = p.store.tags.AddTags(d.ARN, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainStatus": domainToStatus(d),
	})
}

func (p *Provider) deleteDomain(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(d.ARN)
	if err := p.store.DeleteDomain(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainStatus": domainToStatus(d),
	})
}

func (p *Provider) describeDomain(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainStatus": domainToStatus(d),
	})
}

func (p *Provider) describeDomains(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["DomainNames"].([]any)
	var results []map[string]any
	for _, n := range rawNames {
		name, _ := n.(string)
		if name == "" {
			continue
		}
		d, err := p.store.GetDomain(name)
		if err != nil {
			continue
		}
		results = append(results, domainToStatus(d))
	}
	if results == nil {
		results = []map[string]any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainStatusList": results,
	})
}

func (p *Provider) listDomainNames() (*plugin.Response, error) {
	domains, err := p.store.ListDomains()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(domains))
	for _, d := range domains {
		list = append(list, map[string]any{"DomainName": d.Name, "EngineType": "OpenSearch"})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainNames": list,
	})
}

func (p *Provider) updateDomainConfig(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}

	var existing map[string]any
	_ = json.Unmarshal([]byte(d.Config), &existing)
	if existing == nil {
		existing = map[string]any{}
	}
	for k, v := range params {
		existing[k] = v
	}
	configBytes, _ := json.Marshal(existing)

	if err := p.store.UpdateDomainConfig(name, string(configBytes)); err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}

	d.Config = string(configBytes)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainConfig": domainToConfig(d),
	})
}

func (p *Provider) describeDomainConfig(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomain(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "domain not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DomainConfig": domainToConfig(d),
	})
}

// --- Application CRUD ---

func (p *Provider) createApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	arn := shared.BuildARN("opensearch", "application", id)

	app, err := p.store.CreateApplication(id, arn, name)
	if err != nil {
		return nil, err
	}

	if rawTags, ok := params["tagList"].([]any); ok {
		_ = p.store.tags.AddTags(app.ARN, parseTags(rawTags))
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"id":                       app.ID,
		"arn":                      app.ARN,
		"name":                     app.Name,
		"iamIdentityCenterOptions": map[string]any{},
		"appConfigs":               []any{},
		"dataSources":              []any{},
	})
}

func (p *Provider) getApplication(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, appToResponse(app))
}

func (p *Provider) listApplications() (*plugin.Response, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(apps))
	for i := range apps {
		list = append(list, appToResponse(&apps[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationSummaries": list,
		"NextToken":            "",
	})
}

func (p *Provider) deleteApplication(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteApplication(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateApplication(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}

	var existing map[string]any
	_ = json.Unmarshal([]byte(app.Config), &existing)
	if existing == nil {
		existing = map[string]any{}
	}
	for k, v := range params {
		existing[k] = v
	}
	configBytes, _ := json.Marshal(existing)

	if err := p.store.UpdateApplication(id, string(configBytes)); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}

	app.Config = string(configBytes)
	return shared.JSONResponse(http.StatusOK, appToResponse(app))
}

// --- Tags ---

func (p *Provider) addTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["TagList"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) removeTags(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ARN is required", http.StatusBadRequest), nil
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

func (p *Provider) listTags(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("arn")
	if arn == "" {
		return shared.JSONError("ValidationException", "arn query parameter is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TagList": tagList,
	})
}

// --- Static Info ---

func (p *Provider) getCompatibleVersions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CompatibleVersions": []map[string]any{
			{"SourceVersion": "OpenSearch_2.11", "TargetVersions": []string{"OpenSearch_2.11"}},
			{"SourceVersion": "OpenSearch_2.9", "TargetVersions": []string{"OpenSearch_2.11"}},
			{"SourceVersion": "OpenSearch_2.7", "TargetVersions": []string{"OpenSearch_2.9", "OpenSearch_2.11"}},
			{"SourceVersion": "Elasticsearch_7.10", "TargetVersions": []string{"OpenSearch_1.3", "OpenSearch_2.11"}},
		},
	})
}

func (p *Provider) listVersions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Versions": []string{
			"OpenSearch_2.11",
			"OpenSearch_2.9",
			"OpenSearch_2.7",
			"OpenSearch_2.5",
			"OpenSearch_2.3",
			"OpenSearch_1.3",
			"OpenSearch_1.2",
			"OpenSearch_1.1",
			"OpenSearch_1.0",
			"Elasticsearch_7.10",
			"Elasticsearch_7.9",
			"Elasticsearch_7.8",
			"Elasticsearch_7.7",
			"Elasticsearch_7.4",
			"Elasticsearch_7.1",
		},
		"NextToken": "",
	})
}

func (p *Provider) listInstanceTypeDetails() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"InstanceTypeDetails": []map[string]any{
			{"InstanceType": "t3.small.search", "EncryptionEnabled": true, "CognitoEnabled": false, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": false},
			{"InstanceType": "t3.medium.search", "EncryptionEnabled": true, "CognitoEnabled": false, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": false},
			{"InstanceType": "m6g.large.search", "EncryptionEnabled": true, "CognitoEnabled": true, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": true},
			{"InstanceType": "m6g.xlarge.search", "EncryptionEnabled": true, "CognitoEnabled": true, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": true},
			{"InstanceType": "c6g.large.search", "EncryptionEnabled": true, "CognitoEnabled": true, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": true},
			{"InstanceType": "r6g.large.search", "EncryptionEnabled": true, "CognitoEnabled": true, "AppLogsEnabled": true, "AdvancedSecurityEnabled": true, "WarmEnabled": true},
		},
		"NextToken": "",
	})
}

func (p *Provider) describeInstanceTypeLimits() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LimitsByRole": map[string]any{
			"data": map[string]any{
				"InstanceLimits": map[string]any{
					"InstanceCountLimits": map[string]any{
						"MinimumInstanceCount": 1,
						"MaximumInstanceCount": 80,
					},
				},
				"StorageTypes":     []any{},
				"AdditionalLimits": []any{},
			},
		},
	})
}

// --- Helpers ---

func domainToStatus(d *Domain) map[string]any {
	return map[string]any{
		"DomainId":      d.DomainID,
		"DomainName":    d.Name,
		"ARN":           d.ARN,
		"Created":       true,
		"Deleted":       false,
		"Endpoint":      d.Endpoint,
		"Processing":    false,
		"EngineVersion": d.EngineVersion,
		"ClusterConfig": map[string]any{
			"InstanceType":  "m6g.large.search",
			"InstanceCount": 1,
		},
	}
}

func domainToConfig(d *Domain) map[string]any {
	var cfg map[string]any
	_ = json.Unmarshal([]byte(d.Config), &cfg)
	if cfg == nil {
		cfg = map[string]any{}
	}
	return map[string]any{
		"EngineVersion": map[string]any{
			"Options": d.EngineVersion,
			"Status": map[string]any{
				"State": "Active",
			},
		},
		"ClusterConfig": map[string]any{
			"Options": map[string]any{
				"InstanceType":  "m6g.large.search",
				"InstanceCount": 1,
			},
			"Status": map[string]any{
				"State": "Active",
			},
		},
		"AdditionalConfig": cfg,
	}
}

func appToResponse(app *Application) map[string]any {
	return map[string]any{
		"id":                       app.ID,
		"arn":                      app.ARN,
		"name":                     app.Name,
		"status":                   app.Status,
		"iamIdentityCenterOptions": map[string]any{},
		"appConfigs":               []any{},
		"dataSources":              []any{},
		"createdAt":                app.CreatedAt.Unix(),
		"lastUpdatedAt":            app.CreatedAt.Unix(),
	}
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

// resolveOp derives the operation name from the HTTP method and URL path
// for REST-JSON requests that lack an X-Amz-Target header.
func resolveOp(method, path string) string {
	// Normalise: strip trailing slash, split into segments.
	path = strings.TrimRight(path, "/")
	parts := strings.Split(path, "/")

	// Build a segment list without the version prefix (e.g. "2021-01-01").
	// Expected paths look like /2021-01-01/opensearch/domain/...
	var segs []string
	for _, p := range parts {
		if p == "" {
			continue
		}
		// Skip version-like segments (e.g. "2021-01-01").
		if len(p) == 10 && p[4] == '-' && p[7] == '-' {
			continue
		}
		segs = append(segs, p)
	}

	n := len(segs)
	if n == 0 {
		return ""
	}

	// Match against known URL patterns.
	switch {
	// --- Tags ---
	case segs[0] == "tags" && method == "POST":
		return "AddTags"
	case segs[0] == "tags" && method == "GET":
		return "ListTags"
	case segs[0] == "tags-removal" && method == "POST":
		return "RemoveTags"

	// --- Domain list (no "opensearch" prefix) ---
	case segs[0] == "domain" && n == 1 && method == "GET":
		return "ListDomainNames"

	// --- Application ---
	case segs[0] == "application" && n == 1 && method == "POST":
		return "CreateApplication"
	case segs[0] == "application" && n == 1 && method == "GET":
		return "ListApplications"
	case segs[0] == "application" && n == 2 && method == "GET":
		return "GetApplication"
	case segs[0] == "application" && n == 2 && method == "DELETE":
		return "DeleteApplication"
	case segs[0] == "application" && n == 2 && method == "PUT":
		return "UpdateApplication"

	// --- OpenSearch prefixed paths ---
	case n >= 2 && segs[0] == "opensearch":
		return resolveOpenSearchOp(method, segs[1:])
	}

	return ""
}

func resolveOpenSearchOp(method string, segs []string) string {
	n := len(segs)
	switch {
	// POST /opensearch/describeDomains
	case segs[0] == "describeDomains" && method == "POST":
		return "DescribeDomains"

	// /opensearch/domain
	case segs[0] == "domain" && n == 1 && method == "POST":
		return "CreateDomain"
	case segs[0] == "domain" && n == 2 && method == "GET":
		return "DescribeDomain"
	case segs[0] == "domain" && n == 2 && method == "DELETE":
		return "DeleteDomain"
	case segs[0] == "domain" && n == 3 && segs[2] == "config" && method == "POST":
		return "UpdateDomainConfig"
	case segs[0] == "domain" && n == 3 && segs[2] == "config" && method == "GET":
		return "DescribeDomainConfig"

	// /opensearch/versions
	case segs[0] == "versions" && method == "GET":
		return "ListVersions"
	// /opensearch/compatibleVersions
	case segs[0] == "compatibleVersions" && method == "GET":
		return "GetCompatibleVersions"
	// /opensearch/instanceTypeDetails/{version}
	case segs[0] == "instanceTypeDetails" && n == 2 && method == "GET":
		return "ListInstanceTypeDetails"
	// /opensearch/instanceTypeLimits/{version}/{instanceType}
	case segs[0] == "instanceTypeLimits" && n == 3 && method == "GET":
		return "DescribeInstanceTypeLimits"
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

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
