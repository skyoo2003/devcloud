// SPDX-License-Identifier: Apache-2.0

// internal/services/elasticsearchservice/provider.go
package elasticsearchservice

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

// Provider implements the ElasticsearchService2015 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "elasticsearchservice" }
func (p *Provider) ServiceName() string           { return "ElasticsearchService2015" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "elasticsearchservice"))
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
	// Core CRUD
	case "CreateElasticsearchDomain":
		return p.createDomain(params)
	case "DeleteElasticsearchDomain":
		name := extractPathParam(req.URL.Path, "domain")
		return p.deleteDomain(name)
	case "DescribeElasticsearchDomain":
		name := extractPathParam(req.URL.Path, "domain")
		return p.describeDomain(name)
	case "DescribeElasticsearchDomains":
		return p.describeDomains(params)
	case "ListDomainNames":
		return p.listDomainNames()
	case "UpdateElasticsearchDomainConfig":
		name := extractPathParam(req.URL.Path, "domain")
		return p.updateDomainConfig(name, params)
	case "DescribeElasticsearchDomainConfig":
		name := extractPathParam(req.URL.Path, "domain")
		return p.describeDomainConfig(name)

	// Tags
	case "AddTags":
		return p.addTags(params)
	case "RemoveTags":
		return p.removeTags(params)
	case "ListTags":
		return p.listTags(req)

	// Info / Static
	case "GetCompatibleElasticsearchVersions":
		return p.getCompatibleVersions()
	case "ListElasticsearchVersions":
		return p.listElasticsearchVersions()
	case "ListElasticsearchInstanceTypes":
		return p.listInstanceTypes()
	case "DescribeElasticsearchInstanceTypeLimits":
		return p.describeInstanceTypeLimits()

	// Stub operations — validate inputs, return success/empty
	case "AcceptInboundCrossClusterSearchConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnection": map[string]any{}})
	case "AssociatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetails": map[string]any{}})
	case "AuthorizeVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AuthorizedPrincipal": map[string]any{}})
	case "CancelDomainConfigChange":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DryRun": false, "CancelledChangeIds": []any{}, "CancelledChangeProperties": []any{}})
	case "CancelElasticsearchServiceSoftwareUpdate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceSoftwareOptions": map[string]any{}})
	case "CreateOutboundCrossClusterSearchConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnectionId": shared.GenerateUUID(), "ConnectionStatus": map[string]any{"StatusCode": "PENDING_ACCEPTANCE"}})
	case "CreatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "CreateVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoint": map[string]any{}})
	case "DeleteElasticsearchServiceRole":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteInboundCrossClusterSearchConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnection": map[string]any{}})
	case "DeleteOutboundCrossClusterSearchConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnection": map[string]any{}})
	case "DeletePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "DeleteVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummary": map[string]any{}})
	case "DescribeDomainAutoTunes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AutoTunes": []any{}, "NextToken": ""})
	case "DescribeDomainChangeProgress":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ChangeProgressStatus": map[string]any{"ChangeProgressStages": []any{}, "CompletedProperties": []any{}, "PendingProperties": []any{}}})
	case "DescribeInboundCrossClusterSearchConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnections": []any{}, "NextToken": ""})
	case "DescribeOutboundCrossClusterSearchConnections":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnections": []any{}, "NextToken": ""})
	case "DescribePackages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetailsList": []any{}, "NextToken": ""})
	case "DescribeReservedElasticsearchInstanceOfferings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedElasticsearchInstanceOfferings": []any{}, "NextToken": ""})
	case "DescribeReservedElasticsearchInstances":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservedElasticsearchInstances": []any{}, "NextToken": ""})
	case "DescribeVpcEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoints": []any{}, "VpcEndpointErrorList": []any{}})
	case "DissociatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetails": map[string]any{}})
	case "GetPackageVersionHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageVersionHistoryList": []any{}, "NextToken": ""})
	case "GetUpgradeHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"UpgradeHistories": []any{}, "NextToken": ""})
	case "GetUpgradeStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"UpgradeName": "", "StepStatus": "SUCCEEDED", "UpgradeStep": "UPGRADE"})
	case "ListDomainsForPackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}, "NextToken": ""})
	case "ListPackagesForDomain":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DomainPackageDetailsList": []any{}, "NextToken": ""})
	case "ListVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AuthorizedPrincipalList": []any{}, "NextToken": ""})
	case "ListVpcEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummaryList": []any{}, "NextToken": ""})
	case "ListVpcEndpointsForDomain":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpointSummaryList": []any{}, "NextToken": ""})
	case "PurchaseReservedElasticsearchInstanceOffering":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ReservationName": "", "ReservedElasticsearchInstanceId": shared.GenerateUUID()})
	case "RejectInboundCrossClusterSearchConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CrossClusterSearchConnection": map[string]any{}})
	case "RevokeVpcEndpointAccess":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "StartElasticsearchServiceSoftwareUpdate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ServiceSoftwareOptions": map[string]any{}})
	case "UpdatePackage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PackageDetails": map[string]any{}})
	case "UpdateVpcEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"VpcEndpoint": map[string]any{}})
	case "UpgradeElasticsearchDomain":
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
		res = append(res, plugin.Resource{Type: "elasticsearch-domain", ID: d.Name, Name: d.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Core CRUD ---

func (p *Provider) createDomain(params map[string]any) (*plugin.Response, error) {
	name, _ := params["DomainName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "DomainName is required", http.StatusBadRequest), nil
	}
	engineVersion, _ := params["ElasticsearchVersion"].(string)
	if engineVersion == "" {
		engineVersion = "7.10"
	}
	instanceType := "m5.large.elasticsearch"
	instanceCount := 1
	if ec, ok := params["ElasticsearchClusterConfig"].(map[string]any); ok {
		if it, ok := ec["InstanceType"].(string); ok && it != "" {
			instanceType = it
		}
		if ic, ok := ec["InstanceCount"].(float64); ok {
			instanceCount = int(ic)
		}
	}

	arn := shared.BuildARN("es", "domain", name)
	domainID := shared.GenerateID("", 32)

	d, err := p.store.CreateDomain(name, arn, domainID, engineVersion, instanceType, instanceCount)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "domain already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["TagList"].([]any); ok {
		p.store.tags.AddTags(d.ARN, parseTags(rawTags))
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
	p.store.tags.DeleteAllTags(d.ARN)
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
		list = append(list, map[string]any{"DomainName": d.Name})
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

	// Merge new config into existing
	var existing map[string]any
	json.Unmarshal([]byte(d.Config), &existing)
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
		"CompatibleElasticsearchVersions": []map[string]any{
			{"SourceVersion": "7.10", "TargetVersions": []string{"7.10"}},
			{"SourceVersion": "7.9", "TargetVersions": []string{"7.10"}},
			{"SourceVersion": "7.8", "TargetVersions": []string{"7.9", "7.10"}},
		},
	})
}

func (p *Provider) listElasticsearchVersions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ElasticsearchVersions": []string{
			"7.10", "7.9", "7.8", "7.7", "7.4", "7.1",
			"6.8", "6.7", "6.5", "6.4", "6.3", "6.2", "6.0",
			"5.6", "5.5", "5.3", "5.1",
			"2.3", "1.5",
		},
		"NextToken": "",
	})
}

func (p *Provider) listInstanceTypes() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ElasticsearchInstanceTypes": []string{
			"t2.micro.elasticsearch",
			"t2.small.elasticsearch",
			"t2.medium.elasticsearch",
			"m5.large.elasticsearch",
			"m5.xlarge.elasticsearch",
			"m5.2xlarge.elasticsearch",
			"c5.large.elasticsearch",
			"c5.xlarge.elasticsearch",
			"r5.large.elasticsearch",
			"r5.xlarge.elasticsearch",
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
						"MaximumInstanceCount": 40,
					},
				},
				"StorageTypes":     []any{},
				"AdditionalLimits": []any{},
			},
		},
	})
}

// resolveOp maps HTTP method+path to an Elasticsearch operation name.
func resolveOp(method, path string) string {
	// Strip the /2015-01-01 prefix
	path = strings.TrimPrefix(path, "/2015-01-01")
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	switch segs[0] {
	case "es":
		if n >= 2 {
			switch segs[1] {
			case "domain":
				if n == 2 {
					return "CreateElasticsearchDomain" // POST
				}
				if n == 3 {
					switch method {
					case "GET":
						return "DescribeElasticsearchDomain"
					case "DELETE":
						return "DeleteElasticsearchDomain"
					}
				}
				if n == 4 && segs[3] == "config" {
					switch method {
					case "POST":
						return "UpdateElasticsearchDomainConfig"
					case "GET":
						return "DescribeElasticsearchDomainConfig"
					}
				}
			case "domain-info":
				return "DescribeElasticsearchDomains" // POST
			case "compatibleVersions":
				return "GetCompatibleElasticsearchVersions"
			case "versions":
				return "ListElasticsearchVersions"
			case "instanceTypes":
				return "ListElasticsearchInstanceTypes"
			case "instanceTypeLimits":
				return "DescribeElasticsearchInstanceTypeLimits"
			}
		}
	case "domain":
		return "ListDomainNames" // GET /2015-01-01/domain
	case "tags":
		switch method {
		case "GET":
			return "ListTags"
		case "POST":
			return "AddTags"
		}
	case "tags-removal":
		return "RemoveTags" // POST
	}
	return ""
}

// --- Helpers ---

func domainToStatus(d *Domain) map[string]any {
	return map[string]any{
		"DomainId":             d.DomainID,
		"DomainName":           d.Name,
		"ARN":                  d.ARN,
		"Created":              true,
		"Deleted":              false,
		"Endpoint":             d.Endpoint,
		"Processing":           false,
		"UpgradeProcessing":    false,
		"ElasticsearchVersion": d.EngineVersion,
		"ElasticsearchClusterConfig": map[string]any{
			"InstanceType":  d.InstanceType,
			"InstanceCount": d.InstanceCount,
		},
	}
}

func domainToConfig(d *Domain) map[string]any {
	var cfg map[string]any
	json.Unmarshal([]byte(d.Config), &cfg)
	if cfg == nil {
		cfg = map[string]any{}
	}
	return map[string]any{
		"ElasticsearchVersion": map[string]any{
			"Options": d.EngineVersion,
			"Status": map[string]any{
				"State": "Active",
			},
		},
		"ElasticsearchClusterConfig": map[string]any{
			"Options": map[string]any{
				"InstanceType":  d.InstanceType,
				"InstanceCount": d.InstanceCount,
			},
			"Status": map[string]any{
				"State": "Active",
			},
		},
		"AdditionalConfig": cfg,
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
