// SPDX-License-Identifier: Apache-2.0

// internal/services/lakeformation/provider.go
package lakeformation

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

// Provider implements the LakeFormation service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "lakeformation" }
func (p *Provider) ServiceName() string           { return "LakeFormation" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "lakeformation"))
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
	// LFTag CRUD
	case "CreateLFTag":
		return p.createLFTag(params)
	case "GetLFTag":
		return p.getLFTag(params)
	case "UpdateLFTag":
		return p.updateLFTag(params)
	case "DeleteLFTag":
		return p.deleteLFTag(params)
	case "ListLFTags":
		return p.listLFTags(params)

	// LFTagExpression CRUD
	case "CreateLFTagExpression":
		return p.createLFTagExpression(params)
	case "GetLFTagExpression":
		return p.getLFTagExpression(params)
	case "UpdateLFTagExpression":
		return p.updateLFTagExpression(params)
	case "DeleteLFTagExpression":
		return p.deleteLFTagExpression(params)
	case "ListLFTagExpressions":
		return p.listLFTagExpressions(params)

	// Permissions
	case "GrantPermissions":
		return p.grantPermissions(params)
	case "RevokePermissions":
		return p.revokePermissions(params)
	case "ListPermissions":
		return p.listPermissions(params)
	case "BatchGrantPermissions":
		return p.batchGrantPermissions(params)
	case "BatchRevokePermissions":
		return p.batchRevokePermissions(params)
	case "GetEffectivePermissionsForPath":
		return p.getEffectivePermissionsForPath(params)

	// Resource registration
	case "RegisterResource":
		return p.registerResource(params)
	case "DeregisterResource":
		return p.deregisterResource(params)
	case "DescribeResource":
		return p.describeResource(params)
	case "UpdateResource":
		return p.updateResource(params)
	case "ListResources":
		return p.listResources(params)

	// Data Lake Settings
	case "GetDataLakeSettings":
		return p.getDataLakeSettings(params)
	case "PutDataLakeSettings":
		return p.putDataLakeSettings(params)

	// DataCellsFilter CRUD
	case "CreateDataCellsFilter":
		return p.createDataCellsFilter(params)
	case "GetDataCellsFilter":
		return p.getDataCellsFilter(params)
	case "UpdateDataCellsFilter":
		return p.updateDataCellsFilter(params)
	case "DeleteDataCellsFilter":
		return p.deleteDataCellsFilter(params)
	case "ListDataCellsFilter":
		return p.listDataCellsFilter(params)

	// LF Tags on resources
	case "AddLFTagsToResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Failures": []any{}})
	case "RemoveLFTagsFromResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Failures": []any{}})
	case "GetResourceLFTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{"LFTagOnDatabase": []any{}, "LFTagsOnColumns": []any{}, "LFTagsOnTable": []any{}})
	case "SearchDatabasesByLFTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{"DatabaseList": []any{}, "NextToken": ""})
	case "SearchTablesByLFTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextToken": "", "TableList": []any{}})

	// Transaction stubs
	case "StartTransaction":
		return shared.JSONResponse(http.StatusOK, map[string]any{"TransactionId": shared.GenerateUUID()})
	case "CommitTransaction":
		return shared.JSONResponse(http.StatusOK, map[string]any{"TransactionStatus": "COMMITTED"})
	case "CancelTransaction":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ExtendTransaction":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeTransaction":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Transaction": map[string]any{"TransactionStatus": "ACTIVE"}})
	case "ListTransactions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextToken": "", "Transactions": []any{}})

	// OptIn stubs
	case "CreateLakeFormationOptIn":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteLakeFormationOptIn":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListLakeFormationOptIns":
		return shared.JSONResponse(http.StatusOK, map[string]any{"LakeFormationOptInsInfoList": []any{}, "NextToken": ""})

	// IdentityCenter stubs
	case "CreateLakeFormationIdentityCenterConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationArn": ""})
	case "DeleteLakeFormationIdentityCenterConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeLakeFormationIdentityCenterConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CatalogId": "", "InstanceArn": "", "ApplicationArn": "", "ExternalFiltering": map[string]any{}, "ShareRecipients": []any{}})
	case "UpdateLakeFormationIdentityCenterConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// Credentials stubs
	case "GetTemporaryGlueTableCredentials":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccessKeyId": "ASIA" + shared.GenerateID("", 16), "SecretAccessKey": shared.GenerateID("", 40), "SessionToken": shared.GenerateID("", 100), "Expiration": ""})
	case "GetTemporaryGluePartitionCredentials":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccessKeyId": "ASIA" + shared.GenerateID("", 16), "SecretAccessKey": shared.GenerateID("", 40), "SessionToken": shared.GenerateID("", 100), "Expiration": ""})
	case "GetTemporaryDataLocationCredentials":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccessKeyId": "ASIA" + shared.GenerateID("", 16), "SecretAccessKey": shared.GenerateID("", 40), "SessionToken": shared.GenerateID("", 100), "Expiration": ""})
	case "AssumeDecoratedRoleWithSAML":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AccessKeyId": "ASIA" + shared.GenerateID("", 16), "SecretAccessKey": shared.GenerateID("", 40), "SessionToken": shared.GenerateID("", 100), "Expiration": ""})

	// Query planning stubs
	case "StartQueryPlanning":
		return shared.JSONResponse(http.StatusOK, map[string]any{"QueryId": shared.GenerateUUID()})
	case "GetQueryState":
		return shared.JSONResponse(http.StatusOK, map[string]any{"State": "FINISHED"})
	case "GetQueryStatistics":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ExecutionStatistics": map[string]any{}, "PlanningStatistics": map[string]any{}})
	case "GetWorkUnits":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextToken": "", "QueryId": "", "WorkUnitRanges": []any{}})
	case "GetWorkUnitResults":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ResultStream": ""})

	// Table objects stubs
	case "GetTableObjects":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextToken": "", "Objects": []any{}})
	case "UpdateTableObjects":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteObjectsOnCancel":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// Storage optimizer stubs
	case "ListTableStorageOptimizers":
		return shared.JSONResponse(http.StatusOK, map[string]any{"NextToken": "", "StorageOptimizerList": []any{}})
	case "UpdateTableStorageOptimizer":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Result": ""})

	// Misc stubs
	case "GetDataLakePrincipal":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Identity": ""})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	resources, err := p.store.ListResources()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(resources))
	for _, r := range resources {
		res = append(res, plugin.Resource{Type: "lakeformation-resource", ID: r.ARN, Name: r.ARN})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- LFTag ---

func (p *Provider) createLFTag(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	key, _ := params["TagKey"].(string)
	if key == "" {
		return shared.JSONError("ValidationException", "TagKey is required", http.StatusBadRequest), nil
	}
	rawValues, _ := params["TagValues"].([]any)
	valuesJSON := marshalJSON(rawValues)

	if err := p.store.CreateLFTag(catalogID, key, valuesJSON); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "LF tag already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getLFTag(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	key, _ := params["TagKey"].(string)
	if key == "" {
		return shared.JSONError("ValidationException", "TagKey is required", http.StatusBadRequest), nil
	}
	tag, err := p.store.GetLFTag(catalogID, key)
	if err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag not found", http.StatusNotFound), nil
	}
	var values []string
	_ = json.Unmarshal([]byte(tag.ValuesJSON), &values)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CatalogId": tag.CatalogID,
		"TagKey":    tag.Key,
		"TagValues": values,
	})
}

func (p *Provider) updateLFTag(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	key, _ := params["TagKey"].(string)
	if key == "" {
		return shared.JSONError("ValidationException", "TagKey is required", http.StatusBadRequest), nil
	}
	// Fetch existing values
	tag, err := p.store.GetLFTag(catalogID, key)
	if err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag not found", http.StatusNotFound), nil
	}
	var existing []string
	_ = json.Unmarshal([]byte(tag.ValuesJSON), &existing)

	// Add new values
	addRaw, _ := params["TagValuesToAdd"].([]any)
	for _, v := range addRaw {
		if s, ok := v.(string); ok {
			if !containsStr(existing, s) {
				existing = append(existing, s)
			}
		}
	}
	// Delete values
	delRaw, _ := params["TagValuesToDelete"].([]any)
	for _, v := range delRaw {
		if s, ok := v.(string); ok {
			existing = removeStr(existing, s)
		}
	}

	valuesJSON := marshalJSON(existing)
	if err := p.store.UpdateLFTag(catalogID, key, valuesJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteLFTag(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	key, _ := params["TagKey"].(string)
	if key == "" {
		return shared.JSONError("ValidationException", "TagKey is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLFTag(catalogID, key); err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listLFTags(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	tags, err := p.store.ListLFTags(catalogID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(tags))
	for _, t := range tags {
		var values []string
		_ = json.Unmarshal([]byte(t.ValuesJSON), &values)
		list = append(list, map[string]any{
			"CatalogId": t.CatalogID,
			"TagKey":    t.Key,
			"TagValues": values,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LFTags":    list,
		"NextToken": "",
	})
}

// --- LFTagExpression ---

func (p *Provider) createLFTagExpression(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	catalogID := strParam(params, "CatalogId", "000000000000")
	description, _ := params["Description"].(string)
	rawExpr, _ := params["Expression"].([]any)
	exprJSON := marshalJSON(rawExpr)

	if err := p.store.CreateLFTagExpression(name, catalogID, description, exprJSON); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "LF tag expression already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getLFTagExpression(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	expr, err := p.store.GetLFTagExpression(name)
	if err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag expression not found", http.StatusNotFound), nil
	}
	var expression []any
	_ = json.Unmarshal([]byte(expr.Expression), &expression)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Name":        expr.Name,
		"CatalogId":   expr.CatalogID,
		"Description": expr.Description,
		"Expression":  expression,
	})
}

func (p *Provider) updateLFTagExpression(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetLFTagExpression(name); err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag expression not found", http.StatusNotFound), nil
	}
	description, _ := params["Description"].(string)
	rawExpr, _ := params["Expression"].([]any)
	exprJSON := marshalJSON(rawExpr)

	if err := p.store.UpdateLFTagExpression(name, description, exprJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteLFTagExpression(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLFTagExpression(name); err != nil {
		return shared.JSONError("EntityNotFoundException", "LF tag expression not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listLFTagExpressions(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	exprs, err := p.store.ListLFTagExpressions(catalogID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(exprs))
	for _, e := range exprs {
		var expression []any
		_ = json.Unmarshal([]byte(e.Expression), &expression)
		list = append(list, map[string]any{
			"Name":        e.Name,
			"CatalogId":   e.CatalogID,
			"Description": e.Description,
			"Expression":  expression,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LFTagExpressions": list,
		"NextToken":        "",
	})
}

// --- Permissions ---

func (p *Provider) grantPermissions(params map[string]any) (*plugin.Response, error) {
	principal, _ := params["Principal"].(map[string]any)
	if principal == nil {
		return shared.JSONError("ValidationException", "Principal is required", http.StatusBadRequest), nil
	}
	resource, _ := params["Resource"].(map[string]any)
	if resource == nil {
		return shared.JSONError("ValidationException", "Resource is required", http.StatusBadRequest), nil
	}
	principalStr := marshalJSON(principal)
	resourceJSON := marshalJSON(resource)
	perms, _ := params["Permissions"].([]any)
	permissionsJSON := marshalJSON(perms)
	grantOpt, _ := params["PermissionsWithGrantOption"].([]any)
	grantOptJSON := marshalJSON(grantOpt)

	id := shared.GenerateUUID()
	if err := p.store.GrantPermission(id, principalStr, resourceJSON, permissionsJSON, grantOptJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) revokePermissions(params map[string]any) (*plugin.Response, error) {
	principal, _ := params["Principal"].(map[string]any)
	if principal == nil {
		return shared.JSONError("ValidationException", "Principal is required", http.StatusBadRequest), nil
	}
	principalStr := marshalJSON(principal)

	perms, err := p.store.ListPermissions()
	if err != nil {
		return nil, err
	}
	for _, perm := range perms {
		if perm.Principal == principalStr {
			p.store.RevokePermission(perm.ID) //nolint:errcheck
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listPermissions(params map[string]any) (*plugin.Response, error) {
	perms, err := p.store.ListPermissions()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(perms))
	for _, perm := range perms {
		var principal map[string]any
		var resource map[string]any
		var permissions []any
		var grantOption []any
		_ = json.Unmarshal([]byte(perm.Principal), &principal)
		_ = json.Unmarshal([]byte(perm.ResourceJSON), &resource)
		_ = json.Unmarshal([]byte(perm.PermissionsJSON), &permissions)
		_ = json.Unmarshal([]byte(perm.GrantOption), &grantOption)
		list = append(list, map[string]any{
			"Principal":                  principal,
			"Resource":                   resource,
			"Permissions":                permissions,
			"PermissionsWithGrantOption": grantOption,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PrincipalResourcePermissions": list,
		"NextToken":                    "",
	})
}

func (p *Provider) batchGrantPermissions(params map[string]any) (*plugin.Response, error) {
	entries, _ := params["Entries"].([]any)
	for _, e := range entries {
		entry, _ := e.(map[string]any)
		if entry == nil {
			continue
		}
		principal, _ := entry["Principal"].(map[string]any)
		resource, _ := entry["Resource"].(map[string]any)
		if principal == nil || resource == nil {
			continue
		}
		perms, _ := entry["Permissions"].([]any)
		grantOpt, _ := entry["PermissionsWithGrantOption"].([]any)
		id := shared.GenerateUUID()
		p.store.GrantPermission(id, marshalJSON(principal), marshalJSON(resource), marshalJSON(perms), marshalJSON(grantOpt)) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Failures": []any{}})
}

func (p *Provider) batchRevokePermissions(params map[string]any) (*plugin.Response, error) {
	entries, _ := params["Entries"].([]any)
	for _, e := range entries {
		entry, _ := e.(map[string]any)
		if entry == nil {
			continue
		}
		principal, _ := entry["Principal"].(map[string]any)
		if principal == nil {
			continue
		}
		principalStr := marshalJSON(principal)
		perms, err := p.store.ListPermissions()
		if err != nil {
			continue
		}
		for _, perm := range perms {
			if perm.Principal == principalStr {
				p.store.RevokePermission(perm.ID) //nolint:errcheck
			}
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Failures": []any{}})
}

func (p *Provider) getEffectivePermissionsForPath(params map[string]any) (*plugin.Response, error) {
	perms, err := p.store.ListPermissions()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(perms))
	for _, perm := range perms {
		var principal map[string]any
		var resource map[string]any
		var permissions []any
		var grantOption []any
		_ = json.Unmarshal([]byte(perm.Principal), &principal)
		_ = json.Unmarshal([]byte(perm.ResourceJSON), &resource)
		_ = json.Unmarshal([]byte(perm.PermissionsJSON), &permissions)
		_ = json.Unmarshal([]byte(perm.GrantOption), &grantOption)
		list = append(list, map[string]any{
			"Principal":                  principal,
			"Resource":                   resource,
			"Permissions":                permissions,
			"PermissionsWithGrantOption": grantOption,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PrincipalResourcePermissions": list,
		"NextToken":                    "",
	})
}

// --- Resource ---

func (p *Provider) registerResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	roleARN, _ := params["RoleArn"].(string)
	if err := p.store.RegisterResource(arn, roleARN); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "resource already registered", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deregisterResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterResource(arn); err != nil {
		return shared.JSONError("EntityNotFoundException", "resource not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetResource(arn)
	if err != nil {
		return shared.JSONError("EntityNotFoundException", "resource not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceInfo": resourceToMap(r),
	})
}

func (p *Provider) updateResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	roleARN, _ := params["RoleArn"].(string)
	if err := p.store.UpdateResource(arn, roleARN); err != nil {
		return shared.JSONError("EntityNotFoundException", "resource not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listResources(_ map[string]any) (*plugin.Response, error) {
	resources, err := p.store.ListResources()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(resources))
	for _, r := range resources {
		list = append(list, resourceToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceInfoList": list,
		"NextToken":        "",
	})
}

// --- DataLakeSettings ---

func (p *Provider) getDataLakeSettings(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	st, err := p.store.GetSettings(catalogID)
	if err != nil {
		return nil, err
	}
	var admins []any
	var config map[string]any
	_ = json.Unmarshal([]byte(st.Admins), &admins)
	_ = json.Unmarshal([]byte(st.Config), &config)
	if config == nil {
		config = map[string]any{}
	}
	settings := map[string]any{
		"DataLakeAdmins": admins,
	}
	for k, v := range config {
		settings[k] = v
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DataLakeSettings": settings,
	})
}

func (p *Provider) putDataLakeSettings(params map[string]any) (*plugin.Response, error) {
	catalogID := strParam(params, "CatalogId", "000000000000")
	settingsRaw, _ := params["DataLakeSettings"].(map[string]any)
	if settingsRaw == nil {
		settingsRaw = map[string]any{}
	}
	adminsRaw, _ := settingsRaw["DataLakeAdmins"].([]any)
	adminsJSON := marshalJSON(adminsRaw)

	// Store remaining settings as config
	config := make(map[string]any)
	for k, v := range settingsRaw {
		if k != "DataLakeAdmins" {
			config[k] = v
		}
	}
	configJSON := marshalJSON(config)

	if err := p.store.PutSettings(catalogID, adminsJSON, configJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- DataCellsFilter ---

func (p *Provider) createDataCellsFilter(params map[string]any) (*plugin.Response, error) {
	tableData, _ := params["TableData"].(map[string]any)
	if tableData == nil {
		return shared.JSONError("ValidationException", "TableData is required", http.StatusBadRequest), nil
	}
	name, _ := tableData["Name"].(string)
	dbName, _ := tableData["DatabaseName"].(string)
	tableName, _ := tableData["TableName"].(string)
	if name == "" || dbName == "" || tableName == "" {
		return shared.JSONError("ValidationException", "Name, DatabaseName, TableName are required", http.StatusBadRequest), nil
	}
	colNames, _ := tableData["ColumnNames"].([]any)
	colNamesJSON := marshalJSON(colNames)
	rowFilter, _ := tableData["RowFilter"].(map[string]any)
	rowFilterJSON := marshalJSON(rowFilter)

	if err := p.store.CreateDataCellsFilter(name, dbName, tableName, colNamesJSON, rowFilterJSON); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "data cells filter already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getDataCellsFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if name == "" || dbName == "" || tableName == "" {
		return shared.JSONError("ValidationException", "Name, DatabaseName, TableName are required", http.StatusBadRequest), nil
	}
	f, err := p.store.GetDataCellsFilter(name, dbName, tableName)
	if err != nil {
		return shared.JSONError("EntityNotFoundException", "data cells filter not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DataCellsFilter": filterToMap(f),
	})
}

func (p *Provider) updateDataCellsFilter(params map[string]any) (*plugin.Response, error) {
	tableData, _ := params["TableData"].(map[string]any)
	if tableData == nil {
		return shared.JSONError("ValidationException", "TableData is required", http.StatusBadRequest), nil
	}
	name, _ := tableData["Name"].(string)
	dbName, _ := tableData["DatabaseName"].(string)
	tableName, _ := tableData["TableName"].(string)
	if name == "" || dbName == "" || tableName == "" {
		return shared.JSONError("ValidationException", "Name, DatabaseName, TableName are required", http.StatusBadRequest), nil
	}
	colNames, _ := tableData["ColumnNames"].([]any)
	colNamesJSON := marshalJSON(colNames)
	rowFilter, _ := tableData["RowFilter"].(map[string]any)
	rowFilterJSON := marshalJSON(rowFilter)

	if err := p.store.UpdateDataCellsFilter(name, dbName, tableName, colNamesJSON, rowFilterJSON); err != nil {
		return shared.JSONError("EntityNotFoundException", "data cells filter not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteDataCellsFilter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	dbName, _ := params["DatabaseName"].(string)
	tableName, _ := params["TableName"].(string)
	if name == "" || dbName == "" || tableName == "" {
		return shared.JSONError("ValidationException", "Name, DatabaseName, TableName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDataCellsFilter(name, dbName, tableName); err != nil {
		return shared.JSONError("EntityNotFoundException", "data cells filter not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listDataCellsFilter(_ map[string]any) (*plugin.Response, error) {
	filters, err := p.store.ListDataCellsFilters()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		list = append(list, filterToMap(&f))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DataCellsFilters": list,
		"NextToken":        "",
	})
}

// --- Helpers ---

func resourceToMap(r *Resource) map[string]any {
	return map[string]any{
		"ResourceArn":  r.ARN,
		"RoleArn":      r.RoleARN,
		"LastModified": r.LastModified.Format("2006-01-02T15:04:05Z"),
	}
}

func filterToMap(f *DataCellsFilter) map[string]any {
	var colNames []string
	var rowFilter map[string]any
	_ = json.Unmarshal([]byte(f.ColumnNames), &colNames)
	_ = json.Unmarshal([]byte(f.RowFilter), &rowFilter)
	if rowFilter == nil {
		rowFilter = map[string]any{}
	}
	return map[string]any{
		"Name":         f.Name,
		"DatabaseName": f.DatabaseName,
		"TableName":    f.TableName,
		"ColumnNames":  colNames,
		"RowFilter":    rowFilter,
	}
}

// resolveOp maps HTTP method+path to a LakeFormation operation name.
// LakeFormation uses POST with the path as the operation name (e.g. POST /RegisterResource).
func resolveOp(method, path string) string {
	path = strings.Trim(path, "/")
	if path == "" {
		return ""
	}
	return path
}

func strParam(params map[string]any, key, defaultVal string) string {
	v, _ := params[key].(string)
	if v == "" {
		return defaultVal
	}
	return v
}

func marshalJSON(v any) string {
	if v == nil {
		return "[]"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func removeStr(slice []string, s string) []string {
	out := slice[:0]
	for _, v := range slice {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
