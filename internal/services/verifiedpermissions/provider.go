// SPDX-License-Identifier: Apache-2.0

// Package verifiedpermissions implements AWS Verified Permissions.
package verifiedpermissions

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// VerifiedPermissionsProvider implements plugin.ServicePlugin for Verified Permissions.
type VerifiedPermissionsProvider struct {
	dataDir string
}

// ServiceID returns the unique identifier for this plugin.
func (p *VerifiedPermissionsProvider) ServiceID() string { return "verifiedpermissions" }

// ServiceName returns the human-readable name for this plugin.
func (p *VerifiedPermissionsProvider) ServiceName() string { return "AWS Verified Permissions" }

// Protocol returns the wire protocol used by this plugin.
func (p *VerifiedPermissionsProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

// Init initialises the VerifiedPermissionsProvider from cfg.
func (p *VerifiedPermissionsProvider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	return nil
}

// Shutdown closes the VerifiedPermissionsProvider.
func (p *VerifiedPermissionsProvider) Shutdown(_ context.Context) error {
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate Verified Permissions operation.
func (p *VerifiedPermissionsProvider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	switch op {
	case "CreatePolicy":
		return p.handleCreatePolicy(ctx)
	case "DeletePolicy":
		return p.handleDeletePolicy(ctx)
	case "GetPolicy":
		return p.handleGetPolicy(ctx)
	case "ListPolicies":
		return p.handleListPolicies(ctx)
	case "PutPolicy":
		return p.handlePutPolicy(ctx)
	case "CreatePolicyTemplate":
		return p.handleCreatePolicyTemplate(ctx)
	case "DeletePolicyTemplate":
		return p.handleDeletePolicyTemplate(ctx)
	case "GetPolicyTemplate":
		return p.handleGetPolicyTemplate(ctx)
	case "ListPolicyTemplates":
		return p.handleListPolicyTemplates(ctx)
	case "PutPolicyTemplate":
		return p.handlePutPolicyTemplate(ctx)
	case "CreateIdentityPool":
		return p.handleCreateIdentityPool(ctx)
	case "DeleteIdentityPool":
		return p.handleDeleteIdentityPool(ctx)
	case "GetIdentityPool":
		return p.handleGetIdentityPool(ctx)
	case "ListIdentityPools":
		return p.handleListIdentityPools(ctx)
	case "CreatePermissionGroup":
		return p.handleCreatePermissionGroup(ctx)
	case "DeletePermissionGroup":
		return p.handleDeletePermissionGroup(ctx)
	case "GetPermissionGroup":
		return p.handleGetPermissionGroup(ctx)
	case "ListPermissionGroups":
		return p.handleListPermissionGroups(ctx)
	case "CreatePermission":
		return p.handleCreatePermission(ctx)
	case "DeletePermission":
		return p.handleDeletePermission(ctx)
	case "GetPermission":
		return p.handleGetPermission(ctx)
	case "ListPermissions":
		return p.handleListPermissions(ctx)
	case "PutPermissionsPolicy":
		return p.handlePutPermissionsPolicy(ctx)
	case "GetPermissionsPolicy":
		return p.handleGetPermissionsPolicy(ctx)
	case "BatchGetEntityRelationship":
		return p.handleBatchGetEntityRelationship(ctx)
	case "CreateEntityRelationship":
		return p.handleCreateEntityRelationship(ctx)
	case "DeleteEntityRelationship":
		return p.handleDeleteEntityRelationship(ctx)
	case "ListEntityRelationships":
		return p.handleListEntityRelationships(ctx)
	case "IsAuthorized":
		return p.handleIsAuthorized(ctx)
	case "BatchEvaluatePermissions":
		return p.handleBatchEvaluatePermissions(ctx)
	case "CreateSchema":
		return p.handleCreateSchema(ctx)
	case "DeleteSchema":
		return p.handleDeleteSchema(ctx)
	case "GetSchema":
		return p.handleGetSchema(ctx)
	case "ListSchemaReaders":
		return p.handleListSchemaReaders(ctx)
	case "PutSchema":
		return p.handlePutSchema(ctx)
	case "ListSchemaWriters":
		return p.handleListSchemaWriters(ctx)
	default:
		return jsonError("UnsupportedOperation", "operation not supported", http.StatusBadRequest), nil
	}
}

// ListResources returns empty resources.
func (p *VerifiedPermissionsProvider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

// GetMetrics returns empty metrics.
func (p *VerifiedPermissionsProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func jsonError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}
}

func jsonResponse(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

// --- Verified Permissions operation implementations ---

func (p *VerifiedPermissionsProvider) handleCreatePolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyId":             "policy-" + defaultAccountID,
		"PolicyArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy/policy-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyText":           "",
		"PolicyType":           "CUSTOM_POLICY",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeletePolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyId":             "policy-" + defaultAccountID,
		"PolicyArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy/policy-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyText":           "",
		"PolicyType":           "CUSTOM_POLICY",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetPolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyId":             "policy-" + defaultAccountID,
		"PolicyArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy/policy-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyText":           "",
		"PolicyType":           "CUSTOM_POLICY",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListPolicies(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Policies": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handlePutPolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyId":             "policy-" + defaultAccountID,
		"PolicyArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy/policy-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyText":           "",
		"PolicyType":           "CUSTOM_POLICY",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleCreatePolicyTemplate(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyTemplateId":     "template-" + defaultAccountID,
		"PolicyTemplateArn":    "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-template/template-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyTemplateText":   "",
		"PolicyTemplateType":   "PREDEFINED_POLICY_TEMPLATE",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeletePolicyTemplate(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyTemplateId":     "template-" + defaultAccountID,
		"PolicyTemplateArn":    "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-template/template-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyTemplateText":   "",
		"PolicyTemplateType":   "PREDEFINED_POLICY_TEMPLATE",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetPolicyTemplate(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyTemplateId":     "template-" + defaultAccountID,
		"PolicyTemplateArn":    "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-template/template-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyTemplateText":   "",
		"PolicyTemplateType":   "PREDEFINED_POLICY_TEMPLATE",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListPolicyTemplates(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyTemplates": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handlePutPolicyTemplate(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyTemplateId":     "template-" + defaultAccountID,
		"PolicyTemplateArn":    "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-template/template-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"PolicyTemplateText":   "",
		"PolicyTemplateType":   "PREDEFINED_POLICY_TEMPLATE",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleCreateIdentityPool(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"IdentityPoolId":       "identitypool-" + defaultAccountID,
		"IdentityPoolArn":      "arn:aws:verifiedpermissions:" + defaultAccountID + ":identity-pool/identitypool-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeleteIdentityPool(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"IdentityPoolId":       "identitypool-" + defaultAccountID,
		"IdentityPoolArn":      "arn:aws:verifiedpermissions:" + defaultAccountID + ":identity-pool/identitypool-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetIdentityPool(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"IdentityPoolId":       "identitypool-" + defaultAccountID,
		"IdentityPoolArn":      "arn:aws:verifiedpermissions:" + defaultAccountID + ":identity-pool/identitypool-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListIdentityPools(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"IdentityPools": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handleCreatePermissionGroup(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionGroupId":    "permgroup-" + defaultAccountID,
		"PermissionGroupArn":   "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission-group/permgroup-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeletePermissionGroup(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionGroupId":    "permgroup-" + defaultAccountID,
		"PermissionGroupArn":   "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission-group/permgroup-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetPermissionGroup(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionGroupId":    "permgroup-" + defaultAccountID,
		"PermissionGroupArn":   "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission-group/permgroup-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListPermissionGroups(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionGroups": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handleCreatePermission(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionId":         "perm-" + defaultAccountID,
		"PermissionArn":        "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission/perm-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeletePermission(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionId":         "perm-" + defaultAccountID,
		"PermissionArn":        "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission/perm-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetPermission(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PermissionId":         "perm-" + defaultAccountID,
		"PermissionArn":        "arn:aws:verifiedpermissions:" + defaultAccountID + ":permission/perm-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListPermissions(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Permissions": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handlePutPermissionsPolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyStoreId": "store-" + defaultAccountID,
		"PolicyStore":   "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
	})
}

func (p *VerifiedPermissionsProvider) handleGetPermissionsPolicy(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"PolicyStoreId": "store-" + defaultAccountID,
		"PolicyStore":   "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
	})
}

func (p *VerifiedPermissionsProvider) handleBatchGetEntityRelationship(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Relationships": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handleCreateEntityRelationship(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"RelationshipId":       "rel-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeleteEntityRelationship(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"RelationshipId":       "rel-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListEntityRelationships(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Relationships": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handleIsAuthorized(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"IsAuthorized": true,
	})
}

func (p *VerifiedPermissionsProvider) handleBatchEvaluatePermissions(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"EvaluationResults": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handleCreateSchema(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"SchemaId":             "schema-" + defaultAccountID,
		"SchemaArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":schema/schema-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"SchemaText":           "",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleDeleteSchema(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"SchemaId":             "schema-" + defaultAccountID,
		"SchemaArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":schema/schema-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleGetSchema(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"SchemaId":             "schema-" + defaultAccountID,
		"SchemaArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":schema/schema-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"SchemaText":           "",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListSchemaReaders(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Readers": []any{},
	})
}

func (p *VerifiedPermissionsProvider) handlePutSchema(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"SchemaId":             "schema-" + defaultAccountID,
		"SchemaArn":            "arn:aws:verifiedpermissions:" + defaultAccountID + ":schema/schema-" + defaultAccountID,
		"PolicyStoreId":        "store-" + defaultAccountID,
		"PolicyStore":          "arn:aws:verifiedpermissions:" + defaultAccountID + ":policy-store/store-" + defaultAccountID,
		"SchemaText":           "",
		"CreationTimestamp":    "2024-01-01T00:00:00Z",
		"LastUpdatedTimestamp": "2024-01-01T00:00:00Z",
	})
}

func (p *VerifiedPermissionsProvider) handleListSchemaWriters(_ context.Context) (*plugin.Response, error) {
	return jsonResponse(http.StatusOK, map[string]any{
		"Writers": []any{},
	})
}

func init() {
	plugin.DefaultRegistry.Register("verifiedpermissions", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &VerifiedPermissionsProvider{}
	})
}
