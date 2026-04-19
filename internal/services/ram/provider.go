// SPDX-License-Identifier: Apache-2.0

// internal/services/ram/provider.go
package ram

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

// Provider implements the ResourceSharing (RAM) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "ram" }
func (p *Provider) ServiceName() string           { return "ResourceSharing" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "ram"))
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

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	case "CreateResourceShare":
		return p.createResourceShare(params)
	case "GetResourceShares":
		return p.getResourceShares(params)
	case "UpdateResourceShare":
		return p.updateResourceShare(params)
	case "DeleteResourceShare":
		return p.deleteResourceShare(req)
	case "AssociateResourceShare":
		return p.associateResourceShare(params)
	case "DisassociateResourceShare":
		return p.disassociateResourceShare(params)
	case "GetResourceShareAssociations":
		return p.getResourceShareAssociations(params)
	case "GetResourceShareInvitations":
		return p.getResourceShareInvitations(params)
	case "AcceptResourceShareInvitation":
		return p.acceptResourceShareInvitation(params)
	case "RejectResourceShareInvitation":
		return p.rejectResourceShareInvitation(params)
	case "CreatePermission":
		return p.createPermission(params)
	case "GetPermission":
		return p.getPermission(req)
	case "ListPermissions":
		return p.listPermissions(params)
	case "ListPermissionVersions":
		return p.listPermissionVersions(req)
	case "DeletePermission":
		return p.deletePermission(req)
	case "DeletePermissionVersion":
		return p.deletePermissionVersion(req)
	case "SetDefaultPermissionVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true, "clientToken": ""})
	case "AssociateResourceSharePermission":
		return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true, "clientToken": ""})
	case "DisassociateResourceSharePermission":
		return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true, "clientToken": ""})
	case "ListResourceSharePermissions":
		return p.listResourceSharePermissions(req, params)
	case "EnableSharingWithAwsOrganization":
		return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true})
	case "ListResources":
		return p.listResources(params)
	case "ListPrincipals":
		return p.listPrincipals(params)
	case "GetResourcePolicies":
		return shared.JSONResponse(http.StatusOK, map[string]any{"policies": []any{}})
	case "ListResourceTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{"resourceTypes": []any{}})
	case "PromoteResourceShareCreatedFromPolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true})
	case "PromotePermissionCreatedFromPolicy":
		return p.promotePermissionCreatedFromPolicy(params)
	case "ReplacePermissionAssociations":
		return p.replacePermissionAssociations(params)
	case "ListReplacePermissionAssociationsWork":
		return shared.JSONResponse(http.StatusOK, map[string]any{"replacePermissionAssociationsWorks": []any{}})
	case "ListPendingInvitationResources":
		return shared.JSONResponse(http.StatusOK, map[string]any{"resources": []any{}})
	case "ListPermissionAssociations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"permissions": []any{}})
	case "ListSourceAssociations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"allocationStrategies": []any{}})
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req, params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")

	// Tags: /tagresource, /untagresource
	if p == "tagresource" && method == http.MethodPost {
		return "TagResource"
	}
	if p == "untagresource" && method == http.MethodPost {
		return "UntagResource"
	}

	// RAM uses lowercase path names for all operations
	// DELETE /deleteresourceshare has query param resourceShareArn
	ops := map[string]string{
		"createresourceshare":                   "CreateResourceShare",
		"getresourceshares":                     "GetResourceShares",
		"updateresourceshare":                   "UpdateResourceShare",
		"deleteresourceshare":                   "DeleteResourceShare",
		"associateresourceshare":                "AssociateResourceShare",
		"disassociateresourceshare":             "DisassociateResourceShare",
		"getresourceshareassociations":          "GetResourceShareAssociations",
		"getresourceshareinvitations":           "GetResourceShareInvitations",
		"acceptresourceshareinvitation":         "AcceptResourceShareInvitation",
		"rejectresourceshareinvitation":         "RejectResourceShareInvitation",
		"createpermission":                      "CreatePermission",
		"listpermissions":                       "ListPermissions",
		"setdefaultpermissionversion":           "SetDefaultPermissionVersion",
		"associateresourcesharepermission":      "AssociateResourceSharePermission",
		"disassociateresourcesharepermission":   "DisassociateResourceSharePermission",
		"listresourcesharepermissions":          "ListResourceSharePermissions",
		"enablesharingwithawsorganization":      "EnableSharingWithAwsOrganization",
		"listresources":                         "ListResources",
		"listprincipals":                        "ListPrincipals",
		"getresourcepolicies":                   "GetResourcePolicies",
		"listresourcetypes":                     "ListResourceTypes",
		"promoteresourcesharecreatedfrompolicy": "PromoteResourceShareCreatedFromPolicy",
		"promotepermissioncreatedfrompolicy":    "PromotePermissionCreatedFromPolicy",
		"replacepermissionassociations":         "ReplacePermissionAssociations",
		"listreplacepermissionassociationswork": "ListReplacePermissionAssociationsWork",
		"listpendinginvitationresources":        "ListPendingInvitationResources",
		"listpermissionassociations":            "ListPermissionAssociations",
		"listsourceassociations":                "ListSourceAssociations",
	}
	if op, ok := ops[strings.ToLower(p)]; ok {
		return op
	}

	// Handle paths with parameters: /permissions/{arn}, /permissions/{arn}/versions
	if strings.HasPrefix(p, "permissions/") || p == "permissions" {
		rest := strings.TrimPrefix(p, "permissions")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" && method == http.MethodGet {
			return "ListPermissions"
		}
		if strings.HasSuffix(rest, "/versions") {
			return "ListPermissionVersions"
		}
		switch method {
		case http.MethodGet:
			return "GetPermission"
		case http.MethodDelete:
			if strings.Contains(rest, "/version") {
				return "DeletePermissionVersion"
			}
			return "DeletePermission"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	shares, err := p.store.ListShares("")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(shares))
	for _, rs := range shares {
		res = append(res, plugin.Resource{Type: "ram-resource-share", ID: rs.ARN, Name: rs.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- ResourceShare CRUD ---

func (p *Provider) createResourceShare(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("MissingRequiredParameterException", "name is required", http.StatusBadRequest), nil
	}

	allowExternal := false
	if v, ok := params["allowExternalPrincipals"].(bool); ok {
		allowExternal = v
	}

	arn := shared.BuildARN("ram", "resource-share", shared.GenerateUUID())
	rs := &ResourceShare{
		ARN:           arn,
		Name:          name,
		Status:        "ACTIVE",
		Owner:         shared.DefaultAccountID,
		AllowExternal: allowExternal,
	}

	if err := p.store.CreateShare(rs); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceShareAlreadyExistsException", "resource share already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].([]any); ok {
		tags := parseTagList(rawTags)
		_ = p.store.tags.AddTags(arn, tags)
	}

	created, _ := p.store.GetShare(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShare": shareToMap(created),
	})
}

func (p *Provider) getResourceShares(params map[string]any) (*plugin.Response, error) {
	status, _ := params["resourceShareStatus"].(string)
	shares, err := p.store.ListShares(status)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(shares))
	for i := range shares {
		list = append(list, shareToMap(&shares[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShares": list,
	})
}

func (p *Provider) updateResourceShare(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceShareArn"].(string)
	if arn == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateShare(arn, params); err != nil {
		return shared.JSONError("UnknownResourceException", "resource share not found", http.StatusBadRequest), nil
	}
	rs, _ := p.store.GetShare(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShare": shareToMap(rs),
	})
}

func (p *Provider) deleteResourceShare(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "resourceshares")
	if arn == "" {
		// also try query param
		arn = req.URL.Query().Get("resourceShareArn")
	}
	if arn == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}
	rs, err := p.store.GetShare(arn)
	if err != nil {
		return shared.JSONError("UnknownResourceException", "resource share not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(rs.ARN)
	if err := p.store.DeleteShare(arn); err != nil {
		return shared.JSONError("UnknownResourceException", "resource share not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true})
}

// --- Associations ---

func (p *Provider) associateResourceShare(params map[string]any) (*plugin.Response, error) {
	shareARN, _ := params["resourceShareArn"].(string)
	if shareARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}

	var assocs []map[string]any

	if resources, ok := params["resourceArns"].([]any); ok {
		for _, r := range resources {
			rArn, _ := r.(string)
			assocARN := shared.BuildARN("ram", "resource-share-association", shared.GenerateUUID())
			a := &ShareAssociation{
				ARN:              assocARN,
				ShareARN:         shareARN,
				AssociatedEntity: rArn,
				Type:             "RESOURCE",
				Status:           "ASSOCIATED",
			}
			p.store.AddAssociation(a) //nolint:errcheck
			assocs = append(assocs, assocToMap(a))
		}
	}

	if principals, ok := params["principals"].([]any); ok {
		for _, pr := range principals {
			prStr, _ := pr.(string)
			assocARN := shared.BuildARN("ram", "resource-share-association", shared.GenerateUUID())
			a := &ShareAssociation{
				ARN:              assocARN,
				ShareARN:         shareARN,
				AssociatedEntity: prStr,
				Type:             "PRINCIPAL",
				Status:           "ASSOCIATED",
			}
			p.store.AddAssociation(a) //nolint:errcheck
			assocs = append(assocs, assocToMap(a))
		}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareAssociations": assocs,
	})
}

func (p *Provider) disassociateResourceShare(params map[string]any) (*plugin.Response, error) {
	shareARN, _ := params["resourceShareArn"].(string)
	if shareARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}

	if resources, ok := params["resourceArns"].([]any); ok {
		for _, r := range resources {
			rArn, _ := r.(string)
			p.store.DeleteAssociation(shareARN, rArn) //nolint:errcheck
		}
	}
	if principals, ok := params["principals"].([]any); ok {
		for _, pr := range principals {
			prStr, _ := pr.(string)
			p.store.DeleteAssociation(shareARN, prStr) //nolint:errcheck
		}
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareAssociations": []any{},
	})
}

func (p *Provider) getResourceShareAssociations(params map[string]any) (*plugin.Response, error) {
	assocType, _ := params["associationType"].(string)
	shareARN, _ := params["resourceShareArns"].(string)
	assocs, err := p.store.ListAssociations(shareARN, assocType)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for i := range assocs {
		list = append(list, assocToMap(&assocs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareAssociations": list,
	})
}

// --- Invitations ---

func (p *Provider) getResourceShareInvitations(params map[string]any) (*plugin.Response, error) {
	shareARN, _ := params["resourceShareArns"].(string)
	invs, err := p.store.ListInvitations(shareARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(invs))
	for i := range invs {
		list = append(list, invToMap(&invs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareInvitations": list,
	})
}

func (p *Provider) acceptResourceShareInvitation(params map[string]any) (*plugin.Response, error) {
	invARN, _ := params["resourceShareInvitationArn"].(string)
	if invARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareInvitationArn is required", http.StatusBadRequest), nil
	}
	inv, err := p.store.GetInvitation(invARN)
	if err != nil {
		return shared.JSONError("ResourceShareInvitationArnNotFoundException", "invitation not found", http.StatusBadRequest), nil
	}
	if inv.Status != "PENDING" {
		return shared.JSONError("ResourceShareInvitationAlreadyAcceptedException", "invitation already processed", http.StatusBadRequest), nil
	}
	p.store.UpdateInvitationStatus(invARN, "ACCEPTED") //nolint:errcheck
	inv.Status = "ACCEPTED"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareInvitation": invToMap(inv),
	})
}

func (p *Provider) rejectResourceShareInvitation(params map[string]any) (*plugin.Response, error) {
	invARN, _ := params["resourceShareInvitationArn"].(string)
	if invARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareInvitationArn is required", http.StatusBadRequest), nil
	}
	inv, err := p.store.GetInvitation(invARN)
	if err != nil {
		return shared.JSONError("ResourceShareInvitationArnNotFoundException", "invitation not found", http.StatusBadRequest), nil
	}
	if inv.Status != "PENDING" {
		return shared.JSONError("ResourceShareInvitationAlreadyRejectedException", "invitation already processed", http.StatusBadRequest), nil
	}
	p.store.UpdateInvitationStatus(invARN, "REJECTED") //nolint:errcheck
	inv.Status = "REJECTED"
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"resourceShareInvitation": invToMap(inv),
	})
}

// --- Permissions ---

func (p *Provider) createPermission(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("MissingRequiredParameterException", "name is required", http.StatusBadRequest), nil
	}
	resourceType, _ := params["resourceType"].(string)

	arn := shared.BuildARN("ram", "permission", name)
	perm := &Permission{
		ARN:          arn,
		Name:         name,
		ResourceType: resourceType,
		Version:      "1",
		IsDefault:    false,
		Status:       "ATTACHABLE",
	}

	if err := p.store.CreatePermission(perm); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("PermissionAlreadyExistsException", "permission already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].([]any); ok {
		tags := parseTagList(rawTags)
		_ = p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permission": permToMap(perm),
	})
}

func (p *Provider) getPermission(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "permissions")
	if arn == "" {
		arn = req.URL.Query().Get("permissionArn")
	}
	if arn == "" {
		return shared.JSONError("MissingRequiredParameterException", "permissionArn is required", http.StatusBadRequest), nil
	}
	perm, err := p.store.GetPermission(arn)
	if err != nil {
		return shared.JSONError("UnknownResourceException", "permission not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permission": permToMap(perm),
	})
}

func (p *Provider) listPermissions(params map[string]any) (*plugin.Response, error) {
	resourceType, _ := params["resourceType"].(string)
	perms, err := p.store.ListPermissions(resourceType)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(perms))
	for i := range perms {
		list = append(list, permToMap(&perms[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permissions": list,
	})
}

func (p *Provider) listPermissionVersions(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "permissions")
	if arn == "" {
		arn = req.URL.Query().Get("permissionArn")
	}
	var list []map[string]any
	if arn != "" {
		perm, err := p.store.GetPermission(arn)
		if err == nil {
			list = append(list, permToMap(perm))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permissions": list,
	})
}

func (p *Provider) deletePermission(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "permissions")
	if arn == "" {
		arn = req.URL.Query().Get("permissionArn")
	}
	if arn == "" {
		return shared.JSONError("MissingRequiredParameterException", "permissionArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePermission(arn); err != nil {
		return shared.JSONError("UnknownResourceException", "permission not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true, "permissionStatus": "DELETED"})
}

func (p *Provider) deletePermissionVersion(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "permissions")
	if arn == "" {
		arn = req.URL.Query().Get("permissionArn")
	}
	if arn == "" {
		return shared.JSONError("MissingRequiredParameterException", "permissionArn is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"returnValue": true, "permissionStatus": "DELETED"})
}

func (p *Provider) listResourceSharePermissions(_ *http.Request, params map[string]any) (*plugin.Response, error) {
	_ = params
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permissions": []any{},
	})
}

func (p *Provider) listResources(params map[string]any) (*plugin.Response, error) {
	shareARN, _ := params["resourceShareArn"].(string)
	assocs, err := p.store.ListAssociations(shareARN, "RESOURCE")
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for _, a := range assocs {
		list = append(list, map[string]any{
			"arn":              a.AssociatedEntity,
			"resourceShareArn": a.ShareARN,
			"status":           a.Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"resources": list})
}

func (p *Provider) listPrincipals(params map[string]any) (*plugin.Response, error) {
	shareARN, _ := params["resourceShareArn"].(string)
	assocs, err := p.store.ListAssociations(shareARN, "PRINCIPAL")
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(assocs))
	for _, a := range assocs {
		list = append(list, map[string]any{
			"id":               a.AssociatedEntity,
			"resourceShareArn": a.ShareARN,
			"status":           a.Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"principals": list})
}

func (p *Provider) promotePermissionCreatedFromPolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["permissionArn"].(string)
	name, _ := params["name"].(string)
	if arn == "" || name == "" {
		return shared.JSONError("MissingRequiredParameterException", "permissionArn and name are required", http.StatusBadRequest), nil
	}
	newARN := shared.BuildARN("ram", "permission", name)
	perm := &Permission{
		ARN:    newARN,
		Name:   name,
		Status: "ATTACHABLE",
	}
	p.store.CreatePermission(perm) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"permission": permToMap(perm),
	})
}

func (p *Provider) replacePermissionAssociations(params map[string]any) (*plugin.Response, error) {
	workID := shared.GenerateUUID()
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"replacePermissionAssociationsWork": map[string]any{
			"id":     workID,
			"status": "IN_PROGRESS",
		},
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	resourceShareARN, _ := params["resourceShareArn"].(string)
	if resourceShareARN == "" {
		resourceShareARN = extractPathParam(req.URL.Path, "tags")
	}
	if resourceShareARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].([]any)
	tags := parseTagList(rawTags)
	if err := p.store.tags.AddTags(resourceShareARN, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	resourceShareARN, _ := params["resourceShareArn"].(string)
	if resourceShareARN == "" {
		resourceShareARN = extractPathParam(req.URL.Path, "tags")
	}
	if resourceShareARN == "" {
		return shared.JSONError("MissingRequiredParameterException", "resourceShareArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["tagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if len(keys) == 0 {
		keys = req.URL.Query()["tagKeys"]
	}
	if err := p.store.tags.RemoveTags(resourceShareARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Helpers ---

func shareToMap(rs *ResourceShare) map[string]any {
	ext := false
	if rs != nil {
		ext = rs.AllowExternal
	}
	return map[string]any{
		"resourceShareArn":        rs.ARN,
		"name":                    rs.Name,
		"status":                  rs.Status,
		"owningAccountId":         rs.Owner,
		"allowExternalPrincipals": ext,
		"creationTime":            rs.CreatedAt.Unix(),
		"lastUpdatedTime":         rs.UpdatedAt.Unix(),
	}
}

func assocToMap(a *ShareAssociation) map[string]any {
	return map[string]any{
		"resourceShareArn": a.ShareARN,
		"associatedEntity": a.AssociatedEntity,
		"associationType":  a.Type,
		"status":           a.Status,
		"creationTime":     a.CreatedAt.Unix(),
		"lastUpdatedTime":  a.CreatedAt.Unix(),
	}
}

func invToMap(inv *ShareInvitation) map[string]any {
	return map[string]any{
		"resourceShareInvitationArn": inv.ARN,
		"resourceShareArn":           inv.ShareARN,
		"senderAccountId":            inv.Sender,
		"receiverAccountId":          inv.Receiver,
		"status":                     inv.Status,
		"invitationTimestamp":        inv.CreatedAt.Unix(),
	}
}

func permToMap(perm *Permission) map[string]any {
	return map[string]any{
		"arn":          perm.ARN,
		"name":         perm.Name,
		"resourceType": perm.ResourceType,
		"version":      perm.Version,
		"isDefault":    perm.IsDefault,
		"status":       perm.Status,
		"creationTime": perm.CreatedAt.Unix(),
	}
}

func parseTagList(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["key"].(string)
		v, _ := tag["value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func extractPathParam(path, key string) string {
	// find "/key/" prefix and return the rest (supports ARNs with slashes)
	prefix := "/" + key + "/"
	if idx := strings.Index(path, prefix); idx >= 0 {
		return path[idx+len(prefix):]
	}
	return ""
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
