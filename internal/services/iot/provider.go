// SPDX-License-Identifier: Apache-2.0

// internal/services/iot/provider.go
package iot

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

// Provider implements the IoT Core service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "iot" }
func (p *Provider) ServiceName() string           { return "IotService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "iot"))
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

	// For REST requests, path parameters like /things/{thingName} need
	// to be merged into params so handlers can find them.
	injectPathParams(path, params)

	switch op {
	// Things
	case "CreateThing":
		return p.createThing(params)
	case "DescribeThing":
		name := extractLastPathSegment(path)
		return p.describeThing(name)
	case "ListThings":
		return p.listThings()
	case "UpdateThing":
		name := extractLastPathSegment(path)
		return p.updateThing(name, params)
	case "DeleteThing":
		name := extractLastPathSegment(path)
		return p.deleteThing(name)

	// Thing Types
	case "CreateThingType":
		name := extractLastPathSegment(path)
		return p.createThingType(name, params)
	case "DescribeThingType":
		name := extractLastPathSegment(path)
		return p.describeThingType(name)
	case "ListThingTypes":
		return p.listThingTypes()
	case "DeleteThingType":
		name := extractLastPathSegment(path)
		return p.deleteThingType(name)
	case "DeprecateThingType":
		name := extractPathParam(path, "thing-types")
		return p.deprecateThingType(name, params)
	case "UpdateThingType":
		name := extractLastPathSegment(path)
		return p.updateThingType(name, params)

	// Thing Groups
	case "CreateThingGroup":
		name := extractLastPathSegment(path)
		return p.createThingGroup(name, params)
	case "DescribeThingGroup":
		name := extractLastPathSegment(path)
		return p.describeThingGroup(name)
	case "ListThingGroups":
		return p.listThingGroups()
	case "UpdateThingGroup":
		name := extractLastPathSegment(path)
		return p.updateThingGroup(name, params)
	case "DeleteThingGroup":
		name := extractLastPathSegment(path)
		return p.deleteThingGroup(name)
	case "AddThingToThingGroup":
		return p.addThingToThingGroup(params)
	case "RemoveThingFromThingGroup":
		return p.removeThingFromThingGroup(params)
	case "ListThingsInThingGroup":
		name := extractPathParam(path, "thing-groups")
		return p.listThingsInThingGroup(name)
	case "ListThingGroupsForThing":
		name := extractPathParam(path, "things")
		return p.listThingGroupsForThing(name)

	// Policies
	case "CreatePolicy":
		return p.createPolicy(params)
	case "GetPolicy":
		name := extractLastPathSegment(path)
		return p.getPolicy(name)
	case "ListPolicies":
		return p.listPolicies()
	case "DeletePolicy":
		name := extractLastPathSegment(path)
		return p.deletePolicy(name)
	case "CreatePolicyVersion":
		name := extractPathParam(path, "policies")
		return p.createPolicyVersion(name, params)
	case "ListPolicyVersions":
		name := extractPathParam(path, "policies")
		return p.listPolicyVersions(name)
	case "GetPolicyVersion":
		name := extractPathParam(path, "policies")
		versionID := extractLastPathSegment(path)
		return p.getPolicyVersion(name, versionID)
	case "SetDefaultPolicyVersion":
		name := extractPathParam(path, "policies")
		versionID := extractLastPathSegment(path)
		return p.setDefaultPolicyVersion(name, versionID)
	case "DeletePolicyVersion":
		name := extractPathParam(path, "policies")
		versionID := extractLastPathSegment(path)
		return p.deletePolicyVersion(name, versionID)
	case "AttachPolicy":
		name := extractPathParam(path, "policies")
		return p.attachPolicy(name, params)
	case "DetachPolicy":
		name := extractPathParam(path, "policies")
		return p.detachPolicy(name, params)
	case "ListAttachedPolicies":
		return p.listAttachedPolicies(req, params)
	case "ListTargetsForPolicy":
		name := extractPathParam(path, "policies")
		return p.listTargetsForPolicy(name)

	// Certificates
	case "CreateKeysAndCertificate":
		return p.createKeysAndCertificate(params)
	case "RegisterCertificate":
		return p.registerCertificate(params)
	case "RegisterCertificateWithoutCA":
		return p.registerCertificateWithoutCA(params)
	case "DescribeCertificate":
		id := extractLastPathSegment(path)
		return p.describeCertificate(id)
	case "ListCertificates":
		return p.listCertificates()
	case "UpdateCertificate":
		id := extractLastPathSegment(path)
		return p.updateCertificate(id, req, params)
	case "DeleteCertificate":
		id := extractLastPathSegment(path)
		return p.deleteCertificate(id)

	// Thing Principals
	case "AttachThingPrincipal":
		name := extractPathParam(path, "things")
		return p.attachThingPrincipal(name, req)
	case "DetachThingPrincipal":
		name := extractPathParam(path, "things")
		return p.detachThingPrincipal(name, req)
	case "ListThingPrincipals":
		name := extractPathParam(path, "things")
		return p.listThingPrincipals(name)
	case "ListPrincipalThings":
		return p.listPrincipalThings(req)

	// Topic Rules
	case "CreateTopicRule":
		name := extractLastPathSegment(path)
		return p.createTopicRule(name, params)
	case "GetTopicRule":
		name := extractLastPathSegment(path)
		return p.getTopicRule(name)
	case "ListTopicRules":
		return p.listTopicRules()
	case "ReplaceTopicRule":
		name := extractLastPathSegment(path)
		return p.replaceTopicRule(name, params)
	case "DeleteTopicRule":
		name := extractLastPathSegment(path)
		return p.deleteTopicRule(name)
	case "EnableTopicRule":
		name := extractPathParam(path, "rules")
		return p.enableTopicRule(name)
	case "DisableTopicRule":
		name := extractPathParam(path, "rules")
		return p.disableTopicRule(name)

	// Jobs
	case "CreateJob":
		id := extractLastPathSegment(path)
		return p.createJob(id, params)
	case "DescribeJob":
		id := extractLastPathSegment(path)
		return p.describeJob(id)
	case "ListJobs":
		return p.listJobs()
	case "CancelJob":
		id := extractPathParam(path, "jobs")
		return p.cancelJob(id)
	case "DeleteJob":
		id := extractLastPathSegment(path)
		return p.deleteJob(id)

	// Role Aliases
	case "CreateRoleAlias":
		name := extractLastPathSegment(path)
		return p.createRoleAlias(name, params)
	case "DescribeRoleAlias":
		name := extractLastPathSegment(path)
		return p.describeRoleAlias(name)
	case "ListRoleAliases":
		return p.listRoleAliases()
	case "UpdateRoleAlias":
		name := extractLastPathSegment(path)
		return p.updateRoleAlias(name, params)
	case "DeleteRoleAlias":
		name := extractLastPathSegment(path)
		return p.deleteRoleAlias(name)

	// Endpoint
	case "DescribeEndpoint":
		return p.describeEndpoint(req)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Default: return success/empty JSON for all unimplemented ops
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

// resolveOp determines the IoT API operation from the HTTP method and URL path.
// This is needed because REST-JSON requests don't include X-Amz-Target.
func resolveOp(method, path string) string {
	// Strip query string and trailing slash, then split into segments.
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return ""
	}
	segs := strings.Split(path, "/")
	n := len(segs)
	first := segs[0]

	switch first {
	case "things":
		if n == 1 {
			// GET /things → ListThings
			return "ListThings"
		}
		if n == 2 {
			// /things/{thingName}
			switch method {
			case http.MethodPost, http.MethodPut:
				return "CreateThing"
			case http.MethodGet:
				return "DescribeThing"
			case http.MethodPatch:
				return "UpdateThing"
			case http.MethodDelete:
				return "DeleteThing"
			}
		}
		if n == 3 && segs[2] == "principals" {
			// /things/{thingName}/principals
			switch method {
			case http.MethodPut:
				return "AttachThingPrincipal"
			case http.MethodDelete:
				return "DetachThingPrincipal"
			case http.MethodGet:
				return "ListThingPrincipals"
			}
		}
		if n == 3 && segs[2] == "thing-groups" {
			// GET /things/{thing}/thing-groups
			return "ListThingGroupsForThing"
		}

	case "thing-types":
		if n == 1 {
			return "ListThingTypes"
		}
		if n == 2 {
			switch method {
			case http.MethodPost, http.MethodPut:
				return "CreateThingType"
			case http.MethodGet:
				return "DescribeThingType"
			case http.MethodDelete:
				return "DeleteThingType"
			}
		}
		if n == 3 && segs[2] == "deprecate" {
			return "DeprecateThingType"
		}

	case "thing-groups":
		if n == 1 {
			return "ListThingGroups"
		}
		if n == 2 {
			switch method {
			case http.MethodPost:
				return "CreateThingGroup"
			case http.MethodGet:
				return "DescribeThingGroup"
			case http.MethodPatch:
				return "UpdateThingGroup"
			case http.MethodDelete:
				return "DeleteThingGroup"
			}
		}
		if n == 3 && segs[2] == "things" {
			// GET /thing-groups/{group}/things → ListThingsInThingGroup
			return "ListThingsInThingGroup"
		}
		if n == 4 && segs[2] == "things" {
			// PUT /thing-groups/{group}/things/{thing}
			switch method {
			case http.MethodPut:
				return "AddThingToThingGroup"
			case http.MethodDelete:
				return "RemoveThingFromThingGroup"
			}
		}

	case "policies":
		if n == 1 {
			return "ListPolicies"
		}
		if n == 2 {
			switch method {
			case http.MethodPost:
				return "CreatePolicy"
			case http.MethodGet:
				return "GetPolicy"
			case http.MethodDelete:
				return "DeletePolicy"
			}
		}
		if n == 3 && segs[2] == "version" {
			switch method {
			case http.MethodPost:
				return "CreatePolicyVersion"
			case http.MethodGet:
				return "ListPolicyVersions"
			}
		}
		if n == 4 && segs[2] == "version" {
			switch method {
			case http.MethodGet:
				return "GetPolicyVersion"
			case http.MethodDelete:
				return "DeletePolicyVersion"
			case http.MethodPatch:
				return "SetDefaultPolicyVersion"
			}
		}

	case "target-policies":
		// PUT /target-policies/{policyName} → AttachPolicy
		// POST /target-policies/{policyName} → DetachPolicy
		if n == 2 {
			switch method {
			case http.MethodPut:
				return "AttachPolicy"
			case http.MethodPost:
				return "DetachPolicy"
			}
		}

	case "attached-policies":
		if n == 2 && method == http.MethodPost {
			return "ListAttachedPolicies"
		}

	case "policy-targets":
		if n == 2 && method == http.MethodPost {
			return "ListTargetsForPolicy"
		}

	case "keys-and-certificate":
		if method == http.MethodPost {
			return "CreateKeysAndCertificate"
		}

	case "certificate":
		if n == 2 && segs[1] == "register" {
			return "RegisterCertificate"
		}
		if n == 2 && segs[1] == "register-no-ca" {
			return "RegisterCertificateWithoutCA"
		}

	case "certificates":
		if n == 1 {
			return "ListCertificates"
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "DescribeCertificate"
			case http.MethodPut:
				return "UpdateCertificate"
			case http.MethodDelete:
				return "DeleteCertificate"
			}
		}

	case "rules":
		if n == 1 {
			return "ListTopicRules"
		}
		if n == 2 {
			switch method {
			case http.MethodPost:
				return "CreateTopicRule"
			case http.MethodGet:
				return "GetTopicRule"
			case http.MethodPatch:
				return "ReplaceTopicRule"
			case http.MethodDelete:
				return "DeleteTopicRule"
			}
		}
		if n == 3 && segs[2] == "enable" {
			return "EnableTopicRule"
		}
		if n == 3 && segs[2] == "disable" {
			return "DisableTopicRule"
		}

	case "jobs":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateJob"
			case http.MethodGet:
				return "ListJobs"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "DescribeJob"
			case http.MethodDelete:
				return "DeleteJob"
			}
		}
		if n == 3 && segs[2] == "cancel" {
			return "CancelJob"
		}

	case "role-aliases":
		if n == 1 {
			return "ListRoleAliases"
		}
		if n == 2 {
			switch method {
			case http.MethodPost:
				return "CreateRoleAlias"
			case http.MethodGet:
				return "DescribeRoleAlias"
			case http.MethodPut:
				return "UpdateRoleAlias"
			case http.MethodDelete:
				return "DeleteRoleAlias"
			}
		}

	case "endpoint":
		return "DescribeEndpoint"

	case "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		}

	case "untags":
		if method == http.MethodPost {
			return "UntagResource"
		}

	case "target-group-for-things":
		if method == http.MethodPut {
			return "RemoveThingFromThingGroup"
		}
	}

	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	things, err := p.store.ListThings()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(things))
	for _, t := range things {
		res = append(res, plugin.Resource{Type: "thing", ID: t.Name, Name: t.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Thing operations ---

func (p *Provider) createThing(params map[string]any) (*plugin.Response, error) {
	name, _ := params["thingName"].(string)
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iot", "thing", name)
	attrsJSON := "{}"
	if ap, ok := params["attributePayload"].(map[string]any); ok {
		if attrs, ok2 := ap["attributes"].(map[string]any); ok2 {
			if b, err := json.Marshal(attrs); err == nil {
				attrsJSON = string(b)
			}
		}
	}
	t := &Thing{
		Name:       name,
		ARN:        arn,
		TypeName:   strParam(params, "thingTypeName"),
		Attributes: attrsJSON,
		Version:    1,
		CreatedAt:  time.Now().Unix(),
	}
	if err := p.store.CreateThing(t); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "thing already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"thingName": name,
		"thingArn":  arn,
		"thingId":   name,
	})
}

func (p *Provider) describeThing(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetThing(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, thingToMap(t))
}

func (p *Provider) listThings() (*plugin.Response, error) {
	things, err := p.store.ListThings()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(things))
	for i := range things {
		result = append(result, thingToMap(&things[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"things": result})
}

func (p *Provider) updateThing(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateThing(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteThing(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	t, err := p.store.DeleteThing(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(t.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Thing Type operations ---

func (p *Provider) createThingType(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		name, _ = params["thingTypeName"].(string)
	}
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingTypeName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iot", "thingtype", name)
	tt := &ThingType{
		Name:            name,
		ARN:             arn,
		SearchableAttrs: "[]",
		CreatedAt:       time.Now().Unix(),
	}
	if props, ok := params["thingTypeProperties"].(map[string]any); ok {
		if v, ok2 := props["thingTypeDescription"].(string); ok2 {
			tt.Description = v
		}
		if attrs, ok2 := props["searchableAttributes"].([]any); ok2 {
			if b, err := json.Marshal(attrs); err == nil {
				tt.SearchableAttrs = string(b)
			}
		}
	}
	if err := p.store.CreateThingType(tt); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "thing type already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"thingTypeName": name,
		"thingTypeArn":  arn,
	})
}

func (p *Provider) describeThingType(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingTypeName is required", http.StatusBadRequest), nil
	}
	tt, err := p.store.GetThingType(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing type not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, thingTypeToMap(tt))
}

func (p *Provider) listThingTypes() (*plugin.Response, error) {
	types, err := p.store.ListThingTypes()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(types))
	for i := range types {
		result = append(result, thingTypeToMap(&types[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"thingTypes": result})
}

func (p *Provider) deleteThingType(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingTypeName is required", http.StatusBadRequest), nil
	}
	tt, err := p.store.DeleteThingType(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing type not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(tt.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deprecateThingType(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingTypeName is required", http.StatusBadRequest), nil
	}
	deprecated := true
	if v, ok := params["undoDeprecate"].(bool); ok && v {
		deprecated = false
	}
	if err := p.store.DeprecateThingType(name, deprecated); err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing type not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateThingType(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingTypeName is required", http.StatusBadRequest), nil
	}
	// For update, re-use deprecate if needed; otherwise it's a no-op returning success
	_ = params
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Thing Group operations ---

func (p *Provider) createThingGroup(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		name, _ = params["thingGroupName"].(string)
	}
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iot", "thinggroup", name)
	tg := &ThingGroup{
		Name:      name,
		ARN:       arn,
		Version:   1,
		CreatedAt: time.Now().Unix(),
	}
	if v, ok := params["parentGroupName"].(string); ok {
		tg.Parent = v
	}
	if props, ok := params["thingGroupProperties"].(map[string]any); ok {
		if v, ok2 := props["thingGroupDescription"].(string); ok2 {
			tg.Description = v
		}
	}
	if err := p.store.CreateThingGroup(tg); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "thing group already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"thingGroupName": name,
		"thingGroupArn":  arn,
		"thingGroupId":   name,
	})
}

func (p *Provider) describeThingGroup(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName is required", http.StatusBadRequest), nil
	}
	tg, err := p.store.GetThingGroup(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, thingGroupToMap(tg))
}

func (p *Provider) listThingGroups() (*plugin.Response, error) {
	groups, err := p.store.ListThingGroups()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(groups))
	for i := range groups {
		result = append(result, map[string]any{
			"groupName": groups[i].Name,
			"groupArn":  groups[i].ARN,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"thingGroups": result})
}

func (p *Provider) updateThingGroup(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateThingGroup(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing group not found", http.StatusNotFound), nil
	}
	tg, _ := p.store.GetThingGroup(name)
	version := int64(1)
	if tg != nil {
		version = tg.Version
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"version": version})
}

func (p *Provider) deleteThingGroup(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName is required", http.StatusBadRequest), nil
	}
	tg, err := p.store.DeleteThingGroup(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "thing group not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(tg.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) addThingToThingGroup(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["thingGroupName"].(string)
	thingName, _ := params["thingName"].(string)
	if groupName == "" || thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName and thingName are required", http.StatusBadRequest), nil
	}
	if err := p.store.AddThingToThingGroup(groupName, thingName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) removeThingFromThingGroup(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["thingGroupName"].(string)
	thingName, _ := params["thingName"].(string)
	if groupName == "" || thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName and thingName are required", http.StatusBadRequest), nil
	}
	if err := p.store.RemoveThingFromThingGroup(groupName, thingName); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listThingsInThingGroup(groupName string) (*plugin.Response, error) {
	if groupName == "" {
		return shared.JSONError("InvalidRequestException", "thingGroupName is required", http.StatusBadRequest), nil
	}
	things, err := p.store.ListThingsInThingGroup(groupName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(things))
	for _, name := range things {
		result = append(result, name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"things": result})
}

func (p *Provider) listThingGroupsForThing(thingName string) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	groups, err := p.store.ListThingGroupsForThing(thingName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(groups))
	for _, name := range groups {
		result = append(result, map[string]any{"groupName": name})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"thingGroups": result})
}

// --- Policy operations ---

func (p *Provider) createPolicy(params map[string]any) (*plugin.Response, error) {
	name, _ := params["policyName"].(string)
	if name == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	doc, _ := params["policyDocument"].(string)
	if doc == "" {
		doc = "{}"
	}
	arn := shared.BuildARN("iot", "policy", name)
	now := time.Now().Unix()
	pol := &Policy{
		Name:      name,
		ARN:       arn,
		Document:  doc,
		Version:   1,
		CreatedAt: now,
	}
	if err := p.store.CreatePolicy(pol); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "policy already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	// Create initial policy version
	pv := &PolicyVersion{
		PolicyName: name,
		VersionID:  "1",
		Document:   doc,
		IsDefault:  true,
		CreatedAt:  now,
	}
	p.store.CreatePolicyVersion(pv) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policyName":      name,
		"policyArn":       arn,
		"policyDocument":  doc,
		"policyVersionId": "1",
	})
}

func (p *Provider) getPolicy(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	pol, err := p.store.GetPolicy(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, policyToMap(pol))
}

func (p *Provider) listPolicies() (*plugin.Response, error) {
	policies, err := p.store.ListPolicies()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(policies))
	for i := range policies {
		result = append(result, policyToMap(&policies[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"policies": result})
}

func (p *Provider) deletePolicy(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	pol, err := p.store.DeletePolicy(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(pol.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) createPolicyVersion(policyName string, params map[string]any) (*plugin.Response, error) {
	if policyName == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPolicy(policyName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy not found", http.StatusNotFound), nil
	}
	doc, _ := params["policyDocument"].(string)
	if doc == "" {
		doc = "{}"
	}
	setAsDefault, _ := params["setAsDefault"].(bool)
	// Find next version number
	existing, _ := p.store.ListPolicyVersions(policyName)
	nextVersion := len(existing) + 1
	versionID := fmt.Sprintf("%d", nextVersion)
	pv := &PolicyVersion{
		PolicyName: policyName,
		VersionID:  versionID,
		Document:   doc,
		IsDefault:  setAsDefault,
		CreatedAt:  time.Now().Unix(),
	}
	if err := p.store.CreatePolicyVersion(pv); err != nil {
		return nil, err
	}
	if setAsDefault {
		p.store.SetDefaultPolicyVersion(policyName, versionID) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policyArn":        shared.BuildARN("iot", "policy", policyName),
		"policyDocument":   doc,
		"policyVersionId":  versionID,
		"isDefaultVersion": setAsDefault,
	})
}

func (p *Provider) listPolicyVersions(policyName string) (*plugin.Response, error) {
	if policyName == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	versions, err := p.store.ListPolicyVersions(policyName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(versions))
	for i := range versions {
		result = append(result, map[string]any{
			"versionId":        versions[i].VersionID,
			"isDefaultVersion": versions[i].IsDefault,
			"createDate":       versions[i].CreatedAt,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"policyVersions": result})
}

func (p *Provider) getPolicyVersion(policyName, versionID string) (*plugin.Response, error) {
	if policyName == "" || versionID == "" {
		return shared.JSONError("InvalidRequestException", "policyName and versionId are required", http.StatusBadRequest), nil
	}
	pv, err := p.store.GetPolicyVersion(policyName, versionID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy version not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"policyArn":        shared.BuildARN("iot", "policy", policyName),
		"policyName":       policyName,
		"policyDocument":   pv.Document,
		"policyVersionId":  pv.VersionID,
		"isDefaultVersion": pv.IsDefault,
		"creationDate":     pv.CreatedAt,
		"lastModifiedDate": pv.CreatedAt,
	})
}

func (p *Provider) setDefaultPolicyVersion(policyName, versionID string) (*plugin.Response, error) {
	if policyName == "" || versionID == "" {
		return shared.JSONError("InvalidRequestException", "policyName and versionId are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetDefaultPolicyVersion(policyName, versionID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy version not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deletePolicyVersion(policyName, versionID string) (*plugin.Response, error) {
	if policyName == "" || versionID == "" {
		return shared.JSONError("InvalidRequestException", "policyName and versionId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePolicyVersion(policyName, versionID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "policy version not found or is default", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) attachPolicy(policyName string, params map[string]any) (*plugin.Response, error) {
	if policyName == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	target, _ := params["target"].(string)
	if target == "" {
		return shared.JSONError("InvalidRequestException", "target is required", http.StatusBadRequest), nil
	}
	if err := p.store.AttachPolicy(policyName, target); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) detachPolicy(policyName string, params map[string]any) (*plugin.Response, error) {
	if policyName == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	target, _ := params["target"].(string)
	if target == "" {
		return shared.JSONError("InvalidRequestException", "target is required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachPolicy(policyName, target); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listAttachedPolicies(req *http.Request, params map[string]any) (*plugin.Response, error) {
	target := req.URL.Query().Get("target")
	if target == "" {
		target, _ = params["target"].(string)
	}
	if target == "" {
		return shared.JSONError("InvalidRequestException", "target is required", http.StatusBadRequest), nil
	}
	names, err := p.store.ListAttachedPolicies(target)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(names))
	for _, name := range names {
		pol, _ := p.store.GetPolicy(name)
		if pol != nil {
			result = append(result, policyToMap(pol))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"policies": result})
}

func (p *Provider) listTargetsForPolicy(policyName string) (*plugin.Response, error) {
	if policyName == "" {
		return shared.JSONError("InvalidRequestException", "policyName is required", http.StatusBadRequest), nil
	}
	targets, err := p.store.ListTargetsForPolicy(policyName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(targets))
	for _, t := range targets {
		result = append(result, t)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"targets": result})
}

// --- Certificate operations ---

func (p *Provider) createKeysAndCertificate(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateID("", 64)
	arn := shared.BuildARN("iot", "cert", id)
	active := true
	if v, ok := params["setAsActive"].(bool); ok {
		active = v
	}
	status := "INACTIVE"
	if active {
		status = "ACTIVE"
	}
	c := &Certificate{
		ID:        id,
		ARN:       arn,
		Status:    status,
		PEM:       "-----BEGIN CERTIFICATE-----\nMIIDummy\n-----END CERTIFICATE-----\n",
		CAPEM:     "",
		CreatedAt: time.Now().Unix(),
	}
	if err := p.store.CreateCertificate(c); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"certificateArn": arn,
		"certificateId":  id,
		"certificatePem": c.PEM,
		"keyPair": map[string]any{
			"PublicKey":  "-----BEGIN PUBLIC KEY-----\nMIIDummyPublic\n-----END PUBLIC KEY-----\n",
			"PrivateKey": "-----BEGIN RSA PRIVATE KEY-----\nMIIDummyPrivate\n-----END RSA PRIVATE KEY-----\n",
		},
	})
}

func (p *Provider) registerCertificate(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateID("", 64)
	arn := shared.BuildARN("iot", "cert", id)
	pem, _ := params["certificatePem"].(string)
	caPem, _ := params["caCertificatePem"].(string)
	c := &Certificate{
		ID:        id,
		ARN:       arn,
		Status:    "INACTIVE",
		PEM:       pem,
		CAPEM:     caPem,
		CreatedAt: time.Now().Unix(),
	}
	if err := p.store.CreateCertificate(c); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"certificateArn": arn,
		"certificateId":  id,
	})
}

func (p *Provider) registerCertificateWithoutCA(params map[string]any) (*plugin.Response, error) {
	id := shared.GenerateID("", 64)
	arn := shared.BuildARN("iot", "cert", id)
	pem, _ := params["certificatePem"].(string)
	c := &Certificate{
		ID:        id,
		ARN:       arn,
		Status:    "ACTIVE",
		PEM:       pem,
		CreatedAt: time.Now().Unix(),
	}
	if err := p.store.CreateCertificate(c); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"certificateArn": arn,
		"certificateId":  id,
	})
}

func (p *Provider) describeCertificate(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "certificateId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetCertificate(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "certificate not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"certificateDescription": certToMap(c),
	})
}

func (p *Provider) listCertificates() (*plugin.Response, error) {
	certs, err := p.store.ListCertificates()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(certs))
	for i := range certs {
		result = append(result, certToMap(&certs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"certificates": result})
}

func (p *Provider) updateCertificate(id string, req *http.Request, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "certificateId is required", http.StatusBadRequest), nil
	}
	status := req.URL.Query().Get("newStatus")
	if status == "" {
		status, _ = params["newStatus"].(string)
	}
	if status == "" {
		status = "ACTIVE"
	}
	if err := p.store.UpdateCertificate(id, status); err != nil {
		return shared.JSONError("ResourceNotFoundException", "certificate not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteCertificate(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "certificateId is required", http.StatusBadRequest), nil
	}
	c, err := p.store.DeleteCertificate(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "certificate not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(c.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Thing Principal operations ---

func (p *Provider) attachThingPrincipal(thingName string, req *http.Request) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	principal := req.Header.Get("x-amzn-principal")
	if principal == "" {
		return shared.JSONError("InvalidRequestException", "x-amzn-principal header is required", http.StatusBadRequest), nil
	}
	if err := p.store.AttachThingPrincipal(thingName, principal); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) detachThingPrincipal(thingName string, req *http.Request) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	principal := req.Header.Get("x-amzn-principal")
	if principal == "" {
		return shared.JSONError("InvalidRequestException", "x-amzn-principal header is required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachThingPrincipal(thingName, principal); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listThingPrincipals(thingName string) (*plugin.Response, error) {
	if thingName == "" {
		return shared.JSONError("InvalidRequestException", "thingName is required", http.StatusBadRequest), nil
	}
	principals, err := p.store.ListThingPrincipals(thingName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(principals))
	for _, pr := range principals {
		result = append(result, pr)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"principals": result})
}

func (p *Provider) listPrincipalThings(req *http.Request) (*plugin.Response, error) {
	principal := req.Header.Get("x-amzn-principal")
	if principal == "" {
		return shared.JSONError("InvalidRequestException", "x-amzn-principal header is required", http.StatusBadRequest), nil
	}
	things, err := p.store.ListPrincipalThings(principal)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(things))
	for _, name := range things {
		result = append(result, name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"things": result})
}

// --- Topic Rule operations ---

func (p *Provider) createTopicRule(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iot", "rule", name)
	sqlQuery := ""
	actionsJSON := "[]"
	if payload, ok := params["topicRulePayload"].(map[string]any); ok {
		if v, ok2 := payload["sql"].(string); ok2 {
			sqlQuery = v
		}
		if v, ok2 := payload["actions"]; ok2 {
			if b, err := json.Marshal(v); err == nil {
				actionsJSON = string(b)
			}
		}
	}
	tr := &TopicRule{
		Name:      name,
		ARN:       arn,
		SQLQuery:  sqlQuery,
		Actions:   actionsJSON,
		Enabled:   true,
		CreatedAt: time.Now().Unix(),
	}
	if err := p.store.CreateTopicRule(tr); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "topic rule already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getTopicRule(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	tr, err := p.store.GetTopicRule(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "topic rule not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, topicRuleToMap(tr))
}

func (p *Provider) listTopicRules() (*plugin.Response, error) {
	rules, err := p.store.ListTopicRules()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(rules))
	for i := range rules {
		result = append(result, map[string]any{
			"ruleArn":      rules[i].ARN,
			"ruleName":     rules[i].Name,
			"ruleDisabled": !rules[i].Enabled,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"rules": result})
}

func (p *Provider) replaceTopicRule(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	fields := map[string]any{}
	if payload, ok := params["topicRulePayload"].(map[string]any); ok {
		fields = payload
	}
	if err := p.store.ReplaceTopicRule(name, fields); err != nil {
		return shared.JSONError("ResourceNotFoundException", "topic rule not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteTopicRule(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	tr, err := p.store.DeleteTopicRule(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "topic rule not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(tr.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) enableTopicRule(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	if err := p.store.SetTopicRuleEnabled(name, true); err != nil {
		return shared.JSONError("ResourceNotFoundException", "topic rule not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) disableTopicRule(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "ruleName is required", http.StatusBadRequest), nil
	}
	if err := p.store.SetTopicRuleEnabled(name, false); err != nil {
		return shared.JSONError("ResourceNotFoundException", "topic rule not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Job operations ---

func (p *Provider) createJob(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		id, _ = params["jobId"].(string)
	}
	if id == "" {
		return shared.JSONError("InvalidRequestException", "jobId is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("iot", "job", id)
	targetsJSON := "[]"
	if targets, ok := params["targets"]; ok {
		if b, err := json.Marshal(targets); err == nil {
			targetsJSON = string(b)
		}
	}
	docJSON := "{}"
	if v, ok := params["document"].(string); ok && v != "" {
		docJSON = v
	}
	j := &Job{
		ID:          id,
		ARN:         arn,
		Status:      "IN_PROGRESS",
		Targets:     targetsJSON,
		Document:    docJSON,
		Description: strParam(params, "description"),
		CreatedAt:   time.Now().Unix(),
	}
	if err := p.store.CreateJob(j); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "job already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobArn":      arn,
		"jobId":       id,
		"description": j.Description,
	})
}

func (p *Provider) describeJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "jobId is required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetJob(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"job": jobToMap(j),
	})
}

func (p *Provider) listJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListJobs()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(jobs))
	for i := range jobs {
		result = append(result, map[string]any{
			"jobArn":      jobs[i].ARN,
			"jobId":       jobs[i].ID,
			"status":      jobs[i].Status,
			"description": jobs[i].Description,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobs": result})
}

func (p *Provider) cancelJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "jobId is required", http.StatusBadRequest), nil
	}
	if err := p.store.CancelJob(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	j, _ := p.store.GetJob(id)
	if j == nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobArn":      j.ARN,
		"jobId":       j.ID,
		"description": j.Description,
	})
}

func (p *Provider) deleteJob(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("InvalidRequestException", "jobId is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteJob(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Role Alias operations ---

func (p *Provider) createRoleAlias(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		name, _ = params["roleAlias"].(string)
	}
	if name == "" {
		return shared.JSONError("InvalidRequestException", "roleAlias is required", http.StatusBadRequest), nil
	}
	roleARN, _ := params["roleArn"].(string)
	duration := int64(3600)
	if v, ok := params["credentialDurationSeconds"].(float64); ok {
		duration = int64(v)
	}
	arn := shared.BuildARN("iot", "rolealias", name)
	ra := &RoleAlias{
		Name:               name,
		ARN:                arn,
		RoleARN:            roleARN,
		CredentialDuration: duration,
		CreatedAt:          time.Now().Unix(),
	}
	if err := p.store.CreateRoleAlias(ra); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "role alias already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"roleAlias":    name,
		"roleAliasArn": arn,
	})
}

func (p *Provider) describeRoleAlias(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "roleAlias is required", http.StatusBadRequest), nil
	}
	ra, err := p.store.GetRoleAlias(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "role alias not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"roleAliasDescription": roleAliasToMap(ra),
	})
}

func (p *Provider) listRoleAliases() (*plugin.Response, error) {
	aliases, err := p.store.ListRoleAliases()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(aliases))
	for _, ra := range aliases {
		result = append(result, ra.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"roleAliases": result})
}

func (p *Provider) updateRoleAlias(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "roleAlias is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRoleAlias(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "role alias not found", http.StatusNotFound), nil
	}
	ra, _ := p.store.GetRoleAlias(name)
	if ra == nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"roleAlias":    name,
		"roleAliasArn": ra.ARN,
	})
}

func (p *Provider) deleteRoleAlias(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("InvalidRequestException", "roleAlias is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteRoleAlias(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "role alias not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Endpoint ---

func (p *Provider) describeEndpoint(req *http.Request) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"endpointAddress": fmt.Sprintf("%s.iot.%s.amazonaws.com", shared.DefaultAccountID, shared.DefaultRegion),
	})
}

// --- Tags operations ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		if v, ok := params["resourceArn"].(string); ok {
			arn = v
		}
	}
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resourceArn is required", http.StatusBadRequest), nil
	}
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, tagsListToMap(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resourceArn is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	p.store.tags.RemoveTags(arn, keys) //nolint:errcheck
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := req.URL.Query().Get("resourceArn")
	if arn == "" {
		return shared.JSONError("InvalidRequestException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(arn)
	tagList := make([]any, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]any{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tagList})
}

// --- Serialization helpers ---

func thingToMap(t *Thing) map[string]any {
	var attrs map[string]any
	_ = json.Unmarshal([]byte(t.Attributes), &attrs)
	if attrs == nil {
		attrs = map[string]any{}
	}
	return map[string]any{
		"thingName":     t.Name,
		"thingArn":      t.ARN,
		"thingTypeName": t.TypeName,
		"attributes":    attrs,
		"version":       t.Version,
	}
}

func thingTypeToMap(tt *ThingType) map[string]any {
	var searchableAttrs []any
	_ = json.Unmarshal([]byte(tt.SearchableAttrs), &searchableAttrs)
	if searchableAttrs == nil {
		searchableAttrs = []any{}
	}
	return map[string]any{
		"thingTypeName": tt.Name,
		"thingTypeArn":  tt.ARN,
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": tt.Description,
			"searchableAttributes": searchableAttrs,
		},
		"thingTypeMetadata": map[string]any{
			"deprecated":      tt.Deprecated,
			"creationDate":    tt.CreatedAt,
			"deprecationDate": nil,
		},
	}
}

func thingGroupToMap(tg *ThingGroup) map[string]any {
	return map[string]any{
		"thingGroupName": tg.Name,
		"thingGroupArn":  tg.ARN,
		"thingGroupId":   tg.Name,
		"version":        tg.Version,
		"thingGroupProperties": map[string]any{
			"thingGroupDescription": tg.Description,
		},
		"thingGroupMetadata": map[string]any{
			"parentGroupName": tg.Parent,
			"creationDate":    tg.CreatedAt,
		},
	}
}

func policyToMap(pol *Policy) map[string]any {
	return map[string]any{
		"policyName":       pol.Name,
		"policyArn":        pol.ARN,
		"policyDocument":   pol.Document,
		"defaultVersionId": fmt.Sprintf("%d", pol.Version),
		"creationDate":     pol.CreatedAt,
		"lastModifiedDate": pol.CreatedAt,
	}
}

func certToMap(c *Certificate) map[string]any {
	return map[string]any{
		"certificateArn":   c.ARN,
		"certificateId":    c.ID,
		"status":           c.Status,
		"certificatePem":   c.PEM,
		"creationDate":     c.CreatedAt,
		"lastModifiedDate": c.CreatedAt,
	}
}

func topicRuleToMap(tr *TopicRule) map[string]any {
	var actions []any
	_ = json.Unmarshal([]byte(tr.Actions), &actions)
	if actions == nil {
		actions = []any{}
	}
	return map[string]any{
		"ruleArn":  tr.ARN,
		"ruleName": tr.Name,
		"topicRulePayload": map[string]any{
			"sql":          tr.SQLQuery,
			"actions":      actions,
			"ruleDisabled": !tr.Enabled,
		},
		"createdAt": tr.CreatedAt,
	}
}

func jobToMap(j *Job) map[string]any {
	var targets []any
	_ = json.Unmarshal([]byte(j.Targets), &targets)
	if targets == nil {
		targets = []any{}
	}
	return map[string]any{
		"jobArn":      j.ARN,
		"jobId":       j.ID,
		"status":      j.Status,
		"targets":     targets,
		"description": j.Description,
		"createdAt":   j.CreatedAt,
	}
}

func roleAliasToMap(ra *RoleAlias) map[string]any {
	return map[string]any{
		"roleAlias":                 ra.Name,
		"roleAliasArn":              ra.ARN,
		"roleArn":                   ra.RoleARN,
		"credentialDurationSeconds": ra.CredentialDuration,
		"creationDate":              ra.CreatedAt,
		"lastModifiedDate":          ra.CreatedAt,
	}
}

// --- Utility functions ---

// injectPathParams extracts resource names from IoT REST URL paths and
// merges them into the params map so handlers can find them.
// e.g. /things/myThing → params["thingName"] = "myThing"
//
//	/policies/myPolicy → params["policyName"] = "myPolicy"
func injectPathParams(path string, params map[string]any) {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) < 2 {
		return
	}
	// Map first segment to param name for 2-segment paths like /things/{name}
	paramMap := map[string]string{
		"things":       "thingName",
		"thing-types":  "thingTypeName",
		"thing-groups": "thingGroupName",
		"policies":     "policyName",
		"certificates": "certificateId",
		"rules":        "ruleName",
		"jobs":         "jobId",
		"role-aliases": "roleAlias",
	}
	for i, seg := range parts {
		if paramName, ok := paramMap[seg]; ok {
			if i+1 < len(parts) && parts[i+1] != "" {
				// Don't override if already set from body
				if _, exists := params[paramName]; !exists {
					params[paramName] = parts[i+1]
				}
			}
		}
	}
}

func extractLastPathSegment(path string) string {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func extractPathParam(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func strParam(params map[string]any, key string) string {
	v, _ := params[key].(string)
	return v
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func tagsListToMap(list []any) map[string]string {
	m := make(map[string]string, len(list))
	for _, item := range list {
		if t, ok := item.(map[string]any); ok {
			k, _ := t["Key"].(string)
			v, _ := t["Value"].(string)
			if k != "" {
				m[k] = v
			}
		}
	}
	return m
}
