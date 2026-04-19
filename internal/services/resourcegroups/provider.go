// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroups/provider.go
package resourcegroups

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

// Provider implements the Resource Groups (Ardi) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "resourcegroups" }
func (p *Provider) ServiceName() string           { return "Ardi" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "resourcegroups"))
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
	case "CreateGroup":
		return p.createGroup(params)
	case "GetGroup":
		return p.getGroup(params)
	case "ListGroups":
		return p.listGroups()
	case "UpdateGroup":
		return p.updateGroup(params)
	case "DeleteGroup":
		return p.deleteGroup(params)
	case "GetGroupQuery":
		return p.getGroupQuery(params)
	case "UpdateGroupQuery":
		return p.updateGroupQuery(params)
	case "GetGroupConfiguration":
		return p.getGroupConfiguration(params)
	case "PutGroupConfiguration":
		return p.putGroupConfiguration(params)
	case "GetTags":
		return p.getTags(req)
	case "Tag":
		return p.tagResource(req, params)
	case "Untag":
		return p.untagResource(req, params)
	case "GroupResources":
		return p.groupResources(params)
	case "UngroupResources":
		return p.ungroupResources(params)
	case "ListGroupResources":
		return p.listGroupResources(params)
	case "SearchResources":
		return p.searchResources(params)
	case "GetAccountSettings":
		return p.getAccountSettings()
	case "UpdateAccountSettings":
		return p.updateAccountSettings(params)
	// TagSync ops - return success/empty
	case "StartTagSyncTask":
		return shared.JSONResponse(http.StatusOK, map[string]any{"taskArn": "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/" + shared.GenerateID("", 16)})
	case "GetTagSyncTask":
		return shared.JSONResponse(http.StatusOK, map[string]any{"status": "ACTIVE"})
	case "ListTagSyncTasks":
		return shared.JSONResponse(http.StatusOK, map[string]any{"tagSyncTasks": []any{}})
	case "CancelTagSyncTask":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListGroupingStatuses":
		return shared.JSONResponse(http.StatusOK, map[string]any{"groupingStatuses": []any{}})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	groups, err := p.store.ListGroups()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(groups))
	for _, g := range groups {
		res = append(res, plugin.Resource{Type: "resourcegroups-group", ID: g.Name, Name: g.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- CRUD ---

func (p *Provider) createGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}

	description, _ := params["Description"].(string)

	resourceQuery := "{}"
	if rq, ok := params["ResourceQuery"].(map[string]any); ok {
		b, _ := json.Marshal(rq)
		resourceQuery = string(b)
	}

	config := "{}"
	if cfg, ok := params["Configuration"].([]any); ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}

	arn := shared.BuildARN("resource-groups", "group", name)

	g := &Group{
		Name:          name,
		ARN:           arn,
		Description:   description,
		ResourceQuery: resourceQuery,
		Config:        config,
	}

	if err := p.store.CreateGroup(g); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "group already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		_ = p.store.tags.AddTags(arn, tags) //nolint:errcheck
	}

	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group":              groupToMap(g),
		"GroupConfiguration": map[string]any{"configuration": []any{}, "status": "UPDATE_COMPLETE"},
		"ResourceQuery":      json.RawMessage(resourceQuery),
		"Tags":               tags,
	})
}

func (p *Provider) getGroup(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(g),
	})
}

func (p *Provider) listGroups() (*plugin.Response, error) {
	groups, err := p.store.ListGroups()
	if err != nil {
		return nil, err
	}
	identifiers := make([]map[string]any, 0, len(groups))
	groupList := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		identifiers = append(identifiers, map[string]any{
			"GroupArn":  g.ARN,
			"GroupName": g.Name,
		})
		groupList = append(groupList, groupToMap(&g))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupIdentifiers": identifiers,
		"Groups":           groupList,
	})
}

func (p *Provider) updateGroup(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateGroup(name, params); err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(g),
	})
}

func (p *Provider) deleteGroup(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(g.ARN) //nolint:errcheck
	if err := p.store.DeleteGroup(name); err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(g),
	})
}

func (p *Provider) getGroupQuery(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	var rq any
	_ = json.Unmarshal([]byte(g.ResourceQuery), &rq)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupQuery": map[string]any{
			"GroupName":     g.Name,
			"ResourceQuery": rq,
		},
	})
}

func (p *Provider) updateGroupQuery(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	resourceQuery := "{}"
	if rq, ok := params["ResourceQuery"].(map[string]any); ok {
		b, _ := json.Marshal(rq)
		resourceQuery = string(b)
	}
	if err := p.store.UpdateGroupQuery(name, resourceQuery); err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	var rq any
	_ = json.Unmarshal([]byte(resourceQuery), &rq)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupQuery": map[string]any{
			"GroupName":     name,
			"ResourceQuery": rq,
		},
	})
}

func (p *Provider) getGroupConfiguration(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	var cfg any
	_ = json.Unmarshal([]byte(g.Config), &cfg)
	if cfg == nil {
		cfg = []any{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"GroupConfiguration": map[string]any{
			"Configuration": cfg,
			"Status":        "UPDATE_COMPLETE",
		},
	})
}

func (p *Provider) putGroupConfiguration(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	config := "{}"
	if cfg, ok := params["Configuration"].([]any); ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	if err := p.store.UpdateGroupConfig(name, config); err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *Provider) getTags(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "resources")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn":  arn,
		"Tags": tags,
	})
}

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "resources")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := toStringMap(rawTags)
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	allTags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn":  arn,
		"Tags": allTags,
	})
}

func (p *Provider) untagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "resources")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	// Keys come from query string
	keys := req.URL.Query()["Keys"]
	if len(keys) == 0 {
		// Also accept from body
		if rawKeys, ok := params["Keys"].([]any); ok {
			for _, k := range rawKeys {
				if s, ok := k.(string); ok {
					keys = append(keys, s)
				}
			}
		}
	}
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	allTags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Arn":  arn,
		"Keys": keys,
		"Tags": allTags,
	})
}

// --- Resource grouping (in-memory stub, returns success) ---

func (p *Provider) groupResources(params map[string]any) (*plugin.Response, error) {
	arns := toStringSlice(params["ResourceArns"])
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Succeeded": arns,
		"Failed":    []any{},
		"Pending":   []any{},
	})
}

func (p *Provider) ungroupResources(params map[string]any) (*plugin.Response, error) {
	arns := toStringSlice(params["ResourceArns"])
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Succeeded": arns,
		"Failed":    []any{},
		"Pending":   []any{},
	})
}

func (p *Provider) listGroupResources(params map[string]any) (*plugin.Response, error) {
	name := resolveGroupName(params)
	if name == "" {
		return shared.JSONError("ValidationException", "group name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetGroup(name); err != nil {
		return shared.JSONError("NotFoundException", "group not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Resources":           []any{},
		"ResourceIdentifiers": []any{},
		"QueryErrors":         []any{},
	})
}

func (p *Provider) searchResources(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceIdentifiers": []any{},
		"QueryErrors":         []any{},
	})
}

// --- Account settings ---

func (p *Provider) getAccountSettings() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccountSettings": map[string]any{
			"GroupLifecycleEventsDesiredStatus": "ACTIVE",
			"GroupLifecycleEventsStatus":        "ACTIVE",
			"GroupLifecycleEventsStatusMessage": "",
		},
	})
}

func (p *Provider) updateAccountSettings(params map[string]any) (*plugin.Response, error) {
	status, _ := params["GroupLifecycleEventsDesiredStatus"].(string)
	if status == "" {
		status = "ACTIVE"
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AccountSettings": map[string]any{
			"GroupLifecycleEventsDesiredStatus": status,
			"GroupLifecycleEventsStatus":        status,
			"GroupLifecycleEventsStatusMessage": "",
		},
	})
}

// resolveOp maps HTTP method+path to a Resource Groups operation name.
func resolveOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	first := segs[0]

	switch first {
	case "groups":
		if n == 1 {
			return "CreateGroup" // POST /groups
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetGroup"
			case "PUT":
				return "UpdateGroup"
			case "DELETE":
				return "DeleteGroup"
			}
		}
		if n == 3 && segs[2] == "query" {
			switch method {
			case "GET":
				return "GetGroupQuery"
			case "PUT":
				return "UpdateGroupQuery"
			}
		}
	case "groups-list":
		return "ListGroups" // POST /groups-list
	case "get-group":
		return "GetGroup"
	case "update-group":
		return "UpdateGroup"
	case "delete-group":
		return "DeleteGroup"
	case "get-group-query":
		return "GetGroupQuery"
	case "update-group-query":
		return "UpdateGroupQuery"
	case "get-group-configuration":
		return "GetGroupConfiguration"
	case "put-group-configuration":
		return "PutGroupConfiguration"
	case "resources":
		if n >= 3 && segs[n-1] == "tags" {
			switch method {
			case "GET":
				return "GetTags"
			case "PUT":
				return "Tag"
			case "PATCH":
				return "Untag"
			}
		}
		if n == 2 && segs[1] == "search" {
			return "SearchResources"
		}
	case "group-resources":
		return "GroupResources"
	case "ungroup-resources":
		return "UngroupResources"
	case "list-group-resources":
		return "ListGroupResources"
	case "get-account-settings":
		return "GetAccountSettings"
	case "update-account-settings":
		return "UpdateAccountSettings"
	case "start-tag-sync-task":
		return "StartTagSyncTask"
	case "get-tag-sync-task":
		return "GetTagSyncTask"
	case "list-tag-sync-tasks":
		return "ListTagSyncTasks"
	case "cancel-tag-sync-task":
		return "CancelTagSyncTask"
	case "list-grouping-statuses":
		return "ListGroupingStatuses"
	}
	return ""
}

// --- Helpers ---

func groupToMap(g *Group) map[string]any {
	return map[string]any{
		"Name":        g.Name,
		"GroupArn":    g.ARN,
		"Description": g.Description,
	}
}

func resolveGroupName(params map[string]any) string {
	if v, ok := params["GroupName"].(string); ok && v != "" {
		return v
	}
	if v, ok := params["Group"].(string); ok && v != "" {
		return v
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

func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string)
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func toStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
