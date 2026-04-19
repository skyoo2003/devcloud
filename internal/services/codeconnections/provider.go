// SPDX-License-Identifier: Apache-2.0

// Package codeconnections implements AWS CodeConnections.
package codeconnections

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

const defaultAccountID = plugin.DefaultAccountID

// CodeConnectionsProvider implements plugin.ServicePlugin for CodeConnections.
type CodeConnectionsProvider struct {
	store *Store
}

// ServiceID returns the unique identifier for this plugin.
func (p *CodeConnectionsProvider) ServiceID() string { return "codeconnections" }

// ServiceName returns the human-readable name for this plugin.
func (p *CodeConnectionsProvider) ServiceName() string {
	return "com.amazonaws.codeconnections.CodeConnections_20231201"
}

// Protocol returns the wire protocol used by this plugin.
func (p *CodeConnectionsProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

// Init initialises the CodeConnectionsProvider from cfg.
func (p *CodeConnectionsProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codeconnections"))
	return err
}

// Shutdown closes the CodeConnectionsProvider.
func (p *CodeConnectionsProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate CodeConnections operation.
func (p *CodeConnectionsProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			op = target[idx+1:]
		} else {
			op = target
		}
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
	// Connections
	case "CreateConnection":
		return p.createConnection(params)
	case "GetConnection":
		return p.getConnection(params)
	case "ListConnections":
		return p.listConnections(params)
	case "DeleteConnection":
		return p.deleteConnection(params)
	case "UpdateConnection":
		return p.updateConnection(params)
	case "DescribeConnection":
		return p.getConnection(params)
	case "ListConnectionOwners":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Owners": []any{}})

	// Hosts
	case "CreateHost":
		return p.createHost(params)
	case "GetHost":
		return p.getHost(params)
	case "ListHosts":
		return p.listHosts(params)
	case "DeleteHost":
		return p.deleteHost(params)
	case "UpdateHost":
		return p.updateHost(params)

	// Repository Links
	case "CreateRepositoryLink":
		return p.createRepositoryLink(params)
	case "GetRepositoryLink":
		return p.getRepositoryLink(params)
	case "ListRepositoryLinks":
		return p.listRepositoryLinks(params)
	case "DeleteRepositoryLink":
		return p.deleteRepositoryLink(params)
	case "UpdateRepositoryLink":
		return p.updateRepositoryLink(params)

	// Sync Configurations
	case "CreateSyncConfiguration":
		return p.createSyncConfiguration(params)
	case "GetSyncConfiguration":
		return p.getSyncConfiguration(params)
	case "ListSyncConfigurations":
		return p.listSyncConfigurations(params)
	case "DeleteSyncConfiguration":
		return p.deleteSyncConfiguration(params)
	case "UpdateSyncConfiguration":
		return p.updateSyncConfiguration(params)

	// Sync Blockers and Status
	case "GetSyncBlockerSummary":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SyncBlockerSummary": map[string]any{
				"ResourceName":       getString(params, "ResourceName"),
				"ParentResourceName": "",
				"LatestBlockers":     []any{},
			},
		})
	case "ListSyncBlockerSummaries":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SyncBlockerSummaries": []any{},
			"NextToken":            nil,
		})
	case "CreateSyncBlocker":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SyncBlocker": map[string]any{
				"Id":     shared.GenerateID("blocker-", 16),
				"Type":   getString(params, "Type"),
				"Status": "ACTIVE",
			},
		})
	case "UpdateSyncBlocker":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourceName":       getString(params, "ResourceName"),
			"ParentResourceName": "",
			"SyncBlocker": map[string]any{
				"Id":     getString(params, "Id"),
				"Status": "RESOLVED",
			},
		})
	case "StartRepositoryLink":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"RepositoryLinkId": getString(params, "RepositoryLinkId"),
		})
	case "GetRepositorySyncStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"LatestSync": map[string]any{
				"Status":    "SUCCEEDED",
				"StartedAt": time.Now().Unix(),
			},
		})
	case "GetResourceSyncStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"LatestSync": map[string]any{
				"Status":    "SUCCEEDED",
				"StartedAt": time.Now().Unix(),
			},
			"DesiredState": map[string]any{},
			"LatestSuccessfulSync": map[string]any{
				"Status": "SUCCEEDED",
			},
		})
	case "ListRepositorySyncDefinitions":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"RepositorySyncDefinitions": []any{},
			"NextToken":                 nil,
		})

	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// Account configuration
	case "PassConnection":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PassRepository":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation not supported: %s", op), http.StatusBadRequest), nil
	}
}

// ListResources returns connections as plugin resources.
func (p *CodeConnectionsProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	conns, err := p.store.ListConnections()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(conns))
	for _, c := range conns {
		res = append(res, plugin.Resource{Type: "connection", ID: c.ARN, Name: c.Name})
	}
	return res, nil
}

// GetMetrics returns empty metrics.
func (p *CodeConnectionsProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Connection handlers ---

func (p *CodeConnectionsProvider) createConnection(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ConnectionName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ConnectionName is required", http.StatusBadRequest), nil
	}
	providerType, _ := params["ProviderType"].(string)
	if providerType == "" {
		providerType = "GitHub"
	}
	hostARN, _ := params["HostArn"].(string)
	id := shared.GenerateID("", 36)
	arn := fmt.Sprintf("arn:aws:codeconnections:%s:%s:connection/%s", shared.DefaultRegion, defaultAccountID, id)
	conn := &Connection{
		ARN:          arn,
		Name:         name,
		ProviderType: providerType,
		OwnerAccount: defaultAccountID,
		HostARN:      hostARN,
		Status:       "AVAILABLE",
		CreatedAt:    time.Now(),
	}
	if err := p.store.CreateConnection(conn); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.PutTags(arn, parseTagList(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ConnectionArn": arn,
		"Tags":          []any{},
	})
}

func (p *CodeConnectionsProvider) getConnection(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ConnectionArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ConnectionArn is required", http.StatusBadRequest), nil
	}
	conn, err := p.store.GetConnection(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "connection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Connection": connectionToMap(conn),
	})
}

func (p *CodeConnectionsProvider) listConnections(params map[string]any) (*plugin.Response, error) {
	conns, err := p.store.ListConnections()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(conns))
	for _, c := range conns {
		items = append(items, connectionToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Connections": items,
		"NextToken":   nil,
	})
}

func (p *CodeConnectionsProvider) deleteConnection(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ConnectionArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ConnectionArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteConnection(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "connection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) updateConnection(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ConnectionArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ConnectionArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConnection(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "connection not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Host handlers ---

func (p *CodeConnectionsProvider) createHost(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	providerType, _ := params["ProviderType"].(string)
	if providerType == "" {
		providerType = "GitHubEnterpriseServer"
	}
	providerEndpoint, _ := params["ProviderEndpoint"].(string)
	vpcConfigJSON := "{}"
	if v, ok := params["VpcConfiguration"]; ok {
		b, _ := json.Marshal(v)
		vpcConfigJSON = string(b)
	}
	id := shared.GenerateID("", 36)
	arn := fmt.Sprintf("arn:aws:codeconnections:%s:%s:host/%s", shared.DefaultRegion, defaultAccountID, id)
	h := &Host{
		ARN:              arn,
		Name:             name,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           "AVAILABLE",
		VpcConfig:        vpcConfigJSON,
		AccountID:        defaultAccountID,
		CreatedAt:        time.Now(),
	}
	if err := p.store.CreateHost(h); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.PutTags(arn, parseTagList(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"HostArn": arn,
		"Tags":    []any{},
	})
}

func (p *CodeConnectionsProvider) getHost(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["HostArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "HostArn is required", http.StatusBadRequest), nil
	}
	h, err := p.store.GetHost(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "host not found", http.StatusNotFound), nil
	}
	var vpc any
	_ = json.Unmarshal([]byte(h.VpcConfig), &vpc)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Name":             h.Name,
		"Status":           h.Status,
		"ProviderType":     h.ProviderType,
		"ProviderEndpoint": h.ProviderEndpoint,
		"VpcConfiguration": vpc,
	})
}

func (p *CodeConnectionsProvider) listHosts(_ map[string]any) (*plugin.Response, error) {
	hosts, err := p.store.ListHosts()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(hosts))
	for _, h := range hosts {
		items = append(items, map[string]any{
			"HostArn":          h.ARN,
			"Name":             h.Name,
			"Status":           h.Status,
			"ProviderType":     h.ProviderType,
			"ProviderEndpoint": h.ProviderEndpoint,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Hosts":     items,
		"NextToken": nil,
	})
}

func (p *CodeConnectionsProvider) deleteHost(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["HostArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "HostArn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteHost(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "host not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) updateHost(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["HostArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "HostArn is required", http.StatusBadRequest), nil
	}
	providerEndpoint, _ := params["ProviderEndpoint"].(string)
	vpcConfigJSON := "{}"
	if v, ok := params["VpcConfiguration"]; ok {
		b, _ := json.Marshal(v)
		vpcConfigJSON = string(b)
	}
	if err := p.store.UpdateHost(arn, providerEndpoint, vpcConfigJSON); err != nil {
		return shared.JSONError("ResourceNotFoundException", "host not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Repository Link handlers ---

func (p *CodeConnectionsProvider) createRepositoryLink(params map[string]any) (*plugin.Response, error) {
	connARN, _ := params["ConnectionArn"].(string)
	ownerID, _ := params["OwnerId"].(string)
	repoName, _ := params["RepositoryName"].(string)
	if connARN == "" || ownerID == "" || repoName == "" {
		return shared.JSONError("ValidationException", "ConnectionArn, OwnerId, RepositoryName are required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	arn := fmt.Sprintf("arn:aws:codeconnections:%s:%s:repository-link/%s", shared.DefaultRegion, defaultAccountID, id)
	kmsKey, _ := params["EncryptionKeyArn"].(string)
	r := &RepositoryLink{
		ARN:              arn,
		RepoLinkID:       id,
		ConnectionARN:    connARN,
		OwnerID:          ownerID,
		RepositoryName:   repoName,
		EncryptionKeyARN: kmsKey,
		AccountID:        defaultAccountID,
		CreatedAt:        time.Now(),
	}
	if err := p.store.CreateRepoLink(r); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.PutTags(arn, parseTagList(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RepositoryLinkInfo": repoLinkToMap(r),
	})
}

func (p *CodeConnectionsProvider) getRepositoryLink(params map[string]any) (*plugin.Response, error) {
	id, _ := params["RepositoryLinkId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "RepositoryLinkId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepoLink(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository link not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RepositoryLinkInfo": repoLinkToMap(r),
	})
}

func (p *CodeConnectionsProvider) listRepositoryLinks(_ map[string]any) (*plugin.Response, error) {
	links, err := p.store.ListRepoLinks()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(links))
	for _, r := range links {
		items = append(items, repoLinkToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RepositoryLinks": items,
		"NextToken":       nil,
	})
}

func (p *CodeConnectionsProvider) deleteRepositoryLink(params map[string]any) (*plugin.Response, error) {
	id, _ := params["RepositoryLinkId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "RepositoryLinkId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRepoLink(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository link not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) updateRepositoryLink(params map[string]any) (*plugin.Response, error) {
	id, _ := params["RepositoryLinkId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "RepositoryLinkId is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetRepoLink(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "repository link not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RepositoryLinkInfo": repoLinkToMap(r),
	})
}

// --- Sync Configuration handlers ---

func (p *CodeConnectionsProvider) createSyncConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceName, _ := params["ResourceName"].(string)
	syncType, _ := params["SyncType"].(string)
	if resourceName == "" || syncType == "" {
		return shared.JSONError("ValidationException", "ResourceName and SyncType are required", http.StatusBadRequest), nil
	}
	branch, _ := params["Branch"].(string)
	if branch == "" {
		branch = "main"
	}
	configFile, _ := params["ConfigFile"].(string)
	repoLinkID, _ := params["RepositoryLinkId"].(string)
	roleARN, _ := params["RoleArn"].(string)
	publish, _ := params["PublishDeploymentStatus"].(string)
	if publish == "" {
		publish = "DISABLED"
	}
	trigger, _ := params["TriggerResourceUpdateOn"].(string)
	if trigger == "" {
		trigger = "ANY_CHANGE"
	}
	c := &SyncConfiguration{
		ResourceName:            resourceName,
		SyncType:                syncType,
		Branch:                  branch,
		ConfigFile:              configFile,
		RepoLinkID:              repoLinkID,
		RoleARN:                 roleARN,
		PublishDeploymentStatus: publish,
		TriggerResourceUpdateOn: trigger,
		AccountID:               defaultAccountID,
		CreatedAt:               time.Now(),
	}
	if err := p.store.CreateSyncConfig(c); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SyncConfiguration": syncConfigToMap(c),
	})
}

func (p *CodeConnectionsProvider) getSyncConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceName, _ := params["ResourceName"].(string)
	syncType, _ := params["SyncType"].(string)
	if resourceName == "" || syncType == "" {
		return shared.JSONError("ValidationException", "ResourceName and SyncType are required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetSyncConfig(resourceName, syncType)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "sync config not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SyncConfiguration": syncConfigToMap(c),
	})
}

func (p *CodeConnectionsProvider) listSyncConfigurations(params map[string]any) (*plugin.Response, error) {
	syncType, _ := params["SyncType"].(string)
	configs, err := p.store.ListSyncConfigs(syncType)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		items = append(items, syncConfigToMap(&c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SyncConfigurations": items,
		"NextToken":          nil,
	})
}

func (p *CodeConnectionsProvider) deleteSyncConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceName, _ := params["ResourceName"].(string)
	syncType, _ := params["SyncType"].(string)
	if resourceName == "" || syncType == "" {
		return shared.JSONError("ValidationException", "ResourceName and SyncType are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSyncConfig(resourceName, syncType); err != nil {
		return shared.JSONError("ResourceNotFoundException", "sync config not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) updateSyncConfiguration(params map[string]any) (*plugin.Response, error) {
	resourceName, _ := params["ResourceName"].(string)
	syncType, _ := params["SyncType"].(string)
	if resourceName == "" || syncType == "" {
		return shared.JSONError("ValidationException", "ResourceName and SyncType are required", http.StatusBadRequest), nil
	}
	c, err := p.store.GetSyncConfig(resourceName, syncType)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "sync config not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SyncConfiguration": syncConfigToMap(c),
	})
}

// --- Tag handlers ---

func (p *CodeConnectionsProvider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.PutTags(arn, parseTagList(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.DeleteTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *CodeConnectionsProvider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.GetTags(arn)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags": list,
	})
}

// --- helpers ---

func connectionToMap(c *Connection) map[string]any {
	return map[string]any{
		"ConnectionArn":    c.ARN,
		"ConnectionName":   c.Name,
		"ProviderType":     c.ProviderType,
		"OwnerAccountId":   c.OwnerAccount,
		"ConnectionStatus": c.Status,
		"HostArn":          c.HostARN,
	}
}

func repoLinkToMap(r *RepositoryLink) map[string]any {
	return map[string]any{
		"RepositoryLinkArn": r.ARN,
		"RepositoryLinkId":  r.RepoLinkID,
		"ConnectionArn":     r.ConnectionARN,
		"OwnerId":           r.OwnerID,
		"RepositoryName":    r.RepositoryName,
		"EncryptionKeyArn":  r.EncryptionKeyARN,
		"ProviderType":      "GitHub",
	}
}

func syncConfigToMap(c *SyncConfiguration) map[string]any {
	return map[string]any{
		"ResourceName":            c.ResourceName,
		"SyncType":                c.SyncType,
		"Branch":                  c.Branch,
		"ConfigFile":              c.ConfigFile,
		"RepositoryLinkId":        c.RepoLinkID,
		"RoleArn":                 c.RoleARN,
		"PublishDeploymentStatus": c.PublishDeploymentStatus,
		"TriggerResourceUpdateOn": c.TriggerResourceUpdateOn,
		"OwnerId":                 c.AccountID,
		"RepositoryName":          "",
		"ProviderType":            "GitHub",
	}
}

func parseTagList(raw []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range raw {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		k, _ := tm["Key"].(string)
		v, _ := tm["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func getString(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}

func init() {
	plugin.DefaultRegistry.Register("codeconnections", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &CodeConnectionsProvider{}
	})
}
