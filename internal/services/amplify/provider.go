// SPDX-License-Identifier: Apache-2.0

// internal/services/amplify/provider.go
package amplify

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

// Provider implements the Amplify service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "amplify" }
func (p *Provider) ServiceName() string           { return "Amplify" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "amplify"))
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
	// Apps
	case "CreateApp":
		return p.createApp(params)
	case "GetApp":
		appID := extractPathParam(path, "apps")
		return p.getApp(appID)
	case "ListApps":
		return p.listApps()
	case "UpdateApp":
		appID := extractPathParam(path, "apps")
		return p.updateApp(appID, params)
	case "DeleteApp":
		appID := extractPathParam(path, "apps")
		return p.deleteApp(appID)

	// Branches
	case "CreateBranch":
		appID := extractPathParam(path, "apps")
		return p.createBranch(appID, params)
	case "GetBranch":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		return p.getBranch(appID, branchName)
	case "ListBranches":
		appID := extractPathParam(path, "apps")
		return p.listBranches(appID)
	case "UpdateBranch":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		return p.updateBranch(appID, branchName, params)
	case "DeleteBranch":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		return p.deleteBranch(appID, branchName)

	// Domain Associations
	case "CreateDomainAssociation":
		appID := extractPathParam(path, "apps")
		return p.createDomainAssociation(appID, params)
	case "GetDomainAssociation":
		appID := extractPathParam(path, "apps")
		domainName := extractPathParam(path, "domains")
		return p.getDomainAssociation(appID, domainName)
	case "ListDomainAssociations":
		appID := extractPathParam(path, "apps")
		return p.listDomainAssociations(appID)
	case "UpdateDomainAssociation":
		appID := extractPathParam(path, "apps")
		domainName := extractPathParam(path, "domains")
		return p.updateDomainAssociation(appID, domainName, params)
	case "DeleteDomainAssociation":
		appID := extractPathParam(path, "apps")
		domainName := extractPathParam(path, "domains")
		return p.deleteDomainAssociation(appID, domainName)

	// Webhooks
	case "CreateWebhook":
		appID := extractPathParam(path, "apps")
		return p.createWebhook(appID, params)
	case "GetWebhook":
		webhookID := extractPathParam(path, "webhooks")
		return p.getWebhook(webhookID)
	case "ListWebhooks":
		appID := extractPathParam(path, "apps")
		return p.listWebhooks(appID)
	case "UpdateWebhook":
		webhookID := extractPathParam(path, "webhooks")
		return p.updateWebhook(webhookID, params)
	case "DeleteWebhook":
		webhookID := extractPathParam(path, "webhooks")
		return p.deleteWebhook(webhookID)

	// Backend Environments
	case "CreateBackendEnvironment":
		appID := extractPathParam(path, "apps")
		return p.createBackendEnvironment(appID, params)
	case "GetBackendEnvironment":
		appID := extractPathParam(path, "apps")
		envName := extractPathParam(path, "backendenvironments")
		return p.getBackendEnvironment(appID, envName)
	case "ListBackendEnvironments":
		appID := extractPathParam(path, "apps")
		return p.listBackendEnvironments(appID)
	case "DeleteBackendEnvironment":
		appID := extractPathParam(path, "apps")
		envName := extractPathParam(path, "backendenvironments")
		return p.deleteBackendEnvironment(appID, envName)

	// Jobs
	case "StartJob":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		return p.startJob(appID, branchName, params)
	case "GetJob":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		jobID := extractPathParam(path, "jobs")
		return p.getJob(appID, branchName, jobID)
	case "ListJobs":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		return p.listJobs(appID, branchName)
	case "StopJob":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		jobID := extractPathParam(path, "jobs")
		return p.stopJob(appID, branchName, jobID)
	case "DeleteJob":
		appID := extractPathParam(path, "apps")
		branchName := extractPathParam(path, "branches")
		jobID := extractPathParam(path, "jobs")
		return p.deleteJob(appID, branchName, jobID)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Misc stubs
	case "CreateDeployment":
		jobID := shared.GenerateID("", 8)
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobId":          jobID,
			"zipUploadUrl":   "https://s3.amazonaws.com/amplify-uploads/" + jobID,
			"fileUploadUrls": map[string]any{},
		})
	case "StartDeployment":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobSummary": buildJobSummaryMap(&Job{
				ID:     shared.GenerateID("", 8),
				Status: "SUCCEED",
			}),
		})
	case "GenerateAccessLogs":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"logUrl": "https://s3.amazonaws.com/amplify-logs/" + shared.GenerateID("", 16),
		})
	case "GetArtifactUrl":
		artifactID := extractPathParam(path, "artifacts")
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"artifactId":  artifactID,
			"artifactUrl": "https://s3.amazonaws.com/amplify-artifacts/" + artifactID,
		})
	case "ListArtifacts":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"artifacts": []any{},
		})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// Webhooks: /webhooks/{id}
	case n >= 1 && seg[0] == "webhooks":
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetWebhook"
			case http.MethodPost:
				return "UpdateWebhook"
			case http.MethodDelete:
				return "DeleteWebhook"
			}
		}

	// Apps: /apps, /apps/{id}, /apps/{id}/...
	case n >= 1 && seg[0] == "apps":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateApp"
			case http.MethodGet:
				return "ListApps"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetApp"
			case http.MethodPost:
				return "UpdateApp"
			case http.MethodDelete:
				return "DeleteApp"
			}
		}
		// /apps/{id}/branches
		if n >= 3 && seg[2] == "branches" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateBranch"
				case http.MethodGet:
					return "ListBranches"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetBranch"
				case http.MethodPost:
					return "UpdateBranch"
				case http.MethodDelete:
					return "DeleteBranch"
				}
			}
			// /apps/{id}/branches/{name}/jobs
			if n >= 5 && seg[4] == "jobs" {
				if n == 5 {
					switch method {
					case http.MethodPost:
						return "StartJob"
					case http.MethodGet:
						return "ListJobs"
					}
				}
				if n == 6 {
					switch method {
					case http.MethodGet:
						return "GetJob"
					case http.MethodDelete:
						return "DeleteJob"
					}
				}
				// /apps/{id}/branches/{name}/jobs/{id}/stop
				if n == 7 && seg[6] == "stop" && method == http.MethodDelete {
					return "StopJob"
				}
			}
		}
		// /apps/{id}/domains
		if n >= 3 && seg[2] == "domains" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateDomainAssociation"
				case http.MethodGet:
					return "ListDomainAssociations"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetDomainAssociation"
				case http.MethodPost:
					return "UpdateDomainAssociation"
				case http.MethodDelete:
					return "DeleteDomainAssociation"
				}
			}
		}
		// /apps/{id}/webhooks
		if n >= 3 && seg[2] == "webhooks" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateWebhook"
				case http.MethodGet:
					return "ListWebhooks"
				}
			}
		}
		// /apps/{id}/backendenvironments
		if n >= 3 && seg[2] == "backendenvironments" {
			if n == 3 {
				switch method {
				case http.MethodPost:
					return "CreateBackendEnvironment"
				case http.MethodGet:
					return "ListBackendEnvironments"
				}
			}
			if n == 4 {
				switch method {
				case http.MethodGet:
					return "GetBackendEnvironment"
				case http.MethodDelete:
					return "DeleteBackendEnvironment"
				}
			}
		}
		// /apps/{id}/deployments
		if n >= 3 && seg[2] == "deployments" {
			if n == 3 && method == http.MethodPost {
				return "CreateDeployment"
			}
			if n == 4 && seg[3] == "start" && method == http.MethodPost {
				return "StartDeployment"
			}
		}
		// /apps/{id}/accesslogs
		if n >= 3 && seg[2] == "accesslogs" && method == http.MethodPost {
			return "GenerateAccessLogs"
		}
		// /apps/{id}/artifacts/{id}
		if n >= 3 && seg[2] == "artifacts" {
			if n == 4 && method == http.MethodGet {
				return "GetArtifactUrl"
			}
			if n == 3 && method == http.MethodGet {
				return "ListArtifacts"
			}
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApps()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		res = append(res, plugin.Resource{Type: "amplify-app", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- App CRUD ---

func (p *Provider) createApp(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "app name is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("d", 10)
	arn := shared.BuildARN("amplify", "apps", id)
	defaultDomain := fmt.Sprintf("%s.amplifyapp.com", id)

	a := &App{
		ID:            id,
		ARN:           arn,
		Name:          name,
		Description:   strParam(params, "description"),
		Repository:    strParam(params, "repository"),
		Platform:      strParamDefault(params, "platform", "WEB"),
		IAMRole:       strParam(params, "iamServiceRoleArn"),
		DefaultDomain: defaultDomain,
	}

	if err := p.store.CreateApp(a); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "app already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		p.store.tags.AddTags(arn, toStringMap(rawTags))
	}

	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{"app": appToMap(a, tags)})
}

func (p *Provider) getApp(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetApp(appID)
	if err != nil {
		return shared.JSONError("NotFoundException", "app not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"app": appToMap(a, tags)})
}

func (p *Provider) listApps() (*plugin.Response, error) {
	apps, err := p.store.ListApps()
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(apps))
	for i := range apps {
		tags, _ := p.store.tags.ListTags(apps[i].ARN)
		result = append(result, appToMap(&apps[i], tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"apps": result})
}

func (p *Provider) updateApp(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateApp(appID, params); err != nil {
		return shared.JSONError("NotFoundException", "app not found", http.StatusNotFound), nil
	}
	a, _ := p.store.GetApp(appID)
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{"app": appToMap(a, tags)})
}

func (p *Provider) deleteApp(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.DeleteApp(appID)
	if err != nil {
		return shared.JSONError("NotFoundException", "app not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(a.ARN)
	tags := map[string]string{}
	return shared.JSONResponse(http.StatusOK, map[string]any{"app": appToMap(a, tags)})
}

// --- Branch CRUD ---

func (p *Provider) createBranch(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	branchName, _ := params["branchName"].(string)
	if branchName == "" {
		return shared.JSONError("ValidationException", "branchName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("amplify", fmt.Sprintf("apps/%s/branches", appID), branchName)
	b := &Branch{
		AppID:           appID,
		Name:            branchName,
		ARN:             arn,
		DisplayName:     strParamDefault(params, "displayName", branchName),
		Description:     strParam(params, "description"),
		Stage:           strParamDefault(params, "stage", "NONE"),
		Framework:       strParam(params, "framework"),
		EnableAutoBuild: boolParamDefault(params, "enableAutoBuild", true),
	}
	if err := p.store.CreateBranch(b); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "branch already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"branch": branchToMap(b)})
}

func (p *Provider) getBranch(appID, branchName string) (*plugin.Response, error) {
	if appID == "" || branchName == "" {
		return shared.JSONError("ValidationException", "appId and branchName are required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBranch(appID, branchName)
	if err != nil {
		return shared.JSONError("NotFoundException", "branch not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"branch": branchToMap(b)})
}

func (p *Provider) listBranches(appID string) (*plugin.Response, error) {
	branches, err := p.store.ListBranches(appID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(branches))
	for i := range branches {
		result = append(result, branchToMap(&branches[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"branches": result})
}

func (p *Provider) updateBranch(appID, branchName string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || branchName == "" {
		return shared.JSONError("ValidationException", "appId and branchName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateBranch(appID, branchName, params); err != nil {
		return shared.JSONError("NotFoundException", "branch not found", http.StatusNotFound), nil
	}
	b, _ := p.store.GetBranch(appID, branchName)
	return shared.JSONResponse(http.StatusOK, map[string]any{"branch": branchToMap(b)})
}

func (p *Provider) deleteBranch(appID, branchName string) (*plugin.Response, error) {
	if appID == "" || branchName == "" {
		return shared.JSONError("ValidationException", "appId and branchName are required", http.StatusBadRequest), nil
	}
	b, err := p.store.DeleteBranch(appID, branchName)
	if err != nil {
		return shared.JSONError("NotFoundException", "branch not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"branch": branchToMap(b)})
}

// --- Domain Association CRUD ---

func (p *Provider) createDomainAssociation(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	domainName, _ := params["domainName"].(string)
	if domainName == "" {
		return shared.JSONError("ValidationException", "domainName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("amplify", fmt.Sprintf("apps/%s/domains", appID), domainName)
	subDomainsJSON := "[]"
	if v, ok := params["subDomainSettings"]; ok {
		b, _ := json.Marshal(v)
		subDomainsJSON = string(b)
	}
	d := &DomainAssociation{
		AppID:      appID,
		DomainName: domainName,
		ARN:        arn,
		Status:     "AVAILABLE",
		SubDomains: subDomainsJSON,
	}
	if err := p.store.CreateDomainAssociation(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "domain association already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"domainAssociation": domainAssociationToMap(d)})
}

func (p *Provider) getDomainAssociation(appID, domainName string) (*plugin.Response, error) {
	if appID == "" || domainName == "" {
		return shared.JSONError("ValidationException", "appId and domainName are required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDomainAssociation(appID, domainName)
	if err != nil {
		return shared.JSONError("NotFoundException", "domain association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"domainAssociation": domainAssociationToMap(d)})
}

func (p *Provider) listDomainAssociations(appID string) (*plugin.Response, error) {
	domains, err := p.store.ListDomainAssociations(appID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(domains))
	for i := range domains {
		result = append(result, domainAssociationToMap(&domains[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"domainAssociations": result})
}

func (p *Provider) updateDomainAssociation(appID, domainName string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || domainName == "" {
		return shared.JSONError("ValidationException", "appId and domainName are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDomainAssociation(appID, domainName, params); err != nil {
		return shared.JSONError("NotFoundException", "domain association not found", http.StatusNotFound), nil
	}
	d, _ := p.store.GetDomainAssociation(appID, domainName)
	return shared.JSONResponse(http.StatusOK, map[string]any{"domainAssociation": domainAssociationToMap(d)})
}

func (p *Provider) deleteDomainAssociation(appID, domainName string) (*plugin.Response, error) {
	if appID == "" || domainName == "" {
		return shared.JSONError("ValidationException", "appId and domainName are required", http.StatusBadRequest), nil
	}
	d, err := p.store.DeleteDomainAssociation(appID, domainName)
	if err != nil {
		return shared.JSONError("NotFoundException", "domain association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"domainAssociation": domainAssociationToMap(d)})
}

// --- Webhook CRUD ---

func (p *Provider) createWebhook(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	id := shared.GenerateUUID()
	arn := shared.BuildARN("amplify", "webhooks", id)
	w := &Webhook{
		ID:         id,
		ARN:        arn,
		AppID:      appID,
		BranchName: strParam(params, "branchName"),
		URL:        fmt.Sprintf("https://webhooks.amplify.us-east-1.io/%s", id),
	}
	if err := p.store.CreateWebhook(w); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"webhook": webhookToMap(w)})
}

func (p *Provider) getWebhook(webhookID string) (*plugin.Response, error) {
	if webhookID == "" {
		return shared.JSONError("ValidationException", "webhookId is required", http.StatusBadRequest), nil
	}
	w, err := p.store.GetWebhook(webhookID)
	if err != nil {
		return shared.JSONError("NotFoundException", "webhook not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"webhook": webhookToMap(w)})
}

func (p *Provider) listWebhooks(appID string) (*plugin.Response, error) {
	webhooks, err := p.store.ListWebhooks(appID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(webhooks))
	for i := range webhooks {
		result = append(result, webhookToMap(&webhooks[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"webhooks": result})
}

func (p *Provider) updateWebhook(webhookID string, params map[string]any) (*plugin.Response, error) {
	if webhookID == "" {
		return shared.JSONError("ValidationException", "webhookId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateWebhook(webhookID, params); err != nil {
		return shared.JSONError("NotFoundException", "webhook not found", http.StatusNotFound), nil
	}
	w, _ := p.store.GetWebhook(webhookID)
	return shared.JSONResponse(http.StatusOK, map[string]any{"webhook": webhookToMap(w)})
}

func (p *Provider) deleteWebhook(webhookID string) (*plugin.Response, error) {
	if webhookID == "" {
		return shared.JSONError("ValidationException", "webhookId is required", http.StatusBadRequest), nil
	}
	w, err := p.store.DeleteWebhook(webhookID)
	if err != nil {
		return shared.JSONError("NotFoundException", "webhook not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"webhook": webhookToMap(w)})
}

// --- Backend Environment CRUD ---

func (p *Provider) createBackendEnvironment(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("ValidationException", "appId is required", http.StatusBadRequest), nil
	}
	envName, _ := params["environmentName"].(string)
	if envName == "" {
		return shared.JSONError("ValidationException", "environmentName is required", http.StatusBadRequest), nil
	}
	arn := shared.BuildARN("amplify", fmt.Sprintf("apps/%s/backendenvironments", appID), envName)
	be := &BackendEnvironment{
		AppID:     appID,
		Name:      envName,
		ARN:       arn,
		StackName: strParam(params, "stackName"),
	}
	if err := p.store.CreateBackendEnvironment(be); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "backend environment already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"backendEnvironment": backendEnvToMap(be)})
}

func (p *Provider) getBackendEnvironment(appID, envName string) (*plugin.Response, error) {
	if appID == "" || envName == "" {
		return shared.JSONError("ValidationException", "appId and environmentName are required", http.StatusBadRequest), nil
	}
	be, err := p.store.GetBackendEnvironment(appID, envName)
	if err != nil {
		return shared.JSONError("NotFoundException", "backend environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"backendEnvironment": backendEnvToMap(be)})
}

func (p *Provider) listBackendEnvironments(appID string) (*plugin.Response, error) {
	envs, err := p.store.ListBackendEnvironments(appID)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(envs))
	for i := range envs {
		result = append(result, backendEnvToMap(&envs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"backendEnvironments": result})
}

func (p *Provider) deleteBackendEnvironment(appID, envName string) (*plugin.Response, error) {
	if appID == "" || envName == "" {
		return shared.JSONError("ValidationException", "appId and environmentName are required", http.StatusBadRequest), nil
	}
	be, err := p.store.DeleteBackendEnvironment(appID, envName)
	if err != nil {
		return shared.JSONError("NotFoundException", "backend environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"backendEnvironment": backendEnvToMap(be)})
}

// --- Jobs ---

func (p *Provider) startJob(appID, branchName string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || branchName == "" {
		return shared.JSONError("ValidationException", "appId and branchName are required", http.StatusBadRequest), nil
	}
	id := shared.GenerateID("", 8)
	arn := shared.BuildARN("amplify", fmt.Sprintf("apps/%s/branches/%s/jobs", appID, branchName), id)
	j := &Job{
		ID:         id,
		AppID:      appID,
		BranchName: branchName,
		ARN:        arn,
		JobType:    strParamDefault(params, "jobType", "RELEASE"),
		Status:     "SUCCEED",
		CommitID:   strParam(params, "commitId"),
		CommitMsg:  strParam(params, "commitMessage"),
	}
	if err := p.store.CreateJob(j); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobSummary": buildJobSummaryMap(j)})
}

func (p *Provider) getJob(appID, branchName, jobID string) (*plugin.Response, error) {
	if appID == "" || branchName == "" || jobID == "" {
		return shared.JSONError("ValidationException", "appId, branchName and jobId are required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetJob(appID, branchName, jobID)
	if err != nil {
		return shared.JSONError("NotFoundException", "job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"job": map[string]any{
			"summary": buildJobSummaryMap(j),
			"steps":   []any{},
		},
	})
}

func (p *Provider) listJobs(appID, branchName string) (*plugin.Response, error) {
	jobs, err := p.store.ListJobs(appID, branchName)
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(jobs))
	for i := range jobs {
		result = append(result, buildJobSummaryMap(&jobs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobSummaries": result})
}

func (p *Provider) stopJob(appID, branchName, jobID string) (*plugin.Response, error) {
	if appID == "" || branchName == "" || jobID == "" {
		return shared.JSONError("ValidationException", "appId, branchName and jobId are required", http.StatusBadRequest), nil
	}
	j, err := p.store.GetJob(appID, branchName, jobID)
	if err != nil {
		return shared.JSONError("NotFoundException", "job not found", http.StatusNotFound), nil
	}
	p.store.UpdateJobStatus(appID, branchName, jobID, "CANCELLED")
	j.Status = "CANCELLED"
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobSummary": buildJobSummaryMap(j)})
}

func (p *Provider) deleteJob(appID, branchName, jobID string) (*plugin.Response, error) {
	if appID == "" || branchName == "" || jobID == "" {
		return shared.JSONError("ValidationException", "appId, branchName and jobId are required", http.StatusBadRequest), nil
	}
	j, err := p.store.DeleteJob(appID, branchName, jobID)
	if err != nil {
		return shared.JSONError("NotFoundException", "job not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"jobSummary": buildJobSummaryMap(j)})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// --- Map builders ---

func appToMap(a *App, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"appId":                a.ID,
		"appArn":               a.ARN,
		"name":                 a.Name,
		"description":          a.Description,
		"repository":           a.Repository,
		"platform":             a.Platform,
		"iamServiceRoleArn":    a.IAMRole,
		"defaultDomain":        a.DefaultDomain,
		"createTime":           a.CreatedAt.Format(time.RFC3339),
		"updateTime":           a.UpdatedAt.Format(time.RFC3339),
		"tags":                 tags,
		"environmentVariables": map[string]string{},
	}
}

func branchToMap(b *Branch) map[string]any {
	return map[string]any{
		"branchArn":            b.ARN,
		"branchName":           b.Name,
		"displayName":          b.DisplayName,
		"description":          b.Description,
		"stage":                b.Stage,
		"framework":            b.Framework,
		"enableAutoBuild":      b.EnableAutoBuild,
		"createTime":           b.CreatedAt.Format(time.RFC3339),
		"updateTime":           b.UpdatedAt.Format(time.RFC3339),
		"activeJobId":          "",
		"totalNumberOfJobs":    "0",
		"ttl":                  "5",
		"environmentVariables": map[string]string{},
	}
}

func domainAssociationToMap(d *DomainAssociation) map[string]any {
	var subDomains any
	json.Unmarshal([]byte(d.SubDomains), &subDomains)
	if subDomains == nil {
		subDomains = []any{}
	}
	return map[string]any{
		"domainAssociationArn": d.ARN,
		"domainName":           d.DomainName,
		"domainStatus":         d.Status,
		"subDomains":           subDomains,
		"enableAutoSubDomain":  false,
	}
}

func webhookToMap(w *Webhook) map[string]any {
	return map[string]any{
		"webhookId":  w.ID,
		"webhookArn": w.ARN,
		"appId":      w.AppID,
		"branchName": w.BranchName,
		"webhookUrl": w.URL,
		"createTime": w.CreatedAt.Format(time.RFC3339),
		"updateTime": w.CreatedAt.Format(time.RFC3339),
	}
}

func backendEnvToMap(be *BackendEnvironment) map[string]any {
	return map[string]any{
		"backendEnvironmentArn": be.ARN,
		"environmentName":       be.Name,
		"stackName":             be.StackName,
		"createTime":            be.CreatedAt.Format(time.RFC3339),
		"updateTime":            be.CreatedAt.Format(time.RFC3339),
	}
}

func buildJobSummaryMap(j *Job) map[string]any {
	return map[string]any{
		"jobArn":        j.ARN,
		"jobId":         j.ID,
		"jobType":       j.JobType,
		"status":        j.Status,
		"commitId":      j.CommitID,
		"commitMessage": j.CommitMsg,
		"startTime":     j.CreatedAt.Format(time.RFC3339),
		"endTime":       j.UpdatedAt.Format(time.RFC3339),
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
