// SPDX-License-Identifier: Apache-2.0

// Package serverlessrepo implements AWS Serverless Application Repository.
package serverlessrepo

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

// ServerlessRepoProvider implements plugin.ServicePlugin for Serverless Application Repository.
type ServerlessRepoProvider struct {
	store *Store
}

func (p *ServerlessRepoProvider) ServiceID() string             { return "serverlessrepo" }
func (p *ServerlessRepoProvider) ServiceName() string           { return "ServerlessApplicationRepository" }
func (p *ServerlessRepoProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *ServerlessRepoProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "serverlessrepo"))
	return err
}

func (p *ServerlessRepoProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *ServerlessRepoProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
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
	case "CreateApplication":
		return p.createApplication(params)
	case "GetApplication":
		return p.getApplication(req)
	case "UpdateApplication":
		return p.updateApplication(req, params)
	case "DeleteApplication":
		return p.deleteApplication(req)
	case "ListApplications":
		return p.listApplications()
	case "CreateApplicationVersion":
		return p.createApplicationVersion(req, params)
	case "ListApplicationVersions":
		return p.listApplicationVersions(req)
	case "PutApplicationPolicy":
		return p.putApplicationPolicy(req, params)
	case "GetApplicationPolicy":
		return p.getApplicationPolicy(req)
	// Extended: CloudFormation change sets & templates
	case "CreateCloudFormationChangeSet":
		return p.createCloudFormationChangeSet(req, params)
	case "CreateCloudFormationTemplate":
		return p.createCloudFormationTemplate(req, params)
	case "GetCloudFormationTemplate":
		return p.getCloudFormationTemplate(req)
	// Extended: Unshare & dependencies
	case "UnshareApplication":
		return p.unshareApplication(req, params)
	case "ListApplicationDependencies":
		return p.listApplicationDependencies(req)
	// Extended: Version management
	case "GetApplicationVersion":
		return p.getApplicationVersion(req)
	case "DeleteApplicationVersion":
		return p.deleteApplicationVersion(req)
	case "UpdateApplicationVersion":
		return p.updateApplicationVersion(req, params)
	// Extended: Policy management
	case "DeleteApplicationPolicy":
		return p.deleteApplicationPolicy(req)
	// Extended: Version search / misc
	case "SearchApplications":
		return p.listApplications()
	case "GetApplicationPublishStatus":
		return p.getApplicationPublishStatus(req)
	case "PublishApplication":
		return p.publishApplication(req, params)
	case "UnpublishApplication":
		return p.unpublishApplication(req)
	// Tagging
	case "TagResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UntagResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListTagsForResource":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": []any{}})
	// Review / ratings / categories
	case "ListApplicationReviews":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Reviews": []any{}})
	case "SubmitApplicationReview":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListCategories":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Categories": []map[string]any{
				{"Name": "Data Processing"}, {"Name": "Machine Learning"},
			},
		})
	// Statistics / meta
	case "GetApplicationStatistics":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Statistics": map[string]any{"Deployments": 0},
		})
	case "ListApplicationDeployments":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Deployments": []any{}})
	// Internal/share management
	case "ShareApplication":
		return p.shareApplication(req, params)
	case "ListApplicationShares":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Shares": []any{}})
	case "GetApplicationShares":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Shares": []any{}})
	// Change set management
	case "ListCloudFormationChangeSets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ChangeSets": []any{}})
	case "DeleteCloudFormationChangeSet":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListCloudFormationTemplates":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Templates": []any{}})
	case "GetCloudFormationChangeSet":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ChangeSetId": "cs-1",
			"Status":      "CREATE_COMPLETE",
		})
	case "ExecuteCloudFormationChangeSet":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CancelCloudFormationChangeSet":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "AddApplicationDependency":
		appID := extractAppID(req.URL.Path)
		depID, _ := params["DependentApplicationId"].(string)
		depVer, _ := params["DependentSemanticVersion"].(string)
		p.store.AddDependency(appID, depID, depVer) //nolint:errcheck
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RemoveApplicationDependency":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("NotFoundException", fmt.Sprintf("unknown operation: %s", op), http.StatusNotFound), nil
	}
}

func (p *ServerlessRepoProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		arn := appARN(a.ApplicationID)
		res = append(res, plugin.Resource{Type: "serverlessrepo-application", ID: arn, Name: a.Name})
	}
	return res, nil
}

func (p *ServerlessRepoProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Application handlers ---

func (p *ServerlessRepoProvider) createApplication(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name", "name")
	if name == "" {
		return shared.JSONError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description", "description")
	author := strParam(params, "Author", "author")
	homePageURL := strParam(params, "HomePageUrl", "homePageUrl")
	readmeURL := strParam(params, "ReadmeUrl", "readmeUrl")
	readmeBody := strParam(params, "ReadmeBody", "readmeBody")
	if readmeURL == "" && readmeBody != "" {
		readmeURL = ""
	}
	licenseURL := strParam(params, "LicenseUrl", "licenseUrl")

	labelsJSON := "[]"
	for _, k := range []string{"Labels", "labels"} {
		if labels, ok := params[k].([]any); ok {
			b, _ := json.Marshal(labels)
			labelsJSON = string(b)
			break
		}
	}

	appID := shared.GenerateUUID()
	app, err := p.store.CreateApplication(appID, name, description, author, homePageURL, readmeURL, licenseURL, labelsJSON)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "application already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	// If a semantic version is provided, also create a version
	if semVer := strParam(params, "SemanticVersion", "semanticVersion"); semVer != "" {
		templateURL := strParam(params, "TemplateUrl", "templateUrl")
		p.store.CreateVersion(appID, semVer, templateURL) //nolint:errcheck
	}

	return shared.JSONResponse(http.StatusCreated, applicationToMap(app, ""))
}

func (p *ServerlessRepoProvider) getApplication(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(appID)
	if err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	// Get latest version
	versions, _ := p.store.ListVersions(appID)
	latestVer := ""
	if len(versions) > 0 {
		latestVer = versions[len(versions)-1].SemanticVersion
	}
	return shared.JSONResponse(http.StatusOK, applicationToMap(app, latestVer))
}

func (p *ServerlessRepoProvider) updateApplication(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(appID)
	if err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}

	description := app.Description
	if d := strParam(params, "Description", "description"); d != "" {
		description = d
	}
	author := app.Author
	if a := strParam(params, "Author", "author"); a != "" {
		author = a
	}
	homePageURL := app.HomePageURL
	if h := strParam(params, "HomePageUrl", "homePageUrl"); h != "" {
		homePageURL = h
	}
	readmeURL := app.ReadmeURL
	if r := strParam(params, "ReadmeUrl", "readmeUrl"); r != "" {
		readmeURL = r
	}

	if err := p.store.UpdateApplication(appID, description, author, homePageURL, readmeURL); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	app.Description = description
	app.Author = author
	app.HomePageURL = homePageURL
	app.ReadmeURL = readmeURL
	return shared.JSONResponse(http.StatusOK, applicationToMap(app, ""))
}

func (p *ServerlessRepoProvider) deleteApplication(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *ServerlessRepoProvider) listApplications() (*plugin.Response, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		list = append(list, map[string]any{
			"applicationId": a.ApplicationID,
			"ApplicationId": a.ApplicationID,
			"name":          a.Name,
			"Name":          a.Name,
			"description":   a.Description,
			"Description":   a.Description,
			"author":        a.Author,
			"Author":        a.Author,
			"creationTime":  a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			"CreationTime":  a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applications": list,
		"Applications": list,
		"nextToken":    nil, "NextToken": nil,
	})
}

// --- Version handlers ---

func (p *ServerlessRepoProvider) createApplicationVersion(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	semVer := extractSemanticVersion(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if semVer == "" {
		semVer, _ = params["SemanticVersion"].(string)
	}
	if semVer == "" {
		return shared.JSONError("BadRequestException", "SemanticVersion is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	templateURL, _ := params["TemplateUrl"].(string)
	version, err := p.store.CreateVersion(appID, semVer, templateURL) //nolint:errcheck
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "version already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"applicationId":   version.ApplicationID,
		"ApplicationId":   version.ApplicationID,
		"semanticVersion": version.SemanticVersion,
		"SemanticVersion": version.SemanticVersion,
		"templateUrl":     version.TemplateURL,
		"TemplateUrl":     version.TemplateURL,
		"creationTime":    version.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"CreationTime":    version.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

func (p *ServerlessRepoProvider) listApplicationVersions(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	versions, err := p.store.ListVersions(appID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		list = append(list, map[string]any{
			"semanticVersion": v.SemanticVersion,
			"SemanticVersion": v.SemanticVersion,
			"applicationId":   v.ApplicationID,
			"ApplicationId":   v.ApplicationID,
			"creationTime":    v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			"CreationTime":    v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"versions":  list,
		"Versions":  list,
		"nextToken": nil,
		"NextToken": nil,
	})
}

// --- Policy handlers ---

func (p *ServerlessRepoProvider) putApplicationPolicy(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	statementsJSON := "[]"
	if stmts, ok := params["Statements"]; ok {
		b, _ := json.Marshal(stmts)
		statementsJSON = string(b)
	}
	if err := p.store.PutPolicy(appID, statementsJSON); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Statements": json.RawMessage(statementsJSON),
	})
}

func (p *ServerlessRepoProvider) getApplicationPolicy(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	statementsJSON, err := p.store.GetPolicy(appID)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Statements": json.RawMessage(statementsJSON),
	})
}

// --- Extended handlers ---

func (p *ServerlessRepoProvider) createCloudFormationChangeSet(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	stackName, _ := params["StackName"].(string)
	if stackName == "" {
		stackName = "devcloud-stack"
	}
	semVer, _ := params["SemanticVersion"].(string)
	templateURL, _ := params["TemplateUrl"].(string)
	cs, err := p.store.CreateChangeSet(appID, stackName, semVer, templateURL)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"ApplicationId": appID, "applicationId": appID,
		"ChangeSetId": cs.ChangeSetID, "changeSetId": cs.ChangeSetID,
		"StackId":         "arn:aws:cloudformation:us-east-1:000000000000:stack/" + stackName,
		"SemanticVersion": cs.SemanticVersion, "semanticVersion": cs.SemanticVersion,
	})
}

func (p *ServerlessRepoProvider) createCloudFormationTemplate(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("NotFoundException", "application not found", http.StatusNotFound), nil
	}
	semVer, _ := params["SemanticVersion"].(string)
	templateURL, _ := params["TemplateUrl"].(string)
	tmpl, err := p.store.CreateTemplate(appID, semVer, templateURL)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"ApplicationId": appID, "applicationId": appID,
		"TemplateId": tmpl.TemplateID, "templateId": tmpl.TemplateID,
		"SemanticVersion": tmpl.SemanticVersion, "semanticVersion": tmpl.SemanticVersion,
		"TemplateUrl": tmpl.TemplateURL, "templateUrl": tmpl.TemplateURL,
		"Status": tmpl.Status, "status": tmpl.Status,
	})
}

func (p *ServerlessRepoProvider) getCloudFormationTemplate(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	templateID := extractTemplateID(req.URL.Path)
	if appID == "" || templateID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId and TemplateId are required", http.StatusBadRequest), nil
	}
	tmpl, err := p.store.GetTemplate(templateID)
	if err != nil {
		return shared.JSONError("NotFoundException", "template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationId": tmpl.ApplicationID, "applicationId": tmpl.ApplicationID,
		"TemplateId": tmpl.TemplateID, "templateId": tmpl.TemplateID,
		"SemanticVersion": tmpl.SemanticVersion, "semanticVersion": tmpl.SemanticVersion,
		"TemplateUrl": tmpl.TemplateURL, "templateUrl": tmpl.TemplateURL,
		"Status": tmpl.Status, "status": tmpl.Status,
	})
}

func (p *ServerlessRepoProvider) unshareApplication(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	principal := strParam(params, "OrganizationId", "organizationId")
	if principal == "" {
		principal, _ = params["Principal"].(string)
	}
	if principal == "" {
		return shared.JSONError("BadRequestException", "Principal is required", http.StatusBadRequest), nil
	}
	if err := p.store.AddUnshared(appID, principal); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *ServerlessRepoProvider) listApplicationDependencies(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	deps, err := p.store.ListDependencies(appID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		list = append(list, map[string]any{
			"ApplicationId":   d["ApplicationId"],
			"SemanticVersion": d["SemanticVersion"],
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"dependencies": list, "Dependencies": list,
		"nextToken": nil, "NextToken": nil,
	})
}

func (p *ServerlessRepoProvider) getApplicationVersion(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	semVer := extractSemanticVersion(req.URL.Path)
	if appID == "" || semVer == "" {
		return shared.JSONError("BadRequestException", "ApplicationId and SemanticVersion are required", http.StatusBadRequest), nil
	}
	versions, err := p.store.ListVersions(appID)
	if err != nil {
		return nil, err
	}
	for _, v := range versions {
		if v.SemanticVersion == semVer {
			return shared.JSONResponse(http.StatusOK, map[string]any{
				"ApplicationId":   v.ApplicationID,
				"SemanticVersion": v.SemanticVersion,
				"TemplateUrl":     v.TemplateURL,
				"CreationTime":    v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
			})
		}
	}
	return shared.JSONError("NotFoundException", "version not found", http.StatusNotFound), nil
}

func (p *ServerlessRepoProvider) deleteApplicationVersion(req *http.Request) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusNoContent, nil)
}

func (p *ServerlessRepoProvider) updateApplicationVersion(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	semVer := extractSemanticVersion(req.URL.Path)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationId":   appID,
		"SemanticVersion": semVer,
	})
}

func (p *ServerlessRepoProvider) deleteApplicationPolicy(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	if appID == "" {
		return shared.JSONError("BadRequestException", "ApplicationId is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutPolicy(appID, "[]"); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *ServerlessRepoProvider) getApplicationPublishStatus(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationId": appID,
		"Status":        "PUBLISHED",
	})
}

func (p *ServerlessRepoProvider) publishApplication(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationId": appID,
		"Status":        "PUBLISHED",
	})
}

func (p *ServerlessRepoProvider) unpublishApplication(req *http.Request) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationId": appID,
		"Status":        "DRAFT",
	})
}

func (p *ServerlessRepoProvider) shareApplication(req *http.Request, params map[string]any) (*plugin.Response, error) {
	appID := extractAppID(req.URL.Path)
	return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationId": appID, "Status": "SHARED"})
}

func extractTemplateID(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "templates" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// --- helpers ---

// strParam returns the first non-empty string value among the given keys.
// ServerlessRepo uses restJson1 with lowercase jsonName, but older callers
// may send PascalCase. Accept both.
func strParam(params map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := params[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func resolveOp(method, path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	n := len(parts)

	switch {
	// /applications
	case n >= 1 && parts[0] == "applications":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateApplication"
			case http.MethodGet:
				return "ListApplications"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetApplication"
			case http.MethodPatch:
				return "UpdateApplication"
			case http.MethodDelete:
				return "DeleteApplication"
			}
		}
		if n >= 3 && parts[2] == "versions" {
			if n == 3 {
				if method == http.MethodGet {
					return "ListApplicationVersions"
				}
			}
			if n == 4 {
				if method == http.MethodPut {
					return "CreateApplicationVersion"
				}
			}
		}
		if n >= 3 && parts[2] == "policy" {
			switch method {
			case http.MethodGet:
				return "GetApplicationPolicy"
			case http.MethodPut:
				return "PutApplicationPolicy"
			case http.MethodDelete:
				return "DeleteApplicationPolicy"
			}
		}
		if n >= 3 && parts[2] == "changesets" {
			return "CreateCloudFormationChangeSet"
		}
		if n >= 3 && parts[2] == "templates" {
			if n >= 4 {
				return "GetCloudFormationTemplate"
			}
			return "CreateCloudFormationTemplate"
		}
		if n >= 3 && parts[2] == "unshare" {
			return "UnshareApplication"
		}
		if n >= 3 && parts[2] == "dependencies" {
			return "ListApplicationDependencies"
		}
		// /applications/{id}/versions/{semver}
		if n == 4 && parts[2] == "versions" {
			switch method {
			case http.MethodGet:
				return "GetApplicationVersion"
			case http.MethodDelete:
				return "DeleteApplicationVersion"
			case http.MethodPatch:
				return "UpdateApplicationVersion"
			case http.MethodPut:
				return "CreateApplicationVersion"
			}
		}
	}
	return ""
}

func extractAppID(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "applications" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func extractSemanticVersion(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "versions" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func appARN(appID string) string {
	return shared.BuildARN("serverlessrepo", "applications", appID)
}

func applicationToMap(a *Application, latestVersion string) map[string]any {
	// ServerlessRepo restJson1 uses lowercase jsonName. Emit both cases for
	// broad client compatibility (boto3 reads lowercase; older clients
	// PascalCase).
	m := map[string]any{
		"applicationId": a.ApplicationID,
		"ApplicationId": a.ApplicationID,
		"name":          a.Name,
		"Name":          a.Name,
		"description":   a.Description,
		"Description":   a.Description,
		"author":        a.Author,
		"Author":        a.Author,
		"homePageUrl":   a.HomePageURL,
		"HomePageUrl":   a.HomePageURL,
		"readmeUrl":     a.ReadmeURL,
		"ReadmeUrl":     a.ReadmeURL,
		"licenseUrl":    a.LicenseURL,
		"LicenseUrl":    a.LicenseURL,
		"creationTime":  a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"CreationTime":  a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
	if latestVersion != "" {
		ver := map[string]any{
			"semanticVersion": latestVersion,
			"SemanticVersion": latestVersion,
			"applicationId":   a.ApplicationID,
			"ApplicationId":   a.ApplicationID,
		}
		m["version"] = ver
		m["Version"] = ver
	}
	return m
}

func init() {
	plugin.DefaultRegistry.Register("serverlessrepo", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &ServerlessRepoProvider{}
	})
}
