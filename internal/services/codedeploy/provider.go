// SPDX-License-Identifier: Apache-2.0

// internal/services/codedeploy/provider.go
package codedeploy

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

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "codedeploy" }
func (p *Provider) ServiceName() string           { return "CodeDeploy_20141006" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codedeploy"))
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

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		}
	}

	switch action {
	// Application
	case "CreateApplication":
		return p.createApplication(params)
	case "GetApplication":
		return p.getApplication(params)
	case "ListApplications":
		return p.listApplications(params)
	case "UpdateApplication":
		return p.updateApplication(params)
	case "DeleteApplication":
		return p.deleteApplication(params)
	case "BatchGetApplications":
		return p.batchGetApplications(params)
	// DeploymentGroup
	case "CreateDeploymentGroup":
		return p.createDeploymentGroup(params)
	case "GetDeploymentGroup":
		return p.getDeploymentGroup(params)
	case "ListDeploymentGroups":
		return p.listDeploymentGroups(params)
	case "UpdateDeploymentGroup":
		return p.updateDeploymentGroup(params)
	case "DeleteDeploymentGroup":
		return p.deleteDeploymentGroup(params)
	case "BatchGetDeploymentGroups":
		return p.batchGetDeploymentGroups(params)
	// Deployment
	case "CreateDeployment":
		return p.createDeployment(params)
	case "GetDeployment":
		return p.getDeployment(params)
	case "ListDeployments":
		return p.listDeployments(params)
	case "BatchGetDeployments":
		return p.batchGetDeployments(params)
	case "StopDeployment":
		return p.stopDeployment(params)
	// DeploymentConfig
	case "CreateDeploymentConfig":
		return p.createDeploymentConfig(params)
	case "GetDeploymentConfig":
		return p.getDeploymentConfig(params)
	case "ListDeploymentConfigs":
		return p.listDeploymentConfigs(params)
	case "DeleteDeploymentConfig":
		return p.deleteDeploymentConfig(params)
	// Revision
	case "RegisterApplicationRevision":
		return p.registerApplicationRevision(params)
	case "GetApplicationRevision":
		return p.getApplicationRevision(params)
	case "ListApplicationRevisions":
		return p.listApplicationRevisions(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		// Remaining ~27 ops: return success/empty
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		res = append(res, plugin.Resource{Type: "application", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Application handlers ----

func (p *Provider) createApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["applicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	platform, _ := params["computePlatform"].(string)
	if platform == "" {
		platform = "Server"
	}
	id := shared.GenerateID("d-", 10)
	app, err := p.store.CreateApplication(name, id, platform)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ApplicationAlreadyExistsException", "application already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	arn := appARN(app.Name)
	// Handle tags
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(arn, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applicationId": app.ID,
	})
}

func (p *Provider) getApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["applicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"application": applicationToMap(app),
	})
}

func (p *Provider) listApplications(_ map[string]any) (*plugin.Response, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(apps))
	for _, a := range apps {
		names = append(names, a.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applications": names,
	})
}

func (p *Provider) updateApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["applicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	newName, _ := params["newApplicationName"].(string)
	if newName == "" {
		newName = name
	}
	if err := p.store.UpdateApplication(name, newName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["applicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(appARN(app.Name))
	if err := p.store.DeleteApplication(name); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchGetApplications(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["applicationNames"].([]any)
	infos := make([]any, 0, len(rawNames))
	for _, n := range rawNames {
		name, _ := n.(string)
		app, err := p.store.GetApplication(name)
		if err == nil {
			infos = append(infos, applicationToMap(app))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applicationsInfo": infos,
	})
}

// ---- DeploymentGroup handlers ----

func (p *Provider) createDeploymentGroup(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	name, _ := params["deploymentGroupName"].(string)
	if appName == "" || name == "" {
		return shared.JSONError("ValidationException", "applicationName and deploymentGroupName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	serviceRole, _ := params["serviceRoleArn"].(string)
	deployConfig, _ := params["deploymentConfigName"].(string)
	if deployConfig == "" {
		deployConfig = "CodeDeployDefault.OneAtATime"
	}
	autoRollback := "{}"
	if ar, ok := params["autoRollbackConfiguration"]; ok {
		b, _ := json.Marshal(ar)
		autoRollback = string(b)
	}
	deployStyle := "{}"
	if ds, ok := params["deploymentStyle"]; ok {
		b, _ := json.Marshal(ds)
		deployStyle = string(b)
	}
	id := shared.GenerateID("d-", 10)
	grp, err := p.store.CreateDeploymentGroup(id, appName, name, serviceRole, deployConfig, autoRollback, deployStyle)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("DeploymentGroupAlreadyExistsException", "deployment group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentGroupId": grp.ID,
	})
}

func (p *Provider) getDeploymentGroup(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	name, _ := params["deploymentGroupName"].(string)
	if appName == "" || name == "" {
		return shared.JSONError("ValidationException", "applicationName and deploymentGroupName are required", http.StatusBadRequest), nil
	}
	grp, err := p.store.GetDeploymentGroup(appName, name)
	if err != nil {
		return shared.JSONError("DeploymentGroupDoesNotExistException", "deployment group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentGroupInfo": deploymentGroupToMap(grp),
	})
}

func (p *Provider) listDeploymentGroups(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	groups, err := p.store.ListDeploymentGroups(appName)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applicationName":  appName,
		"deploymentGroups": names,
	})
}

func (p *Provider) updateDeploymentGroup(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	name, _ := params["currentDeploymentGroupName"].(string)
	if appName == "" || name == "" {
		return shared.JSONError("ValidationException", "applicationName and currentDeploymentGroupName are required", http.StatusBadRequest), nil
	}
	grp, err := p.store.GetDeploymentGroup(appName, name)
	if err != nil {
		return shared.JSONError("DeploymentGroupDoesNotExistException", "deployment group not found", http.StatusBadRequest), nil
	}
	serviceRole, _ := params["serviceRoleArn"].(string)
	if serviceRole == "" {
		serviceRole = grp.ServiceRole
	}
	deployConfig, _ := params["deploymentConfigName"].(string)
	if deployConfig == "" {
		deployConfig = grp.DeploymentConfig
	}
	autoRollback := grp.AutoRollback
	if ar, ok := params["autoRollbackConfiguration"]; ok {
		b, _ := json.Marshal(ar)
		autoRollback = string(b)
	}
	deployStyle := grp.DeploymentStyle
	if ds, ok := params["deploymentStyle"]; ok {
		b, _ := json.Marshal(ds)
		deployStyle = string(b)
	}
	if err := p.store.UpdateDeploymentGroup(appName, name, serviceRole, deployConfig, autoRollback, deployStyle); err != nil {
		return shared.JSONError("DeploymentGroupDoesNotExistException", "deployment group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"hooksNotCleanedUp": []any{},
	})
}

func (p *Provider) deleteDeploymentGroup(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	name, _ := params["deploymentGroupName"].(string)
	if appName == "" || name == "" {
		return shared.JSONError("ValidationException", "applicationName and deploymentGroupName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDeploymentGroup(appName, name); err != nil {
		return shared.JSONError("DeploymentGroupDoesNotExistException", "deployment group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"hooksNotCleanedUp": []any{},
	})
}

func (p *Provider) batchGetDeploymentGroups(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	rawNames, _ := params["deploymentGroupNames"].([]any)
	infos := make([]any, 0, len(rawNames))
	for _, n := range rawNames {
		name, _ := n.(string)
		grp, err := p.store.GetDeploymentGroup(appName, name)
		if err == nil {
			infos = append(infos, deploymentGroupToMap(grp))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentGroupsInfo": infos,
		"errorMessage":         "",
	})
}

// ---- Deployment handlers ----

func (p *Provider) createDeployment(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	groupName, _ := params["deploymentGroupName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	revision := "{}"
	if rev, ok := params["revision"]; ok {
		b, _ := json.Marshal(rev)
		revision = string(b)
	}
	description, _ := params["description"].(string)
	id := fmt.Sprintf("d-%s", shared.GenerateID("", 9))
	deploy, err := p.store.CreateDeployment(id, appName, groupName, "Succeeded", revision, description)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentId": deploy.ID,
	})
}

func (p *Provider) getDeployment(params map[string]any) (*plugin.Response, error) {
	id, _ := params["deploymentId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "deploymentId is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDeployment(id)
	if err != nil {
		return shared.JSONError("DeploymentDoesNotExistException", "deployment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentInfo": deploymentToMap(d),
	})
}

func (p *Provider) listDeployments(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	groupName, _ := params["deploymentGroupName"].(string)
	deployments, err := p.store.ListDeployments(appName, groupName)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(deployments))
	for _, d := range deployments {
		ids = append(ids, d.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deployments": ids,
	})
}

func (p *Provider) batchGetDeployments(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["deploymentIds"].([]any)
	infos := make([]any, 0, len(rawIDs))
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		d, err := p.store.GetDeployment(id)
		if err == nil {
			infos = append(infos, deploymentToMap(d))
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentsInfo": infos,
	})
}

func (p *Provider) stopDeployment(params map[string]any) (*plugin.Response, error) {
	id, _ := params["deploymentId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "deploymentId is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateDeploymentStatus(id, "Stopped"); err != nil {
		return shared.JSONError("DeploymentDoesNotExistException", "deployment not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"status":        "Pending",
		"statusMessage": "Deployment stop requested",
	})
}

// ---- DeploymentConfig handlers ----

func (p *Provider) createDeploymentConfig(params map[string]any) (*plugin.Response, error) {
	name, _ := params["deploymentConfigName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "deploymentConfigName is required", http.StatusBadRequest), nil
	}
	platform, _ := params["computePlatform"].(string)
	if platform == "" {
		platform = "Server"
	}
	minHealthy := "{}"
	if mh, ok := params["minimumHealthyHosts"]; ok {
		b, _ := json.Marshal(mh)
		minHealthy = string(b)
	}
	cfg, err := p.store.CreateDeploymentConfig(name, platform, minHealthy)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("DeploymentConfigAlreadyExistsException", "deployment config already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentConfigId": cfg.Name,
	})
}

func (p *Provider) getDeploymentConfig(params map[string]any) (*plugin.Response, error) {
	name, _ := params["deploymentConfigName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "deploymentConfigName is required", http.StatusBadRequest), nil
	}
	cfg, err := p.store.GetDeploymentConfig(name)
	if err != nil {
		return shared.JSONError("DeploymentConfigDoesNotExistException", "deployment config not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentConfigInfo": deploymentConfigToMap(cfg),
	})
}

func (p *Provider) listDeploymentConfigs(_ map[string]any) (*plugin.Response, error) {
	cfgs, err := p.store.ListDeploymentConfigs()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		names = append(names, c.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"deploymentConfigsList": names,
	})
}

func (p *Provider) deleteDeploymentConfig(params map[string]any) (*plugin.Response, error) {
	name, _ := params["deploymentConfigName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "deploymentConfigName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDeploymentConfig(name); err != nil {
		return shared.JSONError("DeploymentConfigDoesNotExistException", "deployment config not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Revision handlers ----

func (p *Provider) registerApplicationRevision(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getApplicationRevision(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	revision := map[string]any{}
	if rev, ok := params["revision"]; ok {
		revision, _ = rev.(map[string]any)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"applicationName": appName,
		"revision":        revision,
		"revisionInfo":    map[string]any{},
	})
}

func (p *Provider) listApplicationRevisions(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["applicationName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "applicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ApplicationDoesNotExistException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"revisions": []any{},
	})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["tagKeys"].([]any)
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

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["resourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"tags": tagList,
	})
}

// ---- helpers ----

func appARN(name string) string {
	return shared.BuildARN("codedeploy", "application", name)
}

func applicationToMap(a *Application) map[string]any {
	return map[string]any{
		"applicationId":   a.ID,
		"applicationName": a.Name,
		"computePlatform": a.ComputePlatform,
		"createTime":      a.CreatedAt.Unix(),
		"linkedToGitHub":  false,
	}
}

func deploymentGroupToMap(g *DeploymentGroup) map[string]any {
	var autoRollback any
	_ = json.Unmarshal([]byte(g.AutoRollback), &autoRollback)
	var deployStyle any
	_ = json.Unmarshal([]byte(g.DeploymentStyle), &deployStyle)
	return map[string]any{
		"deploymentGroupId":         g.ID,
		"deploymentGroupName":       g.Name,
		"applicationName":           g.AppName,
		"serviceRoleArn":            g.ServiceRole,
		"deploymentConfigName":      g.DeploymentConfig,
		"autoRollbackConfiguration": autoRollback,
		"deploymentStyle":           deployStyle,
	}
}

func deploymentToMap(d *Deployment) map[string]any {
	var revision any
	_ = json.Unmarshal([]byte(d.Revision), &revision)
	return map[string]any{
		"deploymentId":        d.ID,
		"applicationName":     d.AppName,
		"deploymentGroupName": d.GroupName,
		"status":              d.Status,
		"revision":            revision,
		"description":         d.Description,
		"createTime":          d.CreatedAt.Unix(),
		"completeTime":        d.CompletedAt,
	}
}

func deploymentConfigToMap(c *DeploymentConfig) map[string]any {
	var minHealthy any
	_ = json.Unmarshal([]byte(c.MinHealthy), &minHealthy)
	return map[string]any{
		"deploymentConfigName": c.Name,
		"computePlatform":      c.ComputePlatform,
		"minimumHealthyHosts":  minHealthy,
		"createTime":           c.CreatedAt.Unix(),
	}
}

func parseTags(rawTags []any) map[string]string {
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

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
