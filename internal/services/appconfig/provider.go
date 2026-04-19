// SPDX-License-Identifier: Apache-2.0

// internal/services/appconfig/provider.go
package appconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the AppConfig service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "appconfig" }
func (p *Provider) ServiceName() string           { return "AppConfig" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "appconfig"))
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
		return shared.JSONError("BadRequestException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return shared.JSONError("BadRequestException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	pathParts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")

	switch op {
	// Applications
	case "CreateApplication":
		return p.createApplication(params)
	case "GetApplication":
		return p.getApplication(pathPart(pathParts, "applications", 1))
	case "ListApplications":
		return p.listApplications()
	case "UpdateApplication":
		return p.updateApplication(pathPart(pathParts, "applications", 1), params)
	case "DeleteApplication":
		return p.deleteApplication(pathPart(pathParts, "applications", 1))

	// Environments
	case "CreateEnvironment":
		return p.createEnvironment(pathPart(pathParts, "applications", 1), params)
	case "GetEnvironment":
		return p.getEnvironment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1))
	case "ListEnvironments":
		return p.listEnvironments(pathPart(pathParts, "applications", 1))
	case "UpdateEnvironment":
		return p.updateEnvironment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1), params)
	case "DeleteEnvironment":
		return p.deleteEnvironment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1))

	// Config Profiles
	case "CreateConfigurationProfile":
		return p.createConfigProfile(pathPart(pathParts, "applications", 1), params)
	case "GetConfigurationProfile":
		return p.getConfigProfile(pathPart(pathParts, "applications", 1), pathPart(pathParts, "configurationprofiles", 1))
	case "ListConfigurationProfiles":
		return p.listConfigProfiles(pathPart(pathParts, "applications", 1))
	case "UpdateConfigurationProfile":
		return p.updateConfigProfile(pathPart(pathParts, "applications", 1), pathPart(pathParts, "configurationprofiles", 1), params)
	case "DeleteConfigurationProfile":
		return p.deleteConfigProfile(pathPart(pathParts, "applications", 1), pathPart(pathParts, "configurationprofiles", 1))

	// Deployment Strategies
	case "CreateDeploymentStrategy":
		return p.createDeploymentStrategy(params)
	case "GetDeploymentStrategy":
		return p.getDeploymentStrategy(pathPart(pathParts, "deploymentstrategies", 1))
	case "ListDeploymentStrategies":
		return p.listDeploymentStrategies()
	case "UpdateDeploymentStrategy":
		return p.updateDeploymentStrategy(pathPart(pathParts, "deploymentstrategies", 1), params)
	case "DeleteDeploymentStrategy":
		return p.deleteDeploymentStrategy(pathPart(pathParts, "deployementstrategies", 1))

	// Deployments
	case "StartDeployment":
		return p.startDeployment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1), params)
	case "GetDeployment":
		numStr := pathPart(pathParts, "deployments", 1)
		num, _ := strconv.Atoi(numStr)
		return p.getDeployment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1), num)
	case "ListDeployments":
		return p.listDeployments(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1))
	case "StopDeployment":
		numStr := pathPart(pathParts, "deployments", 1)
		num, _ := strconv.Atoi(numStr)
		return p.stopDeployment(pathPart(pathParts, "applications", 1), pathPart(pathParts, "environments", 1), num)

	// Extensions
	case "CreateExtension":
		return p.createExtension(params)
	case "GetExtension":
		return p.getExtension(pathPart(pathParts, "extensions", 1))
	case "ListExtensions":
		return p.listExtensions()
	case "UpdateExtension":
		return p.updateExtension(pathPart(pathParts, "extensions", 1), params)
	case "DeleteExtension":
		return p.deleteExtension(pathPart(pathParts, "extensions", 1))

	// Extension Associations
	case "CreateExtensionAssociation":
		return p.createExtensionAssociation(params)
	case "GetExtensionAssociation":
		return p.getExtensionAssociation(pathPart(pathParts, "extensionassociations", 1))
	case "ListExtensionAssociations":
		return p.listExtensionAssociations()
	case "UpdateExtensionAssociation":
		return p.updateExtensionAssociation(pathPart(pathParts, "extensionassociations", 1), params)
	case "DeleteExtensionAssociation":
		return p.deleteExtensionAssociation(pathPart(pathParts, "extensionassociations", 1))

	// Hosted Config Versions
	case "CreateHostedConfigurationVersion":
		return p.createHostedConfigVersion(
			pathPart(pathParts, "applications", 1),
			pathPart(pathParts, "configurationprofiles", 1),
			body, req)
	case "GetHostedConfigurationVersion":
		verStr := pathPart(pathParts, "hostedconfigurationversions", 1)
		ver, _ := strconv.Atoi(verStr)
		return p.getHostedConfigVersion(
			pathPart(pathParts, "applications", 1),
			pathPart(pathParts, "configurationprofiles", 1),
			ver)
	case "ListHostedConfigurationVersions":
		return p.listHostedConfigVersions(
			pathPart(pathParts, "applications", 1),
			pathPart(pathParts, "configurationprofiles", 1))
	case "DeleteHostedConfigurationVersion":
		verStr := pathPart(pathParts, "hostedconfigurationversions", 1)
		ver, _ := strconv.Atoi(verStr)
		return p.deleteHostedConfigVersion(
			pathPart(pathParts, "applications", 1),
			pathPart(pathParts, "configurationprofiles", 1),
			ver)

	// GetConfiguration (legacy)
	case "GetConfiguration":
		return p.getConfiguration(pathParts)

	// ValidateConfiguration
	case "ValidateConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// Tags
	case "TagResource":
		return p.tagResource(pathPart(pathParts, "tags", 1), params)
	case "UntagResource":
		return p.untagResource(pathPart(pathParts, "tags", 1), req)
	case "ListTagsForResource":
		return p.listTagsForResource(pathPart(pathParts, "tags", 1))

	// Account Settings
	case "GetAccountSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"DeletionProtection": map[string]any{
				"Enabled":                   false,
				"ProtectionPeriodInMinutes": 0,
			},
		})
	case "UpdateAccountSettings":
		dp, _ := params["DeletionProtection"].(map[string]any)
		if dp == nil {
			dp = map[string]any{}
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"DeletionProtection": dp,
		})

	default:
		return shared.JSONError("BadRequestException", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		res = append(res, plugin.Resource{Type: "appconfig-application", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Applications ---

func (p *Provider) createApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "application name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", "application", id)

	a := &Application{ID: id, ARN: arn, Name: name, Description: description}
	if err := p.store.CreateApplication(a); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "application already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"Id":          id,
		"Name":        name,
		"Description": description,
	})
}

func (p *Provider) getApplication(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetApplication(appID)
	if err != nil {
		// Also try by name
		a, err = p.store.FindApplicationByName(appID)
		if err != nil {
			return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
		}
	}
	return shared.JSONResponse(http.StatusOK, applicationToMap(a))
}

func (p *Provider) listApplications() (*plugin.Response, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		items = append(items, applicationToMap(&a))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Items": items,
	})
}

func (p *Provider) updateApplication(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetApplication(appID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	if v, ok := params["Name"].(string); ok && v != "" {
		a.Name = v
	}
	if v, ok := params["Description"].(string); ok {
		a.Description = v
	}
	if err := p.store.UpdateApplication(appID, a.Name, a.Description); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	a, _ = p.store.GetApplication(appID)
	return shared.JSONResponse(http.StatusOK, applicationToMap(a))
}

func (p *Provider) deleteApplication(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetApplication(appID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(a.ARN)
	if err := p.store.DeleteApplication(appID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Environments ---

func (p *Provider) createEnvironment(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "environment name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", fmt.Sprintf("application/%s/environment", appID), id)

	e := &Environment{ID: id, AppID: appID, ARN: arn, Name: name, Description: description, State: "READY_FOR_DEPLOYMENT"}
	if err := p.store.CreateEnvironment(e); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, environmentToMap(e))
}

func (p *Provider) getEnvironment(appID, envID string) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "application and environment id are required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(appID, envID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, environmentToMap(e))
}

func (p *Provider) listEnvironments(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	envs, err := p.store.ListEnvironments(appID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(envs))
	for _, e := range envs {
		items = append(items, environmentToMap(&e))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) updateEnvironment(appID, envID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(appID, envID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	if v, ok := params["Name"].(string); ok && v != "" {
		e.Name = v
	}
	if v, ok := params["Description"].(string); ok {
		e.Description = v
	}
	if err := p.store.UpdateEnvironment(appID, envID, e.Name, e.Description); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	e, _ = p.store.GetEnvironment(appID, envID)
	return shared.JSONResponse(http.StatusOK, environmentToMap(e))
}

func (p *Provider) deleteEnvironment(appID, envID string) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(appID, envID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(e.ARN)
	if err := p.store.DeleteEnvironment(appID, envID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Config Profiles ---

func (p *Provider) createConfigProfile(appID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusNotFound), nil
	}
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "profile name is required", http.StatusBadRequest), nil
	}
	locationURI, _ := params["LocationUri"].(string)
	if locationURI == "" {
		locationURI = "hosted"
	}
	profileType, _ := params["Type"].(string)
	if profileType == "" {
		profileType = "AWS.Freeform"
	}
	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", fmt.Sprintf("application/%s/configurationprofile", appID), id)

	cp := &ConfigProfile{ID: id, AppID: appID, ARN: arn, Name: name, LocationURI: locationURI, Type: profileType}
	if err := p.store.CreateConfigProfile(cp); err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, configProfileToMap(cp))
}

func (p *Provider) getConfigProfile(appID, profileID string) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetConfigProfile(appID, profileID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, configProfileToMap(cp))
}

func (p *Provider) listConfigProfiles(appID string) (*plugin.Response, error) {
	if appID == "" {
		return shared.JSONError("BadRequestException", "application id is required", http.StatusBadRequest), nil
	}
	profiles, err := p.store.ListConfigProfiles(appID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(profiles))
	for _, cp := range profiles {
		items = append(items, configProfileToMap(&cp))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) updateConfigProfile(appID, profileID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetConfigProfile(appID, profileID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}
	if v, ok := params["Name"].(string); ok && v != "" {
		cp.Name = v
	}
	if err := p.store.UpdateConfigProfile(appID, profileID, cp.Name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}
	cp, _ = p.store.GetConfigProfile(appID, profileID)
	return shared.JSONResponse(http.StatusOK, configProfileToMap(cp))
}

func (p *Provider) deleteConfigProfile(appID, profileID string) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	cp, err := p.store.GetConfigProfile(appID, profileID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(cp.ARN)
	if err := p.store.DeleteConfigProfile(appID, profileID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Deployment Strategies ---

func (p *Provider) createDeploymentStrategy(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "deployment strategy name is required", http.StatusBadRequest), nil
	}
	growthType, _ := params["GrowthType"].(string)
	if growthType == "" {
		growthType = "LINEAR"
	}
	growthFactor, _ := params["GrowthFactor"].(float64)
	if growthFactor == 0 {
		growthFactor = 10
	}
	deploymentDuration := 0
	if v, ok := params["DeploymentDurationInMinutes"].(float64); ok {
		deploymentDuration = int(v)
	}
	finalBake := 0
	if v, ok := params["FinalBakeTimeInMinutes"].(float64); ok {
		finalBake = int(v)
	}
	replicateTo, _ := params["ReplicateTo"].(string)
	if replicateTo == "" {
		replicateTo = "NONE"
	}
	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", "deploymentstrategy", id)

	ds := &DeploymentStrategy{
		ID:                 id,
		ARN:                arn,
		Name:               name,
		GrowthType:         growthType,
		GrowthFactor:       growthFactor,
		DeploymentDuration: deploymentDuration,
		FinalBake:          finalBake,
		ReplicateTo:        replicateTo,
	}
	if err := p.store.CreateDeploymentStrategy(ds); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "deployment strategy already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags))
	}
	return shared.JSONResponse(http.StatusCreated, deploymentStrategyToMap(ds))
}

func (p *Provider) getDeploymentStrategy(stratID string) (*plugin.Response, error) {
	if stratID == "" {
		return shared.JSONError("BadRequestException", "deployment strategy id is required", http.StatusBadRequest), nil
	}
	ds, err := p.store.GetDeploymentStrategy(stratID)
	if err != nil {
		ds, err = p.store.FindDeploymentStrategyByName(stratID)
		if err != nil {
			return shared.JSONError("ResourceNotFoundException", "deployment strategy not found", http.StatusNotFound), nil
		}
	}
	return shared.JSONResponse(http.StatusOK, deploymentStrategyToMap(ds))
}

func (p *Provider) listDeploymentStrategies() (*plugin.Response, error) {
	strategies, err := p.store.ListDeploymentStrategies()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(strategies))
	for _, ds := range strategies {
		items = append(items, deploymentStrategyToMap(&ds))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) updateDeploymentStrategy(stratID string, params map[string]any) (*plugin.Response, error) {
	if stratID == "" {
		return shared.JSONError("BadRequestException", "deployment strategy id is required", http.StatusBadRequest), nil
	}
	ds, err := p.store.GetDeploymentStrategy(stratID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "deployment strategy not found", http.StatusNotFound), nil
	}
	if v, ok := params["GrowthType"].(string); ok && v != "" {
		ds.GrowthType = v
	}
	if v, ok := params["GrowthFactor"].(float64); ok {
		ds.GrowthFactor = v
	}
	if v, ok := params["DeploymentDurationInMinutes"].(float64); ok {
		ds.DeploymentDuration = int(v)
	}
	if v, ok := params["FinalBakeTimeInMinutes"].(float64); ok {
		ds.FinalBake = int(v)
	}
	if err := p.store.UpdateDeploymentStrategy(stratID, ds.GrowthType, ds.GrowthFactor, ds.DeploymentDuration, ds.FinalBake); err != nil {
		return shared.JSONError("ResourceNotFoundException", "deployment strategy not found", http.StatusNotFound), nil
	}
	ds, _ = p.store.GetDeploymentStrategy(stratID)
	return shared.JSONResponse(http.StatusOK, deploymentStrategyToMap(ds))
}

func (p *Provider) deleteDeploymentStrategy(stratID string) (*plugin.Response, error) {
	if stratID == "" {
		return shared.JSONError("BadRequestException", "deployment strategy id is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteDeploymentStrategy(stratID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "deployment strategy not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Deployments ---

func (p *Provider) startDeployment(appID, envID string, params map[string]any) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	configProfileID, _ := params["ConfigurationProfileId"].(string)
	configVersion, _ := params["ConfigurationVersion"].(string)
	strategyID, _ := params["DeploymentStrategyId"].(string)

	d := &Deployment{
		AppID:         appID,
		EnvID:         envID,
		ConfigProfile: configProfileID,
		ConfigVersion: configVersion,
		Strategy:      strategyID,
		State:         "COMPLETE",
	}
	if err := p.store.CreateDeployment(d); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, deploymentToMap(d))
}

func (p *Provider) getDeployment(appID, envID string, number int) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDeployment(appID, envID, number)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "deployment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, deploymentToMap(d))
}

func (p *Provider) listDeployments(appID, envID string) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	deployments, err := p.store.ListDeployments(appID, envID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(deployments))
	for _, d := range deployments {
		items = append(items, deploymentToMap(&d))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) stopDeployment(appID, envID string, number int) (*plugin.Response, error) {
	if appID == "" || envID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDeployment(appID, envID, number)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "deployment not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateDeploymentState(appID, envID, number, "ROLLED_BACK"); err != nil {
		return shared.JSONError("InternalError", "failed to update deployment state", http.StatusInternalServerError), nil
	}
	d.State = "ROLLED_BACK"
	return shared.JSONResponse(http.StatusOK, deploymentToMap(d))
}

// --- Extensions ---

func (p *Provider) createExtension(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("BadRequestException", "extension name is required", http.StatusBadRequest), nil
	}
	description, _ := params["Description"].(string)
	actions := "{}"
	if v, ok := params["Actions"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		actions = string(b)
	}
	parameters := "{}"
	if v, ok := params["Parameters"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		parameters = string(b)
	}
	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", "extension", id)

	e := &Extension{ID: id, ARN: arn, Name: name, Description: description, Actions: actions, Parameters: parameters, Version: 1}
	if err := p.store.CreateExtension(e); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "extension already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, extensionToMap(e))
}

func (p *Provider) getExtension(extID string) (*plugin.Response, error) {
	if extID == "" {
		return shared.JSONError("BadRequestException", "extension id is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetExtension(extID)
	if err != nil {
		e, err = p.store.FindExtensionByName(extID)
		if err != nil {
			return shared.JSONError("ResourceNotFoundException", "extension not found", http.StatusNotFound), nil
		}
	}
	return shared.JSONResponse(http.StatusOK, extensionToMap(e))
}

func (p *Provider) listExtensions() (*plugin.Response, error) {
	exts, err := p.store.ListExtensions()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(exts))
	for _, e := range exts {
		items = append(items, extensionToMap(&e))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) updateExtension(extID string, params map[string]any) (*plugin.Response, error) {
	if extID == "" {
		return shared.JSONError("BadRequestException", "extension id is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetExtension(extID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension not found", http.StatusNotFound), nil
	}
	if v, ok := params["Description"].(string); ok {
		e.Description = v
	}
	if v, ok := params["Actions"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		e.Actions = string(b)
	}
	if v, ok := params["Parameters"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		e.Parameters = string(b)
	}
	if err := p.store.UpdateExtension(extID, e.Description, e.Actions, e.Parameters); err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension not found", http.StatusNotFound), nil
	}
	e, _ = p.store.GetExtension(extID)
	return shared.JSONResponse(http.StatusOK, extensionToMap(e))
}

func (p *Provider) deleteExtension(extID string) (*plugin.Response, error) {
	if extID == "" {
		return shared.JSONError("BadRequestException", "extension id is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteExtension(extID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Extension Associations ---

func (p *Provider) createExtensionAssociation(params map[string]any) (*plugin.Response, error) {
	extIdentifier, _ := params["ExtensionIdentifier"].(string)
	if extIdentifier == "" {
		return shared.JSONError("BadRequestException", "extension identifier is required", http.StatusBadRequest), nil
	}
	resourceIdentifier, _ := params["ResourceIdentifier"].(string)
	parametersStr := "{}"
	if v, ok := params["Parameters"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		parametersStr = string(b)
	}

	// Resolve extension
	e, err := p.store.GetExtension(extIdentifier)
	if err != nil {
		e, err = p.store.FindExtensionByName(extIdentifier)
		if err != nil {
			return shared.JSONError("ResourceNotFoundException", "extension not found", http.StatusNotFound), nil
		}
	}

	id := shared.GenerateID("", 7)
	arn := shared.BuildARN("appconfig", "extensionassociation", id)

	ea := &ExtensionAssociation{
		ID:               id,
		ARN:              arn,
		ExtensionID:      e.ID,
		ExtensionARN:     e.ARN,
		ExtensionVersion: e.Version,
		ResourceARN:      resourceIdentifier,
		Parameters:       parametersStr,
	}
	if err := p.store.CreateExtensionAssociation(ea); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, extensionAssociationToMap(ea))
}

func (p *Provider) getExtensionAssociation(assocID string) (*plugin.Response, error) {
	if assocID == "" {
		return shared.JSONError("BadRequestException", "association id is required", http.StatusBadRequest), nil
	}
	ea, err := p.store.GetExtensionAssociation(assocID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, extensionAssociationToMap(ea))
}

func (p *Provider) listExtensionAssociations() (*plugin.Response, error) {
	assocs, err := p.store.ListExtensionAssociations()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(assocs))
	for _, ea := range assocs {
		items = append(items, extensionAssociationToMap(&ea))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) updateExtensionAssociation(assocID string, params map[string]any) (*plugin.Response, error) {
	if assocID == "" {
		return shared.JSONError("BadRequestException", "association id is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetExtensionAssociation(assocID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension association not found", http.StatusNotFound), nil
	}
	parametersStr := "{}"
	if v, ok := params["Parameters"].(map[string]any); ok {
		b, _ := json.Marshal(v)
		parametersStr = string(b)
	}
	if err := p.store.UpdateExtensionAssociation(assocID, parametersStr); err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension association not found", http.StatusNotFound), nil
	}
	ea, _ := p.store.GetExtensionAssociation(assocID)
	return shared.JSONResponse(http.StatusOK, extensionAssociationToMap(ea))
}

func (p *Provider) deleteExtensionAssociation(assocID string) (*plugin.Response, error) {
	if assocID == "" {
		return shared.JSONError("BadRequestException", "association id is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteExtensionAssociation(assocID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "extension association not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

// --- Hosted Config Versions ---

func (p *Provider) createHostedConfigVersion(appID, profileID string, body []byte, req *http.Request) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetConfigProfile(appID, profileID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "configuration profile not found", http.StatusNotFound), nil
	}

	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	description := req.Header.Get("Description")
	content := string(body)

	hcv := &HostedConfigVersion{
		AppID:       appID,
		ProfileID:   profileID,
		Content:     content,
		ContentType: contentType,
		Description: description,
	}
	if err := p.store.CreateHostedConfigVersion(hcv); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusCreated, hostedConfigVersionToMap(hcv))
}

func (p *Provider) getHostedConfigVersion(appID, profileID string, version int) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	hcv, err := p.store.GetHostedConfigVersion(appID, profileID, version)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "hosted configuration version not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, hostedConfigVersionToMap(hcv))
}

func (p *Provider) listHostedConfigVersions(appID, profileID string) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	versions, err := p.store.ListHostedConfigVersions(appID, profileID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, hostedConfigVersionToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Items": items})
}

func (p *Provider) deleteHostedConfigVersion(appID, profileID string, version int) (*plugin.Response, error) {
	if appID == "" || profileID == "" {
		return shared.JSONError("BadRequestException", "ids are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteHostedConfigVersion(appID, profileID, version); err != nil {
		return shared.JSONError("ResourceNotFoundException", "hosted configuration version not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

func (p *Provider) getConfiguration(pathParts []string) (*plugin.Response, error) {
	// Legacy: /applications/{app}/environments/{env}/configurations/{config}
	app := pathPart(pathParts, "applications", 1)
	env := pathPart(pathParts, "environments", 1)
	if app == "" || env == "" {
		return shared.JSONResponse(http.StatusOK, map[string]any{"Content": "", "ContentType": "application/json", "ConfigurationVersion": "0"})
	}

	// Try to find latest deployment
	a, err := p.store.GetApplication(app)
	if err != nil {
		a, err = p.store.FindApplicationByName(app)
	}
	if err != nil || a == nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{"Content": "", "ContentType": "application/json", "ConfigurationVersion": "0"})
	}

	envs, _ := p.store.ListEnvironments(a.ID)
	var envID string
	for _, e := range envs {
		if e.ID == env || e.Name == env {
			envID = e.ID
			break
		}
	}
	if envID == "" {
		return shared.JSONResponse(http.StatusOK, map[string]any{"Content": "", "ContentType": "application/json", "ConfigurationVersion": "0"})
	}

	d, err := p.store.GetLatestDeployment(a.ID, envID)
	if err != nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{"Content": "", "ContentType": "application/json", "ConfigurationVersion": "0"})
	}

	hcv, err := p.store.GetLatestHostedConfigVersion(a.ID, d.ConfigProfile)
	if err != nil {
		return shared.JSONResponse(http.StatusOK, map[string]any{"Content": d.ConfigVersion, "ContentType": "application/json", "ConfigurationVersion": d.ConfigVersion})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Content":              hcv.Content,
		"ContentType":          hcv.ContentType,
		"ConfigurationVersion": strconv.Itoa(hcv.Version),
	})
}

// --- Tags ---

func (p *Provider) tagResource(arn string, params map[string]any) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	if err := p.store.tags.AddTags(arn, toStringMap(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

func (p *Provider) untagResource(arn string, req *http.Request) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusNoContent, map[string]any{})
}

func (p *Provider) listTagsForResource(arn string) (*plugin.Response, error) {
	if arn == "" {
		return shared.JSONError("BadRequestException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Helpers / serializers ---

func applicationToMap(a *Application) map[string]any {
	return map[string]any{
		"Id":          a.ID,
		"Name":        a.Name,
		"Description": a.Description,
	}
}

func environmentToMap(e *Environment) map[string]any {
	return map[string]any{
		"Id":            e.ID,
		"ApplicationId": e.AppID,
		"Name":          e.Name,
		"Description":   e.Description,
		"State":         e.State,
	}
}

func configProfileToMap(cp *ConfigProfile) map[string]any {
	return map[string]any{
		"Id":            cp.ID,
		"ApplicationId": cp.AppID,
		"Name":          cp.Name,
		"LocationUri":   cp.LocationURI,
		"Type":          cp.Type,
	}
}

func deploymentStrategyToMap(ds *DeploymentStrategy) map[string]any {
	return map[string]any{
		"Id":                          ds.ID,
		"Name":                        ds.Name,
		"GrowthType":                  ds.GrowthType,
		"GrowthFactor":                ds.GrowthFactor,
		"DeploymentDurationInMinutes": ds.DeploymentDuration,
		"FinalBakeTimeInMinutes":      ds.FinalBake,
		"ReplicateTo":                 ds.ReplicateTo,
	}
}

func deploymentToMap(d *Deployment) map[string]any {
	return map[string]any{
		"ApplicationId":          d.AppID,
		"EnvironmentId":          d.EnvID,
		"DeploymentNumber":       d.Number,
		"ConfigurationProfileId": d.ConfigProfile,
		"ConfigurationVersion":   d.ConfigVersion,
		"DeploymentStrategyId":   d.Strategy,
		"State":                  d.State,
		"StartedAt":              d.StartedAt.Unix(),
		"CompletedAt":            d.CompletedAt.Unix(),
		"PercentageComplete":     100.0,
	}
}

func extensionToMap(e *Extension) map[string]any {
	var actions, parameters any
	_ = json.Unmarshal([]byte(e.Actions), &actions)
	_ = json.Unmarshal([]byte(e.Parameters), &parameters)
	if actions == nil {
		actions = map[string]any{}
	}
	if parameters == nil {
		parameters = map[string]any{}
	}
	return map[string]any{
		"Id":            e.ID,
		"Arn":           e.ARN,
		"Name":          e.Name,
		"Description":   e.Description,
		"Actions":       actions,
		"Parameters":    parameters,
		"VersionNumber": e.Version,
	}
}

func extensionAssociationToMap(ea *ExtensionAssociation) map[string]any {
	var parameters any
	_ = json.Unmarshal([]byte(ea.Parameters), &parameters)
	if parameters == nil {
		parameters = map[string]any{}
	}
	return map[string]any{
		"Id":                     ea.ID,
		"Arn":                    ea.ARN,
		"ExtensionArn":           ea.ExtensionARN,
		"ExtensionVersionNumber": ea.ExtensionVersion,
		"ResourceArn":            ea.ResourceARN,
		"Parameters":             parameters,
	}
}

func hostedConfigVersionToMap(hcv *HostedConfigVersion) map[string]any {
	return map[string]any{
		"ApplicationId":          hcv.AppID,
		"ConfigurationProfileId": hcv.ProfileID,
		"VersionNumber":          hcv.Version,
		"Content":                hcv.Content,
		"ContentType":            hcv.ContentType,
		"Description":            hcv.Description,
	}
}

// pathPart returns the path segment following the named segment + offset.
// e.g. pathPart(["applications","abc","environments","xyz"], "applications", 1) -> "abc"
// resolveOp maps HTTP method+path to an AppConfig operation name.
func resolveOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	switch segs[0] {
	case "applications":
		if n == 1 {
			switch method {
			case "POST":
				return "CreateApplication"
			case "GET":
				return "ListApplications"
			}
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetApplication"
			case "PATCH":
				return "UpdateApplication"
			case "DELETE":
				return "DeleteApplication"
			}
		}
		if n >= 3 {
			sub := segs[2]
			switch sub {
			case "environments":
				if n == 3 {
					switch method {
					case "POST":
						return "CreateEnvironment"
					case "GET":
						return "ListEnvironments"
					}
				}
				if n == 4 {
					switch method {
					case "GET":
						return "GetEnvironment"
					case "PATCH":
						return "UpdateEnvironment"
					case "DELETE":
						return "DeleteEnvironment"
					}
				}
				if n >= 5 && segs[4] == "deployments" {
					if n == 5 {
						switch method {
						case "POST":
							return "StartDeployment"
						case "GET":
							return "ListDeployments"
						}
					}
					if n == 6 {
						switch method {
						case "GET":
							return "GetDeployment"
						case "DELETE":
							return "StopDeployment"
						}
					}
				}
			case "configurationprofiles":
				if n == 3 {
					switch method {
					case "POST":
						return "CreateConfigurationProfile"
					case "GET":
						return "ListConfigurationProfiles"
					}
				}
				if n == 4 {
					switch method {
					case "GET":
						return "GetConfigurationProfile"
					case "PATCH":
						return "UpdateConfigurationProfile"
					case "DELETE":
						return "DeleteConfigurationProfile"
					}
				}
				if n >= 5 && segs[4] == "hostedconfigurationversions" {
					if n == 5 {
						switch method {
						case "POST":
							return "CreateHostedConfigurationVersion"
						case "GET":
							return "ListHostedConfigurationVersions"
						}
					}
					if n == 6 {
						switch method {
						case "GET":
							return "GetHostedConfigurationVersion"
						case "DELETE":
							return "DeleteHostedConfigurationVersion"
						}
					}
				}
				if n == 5 && segs[4] == "validators" && method == "POST" {
					return "ValidateConfiguration"
				}
			case "configuration":
				return "GetConfiguration"
			}
		}
	case "deploymentstrategies", "deployementstrategies":
		if n == 1 {
			switch method {
			case "POST":
				return "CreateDeploymentStrategy"
			case "GET":
				return "ListDeploymentStrategies"
			}
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetDeploymentStrategy"
			case "PATCH":
				return "UpdateDeploymentStrategy"
			case "DELETE":
				return "DeleteDeploymentStrategy"
			}
		}
	case "extensions":
		if n == 1 {
			switch method {
			case "POST":
				return "CreateExtension"
			case "GET":
				return "ListExtensions"
			}
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetExtension"
			case "PATCH":
				return "UpdateExtension"
			case "DELETE":
				return "DeleteExtension"
			}
		}
	case "extensionassociations":
		if n == 1 {
			switch method {
			case "POST":
				return "CreateExtensionAssociation"
			case "GET":
				return "ListExtensionAssociations"
			}
		}
		if n == 2 {
			switch method {
			case "GET":
				return "GetExtensionAssociation"
			case "PATCH":
				return "UpdateExtensionAssociation"
			case "DELETE":
				return "DeleteExtensionAssociation"
			}
		}
	case "tags":
		if n >= 2 {
			switch method {
			case "POST":
				return "TagResource"
			case "DELETE":
				return "UntagResource"
			case "GET":
				return "ListTagsForResource"
			}
		}
	case "accountsettings":
		switch method {
		case "GET":
			return "GetAccountSettings"
		case "PATCH":
			return "UpdateAccountSettings"
		}
	}
	return ""
}

func pathPart(parts []string, segment string, offset int) string {
	for i, p := range parts {
		if p == segment && i+offset < len(parts) {
			return parts[i+offset]
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
