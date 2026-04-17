// SPDX-License-Identifier: Apache-2.0

package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the ElasticBeanstalkService (Query/XML protocol).
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "elasticbeanstalk" }
func (p *Provider) ServiceName() string           { return "ElasticBeanstalkService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init elasticbeanstalk: %w", err)
	}
	var err error
	p.store, err = NewStore(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return ebError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return ebError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	// Application
	case "CreateApplication":
		return p.handleCreateApplication(form)
	case "DescribeApplications":
		return p.handleDescribeApplications(form)
	case "UpdateApplication":
		return p.handleUpdateApplication(form)
	case "DeleteApplication":
		return p.handleDeleteApplication(form)

	// Application Version
	case "CreateApplicationVersion":
		return p.handleCreateApplicationVersion(form)
	case "DescribeApplicationVersions":
		return p.handleDescribeApplicationVersions(form)
	case "UpdateApplicationVersion":
		return p.handleUpdateApplicationVersion(form)
	case "DeleteApplicationVersion":
		return p.handleDeleteApplicationVersion(form)

	// Environment
	case "CreateEnvironment":
		return p.handleCreateEnvironment(form)
	case "DescribeEnvironments":
		return p.handleDescribeEnvironments(form)
	case "UpdateEnvironment":
		return p.handleUpdateEnvironment(form)
	case "TerminateEnvironment":
		return p.handleTerminateEnvironment(form)
	case "RebuildEnvironment":
		return p.handleRebuildEnvironment(form)
	case "RestartAppServer":
		return p.handleRestartAppServer(form)

	// Configuration Template
	case "CreateConfigurationTemplate":
		return p.handleCreateConfigurationTemplate(form)
	case "DescribeConfigurationSettings":
		return p.handleDescribeConfigurationSettings(form)
	case "UpdateConfigurationTemplate":
		return p.handleUpdateConfigurationTemplate(form)
	case "DeleteConfigurationTemplate":
		return p.handleDeleteConfigurationTemplate(form)
	case "ValidateConfigurationSettings":
		return p.handleValidateConfigurationSettings(form)
	case "DescribeConfigurationOptions":
		return p.handleDescribeConfigurationOptions(form)

	// Events
	case "DescribeEvents":
		return p.handleDescribeEvents(form)

	// Solution stacks
	case "ListAvailableSolutionStacks":
		return p.handleListAvailableSolutionStacks(form)

	// DNS
	case "CheckDNSAvailability":
		return p.handleCheckDNSAvailability(form)

	// Account
	case "DescribeAccountAttributes":
		return p.handleDescribeAccountAttributes(form)

	// Tags
	case "ListTagsForResource":
		return p.handleListTagsForResource(form)
	case "UpdateTagsForResource":
		return p.handleUpdateTagsForResource(form)

	// No-op / stub operations
	case "AbortEnvironmentUpdate",
		"ApplyEnvironmentManagedAction",
		"AssociateEnvironmentOperationsRole",
		"ComposeEnvironments",
		"CreatePlatformVersion",
		"CreateStorageLocation",
		"DeleteEnvironmentConfiguration",
		"DeletePlatformVersion",
		"DisassociateEnvironmentOperationsRole",
		"ListPlatformBranches",
		"ListPlatformVersions",
		"RequestEnvironmentInfo",
		"RetrieveEnvironmentInfo",
		"SwapEnvironmentCNAMEs",
		"UpdateApplicationResourceLifecycle",
		"DescribeEnvironmentHealth",
		"DescribeEnvironmentManagedActionHistory",
		"DescribeEnvironmentManagedActions",
		"DescribeEnvironmentResources",
		"DescribeInstancesHealth",
		"DescribePlatformVersion":
		return p.handleStubOK(action)

	default:
		return ebError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApplications(nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		out = append(out, plugin.Resource{Type: "application", ID: a.Name, Name: a.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func ebError(code, msg string, status int) *plugin.Response {
	return shared.QueryXMLError(code, msg, status)
}

func ebXMLResponse(status int, v any) (*plugin.Response, error) {
	return shared.XMLResponse(status, v)
}

// --- XML types ---

type applicationXML struct {
	ApplicationName string `xml:"ApplicationName"`
	ApplicationArn  string `xml:"ApplicationArn"`
	Description     string `xml:"Description"`
	DateCreated     string `xml:"DateCreated"`
	DateUpdated     string `xml:"DateUpdated"`
}

type appVersionXML struct {
	ApplicationName       string `xml:"ApplicationName"`
	ApplicationVersionArn string `xml:"ApplicationVersionArn"`
	VersionLabel          string `xml:"VersionLabel"`
	Description           string `xml:"Description"`
	Status                string `xml:"Status"`
	DateCreated           string `xml:"DateCreated"`
}

type environmentXML struct {
	EnvironmentId     string `xml:"EnvironmentId"`
	EnvironmentName   string `xml:"EnvironmentName"`
	EnvironmentArn    string `xml:"EnvironmentArn"`
	ApplicationName   string `xml:"ApplicationName"`
	VersionLabel      string `xml:"VersionLabel"`
	TemplateName      string `xml:"TemplateName"`
	SolutionStackName string `xml:"SolutionStackName"`
	Tier              struct {
		Name string `xml:"Name"`
		Type string `xml:"Type"`
	} `xml:"Tier"`
	Status      string `xml:"Status"`
	Health      string `xml:"Health"`
	CNAME       string `xml:"CNAME"`
	EndpointURL string `xml:"EndpointURL"`
	Description string `xml:"Description"`
	DateCreated string `xml:"DateCreated"`
	DateUpdated string `xml:"DateUpdated"`
}

type configTemplateXML struct {
	ApplicationName   string `xml:"ApplicationName"`
	TemplateName      string `xml:"TemplateName"`
	Description       string `xml:"Description"`
	SolutionStackName string `xml:"SolutionStackName"`
}

func appToXML(a *Application) applicationXML {
	return applicationXML{
		ApplicationName: a.Name,
		ApplicationArn:  a.ARN,
		Description:     a.Description,
		DateCreated:     a.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		DateUpdated:     a.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func versionToXML(v *AppVersion) appVersionXML {
	return appVersionXML{
		ApplicationName:       v.AppName,
		ApplicationVersionArn: v.ARN,
		VersionLabel:          v.VersionLabel,
		Description:           v.Description,
		Status:                v.Status,
		DateCreated:           v.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func envToXML(e *Environment) environmentXML {
	ex := environmentXML{
		EnvironmentId:     e.ID,
		EnvironmentName:   e.Name,
		EnvironmentArn:    e.ARN,
		ApplicationName:   e.AppName,
		VersionLabel:      e.VersionLabel,
		TemplateName:      e.TemplateName,
		SolutionStackName: e.SolutionStack,
		Status:            e.Status,
		Health:            e.Health,
		CNAME:             e.CNAME,
		EndpointURL:       e.EndpointURL,
		Description:       e.Description,
		DateCreated:       e.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		DateUpdated:       e.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
	ex.Tier.Name = e.Tier
	if e.Tier == "Worker" {
		ex.Tier.Type = "SQS/HTTP"
	} else {
		ex.Tier.Type = "Standard"
	}
	return ex
}

func templateToXML(t *ConfigTemplate) configTemplateXML {
	return configTemplateXML{
		ApplicationName:   t.AppName,
		TemplateName:      t.Name,
		Description:       t.Description,
		SolutionStackName: t.SolutionStack,
	}
}

// --- Application handlers ---

func (p *Provider) handleCreateApplication(form url.Values) (*plugin.Response, error) {
	name := form.Get("ApplicationName")
	if name == "" {
		return ebError("MissingParameter", "ApplicationName is required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	arn := shared.BuildARN("elasticbeanstalk", "application", name)
	a, err := p.store.CreateApplication(name, arn, desc)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ebError("InvalidParameterValue", "application already exists: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type createAppResponse struct {
		XMLName     xml.Name       `xml:"CreateApplicationResponse"`
		Application applicationXML `xml:"CreateApplicationResult>Application"`
	}
	return ebXMLResponse(http.StatusOK, createAppResponse{Application: appToXML(a)})
}

func (p *Provider) handleDescribeApplications(form url.Values) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := form.Get(fmt.Sprintf("ApplicationNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	apps, err := p.store.ListApplications(names)
	if err != nil {
		return nil, err
	}
	items := make([]applicationXML, 0, len(apps))
	for i := range apps {
		items = append(items, appToXML(&apps[i]))
	}

	type describeAppsResponse struct {
		XMLName      xml.Name         `xml:"DescribeApplicationsResponse"`
		Applications []applicationXML `xml:"DescribeApplicationsResult>Applications>member"`
	}
	return ebXMLResponse(http.StatusOK, describeAppsResponse{Applications: items})
}

func (p *Provider) handleUpdateApplication(form url.Values) (*plugin.Response, error) {
	name := form.Get("ApplicationName")
	if name == "" {
		return ebError("MissingParameter", "ApplicationName is required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	a, err := p.store.UpdateApplication(name, desc)
	if err != nil {
		if errors.Is(err, errAppNotFound) {
			return ebError("InvalidParameterValue", "application not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type updateAppResponse struct {
		XMLName     xml.Name       `xml:"UpdateApplicationResponse"`
		Application applicationXML `xml:"UpdateApplicationResult>Application"`
	}
	return ebXMLResponse(http.StatusOK, updateAppResponse{Application: appToXML(a)})
}

func (p *Provider) handleDeleteApplication(form url.Values) (*plugin.Response, error) {
	name := form.Get("ApplicationName")
	if name == "" {
		return ebError("MissingParameter", "ApplicationName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.DeleteApplication(name)
	if err != nil {
		if errors.Is(err, errAppNotFound) {
			return ebError("InvalidParameterValue", "application not found: "+name, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteAppResponse struct {
		XMLName xml.Name `xml:"DeleteApplicationResponse"`
	}
	return ebXMLResponse(http.StatusOK, deleteAppResponse{})
}

// --- Application Version handlers ---

func (p *Provider) handleCreateApplicationVersion(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	versionLabel := form.Get("VersionLabel")
	if appName == "" || versionLabel == "" {
		return ebError("MissingParameter", "ApplicationName and VersionLabel are required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	sourceBundle := form.Get("SourceBundle")
	if sourceBundle == "" {
		sourceBundle = "{}"
	}
	arn := shared.BuildARN("elasticbeanstalk", "applicationversion", appName+"/"+versionLabel)
	v, err := p.store.CreateAppVersion(appName, versionLabel, arn, desc, sourceBundle)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ebError("InvalidParameterValue", "version already exists: "+versionLabel, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type createVersionResponse struct {
		XMLName            xml.Name      `xml:"CreateApplicationVersionResponse"`
		ApplicationVersion appVersionXML `xml:"CreateApplicationVersionResult>ApplicationVersion"`
	}
	return ebXMLResponse(http.StatusOK, createVersionResponse{ApplicationVersion: versionToXML(v)})
}

func (p *Provider) handleDescribeApplicationVersions(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	var labels []string
	for i := 1; ; i++ {
		l := form.Get(fmt.Sprintf("VersionLabels.member.%d", i))
		if l == "" {
			break
		}
		labels = append(labels, l)
	}
	versions, err := p.store.ListAppVersions(appName, labels)
	if err != nil {
		return nil, err
	}
	items := make([]appVersionXML, 0, len(versions))
	for i := range versions {
		items = append(items, versionToXML(&versions[i]))
	}

	type describeVersionsResponse struct {
		XMLName             xml.Name        `xml:"DescribeApplicationVersionsResponse"`
		ApplicationVersions []appVersionXML `xml:"DescribeApplicationVersionsResult>ApplicationVersions>member"`
	}
	return ebXMLResponse(http.StatusOK, describeVersionsResponse{ApplicationVersions: items})
}

func (p *Provider) handleUpdateApplicationVersion(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	versionLabel := form.Get("VersionLabel")
	if appName == "" || versionLabel == "" {
		return ebError("MissingParameter", "ApplicationName and VersionLabel are required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	v, err := p.store.UpdateAppVersion(appName, versionLabel, desc)
	if err != nil {
		if errors.Is(err, errVersionNotFound) {
			return ebError("InvalidParameterValue", "version not found: "+versionLabel, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type updateVersionResponse struct {
		XMLName            xml.Name      `xml:"UpdateApplicationVersionResponse"`
		ApplicationVersion appVersionXML `xml:"UpdateApplicationVersionResult>ApplicationVersion"`
	}
	return ebXMLResponse(http.StatusOK, updateVersionResponse{ApplicationVersion: versionToXML(v)})
}

func (p *Provider) handleDeleteApplicationVersion(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	versionLabel := form.Get("VersionLabel")
	if appName == "" || versionLabel == "" {
		return ebError("MissingParameter", "ApplicationName and VersionLabel are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAppVersion(appName, versionLabel); err != nil {
		if errors.Is(err, errVersionNotFound) {
			return ebError("InvalidParameterValue", "version not found: "+versionLabel, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteVersionResponse struct {
		XMLName xml.Name `xml:"DeleteApplicationVersionResponse"`
	}
	return ebXMLResponse(http.StatusOK, deleteVersionResponse{})
}

// --- Environment handlers ---

func (p *Provider) handleCreateEnvironment(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	envName := form.Get("EnvironmentName")
	if appName == "" || envName == "" {
		return ebError("MissingParameter", "ApplicationName and EnvironmentName are required", http.StatusBadRequest), nil
	}
	versionLabel := form.Get("VersionLabel")
	templateName := form.Get("TemplateName")
	solutionStack := form.Get("SolutionStackName")
	if solutionStack == "" {
		solutionStack = "64bit Amazon Linux 2023 v4.0.0 running Docker"
	}
	tier := form.Get("Tier.Name")
	if tier == "" {
		tier = "WebServer"
	}
	desc := form.Get("Description")
	cname := form.Get("CNAMEPrefix")
	if cname != "" {
		cname = cname + ".elasticbeanstalk.com"
	}

	id := shared.GenerateID("e-", 12)
	arn := shared.BuildARN("elasticbeanstalk", "environment", appName+"/"+envName)
	e, err := p.store.CreateEnvironment(id, envName, arn, appName, versionLabel, templateName, solutionStack, tier, desc, cname)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ebError("InvalidParameterValue", "environment already exists: "+envName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type createEnvResponse struct {
		XMLName     xml.Name       `xml:"CreateEnvironmentResponse"`
		Environment environmentXML `xml:"CreateEnvironmentResult"`
	}
	return ebXMLResponse(http.StatusOK, createEnvResponse{Environment: envToXML(e)})
}

func (p *Provider) handleDescribeEnvironments(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	var envNames []string
	for i := 1; ; i++ {
		n := form.Get(fmt.Sprintf("EnvironmentNames.member.%d", i))
		if n == "" {
			break
		}
		envNames = append(envNames, n)
	}
	var envIDs []string
	for i := 1; ; i++ {
		n := form.Get(fmt.Sprintf("EnvironmentIds.member.%d", i))
		if n == "" {
			break
		}
		envIDs = append(envIDs, n)
	}

	envs, err := p.store.ListEnvironments(appName, envNames, envIDs)
	if err != nil {
		return nil, err
	}
	items := make([]environmentXML, 0, len(envs))
	for i := range envs {
		items = append(items, envToXML(&envs[i]))
	}

	type describeEnvsResponse struct {
		XMLName      xml.Name         `xml:"DescribeEnvironmentsResponse"`
		Environments []environmentXML `xml:"DescribeEnvironmentsResult>Environments>member"`
	}
	return ebXMLResponse(http.StatusOK, describeEnvsResponse{Environments: items})
}

func (p *Provider) handleUpdateEnvironment(form url.Values) (*plugin.Response, error) {
	envID := form.Get("EnvironmentId")
	envName := form.Get("EnvironmentName")
	appName := form.Get("ApplicationName")

	var e *Environment
	var err error

	if envID != "" {
		e, err = p.store.GetEnvironment(envID)
	} else if envName != "" && appName != "" {
		e, err = p.store.GetEnvironmentByName(appName, envName)
	} else {
		return ebError("MissingParameter", "EnvironmentId or (ApplicationName+EnvironmentName) required", http.StatusBadRequest), nil
	}
	if err != nil {
		if errors.Is(err, errEnvNotFound) {
			return ebError("InvalidParameterValue", "environment not found", http.StatusBadRequest), nil
		}
		return nil, err
	}

	versionLabel := form.Get("VersionLabel")
	if versionLabel == "" {
		versionLabel = e.VersionLabel
	}
	desc := form.Get("Description")
	if desc == "" {
		desc = e.Description
	}

	updated, err := p.store.UpdateEnvironment(e.ID, versionLabel, desc)
	if err != nil {
		return nil, err
	}

	type updateEnvResponse struct {
		XMLName     xml.Name       `xml:"UpdateEnvironmentResponse"`
		Environment environmentXML `xml:"UpdateEnvironmentResult"`
	}
	return ebXMLResponse(http.StatusOK, updateEnvResponse{Environment: envToXML(updated)})
}

func (p *Provider) handleTerminateEnvironment(form url.Values) (*plugin.Response, error) {
	envID := form.Get("EnvironmentId")
	envName := form.Get("EnvironmentName")
	appName := form.Get("ApplicationName")

	var e *Environment
	var err error

	if envID != "" {
		e, err = p.store.GetEnvironment(envID)
	} else if envName != "" && appName != "" {
		e, err = p.store.GetEnvironmentByName(appName, envName)
	} else {
		return ebError("MissingParameter", "EnvironmentId or (ApplicationName+EnvironmentName) required", http.StatusBadRequest), nil
	}
	if err != nil {
		if errors.Is(err, errEnvNotFound) {
			return ebError("InvalidParameterValue", "environment not found", http.StatusBadRequest), nil
		}
		return nil, err
	}

	terminated, err := p.store.TerminateEnvironment(e.ID)
	if err != nil {
		return nil, err
	}

	type terminateEnvResponse struct {
		XMLName     xml.Name       `xml:"TerminateEnvironmentResponse"`
		Environment environmentXML `xml:"TerminateEnvironmentResult"`
	}
	return ebXMLResponse(http.StatusOK, terminateEnvResponse{Environment: envToXML(terminated)})
}

func (p *Provider) handleRebuildEnvironment(form url.Values) (*plugin.Response, error) {
	envID := form.Get("EnvironmentId")
	envName := form.Get("EnvironmentName")
	if envID == "" && envName == "" {
		return ebError("MissingParameter", "EnvironmentId or EnvironmentName required", http.StatusBadRequest), nil
	}

	type rebuildEnvResponse struct {
		XMLName xml.Name `xml:"RebuildEnvironmentResponse"`
	}
	return ebXMLResponse(http.StatusOK, rebuildEnvResponse{})
}

func (p *Provider) handleRestartAppServer(form url.Values) (*plugin.Response, error) {
	envID := form.Get("EnvironmentId")
	envName := form.Get("EnvironmentName")
	if envID == "" && envName == "" {
		return ebError("MissingParameter", "EnvironmentId or EnvironmentName required", http.StatusBadRequest), nil
	}

	type restartResponse struct {
		XMLName xml.Name `xml:"RestartAppServerResponse"`
	}
	return ebXMLResponse(http.StatusOK, restartResponse{})
}

// --- Configuration Template handlers ---

func (p *Provider) handleCreateConfigurationTemplate(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	tmplName := form.Get("TemplateName")
	if appName == "" || tmplName == "" {
		return ebError("MissingParameter", "ApplicationName and TemplateName are required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	solutionStack := form.Get("SolutionStackName")
	if solutionStack == "" {
		solutionStack = "64bit Amazon Linux 2023 v4.0.0 running Docker"
	}

	t, err := p.store.CreateConfigTemplate(appName, tmplName, desc, solutionStack, "{}")
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ebError("InvalidParameterValue", "template already exists: "+tmplName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type createTemplateResponse struct {
		XMLName  xml.Name          `xml:"CreateConfigurationTemplateResponse"`
		Template configTemplateXML `xml:"CreateConfigurationTemplateResult"`
	}
	return ebXMLResponse(http.StatusOK, createTemplateResponse{Template: templateToXML(t)})
}

func (p *Provider) handleDescribeConfigurationSettings(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	tmplName := form.Get("TemplateName")
	envName := form.Get("EnvironmentName")

	var items []configTemplateXML
	if tmplName != "" {
		t, err := p.store.GetConfigTemplate(appName, tmplName)
		if err != nil {
			if errors.Is(err, errTemplateNotFound) {
				return ebError("InvalidParameterValue", "template not found: "+tmplName, http.StatusBadRequest), nil
			}
			return nil, err
		}
		items = append(items, templateToXML(t))
	} else if envName != "" {
		// Return a dummy template for the environment
		items = append(items, configTemplateXML{
			ApplicationName: appName,
			TemplateName:    envName,
		})
	} else {
		templates, err := p.store.ListConfigTemplates(appName)
		if err != nil {
			return nil, err
		}
		for i := range templates {
			items = append(items, templateToXML(&templates[i]))
		}
	}

	type describeSettingsResponse struct {
		XMLName               xml.Name            `xml:"DescribeConfigurationSettingsResponse"`
		ConfigurationSettings []configTemplateXML `xml:"DescribeConfigurationSettingsResult>ConfigurationSettings>member"`
	}
	return ebXMLResponse(http.StatusOK, describeSettingsResponse{ConfigurationSettings: items})
}

func (p *Provider) handleUpdateConfigurationTemplate(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	tmplName := form.Get("TemplateName")
	if appName == "" || tmplName == "" {
		return ebError("MissingParameter", "ApplicationName and TemplateName are required", http.StatusBadRequest), nil
	}
	desc := form.Get("Description")
	t, err := p.store.UpdateConfigTemplate(appName, tmplName, desc, "{}")
	if err != nil {
		if errors.Is(err, errTemplateNotFound) {
			return ebError("InvalidParameterValue", "template not found: "+tmplName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type updateTemplateResponse struct {
		XMLName  xml.Name          `xml:"UpdateConfigurationTemplateResponse"`
		Template configTemplateXML `xml:"UpdateConfigurationTemplateResult"`
	}
	return ebXMLResponse(http.StatusOK, updateTemplateResponse{Template: templateToXML(t)})
}

func (p *Provider) handleDeleteConfigurationTemplate(form url.Values) (*plugin.Response, error) {
	appName := form.Get("ApplicationName")
	tmplName := form.Get("TemplateName")
	if appName == "" || tmplName == "" {
		return ebError("MissingParameter", "ApplicationName and TemplateName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteConfigTemplate(appName, tmplName); err != nil {
		if errors.Is(err, errTemplateNotFound) {
			return ebError("InvalidParameterValue", "template not found: "+tmplName, http.StatusBadRequest), nil
		}
		return nil, err
	}

	type deleteTemplateResponse struct {
		XMLName xml.Name `xml:"DeleteConfigurationTemplateResponse"`
	}
	return ebXMLResponse(http.StatusOK, deleteTemplateResponse{})
}

func (p *Provider) handleValidateConfigurationSettings(form url.Values) (*plugin.Response, error) {
	type validateResponse struct {
		XMLName  xml.Name `xml:"ValidateConfigurationSettingsResponse"`
		Messages struct{} `xml:"ValidateConfigurationSettingsResult>Messages"`
	}
	return ebXMLResponse(http.StatusOK, validateResponse{})
}

func (p *Provider) handleDescribeConfigurationOptions(form url.Values) (*plugin.Response, error) {
	type optionsResponse struct {
		XMLName xml.Name `xml:"DescribeConfigurationOptionsResponse"`
		Options struct{} `xml:"DescribeConfigurationOptionsResult>Options"`
	}
	return ebXMLResponse(http.StatusOK, optionsResponse{})
}

// --- Events ---

func (p *Provider) handleDescribeEvents(_ url.Values) (*plugin.Response, error) {
	type eventsResponse struct {
		XMLName xml.Name `xml:"DescribeEventsResponse"`
		Events  struct{} `xml:"DescribeEventsResult>Events"`
	}
	return ebXMLResponse(http.StatusOK, eventsResponse{})
}

// --- Solution Stacks ---

func (p *Provider) handleListAvailableSolutionStacks(_ url.Values) (*plugin.Response, error) {
	stacks := []string{
		"64bit Amazon Linux 2023 v4.0.0 running Docker",
		"64bit Amazon Linux 2023 v4.0.0 running Python 3.11",
		"64bit Amazon Linux 2023 v4.0.0 running Node.js 20",
		"64bit Amazon Linux 2023 v4.0.0 running Java 21",
		"64bit Amazon Linux 2023 v4.0.0 running Go 1",
	}

	type stackSummary struct {
		SolutionStackName string `xml:"SolutionStackName"`
	}
	type listStacksResponse struct {
		XMLName              xml.Name       `xml:"ListAvailableSolutionStacksResponse"`
		SolutionStacks       []string       `xml:"ListAvailableSolutionStacksResult>SolutionStacks>member"`
		SolutionStackDetails []stackSummary `xml:"ListAvailableSolutionStacksResult>SolutionStackDetails>member"`
	}
	details := make([]stackSummary, 0, len(stacks))
	for _, s := range stacks {
		details = append(details, stackSummary{SolutionStackName: s})
	}
	return ebXMLResponse(http.StatusOK, listStacksResponse{
		SolutionStacks:       stacks,
		SolutionStackDetails: details,
	})
}

// --- DNS ---

func (p *Provider) handleCheckDNSAvailability(form url.Values) (*plugin.Response, error) {
	cname := form.Get("CNAMEPrefix")
	fqdn := cname + ".elasticbeanstalk.com"

	type dnsResponse struct {
		XMLName             xml.Name `xml:"CheckDNSAvailabilityResponse"`
		Available           bool     `xml:"CheckDNSAvailabilityResult>Available"`
		FullyQualifiedCNAME string   `xml:"CheckDNSAvailabilityResult>FullyQualifiedCNAME"`
	}
	return ebXMLResponse(http.StatusOK, dnsResponse{Available: true, FullyQualifiedCNAME: fqdn})
}

// --- Account Attributes ---

func (p *Provider) handleDescribeAccountAttributes(_ url.Values) (*plugin.Response, error) {
	type quota struct {
		Maximum int `xml:"Maximum"`
	}
	type attrResponse struct {
		XMLName            xml.Name `xml:"DescribeAccountAttributesResponse"`
		MaxApplications    quota    `xml:"DescribeAccountAttributesResult>ResourceQuotas>ApplicationQuota"`
		MaxAppVersions     quota    `xml:"DescribeAccountAttributesResult>ResourceQuotas>ApplicationVersionQuota"`
		MaxEnvironments    quota    `xml:"DescribeAccountAttributesResult>ResourceQuotas>EnvironmentQuota"`
		MaxConfigTemplates quota    `xml:"DescribeAccountAttributesResult>ResourceQuotas>ConfigurationTemplateQuota"`
	}
	return ebXMLResponse(http.StatusOK, attrResponse{
		MaxApplications:    quota{Maximum: 75},
		MaxAppVersions:     quota{Maximum: 1000},
		MaxEnvironments:    quota{Maximum: 200},
		MaxConfigTemplates: quota{Maximum: 2000},
	})
}

// --- Tags ---

func (p *Provider) handleListTagsForResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceArn")
	if arn == "" {
		return ebError("MissingParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type tagXML struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type listTagsResponse struct {
		XMLName     xml.Name `xml:"ListTagsForResourceResponse"`
		ResourceArn string   `xml:"ListTagsForResourceResult>ResourceArn"`
		Tags        []tagXML `xml:"ListTagsForResourceResult>ResourceTags>member"`
	}
	tagList := make([]tagXML, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, tagXML{Key: k, Value: v})
	}
	return ebXMLResponse(http.StatusOK, listTagsResponse{ResourceArn: arn, Tags: tagList})
}

func (p *Provider) handleUpdateTagsForResource(form url.Values) (*plugin.Response, error) {
	arn := form.Get("ResourceArn")
	if arn == "" {
		return ebError("MissingParameter", "ResourceArn is required", http.StatusBadRequest), nil
	}

	tagsToAdd := make(map[string]string)
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagsToAdd.member.%d.Key", i))
		if k == "" {
			break
		}
		v := form.Get(fmt.Sprintf("TagsToAdd.member.%d.Value", i))
		tagsToAdd[k] = v
	}
	if len(tagsToAdd) > 0 {
		if err := p.store.tags.AddTags(arn, tagsToAdd); err != nil {
			return nil, err
		}
	}

	var keysToRemove []string
	for i := 1; ; i++ {
		k := form.Get(fmt.Sprintf("TagsToRemove.member.%d", i))
		if k == "" {
			break
		}
		keysToRemove = append(keysToRemove, k)
	}
	if len(keysToRemove) > 0 {
		if err := p.store.tags.RemoveTags(arn, keysToRemove); err != nil {
			return nil, err
		}
	}

	type updateTagsResponse struct {
		XMLName xml.Name `xml:"UpdateTagsForResourceResponse"`
	}
	return ebXMLResponse(http.StatusOK, updateTagsResponse{})
}

// --- Stub handler for no-op operations ---

func (p *Provider) handleStubOK(action string) (*plugin.Response, error) {
	type stubResponse struct {
		XMLName xml.Name
	}
	r := stubResponse{XMLName: xml.Name{Local: action + "Response"}}
	return ebXMLResponse(http.StatusOK, r)
}
