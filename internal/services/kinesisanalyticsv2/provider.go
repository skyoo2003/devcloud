// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesisanalyticsv2/provider.go
package kinesisanalyticsv2

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

func (p *Provider) ServiceID() string             { return "kinesisanalyticsv2" }
func (p *Provider) ServiceName() string           { return "KinesisAnalytics_20180523" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "kinesisanalyticsv2"))
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
	case "CreateApplication":
		return p.createApplication(params)
	case "DescribeApplication":
		return p.describeApplication(params)
	case "ListApplications":
		return p.listApplications(params)
	case "DeleteApplication":
		return p.deleteApplication(params)
	case "UpdateApplication":
		return p.updateApplication(params)
	case "StartApplication":
		return p.startApplication(params)
	case "StopApplication":
		return p.stopApplication(params)
	case "CreateApplicationSnapshot":
		return p.createApplicationSnapshot(params)
	case "DescribeApplicationSnapshot":
		return p.describeApplicationSnapshot(params)
	case "ListApplicationSnapshots":
		return p.listApplicationSnapshots(params)
	case "DeleteApplicationSnapshot":
		return p.deleteApplicationSnapshot(params)
	case "CreateApplicationPresignedUrl":
		return p.createApplicationPresignedUrl(params)
	case "DescribeApplicationVersion":
		return p.describeApplicationVersion(params)
	case "ListApplicationVersions":
		return p.listApplicationVersions(params)
	case "DescribeApplicationOperation":
		return p.describeApplicationOperation(params)
	case "ListApplicationOperations":
		return p.listApplicationOperations(params)
	case "DiscoverInputSchema":
		return p.discoverInputSchema(params)
	case "RollbackApplication":
		return p.rollbackApplication(params)
	case "UpdateApplicationMaintenanceConfiguration":
		return p.updateApplicationMaintenanceConfiguration(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	case "AddApplicationCloudWatchLoggingOption",
		"DeleteApplicationCloudWatchLoggingOption",
		"AddApplicationInput",
		"AddApplicationInputProcessingConfiguration",
		"DeleteApplicationInputProcessingConfiguration",
		"AddApplicationOutput",
		"DeleteApplicationOutput",
		"AddApplicationReferenceDataSource",
		"DeleteApplicationReferenceDataSource",
		"AddApplicationVpcConfiguration",
		"DeleteApplicationVpcConfiguration":
		return p.appSubConfigOperation(action, params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(apps))
	for _, a := range apps {
		res = append(res, plugin.Resource{Type: "application", ID: a.Name, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func appToMap(app *Application) map[string]any {
	var cfg any
	_ = json.Unmarshal([]byte(app.Config), &cfg)
	return map[string]any{
		"ApplicationName":                     app.Name,
		"ApplicationARN":                      app.ARN,
		"ApplicationStatus":                   app.Status,
		"RuntimeEnvironment":                  app.Runtime,
		"ServiceExecutionRole":                app.ServiceRole,
		"ApplicationDescription":              app.Description,
		"ApplicationVersionId":                app.VersionID,
		"ApplicationConfigurationDescription": cfg,
		"CreateTimestamp":                     app.CreatedAt.Unix(),
		"LastUpdateTimestamp":                 app.UpdatedAt.Unix(),
	}
}

func snapshotToMap(snap *ApplicationSnapshot) map[string]any {
	return map[string]any{
		"SnapshotName":              snap.Name,
		"ApplicationVersionId":      int64(1),
		"SnapshotStatus":            snap.Status,
		"SnapshotCreationTimestamp": snap.CreatedAt.Unix(),
	}
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func sqlite_isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// --- operations ---

func (p *Provider) createApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	runtime, _ := params["RuntimeEnvironment"].(string)
	if runtime == "" {
		runtime = "FLINK-1_18"
	}
	serviceRole, _ := params["ServiceExecutionRole"].(string)
	description, _ := params["ApplicationDescription"].(string)
	config := "{}"
	if cfg, ok := params["ApplicationConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	arn := shared.BuildARN("kinesisanalytics", "application", name)
	app, err := p.store.CreateApplication(name, arn, runtime, serviceRole, description, config)
	if err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("ResourceInUseException", "application already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		_ = p.store.tags.AddTags(app.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationDetail": appToMap(app),
	})
}

func (p *Provider) describeApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationDetail": appToMap(app),
	})
}

func (p *Provider) listApplications(_ map[string]any) (*plugin.Response, error) {
	apps, err := p.store.ListApplications()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		summaries = append(summaries, map[string]any{
			"ApplicationName":    a.Name,
			"ApplicationARN":     a.ARN,
			"ApplicationStatus":  a.Status,
			"RuntimeEnvironment": a.Runtime,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationSummaries": summaries,
		"NextToken":            nil,
	})
}

func (p *Provider) deleteApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(app.ARN)
	if err := p.store.DeleteApplication(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) updateApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	config := "{}"
	if cfg, ok := params["ApplicationConfigurationUpdate"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	if err := p.store.UpdateApplicationConfig(name, config); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationDetail": appToMap(app),
	})
}

func (p *Provider) startApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateApplicationStatus(name, "RUNNING"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateApplicationStatus(name, "READY"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) createApplicationSnapshot(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["ApplicationName"].(string)
	snapName, _ := params["SnapshotName"].(string)
	if appName == "" || snapName == "" {
		return shared.JSONError("ValidationException", "ApplicationName and SnapshotName are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(appName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	if _, err := p.store.CreateSnapshot(snapName, appName); err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("ResourceInUseException", "snapshot already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeApplicationSnapshot(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["ApplicationName"].(string)
	snapName, _ := params["SnapshotName"].(string)
	if appName == "" || snapName == "" {
		return shared.JSONError("ValidationException", "ApplicationName and SnapshotName are required", http.StatusBadRequest), nil
	}
	snap, err := p.store.GetSnapshot(appName, snapName)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotDetails": snapshotToMap(snap),
	})
}

func (p *Provider) listApplicationSnapshots(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["ApplicationName"].(string)
	if appName == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	snaps, err := p.store.ListSnapshots(appName)
	if err != nil {
		return nil, err
	}
	details := make([]map[string]any, 0, len(snaps))
	for _, sn := range snaps {
		details = append(details, snapshotToMap(&sn))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SnapshotSummaries": details,
		"NextToken":         nil,
	})
}

func (p *Provider) deleteApplicationSnapshot(params map[string]any) (*plugin.Response, error) {
	appName, _ := params["ApplicationName"].(string)
	snapName, _ := params["SnapshotName"].(string)
	if appName == "" || snapName == "" {
		return shared.JSONError("ValidationException", "ApplicationName and SnapshotName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSnapshot(appName, snapName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "snapshot not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) createApplicationPresignedUrl(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if _, err := p.store.GetApplication(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AuthorizedUrl": fmt.Sprintf("https://devcloud.local/kinesisanalyticsv2/presigned/%s?token=%s", name, shared.GenerateUUID()),
	})
}

func (p *Provider) describeApplicationVersion(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationVersionDetail": appToMap(app),
	})
}

func (p *Provider) listApplicationVersions(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	versions := []map[string]any{
		{
			"ApplicationVersionId": app.VersionID,
			"ApplicationStatus":    app.Status,
		},
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationVersionSummaries": versions,
		"NextToken":                   nil,
	})
}

func (p *Provider) describeApplicationOperation(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	operationId, _ := params["OperationId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationOperationInfoDetails": map[string]any{
			"OperationId":     operationId,
			"OperationStatus": "SUCCESSFUL",
		},
	})
}

func (p *Provider) listApplicationOperations(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationOperationInfoList": []any{},
		"NextToken":                    nil,
	})
}

func (p *Provider) discoverInputSchema(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"InputSchema": map[string]any{
			"RecordFormat": map[string]any{
				"RecordFormatType": "JSON",
			},
			"RecordColumns": []any{},
		},
	})
}

func (p *Provider) rollbackApplication(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationDetail": appToMap(app),
	})
}

func (p *Provider) updateApplicationMaintenanceConfiguration(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetApplication(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationName": name,
		"ApplicationMaintenanceConfigurationDescription": map[string]any{},
	})
}

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagKeys"].([]any)
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
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags": tagList,
	})
}

func (p *Provider) appSubConfigOperation(action string, params map[string]any) (*plugin.Response, error) {
	name, _ := params["ApplicationName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "ApplicationName is required", http.StatusBadRequest), nil
	}
	app, err := p.store.GetApplication(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "application not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ApplicationARN":       app.ARN,
		"ApplicationVersionId": app.VersionID,
	})
}
