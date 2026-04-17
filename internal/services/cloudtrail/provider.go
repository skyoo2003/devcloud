// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudtrail/provider.go
package cloudtrail

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

// Provider implements the CloudTrail_20131101 service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "cloudtrail" }
func (p *Provider) ServiceName() string           { return "CloudTrail_20131101" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "cloudtrail"))
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
	// Trail
	case "CreateTrail":
		return p.createTrail(params)
	case "GetTrail":
		return p.getTrail(params)
	case "DescribeTrails":
		return p.describeTrails(params)
	case "ListTrails":
		return p.listTrails(params)
	case "UpdateTrail":
		return p.updateTrail(params)
	case "DeleteTrail":
		return p.deleteTrail(params)
	case "StartLogging":
		return p.startLogging(params)
	case "StopLogging":
		return p.stopLogging(params)
	case "GetTrailStatus":
		return p.getTrailStatus(params)
	case "GetEventSelectors":
		return p.getEventSelectors(params)
	case "PutEventSelectors":
		return p.putEventSelectors(params)
	case "GetInsightSelectors":
		return p.getInsightSelectors(params)
	case "PutInsightSelectors":
		return p.putInsightSelectors(params)
	// EventDataStore
	case "CreateEventDataStore":
		return p.createEventDataStore(params)
	case "GetEventDataStore":
		return p.getEventDataStore(params)
	case "ListEventDataStores":
		return p.listEventDataStores(params)
	case "UpdateEventDataStore":
		return p.updateEventDataStore(params)
	case "DeleteEventDataStore":
		return p.deleteEventDataStore(params)
	case "RestoreEventDataStore":
		return p.restoreEventDataStore(params)
	case "StartEventDataStoreIngestion":
		return p.startEventDataStoreIngestion(params)
	case "StopEventDataStoreIngestion":
		return p.stopEventDataStoreIngestion(params)
	// Channel
	case "CreateChannel":
		return p.createChannel(params)
	case "GetChannel":
		return p.getChannel(params)
	case "ListChannels":
		return p.listChannels(params)
	case "UpdateChannel":
		return p.updateChannel(params)
	case "DeleteChannel":
		return p.deleteChannel(params)
	// Dashboard
	case "CreateDashboard":
		return p.createDashboard(params)
	case "GetDashboard":
		return p.getDashboard(params)
	case "ListDashboards":
		return p.listDashboards(params)
	case "UpdateDashboard":
		return p.updateDashboard(params)
	case "DeleteDashboard":
		return p.deleteDashboard(params)
	case "StartDashboardRefresh":
		return p.startDashboardRefresh(params)
	// Tags
	case "AddTags":
		return p.addTags(params)
	case "RemoveTags":
		return p.removeTags(params)
	case "ListTags":
		return p.listTags(params)
	// Resource policy
	case "GetResourcePolicy":
		return p.getResourcePolicy(params)
	case "PutResourcePolicy":
		return p.putResourcePolicy(params)
	case "DeleteResourcePolicy":
		return p.deleteResourcePolicy(params)
	// Query
	case "LookupEvents":
		return p.lookupEvents(params)
	case "StartQuery":
		return p.startQuery(params)
	case "DescribeQuery":
		return p.describeQuery(params)
	case "GetQueryResults":
		return p.getQueryResults(params)
	case "ListQueries":
		return p.listQueries(params)
	case "CancelQuery":
		return p.cancelQuery(params)
	// Remaining ops: return success/empty
	case "DeregisterOrganizationDelegatedAdmin", "RegisterOrganizationDelegatedAdmin",
		"DisableFederation", "EnableFederation", "GenerateQuery",
		"GetEventConfiguration", "PutEventConfiguration",
		"GetImport", "StartImport", "StopImport",
		"ListImportFailures", "ListImports",
		"ListInsightsData", "ListInsightsMetricData",
		"ListPublicKeys", "SearchSampleQueries":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	trails, err := p.store.ListTrails()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(trails))
	for _, t := range trails {
		res = append(res, plugin.Resource{Type: "trail", ID: t.ARN, Name: t.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Trail handlers ----

func (p *Provider) createTrail(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterCombinationException", "Name is required", http.StatusBadRequest), nil
	}
	s3Bucket, _ := params["S3BucketName"].(string)
	s3Prefix, _ := params["S3KeyPrefix"].(string)
	logGroup, _ := params["CloudWatchLogsLogGroupArn"].(string)
	snsTopic, _ := params["SnsTopicName"].(string)
	multiRegion, _ := params["IsMultiRegionTrail"].(bool)
	orgTrail, _ := params["IsOrganizationTrail"].(bool)

	arn := shared.BuildARN("cloudtrail", "trail", name)
	trail, err := p.store.CreateTrail(name, arn, s3Bucket, s3Prefix, logGroup, snsTopic, multiRegion, orgTrail)
	if err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("TrailAlreadyExistsException", "trail already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["TagsList"].([]any); ok {
		p.store.tags.AddTags(trail.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, trailToMap(trail))
}

func (p *Provider) getTrail(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterCombinationException", "Name is required", http.StatusBadRequest), nil
	}
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Trail": trailToMap(trail)})
}

func (p *Provider) describeTrails(params map[string]any) (*plugin.Response, error) {
	trails, err := p.store.ListTrails()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(trails))
	for _, t := range trails {
		list = append(list, trailToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"trailList": list})
}

func (p *Provider) listTrails(params map[string]any) (*plugin.Response, error) {
	trails, err := p.store.ListTrails()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(trails))
	for _, t := range trails {
		list = append(list, map[string]any{
			"TrailARN":   t.ARN,
			"Name":       t.Name,
			"HomeRegion": shared.DefaultRegion,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Trails": list})
}

func (p *Provider) updateTrail(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterCombinationException", "Name is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	s3Bucket := strOrDefault(params, "S3BucketName", existing.S3Bucket)
	s3Prefix := strOrDefault(params, "S3KeyPrefix", existing.S3Prefix)
	logGroup := strOrDefault(params, "CloudWatchLogsLogGroupArn", existing.LogGroup)
	snsTopic := strOrDefault(params, "SnsTopicName", existing.SNSTopic)
	multiRegion := boolOrDefault(params, "IsMultiRegionTrail", existing.IsMultiRegion)
	orgTrail := boolOrDefault(params, "IsOrganizationTrail", existing.IsOrgTrail)

	trail, err := p.store.UpdateTrail(name, s3Bucket, s3Prefix, logGroup, snsTopic, multiRegion, orgTrail)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, trailToMap(trail))
}

func (p *Provider) deleteTrail(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterCombinationException", "Name is required", http.StatusBadRequest), nil
	}
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(trail.ARN)
	if err := p.store.DeleteTrail(name); err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startLogging(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if err := p.store.SetLogging(name, true); err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopLogging(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if err := p.store.SetLogging(name, false); err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) getTrailStatus(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"IsLogging":                         trail.IsLogging,
		"LatestDeliveryError":               "",
		"LatestNotificationError":           "",
		"LatestDeliveryTime":                nil,
		"LatestNotificationTime":            nil,
		"StartLoggingTime":                  trail.CreatedAt.Unix(),
		"StopLoggingTime":                   nil,
		"LatestCloudWatchLogsDeliveryError": "",
		"LatestCloudWatchLogsDeliveryTime":  nil,
	})
}

func (p *Provider) getEventSelectors(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TrailName"].(string)
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	selectors, advanced, err := p.store.GetEventSelectors(trail.ARN)
	if err != nil {
		return nil, err
	}
	var sel any
	var adv any
	json.Unmarshal([]byte(selectors), &sel)
	json.Unmarshal([]byte(advanced), &adv)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrailARN":               trail.ARN,
		"EventSelectors":         sel,
		"AdvancedEventSelectors": adv,
	})
}

func (p *Provider) putEventSelectors(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TrailName"].(string)
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	selectors := marshalOrEmpty(params["EventSelectors"])
	advanced := marshalOrEmpty(params["AdvancedEventSelectors"])
	if err := p.store.PutEventSelectors(trail.ARN, selectors, advanced); err != nil {
		return nil, err
	}
	var sel any
	var adv any
	json.Unmarshal([]byte(selectors), &sel)
	json.Unmarshal([]byte(advanced), &adv)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrailARN":               trail.ARN,
		"EventSelectors":         sel,
		"AdvancedEventSelectors": adv,
	})
}

func (p *Provider) getInsightSelectors(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TrailName"].(string)
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	selectors, err := p.store.GetInsightSelectors(trail.ARN)
	if err != nil {
		return nil, err
	}
	var sel any
	json.Unmarshal([]byte(selectors), &sel)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrailARN":         trail.ARN,
		"InsightSelectors": sel,
	})
}

func (p *Provider) putInsightSelectors(params map[string]any) (*plugin.Response, error) {
	name, _ := params["TrailName"].(string)
	trail, err := p.store.GetTrail(name)
	if err != nil {
		return shared.JSONError("TrailNotFoundException", "trail not found", http.StatusBadRequest), nil
	}
	selectors := marshalOrEmpty(params["InsightSelectors"])
	if err := p.store.PutInsightSelectors(trail.ARN, selectors); err != nil {
		return nil, err
	}
	var sel any
	json.Unmarshal([]byte(selectors), &sel)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TrailARN":         trail.ARN,
		"InsightSelectors": sel,
	})
}

// ---- EventDataStore handlers ----

func (p *Provider) createEventDataStore(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	retention := int(floatOrDefault(params, "RetentionPeriod", 2555))
	multiRegion := boolOrDefault(params, "MultiRegionEnabled", true)
	orgEnabled := boolOrDefault(params, "OrganizationEnabled", false)

	arn := shared.BuildARN("cloudtrail", "eventdatastore", shared.GenerateUUID())
	eds, err := p.store.CreateEventDataStore(arn, name, retention, multiRegion, orgEnabled)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["TagsList"].([]any); ok {
		p.store.tags.AddTags(eds.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, edsToMap(eds))
}

func (p *Provider) getEventDataStore(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "EventDataStore is required", http.StatusBadRequest), nil
	}
	eds, err := p.store.GetEventDataStore(arn)
	if err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, edsToMap(eds))
}

func (p *Provider) listEventDataStores(params map[string]any) (*plugin.Response, error) {
	stores, err := p.store.ListEventDataStores()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(stores))
	for _, e := range stores {
		list = append(list, edsToMap(&e))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"EventDataStores": list})
}

func (p *Provider) updateEventDataStore(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "EventDataStore is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetEventDataStore(arn)
	if err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	retention := int(floatOrDefault(params, "RetentionPeriod", float64(existing.Retention)))
	multiRegion := boolOrDefault(params, "MultiRegionEnabled", existing.MultiRegion)
	orgEnabled := boolOrDefault(params, "OrganizationEnabled", existing.OrgEnabled)
	eds, err := p.store.UpdateEventDataStore(arn, retention, multiRegion, orgEnabled)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, edsToMap(eds))
}

func (p *Provider) deleteEventDataStore(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	eds, err := p.store.GetEventDataStore(arn)
	if err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(eds.ARN)
	if err := p.store.DeleteEventDataStore(arn); err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) restoreEventDataStore(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	if err := p.store.SetEventDataStoreStatus(arn, "ENABLED"); err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	eds, _ := p.store.GetEventDataStore(arn)
	return shared.JSONResponse(http.StatusOK, edsToMap(eds))
}

func (p *Provider) startEventDataStoreIngestion(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	if err := p.store.SetEventDataStoreStatus(arn, "ENABLED"); err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopEventDataStoreIngestion(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["EventDataStore"].(string)
	if err := p.store.SetEventDataStoreStatus(arn, "STOPPED_INGESTION"); err != nil {
		return shared.JSONError("EventDataStoreNotFoundException", "event data store not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Channel handlers ----

func (p *Provider) createChannel(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	source, _ := params["Source"].(string)
	destinations := marshalOrEmpty(params["Destinations"])
	arn := shared.BuildARN("cloudtrail", "channel", shared.GenerateUUID())
	ch, err := p.store.CreateChannel(arn, name, source, destinations)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["Tags"].([]any); ok {
		p.store.tags.AddTags(ch.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, channelToMap(ch))
}

func (p *Provider) getChannel(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Channel"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "Channel is required", http.StatusBadRequest), nil
	}
	ch, err := p.store.GetChannel(arn)
	if err != nil {
		return shared.JSONError("ChannelNotFoundException", "channel not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, channelToMap(ch))
}

func (p *Provider) listChannels(params map[string]any) (*plugin.Response, error) {
	channels, err := p.store.ListChannels()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(channels))
	for _, c := range channels {
		list = append(list, map[string]any{
			"ChannelArn": c.ARN,
			"Name":       c.Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Channels": list})
}

func (p *Provider) updateChannel(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Channel"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "Channel is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetChannel(arn)
	if err != nil {
		return shared.JSONError("ChannelNotFoundException", "channel not found", http.StatusBadRequest), nil
	}
	name := strOrDefault(params, "Name", existing.Name)
	destinations := marshalOrEmptyDefault(params["Destinations"], existing.Destinations)
	ch, err := p.store.UpdateChannel(arn, name, destinations)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, channelToMap(ch))
}

func (p *Provider) deleteChannel(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["Channel"].(string)
	ch, err := p.store.GetChannel(arn)
	if err != nil {
		return shared.JSONError("ChannelNotFoundException", "channel not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(ch.ARN)
	if err := p.store.DeleteChannel(arn); err != nil {
		return shared.JSONError("ChannelNotFoundException", "channel not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Dashboard handlers ----

func (p *Provider) createDashboard(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("InvalidParameterException", "Name is required", http.StatusBadRequest), nil
	}
	dashType := "CUSTOM"
	widgets := marshalOrEmpty(params["Widgets"])
	arn := shared.BuildARN("cloudtrail", "dashboard", shared.GenerateUUID())
	dash, err := p.store.CreateDashboard(arn, name, dashType, widgets)
	if err != nil {
		if sqlite_isUnique(err) {
			return shared.JSONError("ConflictException", "dashboard already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["TagsList"].([]any); ok {
		p.store.tags.AddTags(dash.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, dashboardToMap(dash))
}

func (p *Provider) getDashboard(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["DashboardId"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "DashboardId is required", http.StatusBadRequest), nil
	}
	dash, err := p.store.GetDashboard(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "dashboard not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, dashboardToMap(dash))
}

func (p *Provider) listDashboards(params map[string]any) (*plugin.Response, error) {
	dashboards, err := p.store.ListDashboards()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		list = append(list, map[string]any{
			"DashboardArn": d.ARN,
			"Name":         d.Name,
			"Type":         d.Type,
			"Status":       d.Status,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Dashboards": list})
}

func (p *Provider) updateDashboard(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["DashboardId"].(string)
	if arn == "" {
		return shared.JSONError("InvalidParameterException", "DashboardId is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetDashboard(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "dashboard not found", http.StatusBadRequest), nil
	}
	widgets := marshalOrEmptyDefault(params["Widgets"], existing.Widgets)
	dash, err := p.store.UpdateDashboard(arn, widgets)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, dashboardToMap(dash))
}

func (p *Provider) deleteDashboard(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["DashboardId"].(string)
	dash, err := p.store.GetDashboard(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "dashboard not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(dash.ARN)
	if err := p.store.DeleteDashboard(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "dashboard not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) startDashboardRefresh(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["DashboardId"].(string)
	if _, err := p.store.GetDashboard(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "dashboard not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RefreshId": shared.GenerateUUID()})
}

// ---- Tags handlers ----

func (p *Provider) addTags(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("InvalidParameterException", "ResourceId is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["TagsList"].([]any)
	if err := p.store.tags.AddTags(resourceID, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) removeTags(params map[string]any) (*plugin.Response, error) {
	resourceID, _ := params["ResourceId"].(string)
	if resourceID == "" {
		return shared.JSONError("InvalidParameterException", "ResourceId is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["TagsList"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, t := range rawKeys {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		if k != "" {
			keys = append(keys, k)
		}
	}
	if err := p.store.tags.RemoveTags(resourceID, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTags(params map[string]any) (*plugin.Response, error) {
	rawARNs, _ := params["ResourceIdList"].([]any)
	result := make([]map[string]any, 0, len(rawARNs))
	for _, r := range rawARNs {
		arn, _ := r.(string)
		tags, err := p.store.tags.ListTags(arn)
		if err != nil {
			return nil, err
		}
		tagList := make([]map[string]string, 0, len(tags))
		for k, v := range tags {
			tagList = append(tagList, map[string]string{"Key": k, "Value": v})
		}
		result = append(result, map[string]any{
			"ResourceId": arn,
			"TagsList":   tagList,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ResourceTagList": result})
}

// ---- Resource policy handlers ----

func (p *Provider) getResourcePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	policy, err := p.store.GetResourcePolicy(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceArn":    arn,
		"ResourcePolicy": policy,
	})
}

func (p *Provider) putResourcePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	policy, _ := params["ResourcePolicy"].(string)
	if err := p.store.PutResourcePolicy(arn, policy); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceArn":    arn,
		"ResourcePolicy": policy,
	})
}

func (p *Provider) deleteResourcePolicy(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if err := p.store.DeleteResourcePolicy(arn); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Query handlers ----

func (p *Provider) lookupEvents(params map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Events":    []any{},
		"NextToken": nil,
	})
}

func (p *Provider) startQuery(params map[string]any) (*plugin.Response, error) {
	querySQL, _ := params["QueryStatement"].(string)
	queryID := shared.GenerateUUID()
	if _, err := p.store.CreateQuery(queryID, querySQL); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"QueryId": queryID})
}

func (p *Provider) describeQuery(params map[string]any) (*plugin.Response, error) {
	queryID, _ := params["QueryId"].(string)
	q, err := p.store.GetQuery(queryID)
	if err != nil {
		return shared.JSONError("QueryIdNotFoundException", "query not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryId":      q.QueryID,
		"QueryString":  q.QuerySQL,
		"QueryStatus":  q.Status,
		"CreationTime": q.CreatedAt.Unix(),
	})
}

func (p *Provider) getQueryResults(params map[string]any) (*plugin.Response, error) {
	queryID, _ := params["QueryId"].(string)
	if _, err := p.store.GetQuery(queryID); err != nil {
		return shared.JSONError("QueryIdNotFoundException", "query not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryStatus":     "FINISHED",
		"QueryResultRows": []any{},
		"QueryStatistics": map[string]any{
			"ResultsScanned": 0,
			"BytesScanned":   0,
		},
	})
}

func (p *Provider) listQueries(params map[string]any) (*plugin.Response, error) {
	queries, err := p.store.ListQueries()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(queries))
	for _, q := range queries {
		list = append(list, map[string]any{
			"QueryId":      q.QueryID,
			"QueryStatus":  q.Status,
			"CreationTime": q.CreatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Queries": list})
}

func (p *Provider) cancelQuery(params map[string]any) (*plugin.Response, error) {
	queryID, _ := params["QueryId"].(string)
	if err := p.store.CancelQuery(queryID); err != nil {
		return shared.JSONError("QueryIdNotFoundException", "query not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"QueryId":     queryID,
		"QueryStatus": "CANCELLED",
	})
}

// ---- Serializers ----

func trailToMap(t *Trail) map[string]any {
	return map[string]any{
		"Name":                      t.Name,
		"TrailARN":                  t.ARN,
		"S3BucketName":              t.S3Bucket,
		"S3KeyPrefix":               t.S3Prefix,
		"IsMultiRegionTrail":        t.IsMultiRegion,
		"IsOrganizationTrail":       t.IsOrgTrail,
		"HasCustomEventSelectors":   false,
		"HasInsightSelectors":       false,
		"LogFileValidationEnabled":  false,
		"CloudWatchLogsLogGroupArn": t.LogGroup,
		"SnsTopicName":              t.SNSTopic,
		"HomeRegion":                shared.DefaultRegion,
	}
}

func edsToMap(e *EventDataStore) map[string]any {
	return map[string]any{
		"EventDataStoreArn":   e.ARN,
		"Name":                e.Name,
		"Status":              e.Status,
		"RetentionPeriod":     e.Retention,
		"MultiRegionEnabled":  e.MultiRegion,
		"OrganizationEnabled": e.OrgEnabled,
		"CreatedTimestamp":    e.CreatedAt.Unix(),
		"UpdatedTimestamp":    e.UpdatedAt.Unix(),
	}
}

func channelToMap(c *Channel) map[string]any {
	var dests any
	json.Unmarshal([]byte(c.Destinations), &dests)
	return map[string]any{
		"ChannelArn":   c.ARN,
		"Name":         c.Name,
		"Source":       c.Source,
		"Destinations": dests,
	}
}

func dashboardToMap(d *Dashboard) map[string]any {
	var widgets any
	json.Unmarshal([]byte(d.Widgets), &widgets)
	return map[string]any{
		"DashboardArn":     d.ARN,
		"Name":             d.Name,
		"Type":             d.Type,
		"Status":           d.Status,
		"Widgets":          widgets,
		"CreatedTimestamp": d.CreatedAt.Unix(),
		"UpdatedTimestamp": d.UpdatedAt.Unix(),
	}
}

// ---- Helpers ----

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

func strOrDefault(params map[string]any, key, def string) string {
	if v, ok := params[key].(string); ok && v != "" {
		return v
	}
	return def
}

func boolOrDefault(params map[string]any, key string, def bool) bool {
	if v, ok := params[key].(bool); ok {
		return v
	}
	return def
}

func floatOrDefault(params map[string]any, key string, def float64) float64 {
	if v, ok := params[key].(float64); ok {
		return v
	}
	return def
}

func marshalOrEmpty(v any) string {
	if v == nil {
		return "[]"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func marshalOrEmptyDefault(v any, def string) string {
	if v == nil {
		return def
	}
	b, _ := json.Marshal(v)
	return string(b)
}
