// SPDX-License-Identifier: Apache-2.0

// internal/services/pinpoint/provider.go
package pinpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the Pinpoint service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "pinpoint" }
func (p *Provider) ServiceName() string           { return "Pinpoint" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "pinpoint"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
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

	path := req.URL.Path

	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// ── App ──────────────────────────────────────────────────────────────────
	case "CreateApp":
		return p.createApp(params)
	case "GetApp":
		appID := pathSegment(path, "apps", 1)
		return p.getApp(appID)
	case "GetApps":
		return p.getApps()
	case "DeleteApp":
		appID := pathSegment(path, "apps", 1)
		return p.deleteApp(appID)

	// ── Campaign ─────────────────────────────────────────────────────────────
	case "CreateCampaign":
		appID := pathSegment(path, "apps", 1)
		return p.createCampaign(appID, params)
	case "GetCampaign":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.getCampaign(appID, campaignID)
	case "GetCampaigns":
		appID := pathSegment(path, "apps", 1)
		return p.getCampaigns(appID)
	case "UpdateCampaign":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.updateCampaign(appID, campaignID, params)
	case "DeleteCampaign":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.deleteCampaign(appID, campaignID)
	case "GetCampaignActivities":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.getCampaignActivities(appID, campaignID)
	case "GetCampaignVersion":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.getCampaignVersion(appID, campaignID)
	case "GetCampaignVersions":
		appID, campaignID := twoSegments(path, "apps", "campaigns")
		return p.getCampaignVersions(appID, campaignID)

	// ── Segment ──────────────────────────────────────────────────────────────
	case "CreateSegment":
		appID := pathSegment(path, "apps", 1)
		return p.createSegment(appID, params)
	case "GetSegment":
		appID, segmentID := twoSegments(path, "apps", "segments")
		return p.getSegment(appID, segmentID)
	case "GetSegments":
		appID := pathSegment(path, "apps", 1)
		return p.getSegments(appID)
	case "UpdateSegment":
		appID, segmentID := twoSegments(path, "apps", "segments")
		return p.updateSegment(appID, segmentID, params)
	case "DeleteSegment":
		appID, segmentID := twoSegments(path, "apps", "segments")
		return p.deleteSegment(appID, segmentID)
	case "GetSegmentVersion":
		appID, segmentID := twoSegments(path, "apps", "segments")
		return p.getSegmentVersion(appID, segmentID)
	case "GetSegmentVersions":
		appID, segmentID := twoSegments(path, "apps", "segments")
		return p.getSegmentVersions(appID, segmentID)

	// ── Journey ──────────────────────────────────────────────────────────────
	case "CreateJourney":
		appID := pathSegment(path, "apps", 1)
		return p.createJourney(appID, params)
	case "GetJourney":
		appID, journeyID := twoSegments(path, "apps", "journeys")
		return p.getJourney(appID, journeyID)
	case "ListJourneys":
		appID := pathSegment(path, "apps", 1)
		return p.listJourneys(appID)
	case "UpdateJourney":
		appID, journeyID := twoSegments(path, "apps", "journeys")
		return p.updateJourney(appID, journeyID, params)
	case "UpdateJourneyState":
		appID, journeyID := twoSegments(path, "apps", "journeys")
		return p.updateJourneyState(appID, journeyID, params)
	case "DeleteJourney":
		appID, journeyID := twoSegments(path, "apps", "journeys")
		return p.deleteJourney(appID, journeyID)

	// ── Email Template ────────────────────────────────────────────────────────
	case "CreateEmailTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.createTemplate(tmplName, "email", params)
	case "GetEmailTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.getTemplate(tmplName, "email")
	case "UpdateEmailTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.updateTemplate(tmplName, "email", params)
	case "DeleteEmailTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.deleteTemplate(tmplName, "email")

	// ── SMS Template ──────────────────────────────────────────────────────────
	case "CreateSmsTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.createTemplate(tmplName, "sms", params)
	case "GetSmsTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.getTemplate(tmplName, "sms")
	case "UpdateSmsTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.updateTemplate(tmplName, "sms", params)
	case "DeleteSmsTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.deleteTemplate(tmplName, "sms")

	// ── Push Template ─────────────────────────────────────────────────────────
	case "CreatePushTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.createTemplate(tmplName, "push", params)
	case "GetPushTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.getTemplate(tmplName, "push")
	case "UpdatePushTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.updateTemplate(tmplName, "push", params)
	case "DeletePushTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.deleteTemplate(tmplName, "push")

	// ── Voice Template ────────────────────────────────────────────────────────
	case "CreateVoiceTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.createTemplate(tmplName, "voice", params)
	case "GetVoiceTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.getTemplate(tmplName, "voice")
	case "UpdateVoiceTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.updateTemplate(tmplName, "voice", params)
	case "DeleteVoiceTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.deleteTemplate(tmplName, "voice")

	// ── InApp Template ────────────────────────────────────────────────────────
	case "CreateInAppTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.createTemplate(tmplName, "inapp", params)
	case "GetInAppTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.getTemplate(tmplName, "inapp")
	case "UpdateInAppTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.updateTemplate(tmplName, "inapp", params)
	case "DeleteInAppTemplate":
		tmplName := pathSegment(path, "templates", 1)
		return p.deleteTemplate(tmplName, "inapp")

	// ── Template list / version ───────────────────────────────────────────────
	case "ListTemplates":
		return p.listTemplates()
	case "ListTemplateVersions":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TemplateVersionsResponse": map[string]any{"Item": []any{}, "NextToken": ""},
		})
	case "UpdateTemplateActiveVersion":
		return shared.JSONResponse(http.StatusOK, map[string]any{"MessageBody": map[string]any{"Message": "ok"}})

	// ── Tags ──────────────────────────────────────────────────────────────────
	case "TagResource":
		arn := tagARN(path)
		return p.tagResource(arn, params)
	case "UntagResource":
		arn := tagARN(path)
		tagKeys := req.URL.Query()["tagKeys"]
		return p.untagResource(arn, tagKeys)
	case "ListTagsForResource":
		arn := tagARN(path)
		return p.listTagsForResource(arn)

	// ── Messaging ─────────────────────────────────────────────────────────────
	case "SendMessages":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ApplicationId":  pathSegment(path, "apps", 1),
			"EndpointResult": map[string]any{},
			"RequestId":      generateID(),
			"Result":         map[string]any{},
		})
	case "SendUsersMessages":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ApplicationId": pathSegment(path, "apps", 1),
			"RequestId":     generateID(),
			"Result":        map[string]any{},
		})
	case "PutEvents":
		return shared.JSONResponse(http.StatusAccepted, map[string]any{"Results": map[string]any{}})

	// ── Stub: Channels ────────────────────────────────────────────────────────
	case "GetAdmChannel", "GetApnsChannel", "GetApnsSandboxChannel",
		"GetApnsVoipChannel", "GetApnsVoipSandboxChannel",
		"GetBaiduChannel", "GetEmailChannel", "GetGcmChannel",
		"GetSmsChannel", "GetVoiceChannel":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	case "UpdateAdmChannel", "UpdateApnsChannel", "UpdateApnsSandboxChannel",
		"UpdateApnsVoipChannel", "UpdateApnsVoipSandboxChannel",
		"UpdateBaiduChannel", "UpdateEmailChannel", "UpdateGcmChannel",
		"UpdateSmsChannel", "UpdateVoiceChannel":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	case "DeleteAdmChannel", "DeleteApnsChannel", "DeleteApnsSandboxChannel",
		"DeleteApnsVoipChannel", "DeleteApnsVoipSandboxChannel",
		"DeleteBaiduChannel", "DeleteEmailChannel", "DeleteGcmChannel",
		"DeleteSmsChannel", "DeleteVoiceChannel":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	case "GetChannels":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ChannelsResponse": map[string]any{"Channels": map[string]any{}},
		})

	// ── Stub: Endpoints ───────────────────────────────────────────────────────
	case "GetEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointResponse": map[string]any{}})
	case "UpdateEndpoint", "UpdateEndpointsBatch":
		return shared.JSONResponse(http.StatusAccepted, map[string]any{"MessageBody": map[string]any{"Message": "ok"}})
	case "DeleteEndpoint":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointResponse": map[string]any{}})
	case "GetUserEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointsResponse": map[string]any{"Item": []any{}}})
	case "DeleteUserEndpoints":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EndpointsResponse": map[string]any{"Item": []any{}}})

	// ── Stub: EventStream ─────────────────────────────────────────────────────
	case "PutEventStream", "GetEventStream":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EventStream": map[string]any{}})
	case "DeleteEventStream":
		return shared.JSONResponse(http.StatusOK, map[string]any{"EventStream": map[string]any{}})

	// ── Stub: Import/Export Jobs ──────────────────────────────────────────────
	case "CreateImportJob", "GetImportJob":
		return shared.JSONResponse(http.StatusCreated, map[string]any{"ImportJobResponse": map[string]any{}})
	case "GetImportJobs", "GetSegmentImportJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ImportJobsResponse": map[string]any{"Item": []any{}}})
	case "CreateExportJob", "GetExportJob":
		return shared.JSONResponse(http.StatusCreated, map[string]any{"ExportJobResponse": map[string]any{}})
	case "GetExportJobs", "GetSegmentExportJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ExportJobsResponse": map[string]any{"Item": []any{}}})

	// ── Stub: Recommender ─────────────────────────────────────────────────────
	case "CreateRecommenderConfiguration", "GetRecommenderConfiguration", "UpdateRecommenderConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RecommenderConfigurationResponse": map[string]any{}})
	case "GetRecommenderConfigurations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ListRecommenderConfigurationsResponse": map[string]any{"Item": []any{}}})
	case "DeleteRecommenderConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RecommenderConfigurationResponse": map[string]any{}})

	// ── Stub: Application Settings ────────────────────────────────────────────
	case "GetApplicationSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationSettingsResource": map[string]any{}})
	case "UpdateApplicationSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ApplicationSettingsResource": map[string]any{}})

	// ── Stub: KPI ─────────────────────────────────────────────────────────────
	case "GetApplicationDateRangeKpi", "GetCampaignDateRangeKpi", "GetJourneyDateRangeKpi":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// ── Stub: Journey metrics/runs ────────────────────────────────────────────
	case "GetJourneyExecutionMetrics", "GetJourneyExecutionActivityMetrics",
		"GetJourneyRunExecutionMetrics", "GetJourneyRunExecutionActivityMetrics":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetJourneyRuns":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JourneyRunsResponse": map[string]any{"Item": []any{}}})

	// ── Stub: OTP / Verify ────────────────────────────────────────────────────
	case "SendOTPMessage":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "VerifyOTPMessage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Valid": true})

	// ── Stub: Misc ────────────────────────────────────────────────────────────
	case "PhoneNumberValidate":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RemoveAttributes":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetInAppMessages":
		return shared.JSONResponse(http.StatusOK, map[string]any{"InAppMessageCampaigns": []any{}})

	default:
		return shared.JSONError("BadRequestException", fmt.Sprintf("unknown operation: %s", op), http.StatusBadRequest), nil
	}
}

// ── App handlers ──────────────────────────────────────────────────────────────

func (p *Provider) createApp(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		name = strParam(params, "name")
	}
	if name == "" {
		return shared.JSONError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	id := generateID()
	arn := fmt.Sprintf("arn:aws:mobiletargeting:us-east-1:000000000000:apps/%s", id)
	app, err := p.store.CreateApp(id, arn, name)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusCreated, appToMap(app))
}

func (p *Provider) getApp(id string) (*plugin.Response, error) {
	app, err := p.store.GetApp(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "App not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, appToMap(app))
}

func (p *Provider) getApps() (*plugin.Response, error) {
	apps, err := p.store.ListApps()
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	items := make([]map[string]any, 0, len(apps))
	for i := range apps {
		items = append(items, appToMap(&apps[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": items})
}

func (p *Provider) deleteApp(id string) (*plugin.Response, error) {
	app, err := p.store.GetApp(id)
	if err != nil {
		return shared.JSONError("NotFoundException", "App not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteApp(id); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, appToMap(app))
}

func appToMap(a *App) map[string]any {
	return map[string]any{
		"Id":           a.ID,
		"Arn":          a.ARN,
		"Name":         a.Name,
		"CreationDate": a.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ── Campaign handlers ─────────────────────────────────────────────────────────

func (p *Provider) createCampaign(appID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description := strParam(params, "Description")
	segmentID := strParam(params, "SegmentId")
	id := generateID()
	arn := fmt.Sprintf("arn:aws:mobiletargeting:us-east-1:000000000000:apps/%s/campaigns/%s", appID, id)
	c, err := p.store.CreateCampaign(id, appID, arn, name, description, segmentID)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusCreated, campaignToMap(c))
}

func (p *Provider) getCampaign(appID, campaignID string) (*plugin.Response, error) {
	c, err := p.store.GetCampaign(appID, campaignID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, campaignToMap(c))
}

func (p *Provider) getCampaigns(appID string) (*plugin.Response, error) {
	campaigns, err := p.store.ListCampaigns(appID)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	items := make([]map[string]any, 0, len(campaigns))
	for i := range campaigns {
		items = append(items, campaignToMap(&campaigns[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": items})
}

func (p *Provider) updateCampaign(appID, campaignID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	description := strParam(params, "Description")
	segmentID := strParam(params, "SegmentId")
	c, err := p.store.UpdateCampaign(appID, campaignID, name, description, segmentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, campaignToMap(c))
}

func (p *Provider) deleteCampaign(appID, campaignID string) (*plugin.Response, error) {
	c, err := p.store.GetCampaign(appID, campaignID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteCampaign(appID, campaignID); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, campaignToMap(c))
}

func (p *Provider) getCampaignActivities(appID, campaignID string) (*plugin.Response, error) {
	if _, err := p.store.GetCampaign(appID, campaignID); err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": []any{}})
}

func (p *Provider) getCampaignVersion(appID, campaignID string) (*plugin.Response, error) {
	c, err := p.store.GetCampaign(appID, campaignID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, campaignToMap(c))
}

func (p *Provider) getCampaignVersions(appID, campaignID string) (*plugin.Response, error) {
	c, err := p.store.GetCampaign(appID, campaignID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Campaign not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": []map[string]any{campaignToMap(c)}})
}

func campaignToMap(c *Campaign) map[string]any {
	return map[string]any{
		"Id":            c.ID,
		"ApplicationId": c.AppID,
		"Arn":           c.ARN,
		"Name":          c.Name,
		"State":         map[string]any{"CampaignStatus": c.State},
		"Description":   c.Description,
		"SegmentId":     c.SegmentID,
		"Version":       c.Version,
		"CreationDate":  c.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ── Segment handlers ──────────────────────────────────────────────────────────

func (p *Provider) createSegment(appID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	segType := strParam(params, "SegmentType")
	if segType == "" {
		segType = "DIMENSIONAL"
	}
	id := generateID()
	arn := fmt.Sprintf("arn:aws:mobiletargeting:us-east-1:000000000000:apps/%s/segments/%s", appID, id)
	seg, err := p.store.CreateSegment(id, appID, arn, name, segType)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusCreated, segmentToMap(seg))
}

func (p *Provider) getSegment(appID, segmentID string) (*plugin.Response, error) {
	seg, err := p.store.GetSegment(appID, segmentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Segment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, segmentToMap(seg))
}

func (p *Provider) getSegments(appID string) (*plugin.Response, error) {
	segs, err := p.store.ListSegments(appID)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	items := make([]map[string]any, 0, len(segs))
	for i := range segs {
		items = append(items, segmentToMap(&segs[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": items})
}

func (p *Provider) updateSegment(appID, segmentID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	seg, err := p.store.UpdateSegment(appID, segmentID, name)
	if err != nil {
		return shared.JSONError("NotFoundException", "Segment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, segmentToMap(seg))
}

func (p *Provider) deleteSegment(appID, segmentID string) (*plugin.Response, error) {
	seg, err := p.store.GetSegment(appID, segmentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Segment not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteSegment(appID, segmentID); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, segmentToMap(seg))
}

func (p *Provider) getSegmentVersion(appID, segmentID string) (*plugin.Response, error) {
	seg, err := p.store.GetSegment(appID, segmentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Segment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, segmentToMap(seg))
}

func (p *Provider) getSegmentVersions(appID, segmentID string) (*plugin.Response, error) {
	seg, err := p.store.GetSegment(appID, segmentID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Segment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": []map[string]any{segmentToMap(seg)}})
}

func segmentToMap(seg *Segment) map[string]any {
	return map[string]any{
		"Id":            seg.ID,
		"ApplicationId": seg.AppID,
		"Arn":           seg.ARN,
		"Name":          seg.Name,
		"SegmentType":   seg.Type,
		"Version":       seg.Version,
		"CreationDate":  seg.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ── Journey handlers ──────────────────────────────────────────────────────────

func (p *Provider) createJourney(appID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	if name == "" {
		return shared.JSONError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	id := generateID()
	arn := fmt.Sprintf("arn:aws:mobiletargeting:us-east-1:000000000000:apps/%s/journeys/%s", appID, id)
	j, err := p.store.CreateJourney(id, appID, arn, name)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusCreated, journeyToMap(j))
}

func (p *Provider) getJourney(appID, journeyID string) (*plugin.Response, error) {
	j, err := p.store.GetJourney(appID, journeyID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Journey not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, journeyToMap(j))
}

func (p *Provider) listJourneys(appID string) (*plugin.Response, error) {
	journeys, err := p.store.ListJourneys(appID)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	items := make([]map[string]any, 0, len(journeys))
	for i := range journeys {
		items = append(items, journeyToMap(&journeys[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": items})
}

func (p *Provider) updateJourney(appID, journeyID string, params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "Name")
	j, err := p.store.UpdateJourney(appID, journeyID, name)
	if err != nil {
		return shared.JSONError("NotFoundException", "Journey not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, journeyToMap(j))
}

func (p *Provider) updateJourneyState(appID, journeyID string, params map[string]any) (*plugin.Response, error) {
	state := strParam(params, "State")
	if state == "" {
		state = "ACTIVE"
	}
	j, err := p.store.UpdateJourneyState(appID, journeyID, state)
	if err != nil {
		return shared.JSONError("NotFoundException", "Journey not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, journeyToMap(j))
}

func (p *Provider) deleteJourney(appID, journeyID string) (*plugin.Response, error) {
	j, err := p.store.GetJourney(appID, journeyID)
	if err != nil {
		return shared.JSONError("NotFoundException", "Journey not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteJourney(appID, journeyID); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, journeyToMap(j))
}

func journeyToMap(j *Journey) map[string]any {
	return map[string]any{
		"Id":               j.ID,
		"ApplicationId":    j.AppID,
		"Arn":              j.ARN,
		"Name":             j.Name,
		"State":            j.State,
		"CreationDate":     j.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		"LastModifiedDate": j.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ── Template handlers ─────────────────────────────────────────────────────────

func (p *Provider) createTemplate(name, tType string, params map[string]any) (*plugin.Response, error) {
	subject := strParam(params, "Subject")
	htmlBody := strParam(params, "HtmlPart")
	if htmlBody == "" {
		htmlBody = strParam(params, "HtmlBody")
	}
	textBody := strParam(params, "TextPart")
	if textBody == "" {
		textBody = strParam(params, "TextBody")
	}
	body := strParam(params, "Body")
	_, err := p.store.CreateTemplate(name, tType, subject, htmlBody, textBody, body)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"Arn":     fmt.Sprintf("arn:aws:mobiletargeting:us-east-1:000000000000:templates/%s/%s", name, tType),
		"Message": "Created",
	})
}

func (p *Provider) getTemplate(name, tType string) (*plugin.Response, error) {
	tmpl, err := p.store.GetTemplate(name, tType)
	if err != nil {
		return shared.JSONError("NotFoundException", "Template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, templateToMap(tmpl))
}

func (p *Provider) updateTemplate(name, tType string, params map[string]any) (*plugin.Response, error) {
	subject := strParam(params, "Subject")
	htmlBody := strParam(params, "HtmlPart")
	if htmlBody == "" {
		htmlBody = strParam(params, "HtmlBody")
	}
	textBody := strParam(params, "TextPart")
	if textBody == "" {
		textBody = strParam(params, "TextBody")
	}
	body := strParam(params, "Body")
	_, err := p.store.UpdateTemplate(name, tType, subject, htmlBody, textBody, body)
	if err != nil {
		return shared.JSONError("NotFoundException", "Template not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Message": "Updated"})
}

func (p *Provider) deleteTemplate(name, tType string) (*plugin.Response, error) {
	_, err := p.store.GetTemplate(name, tType)
	if err != nil {
		return shared.JSONError("NotFoundException", "Template not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteTemplate(name, tType); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Message": "Deleted"})
}

func (p *Provider) listTemplates() (*plugin.Response, error) {
	templates, err := p.store.ListTemplates()
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	items := make([]map[string]any, 0, len(templates))
	for i := range templates {
		items = append(items, templateToMap(&templates[i]))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Item": items})
}

func templateToMap(t *Template) map[string]any {
	return map[string]any{
		"TemplateName":     t.Name,
		"TemplateType":     strings.ToUpper(t.Type),
		"Subject":          t.Subject,
		"HtmlPart":         t.HTMLBody,
		"TextPart":         t.TextBody,
		"Body":             t.Body,
		"Version":          t.Version,
		"CreationDate":     t.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		"LastModifiedDate": t.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ── Tag handlers ──────────────────────────────────────────────────────────────

func (p *Provider) tagResource(arn string, params map[string]any) (*plugin.Response, error) {
	tags := extractTags(params)
	if err := p.store.AddTags(arn, tags); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(arn string, tagKeys []string) (*plugin.Response, error) {
	if err := p.store.RemoveTags(arn, tagKeys); err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(arn string) (*plugin.Response, error) {
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return shared.JSONError("InternalServerErrorException", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// ── Path helpers ──────────────────────────────────────────────────────────────

// pathSegment returns the n-th segment after key in path.
// resolveOp maps an HTTP method + URL path to a Pinpoint operation name.
// REST-JSON requests lack X-Amz-Target, so op arrives empty.
func resolveOp(method, path string) string {
	// Strip /v1/ prefix and split into segments.
	p := strings.TrimPrefix(path, "/v1/")
	p = strings.Trim(p, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// ── Tags: /v1/tags/{arn...} ─────────────────────────────────────────────
	case n >= 2 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		}

	// ── Templates ───────────────────────────────────────────────────────────
	case n == 1 && seg[0] == "templates" && method == http.MethodGet:
		return "ListTemplates"
	case n == 3 && seg[0] == "templates":
		kind := seg[2] // "email" or "sms"
		switch {
		case kind == "email":
			switch method {
			case http.MethodPost:
				return "CreateEmailTemplate"
			case http.MethodGet:
				return "GetEmailTemplate"
			case http.MethodPut:
				return "UpdateEmailTemplate"
			case http.MethodDelete:
				return "DeleteEmailTemplate"
			}
		case kind == "sms":
			switch method {
			case http.MethodPost:
				return "CreateSmsTemplate"
			case http.MethodGet:
				return "GetSmsTemplate"
			case http.MethodPut:
				return "UpdateSmsTemplate"
			case http.MethodDelete:
				return "DeleteSmsTemplate"
			}
		}

	// ── Apps top-level ──────────────────────────────────────────────────────
	case n == 1 && seg[0] == "apps":
		switch method {
		case http.MethodPost:
			return "CreateApp"
		case http.MethodGet:
			return "GetApps"
		}

	// ── Apps sub-resources: /v1/apps/{id}/... ───────────────────────────────
	case n >= 2 && seg[0] == "apps":
		if n == 2 {
			// /v1/apps/{id}
			switch method {
			case http.MethodGet:
				return "GetApp"
			case http.MethodDelete:
				return "DeleteApp"
			}
		}
		if n >= 3 {
			resource := seg[2]
			switch resource {
			case "campaigns":
				return resolveCampaignOp(method, seg, n)
			case "segments":
				return resolveSegmentOp(method, seg, n)
			case "journeys":
				return resolveJourneyOp(method, seg, n)
			case "messages":
				if method == http.MethodPost {
					return "SendMessages"
				}
			case "users-messages":
				if method == http.MethodPost {
					return "SendUsersMessages"
				}
			case "events":
				if method == http.MethodPost {
					return "PutEvents"
				}
			case "channels":
				if method == http.MethodGet {
					return "GetChannels"
				}
			case "settings":
				switch method {
				case http.MethodGet:
					return "GetApplicationSettings"
				case http.MethodPut:
					return "UpdateApplicationSettings"
				}
			case "endpoints":
				if n == 4 && method == http.MethodGet {
					return "GetEndpoint"
				}
			}
		}
	}
	return ""
}

func resolveCampaignOp(method string, seg []string, n int) string {
	switch {
	case n == 3: // /apps/{id}/campaigns
		switch method {
		case http.MethodPost:
			return "CreateCampaign"
		case http.MethodGet:
			return "GetCampaigns"
		}
	case n == 4: // /apps/{id}/campaigns/{cid}
		switch method {
		case http.MethodGet:
			return "GetCampaign"
		case http.MethodPut:
			return "UpdateCampaign"
		case http.MethodDelete:
			return "DeleteCampaign"
		}
	case n == 5 && seg[4] == "activities" && method == http.MethodGet:
		return "GetCampaignActivities"
	case n == 5 && seg[4] == "versions" && method == http.MethodGet:
		return "GetCampaignVersions"
	case n == 6 && seg[4] == "versions" && method == http.MethodGet:
		return "GetCampaignVersion"
	}
	return ""
}

func resolveSegmentOp(method string, seg []string, n int) string {
	switch {
	case n == 3:
		switch method {
		case http.MethodPost:
			return "CreateSegment"
		case http.MethodGet:
			return "GetSegments"
		}
	case n == 4:
		switch method {
		case http.MethodGet:
			return "GetSegment"
		case http.MethodPut:
			return "UpdateSegment"
		case http.MethodDelete:
			return "DeleteSegment"
		}
	case n == 5 && seg[4] == "versions" && method == http.MethodGet:
		return "GetSegmentVersions"
	case n == 6 && seg[4] == "versions" && method == http.MethodGet:
		return "GetSegmentVersion"
	}
	return ""
}

func resolveJourneyOp(method string, seg []string, n int) string {
	switch {
	case n == 3:
		switch method {
		case http.MethodPost:
			return "CreateJourney"
		case http.MethodGet:
			return "ListJourneys"
		}
	case n == 4:
		switch method {
		case http.MethodGet:
			return "GetJourney"
		case http.MethodPut:
			return "UpdateJourney"
		case http.MethodDelete:
			return "DeleteJourney"
		}
	case n == 5 && seg[4] == "state" && method == http.MethodPut:
		return "UpdateJourneyState"
	}
	return ""
}

func pathSegment(path, key string, n int) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == key && i+n < len(parts) {
			return parts[i+n]
		}
	}
	return ""
}

// twoSegments extracts IDs: one after key1 and one after key2.
func twoSegments(path, key1, key2 string) (string, string) {
	return pathSegment(path, key1, 1), pathSegment(path, key2, 1)
}

// tagARN extracts the resource ARN from /v1/tags/{ResourceArn}.
func tagARN(path string) string {
	const prefix = "/v1/tags/"
	idx := strings.Index(path, prefix)
	if idx == -1 {
		return ""
	}
	return path[idx+len(prefix):]
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func extractTags(params map[string]any) map[string]string {
	tags := map[string]string{}
	for _, key := range []string{"tags", "Tags"} {
		if v, ok := params[key]; ok {
			if m, ok := v.(map[string]any); ok {
				for k, val := range m {
					if s, ok := val.(string); ok {
						tags[k] = s
					}
				}
			}
		}
	}
	return tags
}

// generateID returns a random hex ID.
func generateID() string {
	return fmt.Sprintf("%016x", rand.Uint64())
}
