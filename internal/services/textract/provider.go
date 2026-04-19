// SPDX-License-Identifier: Apache-2.0

// internal/services/textract/provider.go
package textract

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

// Provider implements the Textract service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "textract" }
func (p *Provider) ServiceName() string           { return "Textract" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "textract"))
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
	// Adapter
	case "CreateAdapter":
		return p.createAdapter(params)
	case "GetAdapter":
		return p.getAdapter(params)
	case "ListAdapters":
		return p.listAdapters(params)
	case "UpdateAdapter":
		return p.updateAdapter(params)
	case "DeleteAdapter":
		return p.deleteAdapter(params)
	// AdapterVersion
	case "CreateAdapterVersion":
		return p.createAdapterVersion(params)
	case "GetAdapterVersion":
		return p.getAdapterVersion(params)
	case "ListAdapterVersions":
		return p.listAdapterVersions(params)
	case "DeleteAdapterVersion":
		return p.deleteAdapterVersion(params)
	// Sync document analysis
	case "AnalyzeDocument":
		return p.analyzeDocument(params)
	case "AnalyzeExpense":
		return p.analyzeExpense(params)
	case "AnalyzeID":
		return p.analyzeID(params)
	case "DetectDocumentText":
		return p.detectDocumentText(params)
	// Async start
	case "StartDocumentAnalysis":
		return p.startJob("DocumentAnalysis")
	case "StartDocumentTextDetection":
		return p.startJob("DocumentTextDetection")
	case "StartExpenseAnalysis":
		return p.startJob("ExpenseAnalysis")
	case "StartLendingAnalysis":
		return p.startJob("LendingAnalysis")
	// Async get
	case "GetDocumentAnalysis":
		return p.getJobResult(params, "DocumentAnalysis")
	case "GetDocumentTextDetection":
		return p.getJobResult(params, "DocumentTextDetection")
	case "GetExpenseAnalysis":
		return p.getJobResult(params, "ExpenseAnalysis")
	case "GetLendingAnalysis":
		return p.getLendingAnalysis(params)
	case "GetLendingAnalysisSummary":
		return p.getLendingAnalysisSummary(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Extended analysis & ID operations
	case "StartDocumentTextDetectionV2":
		return p.startJob("DocumentTextDetection")
	case "GetAnalyzeDocumentJobResults":
		return p.getJobResult(params, "DocumentAnalysis")
	case "GetDetectDominantLanguagesJobResults":
		return p.getJobResult(params, "DetectDominantLanguages")
	case "StartDetectDominantLanguages":
		return p.startJob("DetectDominantLanguages")
	case "StartLabelDetection":
		return p.startJob("LabelDetection")
	case "GetLabelDetectionJobResults":
		return p.getLabelResults(params)
	case "StartKeyValuesDetection":
		return p.startJob("KeyValuesDetection")
	case "GetKeyValuesDetectionJobResults":
		return p.getJobResult(params, "KeyValuesDetection")
	case "StartEntitiesDetection":
		return p.startJob("EntitiesDetection")
	case "GetEntitiesDetectionJobResults":
		return p.getEntitiesResults(params)
	// Adapter lifecycle helpers
	case "ListAdapterJobs":
		return p.listAdapterJobs(params)
	case "DescribeAdapter":
		return p.getAdapter(params)
	case "DescribeAdapterVersion":
		return p.getAdapterVersion(params)
	case "StartAdapterTraining":
		return p.startAdapterTraining(params)
	case "StopAdapterTraining":
		return p.stopAdapterTraining(params)
	// Feedback / correction
	case "SubmitFeedback":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListFeedback":
		return shared.JSONResponse(http.StatusOK, map[string]any{"FeedbackList": []any{}})
	// Job management
	case "CancelJob":
		return p.cancelJob(params)
	case "ListJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{"JobList": []any{}})
	// Service health / meta
	case "DescribeServiceQuotas":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Quotas": []any{}})
	case "GetServiceStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "AVAILABLE"})
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	adapters, err := p.store.ListAdapters()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(adapters))
	for _, a := range adapters {
		res = append(res, plugin.Resource{Type: "adapter", ID: a.ID, Name: a.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Adapter operations ---

func (p *Provider) createAdapter(params map[string]any) (*plugin.Response, error) {
	name, _ := params["AdapterName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "AdapterName is required", http.StatusBadRequest), nil
	}
	autoUpdate, _ := params["AutoUpdate"].(string)
	if autoUpdate == "" {
		autoUpdate = "ENABLED"
	}
	featureTypes := "[]"
	if ft, ok := params["FeatureTypes"]; ok {
		b, _ := json.Marshal(ft)
		featureTypes = string(b)
	}
	id := shared.GenerateUUID()
	arn := shared.BuildARN("textract", "adapter", id)
	a, err := p.store.CreateAdapter(id, arn, name, featureTypes, autoUpdate)
	if err != nil {
		return nil, err
	}
	// Tags
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := flatMapTags(rawTags)
		_ = p.store.tags.AddTags(a.ARN, tags) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AdapterId": a.ID})
}

func (p *Provider) getAdapter(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AdapterId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAdapter(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, adapterToMap(a, tags))
}

func (p *Provider) listAdapters(params map[string]any) (*plugin.Response, error) {
	adapters, err := p.store.ListAdapters()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(adapters))
	for _, a := range adapters {
		items = append(items, map[string]any{
			"AdapterId":    a.ID,
			"AdapterName":  a.Name,
			"CreationTime": a.CreatedAt.Unix(),
			"FeatureTypes": jsonParse(a.FeatureTypes),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Adapters": items})
}

func (p *Provider) updateAdapter(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AdapterId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAdapter(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	autoUpdate := a.AutoUpdate
	if v, ok := params["AutoUpdate"].(string); ok && v != "" {
		autoUpdate = v
	}
	featureTypes := a.FeatureTypes
	if ft, ok := params["FeatureTypes"]; ok {
		b, _ := json.Marshal(ft)
		featureTypes = string(b)
	}
	if err := p.store.UpdateAdapter(id, autoUpdate, featureTypes); err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	updated, _ := p.store.GetAdapter(id)
	tags, _ := p.store.tags.ListTags(a.ARN)
	return shared.JSONResponse(http.StatusOK, adapterToMap(updated, tags))
}

func (p *Provider) deleteAdapter(params map[string]any) (*plugin.Response, error) {
	id, _ := params["AdapterId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	a, err := p.store.GetAdapter(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(a.ARN) //nolint:errcheck
	if err := p.store.DeleteAdapter(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- AdapterVersion operations ---

func (p *Provider) createAdapterVersion(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	if adapterID == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAdapter(adapterID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	datasetConfig := "{}"
	if dc, ok := params["DatasetConfig"]; ok {
		b, _ := json.Marshal(dc)
		datasetConfig = string(b)
	}
	version := shared.GenerateID("v", 8)
	av, err := p.store.CreateAdapterVersion(adapterID, version, datasetConfig)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AdapterId":      av.AdapterID,
		"AdapterVersion": av.Version,
	})
}

func (p *Provider) getAdapterVersion(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	version, _ := params["AdapterVersion"].(string)
	if adapterID == "" || version == "" {
		return shared.JSONError("ValidationException", "AdapterId and AdapterVersion are required", http.StatusBadRequest), nil
	}
	av, err := p.store.GetAdapterVersion(adapterID, version)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter version not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, adapterVersionToMap(av))
}

func (p *Provider) listAdapterVersions(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	if adapterID == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	versions, err := p.store.ListAdapterVersions(adapterID)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, adapterVersionToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AdapterVersions": items})
}

func (p *Provider) deleteAdapterVersion(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	version, _ := params["AdapterVersion"].(string)
	if adapterID == "" || version == "" {
		return shared.JSONError("ValidationException", "AdapterId and AdapterVersion are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteAdapterVersion(adapterID, version); err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter version not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Sync document analysis (dummy responses) ---

func dummyBlocks() []map[string]any {
	return []map[string]any{
		{
			"BlockType":     "PAGE",
			"Confidence":    99.0,
			"Text":          "",
			"Id":            shared.GenerateUUID(),
			"Page":          1,
			"Relationships": []any{},
		},
	}
}

func (p *Provider) analyzeDocument(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Blocks":           dummyBlocks(),
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

func (p *Provider) analyzeExpense(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ExpenseDocuments": []any{},
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

func (p *Provider) analyzeID(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"IdentityDocuments": []any{},
		"DocumentMetadata":  map[string]any{"Pages": 1},
	})
}

func (p *Provider) detectDocumentText(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Blocks":           dummyBlocks(),
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

// --- Async job operations ---

func (p *Provider) startJob(jobType string) (*plugin.Response, error) {
	id := shared.GenerateUUID()
	if _, err := p.store.CreateJob(id, jobType); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"JobId": id})
}

func (p *Provider) getJobResult(params map[string]any, jobType string) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(jobID)
	if err != nil {
		return shared.JSONError("InvalidJobIdException", "job not found", http.StatusBadRequest), nil
	}
	_ = jobType
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobStatus":        job.Status,
		"Blocks":           dummyBlocks(),
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

func (p *Provider) getLendingAnalysis(params map[string]any) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(jobID)
	if err != nil {
		return shared.JSONError("InvalidJobIdException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobStatus":        job.Status,
		"Results":          []any{},
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

func (p *Provider) getLendingAnalysisSummary(params map[string]any) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(jobID)
	if err != nil {
		return shared.JSONError("InvalidJobIdException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobStatus":        job.Status,
		"Summary":          map[string]any{},
		"DocumentMetadata": map[string]any{"Pages": 1},
	})
}

// --- Tag operations ---

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := flatMapTags(rawTags)
	if err := p.store.tags.AddTags(arn, tags); err != nil {
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
	if tags == nil {
		tags = map[string]string{}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Extended handlers ---

func (p *Provider) getLabelResults(params map[string]any) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetJob(jobID); err != nil {
		return shared.JSONError("InvalidJobIdException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobStatus": "SUCCEEDED",
		"Labels":    []any{},
	})
}

func (p *Provider) getEntitiesResults(params map[string]any) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetJob(jobID); err != nil {
		return shared.JSONError("InvalidJobIdException", "job not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobStatus": "SUCCEEDED",
		"Entities":  []any{},
	})
}

func (p *Provider) listAdapterJobs(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"AdapterJobs": []any{}})
}

func (p *Provider) startAdapterTraining(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	if adapterID == "" {
		return shared.JSONError("ValidationException", "AdapterId is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetAdapter(adapterID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "adapter not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AdapterId": adapterID,
		"Status":    "TRAINING",
	})
}

func (p *Provider) stopAdapterTraining(params map[string]any) (*plugin.Response, error) {
	adapterID, _ := params["AdapterId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AdapterId": adapterID,
		"Status":    "STOPPED",
	})
}

func (p *Provider) cancelJob(params map[string]any) (*plugin.Response, error) {
	jobID, _ := params["JobId"].(string)
	if jobID == "" {
		return shared.JSONError("ValidationException", "JobId is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"JobId":  jobID,
		"Status": "CANCELED",
	})
}

// --- helpers ---

func adapterToMap(a *Adapter, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"AdapterId":    a.ID,
		"AdapterArn":   a.ARN,
		"AdapterName":  a.Name,
		"AutoUpdate":   a.AutoUpdate,
		"FeatureTypes": jsonParse(a.FeatureTypes),
		"CreationTime": a.CreatedAt.Unix(),
		"Tags":         tags,
	}
}

func adapterVersionToMap(av *AdapterVersion) map[string]any {
	return map[string]any{
		"AdapterId":      av.AdapterID,
		"AdapterVersion": av.Version,
		"Status":         av.Status,
		"CreationTime":   av.CreatedAt.Unix(),
	}
}

func jsonParse(s string) any {
	var v any
	_ = json.Unmarshal([]byte(s), &v)
	return v
}

func flatMapTags(raw map[string]any) map[string]string {
	tags := make(map[string]string, len(raw))
	for k, v := range raw {
		if sv, ok := v.(string); ok {
			tags[k] = sv
		}
	}
	return tags
}
