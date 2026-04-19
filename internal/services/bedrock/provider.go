// SPDX-License-Identifier: Apache-2.0

package bedrock

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/bedrock"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

const (
	defaultAccountID = plugin.DefaultAccountID
	defaultRegion    = "us-east-1"
)

// hardcoded foundation models
var foundationModels = []map[string]any{
	{
		"modelId":                    "anthropic.claude-3-opus-20240229-v1:0",
		"modelName":                  "Claude 3 Opus",
		"providerName":               "Anthropic",
		"inputModalities":            []string{"TEXT", "IMAGE"},
		"outputModalities":           []string{"TEXT"},
		"responseStreamingSupported": true,
		"customizationsSupported":    []string{},
		"inferenceTypesSupported":    []string{"ON_DEMAND"},
		"modelLifecycle":             map[string]any{"status": "ACTIVE"},
	},
	{
		"modelId":                    "anthropic.claude-3-sonnet-20240229-v1:0",
		"modelName":                  "Claude 3 Sonnet",
		"providerName":               "Anthropic",
		"inputModalities":            []string{"TEXT", "IMAGE"},
		"outputModalities":           []string{"TEXT"},
		"responseStreamingSupported": true,
		"customizationsSupported":    []string{},
		"inferenceTypesSupported":    []string{"ON_DEMAND"},
		"modelLifecycle":             map[string]any{"status": "ACTIVE"},
	},
	{
		"modelId":                    "meta.llama3-70b-instruct-v1:0",
		"modelName":                  "Llama 3 70B Instruct",
		"providerName":               "Meta",
		"inputModalities":            []string{"TEXT"},
		"outputModalities":           []string{"TEXT"},
		"responseStreamingSupported": true,
		"customizationsSupported":    []string{"FINE_TUNING"},
		"inferenceTypesSupported":    []string{"ON_DEMAND"},
		"modelLifecycle":             map[string]any{"status": "ACTIVE"},
	},
	{
		"modelId":                    "amazon.titan-text-express-v1",
		"modelName":                  "Titan Text G1 - Express",
		"providerName":               "Amazon",
		"inputModalities":            []string{"TEXT"},
		"outputModalities":           []string{"TEXT"},
		"responseStreamingSupported": true,
		"customizationsSupported":    []string{"FINE_TUNING", "CONTINUED_PRE_TRAINING"},
		"inferenceTypesSupported":    []string{"ON_DEMAND"},
		"modelLifecycle":             map[string]any{"status": "ACTIVE"},
	},
}

// Provider implements the BedrockControlPlaneService service.
type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "bedrock" }
func (p *Provider) ServiceName() string           { return "BedrockControlPlaneService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dir := cfg.DataDir
	if dir == "" {
		dir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dir, "bedrock"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var bodyMap map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &bodyMap)
	}
	if bodyMap == nil {
		bodyMap = map[string]any{}
	}

	path := req.URL.Path
	method := req.Method

	// Route by path/method since bedrock router is empty
	switch {
	// Foundation models
	case method == http.MethodGet && path == "/foundation-models":
		return p.listFoundationModels(req.URL.Query())
	case method == http.MethodGet && strings.HasPrefix(path, "/foundation-models/"):
		modelID := strings.TrimPrefix(path, "/foundation-models/")
		return p.getFoundationModel(modelID)

	// InvokeModel
	case method == http.MethodPost && strings.HasSuffix(path, "/invoke"):
		parts := strings.Split(path, "/")
		// /model/{modelId}/invoke
		if len(parts) >= 3 {
			return p.invokeModel(parts[len(parts)-2], false)
		}
		return shared.JSONError("ValidationException", "invalid path", http.StatusBadRequest), nil
	case method == http.MethodPost && strings.HasSuffix(path, "/invoke-with-response-stream"):
		parts := strings.Split(path, "/")
		if len(parts) >= 3 {
			return p.invokeModel(parts[len(parts)-2], true)
		}
		return shared.JSONError("ValidationException", "invalid path", http.StatusBadRequest), nil

	// Customization jobs
	case method == http.MethodPost && path == "/model-customization-jobs":
		return p.createModelCustomizationJob(bodyMap)
	case method == http.MethodGet && path == "/model-customization-jobs":
		return p.listModelCustomizationJobs()
	case method == http.MethodGet && strings.HasPrefix(path, "/model-customization-jobs/") && !strings.HasSuffix(path, "/stop"):
		jobID := extractSegment(path, "/model-customization-jobs/")
		return p.getModelCustomizationJob(jobID)
	case method == http.MethodPost && strings.HasSuffix(path, "/stop"):
		jobID := extractSegment(path, "/model-customization-jobs/")
		jobID = strings.TrimSuffix(jobID, "/stop")
		return p.stopModelCustomizationJob(jobID)

	// Custom models
	case method == http.MethodGet && path == "/custom-models":
		return p.listCustomModels()
	case method == http.MethodGet && strings.HasPrefix(path, "/custom-models/"):
		modelID := extractSegment(path, "/custom-models/")
		return p.getCustomModel(modelID)
	case method == http.MethodDelete && strings.HasPrefix(path, "/custom-models/"):
		modelID := extractSegment(path, "/custom-models/")
		return p.deleteCustomModel(modelID)

	// Guardrails
	case method == http.MethodPost && path == "/guardrails":
		return p.createGuardrail(bodyMap)
	case method == http.MethodGet && path == "/guardrails":
		return p.listGuardrails()
	case method == http.MethodGet && strings.HasPrefix(path, "/guardrails/"):
		guardrailID := extractSegment(path, "/guardrails/")
		return p.getGuardrail(guardrailID)
	case method == http.MethodPut && strings.HasPrefix(path, "/guardrails/"):
		guardrailID := extractSegment(path, "/guardrails/")
		return p.updateGuardrail(guardrailID, bodyMap)
	case method == http.MethodDelete && strings.HasPrefix(path, "/guardrails/"):
		guardrailID := extractSegment(path, "/guardrails/")
		return p.deleteGuardrail(guardrailID)

	// Tags (boto3 bedrock uses body-based paths)
	case method == http.MethodPost && path == "/tagResource":
		arn, _ := bodyMap["resourceARN"].(string)
		return p.tagResource(arn, bodyMap)
	case method == http.MethodPost && path == "/untagResource":
		arn, _ := bodyMap["resourceARN"].(string)
		var keys []string
		if rawKeys, ok := bodyMap["tagKeys"].([]any); ok {
			for _, k := range rawKeys {
				if s, ok := k.(string); ok {
					keys = append(keys, s)
				}
			}
		}
		return p.untagResource(arn, keys)
	case method == http.MethodPost && path == "/listTagsForResource":
		arn, _ := bodyMap["resourceARN"].(string)
		return p.listTagsForResource(arn)
	// Tags (REST-style legacy paths)
	case method == http.MethodPost && strings.HasPrefix(path, "/tags/"):
		rawARN := strings.TrimPrefix(path, "/tags/")
		arn, _ := url.PathUnescape(rawARN)
		return p.tagResource(arn, bodyMap)
	case method == http.MethodGet && strings.HasPrefix(path, "/tags/"):
		rawARN := strings.TrimPrefix(path, "/tags/")
		arn, _ := url.PathUnescape(rawARN)
		return p.listTagsForResource(arn)
	case method == http.MethodDelete && strings.HasPrefix(path, "/tags/"):
		rawARN := strings.TrimPrefix(path, "/tags/")
		arn, _ := url.PathUnescape(rawARN)
		keys := req.URL.Query()["tagKeys"]
		return p.untagResource(arn, keys)

	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation on %s %s not implemented", method, path), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	models, err := p.store.ListCustomModels(defaultAccountID)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(models))
	for _, m := range models {
		out = append(out, plugin.Resource{Type: "custom-model", ID: m.ModelID, Name: m.ModelName})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Foundation Models ---

func (p *Provider) listFoundationModels(_ url.Values) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"modelSummaries": foundationModels})
}

func (p *Provider) getFoundationModel(modelID string) (*plugin.Response, error) {
	// URL decode in case needed
	decoded, _ := url.PathUnescape(modelID)
	for _, fm := range foundationModels {
		if fm["modelId"] == decoded || fm["modelId"] == modelID {
			return shared.JSONResponse(http.StatusOK, map[string]any{"modelDetails": fm})
		}
	}
	return shared.JSONError("ResourceNotFoundException", "foundation model not found", http.StatusNotFound), nil
}

func (p *Provider) invokeModel(modelID string, _ bool) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"completion":  "stub response",
		"stop_reason": "end_turn",
		"model":       modelID,
	})
}

// --- Customization Jobs ---

func (p *Provider) createModelCustomizationJob(body map[string]any) (*plugin.Response, error) {
	jobID := shared.GenerateUUID()
	jobName := strVal(body, "jobName")
	customModelName := strVal(body, "customModelName")
	baseModelID := strVal(body, "baseModelIdentifier")

	j := &CustomizationJob{
		JobID:           jobID,
		JobName:         jobName,
		CustomModelName: customModelName,
		BaseModelID:     baseModelID,
		Status:          "InProgress",
		AccountID:       defaultAccountID,
		CreatedAt:       time.Now().UTC(),
	}
	if err := p.store.CreateCustomizationJob(j); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}

	// Also create the custom model entry
	modelID := shared.GenerateID("", 32)
	m := &CustomModel{
		ModelID:     modelID,
		ModelName:   customModelName,
		BaseModelID: baseModelID,
		JobID:       jobID,
		Status:      "Creating",
		AccountID:   defaultAccountID,
		CreatedAt:   time.Now().UTC(),
	}
	p.store.CreateCustomModel(m) //nolint:errcheck

	jobARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:model-customization-job/%s", defaultRegion, defaultAccountID, jobID)
	return shared.JSONResponse(http.StatusCreated, map[string]any{"jobArn": jobARN})
}

func (p *Provider) getModelCustomizationJob(jobID string) (*plugin.Response, error) {
	j, err := p.store.GetCustomizationJob(jobID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	jobARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:model-customization-job/%s", defaultRegion, defaultAccountID, j.JobID)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"jobArn":          jobARN,
		"jobName":         j.JobName,
		"customModelName": j.CustomModelName,
		"baseModelArn":    j.BaseModelID,
		"status":          j.Status,
		"creationTime":    j.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) listModelCustomizationJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListCustomizationJobs(defaultAccountID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	summaries := make([]any, 0, len(jobs))
	for _, j := range jobs {
		jobARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:model-customization-job/%s", defaultRegion, defaultAccountID, j.JobID)
		summaries = append(summaries, map[string]any{
			"jobArn":          jobARN,
			"jobName":         j.JobName,
			"customModelName": j.CustomModelName,
			"baseModelArn":    j.BaseModelID,
			"status":          j.Status,
			"creationTime":    j.CreatedAt.Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"modelCustomizationJobSummaries": summaries})
}

func (p *Provider) stopModelCustomizationJob(jobID string) (*plugin.Response, error) {
	if err := p.store.StopCustomizationJob(jobID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "job not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- Custom Models ---

func (p *Provider) listCustomModels() (*plugin.Response, error) {
	models, err := p.store.ListCustomModels(defaultAccountID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	summaries := make([]any, 0, len(models))
	for _, m := range models {
		modelARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:custom-model/%s", defaultRegion, defaultAccountID, m.ModelID)
		summaries = append(summaries, map[string]any{
			"modelArn":     modelARN,
			"modelName":    m.ModelName,
			"baseModelArn": m.BaseModelID,
			"creationTime": m.CreatedAt.Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"modelSummaries": summaries})
}

func (p *Provider) getCustomModel(modelID string) (*plugin.Response, error) {
	m, err := p.store.GetCustomModel(modelID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "custom model not found", http.StatusNotFound), nil
	}
	modelARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:custom-model/%s", defaultRegion, defaultAccountID, m.ModelID)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"modelArn":     modelARN,
		"modelName":    m.ModelName,
		"baseModelArn": m.BaseModelID,
		"creationTime": m.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) deleteCustomModel(modelID string) (*plugin.Response, error) {
	if err := p.store.DeleteCustomModel(modelID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "custom model not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- Guardrails ---

func (p *Provider) createGuardrail(body map[string]any) (*plugin.Response, error) {
	name := strVal(body, "name")
	description := strVal(body, "description")
	guardrailID := shared.GenerateID("", 10)

	g := &Guardrail{
		GuardrailID: guardrailID,
		Name:        name,
		Description: description,
		Version:     "DRAFT",
		Status:      "READY",
		AccountID:   defaultAccountID,
		CreatedAt:   time.Now().UTC(),
	}
	if err := p.store.CreateGuardrail(g); err != nil {
		return shared.JSONError("ConflictException", "guardrail already exists", http.StatusConflict), nil
	}

	guardrailARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:guardrail/%s", defaultRegion, defaultAccountID, guardrailID)
	return shared.JSONResponse(http.StatusCreated, map[string]any{
		"guardrailId":  guardrailID,
		"guardrailArn": guardrailARN,
		"version":      "DRAFT",
		"createdAt":    g.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) getGuardrail(guardrailID string) (*plugin.Response, error) {
	g, err := p.store.GetGuardrail(guardrailID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "guardrail not found", http.StatusNotFound), nil
	}
	guardrailARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:guardrail/%s", defaultRegion, defaultAccountID, g.GuardrailID)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"guardrailId":  g.GuardrailID,
		"guardrailArn": guardrailARN,
		"name":         g.Name,
		"description":  g.Description,
		"version":      g.Version,
		"status":       g.Status,
		"createdAt":    g.CreatedAt.Format(time.RFC3339),
	})
}

func (p *Provider) listGuardrails() (*plugin.Response, error) {
	gs, err := p.store.ListGuardrails(defaultAccountID)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	summaries := make([]any, 0, len(gs))
	for _, g := range gs {
		guardrailARN := fmt.Sprintf("arn:aws:bedrock:%s:%s:guardrail/%s", defaultRegion, defaultAccountID, g.GuardrailID)
		summaries = append(summaries, map[string]any{
			"id":           g.GuardrailID,
			"guardrailId":  g.GuardrailID,
			"arn":          guardrailARN,
			"guardrailArn": guardrailARN,
			"name":         g.Name,
			"version":      g.Version,
			"status":       g.Status,
			"createdAt":    g.CreatedAt.Format(time.RFC3339),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"guardrails": summaries})
}

func (p *Provider) updateGuardrail(guardrailID string, body map[string]any) (*plugin.Response, error) {
	g, err := p.store.GetGuardrail(guardrailID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "guardrail not found", http.StatusNotFound), nil
	}
	name := strVal(body, "name")
	if name == "" {
		name = g.Name
	}
	description := strVal(body, "description")
	if err := p.store.UpdateGuardrail(guardrailID, name, description); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return shared.JSONResponse(http.StatusAccepted, map[string]any{
		"guardrailId":  guardrailID,
		"guardrailArn": fmt.Sprintf("arn:aws:bedrock:%s:%s:guardrail/%s", defaultRegion, defaultAccountID, guardrailID),
		"version":      g.Version,
		"updatedAt":    time.Now().Format(time.RFC3339),
	})
}

func (p *Provider) deleteGuardrail(guardrailID string) (*plugin.Response, error) {
	if err := p.store.DeleteGuardrail(guardrailID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "guardrail not found", http.StatusNotFound), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- Tags ---

func (p *Provider) tagResource(arn string, body map[string]any) (*plugin.Response, error) {
	tags := make(map[string]string)
	// Handle list format: [{"key": "k", "value": "v"}, ...]
	if tagsList, ok := body["tags"].([]any); ok {
		for _, item := range tagsList {
			if tagMap, ok := item.(map[string]any); ok {
				k, _ := tagMap["key"].(string)
				v, _ := tagMap["value"].(string)
				if k != "" {
					tags[k] = v
				}
			}
		}
	} else if tagsRaw, ok := body["tags"].(map[string]any); ok {
		// Handle map format: {"key": "value", ...}
		for k, v := range tagsRaw {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
	}
	if err := p.store.TagResource(arn, tags); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

func (p *Provider) listTagsForResource(arn string) (*plugin.Response, error) {
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tagList})
}

func (p *Provider) untagResource(arn string, keys []string) (*plugin.Response, error) {
	if err := p.store.UntagResource(arn, keys); err != nil {
		return shared.JSONError("InternalError", err.Error(), http.StatusInternalServerError), nil
	}
	return &plugin.Response{StatusCode: http.StatusNoContent, Body: []byte{}}, nil
}

// --- helpers ---

func strVal(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func extractSegment(path, prefix string) string {
	s := strings.TrimPrefix(path, prefix)
	// Remove trailing slash segments beyond first
	if idx := strings.Index(s, "/"); idx >= 0 {
		return s // return full remainder including sub-path
	}
	return s
}
