// SPDX-License-Identifier: Apache-2.0

// Package mediaconvert implements AWS Elemental MediaConvert.
package mediaconvert

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

const defaultAccountID = plugin.DefaultAccountID

// MediaConvertProvider implements plugin.ServicePlugin for MediaConvert.
type MediaConvertProvider struct {
	store *Store
}

// ServiceID returns the unique identifier for this plugin.
func (p *MediaConvertProvider) ServiceID() string { return "mediaconvert" }

// ServiceName returns the human-readable name for this plugin.
func (p *MediaConvertProvider) ServiceName() string { return "AWS Elemental MediaConvert" }

// Protocol returns the wire protocol used by this plugin.
func (p *MediaConvertProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

// Init initialises the MediaConvertProvider from cfg.
func (p *MediaConvertProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "mediaconvert"), 0o755); err != nil {
		return fmt.Errorf("init mediaconvert: %w", err)
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "mediaconvert"))
	return err
}

// Shutdown closes the MediaConvertProvider.
func (p *MediaConvertProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the incoming HTTP request to the appropriate MediaConvert operation.
func (p *MediaConvertProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return mcError("BadRequestException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return mcError("BadRequestException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	path := req.URL.Path
	if op == "" {
		op = resolveOp(req.Method, path)
	}

	switch op {
	// Jobs
	case "CreateJob":
		return p.handleCreateJob(params)
	case "GetJob":
		id := extractSegment(path, "jobs", 1)
		return p.handleGetJob(id)
	case "ListJobs":
		return p.handleListJobs()
	case "CancelJob":
		id := extractSegment(path, "jobs", 1)
		return p.handleCancelJob(id)
	case "SearchJobs":
		return p.handleListJobs()
	// Job Templates
	case "CreateJobTemplate":
		return p.handleCreateJobTemplate(params)
	case "GetJobTemplate":
		name := extractSegment(path, "jobTemplates", 1)
		return p.handleGetJobTemplate(name)
	case "ListJobTemplates":
		return p.handleListJobTemplates()
	case "DeleteJobTemplate":
		name := extractSegment(path, "jobTemplates", 1)
		return p.handleDeleteJobTemplate(name)
	case "UpdateJobTemplate":
		name := extractSegment(path, "jobTemplates", 1)
		return p.handleUpdateJobTemplate(name, params)
	// Queues
	case "CreateQueue":
		return p.handleCreateQueue(params)
	case "GetQueue":
		name := extractSegment(path, "queues", 1)
		return p.handleGetQueue(name)
	case "ListQueues":
		return p.handleListQueues()
	case "DeleteQueue":
		name := extractSegment(path, "queues", 1)
		return p.handleDeleteQueue(name)
	case "UpdateQueue":
		name := extractSegment(path, "queues", 1)
		return p.handleUpdateQueue(name, params)
	// Presets
	case "ListPresets":
		return p.handleListPresets()
	case "CreatePreset":
		return p.handleCreatePreset(params)
	case "GetPreset":
		name := extractSegment(path, "presets", 1)
		return p.handleGetPreset(name)
	case "DeletePreset":
		name := extractSegment(path, "presets", 1)
		return p.handleDeletePreset(name)
	case "UpdatePreset":
		name := extractSegment(path, "presets", 1)
		return p.handleUpdatePreset(name, params)
	// Endpoints / Versions
	case "DescribeEndpoints":
		return p.handleDescribeEndpoints()
	case "ListVersions":
		return p.handleListVersions()
	// Tag operations
	case "TagResource":
		return p.handleTagResource(params)
	case "UntagResource":
		return p.handleUntagResource(params)
	case "ListTagsForResource":
		arn := extractSegment(path, "tags", 1)
		return p.handleListTagsForResource(arn, params)
	// Policy
	case "GetPolicy":
		return p.handleGetPolicy()
	case "PutPolicy":
		return p.handlePutPolicy(params)
	case "DeletePolicy":
		return p.handleDeletePolicy()
	// Additional placeholders
	case "AssociateCertificate":
		return mcResponse(http.StatusOK, map[string]any{})
	case "DisassociateCertificate":
		return mcResponse(http.StatusOK, map[string]any{})
	case "ProbeMediaFile":
		return mcResponse(http.StatusOK, map[string]any{"probeResults": []any{}})
	case "DescribeJob":
		id := extractSegment(path, "jobs", 1)
		return p.handleGetJob(id)
	case "DescribeQueue":
		name := extractSegment(path, "queues", 1)
		return p.handleGetQueue(name)
	case "DescribePreset":
		name := extractSegment(path, "presets", 1)
		return p.handleGetPreset(name)
	case "DescribeJobTemplate":
		name := extractSegment(path, "jobTemplates", 1)
		return p.handleGetJobTemplate(name)
	default:
		return mcError("NotFoundException", fmt.Sprintf("unknown operation: %s", op), http.StatusNotFound), nil
	}
}

// ListResources returns active job templates and queues.
func (p *MediaConvertProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	templates, err := p.store.ListJobTemplates(defaultAccountID)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(templates))
	for _, t := range templates {
		res = append(res, plugin.Resource{Type: "job-template", ID: t.ARN, Name: t.Name})
	}
	return res, nil
}

// GetMetrics returns empty metrics.
func (p *MediaConvertProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Job handlers ---

func (p *MediaConvertProvider) handleCreateJob(params map[string]any) (*plugin.Response, error) {
	settings := "{}"
	if s, ok := params["settings"]; ok {
		b, _ := json.Marshal(s)
		settings = string(b)
	}
	queue, _ := params["queue"].(string)
	id := shared.GenerateUUID()
	job, err := p.store.CreateJob(defaultAccountID, id, settings, queue)
	if err != nil {
		return nil, err
	}
	return mcResponse(http.StatusCreated, map[string]any{"job": jobToMap(job)})
}

func (p *MediaConvertProvider) handleGetJob(id string) (*plugin.Response, error) {
	if id == "" {
		return mcError("BadRequestException", "job id required", http.StatusBadRequest), nil
	}
	job, err := p.store.GetJob(defaultAccountID, id)
	if err != nil {
		return mcError("NotFoundException", "job not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"job": jobToMap(job)})
}

func (p *MediaConvertProvider) handleListJobs() (*plugin.Response, error) {
	jobs, err := p.store.ListJobs(defaultAccountID)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(jobs))
	for i := range jobs {
		items = append(items, jobToMap(&jobs[i]))
	}
	return mcResponse(http.StatusOK, map[string]any{"jobs": items})
}

// --- Job Template handlers ---

func (p *MediaConvertProvider) handleCreateJobTemplate(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return mcError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	category, _ := params["category"].(string)
	settings := "{}"
	if s, ok := params["settings"]; ok {
		b, _ := json.Marshal(s)
		settings = string(b)
	}
	queue, _ := params["queue"].(string)
	tmpl, err := p.store.CreateJobTemplate(defaultAccountID, name, description, category, settings, queue)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return mcError("ConflictException", "job template already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return mcResponse(http.StatusCreated, map[string]any{"jobTemplate": jobTemplateToMap(tmpl)})
}

func (p *MediaConvertProvider) handleGetJobTemplate(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "template name required", http.StatusBadRequest), nil
	}
	tmpl, err := p.store.GetJobTemplate(defaultAccountID, name)
	if err != nil {
		return mcError("NotFoundException", "job template not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"jobTemplate": jobTemplateToMap(tmpl)})
}

func (p *MediaConvertProvider) handleListJobTemplates() (*plugin.Response, error) {
	templates, err := p.store.ListJobTemplates(defaultAccountID)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(templates))
	for i := range templates {
		items = append(items, jobTemplateToMap(&templates[i]))
	}
	return mcResponse(http.StatusOK, map[string]any{"jobTemplates": items})
}

func (p *MediaConvertProvider) handleDeleteJobTemplate(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "template name required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteJobTemplate(defaultAccountID, name); err != nil {
		return mcError("NotFoundException", "job template not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusAccepted, map[string]any{})
}

// --- Queue handlers ---

func (p *MediaConvertProvider) handleCreateQueue(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return mcError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	pricingPlan, _ := params["pricingPlan"].(string)
	queue, err := p.store.CreateQueue(defaultAccountID, name, description, pricingPlan)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return mcError("ConflictException", "queue already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return mcResponse(http.StatusCreated, map[string]any{"queue": queueToMap(queue)})
}

func (p *MediaConvertProvider) handleGetQueue(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "queue name required", http.StatusBadRequest), nil
	}
	queue, err := p.store.GetQueue(defaultAccountID, name)
	if err != nil {
		return mcError("NotFoundException", "queue not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"queue": queueToMap(queue)})
}

func (p *MediaConvertProvider) handleListQueues() (*plugin.Response, error) {
	queues, err := p.store.ListQueues(defaultAccountID)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(queues))
	for i := range queues {
		items = append(items, queueToMap(&queues[i]))
	}
	return mcResponse(http.StatusOK, map[string]any{"queues": items})
}

func (p *MediaConvertProvider) handleDeleteQueue(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "queue name required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteQueue(defaultAccountID, name); err != nil {
		return mcError("NotFoundException", "queue not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusAccepted, map[string]any{})
}

// --- Job extension handlers ---

func (p *MediaConvertProvider) handleCancelJob(id string) (*plugin.Response, error) {
	if id == "" {
		return mcError("BadRequestException", "job id required", http.StatusBadRequest), nil
	}
	if err := p.store.CancelJob(defaultAccountID, id); err != nil {
		return mcError("NotFoundException", "job not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusAccepted, map[string]any{})
}

func (p *MediaConvertProvider) handleUpdateJobTemplate(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "template name required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	category, _ := params["category"].(string)
	settings := ""
	if s, ok := params["settings"]; ok {
		b, _ := json.Marshal(s)
		settings = string(b)
	}
	queue, _ := params["queue"].(string)
	tmpl, err := p.store.UpdateJobTemplate(defaultAccountID, name, description, category, settings, queue)
	if err != nil {
		return mcError("NotFoundException", "job template not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"jobTemplate": jobTemplateToMap(tmpl)})
}

func (p *MediaConvertProvider) handleUpdateQueue(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "queue name required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	pricingPlan, _ := params["pricingPlan"].(string)
	q, err := p.store.UpdateQueue(defaultAccountID, name, description, pricingPlan)
	if err != nil {
		return mcError("NotFoundException", "queue not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"queue": queueToMap(q)})
}

// --- Preset handlers ---

func (p *MediaConvertProvider) handleListPresets() (*plugin.Response, error) {
	presets, err := p.store.ListPresets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(presets))
	for i := range presets {
		items = append(items, presetToMap(&presets[i]))
	}
	return mcResponse(http.StatusOK, map[string]any{"presets": items})
}

func (p *MediaConvertProvider) handleCreatePreset(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return mcError("BadRequestException", "Name is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	category, _ := params["category"].(string)
	settings := "{}"
	if s, ok := params["settings"]; ok {
		b, _ := json.Marshal(s)
		settings = string(b)
	}
	preset, err := p.store.CreatePreset(defaultAccountID, name, description, category, settings)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return mcError("ConflictException", "preset already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return mcResponse(http.StatusCreated, map[string]any{"preset": presetToMap(preset)})
}

func (p *MediaConvertProvider) handleGetPreset(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "preset name required", http.StatusBadRequest), nil
	}
	preset, err := p.store.GetPreset(defaultAccountID, name)
	if err != nil {
		return mcError("NotFoundException", "preset not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"preset": presetToMap(preset)})
}

func (p *MediaConvertProvider) handleDeletePreset(name string) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "preset name required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePreset(defaultAccountID, name); err != nil {
		return mcError("NotFoundException", "preset not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusAccepted, map[string]any{})
}

func (p *MediaConvertProvider) handleUpdatePreset(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return mcError("BadRequestException", "preset name required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	category, _ := params["category"].(string)
	settings := ""
	if s, ok := params["settings"]; ok {
		b, _ := json.Marshal(s)
		settings = string(b)
	}
	preset, err := p.store.UpdatePreset(defaultAccountID, name, description, category, settings)
	if err != nil {
		return mcError("NotFoundException", "preset not found", http.StatusNotFound), nil
	}
	return mcResponse(http.StatusOK, map[string]any{"preset": presetToMap(preset)})
}

// --- Endpoint / Version handlers ---

func (p *MediaConvertProvider) handleDescribeEndpoints() (*plugin.Response, error) {
	return mcResponse(http.StatusOK, map[string]any{
		"endpoints": []map[string]any{
			{"url": "https://mediaconvert.us-east-1.amazonaws.com"},
		},
	})
}

func (p *MediaConvertProvider) handleListVersions() (*plugin.Response, error) {
	return mcResponse(http.StatusOK, map[string]any{
		"versions": []map[string]any{
			{"version": "2017-08-29", "lastUpdated": "2024-01-01T00:00:00Z"},
		},
	})
}

// --- Tag handlers ---

func (p *MediaConvertProvider) handleTagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["arn"].(string)
	if arn == "" {
		return mcError("BadRequestException", "arn is required", http.StatusBadRequest), nil
	}
	tagsMap := map[string]string{}
	if raw, ok := params["tags"].(map[string]any); ok {
		for k, v := range raw {
			if s, ok := v.(string); ok {
				tagsMap[k] = s
			}
		}
	}
	if err := p.store.AddTags(arn, tagsMap); err != nil {
		return nil, err
	}
	return mcResponse(http.StatusOK, map[string]any{})
}

func (p *MediaConvertProvider) handleUntagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["arn"].(string)
	if arn == "" {
		return mcError("BadRequestException", "arn is required", http.StatusBadRequest), nil
	}
	keys := []string{}
	if raw, ok := params["tagKeys"].([]any); ok {
		for _, k := range raw {
			if s, ok := k.(string); ok {
				keys = append(keys, s)
			}
		}
	}
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return mcResponse(http.StatusOK, map[string]any{})
}

func (p *MediaConvertProvider) handleListTagsForResource(arnParam string, params map[string]any) (*plugin.Response, error) {
	arn := arnParam
	if arn == "" {
		arn, _ = params["arn"].(string)
	}
	if arn == "" {
		return mcError("BadRequestException", "arn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return mcResponse(http.StatusOK, map[string]any{
		"resourceTags": map[string]any{
			"arn":  arn,
			"tags": tags,
		},
	})
}

// --- Policy handlers ---

func (p *MediaConvertProvider) handleGetPolicy() (*plugin.Response, error) {
	policy, err := p.store.GetPolicy(defaultAccountID)
	if err != nil {
		return nil, err
	}
	var v any
	_ = json.Unmarshal([]byte(policy), &v)
	return mcResponse(http.StatusOK, map[string]any{"policy": v})
}

func (p *MediaConvertProvider) handlePutPolicy(params map[string]any) (*plugin.Response, error) {
	policy := "{}"
	if v, ok := params["policy"]; ok {
		b, _ := json.Marshal(v)
		policy = string(b)
	}
	if err := p.store.PutPolicy(defaultAccountID, policy); err != nil {
		return nil, err
	}
	var v any
	_ = json.Unmarshal([]byte(policy), &v)
	return mcResponse(http.StatusOK, map[string]any{"policy": v})
}

func (p *MediaConvertProvider) handleDeletePolicy() (*plugin.Response, error) {
	if err := p.store.DeletePolicy(defaultAccountID); err != nil {
		return nil, err
	}
	return mcResponse(http.StatusOK, map[string]any{})
}

func presetToMap(pr *MCPreset) map[string]any {
	var settings any
	_ = json.Unmarshal([]byte(pr.Settings), &settings)
	return map[string]any{
		"name":        pr.Name,
		"arn":         pr.ARN,
		"description": pr.Description,
		"category":    pr.Category,
		"settings":    settings,
		"createdAt":   pr.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"lastUpdated": pr.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

// --- helpers ---

func mcError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}
}

func mcResponse(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

func jobToMap(j *MCJob) map[string]any {
	return map[string]any{
		"id":        j.ID,
		"arn":       j.ARN,
		"status":    j.Status,
		"queue":     j.Queue,
		"createdAt": j.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

func jobTemplateToMap(t *MCJobTemplate) map[string]any {
	var settings any
	_ = json.Unmarshal([]byte(t.Settings), &settings)
	return map[string]any{
		"name":        t.Name,
		"arn":         t.ARN,
		"description": t.Description,
		"category":    t.Category,
		"queue":       t.Queue,
		"settings":    settings,
		"createdAt":   t.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"lastUpdated": t.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

func queueToMap(q *MCQueue) map[string]any {
	return map[string]any{
		"name":        q.Name,
		"arn":         q.ARN,
		"description": q.Description,
		"status":      q.Status,
		"pricingPlan": q.PricingPlan,
		"createdAt":   q.CreatedAt.Format("2006-01-02T15:04:05Z"),
		"lastUpdated": q.CreatedAt.Format("2006-01-02T15:04:05Z"),
	}
}

// resolveOp maps HTTP method+path to operation name for MediaConvert.
func resolveOp(method, path string) string {
	parts := splitPath(path)
	// /2017-08-29/jobs
	// /2017-08-29/jobs/{id}
	// /2017-08-29/jobTemplates
	// /2017-08-29/jobTemplates/{name}
	// /2017-08-29/queues
	// /2017-08-29/queues/{name}
	// /2017-08-29/presets

	if len(parts) >= 2 {
		resource := parts[1]
		hasID := len(parts) >= 3 && parts[2] != ""

		switch resource {
		case "jobs":
			if !hasID {
				if method == http.MethodPost {
					return "CreateJob"
				}
				return "ListJobs"
			}
			switch method {
			case http.MethodDelete:
				return "CancelJob"
			default:
				return "GetJob"
			}
		case "jobTemplates":
			if !hasID {
				if method == http.MethodPost {
					return "CreateJobTemplate"
				}
				return "ListJobTemplates"
			}
			switch method {
			case http.MethodGet:
				return "GetJobTemplate"
			case http.MethodDelete:
				return "DeleteJobTemplate"
			case http.MethodPut:
				return "UpdateJobTemplate"
			}
		case "queues":
			if !hasID {
				if method == http.MethodPost {
					return "CreateQueue"
				}
				return "ListQueues"
			}
			switch method {
			case http.MethodGet:
				return "GetQueue"
			case http.MethodDelete:
				return "DeleteQueue"
			case http.MethodPut:
				return "UpdateQueue"
			}
		case "presets":
			if !hasID {
				if method == http.MethodPost {
					return "CreatePreset"
				}
				return "ListPresets"
			}
			switch method {
			case http.MethodGet:
				return "GetPreset"
			case http.MethodDelete:
				return "DeletePreset"
			case http.MethodPut:
				return "UpdatePreset"
			}
		case "endpoints":
			return "DescribeEndpoints"
		case "versions":
			return "ListVersions"
		case "tags":
			switch method {
			case http.MethodGet:
				return "ListTagsForResource"
			case http.MethodPost:
				return "TagResource"
			case http.MethodPut:
				return "TagResource"
			case http.MethodDelete:
				return "UntagResource"
			}
		case "policy":
			switch method {
			case http.MethodGet:
				return "GetPolicy"
			case http.MethodPut:
				return "PutPolicy"
			case http.MethodDelete:
				return "DeletePolicy"
			}
		}
	}
	return ""
}

func splitPath(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return parts
}

// extractSegment finds a named segment after the given key in the URL path.
// e.g. extractSegment("/2017-08-29/jobTemplates/myTemplate", "jobTemplates", 1) → "myTemplate"
func extractSegment(path, key string, offset int) string {
	parts := splitPath(path)
	for i, p := range parts {
		if p == key && i+offset < len(parts) {
			return parts[i+offset]
		}
	}
	return ""
}

func init() {
	plugin.DefaultRegistry.Register("mediaconvert", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &MediaConvertProvider{}
	})
}
