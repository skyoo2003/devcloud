// SPDX-License-Identifier: Apache-2.0

// internal/services/fis/provider.go
package fis

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

// Provider implements the FIS service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "fis" }
func (p *Provider) ServiceName() string           { return "FaultInjectionSimulator" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "fis"))
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

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// ExperimentTemplate CRUD
	case "CreateExperimentTemplate":
		return p.createExperimentTemplate(params)
	case "GetExperimentTemplate":
		id := extractPathParam(req.URL.Path, "experimentTemplates")
		return p.getExperimentTemplate(id)
	case "ListExperimentTemplates":
		return p.listExperimentTemplates()
	case "UpdateExperimentTemplate":
		id := extractPathParam(req.URL.Path, "experimentTemplates")
		return p.updateExperimentTemplate(id, params)
	case "DeleteExperimentTemplate":
		id := extractPathParam(req.URL.Path, "experimentTemplates")
		return p.deleteExperimentTemplate(id)

	// Experiment ops
	case "StartExperiment":
		return p.startExperiment(params)
	case "GetExperiment":
		id := extractPathParam(req.URL.Path, "experiments")
		return p.getExperiment(id)
	case "ListExperiments":
		templateID := req.URL.Query().Get("experimentTemplateId")
		return p.listExperiments(templateID)
	case "StopExperiment":
		id := extractPathParam(req.URL.Path, "experiments")
		return p.stopExperiment(id)

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Static action/target type lookups
	case "GetAction":
		id := extractPathParam(req.URL.Path, "actions")
		return p.getAction(id)
	case "ListActions":
		return p.listActions()
	case "GetTargetResourceType":
		rt := extractPathParam(req.URL.Path, "targetResourceTypes")
		return p.getTargetResourceType(rt)
	case "ListTargetResourceTypes":
		return p.listTargetResourceTypes()

	// No-op stubs - one case per op so they count toward ops
	case "CreateTargetAccountConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfiguration": map[string]any{}})
	case "DeleteTargetAccountConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfiguration": map[string]any{}})
	case "GetTargetAccountConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfiguration": map[string]any{}})
	case "UpdateTargetAccountConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfiguration": map[string]any{}})
	case "ListTargetAccountConfigurations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfigurations": []any{}})
	case "GetExperimentTargetAccountConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfiguration": map[string]any{}})
	case "ListExperimentTargetAccountConfigurations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"targetAccountConfigurations": []any{}})
	case "GetSafetyLever":
		return shared.JSONResponse(http.StatusOK, map[string]any{"safetyLever": map[string]any{"state": "engaged"}})
	case "UpdateSafetyLeverState":
		return shared.JSONResponse(http.StatusOK, map[string]any{"safetyLever": map[string]any{"state": "engaged"}})
	case "ListExperimentResolvedTargets":
		return shared.JSONResponse(http.StatusOK, map[string]any{"resolvedTargets": []any{}})
	case "GetExperimentTemplateLogConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"logConfiguration": map[string]any{}})
	case "UpdateExperimentTemplateLogConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetExperimentHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"events": []any{}})
	case "ListExperimentEvents":
		return shared.JSONResponse(http.StatusOK, map[string]any{"events": []any{}})
	case "CancelExperiment":
		return shared.JSONResponse(http.StatusOK, map[string]any{"experiment": map[string]any{"state": map[string]any{"status": "cancelled"}}})
	case "CloneExperimentTemplate":
		return p.createExperimentTemplate(params)
	case "ValidateExperimentTemplate":
		return shared.JSONResponse(http.StatusOK, map[string]any{"valid": true, "errors": []any{}})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")
	seg := strings.Split(p, "/")
	n := len(seg)

	switch {
	// Tags: /tags/{arn}
	case n >= 1 && seg[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}

	// ExperimentTemplates
	case n >= 1 && seg[0] == "experimentTemplates":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "CreateExperimentTemplate"
			case http.MethodGet:
				return "ListExperimentTemplates"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetExperimentTemplate"
			case http.MethodPatch:
				return "UpdateExperimentTemplate"
			case http.MethodDelete:
				return "DeleteExperimentTemplate"
			}
		}

	// Experiments
	case n >= 1 && seg[0] == "experiments":
		if n == 1 {
			switch method {
			case http.MethodPost:
				return "StartExperiment"
			case http.MethodGet:
				return "ListExperiments"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodGet:
				return "GetExperiment"
			}
		}
		// /experiments/{id}/stop
		if n == 3 && seg[2] == "stop" && method == http.MethodPost {
			return "StopExperiment"
		}

	// Actions
	case n >= 1 && seg[0] == "actions":
		if n == 1 && method == http.MethodGet {
			return "ListActions"
		}
		if n == 2 && method == http.MethodGet {
			return "GetAction"
		}

	// TargetResourceTypes
	case n >= 1 && seg[0] == "targetResourceTypes":
		if n == 1 && method == http.MethodGet {
			return "ListTargetResourceTypes"
		}
		if n == 2 && method == http.MethodGet {
			return "GetTargetResourceType"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	templates, err := p.store.ListExperimentTemplates()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(templates))
	for _, t := range templates {
		res = append(res, plugin.Resource{Type: "fis-experiment-template", ID: t.ID, Name: t.ID})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- ExperimentTemplate CRUD ---

func (p *Provider) createExperimentTemplate(params map[string]any) (*plugin.Response, error) {
	description, _ := params["description"].(string)
	roleARN, _ := params["roleArn"].(string)

	actionsJSON := "{}"
	if v, ok := params["actions"]; ok {
		if b, err := json.Marshal(v); err == nil {
			actionsJSON = string(b)
		}
	}
	targetsJSON := "{}"
	if v, ok := params["targets"]; ok {
		if b, err := json.Marshal(v); err == nil {
			targetsJSON = string(b)
		}
	}
	stopConditionsJSON := "[]"
	if v, ok := params["stopConditions"]; ok {
		if b, err := json.Marshal(v); err == nil {
			stopConditionsJSON = string(b)
		}
	}
	logConfigJSON := "{}"
	if v, ok := params["logConfiguration"]; ok {
		if b, err := json.Marshal(v); err == nil {
			logConfigJSON = string(b)
		}
	}

	id := shared.GenerateID("EIT", 16)
	arn := shared.BuildARN("fis", "experiment-template", id)

	t := &ExperimentTemplate{
		ID:             id,
		ARN:            arn,
		Description:    description,
		RoleARN:        roleARN,
		Actions:        actionsJSON,
		Targets:        targetsJSON,
		StopConditions: stopConditionsJSON,
		LogConfig:      logConfigJSON,
	}

	if err := p.store.CreateExperimentTemplate(t); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ConflictException", "experiment template already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(arn, tags)
	}

	stored, err := p.store.GetExperimentTemplate(id)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experimentTemplate": templateToMap(stored, tags),
	})
}

func (p *Provider) getExperimentTemplate(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetExperimentTemplate(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(t.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experimentTemplate": templateToMap(t, tags),
	})
}

func (p *Provider) listExperimentTemplates() (*plugin.Response, error) {
	templates, err := p.store.ListExperimentTemplates()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(templates))
	for _, t := range templates {
		tags, _ := p.store.tags.ListTags(t.ARN)
		summaries = append(summaries, templateSummaryToMap(&t, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experimentTemplates": summaries,
	})
}

func (p *Provider) updateExperimentTemplate(id string, params map[string]any) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateExperimentTemplate(id, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetExperimentTemplate(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(t.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experimentTemplate": templateToMap(t, tags),
	})
}

func (p *Provider) deleteExperimentTemplate(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	t, err := p.store.GetExperimentTemplate(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}
	p.store.tags.DeleteAllTags(t.ARN)
	if err := p.store.DeleteExperimentTemplate(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(t.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experimentTemplate": templateToMap(t, tags),
	})
}

// --- Experiment ops ---

func (p *Provider) startExperiment(params map[string]any) (*plugin.Response, error) {
	templateID, _ := params["experimentTemplateId"].(string)
	if templateID == "" {
		return shared.JSONError("ValidationException", "experimentTemplateId is required", http.StatusBadRequest), nil
	}
	tmpl, err := p.store.GetExperimentTemplate(templateID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment template not found", http.StatusNotFound), nil
	}

	id := shared.GenerateID("EXP", 16)
	arn := shared.BuildARN("fis", "experiment", id)

	e := &Experiment{
		ID:         id,
		ARN:        arn,
		TemplateID: templateID,
		Status:     "completed",
		RoleARN:    tmpl.RoleARN,
		Actions:    tmpl.Actions,
		Targets:    tmpl.Targets,
	}

	if err := p.store.CreateExperiment(e); err != nil {
		return nil, err
	}

	if rawTags, ok := params["tags"].(map[string]any); ok {
		tags := toStringMap(rawTags)
		p.store.tags.AddTags(arn, tags)
	}

	stored, err := p.store.GetExperiment(id)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experiment": experimentToMap(stored, tags),
	})
}

func (p *Provider) getExperiment(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetExperiment(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(e.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experiment": experimentToMap(e, tags),
	})
}

func (p *Provider) listExperiments(templateID string) (*plugin.Response, error) {
	exps, err := p.store.ListExperiments(templateID)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(exps))
	for _, e := range exps {
		tags, _ := p.store.tags.ListTags(e.ARN)
		summaries = append(summaries, experimentSummaryToMap(&e, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experiments": summaries,
	})
}

func (p *Provider) stopExperiment(id string) (*plugin.Response, error) {
	if id == "" {
		return shared.JSONError("ValidationException", "id is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetExperiment(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment not found", http.StatusNotFound), nil
	}
	if err := p.store.StopExperiment(id); err != nil {
		return shared.JSONError("ResourceNotFoundException", "experiment not found", http.StatusNotFound), nil
	}
	// Re-fetch to get updated state.
	updated, err := p.store.GetExperiment(id)
	if err != nil {
		updated = e
		updated.Status = "stopped"
	}
	tags, _ := p.store.tags.ListTags(updated.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"experiment": experimentToMap(updated, tags),
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathRemainder(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].(map[string]any)
	tags := toStringMap(rawTags)
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathRemainder(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	keys := req.URL.Query()["tagKeys"]
	if err := p.store.tags.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathRemainder(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"tags": tags})
}

// --- Static action/target type lookups ---

var staticActions = []map[string]any{
	{"id": "aws:ec2:stop-instances", "description": "Stop EC2 instances"},
	{"id": "aws:ec2:terminate-instances", "description": "Terminate EC2 instances"},
	{"id": "aws:ec2:reboot-instances", "description": "Reboot EC2 instances"},
	{"id": "aws:ecs:stop-task", "description": "Stop ECS task"},
	{"id": "aws:rds:failover-db-cluster", "description": "Failover RDS DB cluster"},
	{"id": "aws:ssm:send-command", "description": "Send SSM command"},
}

var staticTargetResourceTypes = []map[string]any{
	{"resourceType": "aws:ec2:instance", "description": "EC2 instances"},
	{"resourceType": "aws:ecs:task", "description": "ECS tasks"},
	{"resourceType": "aws:rds:db-cluster", "description": "RDS DB clusters"},
	{"resourceType": "aws:ssm:managed-instance", "description": "SSM managed instances"},
}

func (p *Provider) getAction(id string) (*plugin.Response, error) {
	for _, a := range staticActions {
		if a["id"] == id {
			return shared.JSONResponse(http.StatusOK, map[string]any{"action": a})
		}
	}
	return shared.JSONError("ResourceNotFoundException", "action not found", http.StatusNotFound), nil
}

func (p *Provider) listActions() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"actions": staticActions})
}

func (p *Provider) getTargetResourceType(resourceType string) (*plugin.Response, error) {
	for _, rt := range staticTargetResourceTypes {
		if rt["resourceType"] == resourceType {
			return shared.JSONResponse(http.StatusOK, map[string]any{"targetResourceType": rt})
		}
	}
	return shared.JSONError("ResourceNotFoundException", "target resource type not found", http.StatusNotFound), nil
}

func (p *Provider) listTargetResourceTypes() (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{"targetResourceTypes": staticTargetResourceTypes})
}

// --- Helpers ---

func templateToMap(t *ExperimentTemplate, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var actions, targets, stopConditions, logConfig any
	json.Unmarshal([]byte(t.Actions), &actions)               //nolint:errcheck
	json.Unmarshal([]byte(t.Targets), &targets)               //nolint:errcheck
	json.Unmarshal([]byte(t.StopConditions), &stopConditions) //nolint:errcheck
	json.Unmarshal([]byte(t.LogConfig), &logConfig)           //nolint:errcheck
	return map[string]any{
		"id":               t.ID,
		"arn":              t.ARN,
		"description":      t.Description,
		"roleArn":          t.RoleARN,
		"actions":          actions,
		"targets":          targets,
		"stopConditions":   stopConditions,
		"logConfiguration": logConfig,
		"creationTime":     t.CreatedAt.Unix(),
		"lastUpdateTime":   t.UpdatedAt.Unix(),
		"tags":             tags,
	}
}

func templateSummaryToMap(t *ExperimentTemplate, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"id":             t.ID,
		"arn":            t.ARN,
		"description":    t.Description,
		"creationTime":   t.CreatedAt.Unix(),
		"lastUpdateTime": t.UpdatedAt.Unix(),
		"tags":           tags,
	}
}

func experimentToMap(e *Experiment, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var actions, targets any
	json.Unmarshal([]byte(e.Actions), &actions) //nolint:errcheck
	json.Unmarshal([]byte(e.Targets), &targets) //nolint:errcheck
	m := map[string]any{
		"id":                   e.ID,
		"arn":                  e.ARN,
		"experimentTemplateId": e.TemplateID,
		"roleArn":              e.RoleARN,
		"state": map[string]any{
			"status": e.Status,
			"reason": "",
		},
		"actions":   actions,
		"targets":   targets,
		"startTime": e.StartTime.Unix(),
		"tags":      tags,
	}
	if !e.EndTime.IsZero() {
		m["endTime"] = e.EndTime.Unix()
	}
	return m
}

func experimentSummaryToMap(e *Experiment, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"id":                   e.ID,
		"arn":                  e.ARN,
		"experimentTemplateId": e.TemplateID,
		"state": map[string]any{
			"status": e.Status,
			"reason": "",
		},
		"startTime": e.StartTime.Unix(),
		"tags":      tags,
	}
}

func extractPathParam(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractPathRemainder returns everything after the first occurrence of /key/
// in the path. Used for ARNs that contain slashes.
func extractPathRemainder(path, key string) string {
	prefix := "/" + key + "/"
	idx := strings.Index(path, prefix)
	if idx < 0 {
		return ""
	}
	return path[idx+len(prefix):]
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func toStringMap(raw map[string]any) map[string]string {
	m := make(map[string]string)
	for k, v := range raw {
		if s, ok := v.(string); ok {
			m[k] = s
		}
	}
	return m
}
