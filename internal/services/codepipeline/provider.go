// SPDX-License-Identifier: Apache-2.0

// internal/services/codepipeline/provider.go
package codepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "codepipeline" }
func (p *Provider) ServiceName() string           { return "CodePipeline_20150709" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codepipeline"))
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
	case "CreatePipeline":
		return p.createPipeline(params)
	case "GetPipeline":
		return p.getPipeline(params)
	case "ListPipelines":
		return p.listPipelines(params)
	case "UpdatePipeline":
		return p.updatePipeline(params)
	case "DeletePipeline":
		return p.deletePipeline(params)
	case "StartPipelineExecution":
		return p.startPipelineExecution(params)
	case "GetPipelineExecution":
		return p.getPipelineExecution(params)
	case "ListPipelineExecutions":
		return p.listPipelineExecutions(params)
	case "StopPipelineExecution":
		return p.stopPipelineExecution(params)
	case "GetPipelineState":
		return p.getPipelineState(params)
	case "PutWebhook":
		return p.putWebhook(params)
	case "ListWebhooks":
		return p.listWebhooks(params)
	case "DeleteWebhook":
		return p.deleteWebhook(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	case "ListActionExecutions":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"actionExecutionDetails": []any{},
			"nextToken":              nil,
		})
	case "GetActionExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"actionExecution": map[string]any{
				"pipelineExecutionId": params["pipelineExecutionId"],
				"actionExecutionId":   shared.GenerateUUID(),
				"status":              "Succeeded",
			},
		})
	case "PutApprovalResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"approvedAt": time.Now().Unix(),
		})
	case "AcknowledgeJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"status": "InProgress",
		})
	case "AcknowledgeThirdPartyJob":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"status": "InProgress",
		})
	case "PutJobFailureResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutJobSuccessResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PollForJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobs": []any{},
		})
	case "PollForThirdPartyJobs":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobs": []any{},
		})
	case "PutThirdPartyJobFailureResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutThirdPartyJobSuccessResult":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "EnableStageTransition":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DisableStageTransition":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "UpdatePipelineSettings":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RetryStageExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"pipelineExecutionId": shared.GenerateUUID(),
		})
	case "RegisterWebhookWithThirdParty":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeregisterWebhookWithThirdParty":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "PutActionRevision":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"newRevision":         true,
			"pipelineExecutionId": shared.GenerateUUID(),
		})
	case "GetThirdPartyJobDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobDetails": map[string]any{},
		})
	case "GetJobDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"jobDetails": map[string]any{},
		})
	case "ListActionTypes":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"actionTypes": []any{},
		})
	case "GetActionType":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"actionType": map[string]any{},
		})
	case "UpdateActionType":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "CreateCustomActionType":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DeleteCustomActionType":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "RollbackStage":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"pipelineExecutionId": shared.GenerateUUID(),
		})
	case "StartPipelineExecutionResumption":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"pipelineExecutionId": shared.GenerateUUID(),
		})
	default:
		// Remaining ops: return success/empty
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	pipelines, err := p.store.ListPipelines()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(pipelines))
	for _, pl := range pipelines {
		res = append(res, plugin.Resource{Type: "pipeline", ID: pl.Name, Name: pl.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Pipeline handlers ----

func (p *Provider) createPipeline(params map[string]any) (*plugin.Response, error) {
	pipelineDecl, _ := params["pipeline"].(map[string]any)
	if pipelineDecl == nil {
		return shared.JSONError("ValidationException", "pipeline is required", http.StatusBadRequest), nil
	}
	name, _ := pipelineDecl["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "pipeline.name is required", http.StatusBadRequest), nil
	}
	roleARN, _ := pipelineDecl["roleArn"].(string)
	stagesJSON := "[]"
	if stages, ok := pipelineDecl["stages"]; ok {
		b, _ := json.Marshal(stages)
		stagesJSON = string(b)
	}
	arn := shared.BuildARN("codepipeline", "pipeline", name)
	pl, err := p.store.CreatePipeline(name, arn, roleARN, stagesJSON)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("PipelineNameInUseException", "pipeline already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags
	if rawTags, ok := params["tags"].([]any); ok {
		p.store.tags.AddTags(pl.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipeline": pipelineToMap(pl),
	})
}

func (p *Provider) getPipeline(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipeline": pipelineToMap(pl),
		"metadata": map[string]any{
			"pipelineArn":     pl.ARN,
			"created":         pl.CreatedAt.Unix(),
			"updated":         pl.UpdatedAt.Unix(),
			"pollingDisabled": false,
		},
	})
}

func (p *Provider) listPipelines(_ map[string]any) (*plugin.Response, error) {
	pipelines, err := p.store.ListPipelines()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(pipelines))
	for _, pl := range pipelines {
		items = append(items, map[string]any{
			"name":    pl.Name,
			"version": pl.Version,
			"created": pl.CreatedAt.Unix(),
			"updated": pl.UpdatedAt.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelines": items,
	})
}

func (p *Provider) updatePipeline(params map[string]any) (*plugin.Response, error) {
	pipelineDecl, _ := params["pipeline"].(map[string]any)
	if pipelineDecl == nil {
		return shared.JSONError("ValidationException", "pipeline is required", http.StatusBadRequest), nil
	}
	name, _ := pipelineDecl["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "pipeline.name is required", http.StatusBadRequest), nil
	}
	roleARN, _ := pipelineDecl["roleArn"].(string)
	stagesJSON := "[]"
	if stages, ok := pipelineDecl["stages"]; ok {
		b, _ := json.Marshal(stages)
		stagesJSON = string(b)
	}
	pl, err := p.store.UpdatePipeline(name, roleARN, stagesJSON)
	if err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipeline": pipelineToMap(pl),
	})
}

func (p *Provider) deletePipeline(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(pl.ARN)
	if err := p.store.DeletePipeline(name); err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- PipelineExecution handlers ----

func (p *Provider) startPipelineExecution(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetPipeline(name); err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	execID := shared.GenerateUUID()
	exec, err := p.store.CreateExecution(execID, name, "InProgress")
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelineExecutionId": exec.ID,
	})
}

func (p *Provider) getPipelineExecution(params map[string]any) (*plugin.Response, error) {
	pipelineName, _ := params["pipelineName"].(string)
	executionID, _ := params["pipelineExecutionId"].(string)
	if pipelineName == "" || executionID == "" {
		return shared.JSONError("ValidationException", "pipelineName and pipelineExecutionId are required", http.StatusBadRequest), nil
	}
	exec, err := p.store.GetExecution(pipelineName, executionID)
	if err != nil {
		return shared.JSONError("PipelineExecutionNotFoundException", "execution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelineExecution": executionToMap(exec),
	})
}

func (p *Provider) listPipelineExecutions(params map[string]any) (*plugin.Response, error) {
	pipelineName, _ := params["pipelineName"].(string)
	if pipelineName == "" {
		return shared.JSONError("ValidationException", "pipelineName is required", http.StatusBadRequest), nil
	}
	execs, err := p.store.ListExecutions(pipelineName)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(execs))
	for _, e := range execs {
		items = append(items, executionToMap(&e))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelineExecutionSummaries": items,
	})
}

func (p *Provider) stopPipelineExecution(params map[string]any) (*plugin.Response, error) {
	pipelineName, _ := params["pipelineName"].(string)
	executionID, _ := params["pipelineExecutionId"].(string)
	if pipelineName == "" || executionID == "" {
		return shared.JSONError("ValidationException", "pipelineName and pipelineExecutionId are required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateExecutionStatus(pipelineName, executionID, "Stopped"); err != nil {
		return shared.JSONError("PipelineExecutionNotFoundException", "execution not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelineExecutionId": executionID,
	})
}

func (p *Provider) getPipelineState(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	pl, err := p.store.GetPipeline(name)
	if err != nil {
		return shared.JSONError("PipelineNotFoundException", "pipeline not found", http.StatusBadRequest), nil
	}
	var stages []any
	json.Unmarshal([]byte(pl.Stages), &stages)
	stageStates := make([]map[string]any, 0, len(stages))
	for _, s := range stages {
		sm, _ := s.(map[string]any)
		stageName, _ := sm["name"].(string)
		stageStates = append(stageStates, map[string]any{
			"stageName": stageName,
			"inboundTransitionState": map[string]any{
				"enabled": true,
			},
			"actionStates": []any{},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"pipelineName":    pl.Name,
		"pipelineVersion": pl.Version,
		"stageStates":     stageStates,
		"created":         pl.CreatedAt.Unix(),
		"updated":         pl.UpdatedAt.Unix(),
	})
}

// ---- Webhook handlers ----

func (p *Provider) putWebhook(params map[string]any) (*plugin.Response, error) {
	webhookDecl, _ := params["webhook"].(map[string]any)
	if webhookDecl == nil {
		return shared.JSONError("ValidationException", "webhook is required", http.StatusBadRequest), nil
	}
	name, _ := webhookDecl["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "webhook.name is required", http.StatusBadRequest), nil
	}
	pipeline, _ := webhookDecl["targetPipeline"].(string)
	targetAction, _ := webhookDecl["targetAction"].(string)
	filtersJSON := "[]"
	if filters, ok := webhookDecl["filters"]; ok {
		b, _ := json.Marshal(filters)
		filtersJSON = string(b)
	}
	authType := "GITHUB_HMAC"
	authConfigJSON := "{}"
	if authDecl, ok := webhookDecl["authentication"].(string); ok && authDecl != "" {
		authType = authDecl
	}
	if authCfg, ok := webhookDecl["authenticationConfiguration"]; ok {
		b, _ := json.Marshal(authCfg)
		authConfigJSON = string(b)
	}
	arn := shared.BuildARN("codepipeline", "webhook", name)
	url := fmt.Sprintf("https://devcloud.local/webhooks/%s", name)
	wh, err := p.store.PutWebhook(name, arn, url, pipeline, targetAction, filtersJSON, authType, authConfigJSON)
	if err != nil {
		return nil, err
	}
	// Handle tags
	if rawTags, ok := params["tags"].([]any); ok {
		p.store.tags.AddTags(wh.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"webhook": webhookToMap(wh),
	})
}

func (p *Provider) listWebhooks(_ map[string]any) (*plugin.Response, error) {
	webhooks, err := p.store.ListWebhooks()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(webhooks))
	for _, wh := range webhooks {
		items = append(items, map[string]any{
			"definition": webhookToMap(&wh),
			"url":        wh.URL,
			"arn":        wh.ARN,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"webhooks": items,
	})
}

func (p *Provider) deleteWebhook(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	wh, err := p.store.GetWebhook(name)
	if err != nil {
		return shared.JSONError("WebhookNotFoundException", "webhook not found", http.StatusBadRequest), nil
	}
	p.store.tags.DeleteAllTags(wh.ARN)
	if err := p.store.DeleteWebhook(name); err != nil {
		return shared.JSONError("WebhookNotFoundException", "webhook not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
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

func pipelineToMap(pl *Pipeline) map[string]any {
	var stages any
	json.Unmarshal([]byte(pl.Stages), &stages)
	if stages == nil {
		stages = []any{}
	}
	return map[string]any{
		"name":    pl.Name,
		"roleArn": pl.RoleARN,
		"stages":  stages,
		"version": pl.Version,
	}
}

func executionToMap(e *PipelineExecution) map[string]any {
	return map[string]any{
		"pipelineExecutionId": e.ID,
		"pipelineName":        e.PipelineName,
		"status":              e.Status,
		"startTime":           e.StartedAt.Unix(),
		"lastUpdateTime":      e.UpdatedAt.Unix(),
	}
}

func webhookToMap(wh *Webhook) map[string]any {
	var filters any
	json.Unmarshal([]byte(wh.Filters), &filters)
	if filters == nil {
		filters = []any{}
	}
	var authCfg any
	json.Unmarshal([]byte(wh.AuthConfig), &authCfg)
	return map[string]any{
		"name":                        wh.Name,
		"targetPipeline":              wh.Pipeline,
		"targetAction":                wh.TargetAction,
		"filters":                     filters,
		"authentication":              wh.AuthType,
		"authenticationConfiguration": authCfg,
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
