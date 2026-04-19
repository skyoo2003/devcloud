// SPDX-License-Identifier: Apache-2.0

// Package pipes implements AWS EventBridge Pipes.
package pipes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// PipesProvider implements plugin.ServicePlugin for EventBridge Pipes.
type PipesProvider struct {
	store *Store
}

func (p *PipesProvider) ServiceID() string             { return "pipes" }
func (p *PipesProvider) ServiceName() string           { return "AWS EventBridge Pipes" }
func (p *PipesProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *PipesProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "pipes"))
	return err
}

func (p *PipesProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *PipesProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op = resolvePipeOp(req.Method, req.URL.Path)
	}
	switch op {
	case "CreatePipe":
		return p.handleCreatePipe(req)
	case "DescribePipe":
		return p.handleDescribePipe(req)
	case "ListPipes":
		return p.handleListPipes(req)
	case "UpdatePipe":
		return p.handleUpdatePipe(req)
	case "DeletePipe":
		return p.handleDeletePipe(req)
	case "StartPipe":
		return p.handleStartPipe(req)
	case "StopPipe":
		return p.handleStopPipe(req)
	case "TagResource":
		return p.handleTagResource(req)
	case "ListTagsForResource":
		return p.handleListTagsForResource(req)
	case "UntagResource":
		return p.handleUntagResource(req)
	case "GetPipeStatistics":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"InvocationsSuccess": 0,
			"InvocationsFailure": 0,
			"LastInvocationTime": time.Now().UTC().Format(time.RFC3339),
		})
	case "ListPipeEvents":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Events":    []any{},
			"NextToken": nil,
		})
	case "DescribePipeTargets":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Targets":   []any{},
			"NextToken": nil,
		})
	case "PutPipeConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetPipeConfiguration":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Configuration": map[string]any{},
		})
	case "DescribePipeHealth":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Status":      "HEALTHY",
			"LastChecked": time.Now().UTC().Format(time.RFC3339),
		})
	case "ListPipeExecutions":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Executions": []any{},
			"NextToken":  nil,
		})
	case "TestPipe":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Success": true,
		})
	case "ValidatePipeParameters":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Valid":  true,
			"Errors": []any{},
		})
	case "ListPipeTargets":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Targets":   []any{},
			"NextToken": nil,
		})
	case "GetPipeMetrics":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Metrics": []any{},
		})
	case "EnablePipe":
		return p.handleStartPipe(req)
	case "DisablePipe":
		return p.handleStopPipe(req)
	default:
		return shared.JSONError("UnsupportedOperation", fmt.Sprintf("operation not supported: %s", op), http.StatusBadRequest), nil
	}
}

func (p *PipesProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	pipes, err := p.store.ListPipes(shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(pipes))
	for _, pipe := range pipes {
		res = append(res, plugin.Resource{Type: "pipe", ID: pipe.Name, Name: pipe.Name})
	}
	return res, nil
}

func (p *PipesProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Operations ---

func (p *PipesProvider) handleCreatePipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameFromPath(req.URL.Path)

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

	if name == "" {
		name, _ = params["Name"].(string)
	}
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}

	arn := fmt.Sprintf("arn:aws:pipes:%s:%s:pipe/%s", shared.DefaultRegion, shared.DefaultAccountID, name)

	pipe := &Pipe{
		Name:             name,
		AccountID:        shared.DefaultAccountID,
		ARN:              arn,
		State:            "RUNNING",
		SourceParameters: "{}",
		TargetParameters: "{}",
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
	}
	if v, ok := params["Source"].(string); ok {
		pipe.Source = v
	}
	if v, ok := params["Target"].(string); ok {
		pipe.Target = v
	}
	if v, ok := params["RoleArn"].(string); ok {
		pipe.RoleARN = v
	}
	if v, ok := params["Description"].(string); ok {
		pipe.Description = v
	}
	if v, ok := params["DesiredState"].(string); ok && v != "" {
		pipe.State = v
	}
	if v, ok := params["Enrichment"].(string); ok {
		pipe.Enrichment = v
	}
	if v, ok := params["SourceParameters"]; ok {
		b, _ := json.Marshal(v)
		pipe.SourceParameters = string(b)
	}
	if v, ok := params["TargetParameters"]; ok {
		b, _ := json.Marshal(v)
		pipe.TargetParameters = string(b)
	}

	if err := p.store.CreatePipe(pipe); err != nil {
		return shared.JSONError("ConflictException", "pipe already exists", http.StatusConflict), nil
	}

	// Handle tags
	if tagsRaw, ok := params["Tags"].(map[string]any); ok {
		tags := make(map[string]string, len(tagsRaw))
		for k, v := range tagsRaw {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		p.store.PutTags(arn, tags) //nolint:errcheck
	}

	return shared.JSONResponse(http.StatusCreated, pipeToResponse(pipe))
}

func (p *PipesProvider) handleDescribePipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameFromPath(req.URL.Path)
	if name == "" {
		return shared.JSONError("ValidationException", "pipe name is required", http.StatusBadRequest), nil
	}

	pipe, err := p.store.GetPipe(name, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errPipeNotFound) {
			return shared.JSONError("NotFoundException", fmt.Sprintf("Pipe %s not found", name), http.StatusNotFound), nil
		}
		return nil, err
	}

	tags, _ := p.store.GetTags(pipe.ARN)
	resp := pipeToResponse(pipe)
	resp["Tags"] = tags

	return shared.JSONResponse(http.StatusOK, resp)
}

func (p *PipesProvider) handleListPipes(req *http.Request) (*plugin.Response, error) {
	pipes, err := p.store.ListPipes(shared.DefaultAccountID)
	if err != nil {
		return nil, err
	}

	items := make([]map[string]any, 0, len(pipes))
	for _, pipe := range pipes {
		items = append(items, map[string]any{
			"Name":             pipe.Name,
			"Arn":              pipe.ARN,
			"CurrentState":     pipe.State,
			"DesiredState":     pipe.State,
			"Source":           pipe.Source,
			"Target":           pipe.Target,
			"StateReason":      "",
			"CreationTime":     pipe.CreatedAt.UTC().Format(time.RFC3339),
			"LastModifiedTime": pipe.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Pipes":     items,
		"NextToken": nil,
	})
}

func (p *PipesProvider) handleUpdatePipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameFromPath(req.URL.Path)
	if name == "" {
		return shared.JSONError("ValidationException", "pipe name is required", http.StatusBadRequest), nil
	}

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

	pipe, err := p.store.GetPipe(name, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errPipeNotFound) {
			return shared.JSONError("NotFoundException", fmt.Sprintf("Pipe %s not found", name), http.StatusNotFound), nil
		}
		return nil, err
	}

	if v, ok := params["Target"].(string); ok {
		pipe.Target = v
	}
	if v, ok := params["RoleArn"].(string); ok {
		pipe.RoleARN = v
	}
	if v, ok := params["Description"].(string); ok {
		pipe.Description = v
	}
	if v, ok := params["Enrichment"].(string); ok {
		pipe.Enrichment = v
	}
	if v, ok := params["DesiredState"].(string); ok && v != "" {
		pipe.State = v
	}

	if err := p.store.UpdatePipe(pipe); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, pipeToResponse(pipe))
}

func (p *PipesProvider) handleDeletePipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameFromPath(req.URL.Path)
	if name == "" {
		return shared.JSONError("ValidationException", "pipe name is required", http.StatusBadRequest), nil
	}

	pipe, err := p.store.GetPipe(name, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errPipeNotFound) {
			return shared.JSONError("NotFoundException", fmt.Sprintf("Pipe %s not found", name), http.StatusNotFound), nil
		}
		return nil, err
	}

	if err := p.store.DeletePipe(name, shared.DefaultAccountID); err != nil {
		return nil, err
	}

	pipe.State = "DELETING"
	return shared.JSONResponse(http.StatusOK, pipeToResponse(pipe))
}

func (p *PipesProvider) handleStartPipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameBeforeAction(req.URL.Path, "start")
	if name == "" {
		return shared.JSONError("ValidationException", "pipe name is required", http.StatusBadRequest), nil
	}

	pipe, err := p.store.GetPipe(name, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errPipeNotFound) {
			return shared.JSONError("NotFoundException", fmt.Sprintf("Pipe %s not found", name), http.StatusNotFound), nil
		}
		return nil, err
	}

	p.store.UpdatePipeState(name, shared.DefaultAccountID, "RUNNING") //nolint:errcheck
	pipe.State = "RUNNING"
	return shared.JSONResponse(http.StatusOK, pipeToResponse(pipe))
}

func (p *PipesProvider) handleStopPipe(req *http.Request) (*plugin.Response, error) {
	name := extractPipeNameBeforeAction(req.URL.Path, "stop")
	if name == "" {
		return shared.JSONError("ValidationException", "pipe name is required", http.StatusBadRequest), nil
	}

	pipe, err := p.store.GetPipe(name, shared.DefaultAccountID)
	if err != nil {
		if errors.Is(err, errPipeNotFound) {
			return shared.JSONError("NotFoundException", fmt.Sprintf("Pipe %s not found", name), http.StatusNotFound), nil
		}
		return nil, err
	}

	p.store.UpdatePipeState(name, shared.DefaultAccountID, "STOPPED") //nolint:errcheck
	pipe.State = "STOPPED"
	return shared.JSONResponse(http.StatusOK, pipeToResponse(pipe))
}

func (p *PipesProvider) handleTagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractARNFromTagPath(req.URL.Path)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}

	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	} else {
		params = map[string]any{}
	}

	tags := map[string]string{}
	for _, key := range []string{"tags", "Tags"} {
		if tagsRaw, ok := params[key].(map[string]any); ok {
			for k, v := range tagsRaw {
				if s, ok := v.(string); ok {
					tags[k] = s
				}
			}
			break
		}
	}

	if err := p.store.PutTags(arn, tags); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *PipesProvider) handleListTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := extractARNFromTagPath(req.URL.Path)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}

	tags, err := p.store.GetTags(arn)
	if err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{
		"tags": tags,
	})
}

func (p *PipesProvider) handleUntagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractARNFromTagPath(req.URL.Path)
	if arn == "" {
		return shared.JSONError("ValidationException", "resourceArn is required", http.StatusBadRequest), nil
	}

	tagKeys := req.URL.Query()["tagKeys"]

	if err := p.store.DeleteTags(arn, tagKeys); err != nil {
		return nil, err
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Helpers ---

func pipeToResponse(pipe *Pipe) map[string]any {
	return map[string]any{
		"Name":             pipe.Name,
		"Arn":              pipe.ARN,
		"CurrentState":     pipe.State,
		"DesiredState":     pipe.State,
		"Source":           pipe.Source,
		"Target":           pipe.Target,
		"RoleArn":          pipe.RoleARN,
		"Description":      pipe.Description,
		"Enrichment":       pipe.Enrichment,
		"StateReason":      "",
		"CreationTime":     pipe.CreatedAt.UTC().Format(time.RFC3339),
		"LastModifiedTime": pipe.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func resolvePipeOp(method, path string) string {
	segs := strings.Split(strings.Trim(path, "/"), "/")
	n := len(segs)
	if n == 0 {
		return ""
	}

	// /v1/pipes → ListPipes or CreatePipe (if POST)
	// /v1/pipes/{name} → DescribePipe/UpdatePipe/DeletePipe
	// /v1/pipes/{name}/start → StartPipe
	// /v1/pipes/{name}/stop → StopPipe
	// /tags/{arn} → tag operations

	// Normalize: handle both /v1/pipes and /pipes paths
	start := 0
	if segs[0] == "v1" {
		start = 1
	}
	if start >= n {
		return ""
	}

	if segs[start] == "tags" {
		switch method {
		case "POST":
			return "TagResource"
		case "GET":
			return "ListTagsForResource"
		case "DELETE":
			return "UntagResource"
		}
	}

	if segs[start] != "pipes" {
		return ""
	}

	remaining := n - start - 1 // segments after "pipes"
	switch {
	case remaining == 0 && method == "GET":
		return "ListPipes"
	case remaining == 1 && method == "POST":
		return "CreatePipe"
	case remaining == 1 && method == "GET":
		return "DescribePipe"
	case remaining == 1 && method == "PUT":
		return "UpdatePipe"
	case remaining == 1 && method == "DELETE":
		return "DeletePipe"
	case remaining == 2:
		action := segs[start+2]
		switch action {
		case "start":
			return "StartPipe"
		case "stop":
			return "StopPipe"
		}
	}
	return ""
}

func extractPipeNameFromPath(path string) string {
	// /v1/pipes/{name} or /pipes/{name}
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i, s := range segs {
		if s == "pipes" && i+1 < len(segs) {
			return segs[i+1]
		}
	}
	return ""
}

func extractPipeNameBeforeAction(path, action string) string {
	// /v1/pipes/{name}/start
	segs := strings.Split(strings.Trim(path, "/"), "/")
	for i, s := range segs {
		if s == action && i >= 1 {
			return segs[i-1]
		}
	}
	return ""
}

func extractARNFromTagPath(path string) string {
	// /tags/{arn} — the ARN may contain slashes, so take everything after /tags/
	idx := strings.Index(path, "/tags/")
	if idx < 0 {
		return ""
	}
	return path[idx+6:]
}

func init() {
	plugin.DefaultRegistry.Register("pipes", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &PipesProvider{}
	})
}
