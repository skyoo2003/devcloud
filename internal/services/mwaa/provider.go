// SPDX-License-Identifier: Apache-2.0

// internal/services/mwaa/provider.go
package mwaa

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

// Provider implements the MWAA service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "mwaa" }
func (p *Provider) ServiceName() string           { return "MWAA" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "mwaa"))
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
	// Environment
	case "CreateEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.createEnvironment(name, params)
	case "GetEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.getEnvironment(name)
	case "ListEnvironments":
		return p.listEnvironments()
	case "UpdateEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.updateEnvironment(name, params)
	case "DeleteEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.deleteEnvironment(name)
	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	// Tokens
	case "CreateCliToken":
		name := extractPathParam(req.URL.Path, "clitoken")
		return p.createCliToken(name)
	case "CreateWebLoginToken":
		name := extractPathParam(req.URL.Path, "webtoken")
		return p.createWebLoginToken(name)
	// Metrics / REST API
	case "PublishMetrics":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "InvokeRestApi":
		return p.invokeRestApi(req, params)
	// DAGs
	case "ListDags":
		return p.listDags(req)
	case "GetDag":
		return p.getDag(req)
	case "CreateDag":
		return p.createDag(req, params)
	case "DeleteDag":
		return p.deleteDag(req)
	case "PauseDag":
		return p.pauseDag(req, params)
	// DAG Runs
	case "ListDagRuns":
		return p.listDagRuns(req)
	case "GetDagRun":
		return p.getDagRun(req)
	case "CreateDagRun":
		return p.createDagRun(req, params)
	case "DeleteDagRun":
		return p.deleteDagRun(req)
	// Variables
	case "CreateVariables":
		return p.createVariables(req, params)
	case "UpdateVariables":
		return p.updateVariables(req, params)
	case "DeleteVariables":
		return p.deleteVariables(req, params)
	case "GetVariables":
		return p.getVariables(req, params)
	case "ListVariables":
		return p.listVariables(req)
	// Status / Stats / Logs / Health
	case "GetEnvironmentStatus":
		name := extractPathParam(req.URL.Path, "environments")
		return p.getEnvironmentStatus(name)
	case "GetEnvironmentConfig":
		name := extractPathParam(req.URL.Path, "environments")
		return p.getEnvironmentConfig(name)
	case "RestartEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.restartEnvironment(name)
	case "StartEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.startEnvironment(name)
	case "StopEnvironment":
		name := extractPathParam(req.URL.Path, "environments")
		return p.stopEnvironment(name)
	case "GetEnvironmentLogs":
		name := extractPathParam(req.URL.Path, "environments")
		return p.getEnvironmentLogs(name)
	case "DescribeClusters":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Clusters": []any{}})
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

	// Environments
	case n >= 1 && seg[0] == "environments":
		if n == 1 {
			switch method {
			case http.MethodGet:
				return "ListEnvironments"
			}
		}
		if n == 2 {
			switch method {
			case http.MethodPut:
				return "CreateEnvironment"
			case http.MethodGet:
				return "GetEnvironment"
			case http.MethodPatch:
				return "UpdateEnvironment"
			case http.MethodDelete:
				return "DeleteEnvironment"
			}
		}
		if n == 3 {
			// /environments/{name}/{subresource}
			switch seg[2] {
			case "status":
				if method == http.MethodGet {
					return "GetEnvironmentStatus"
				}
			case "config":
				if method == http.MethodGet {
					return "GetEnvironmentConfig"
				}
			case "restart":
				if method == http.MethodPost {
					return "RestartEnvironment"
				}
			case "start":
				if method == http.MethodPost {
					return "StartEnvironment"
				}
			case "stop":
				if method == http.MethodPost {
					return "StopEnvironment"
				}
			case "logs":
				if method == http.MethodGet {
					return "GetEnvironmentLogs"
				}
			case "dags":
				switch method {
				case http.MethodGet:
					return "ListDags"
				case http.MethodPost:
					return "CreateDag"
				}
			case "dagruns":
				switch method {
				case http.MethodGet:
					return "ListDagRuns"
				case http.MethodPost:
					return "CreateDagRun"
				}
			case "variables":
				switch method {
				case http.MethodGet:
					return "ListVariables"
				case http.MethodPost:
					return "CreateVariables"
				case http.MethodPatch:
					return "UpdateVariables"
				case http.MethodDelete:
					return "DeleteVariables"
				case http.MethodPut:
					return "GetVariables"
				}
			}
		}
		if n == 4 {
			// /environments/{name}/dags/{dagId}
			// /environments/{name}/variables/{key}
			switch seg[2] {
			case "dags":
				switch method {
				case http.MethodGet:
					return "GetDag"
				case http.MethodDelete:
					return "DeleteDag"
				case http.MethodPatch:
					return "PauseDag"
				}
			case "dagruns":
				switch method {
				case http.MethodGet:
					return "ListDagRuns"
				case http.MethodPost:
					return "CreateDagRun"
				}
			case "variables":
				switch method {
				case http.MethodGet:
					return "GetVariables"
				}
			}
		}
		if n == 5 {
			// /environments/{name}/dagruns/{dagId}/{runId}
			if seg[2] == "dagruns" {
				switch method {
				case http.MethodGet:
					return "GetDagRun"
				case http.MethodDelete:
					return "DeleteDagRun"
				}
			}
		}

	// CLI token: /clitoken/{name}
	case n >= 1 && seg[0] == "clitoken":
		if n == 2 && method == http.MethodPost {
			return "CreateCliToken"
		}

	// Web token: /webtoken/{name}
	case n >= 1 && seg[0] == "webtoken":
		if n == 2 && method == http.MethodPost {
			return "CreateWebLoginToken"
		}

	// Metrics: /metrics/environments/{name}
	case n >= 1 && seg[0] == "metrics":
		return "PublishMetrics"

	// REST API: /restapi/{name}
	case n >= 1 && seg[0] == "restapi":
		return "InvokeRestApi"

	// Clusters
	case n >= 1 && seg[0] == "clusters":
		if method == http.MethodGet {
			return "DescribeClusters"
		}
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	envs, err := p.store.ListEnvironments()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(envs))
	for _, e := range envs {
		res = append(res, plugin.Resource{Type: "mwaa-environment", ID: e.Name, Name: e.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Environment CRUD ---

func (p *Provider) createEnvironment(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		if n, ok := params["Name"].(string); ok {
			name = n
		}
	}
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}

	airflowVersion := "2.8.1"
	if v, ok := params["AirflowVersion"].(string); ok && v != "" {
		airflowVersion = v
	}
	envClass := "mw1.small"
	if v, ok := params["EnvironmentClass"].(string); ok && v != "" {
		envClass = v
	}
	maxWorkers := 10
	if v, ok := params["MaxWorkers"].(float64); ok {
		maxWorkers = int(v)
	}
	minWorkers := 1
	if v, ok := params["MinWorkers"].(float64); ok {
		minWorkers = int(v)
	}
	sourceBucket, _ := params["SourceBucketArn"].(string)
	dagPath := "dags"
	if v, ok := params["DagS3Path"].(string); ok && v != "" {
		dagPath = v
	}
	executionRole, _ := params["ExecutionRoleArn"].(string)
	webserverURL := fmt.Sprintf("%s.airflow.amazonaws.com", name)

	arn := shared.BuildARN("airflow", "environment", name)

	e := &Environment{
		Name:             name,
		ARN:              arn,
		Status:           "AVAILABLE",
		AirflowVersion:   airflowVersion,
		EnvironmentClass: envClass,
		MaxWorkers:       maxWorkers,
		MinWorkers:       minWorkers,
		SourceBucket:     sourceBucket,
		DagS3Path:        dagPath,
		ExecutionRole:    executionRole,
		WebserverURL:     webserverURL,
		Config:           "{}",
	}

	if err := p.store.CreateEnvironment(e); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "environment already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	if rawTags, ok := params["Tags"].(map[string]any); ok {
		tags := make(map[string]string)
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		_ = p.store.tags.AddTags(arn, tags)
	}

	return shared.JSONResponse(http.StatusOK, map[string]any{"Arn": arn})
}

func (p *Provider) getEnvironment(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(e.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Environment": environmentToMap(e, tags),
	})
}

func (p *Provider) listEnvironments() (*plugin.Response, error) {
	envs, err := p.store.ListEnvironments()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(envs))
	for _, e := range envs {
		names = append(names, e.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Environments": names,
	})
}

func (p *Provider) updateEnvironment(name string, params map[string]any) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateEnvironment(name, params); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Arn": e.ARN})
}

func (p *Provider) deleteEnvironment(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}
	e, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	_ = p.store.tags.DeleteAllTags(e.ARN)
	if err := p.store.DeleteEnvironment(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Lifecycle ---

func (p *Provider) getEnvironmentStatus(name string) (*plugin.Response, error) {
	e, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Name":   e.Name,
		"Status": e.Status,
	})
}

func (p *Provider) getEnvironmentConfig(name string) (*plugin.Response, error) {
	e, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	var cfg map[string]any
	_ = json.Unmarshal([]byte(e.Config), &cfg)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AirflowConfigurationOptions": cfg,
	})
}

func (p *Provider) restartEnvironment(name string) (*plugin.Response, error) {
	_, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "RESTARTING"})
}

func (p *Provider) startEnvironment(name string) (*plugin.Response, error) {
	_, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "AVAILABLE"})
}

func (p *Provider) stopEnvironment(name string) (*plugin.Response, error) {
	_, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Status": "PAUSED"})
}

func (p *Provider) getEnvironmentLogs(name string) (*plugin.Response, error) {
	_, err := p.store.GetEnvironment(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LogStreams": []any{},
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string)
	for k, v := range rawTags {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}
	if err := p.store.tags.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := extractPathParam(req.URL.Path, "tags")
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
	arn := extractPathParam(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Tokens ---

func (p *Provider) createCliToken(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CliToken":          shared.GenerateID("", 64),
		"WebServerHostname": fmt.Sprintf("%s.airflow.amazonaws.com", name),
	})
}

func (p *Provider) createWebLoginToken(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "environment name is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"WebToken":          shared.GenerateID("", 64),
		"WebServerHostname": fmt.Sprintf("%s.airflow.amazonaws.com", name),
	})
}

func (p *Provider) invokeRestApi(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := extractPathParam(req.URL.Path, "restapi")
	if name == "" {
		if n, ok := params["Name"].(string); ok {
			name = n
		}
	}
	method, _ := params["Method"].(string)
	path, _ := params["Path"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RestApiStatusCode": 200,
		"RestApiResponse": map[string]any{
			"environment": name,
			"method":      method,
			"path":        path,
		},
	})
}

// --- DAGs ---

func (p *Provider) listDags(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	dags, err := p.store.ListDags(env)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(dags))
	for _, d := range dags {
		list = append(list, dagToMap(&d))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Dags": list})
}

func (p *Provider) getDag(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	dagID := extractPathParam(req.URL.Path, "dags")
	d, err := p.store.GetDag(env, dagID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "dag not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, dagToMap(d))
}

func (p *Provider) createDag(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	dagID, _ := params["DagId"].(string)
	if dagID == "" {
		return shared.JSONError("ValidationException", "DagId is required", http.StatusBadRequest), nil
	}
	fileURI, _ := params["FileUri"].(string)
	d := &DAG{
		EnvironmentName: env,
		DagID:           dagID,
		FileURI:         fileURI,
		Status:          "active",
	}
	if err := p.store.CreateDag(d); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "dag already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, dagToMap(d))
}

func (p *Provider) deleteDag(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	dagID := extractPathParam(req.URL.Path, "dags")
	if err := p.store.DeleteDag(env, dagID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "dag not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) pauseDag(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	dagID := extractPathParam(req.URL.Path, "dags")
	paused := true
	if v, ok := params["Paused"].(bool); ok {
		paused = v
	}
	if err := p.store.PauseDag(env, dagID, paused); err != nil {
		return shared.JSONError("ResourceNotFoundException", "dag not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DagId": dagID, "Paused": paused})
}

// --- DAG Runs ---

func (p *Provider) listDagRuns(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	dagID := extractPathParam(req.URL.Path, "dagruns")
	runs, err := p.store.ListDagRuns(env, dagID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(runs))
	for _, r := range runs {
		list = append(list, dagRunToMap(&r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DagRuns": list})
}

func (p *Provider) getDagRun(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	var dagID, runID string
	for i, seg := range parts {
		if seg == "dagruns" && i+2 < len(parts) {
			dagID = parts[i+1]
			runID = parts[i+2]
			break
		}
	}
	r, err := p.store.GetDagRun(env, dagID, runID)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "dag run not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, dagRunToMap(r))
}

func (p *Provider) createDagRun(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	dagID, _ := params["DagId"].(string)
	if dagID == "" {
		dagID = extractPathParam(req.URL.Path, "dagruns")
	}
	if dagID == "" {
		return shared.JSONError("ValidationException", "DagId is required", http.StatusBadRequest), nil
	}
	runID, _ := params["RunId"].(string)
	confJSON := "{}"
	if c, ok := params["Conf"]; ok {
		b, _ := json.Marshal(c)
		confJSON = string(b)
	}
	state := "queued"
	if s, ok := params["State"].(string); ok && s != "" {
		state = s
	}
	r := &DagRun{
		EnvironmentName: env,
		DagID:           dagID,
		RunID:           runID,
		State:           state,
		ExecutionDate:   time.Now().UTC().Format(time.RFC3339),
		Conf:            confJSON,
	}
	if err := p.store.CreateDagRun(r); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "dag run already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, dagRunToMap(r))
}

func (p *Provider) deleteDagRun(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	var dagID, runID string
	for i, seg := range parts {
		if seg == "dagruns" && i+2 < len(parts) {
			dagID = parts[i+1]
			runID = parts[i+2]
			break
		}
	}
	if err := p.store.DeleteDagRun(env, dagID, runID); err != nil {
		return shared.JSONError("ResourceNotFoundException", "dag run not found", http.StatusNotFound), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- Variables ---

func (p *Provider) createVariables(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	created := p.bulkSetVariables(env, params)
	return shared.JSONResponse(http.StatusOK, map[string]any{"CreatedVariables": created})
}

func (p *Provider) updateVariables(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	updated := p.bulkSetVariables(env, params)
	return shared.JSONResponse(http.StatusOK, map[string]any{"UpdatedVariables": updated})
}

func (p *Provider) bulkSetVariables(env string, params map[string]any) []string {
	names := []string{}
	if raw, ok := params["Variables"].([]any); ok {
		for _, v := range raw {
			vm, ok := v.(map[string]any)
			if !ok {
				continue
			}
			key, _ := vm["Key"].(string)
			if key == "" {
				key, _ = vm["Name"].(string)
			}
			if key == "" {
				continue
			}
			value, _ := vm["Value"].(string)
			desc, _ := vm["Description"].(string)
			enc := false
			if e, ok := vm["IsEncrypted"].(bool); ok {
				enc = e
			}
			_ = p.store.SetVariable(&Variable{
				EnvironmentName: env,
				Key:             key,
				Value:           value,
				IsEncrypted:     enc,
				Description:     desc,
			})
			names = append(names, key)
		}
	}
	return names
}

func (p *Provider) deleteVariables(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	deleted := []string{}
	if raw, ok := params["VariableNames"].([]any); ok {
		for _, n := range raw {
			if key, ok := n.(string); ok {
				p.store.DeleteVariable(env, key) //nolint:errcheck
				deleted = append(deleted, key)
			}
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DeletedVariables": deleted})
}

func (p *Provider) getVariables(req *http.Request, params map[string]any) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}

	names := []string{}
	if raw, ok := params["VariableNames"].([]any); ok {
		for _, n := range raw {
			if key, ok := n.(string); ok {
				names = append(names, key)
			}
		}
	}
	// single-variable via path param
	if key := extractPathParam(req.URL.Path, "variables"); key != "" && len(names) == 0 {
		names = []string{key}
	}

	list := []map[string]any{}
	if len(names) == 0 {
		vars, _ := p.store.ListVariables(env)
		for _, v := range vars {
			list = append(list, variableToMap(&v))
		}
	} else {
		for _, key := range names {
			v, err := p.store.GetVariable(env, key)
			if err == nil {
				list = append(list, variableToMap(v))
			}
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Variables": list})
}

func (p *Provider) listVariables(req *http.Request) (*plugin.Response, error) {
	env := extractPathParam(req.URL.Path, "environments")
	if _, err := p.store.GetEnvironment(env); err != nil {
		return shared.JSONError("ResourceNotFoundException", "environment not found", http.StatusNotFound), nil
	}
	vars, err := p.store.ListVariables(env)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(vars))
	for _, v := range vars {
		list = append(list, variableToMap(&v))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Variables": list})
}

// --- Helpers ---

func environmentToMap(e *Environment, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"Name":             e.Name,
		"Arn":              e.ARN,
		"Status":           e.Status,
		"AirflowVersion":   e.AirflowVersion,
		"EnvironmentClass": e.EnvironmentClass,
		"MaxWorkers":       e.MaxWorkers,
		"MinWorkers":       e.MinWorkers,
		"SourceBucketArn":  e.SourceBucket,
		"DagS3Path":        e.DagS3Path,
		"ExecutionRoleArn": e.ExecutionRole,
		"WebserverUrl":     e.WebserverURL,
		"CreatedAt":        e.CreatedAt.Unix(),
		"LastUpdated": map[string]any{
			"Status":    "SUCCESS",
			"CreatedAt": e.UpdatedAt.Unix(),
		},
		"Tags": tags,
	}
}

func dagToMap(d *DAG) map[string]any {
	return map[string]any{
		"DagId":           d.DagID,
		"EnvironmentName": d.EnvironmentName,
		"FileUri":         d.FileURI,
		"Status":          d.Status,
		"Paused":          d.Paused,
		"CreatedAt":       d.CreatedAt.Unix(),
	}
}

func dagRunToMap(r *DagRun) map[string]any {
	var conf map[string]any
	_ = json.Unmarshal([]byte(r.Conf), &conf)
	return map[string]any{
		"DagId":           r.DagID,
		"EnvironmentName": r.EnvironmentName,
		"RunId":           r.RunID,
		"State":           r.State,
		"ExecutionDate":   r.ExecutionDate,
		"Conf":            conf,
	}
}

func variableToMap(v *Variable) map[string]any {
	return map[string]any{
		"Key":         v.Key,
		"Value":       v.Value,
		"IsEncrypted": v.IsEncrypted,
		"Description": v.Description,
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

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
