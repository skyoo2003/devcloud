// SPDX-License-Identifier: Apache-2.0

// internal/services/codebuild/provider.go
package codebuild

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

func (p *Provider) ServiceID() string             { return "codebuild" }
func (p *Provider) ServiceName() string           { return "CodeBuild_20161006" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "codebuild"))
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
	// Project
	case "CreateProject":
		return p.createProject(params)
	case "BatchGetProjects":
		return p.batchGetProjects(params)
	case "ListProjects":
		return p.listProjects(params)
	case "UpdateProject":
		return p.updateProject(params)
	case "DeleteProject":
		return p.deleteProject(params)
	case "InvalidateProjectCache":
		return p.invalidateProjectCache(params)
	// Build
	case "StartBuild":
		return p.startBuild(params)
	case "BatchGetBuilds":
		return p.batchGetBuilds(params)
	case "ListBuilds":
		return p.listBuilds(params)
	case "ListBuildsForProject":
		return p.listBuildsForProject(params)
	case "StopBuild":
		return p.stopBuild(params)
	case "RetryBuild":
		return p.retryBuild(params)
	case "BatchDeleteBuilds":
		return p.batchDeleteBuilds(params)
	// ReportGroup
	case "CreateReportGroup":
		return p.createReportGroup(params)
	case "BatchGetReportGroups":
		return p.batchGetReportGroups(params)
	case "ListReportGroups":
		return p.listReportGroups(params)
	case "UpdateReportGroup":
		return p.updateReportGroup(params)
	case "DeleteReportGroup":
		return p.deleteReportGroup(params)
	case "ListReports":
		return p.listReports(params)
	case "ListReportsForReportGroup":
		return p.listReportsForReportGroup(params)
	// Fleet
	case "CreateFleet":
		return p.createFleet(params)
	case "BatchGetFleets":
		return p.batchGetFleets(params)
	case "ListFleets":
		return p.listFleets(params)
	case "UpdateFleet":
		return p.updateFleet(params)
	case "DeleteFleet":
		return p.deleteFleet(params)
	// Source credentials
	case "ImportSourceCredentials":
		return p.importSourceCredentials(params)
	case "ListSourceCredentials":
		return p.listSourceCredentials(params)
	case "DeleteSourceCredentials":
		return p.deleteSourceCredentials(params)
	// Environment images
	case "ListCuratedEnvironmentImages":
		return p.listCuratedEnvironmentImages(params)
	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		// Remaining ops (BuildBatch, Sandbox, CommandExecution, etc.): return success/empty
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	projects, err := p.store.ListProjects()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(projects))
	for _, proj := range projects {
		res = append(res, plugin.Resource{Type: "project", ID: proj.Name, Name: proj.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- Project handlers ----

func (p *Provider) createProject(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "name is required", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	serviceRole, _ := params["serviceRole"].(string)
	timeout := int64(60)
	if t, ok := params["timeoutInMinutes"].(float64); ok {
		timeout = int64(t)
	}
	source := "{}"
	if s, ok := params["source"]; ok {
		b, _ := json.Marshal(s)
		source = string(b)
	}
	artifacts := "{}"
	if a, ok := params["artifacts"]; ok {
		b, _ := json.Marshal(a)
		artifacts = string(b)
	}
	environment := "{}"
	if e, ok := params["environment"]; ok {
		b, _ := json.Marshal(e)
		environment = string(b)
	}
	arn := projectARN(name)
	proj, err := p.store.CreateProject(name, arn, description, source, artifacts, environment, serviceRole, timeout)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "project already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags if provided
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(proj.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"project": projectToMap(proj),
	})
}

func (p *Provider) batchGetProjects(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["names"].([]any)
	projects := make([]any, 0, len(rawNames))
	notFound := make([]string, 0)
	for _, n := range rawNames {
		name, _ := n.(string)
		proj, err := p.store.GetProject(name)
		if err == nil {
			projects = append(projects, projectToMap(proj))
		} else {
			notFound = append(notFound, name)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"projects":         projects,
		"projectsNotFound": notFound,
	})
}

func (p *Provider) listProjects(_ map[string]any) (*plugin.Response, error) {
	projects, err := p.store.ListProjects()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(projects))
	for _, proj := range projects {
		names = append(names, proj.Name)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"projects": names,
	})
}

func (p *Provider) updateProject(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "name is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetProject(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	description, _ := params["description"].(string)
	if description == "" {
		description = existing.Description
	}
	serviceRole, _ := params["serviceRole"].(string)
	if serviceRole == "" {
		serviceRole = existing.ServiceRole
	}
	timeout := existing.Timeout
	if t, ok := params["timeoutInMinutes"].(float64); ok {
		timeout = int64(t)
	}
	source := existing.Source
	if s, ok := params["source"]; ok {
		b, _ := json.Marshal(s)
		source = string(b)
	}
	artifacts := existing.Artifacts
	if a, ok := params["artifacts"]; ok {
		b, _ := json.Marshal(a)
		artifacts = string(b)
	}
	environment := existing.Environment
	if e, ok := params["environment"]; ok {
		b, _ := json.Marshal(e)
		environment = string(b)
	}
	if err := p.store.UpdateProject(name, description, source, artifacts, environment, serviceRole, timeout); err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	updated, err := p.store.GetProject(name)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"project": projectToMap(updated),
	})
}

func (p *Provider) deleteProject(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "name is required", http.StatusBadRequest), nil
	}
	proj, err := p.store.GetProject(name)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(proj.ARN)
	if err := p.store.DeleteProject(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) invalidateProjectCache(params map[string]any) (*plugin.Response, error) {
	name, _ := params["projectName"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "projectName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetProject(name); err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- Build handlers ----

func (p *Provider) startBuild(params map[string]any) (*plugin.Response, error) {
	projectName, _ := params["projectName"].(string)
	if projectName == "" {
		return shared.JSONError("InvalidInputException", "projectName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetProject(projectName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	sourceVersion, _ := params["sourceVersion"].(string)
	id := fmt.Sprintf("%s:%s", projectName, shared.GenerateID("", 32))
	arn := shared.BuildARN("codebuild", "build", id)
	build, err := p.store.CreateBuild(id, arn, projectName, "SUCCEEDED", sourceVersion)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"build": buildToMap(build),
	})
}

func (p *Provider) batchGetBuilds(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["ids"].([]any)
	builds := make([]any, 0, len(rawIDs))
	notFound := make([]string, 0)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		b, err := p.store.GetBuild(id)
		if err == nil {
			builds = append(builds, buildToMap(b))
		} else {
			notFound = append(notFound, id)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"builds":         builds,
		"buildsNotFound": notFound,
	})
}

func (p *Provider) listBuilds(_ map[string]any) (*plugin.Response, error) {
	builds, err := p.store.ListBuilds()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(builds))
	for _, b := range builds {
		ids = append(ids, b.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ids": ids,
	})
}

func (p *Provider) listBuildsForProject(params map[string]any) (*plugin.Response, error) {
	projectName, _ := params["projectName"].(string)
	if projectName == "" {
		return shared.JSONError("InvalidInputException", "projectName is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetProject(projectName); err != nil {
		return shared.JSONError("ResourceNotFoundException", "project not found", http.StatusBadRequest), nil
	}
	builds, err := p.store.ListBuildsForProject(projectName)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(builds))
	for _, b := range builds {
		ids = append(ids, b.ID)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ids": ids,
	})
}

func (p *Provider) stopBuild(params map[string]any) (*plugin.Response, error) {
	id, _ := params["id"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "id is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateBuildStatus(id, "STOPPED"); err != nil {
		return shared.JSONError("ResourceNotFoundException", "build not found", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBuild(id)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"build": buildToMap(b),
	})
}

func (p *Provider) retryBuild(params map[string]any) (*plugin.Response, error) {
	id, _ := params["id"].(string)
	if id == "" {
		return shared.JSONError("InvalidInputException", "id is required", http.StatusBadRequest), nil
	}
	orig, err := p.store.GetBuild(id)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "build not found", http.StatusBadRequest), nil
	}
	newID := fmt.Sprintf("%s:%s", orig.ProjectName, shared.GenerateID("", 32))
	newARN := shared.BuildARN("codebuild", "build", newID)
	newBuild, err := p.store.CreateBuild(newID, newARN, orig.ProjectName, "SUCCEEDED", orig.SourceVersion)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"build": buildToMap(newBuild),
	})
}

func (p *Provider) batchDeleteBuilds(params map[string]any) (*plugin.Response, error) {
	rawIDs, _ := params["ids"].([]any)
	deleted := make([]string, 0, len(rawIDs))
	notDeleted := make([]any, 0)
	for _, raw := range rawIDs {
		id, _ := raw.(string)
		b, err := p.store.GetBuild(id)
		if err == nil {
			_ = b
			deleted = append(deleted, id)
		} else {
			notDeleted = append(notDeleted, map[string]any{"id": id, "statusCode": "BUILD_NOT_DELETED"})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"buildsDeleted":    deleted,
		"buildsNotDeleted": notDeleted,
	})
}

// ---- ReportGroup handlers ----

func (p *Provider) createReportGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "name is required", http.StatusBadRequest), nil
	}
	rgType, _ := params["type"].(string)
	if rgType == "" {
		rgType = "TEST"
	}
	exportConfig := "{}"
	if ec, ok := params["exportConfig"]; ok {
		b, _ := json.Marshal(ec)
		exportConfig = string(b)
	}
	arn := reportGroupARN(name)
	rg, err := p.store.CreateReportGroup(arn, name, rgType, exportConfig)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "report group already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags if provided
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(rg.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reportGroup": reportGroupToMap(rg),
	})
}

func (p *Provider) batchGetReportGroups(params map[string]any) (*plugin.Response, error) {
	rawARNs, _ := params["reportGroupArns"].([]any)
	groups := make([]any, 0, len(rawARNs))
	notFound := make([]string, 0)
	for _, raw := range rawARNs {
		arn, _ := raw.(string)
		rg, err := p.store.GetReportGroup(arn)
		if err == nil {
			groups = append(groups, reportGroupToMap(rg))
		} else {
			notFound = append(notFound, arn)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reportGroups":         groups,
		"reportGroupsNotFound": notFound,
	})
}

func (p *Provider) listReportGroups(_ map[string]any) (*plugin.Response, error) {
	groups, err := p.store.ListReportGroups()
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(groups))
	for _, g := range groups {
		arns = append(arns, g.ARN)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reportGroups": arns,
	})
}

func (p *Provider) updateReportGroup(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["arn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "arn is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetReportGroup(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "report group not found", http.StatusBadRequest), nil
	}
	exportConfig := existing.ExportConfig
	if ec, ok := params["exportConfig"]; ok {
		b, _ := json.Marshal(ec)
		exportConfig = string(b)
	}
	if err := p.store.UpdateReportGroup(arn, exportConfig); err != nil {
		return shared.JSONError("ResourceNotFoundException", "report group not found", http.StatusBadRequest), nil
	}
	updated, err := p.store.GetReportGroup(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reportGroup": reportGroupToMap(updated),
	})
}

func (p *Provider) deleteReportGroup(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["arn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "arn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetReportGroup(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "report group not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteReportGroup(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "report group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listReports(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reports": []any{},
	})
}

func (p *Provider) listReportsForReportGroup(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["reportGroupArn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "reportGroupArn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetReportGroup(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "report group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"reports": []any{},
	})
}

// ---- Fleet handlers ----

func (p *Provider) createFleet(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return shared.JSONError("InvalidInputException", "name is required", http.StatusBadRequest), nil
	}
	baseCapacity := int64(1)
	if bc, ok := params["baseCapacity"].(float64); ok {
		baseCapacity = int64(bc)
	}
	computeType, _ := params["computeType"].(string)
	if computeType == "" {
		computeType = "BUILD_GENERAL1_SMALL"
	}
	environmentType, _ := params["environmentType"].(string)
	if environmentType == "" {
		environmentType = "LINUX_CONTAINER"
	}
	arn := fleetARN(name)
	fleet, err := p.store.CreateFleet(arn, name, baseCapacity, computeType, environmentType)
	if err != nil {
		if isUnique(err) {
			return shared.JSONError("ResourceAlreadyExistsException", "fleet already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	// Handle tags if provided
	if rawTags, ok := params["tags"].([]any); ok {
		_ = p.store.tags.AddTags(fleet.ARN, parseTags(rawTags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"fleet": fleetToMap(fleet),
	})
}

func (p *Provider) batchGetFleets(params map[string]any) (*plugin.Response, error) {
	rawNames, _ := params["names"].([]any)
	fleets := make([]any, 0, len(rawNames))
	notFound := make([]string, 0)
	for _, raw := range rawNames {
		name, _ := raw.(string)
		f, err := p.store.GetFleetByName(name)
		if err == nil {
			fleets = append(fleets, fleetToMap(f))
		} else {
			notFound = append(notFound, name)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"fleets":         fleets,
		"fleetsNotFound": notFound,
	})
}

func (p *Provider) listFleets(_ map[string]any) (*plugin.Response, error) {
	fleets, err := p.store.ListFleets()
	if err != nil {
		return nil, err
	}
	arns := make([]string, 0, len(fleets))
	for _, f := range fleets {
		arns = append(arns, f.ARN)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"fleets": arns,
	})
}

func (p *Provider) updateFleet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["fleet"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "fleet arn is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetFleet(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "fleet not found", http.StatusBadRequest), nil
	}
	baseCapacity := existing.BaseCapacity
	if bc, ok := params["baseCapacity"].(float64); ok {
		baseCapacity = int64(bc)
	}
	computeType := existing.ComputeType
	if ct, ok := params["computeType"].(string); ok && ct != "" {
		computeType = ct
	}
	environmentType := existing.EnvironmentType
	if et, ok := params["environmentType"].(string); ok && et != "" {
		environmentType = et
	}
	if err := p.store.UpdateFleet(arn, baseCapacity, computeType, environmentType); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fleet not found", http.StatusBadRequest), nil
	}
	updated, err := p.store.GetFleet(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"fleet": fleetToMap(updated),
	})
}

func (p *Provider) deleteFleet(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["fleet"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "fleet arn is required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetFleet(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fleet not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn)
	if err := p.store.DeleteFleet(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "fleet not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ---- SourceCredentials handlers ----

func (p *Provider) importSourceCredentials(params map[string]any) (*plugin.Response, error) {
	serverType, _ := params["serverType"].(string)
	if serverType == "" {
		return shared.JSONError("InvalidInputException", "serverType is required", http.StatusBadRequest), nil
	}
	authType, _ := params["authType"].(string)
	if authType == "" {
		return shared.JSONError("InvalidInputException", "authType is required", http.StatusBadRequest), nil
	}
	token, _ := params["token"].(string)
	arn := credentialARN(serverType)
	cred, err := p.store.UpsertSourceCredential(arn, serverType, authType, token)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"arn": cred.ARN,
	})
}

func (p *Provider) listSourceCredentials(_ map[string]any) (*plugin.Response, error) {
	creds, err := p.store.ListSourceCredentials()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(creds))
	for _, c := range creds {
		list = append(list, map[string]any{
			"arn":        c.ARN,
			"serverType": c.ServerType,
			"authType":   c.AuthType,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"credentialsInfos": list,
	})
}

func (p *Provider) deleteSourceCredentials(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["arn"].(string)
	if arn == "" {
		return shared.JSONError("InvalidInputException", "arn is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSourceCredential(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "source credential not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"arn": arn,
	})
}

// ---- Environment images ----

func (p *Provider) listCuratedEnvironmentImages(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"platforms": []any{
			map[string]any{
				"platform": "AMAZON_LINUX_2",
				"languages": []any{
					map[string]any{
						"language": "GOLANG",
						"images": []any{
							map[string]any{
								"name":        "aws/codebuild/amazonlinux2-x86_64-standard:4.0",
								"description": "AWS CodeBuild - Amazon Linux 2",
								"versions":    []string{"aws/codebuild/amazonlinux2-x86_64-standard:4.0"},
							},
						},
					},
				},
			},
		},
	})
}

// ---- Tag handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["resourceArn"].(string)
	if resourceARN == "" {
		return shared.JSONError("InvalidInputException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["tags"].([]any)
	if err := p.store.tags.AddTags(resourceARN, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["resourceArn"].(string)
	if resourceARN == "" {
		return shared.JSONError("InvalidInputException", "resourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["tagKeys"].([]any)
	keys := make([]string, 0, len(rawKeys))
	for _, k := range rawKeys {
		if s, ok := k.(string); ok {
			keys = append(keys, s)
		}
	}
	if err := p.store.tags.RemoveTags(resourceARN, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	resourceARN, _ := params["resourceArn"].(string)
	if resourceARN == "" {
		return shared.JSONError("InvalidInputException", "resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(resourceARN)
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

func projectARN(name string) string {
	return shared.BuildARN("codebuild", "project", name)
}

func reportGroupARN(name string) string {
	return shared.BuildARN("codebuild", "report-group", name)
}

func fleetARN(name string) string {
	return shared.BuildARN("codebuild", "fleet", name)
}

func credentialARN(serverType string) string {
	return shared.BuildARN("codebuild", "token", strings.ToLower(serverType))
}

func projectToMap(p *Project) map[string]any {
	var source, artifacts, environment any
	_ = json.Unmarshal([]byte(p.Source), &source)
	_ = json.Unmarshal([]byte(p.Artifacts), &artifacts)
	_ = json.Unmarshal([]byte(p.Environment), &environment)
	return map[string]any{
		"name":             p.Name,
		"arn":              p.ARN,
		"description":      p.Description,
		"source":           source,
		"artifacts":        artifacts,
		"environment":      environment,
		"serviceRole":      p.ServiceRole,
		"timeoutInMinutes": p.Timeout,
		"created":          p.CreatedAt.Unix(),
		"lastModified":     p.UpdatedAt.Unix(),
	}
}

func buildToMap(b *Build) map[string]any {
	var phases, logs any
	_ = json.Unmarshal([]byte(b.Phases), &phases)
	_ = json.Unmarshal([]byte(b.Logs), &logs)
	return map[string]any{
		"id":            b.ID,
		"arn":           b.ARN,
		"projectName":   b.ProjectName,
		"buildStatus":   b.Status,
		"sourceVersion": b.SourceVersion,
		"startTime":     b.StartTime.Unix(),
		"endTime":       b.EndTime,
		"phases":        phases,
		"logs":          logs,
	}
}

func reportGroupToMap(rg *ReportGroup) map[string]any {
	var exportConfig any
	_ = json.Unmarshal([]byte(rg.ExportConfig), &exportConfig)
	return map[string]any{
		"arn":          rg.ARN,
		"name":         rg.Name,
		"type":         rg.Type,
		"exportConfig": exportConfig,
		"created":      rg.CreatedAt.Unix(),
	}
}

func fleetToMap(f *Fleet) map[string]any {
	return map[string]any{
		"arn":              f.ARN,
		"name":             f.Name,
		"baseCapacity":     f.BaseCapacity,
		"computeType":      f.ComputeType,
		"environmentType":  f.EnvironmentType,
		"fleetServiceRole": "",
		"status": map[string]any{
			"statusCode": f.Status,
		},
		"created": f.CreatedAt.Unix(),
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
