// SPDX-License-Identifier: Apache-2.0

// Package scheduler implements AWS EventBridge Scheduler.
package scheduler

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

// SchedulerProvider implements plugin.ServicePlugin for EventBridge Scheduler.
type SchedulerProvider struct {
	store *Store
}

func (p *SchedulerProvider) ServiceID() string             { return "scheduler" }
func (p *SchedulerProvider) ServiceName() string           { return "AmazonEventBridgeScheduler" }
func (p *SchedulerProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *SchedulerProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "scheduler"))
	return err
}

func (p *SchedulerProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *SchedulerProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		json.Unmarshal(body, &params)
	}
	if params == nil {
		params = map[string]any{}
	}

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// Schedules
	case "CreateSchedule":
		return p.createSchedule(req, params)
	case "GetSchedule":
		return p.getSchedule(req)
	case "UpdateSchedule":
		return p.updateSchedule(req, params)
	case "DeleteSchedule":
		return p.deleteSchedule(req)
	case "ListSchedules":
		return p.listSchedules(req)
	case "EnableSchedule":
		return p.enableSchedule(req)
	case "DisableSchedule":
		return p.disableSchedule(req)
	// Groups
	case "CreateScheduleGroup":
		return p.createScheduleGroup(req, params)
	case "GetScheduleGroup":
		return p.getScheduleGroup(req)
	case "UpdateScheduleGroup":
		return p.updateScheduleGroup(req, params)
	case "DeleteScheduleGroup":
		return p.deleteScheduleGroup(req)
	case "ListScheduleGroups":
		return p.listScheduleGroups(req)
	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	// Rate Limiting
	case "PutRateLimit":
		return p.putRateLimit(req, params)
	case "GetRateLimit":
		return p.getRateLimit(req)
	case "DeleteRateLimit":
		return p.deleteRateLimit(req)
	case "ListRateLimits":
		return p.listRateLimits()
	case "BatchCheckMaxRateLimits":
		return p.batchCheckMaxRateLimits(params)
	// Cross-schedule operations
	case "ListFleetSchedules":
		return p.listFleetSchedules(req)
	case "BatchCreateSchedules":
		return p.batchCreateSchedules(params)
	case "BatchDeleteSchedules":
		return p.batchDeleteSchedules(params)
	case "BatchUpdateSchedules":
		return p.batchUpdateSchedules(params)
	case "PreviewSchedule":
		return p.previewSchedule(params)
	case "ValidateScheduleExpression":
		return p.validateScheduleExpression(params)
	// Connection (future-proofing)
	case "ListConnections":
		return jsonResponse(http.StatusOK, map[string]any{"Connections": []any{}})
	// Metrics / observability
	case "GetScheduleMetrics":
		return p.getScheduleMetrics(req)
	case "ListScheduleMetrics":
		return p.listScheduleMetrics(req)
	case "GetScheduleGroupMetrics":
		return p.getScheduleGroupMetrics(req)
	case "GetScheduleAuditLogs":
		return jsonResponse(http.StatusOK, map[string]any{"Entries": []any{}})
	case "ListScheduleAuditLogs":
		return jsonResponse(http.StatusOK, map[string]any{"Entries": []any{}})
	// Lock
	case "LockSchedule":
		return p.lockSchedule(req)
	case "UnlockSchedule":
		return p.unlockSchedule(req)
	// Execution
	case "GetScheduleExecutionStatus":
		return p.getScheduleExecutionStatus(req)
	case "ListScheduleExecutions":
		return jsonResponse(http.StatusOK, map[string]any{"Executions": []any{}})
	case "StopScheduleExecution":
		return jsonResponse(http.StatusOK, map[string]any{})
	default:
		return jsonError("ResourceNotFoundException", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func (p *SchedulerProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	schedules, err := p.store.ListSchedules("", "", "")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(schedules))
	for _, s := range schedules {
		res = append(res, plugin.Resource{Type: "schedule", ID: s.ARN, Name: s.Name})
	}
	return res, nil
}

func (p *SchedulerProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Schedule handlers ---

func (p *SchedulerProvider) createSchedule(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	if groupNameParam, ok := params["GroupName"].(string); ok && groupNameParam != "" {
		groupName = groupNameParam
	}
	expr, _ := params["ScheduleExpression"].(string)
	if expr == "" {
		return jsonError("ValidationException", "ScheduleExpression is required", http.StatusBadRequest), nil
	}
	targetJSON := "{}"
	if t, ok := params["Target"]; ok {
		b, _ := json.Marshal(t)
		targetJSON = string(b)
	}
	state := "ENABLED"
	if s, ok := params["State"].(string); ok && s != "" {
		state = s
	}
	flexJSON := `{"Mode":"OFF"}`
	if fw, ok := params["FlexibleTimeWindow"]; ok {
		b, _ := json.Marshal(fw)
		flexJSON = string(b)
	}
	description, _ := params["Description"].(string)

	sc, err := p.store.CreateSchedule(name, groupName, expr, targetJSON, state, flexJSON, description)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return jsonError("ConflictException", "schedule already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return jsonResponse(http.StatusOK, map[string]any{"ScheduleArn": sc.ARN})
}

func (p *SchedulerProvider) getSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	sc, err := p.store.GetSchedule(name, groupName)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, scheduleToMap(sc))
}

func (p *SchedulerProvider) updateSchedule(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	if groupNameParam, ok := params["GroupName"].(string); ok && groupNameParam != "" {
		groupName = groupNameParam
	}

	existing, err := p.store.GetSchedule(name, groupName)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}

	expr := existing.ScheduleExpression
	if e, ok := params["ScheduleExpression"].(string); ok && e != "" {
		expr = e
	}
	targetJSON := existing.Target
	if t, ok := params["Target"]; ok {
		b, _ := json.Marshal(t)
		targetJSON = string(b)
	}
	state := existing.State
	if s, ok := params["State"].(string); ok && s != "" {
		state = s
	}
	flexJSON := existing.FlexibleTimeWindow
	if fw, ok := params["FlexibleTimeWindow"]; ok {
		b, _ := json.Marshal(fw)
		flexJSON = string(b)
	}
	description := existing.Description
	if d, ok := params["Description"].(string); ok {
		description = d
	}

	if err := p.store.UpdateSchedule(name, groupName, expr, targetJSON, state, flexJSON, description); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"ScheduleArn": existing.ARN})
}

func (p *SchedulerProvider) deleteSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	if err := p.store.DeleteSchedule(name, groupName); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *SchedulerProvider) listSchedules(req *http.Request) (*plugin.Response, error) {
	groupName := req.URL.Query().Get("ScheduleGroup")
	prefix := req.URL.Query().Get("NamePrefix")
	state := req.URL.Query().Get("State")

	schedules, err := p.store.ListSchedules(groupName, prefix, state)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(schedules))
	for _, sc := range schedules {
		var target map[string]any
		json.Unmarshal([]byte(sc.Target), &target)
		list = append(list, map[string]any{
			"Arn":                sc.ARN,
			"Name":               sc.Name,
			"GroupName":          sc.GroupName,
			"ScheduleExpression": sc.ScheduleExpression,
			"State":              sc.State,
			"Target":             target,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"Schedules": list,
		"NextToken": nil,
	})
}

func (p *SchedulerProvider) enableSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	existing, err := p.store.GetSchedule(name, groupName)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateSchedule(name, groupName, existing.ScheduleExpression, existing.Target, "ENABLED", existing.FlexibleTimeWindow, existing.Description); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"ScheduleArn": existing.ARN})
}

func (p *SchedulerProvider) disableSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	existing, err := p.store.GetSchedule(name, groupName)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	if err := p.store.UpdateSchedule(name, groupName, existing.ScheduleExpression, existing.Target, "DISABLED", existing.FlexibleTimeWindow, existing.Description); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"ScheduleArn": existing.ARN})
}

// --- Group handlers ---

func (p *SchedulerProvider) createScheduleGroup(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := groupName(req.URL.Path)
	if name == "" {
		if n, ok := params["Name"].(string); ok {
			name = n
		}
	}
	if name == "" {
		return jsonError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	g, err := p.store.CreateGroup(name)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return jsonError("ConflictException", "group already exists", http.StatusConflict), nil
		}
		return nil, err
	}

	rawTags, _ := params["Tags"].(map[string]any)
	if len(rawTags) > 0 {
		tags := make(map[string]string)
		for k, v := range rawTags {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		p.store.tags.AddTags(g.ARN, tags)
	}

	return jsonResponse(http.StatusOK, map[string]any{"ScheduleGroupArn": g.ARN})
}

func (p *SchedulerProvider) getScheduleGroup(req *http.Request) (*plugin.Response, error) {
	name := groupName(req.URL.Path)
	g, err := p.store.GetGroup(name)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule group not found", http.StatusNotFound), nil
	}
	tags, _ := p.store.tags.ListTags(g.ARN)
	return jsonResponse(http.StatusOK, map[string]any{
		"Arn":                  g.ARN,
		"Name":                 g.Name,
		"State":                g.State,
		"CreationDate":         g.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"LastModificationDate": g.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"Tags":                 tags,
	})
}

func (p *SchedulerProvider) updateScheduleGroup(req *http.Request, params map[string]any) (*plugin.Response, error) {
	name := groupName(req.URL.Path)
	state, _ := params["State"].(string)
	g, err := p.store.UpdateGroup(name, state)
	if err != nil {
		return jsonError("ResourceNotFoundException", "schedule group not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ScheduleGroupArn": g.ARN,
	})
}

func (p *SchedulerProvider) deleteScheduleGroup(req *http.Request) (*plugin.Response, error) {
	name := groupName(req.URL.Path)
	if err := p.store.DeleteGroup(name); err != nil {
		return jsonError("ResourceNotFoundException", "schedule group not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *SchedulerProvider) listScheduleGroups(req *http.Request) (*plugin.Response, error) {
	prefix := req.URL.Query().Get("NamePrefix")
	groups, err := p.store.ListGroups(prefix)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		list = append(list, map[string]any{
			"Arn":          g.ARN,
			"Name":         g.Name,
			"State":        g.State,
			"CreationDate": g.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ScheduleGroups": list,
		"NextToken":      nil,
	})
}

// --- Tag handlers ---

func (p *SchedulerProvider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := tagARN(req.URL.Path)
	if arn == "" {
		return jsonError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string)
	for k, v := range rawTags {
		if s, ok := v.(string); ok {
			tags[k] = s
		}
	}
	p.store.tags.AddTags(arn, tags)
	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *SchedulerProvider) untagResource(req *http.Request) (*plugin.Response, error) {
	arn := tagARN(req.URL.Path)
	keys := req.URL.Query()["TagKeys"]
	p.store.tags.RemoveTags(arn, keys)
	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *SchedulerProvider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	arn := tagARN(req.URL.Path)
	tags, _ := p.store.tags.ListTags(arn)
	return jsonResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Rate limiting ---

func (p *SchedulerProvider) putRateLimit(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := rateLimitARN(req.URL.Path)
	if arn == "" {
		arn, _ = params["ResourceArn"].(string)
	}
	if arn == "" {
		return jsonError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	maxCalls := 100
	if v, ok := params["MaxCalls"].(float64); ok {
		maxCalls = int(v)
	}
	tw := "1h"
	if v, ok := params["TimeWindow"].(string); ok && v != "" {
		tw = v
	}
	if err := p.store.PutRateLimit(&RateLimit{ResourceARN: arn, MaxCalls: maxCalls, TimeWindow: tw}); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ResourceArn": arn, "MaxCalls": maxCalls, "TimeWindow": tw,
	})
}

func (p *SchedulerProvider) getRateLimit(req *http.Request) (*plugin.Response, error) {
	arn := rateLimitARN(req.URL.Path)
	rl, err := p.store.GetRateLimit(arn)
	if err != nil {
		return jsonError("ResourceNotFoundException", "rate limit not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ResourceArn": rl.ResourceARN,
		"MaxCalls":    rl.MaxCalls,
		"TimeWindow":  rl.TimeWindow,
	})
}

func (p *SchedulerProvider) deleteRateLimit(req *http.Request) (*plugin.Response, error) {
	arn := rateLimitARN(req.URL.Path)
	if err := p.store.DeleteRateLimit(arn); err != nil {
		return jsonError("ResourceNotFoundException", "rate limit not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *SchedulerProvider) listRateLimits() (*plugin.Response, error) {
	list, err := p.store.ListRateLimits()
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(list))
	for _, rl := range list {
		out = append(out, map[string]any{
			"ResourceArn": rl.ResourceARN,
			"MaxCalls":    rl.MaxCalls,
			"TimeWindow":  rl.TimeWindow,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"RateLimits": out})
}

func (p *SchedulerProvider) batchCheckMaxRateLimits(params map[string]any) (*plugin.Response, error) {
	arns := []string{}
	if raw, ok := params["ResourceArns"].([]any); ok {
		for _, a := range raw {
			if s, ok := a.(string); ok {
				arns = append(arns, s)
			}
		}
	}
	checks := make([]map[string]any, 0, len(arns))
	for _, arn := range arns {
		rl, err := p.store.GetRateLimit(arn)
		if err != nil {
			checks = append(checks, map[string]any{
				"ResourceArn": arn,
				"Exceeded":    false,
			})
			continue
		}
		checks = append(checks, map[string]any{
			"ResourceArn": arn,
			"MaxCalls":    rl.MaxCalls,
			"TimeWindow":  rl.TimeWindow,
			"Exceeded":    false,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"Checks": checks})
}

// --- Batch / cross-cutting ---

func (p *SchedulerProvider) listFleetSchedules(req *http.Request) (*plugin.Response, error) {
	schedules, err := p.store.ListSchedules("", "", "")
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(schedules))
	for _, sc := range schedules {
		list = append(list, map[string]any{
			"Arn":       sc.ARN,
			"Name":      sc.Name,
			"GroupName": sc.GroupName,
			"State":     sc.State,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"Schedules": list})
}

func (p *SchedulerProvider) batchCreateSchedules(params map[string]any) (*plugin.Response, error) {
	created := []map[string]any{}
	failed := []map[string]any{}
	if raw, ok := params["Schedules"].([]any); ok {
		for _, s := range raw {
			sm, ok := s.(map[string]any)
			if !ok {
				continue
			}
			name, _ := sm["Name"].(string)
			group, _ := sm["GroupName"].(string)
			expr, _ := sm["ScheduleExpression"].(string)
			targetJSON := "{}"
			if t, ok := sm["Target"]; ok {
				b, _ := json.Marshal(t)
				targetJSON = string(b)
			}
			state := "ENABLED"
			if st, ok := sm["State"].(string); ok && st != "" {
				state = st
			}
			flexJSON := `{"Mode":"OFF"}`
			if fw, ok := sm["FlexibleTimeWindow"]; ok {
				b, _ := json.Marshal(fw)
				flexJSON = string(b)
			}
			desc, _ := sm["Description"].(string)
			sc, err := p.store.CreateSchedule(name, group, expr, targetJSON, state, flexJSON, desc)
			if err != nil {
				failed = append(failed, map[string]any{"Name": name, "Error": err.Error()})
				continue
			}
			created = append(created, map[string]any{"Name": sc.Name, "Arn": sc.ARN})
		}
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"Created": created,
		"Failed":  failed,
	})
}

func (p *SchedulerProvider) batchDeleteSchedules(params map[string]any) (*plugin.Response, error) {
	deleted := []string{}
	failed := []map[string]any{}
	if raw, ok := params["Schedules"].([]any); ok {
		for _, s := range raw {
			sm, ok := s.(map[string]any)
			if !ok {
				continue
			}
			name, _ := sm["Name"].(string)
			group, _ := sm["GroupName"].(string)
			if err := p.store.DeleteSchedule(name, group); err != nil {
				failed = append(failed, map[string]any{"Name": name, "Error": err.Error()})
				continue
			}
			deleted = append(deleted, name)
		}
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"Deleted": deleted,
		"Failed":  failed,
	})
}

func (p *SchedulerProvider) batchUpdateSchedules(params map[string]any) (*plugin.Response, error) {
	updated := []string{}
	failed := []map[string]any{}
	if raw, ok := params["Schedules"].([]any); ok {
		for _, s := range raw {
			sm, ok := s.(map[string]any)
			if !ok {
				continue
			}
			name, _ := sm["Name"].(string)
			group, _ := sm["GroupName"].(string)
			existing, err := p.store.GetSchedule(name, group)
			if err != nil {
				failed = append(failed, map[string]any{"Name": name, "Error": err.Error()})
				continue
			}
			expr := existing.ScheduleExpression
			if e, ok := sm["ScheduleExpression"].(string); ok && e != "" {
				expr = e
			}
			targetJSON := existing.Target
			if t, ok := sm["Target"]; ok {
				b, _ := json.Marshal(t)
				targetJSON = string(b)
			}
			state := existing.State
			if st, ok := sm["State"].(string); ok && st != "" {
				state = st
			}
			flexJSON := existing.FlexibleTimeWindow
			if fw, ok := sm["FlexibleTimeWindow"]; ok {
				b, _ := json.Marshal(fw)
				flexJSON = string(b)
			}
			desc := existing.Description
			if d, ok := sm["Description"].(string); ok {
				desc = d
			}
			if err := p.store.UpdateSchedule(name, group, expr, targetJSON, state, flexJSON, desc); err != nil {
				failed = append(failed, map[string]any{"Name": name, "Error": err.Error()})
				continue
			}
			updated = append(updated, name)
		}
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"Updated": updated,
		"Failed":  failed,
	})
}

func (p *SchedulerProvider) previewSchedule(params map[string]any) (*plugin.Response, error) {
	expr, _ := params["ScheduleExpression"].(string)
	return jsonResponse(http.StatusOK, map[string]any{
		"ScheduleExpression": expr,
		"NextInvocations":    []any{},
	})
}

func (p *SchedulerProvider) validateScheduleExpression(params map[string]any) (*plugin.Response, error) {
	expr, _ := params["ScheduleExpression"].(string)
	valid := expr != "" && (strings.HasPrefix(expr, "rate(") || strings.HasPrefix(expr, "cron(") || strings.HasPrefix(expr, "at("))
	return jsonResponse(http.StatusOK, map[string]any{
		"Valid":              valid,
		"ScheduleExpression": expr,
	})
}

// --- Metrics / Lock / Execution helpers ---

func (p *SchedulerProvider) getScheduleMetrics(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	groupName := scheduleGroup(req.URL.Path)
	if _, err := p.store.GetSchedule(name, groupName); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ScheduleName": name,
		"GroupName":    groupName,
		"Invocations":  0,
		"Failures":     0,
		"Throttled":    0,
	})
}

func (p *SchedulerProvider) listScheduleMetrics(req *http.Request) (*plugin.Response, error) {
	group := req.URL.Query().Get("ScheduleGroup")
	schedules, err := p.store.ListSchedules(group, "", "")
	if err != nil {
		return nil, err
	}
	metrics := make([]map[string]any, 0, len(schedules))
	for _, sc := range schedules {
		metrics = append(metrics, map[string]any{
			"ScheduleName": sc.Name,
			"GroupName":    sc.GroupName,
			"Invocations":  0,
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"Metrics": metrics})
}

func (p *SchedulerProvider) getScheduleGroupMetrics(req *http.Request) (*plugin.Response, error) {
	name := groupName(req.URL.Path)
	if _, err := p.store.GetGroup(name); err != nil {
		return jsonError("ResourceNotFoundException", "schedule group not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"GroupName":     name,
		"ScheduleCount": 0,
		"Invocations":   0,
	})
}

func (p *SchedulerProvider) lockSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	group := scheduleGroup(req.URL.Path)
	if _, err := p.store.GetSchedule(name, group); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"Locked": true, "ScheduleName": name})
}

func (p *SchedulerProvider) unlockSchedule(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	group := scheduleGroup(req.URL.Path)
	if _, err := p.store.GetSchedule(name, group); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"Locked": false, "ScheduleName": name})
}

func (p *SchedulerProvider) getScheduleExecutionStatus(req *http.Request) (*plugin.Response, error) {
	name := scheduleName(req.URL.Path)
	group := scheduleGroup(req.URL.Path)
	if _, err := p.store.GetSchedule(name, group); err != nil {
		return jsonError("ResourceNotFoundException", "schedule not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"ScheduleName": name,
		"Status":       "SUCCESS",
	})
}

// --- helpers ---

func resolveOp(method, path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	n := len(parts)
	switch {
	case n >= 1 && parts[0] == "schedules":
		switch n {
		case 1:
			if method == http.MethodGet {
				return "ListSchedules"
			}
		case 2:
			switch method {
			case http.MethodPost, http.MethodPut:
				return "CreateSchedule"
			case http.MethodGet:
				return "GetSchedule"
			case http.MethodPatch:
				return "UpdateSchedule"
			case http.MethodDelete:
				return "DeleteSchedule"
			}
		case 3:
			// /schedules/{groupName}/{scheduleName}
			switch method {
			case http.MethodPost, http.MethodPut:
				return "CreateSchedule"
			case http.MethodGet:
				return "GetSchedule"
			case http.MethodPatch:
				return "UpdateSchedule"
			case http.MethodDelete:
				return "DeleteSchedule"
			}
		case 4:
			// /schedules/{groupName}/{scheduleName}/enable|disable
			if parts[3] == "enable" && method == http.MethodPost {
				return "EnableSchedule"
			}
			if parts[3] == "disable" && method == http.MethodPost {
				return "DisableSchedule"
			}
		}
	case n >= 1 && parts[0] == "schedule-groups":
		switch n {
		case 1:
			if method == http.MethodGet {
				return "ListScheduleGroups"
			}
		case 2:
			switch method {
			case http.MethodPost, http.MethodPut:
				return "CreateScheduleGroup"
			case http.MethodGet:
				return "GetScheduleGroup"
			case http.MethodPatch:
				return "UpdateScheduleGroup"
			case http.MethodDelete:
				return "DeleteScheduleGroup"
			}
		}
	case n >= 1 && parts[0] == "tags":
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		}
	case n >= 1 && parts[0] == "rate-limits":
		switch n {
		case 1:
			switch method {
			case http.MethodGet:
				return "ListRateLimits"
			case http.MethodPost:
				return "BatchCheckMaxRateLimits"
			}
		default:
			switch method {
			case http.MethodPut, http.MethodPost:
				return "PutRateLimit"
			case http.MethodGet:
				return "GetRateLimit"
			case http.MethodDelete:
				return "DeleteRateLimit"
			}
		}
	case n >= 1 && parts[0] == "fleet-schedules":
		return "ListFleetSchedules"
	case n >= 1 && parts[0] == "batch":
		if n == 2 {
			switch parts[1] {
			case "create-schedules":
				return "BatchCreateSchedules"
			case "delete-schedules":
				return "BatchDeleteSchedules"
			case "update-schedules":
				return "BatchUpdateSchedules"
			case "check-rate-limits":
				return "BatchCheckMaxRateLimits"
			}
		}
	case n >= 1 && parts[0] == "preview":
		return "PreviewSchedule"
	case n >= 1 && parts[0] == "validate":
		return "ValidateScheduleExpression"
	case n >= 1 && parts[0] == "connections":
		return "ListConnections"
	}
	return ""
}

func scheduleName(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// /schedules/{name} or /schedules/{group}/{name}
	if len(parts) >= 3 && parts[0] == "schedules" {
		return parts[2]
	}
	if len(parts) >= 2 && parts[0] == "schedules" {
		return parts[1]
	}
	return ""
}

func scheduleGroup(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "schedules" {
		return parts[1]
	}
	return "default"
}

func groupName(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "schedule-groups" {
		return parts[1]
	}
	return ""
}

func tagARN(path string) string {
	// /tags/{arn}
	idx := strings.Index(path, "/tags/")
	if idx >= 0 {
		return path[idx+6:]
	}
	return ""
}

func rateLimitARN(path string) string {
	idx := strings.Index(path, "/rate-limits/")
	if idx >= 0 {
		return path[idx+13:]
	}
	return ""
}

func scheduleToMap(sc *Schedule) map[string]any {
	var target map[string]any
	json.Unmarshal([]byte(sc.Target), &target)
	var flexWindow map[string]any
	json.Unmarshal([]byte(sc.FlexibleTimeWindow), &flexWindow)
	return map[string]any{
		"Arn":                  sc.ARN,
		"Name":                 sc.Name,
		"GroupName":            sc.GroupName,
		"ScheduleExpression":   sc.ScheduleExpression,
		"State":                sc.State,
		"Target":               target,
		"FlexibleTimeWindow":   flexWindow,
		"Description":          sc.Description,
		"CreationDate":         sc.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		"LastModificationDate": sc.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func jsonError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}
}

func jsonResponse(status int, v any) (*plugin.Response, error) {
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

func init() {
	plugin.DefaultRegistry.Register("scheduler", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &SchedulerProvider{}
	})
}

// Ensure shared is used
var _ = shared.DefaultAccountID
