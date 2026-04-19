// SPDX-License-Identifier: Apache-2.0

// internal/services/xray/provider.go
package xray

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

// Provider implements the XRay service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "xray" }
func (p *Provider) ServiceName() string           { return "XRay" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "xray"))
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
	// Group CRUD
	case "CreateGroup":
		return p.createGroup(params)
	case "GetGroup":
		name := req.URL.Query().Get("GroupName")
		if name == "" {
			name, _ = params["GroupName"].(string)
		}
		if name == "" {
			name = extractPathParam(req.URL.Path, "Groups")
		}
		return p.getGroup(name)
	case "GetGroups":
		return p.getGroups()
	case "UpdateGroup":
		return p.updateGroup(params)
	case "DeleteGroup":
		name := req.URL.Query().Get("GroupName")
		if name == "" {
			name, _ = params["GroupName"].(string)
		}
		return p.deleteGroup(name)

	// SamplingRule CRUD
	case "CreateSamplingRule":
		return p.createSamplingRule(params)
	case "GetSamplingRules":
		return p.getSamplingRules()
	case "UpdateSamplingRule":
		return p.updateSamplingRule(params)
	case "DeleteSamplingRule":
		return p.deleteSamplingRule(params)

	// Traces
	case "PutTraceSegments":
		return p.putTraceSegments(params)
	case "BatchGetTraces":
		return p.batchGetTraces(params)
	case "GetTraceSummaries":
		return p.getTraceSummaries()

	// Tags
	case "TagResource":
		return p.tagResource(req, params)
	case "UntagResource":
		return p.untagResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)

	// Encryption config
	case "GetEncryptionConfig":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EncryptionConfig": map[string]any{
				"Type":   "NONE",
				"Status": "ACTIVE",
			},
		})
	case "PutEncryptionConfig":
		keyID, _ := params["KeyId"].(string)
		cfgType, _ := params["Type"].(string)
		if cfgType == "" {
			cfgType = "NONE"
		}
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"EncryptionConfig": map[string]any{
				"KeyId":  keyID,
				"Type":   cfgType,
				"Status": "ACTIVE",
			},
		})

	// No-op / empty stubs
	case "PutTelemetryRecords":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "GetServiceGraph":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Services":  []any{},
			"StartTime": time.Now().Unix(),
			"EndTime":   time.Now().Unix(),
		})
	case "GetTraceGraph":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Services": []any{},
		})
	case "GetTimeSeriesServiceStatistics":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"TimeSeriesServiceStatistics": []any{},
		})
	case "GetSamplingTargets":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SamplingTargetDocuments": []any{},
			"LastRuleModification":    time.Now().Unix(),
			"UnprocessedStatistics":   []any{},
		})
	case "GetSamplingStatisticSummaries":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"SamplingStatisticSummaries": []any{},
		})
	case "GetInsight":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"Insight": map[string]any{},
		})
	case "GetInsightEvents":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"InsightEvents": []any{},
		})
	case "GetInsightImpactGraph":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"InsightId": "",
			"Services":  []any{},
		})
	case "GetInsightSummaries":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"InsightSummaries": []any{},
		})
	case "GetResourcePolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourcePolicies": []any{},
		})
	case "PutResourcePolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourcePolicy": map[string]any{},
		})
	case "DeleteResourcePolicy":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "ListResourcePolicies":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"ResourcePolicies": []any{},
		})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", op), http.StatusBadRequest), nil
	}
}

func resolveOp(method, path string) string {
	p := strings.Trim(path, "/")

	// Tags: /tags/{arn}
	if strings.HasPrefix(p, "tags/") || p == "tags" {
		switch method {
		case http.MethodPost:
			return "TagResource"
		case http.MethodGet:
			return "ListTagsForResource"
		case http.MethodDelete:
			return "UntagResource"
		}
	}

	// X-Ray uses PascalCase path names that map directly to operation names
	ops := map[string]string{
		"CreateGroup":                    "CreateGroup",
		"GetGroup":                       "GetGroup",
		"Groups":                         "GetGroups",
		"UpdateGroup":                    "UpdateGroup",
		"DeleteGroup":                    "DeleteGroup",
		"CreateSamplingRule":             "CreateSamplingRule",
		"GetSamplingRules":               "GetSamplingRules",
		"UpdateSamplingRule":             "UpdateSamplingRule",
		"DeleteSamplingRule":             "DeleteSamplingRule",
		"PutTraceSegments":               "PutTraceSegments",
		"BatchGetTraces":                 "BatchGetTraces",
		"GetTraceSummaries":              "GetTraceSummaries",
		"GetEncryptionConfig":            "GetEncryptionConfig",
		"PutEncryptionConfig":            "PutEncryptionConfig",
		"PutTelemetryRecords":            "PutTelemetryRecords",
		"GetServiceGraph":                "GetServiceGraph",
		"GetTraceGraph":                  "GetTraceGraph",
		"GetTimeSeriesServiceStatistics": "GetTimeSeriesServiceStatistics",
		"GetSamplingTargets":             "GetSamplingTargets",
		"GetSamplingStatisticSummaries":  "GetSamplingStatisticSummaries",
		"GetInsight":                     "GetInsight",
		"GetInsightEvents":               "GetInsightEvents",
		"GetInsightImpactGraph":          "GetInsightImpactGraph",
		"InsightSummaries":               "GetInsightSummaries",
		"GetResourcePolicy":              "GetResourcePolicy",
		"PutResourcePolicy":              "PutResourcePolicy",
		"DeleteResourcePolicy":           "DeleteResourcePolicy",
		"ListResourcePolicies":           "ListResourcePolicies",
		"TagResource":                    "TagResource",
		"UntagResource":                  "UntagResource",
		"ListTagsForResource":            "ListTagsForResource",
	}
	if op, ok := ops[p]; ok {
		return op
	}
	return ""
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	groups, err := p.store.ListGroups()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(groups))
	for _, g := range groups {
		res = append(res, plugin.Resource{Type: "xray-group", ID: g.Name, Name: g.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Group ---

func (p *Provider) createGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["GroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "GroupName is required", http.StatusBadRequest), nil
	}
	filterExpr, _ := params["FilterExpression"].(string)
	insightsConfig := "{}"
	if v, ok := params["InsightsConfiguration"]; ok {
		if b, err := json.Marshal(v); err == nil {
			insightsConfig = string(b)
		}
	}

	arn := shared.BuildARN("xray", "group", name)
	g := &Group{
		Name:           name,
		ARN:            arn,
		FilterExpr:     filterExpr,
		InsightsConfig: insightsConfig,
	}
	if err := p.store.CreateGroup(g); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "group already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags)) //nolint:errcheck
	}
	stored, err := p.store.GetGroup(name)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(stored, tags),
	})
}

func (p *Provider) getGroup(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "GroupName is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "group not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(g.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(g, tags),
	})
}

func (p *Provider) getGroups() (*plugin.Response, error) {
	groups, err := p.store.ListGroups()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		tags, _ := p.store.tags.ListTags(g.ARN)
		summaries = append(summaries, groupToMap(&g, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Groups": summaries,
	})
}

func (p *Provider) updateGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["GroupName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "GroupName is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "group not found", http.StatusBadRequest), nil
	}

	filterExpr := existing.FilterExpr
	if v, ok := params["FilterExpression"].(string); ok {
		filterExpr = v
	}
	insightsConfig := existing.InsightsConfig
	if v, ok := params["InsightsConfiguration"]; ok {
		if b, err := json.Marshal(v); err == nil {
			insightsConfig = string(b)
		}
	}
	if err := p.store.UpdateGroup(name, filterExpr, insightsConfig); err != nil {
		return shared.JSONError("InvalidRequestException", "group not found", http.StatusBadRequest), nil
	}
	updated, err := p.store.GetGroup(name)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(updated.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Group": groupToMap(updated, tags),
	})
}

func (p *Provider) deleteGroup(name string) (*plugin.Response, error) {
	if name == "" {
		return shared.JSONError("ValidationException", "GroupName is required", http.StatusBadRequest), nil
	}
	g, err := p.store.GetGroup(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "group not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(g.ARN) //nolint:errcheck
	if err := p.store.DeleteGroup(name); err != nil {
		return shared.JSONError("InvalidRequestException", "group not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// --- SamplingRule ---

func (p *Provider) createSamplingRule(params map[string]any) (*plugin.Response, error) {
	ruleRaw, _ := params["SamplingRule"].(map[string]any)
	if ruleRaw == nil {
		return shared.JSONError("ValidationException", "SamplingRule is required", http.StatusBadRequest), nil
	}
	name, _ := ruleRaw["RuleName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "RuleName is required", http.StatusBadRequest), nil
	}

	arn := shared.BuildARN("xray", "sampling-rule", name)
	r := &SamplingRule{
		Name:          name,
		ARN:           arn,
		Priority:      1000,
		FixedRate:     0.05,
		ReservoirSize: 1,
		ServiceName:   "*",
		ServiceType:   "*",
		Host:          "*",
		HTTPMethod:    "*",
		URLPath:       "*",
		ResourceARN:   "*",
		Version:       1,
	}
	if v, ok := toFloat(ruleRaw["FixedRate"]); ok {
		r.FixedRate = v
	}
	if v, ok := toInt64(ruleRaw["ReservoirSize"]); ok {
		r.ReservoirSize = v
	}
	if v, ok := toInt64(ruleRaw["Priority"]); ok {
		r.Priority = v
	}
	if v, ok := ruleRaw["ServiceName"].(string); ok {
		r.ServiceName = v
	}
	if v, ok := ruleRaw["ServiceType"].(string); ok {
		r.ServiceType = v
	}
	if v, ok := ruleRaw["Host"].(string); ok {
		r.Host = v
	}
	if v, ok := ruleRaw["HTTPMethod"].(string); ok {
		r.HTTPMethod = v
	}
	if v, ok := ruleRaw["URLPath"].(string); ok {
		r.URLPath = v
	}
	if v, ok := ruleRaw["ResourceARN"].(string); ok {
		r.ResourceARN = v
	}
	if v, ok := toInt64(ruleRaw["Version"]); ok {
		r.Version = v
	}

	if err := p.store.CreateSamplingRule(r); err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("AlreadyExistsException", "sampling rule already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	if rawTags, ok := params["Tags"].(map[string]any); ok {
		_ = p.store.tags.AddTags(arn, toStringMap(rawTags)) //nolint:errcheck
	}
	stored, err := p.store.GetSamplingRule(name)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(arn)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SamplingRuleRecord": samplingRuleToMap(stored, tags),
	})
}

func (p *Provider) getSamplingRules() (*plugin.Response, error) {
	rules, err := p.store.ListSamplingRules()
	if err != nil {
		return nil, err
	}
	records := make([]map[string]any, 0, len(rules))
	for _, r := range rules {
		tags, _ := p.store.tags.ListTags(r.ARN)
		records = append(records, samplingRuleToMap(&r, tags))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SamplingRuleRecords": records,
	})
}

func (p *Provider) updateSamplingRule(params map[string]any) (*plugin.Response, error) {
	updateRaw, _ := params["SamplingRuleUpdate"].(map[string]any)
	if updateRaw == nil {
		return shared.JSONError("ValidationException", "SamplingRuleUpdate is required", http.StatusBadRequest), nil
	}
	name, _ := updateRaw["RuleName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "RuleName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateSamplingRule(name, updateRaw); err != nil {
		return shared.JSONError("InvalidRequestException", "sampling rule not found", http.StatusBadRequest), nil
	}
	updated, err := p.store.GetSamplingRule(name)
	if err != nil {
		return nil, err
	}
	tags, _ := p.store.tags.ListTags(updated.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SamplingRuleRecord": samplingRuleToMap(updated, tags),
	})
}

func (p *Provider) deleteSamplingRule(params map[string]any) (*plugin.Response, error) {
	name, _ := params["RuleName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "RuleName is required", http.StatusBadRequest), nil
	}
	r, err := p.store.GetSamplingRule(name)
	if err != nil {
		return shared.JSONError("InvalidRequestException", "sampling rule not found", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(r.ARN) //nolint:errcheck
	if err := p.store.DeleteSamplingRule(name); err != nil {
		return shared.JSONError("InvalidRequestException", "sampling rule not found", http.StatusBadRequest), nil
	}
	tags, _ := p.store.tags.ListTags(r.ARN)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SamplingRuleRecord": samplingRuleToMap(r, tags),
	})
}

// --- Traces ---

func (p *Provider) putTraceSegments(params map[string]any) (*plugin.Response, error) {
	docs, _ := params["TraceSegmentDocuments"].([]any)
	unprocessed := []any{}
	now := time.Now().Unix()
	for _, d := range docs {
		docStr, ok := d.(string)
		if !ok {
			continue
		}
		// Parse the segment document to extract trace_id and id
		var doc map[string]any
		if err := json.Unmarshal([]byte(docStr), &doc); err != nil {
			continue
		}
		traceID, _ := doc["trace_id"].(string)
		segmentID, _ := doc["id"].(string)
		if traceID == "" || segmentID == "" {
			continue
		}
		seg := &TraceSegment{
			TraceID:   traceID,
			SegmentID: segmentID,
			Document:  docStr,
			CreatedAt: now,
		}
		p.store.PutSegment(seg) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"UnprocessedTraceSegments": unprocessed,
	})
}

func (p *Provider) batchGetTraces(params map[string]any) (*plugin.Response, error) {
	idsRaw, _ := params["TraceIds"].([]any)
	traces := []map[string]any{}
	unprocessed := []any{}
	for _, idRaw := range idsRaw {
		traceID, ok := idRaw.(string)
		if !ok {
			continue
		}
		segs, err := p.store.GetSegmentsByTraceID(traceID)
		if err != nil || len(segs) == 0 {
			unprocessed = append(unprocessed, traceID)
			continue
		}
		segments := make([]map[string]any, 0, len(segs))
		for _, s := range segs {
			segments = append(segments, map[string]any{
				"Id":       s.SegmentID,
				"Document": s.Document,
			})
		}
		traces = append(traces, map[string]any{
			"Id":       traceID,
			"Segments": segments,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Traces":              traces,
		"UnprocessedTraceIds": unprocessed,
	})
}

func (p *Provider) getTraceSummaries() (*plugin.Response, error) {
	ids, err := p.store.ListDistinctTraceIDs()
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		summaries = append(summaries, map[string]any{
			"Id":         id,
			"Duration":   0,
			"IsThrottle": false,
			"IsFault":    false,
			"IsError":    false,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"TraceSummaries":       summaries,
		"ApproximateTime":      time.Now().Unix(),
		"TracesProcessedCount": int64(len(summaries)),
	})
}

// --- Tags ---

func (p *Provider) tagResource(req *http.Request, params map[string]any) (*plugin.Response, error) {
	arn := extractPathRemainder(req.URL.Path, "tags")
	if arn == "" {
		return shared.JSONError("ValidationException", "resource ARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
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
	keys := req.URL.Query()["TagKeys"]
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
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tags})
}

// --- Helpers ---

func groupToMap(g *Group, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	var insightsConfig any
	_ = json.Unmarshal([]byte(g.InsightsConfig), &insightsConfig) //nolint:errcheck
	return map[string]any{
		"GroupName":             g.Name,
		"GroupARN":              g.ARN,
		"FilterExpression":      g.FilterExpr,
		"InsightsConfiguration": insightsConfig,
		"Tags":                  tags,
	}
}

func samplingRuleToMap(r *SamplingRule, tags map[string]string) map[string]any {
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{
		"SamplingRule": map[string]any{
			"RuleName":      r.Name,
			"RuleARN":       r.ARN,
			"Priority":      r.Priority,
			"FixedRate":     r.FixedRate,
			"ReservoirSize": r.ReservoirSize,
			"ServiceName":   r.ServiceName,
			"ServiceType":   r.ServiceType,
			"Host":          r.Host,
			"HTTPMethod":    r.HTTPMethod,
			"URLPath":       r.URLPath,
			"ResourceARN":   r.ResourceARN,
			"Version":       r.Version,
		},
		"CreatedAt":  time.Now().Unix(),
		"ModifiedAt": time.Now().Unix(),
		"Tags":       tags,
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
