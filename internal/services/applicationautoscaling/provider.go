// SPDX-License-Identifier: Apache-2.0

// internal/services/applicationautoscaling/provider.go
package applicationautoscaling

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

func (p *Provider) ServiceID() string             { return "applicationautoscaling" }
func (p *Provider) ServiceName() string           { return "AnyScaleFrontendService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "applicationautoscaling"))
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
	case "RegisterScalableTarget":
		return p.registerScalableTarget(params)
	case "DescribeScalableTargets":
		return p.describeScalableTargets(params)
	case "DeregisterScalableTarget":
		return p.deregisterScalableTarget(params)
	case "UpdateScalableTarget":
		return p.updateScalableTarget(params)
	case "PutScalingPolicy":
		return p.putScalingPolicy(params)
	case "DescribeScalingPolicies":
		return p.describeScalingPolicies(params)
	case "DeleteScalingPolicy":
		return p.deleteScalingPolicy(params)
	case "UpdateScalingPolicy":
		return p.putScalingPolicy(params)
	case "PutScheduledAction":
		return p.putScheduledAction(params)
	case "DescribeScheduledActions":
		return p.describeScheduledActions(params)
	case "DeleteScheduledAction":
		return p.deleteScheduledAction(params)
	case "UpdateScheduledAction":
		return p.putScheduledAction(params)
	case "DescribeScalingActivities":
		return p.describeScalingActivities(params)
	case "RecordScalingActivity":
		return p.recordScalingActivity(params)
	case "GetPredictiveScalingForecast":
		return p.getPredictiveScalingForecast(params)
	case "PutPredictiveScalingForecast":
		return p.putPredictiveScalingForecast(params)
	case "PutCustomizedMetricStatistics":
		return p.putCustomizedMetric(params)
	case "DeleteCustomizedMetricStatistics":
		return p.deleteCustomizedMetric(params)
	case "DescribeCustomizedMetricStatistics":
		return p.describeCustomizedMetrics(params)
	case "DescribeStatistics":
		return p.describeStatistics(params)
	case "GetMetricStatistics":
		return p.describeStatistics(params)
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)
	// Forecast and predictive scaling extras
	case "DescribePredictiveScalingForecast":
		return p.getPredictiveScalingForecast(params)
	case "DescribeAccountLimits":
		return p.describeAccountLimits(params)
	case "DescribeServiceQuotas":
		return p.describeServiceQuotas(params)
	// Bulk operations
	case "BatchDeleteScalingPolicy":
		return p.batchDeleteScalingPolicy(params)
	case "BatchDeleteScheduledAction":
		return p.batchDeleteScheduledAction(params)
	case "BatchDeregisterScalableTarget":
		return p.batchDeregisterScalableTarget(params)
	case "BatchPutScalingPolicy":
		return p.batchPutScalingPolicy(params)
	case "BatchPutScheduledAction":
		return p.batchPutScheduledAction(params)
	case "BatchRegisterScalableTarget":
		return p.batchRegisterScalableTarget(params)
	// Target tracking helpers
	case "GetTargetTrackingScalingPolicy":
		return p.getTargetTrackingScalingPolicy(params)
	case "PutTargetTrackingScalingPolicy":
		return p.putScalingPolicy(params)
	case "DeleteTargetTrackingScalingPolicy":
		return p.deleteScalingPolicy(params)
	// Resource discovery
	case "ListScalableTargetsByService":
		return p.listScalableTargetsByService(params)
	case "DescribeResourceActions":
		return p.describeResourceActions(params)
	// Health and status
	case "GetScalingHealth":
		return p.getScalingHealth(params)
	case "GetScalingStatus":
		return p.getScalingStatus(params)
	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	targets, err := p.store.ListTargets("", "", "")
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(targets))
	for _, t := range targets {
		res = append(res, plugin.Resource{Type: "scalable-target", ID: t.ID, Name: t.ResourceID})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) registerScalableTarget(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	minCap := 1
	maxCap := 10
	if v, ok := params["MinCapacity"].(float64); ok {
		minCap = int(v)
	}
	if v, ok := params["MaxCapacity"].(float64); ok {
		maxCap = int(v)
	}
	roleARN, _ := params["RoleARN"].(string)

	t, err := p.store.RegisterTarget(ns, resourceID, dimension, roleARN, minCap, maxCap)
	if err != nil {
		return nil, err
	}
	arn := shared.BuildARN("application-autoscaling", "scalableTarget", t.ID)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScalableTargetARN": arn,
	})
}

func (p *Provider) describeScalableTargets(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	var resourceIDs []string
	if raw, ok := params["ResourceIds"].([]any); ok {
		for _, r := range raw {
			if s, ok := r.(string); ok {
				resourceIDs = append(resourceIDs, s)
			}
		}
	}
	dimension, _ := params["ScalableDimension"].(string)

	var targets []ScalableTarget
	if len(resourceIDs) > 0 {
		for _, rid := range resourceIDs {
			list, err := p.store.ListTargets(ns, rid, dimension)
			if err != nil {
				return nil, err
			}
			targets = append(targets, list...)
		}
	} else {
		var err error
		targets, err = p.store.ListTargets(ns, "", dimension)
		if err != nil {
			return nil, err
		}
	}

	result := make([]map[string]any, 0, len(targets))
	for _, t := range targets {
		result = append(result, targetToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScalableTargets": result,
		"NextToken":       nil,
	})
}

func (p *Provider) deregisterScalableTarget(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterTarget(ns, resourceID, dimension); err != nil {
		return shared.JSONError("ObjectNotFoundException", "scalable target not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putScalingPolicy(params map[string]any) (*plugin.Response, error) {
	name, _ := params["PolicyName"].(string)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if name == "" || ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "PolicyName, ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	policyType, _ := params["PolicyType"].(string)
	if policyType == "" {
		policyType = "TargetTrackingScaling"
	}
	configJSON := "{}"
	if cfg, ok := params["TargetTrackingScalingPolicyConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	} else if cfg, ok := params["StepScalingPolicyConfiguration"]; ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	}

	policy, err := p.store.PutPolicy(name, ns, resourceID, dimension, policyType, configJSON)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"PolicyARN": policy.ARN,
		"Alarms":    []any{},
	})
}

func (p *Provider) describeScalingPolicies(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)

	policies, err := p.store.ListPolicies(ns, resourceID, dimension)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(policies))
	for _, pol := range policies {
		result = append(result, policyToMap(&pol))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScalingPolicies": result,
		"NextToken":       nil,
	})
}

func (p *Provider) deleteScalingPolicy(params map[string]any) (*plugin.Response, error) {
	name, _ := params["PolicyName"].(string)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if name == "" || ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "PolicyName, ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeletePolicy(name, ns, resourceID, dimension); err != nil {
		return shared.JSONError("ObjectNotFoundException", "scaling policy not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putScheduledAction(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ScheduledActionName"].(string)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if name == "" || ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "ScheduledActionName, ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	schedule, _ := params["Schedule"].(string)
	configJSON := "{}"
	if cfg, ok := params["ScalableTargetAction"]; ok {
		b, _ := json.Marshal(cfg)
		configJSON = string(b)
	}

	action, err := p.store.PutScheduledAction(name, ns, resourceID, dimension, schedule, configJSON)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScheduledActionARN": action.ARN,
	})
}

func (p *Provider) describeScheduledActions(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)

	actions, err := p.store.ListScheduledActions(ns, resourceID, dimension)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(actions))
	for _, a := range actions {
		result = append(result, scheduledActionToMap(&a))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScheduledActions": result,
		"NextToken":        nil,
	})
}

func (p *Provider) deleteScheduledAction(params map[string]any) (*plugin.Response, error) {
	name, _ := params["ScheduledActionName"].(string)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	if name == "" || ns == "" || resourceID == "" || dimension == "" {
		return shared.JSONError("ValidationException", "ScheduledActionName, ServiceNamespace, ResourceId, and ScalableDimension are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteScheduledAction(name, ns, resourceID, dimension); err != nil {
		return shared.JSONError("ObjectNotFoundException", "scheduled action not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeScalingActivities(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ScalingActivities": []any{},
		"NextToken":         nil,
	})
}

func (p *Provider) getPredictiveScalingForecast(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"LoadForecast":     []any{},
		"CapacityForecast": map[string]any{},
	})
}

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceARN"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceARN is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["Tags"].(map[string]any)
	tags := make(map[string]string, len(rawTags))
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
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags": tags,
	})
}

func targetToMap(t *ScalableTarget) map[string]any {
	arn := shared.BuildARN("application-autoscaling", "scalableTarget", t.ID)
	return map[string]any{
		"ServiceNamespace":  t.ServiceNamespace,
		"ResourceId":        t.ResourceID,
		"ScalableDimension": t.Dimension,
		"MinCapacity":       t.MinCapacity,
		"MaxCapacity":       t.MaxCapacity,
		"RoleARN":           t.RoleARN,
		"ScalableTargetARN": arn,
		"CreationTime":      t.CreatedAt.Unix(),
	}
}

func policyToMap(p *ScalingPolicy) map[string]any {
	return map[string]any{
		"PolicyARN":         p.ARN,
		"PolicyName":        p.Name,
		"ServiceNamespace":  p.ServiceNamespace,
		"ResourceId":        p.ResourceID,
		"ScalableDimension": p.Dimension,
		"PolicyType":        p.PolicyType,
		"CreationTime":      p.CreatedAt.Unix(),
	}
}

func scheduledActionToMap(a *ScheduledAction) map[string]any {
	return map[string]any{
		"ScheduledActionARN":  a.ARN,
		"ScheduledActionName": a.Name,
		"ServiceNamespace":    a.ServiceNamespace,
		"ResourceId":          a.ResourceID,
		"ScalableDimension":   a.Dimension,
		"Schedule":            a.Schedule,
		"CreationTime":        a.CreatedAt.Unix(),
	}
}

// --- Extra handlers ---

func (p *Provider) updateScalableTarget(params map[string]any) (*plugin.Response, error) {
	return p.registerScalableTarget(params)
}

func (p *Provider) recordScalingActivity(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	description, _ := params["Description"].(string)
	cause, _ := params["Cause"].(string)
	act, err := p.store.RecordActivity(ns, resourceID, dimension, description, cause)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ActivityId": act.ID,
	})
}

func (p *Provider) putPredictiveScalingForecast(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	policy, _ := params["PolicyName"].(string)
	data := "{}"
	if d, ok := params["ForecastData"]; ok {
		b, _ := json.Marshal(d)
		data = string(b)
	}
	if err := p.store.SaveForecast(ns, resourceID, dimension, policy, data); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) putCustomizedMetric(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	metricName, _ := params["MetricName"].(string)
	namespace, _ := params["Namespace"].(string)
	statistic, _ := params["Statistic"].(string)
	unit, _ := params["Unit"].(string)
	if statistic == "" {
		statistic = "Average"
	}
	config := "{}"
	if cfg, ok := params["MetricSpecification"]; ok {
		b, _ := json.Marshal(cfg)
		config = string(b)
	}
	metric, err := p.store.PutCustomizedMetric(ns, resourceID, dimension, metricName, namespace, statistic, unit, config)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MetricId": metric.ID,
	})
}

func (p *Provider) deleteCustomizedMetric(params map[string]any) (*plugin.Response, error) {
	id, _ := params["MetricId"].(string)
	if id == "" {
		return shared.JSONError("ValidationException", "MetricId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCustomizedMetric(id); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeCustomizedMetrics(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	metrics, err := p.store.ListCustomizedMetrics(ns, resourceID, dimension)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(metrics))
	for _, m := range metrics {
		items = append(items, map[string]any{
			"MetricId":         m.ID,
			"ServiceNamespace": m.ServiceNamespace,
			"ResourceId":       m.ResourceID,
			"MetricName":       m.MetricName,
			"Namespace":        m.Namespace,
			"Statistic":        m.Statistic,
			"Unit":             m.Unit,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Metrics": items,
	})
}

func (p *Provider) describeStatistics(params map[string]any) (*plugin.Response, error) {
	// Return synthetic statistics for the provided namespace/resource/dimension.
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Statistics": []map[string]any{
			{"StatisticType": "Average", "Value": 75.0, "Timestamp": time.Now().Unix()},
			{"StatisticType": "Maximum", "Value": 95.0, "Timestamp": time.Now().Unix()},
			{"StatisticType": "Minimum", "Value": 25.0, "Timestamp": time.Now().Unix()},
		},
		"Datapoints": []map[string]any{
			{"Timestamp": time.Now().Unix(), "Average": 75.0, "Unit": "Percent"},
		},
	})
}

func (p *Provider) describeAccountLimits(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MaxNumberOfScalableTargets":                  2000,
		"NumberOfScalableTargets":                     0,
		"MaxNumberOfScalingPoliciesPerScalableTarget": 50,
	})
}

func (p *Provider) describeServiceQuotas(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Quotas": []map[string]any{
			{"Name": "ScalableTargets", "Value": 2000},
			{"Name": "ScalingPoliciesPerTarget", "Value": 50},
		},
	})
}

// --- Batch operations ---

func (p *Provider) batchDeleteScalingPolicy(params map[string]any) (*plugin.Response, error) {
	policies, _ := params["PolicyNames"].([]any)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	for _, n := range policies {
		if name, ok := n.(string); ok {
			p.store.DeletePolicy(name, ns, resourceID, dimension)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchDeleteScheduledAction(params map[string]any) (*plugin.Response, error) {
	actions, _ := params["ScheduledActionNames"].([]any)
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	for _, n := range actions {
		if name, ok := n.(string); ok {
			p.store.DeleteScheduledAction(name, ns, resourceID, dimension)
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchDeregisterScalableTarget(params map[string]any) (*plugin.Response, error) {
	targets, _ := params["ScalableTargets"].([]any)
	for _, t := range targets {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		ns, _ := m["ServiceNamespace"].(string)
		rid, _ := m["ResourceId"].(string)
		dim, _ := m["ScalableDimension"].(string)
		p.store.DeregisterTarget(ns, rid, dim)
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) batchPutScalingPolicy(params map[string]any) (*plugin.Response, error) {
	policies, _ := params["Policies"].([]any)
	created := make([]map[string]any, 0, len(policies))
	for _, item := range policies {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["PolicyName"].(string)
		ns, _ := m["ServiceNamespace"].(string)
		rid, _ := m["ResourceId"].(string)
		dim, _ := m["ScalableDimension"].(string)
		ptype, _ := m["PolicyType"].(string)
		if ptype == "" {
			ptype = "TargetTrackingScaling"
		}
		policy, err := p.store.PutPolicy(name, ns, rid, dim, ptype, "{}")
		if err == nil {
			created = append(created, map[string]any{"PolicyARN": policy.ARN, "PolicyName": name})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Policies": created})
}

func (p *Provider) batchPutScheduledAction(params map[string]any) (*plugin.Response, error) {
	actions, _ := params["Actions"].([]any)
	created := make([]map[string]any, 0, len(actions))
	for _, item := range actions {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["ScheduledActionName"].(string)
		ns, _ := m["ServiceNamespace"].(string)
		rid, _ := m["ResourceId"].(string)
		dim, _ := m["ScalableDimension"].(string)
		sched, _ := m["Schedule"].(string)
		act, err := p.store.PutScheduledAction(name, ns, rid, dim, sched, "{}")
		if err == nil {
			created = append(created, map[string]any{"ScheduledActionARN": act.ARN, "ScheduledActionName": name})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ScheduledActions": created})
}

func (p *Provider) batchRegisterScalableTarget(params map[string]any) (*plugin.Response, error) {
	targets, _ := params["ScalableTargets"].([]any)
	created := make([]map[string]any, 0, len(targets))
	for _, item := range targets {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ns, _ := m["ServiceNamespace"].(string)
		rid, _ := m["ResourceId"].(string)
		dim, _ := m["ScalableDimension"].(string)
		minCap := 1
		maxCap := 10
		if v, ok := m["MinCapacity"].(float64); ok {
			minCap = int(v)
		}
		if v, ok := m["MaxCapacity"].(float64); ok {
			maxCap = int(v)
		}
		roleARN, _ := m["RoleARN"].(string)
		target, err := p.store.RegisterTarget(ns, rid, dim, roleARN, minCap, maxCap)
		if err == nil {
			created = append(created, map[string]any{"ScalableTargetARN": shared.BuildARN("application-autoscaling", "scalableTarget", target.ID)})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ScalableTargets": created})
}

// --- Target tracking helpers ---

func (p *Provider) getTargetTrackingScalingPolicy(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	policies, err := p.store.ListPolicies(ns, resourceID, dimension)
	if err != nil {
		return nil, err
	}
	for _, pol := range policies {
		if pol.PolicyType == "TargetTrackingScaling" {
			return shared.JSONResponse(http.StatusOK, map[string]any{
				"Policy": policyToMap(&pol),
			})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Policy": nil})
}

func (p *Provider) listScalableTargetsByService(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	targets, err := p.store.ListTargets(ns, "", "")
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(targets))
	for _, t := range targets {
		items = append(items, targetToMap(&t))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ScalableTargets": items})
}

func (p *Provider) describeResourceActions(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	dimension, _ := params["ScalableDimension"].(string)
	activities, err := p.store.ListActivities(ns, resourceID, dimension)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(activities))
	for _, a := range activities {
		items = append(items, map[string]any{
			"ActivityId":    a.ID,
			"Description":   a.Description,
			"Cause":         a.Cause,
			"StatusCode":    a.StatusCode,
			"StatusMessage": a.StatusMessage,
			"StartTime":     a.StartTime.Unix(),
			"EndTime":       a.EndTime.Unix(),
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Actions": items})
}

func (p *Provider) getScalingHealth(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Status":  "Healthy",
		"Details": map[string]any{"Issues": []any{}},
	})
}

func (p *Provider) getScalingStatus(params map[string]any) (*plugin.Response, error) {
	ns, _ := params["ServiceNamespace"].(string)
	resourceID, _ := params["ResourceId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ServiceNamespace": ns,
		"ResourceId":       resourceID,
		"Status":           "Active",
	})
}
