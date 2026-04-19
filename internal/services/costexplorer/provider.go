// SPDX-License-Identifier: Apache-2.0

// internal/services/costexplorer/provider.go
package costexplorer

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

func (p *Provider) ServiceID() string             { return "costexplorer" }
func (p *Provider) ServiceName() string           { return "InsightsIndexService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "costexplorer"))
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
	// CostCategoryDefinition CRUD
	case "CreateCostCategoryDefinition":
		return p.createCostCategoryDefinition(params)
	case "DescribeCostCategoryDefinition":
		return p.describeCostCategoryDefinition(params)
	case "ListCostCategoryDefinitions":
		return p.listCostCategoryDefinitions(params)
	case "UpdateCostCategoryDefinition":
		return p.updateCostCategoryDefinition(params)
	case "DeleteCostCategoryDefinition":
		return p.deleteCostCategoryDefinition(params)

	// AnomalyMonitor CRUD
	case "CreateAnomalyMonitor":
		return p.createAnomalyMonitor(params)
	case "GetAnomalyMonitors":
		return p.getAnomalyMonitors(params)
	case "UpdateAnomalyMonitor":
		return p.updateAnomalyMonitor(params)
	case "DeleteAnomalyMonitor":
		return p.deleteAnomalyMonitor(params)

	// AnomalySubscription CRUD
	case "CreateAnomalySubscription":
		return p.createAnomalySubscription(params)
	case "GetAnomalySubscriptions":
		return p.getAnomalySubscriptions(params)
	case "UpdateAnomalySubscription":
		return p.updateAnomalySubscription(params)
	case "DeleteAnomalySubscription":
		return p.deleteAnomalySubscription(params)

	// Tags
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// Cost/Usage — dummy data
	case "GetCostAndUsage":
		return p.getCostAndUsage(params)
	case "GetCostAndUsageWithResources":
		return p.getCostAndUsage(params)
	case "GetCostForecast":
		return p.getCostForecast(params)
	case "GetUsageForecast":
		return p.getUsageForecast(params)
	case "GetDimensionValues":
		return p.getDimensionValues(params)
	case "GetTags":
		return p.getTagsQuery(params)
	case "GetReservationCoverage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CoveragesByTime": []any{}, "Total": map[string]any{}})
	case "GetReservationUtilization":
		return shared.JSONResponse(http.StatusOK, map[string]any{"UtilizationsByTime": []any{}, "Total": map[string]any{}})
	case "GetReservationPurchaseRecommendation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Recommendations": []any{}, "Metadata": map[string]any{}})
	case "GetSavingsPlansCoverage":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SavingsPlansCoverages": []any{}})
	case "GetSavingsPlansUtilization":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SavingsPlansUtilizationsByTime": []any{}, "Total": map[string]any{}})
	case "GetSavingsPlansUtilizationDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SavingsPlansUtilizationDetails": []any{}, "Total": map[string]any{}})
	case "GetSavingsPlansPurchaseRecommendation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"SavingsPlansPurchaseRecommendation": map[string]any{}})
	case "GetSavingsPlanPurchaseRecommendationDetails":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RecommendationDetailData": map[string]any{}})
	case "GetRightsizingRecommendation":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RightsizingRecommendations": []any{}, "Summary": map[string]any{}})
	case "GetAnomalies":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Anomalies": []any{}})
	case "GetApproximateUsageRecords":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Services": map[string]any{}, "TotalRecords": 0, "LookbackPeriod": map[string]any{}})
	case "GetCostCategories":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CostCategoryNames": []any{}, "ReturnSize": 0, "TotalSize": 0})
	case "GetCostAndUsageComparisons":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CostAndUsageComparisons": []any{}})
	case "GetCostComparisonDrivers":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CostComparisonDrivers": []any{}})
	case "GetCommitmentPurchaseAnalysis":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AnalysisDetails": map[string]any{}, "AnalysisSummary": map[string]any{}})

	// List operations — return empty
	case "ListCommitmentPurchaseAnalyses":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AnalysisSummaryList": []any{}})
	case "ListCostAllocationTagBackfillHistory":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BackfillRequests": []any{}})
	case "ListCostAllocationTags":
		return shared.JSONResponse(http.StatusOK, map[string]any{"CostAllocationTags": []any{}})
	case "ListCostCategoryResourceAssociations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"ResourceTags": []any{}})
	case "ListSavingsPlansPurchaseRecommendationGeneration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"GenerationSummaryList": []any{}})

	// Misc operations
	case "ProvideAnomalyFeedback":
		return p.provideAnomalyFeedback(params)
	case "StartCommitmentPurchaseAnalysis":
		return shared.JSONResponse(http.StatusOK, map[string]any{"AnalysisId": shared.GenerateUUID(), "CommitmentPurchaseAnalysisStatus": "PROCESSING", "EstimatedCompletionTime": ""})
	case "StartCostAllocationTagBackfill":
		return shared.JSONResponse(http.StatusOK, map[string]any{"BackfillRequest": map[string]any{"BackfillStatus": "PROCESSING"}})
	case "StartSavingsPlansPurchaseRecommendationGeneration":
		return shared.JSONResponse(http.StatusOK, map[string]any{"GenerationSummary": map[string]any{"Status": "PROCESSING"}})
	case "UpdateCostAllocationTagsStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"Errors": []any{}})

	default:
		return shared.JSONError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	cats, err := p.store.ListCostCategories()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(cats))
	for _, c := range cats {
		res = append(res, plugin.Resource{Type: "cost-category", ID: c.ARN, Name: c.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ----- CostCategoryDefinition -----

func (p *Provider) createCostCategoryDefinition(params map[string]any) (*plugin.Response, error) {
	name, _ := params["Name"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "Name is required", http.StatusBadRequest), nil
	}
	ruleVersion, _ := params["RuleVersion"].(string)
	if ruleVersion == "" {
		ruleVersion = "CostCategoryExpression.v1"
	}
	rulesJSON := "[]"
	if rules, ok := params["Rules"]; ok {
		b, _ := json.Marshal(rules)
		rulesJSON = string(b)
	}
	effectiveStart, _ := params["EffectiveStart"].(string)
	arn := shared.BuildARN("ce", "costcategory", name)
	cat, err := p.store.CreateCostCategory(arn, name, ruleVersion, rulesJSON, effectiveStart)
	if err != nil {
		if isUniqueErr(err) {
			return shared.JSONError("ResourceInUseException", "cost category already exists", http.StatusBadRequest), nil
		}
		return nil, err
	}
	if rawTags, ok := params["ResourceTags"].([]any); ok {
		_ = p.store.tags.AddTags(cat.ARN, parseTags(rawTags)) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CostCategoryArn": cat.ARN,
		"EffectiveStart":  cat.EffectiveStart,
	})
}

func (p *Provider) describeCostCategoryDefinition(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["CostCategoryArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "CostCategoryArn is required", http.StatusBadRequest), nil
	}
	cat, err := p.store.GetCostCategoryByARN(arn)
	if err != nil {
		return shared.JSONError("ResourceNotFoundException", "cost category not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CostCategory": costCategoryToMap(cat),
	})
}

func (p *Provider) listCostCategoryDefinitions(_ map[string]any) (*plugin.Response, error) {
	cats, err := p.store.ListCostCategories()
	if err != nil {
		return nil, err
	}
	refs := make([]map[string]any, 0, len(cats))
	for _, c := range cats {
		refs = append(refs, map[string]any{
			"CostCategoryArn": c.ARN,
			"Name":            c.Name,
			"EffectiveStart":  c.EffectiveStart,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CostCategoryReferences": refs,
	})
}

func (p *Provider) updateCostCategoryDefinition(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["CostCategoryArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "CostCategoryArn is required", http.StatusBadRequest), nil
	}
	ruleVersion, _ := params["RuleVersion"].(string)
	if ruleVersion == "" {
		ruleVersion = "CostCategoryExpression.v1"
	}
	rulesJSON := "[]"
	if rules, ok := params["Rules"]; ok {
		b, _ := json.Marshal(rules)
		rulesJSON = string(b)
	}
	if err := p.store.UpdateCostCategory(arn, ruleVersion, rulesJSON); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cost category not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CostCategoryArn": arn,
		"EffectiveStart":  "",
	})
}

func (p *Provider) deleteCostCategoryDefinition(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["CostCategoryArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "CostCategoryArn is required", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn) //nolint:errcheck
	if err := p.store.DeleteCostCategory(arn); err != nil {
		return shared.JSONError("ResourceNotFoundException", "cost category not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"CostCategoryArn": arn,
		"EffectiveEnd":    "",
	})
}

// ----- AnomalyMonitor -----

func (p *Provider) createAnomalyMonitor(params map[string]any) (*plugin.Response, error) {
	anomMon, _ := params["AnomalyMonitor"].(map[string]any)
	if anomMon == nil {
		anomMon = params
	}
	name, _ := anomMon["MonitorName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "MonitorName is required", http.StatusBadRequest), nil
	}
	monType, _ := anomMon["MonitorType"].(string)
	if monType == "" {
		monType = "DIMENSIONAL"
	}
	dimension, _ := anomMon["MonitorDimension"].(string)
	if dimension == "" {
		dimension = "SERVICE"
	}
	configJSON := "{}"
	if spec, ok := anomMon["MonitorSpecification"]; ok {
		b, _ := json.Marshal(spec)
		configJSON = string(b)
	}
	arn := shared.BuildARN("ce", "anomalymonitor", shared.GenerateUUID())
	mon, err := p.store.CreateAnomalyMonitor(arn, name, monType, dimension, configJSON)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["ResourceTags"].([]any); ok {
		_ = p.store.tags.AddTags(mon.ARN, parseTags(rawTags)) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MonitorArn": mon.ARN,
	})
}

func (p *Provider) getAnomalyMonitors(params map[string]any) (*plugin.Response, error) {
	var filterARNs []string
	if rawARNs, ok := params["MonitorArnList"].([]any); ok {
		for _, a := range rawARNs {
			if s, ok := a.(string); ok {
				filterARNs = append(filterARNs, s)
			}
		}
	}
	monitors, err := p.store.ListAnomalyMonitors(filterARNs)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(monitors))
	for _, m := range monitors {
		list = append(list, anomalyMonitorToMap(&m))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AnomalyMonitors": list,
	})
}

func (p *Provider) updateAnomalyMonitor(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["MonitorArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "MonitorArn is required", http.StatusBadRequest), nil
	}
	name, _ := params["MonitorName"].(string)
	if err := p.store.UpdateAnomalyMonitor(arn, name); err != nil {
		return shared.JSONError("UnknownMonitorException", "monitor not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"MonitorArn": arn,
	})
}

func (p *Provider) deleteAnomalyMonitor(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["MonitorArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "MonitorArn is required", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn) //nolint:errcheck
	if err := p.store.DeleteAnomalyMonitor(arn); err != nil {
		return shared.JSONError("UnknownMonitorException", "monitor not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ----- AnomalySubscription -----

func (p *Provider) createAnomalySubscription(params map[string]any) (*plugin.Response, error) {
	sub, _ := params["AnomalySubscription"].(map[string]any)
	if sub == nil {
		sub = params
	}
	name, _ := sub["SubscriptionName"].(string)
	if name == "" {
		return shared.JSONError("ValidationException", "SubscriptionName is required", http.StatusBadRequest), nil
	}
	frequency, _ := sub["Frequency"].(string)
	if frequency == "" {
		frequency = "DAILY"
	}
	var threshold float64
	if t, ok := sub["Threshold"].(float64); ok {
		threshold = t
	}
	monitorARNsJSON := "[]"
	if ma, ok := sub["MonitorArnList"]; ok {
		b, _ := json.Marshal(ma)
		monitorARNsJSON = string(b)
	}
	subscribersJSON := "[]"
	if subs, ok := sub["Subscribers"]; ok {
		b, _ := json.Marshal(subs)
		subscribersJSON = string(b)
	}
	arn := shared.BuildARN("ce", "anomalysubscription", shared.GenerateUUID())
	s, err := p.store.CreateAnomalySubscription(arn, name, monitorARNsJSON, threshold, frequency, subscribersJSON)
	if err != nil {
		return nil, err
	}
	if rawTags, ok := params["ResourceTags"].([]any); ok {
		_ = p.store.tags.AddTags(s.ARN, parseTags(rawTags)) //nolint:errcheck
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SubscriptionArn": s.ARN,
	})
}

func (p *Provider) getAnomalySubscriptions(params map[string]any) (*plugin.Response, error) {
	monitorARN, _ := params["MonitorArn"].(string)
	subs, err := p.store.ListAnomalySubscriptions(monitorARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(subs))
	for _, s := range subs {
		list = append(list, anomalySubscriptionToMap(&s))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AnomalySubscriptions": list,
	})
}

func (p *Provider) updateAnomalySubscription(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["SubscriptionArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "SubscriptionArn is required", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetAnomalySubscriptionByARN(arn)
	if err != nil {
		return shared.JSONError("UnknownSubscriptionException", "subscription not found", http.StatusBadRequest), nil
	}
	name := existing.Name
	if n, ok := params["SubscriptionName"].(string); ok && n != "" {
		name = n
	}
	frequency := existing.Frequency
	if f, ok := params["Frequency"].(string); ok && f != "" {
		frequency = f
	}
	threshold := existing.Threshold
	if t, ok := params["Threshold"].(float64); ok {
		threshold = t
	}
	monitorARNsJSON := existing.MonitorARNs
	if ma, ok := params["MonitorArnList"]; ok {
		b, _ := json.Marshal(ma)
		monitorARNsJSON = string(b)
	}
	subscribersJSON := existing.Subscribers
	if subs, ok := params["Subscribers"]; ok {
		b, _ := json.Marshal(subs)
		subscribersJSON = string(b)
	}
	if err := p.store.UpdateAnomalySubscription(arn, name, monitorARNsJSON, threshold, frequency, subscribersJSON); err != nil {
		return shared.JSONError("UnknownSubscriptionException", "subscription not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"SubscriptionArn": arn,
	})
}

func (p *Provider) deleteAnomalySubscription(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["SubscriptionArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "SubscriptionArn is required", http.StatusBadRequest), nil
	}
	_ = p.store.tags.DeleteAllTags(arn) //nolint:errcheck
	if err := p.store.DeleteAnomalySubscription(arn); err != nil {
		return shared.JSONError("UnknownSubscriptionException", "subscription not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

// ----- Tags -----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawTags, _ := params["ResourceTags"].([]any)
	if err := p.store.tags.AddTags(arn, parseTags(rawTags)); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	rawKeys, _ := params["ResourceTagKeys"].([]any)
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
	arn, _ := params["ResourceArn"].(string)
	if arn == "" {
		return shared.JSONError("ValidationException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResourceTags": tagList,
	})
}

// ----- Cost/Usage dummy -----

func (p *Provider) getCostAndUsage(params map[string]any) (*plugin.Response, error) {
	period := extractTimePeriod(params)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ResultsByTime":            []any{},
		"DimensionValueAttributes": []any{},
		"NextPageToken":            nil,
		"TimePeriod":               period,
	})
}

func (p *Provider) getCostForecast(params map[string]any) (*plugin.Response, error) {
	period := extractTimePeriod(params)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Total":                 map[string]any{"Amount": "0.00", "Unit": "USD"},
		"ForecastResultsByTime": []any{},
		"TimePeriod":            period,
	})
}

func (p *Provider) getUsageForecast(params map[string]any) (*plugin.Response, error) {
	period := extractTimePeriod(params)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Total":                 map[string]any{"Amount": "0.00", "Unit": "QUANTITY"},
		"ForecastResultsByTime": []any{},
		"TimePeriod":            period,
	})
}

func (p *Provider) getDimensionValues(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"DimensionValues": []any{},
		"ReturnSize":      0,
		"TotalSize":       0,
	})
}

func (p *Provider) getTagsQuery(_ map[string]any) (*plugin.Response, error) {
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"Tags":       []any{},
		"ReturnSize": 0,
		"TotalSize":  0,
	})
}

func (p *Provider) provideAnomalyFeedback(params map[string]any) (*plugin.Response, error) {
	anomalyID, _ := params["AnomalyId"].(string)
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AnomalyId": anomalyID,
	})
}

// ----- helpers -----

func extractTimePeriod(params map[string]any) map[string]any {
	if tp, ok := params["TimePeriod"].(map[string]any); ok {
		return tp
	}
	return map[string]any{"Start": "", "End": ""}
}

func costCategoryToMap(c *CostCategory) map[string]any {
	var rules any
	_ = json.Unmarshal([]byte(c.Rules), &rules)
	return map[string]any{
		"CostCategoryArn": c.ARN,
		"Name":            c.Name,
		"RuleVersion":     c.RuleVersion,
		"Rules":           rules,
		"EffectiveStart":  c.EffectiveStart,
	}
}

func anomalyMonitorToMap(m *AnomalyMonitor) map[string]any {
	return map[string]any{
		"MonitorArn":       m.ARN,
		"MonitorName":      m.Name,
		"MonitorType":      m.Type,
		"MonitorDimension": m.Dimension,
		"CreationDate":     m.CreatedAt.Format("2006-01-02"),
	}
}

func anomalySubscriptionToMap(s *AnomalySubscription) map[string]any {
	var monitorARNs any
	var subscribers any
	_ = json.Unmarshal([]byte(s.MonitorARNs), &monitorARNs)
	_ = json.Unmarshal([]byte(s.Subscribers), &subscribers)
	return map[string]any{
		"SubscriptionArn":  s.ARN,
		"SubscriptionName": s.Name,
		"MonitorArnList":   monitorARNs,
		"Threshold":        s.Threshold,
		"Frequency":        s.Frequency,
		"Subscribers":      subscribers,
	}
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
