// SPDX-License-Identifier: Apache-2.0

// internal/services/configservice/provider.go
package configservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the StarlingDoveService (AWS Config) service.
type Provider struct {
	store *Store
}

func (p *Provider) ServiceID() string             { return "configservice" }
func (p *Provider) ServiceName() string           { return "StarlingDoveService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "configservice"))
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
	// ---- ConfigRule ----
	case "PutConfigRule":
		return p.putConfigRule(params)
	case "DescribeConfigRules":
		return p.describeConfigRules(params)
	case "DeleteConfigRule":
		return p.deleteConfigRule(params)
	case "StartConfigRulesEvaluation":
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	case "DescribeConfigRuleEvaluationStatus":
		return p.describeConfigRuleEvaluationStatus(params)

	// ---- ConfigurationRecorder ----
	case "PutConfigurationRecorder":
		return p.putConfigurationRecorder(params)
	case "DescribeConfigurationRecorders":
		return p.describeConfigurationRecorders(params)
	case "DescribeConfigurationRecorderStatus":
		return p.describeConfigurationRecorderStatus(params)
	case "StartConfigurationRecorder":
		return p.startConfigurationRecorder(params)
	case "StopConfigurationRecorder":
		return p.stopConfigurationRecorder(params)
	case "DeleteConfigurationRecorder":
		return p.deleteConfigurationRecorder(params)

	// ---- DeliveryChannel ----
	case "PutDeliveryChannel":
		return p.putDeliveryChannel(params)
	case "DescribeDeliveryChannels":
		return p.describeDeliveryChannels(params)
	case "DescribeDeliveryChannelStatus":
		return p.describeDeliveryChannelStatus(params)
	case "DeleteDeliveryChannel":
		return p.deleteDeliveryChannel(params)
	case "DeliverConfigSnapshot":
		return shared.JSONResponse(http.StatusOK, map[string]any{"configSnapshotId": shared.GenerateUUID()})

	// ---- ConformancePack ----
	case "PutConformancePack":
		return p.putConformancePack(params)
	case "DescribeConformancePacks":
		return p.describeConformancePacks(params)
	case "DescribeConformancePackStatus":
		return p.describeConformancePackStatus(params)
	case "DescribeConformancePackCompliance":
		return p.describeConformancePackCompliance(params)
	case "DeleteConformancePack":
		return p.deleteConformancePack(params)

	// ---- ConfigurationAggregator ----
	case "PutConfigurationAggregator":
		return p.putConfigurationAggregator(params)
	case "DescribeConfigurationAggregators":
		return p.describeConfigurationAggregators(params)
	case "DescribeConfigurationAggregatorSourcesStatus":
		return p.describeConfigurationAggregatorSourcesStatus(params)
	case "DeleteConfigurationAggregator":
		return p.deleteConfigurationAggregator(params)

	// ---- StoredQuery ----
	case "PutStoredQuery":
		return p.putStoredQuery(params)
	case "GetStoredQuery":
		return p.getStoredQuery(params)
	case "ListStoredQueries":
		return p.listStoredQueries(params)
	case "DeleteStoredQuery":
		return p.deleteStoredQuery(params)

	// ---- RetentionConfiguration ----
	case "PutRetentionConfiguration":
		return p.putRetentionConfiguration(params)
	case "DescribeRetentionConfigurations":
		return p.describeRetentionConfigurations(params)
	case "DeleteRetentionConfiguration":
		return p.deleteRetentionConfiguration(params)

	// ---- AggregationAuthorization ----
	case "PutAggregationAuthorization":
		return p.putAggregationAuthorization(params)
	case "DescribeAggregationAuthorizations":
		return p.describeAggregationAuthorizations(params)
	case "DeleteAggregationAuthorization":
		return p.deleteAggregationAuthorization(params)
	case "DescribePendingAggregationRequests":
		return shared.JSONResponse(http.StatusOK, map[string]any{"PendingAggregationRequests": []any{}})

	// ---- RemediationConfiguration ----
	case "PutRemediationConfigurations":
		return p.putRemediationConfigurations(params)
	case "DescribeRemediationConfigurations":
		return p.describeRemediationConfigurations(params)
	case "DeleteRemediationConfiguration":
		return p.deleteRemediationConfiguration(params)
	case "PutRemediationExceptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"FailedBatches": []any{}})
	case "DescribeRemediationExceptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RemediationExceptions": []any{}})
	case "DeleteRemediationExceptions":
		return shared.JSONResponse(http.StatusOK, map[string]any{"FailedBatches": []any{}})
	case "StartRemediationExecution":
		return shared.JSONResponse(http.StatusOK, map[string]any{"FailureMessage": "", "FailedItems": []any{}})
	case "DescribeRemediationExecutionStatus":
		return shared.JSONResponse(http.StatusOK, map[string]any{"RemediationExecutionStatuses": []any{}})

	// ---- Evaluations ----
	case "PutEvaluations":
		return shared.JSONResponse(http.StatusOK, map[string]any{"FailedEvaluations": []any{}})
	case "PutExternalEvaluation":
		return shared.JSONResponse(http.StatusOK, map[string]any{})

	// ---- Tags ----
	case "TagResource":
		return p.tagResource(params)
	case "UntagResource":
		return p.untagResource(params)
	case "ListTagsForResource":
		return p.listTagsForResource(params)

	// ---- ~60 remaining ops: compliance, aggregate queries, org rules, resource discovery ----
	default:
		return shared.JSONResponse(http.StatusOK, map[string]any{})
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	rules, err := p.store.ListConfigRules(nil)
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(rules))
	for _, r := range rules {
		res = append(res, plugin.Resource{Type: "config-rule", ID: r.Name, Name: r.Name})
	}
	return res, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// ---- helpers ----

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return def
}

func boolParam(params map[string]any, key string) bool {
	if v, ok := params[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func marshalParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "{}"
}

func marshalParamArray(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		b, _ := json.Marshal(v)
		return string(b)
	}
	return "[]"
}

func stringsParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func tagsFromParams(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	// JSON 1.1: Tags is a map[string]string
	if m, ok := v.(map[string]any); ok {
		out := make(map[string]string, len(m))
		for k, val := range m {
			if s, ok := val.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	// Tags as array of {Key, Value}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			k := strParam(m, "Key")
			val := strParam(m, "Value")
			if k != "" {
				out[k] = val
			}
		}
	}
	return out
}

func tagsToList(tags map[string]string) []any {
	list := make([]any, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]any{"Key": k, "Value": v})
	}
	return list
}

func buildConfigRuleARN(name string) string {
	return shared.BuildARN("config", "config-rule", name)
}

func buildConformancePackARN(name string) string {
	return shared.BuildARN("config", "conformance-pack", name)
}

func buildAggregatorARN(name string) string {
	return shared.BuildARN("config", "config-aggregator", name)
}

func buildStoredQueryARN(name string) string {
	return shared.BuildARN("config", "stored-query", name)
}

func buildAggAuthARN(account, region string) string {
	return shared.BuildARNWithAccount("config", "aggregation-authorization", account+"/"+region, region, account)
}

// ---- ConfigRule handlers ----

func (p *Provider) putConfigRule(params map[string]any) (*plugin.Response, error) {
	input, _ := params["ConfigRule"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidParameterValueException", "ConfigRule is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "ConfigRuleName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigRuleName is required", http.StatusBadRequest), nil
	}
	r := &ConfigRule{
		Name:        name,
		ARN:         buildConfigRuleARN(name),
		Source:      marshalParam(input, "Source"),
		Scope:       marshalParam(input, "Scope"),
		InputParams: strParam(input, "InputParameters"),
		State:       "ACTIVE",
		CreatedAt:   now(),
	}
	if err := p.store.PutConfigRule(r); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeConfigRules(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigRuleNames")
	rules, err := p.store.ListConfigRules(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(rules))
	for _, r := range rules {
		list = append(list, configRuleToMap(r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigRules": list})
}

func (p *Provider) deleteConfigRule(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigRuleName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigRuleName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteConfigRule(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchConfigRuleException", "Config rule not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeConfigRuleEvaluationStatus(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigRuleNames")
	rules, err := p.store.ListConfigRules(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(rules))
	for _, r := range rules {
		list = append(list, map[string]any{
			"ConfigRuleName": r.Name,
			"ConfigRuleArn":  r.ARN,
			"ConfigRuleId":   r.Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigRulesEvaluationStatus": list})
}

func configRuleToMap(r *ConfigRule) map[string]any {
	var source, scope any
	_ = json.Unmarshal([]byte(r.Source), &source)
	_ = json.Unmarshal([]byte(r.Scope), &scope)
	return map[string]any{
		"ConfigRuleName":  r.Name,
		"ConfigRuleArn":   r.ARN,
		"Source":          source,
		"Scope":           scope,
		"InputParameters": r.InputParams,
		"ConfigRuleState": r.State,
	}
}

// ---- ConfigurationRecorder handlers ----

func (p *Provider) putConfigurationRecorder(params map[string]any) (*plugin.Response, error) {
	input, _ := params["ConfigurationRecorder"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationRecorder is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "name")
	if name == "" {
		name = strParam(input, "Name")
	}
	if name == "" {
		name = "default"
	}
	r := &ConfigurationRecorder{
		Name:           name,
		RoleARN:        strParam(input, "roleARN"),
		RecordingGroup: marshalParam(input, "recordingGroup"),
		RecordingMode:  marshalParam(input, "recordingMode"),
		Status:         "STOPPED",
	}
	if r.RoleARN == "" {
		r.RoleARN = strParam(input, "RoleARN")
	}
	if err := p.store.PutConfigurationRecorder(r); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeConfigurationRecorders(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigurationRecorderNames")
	recs, err := p.store.ListConfigurationRecorders(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(recs))
	for _, r := range recs {
		list = append(list, recorderToMap(r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigurationRecorders": list})
}

func (p *Provider) describeConfigurationRecorderStatus(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigurationRecorderNames")
	recs, err := p.store.ListConfigurationRecorders(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(recs))
	for _, r := range recs {
		list = append(list, map[string]any{
			"name":      r.Name,
			"recording": r.Status == "RECORDING",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigurationRecordersStatus": list})
}

func (p *Provider) startConfigurationRecorder(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationRecorderName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationRecorderName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRecorderStatus(name, "RECORDING"); err != nil {
		if err == errRecorderNotFound {
			return shared.JSONError("NoSuchConfigurationRecorderException", "Recorder not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) stopConfigurationRecorder(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationRecorderName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationRecorderName is required", http.StatusBadRequest), nil
	}
	if err := p.store.UpdateRecorderStatus(name, "STOPPED"); err != nil {
		if err == errRecorderNotFound {
			return shared.JSONError("NoSuchConfigurationRecorderException", "Recorder not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteConfigurationRecorder(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationRecorderName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationRecorderName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteConfigurationRecorder(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchConfigurationRecorderException", "Recorder not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func recorderToMap(r *ConfigurationRecorder) map[string]any {
	var rg, rm any
	_ = json.Unmarshal([]byte(r.RecordingGroup), &rg)
	_ = json.Unmarshal([]byte(r.RecordingMode), &rm)
	return map[string]any{
		"name":           r.Name,
		"roleARN":        r.RoleARN,
		"recordingGroup": rg,
		"recordingMode":  rm,
	}
}

// ---- DeliveryChannel handlers ----

func (p *Provider) putDeliveryChannel(params map[string]any) (*plugin.Response, error) {
	input, _ := params["DeliveryChannel"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidParameterValueException", "DeliveryChannel is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "name")
	if name == "" {
		name = strParam(input, "Name")
	}
	if name == "" {
		name = "default"
	}
	d := &DeliveryChannel{
		Name:      name,
		S3Bucket:  strParam(input, "s3BucketName"),
		S3Prefix:  strParam(input, "s3KeyPrefix"),
		SNSTopic:  strParam(input, "snsTopicARN"),
		Frequency: "TwentyFour_Hours",
	}
	if freq, ok := input["configSnapshotDeliveryProperties"].(map[string]any); ok {
		if f := strParam(freq, "deliveryFrequency"); f != "" {
			d.Frequency = f
		}
	}
	if err := p.store.PutDeliveryChannel(d); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) describeDeliveryChannels(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "DeliveryChannelNames")
	channels, err := p.store.ListDeliveryChannels(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(channels))
	for _, d := range channels {
		list = append(list, deliveryChannelToMap(d))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DeliveryChannels": list})
}

func (p *Provider) describeDeliveryChannelStatus(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "DeliveryChannelNames")
	channels, err := p.store.ListDeliveryChannels(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(channels))
	for _, d := range channels {
		list = append(list, map[string]any{
			"name": d.Name,
			"configHistoryDeliveryInfo": map[string]any{
				"lastStatus": "SUCCESS",
			},
			"configStreamDeliveryInfo": map[string]any{
				"lastStatus": "SUCCESS",
			},
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"DeliveryChannelsStatus": list})
}

func (p *Provider) deleteDeliveryChannel(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "DeliveryChannelName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "DeliveryChannelName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteDeliveryChannel(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchDeliveryChannelException", "Delivery channel not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func deliveryChannelToMap(d *DeliveryChannel) map[string]any {
	return map[string]any{
		"name":         d.Name,
		"s3BucketName": d.S3Bucket,
		"s3KeyPrefix":  d.S3Prefix,
		"snsTopicARN":  d.SNSTopic,
		"configSnapshotDeliveryProperties": map[string]any{
			"deliveryFrequency": d.Frequency,
		},
	}
}

// ---- ConformancePack handlers ----

func (p *Provider) putConformancePack(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConformancePackName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConformancePackName is required", http.StatusBadRequest), nil
	}
	c := &ConformancePack{
		Name:           name,
		ARN:            buildConformancePackARN(name),
		TemplateBody:   strParam(params, "TemplateBody"),
		DeliveryBucket: strParam(params, "DeliveryS3Bucket"),
		Status:         "CREATE_COMPLETE",
		CreatedAt:      now(),
	}
	if err := p.store.PutConformancePack(c); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConformancePackArn": c.ARN})
}

func (p *Provider) describeConformancePacks(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConformancePackNames")
	packs, err := p.store.ListConformancePacks(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(packs))
	for _, c := range packs {
		list = append(list, conformancePackToMap(c))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConformancePackDetails": list})
}

func (p *Provider) describeConformancePackStatus(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConformancePackNames")
	packs, err := p.store.ListConformancePacks(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(packs))
	for _, c := range packs {
		list = append(list, map[string]any{
			"ConformancePackName":         c.Name,
			"ConformancePackArn":          c.ARN,
			"ConformancePackState":        c.Status,
			"ConformancePackStatusReason": "",
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConformancePackStatusDetails": list})
}

func (p *Provider) describeConformancePackCompliance(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConformancePackName")
	_ = name
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ConformancePackName":               name,
		"ConformancePackRuleComplianceList": []any{},
	})
}

func (p *Provider) deleteConformancePack(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConformancePackName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConformancePackName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteConformancePack(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchConformancePackException", "Conformance pack not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func conformancePackToMap(c *ConformancePack) map[string]any {
	return map[string]any{
		"ConformancePackName":            c.Name,
		"ConformancePackArn":             c.ARN,
		"DeliveryS3Bucket":               c.DeliveryBucket,
		"ConformancePackInputParameters": []any{},
		"CreatedBy":                      "",
	}
}

// ---- ConfigurationAggregator handlers ----

func (p *Provider) putConfigurationAggregator(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationAggregatorName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationAggregatorName is required", http.StatusBadRequest), nil
	}
	a := &Aggregator{
		Name:           name,
		ARN:            buildAggregatorARN(name),
		AccountSources: marshalParamArray(params, "AccountAggregationSources"),
		OrgSource:      marshalParam(params, "OrganizationAggregationSource"),
		CreatedAt:      now(),
	}
	if err := p.store.PutAggregator(a); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"ConfigurationAggregator": aggregatorToMap(a),
	})
}

func (p *Provider) describeConfigurationAggregators(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigurationAggregatorNames")
	aggs, err := p.store.ListAggregators(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(aggs))
	for _, a := range aggs {
		list = append(list, aggregatorToMap(a))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"ConfigurationAggregators": list})
}

func (p *Provider) describeConfigurationAggregatorSourcesStatus(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationAggregatorName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationAggregatorName is required", http.StatusBadRequest), nil
	}
	_, err := p.store.GetAggregator(name)
	if err != nil {
		if err == errAggregatorNotFound {
			return shared.JSONError("NoSuchConfigurationAggregatorException", "Aggregator not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AggregatedSourceStatusList": []any{}})
}

func (p *Provider) deleteConfigurationAggregator(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigurationAggregatorName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigurationAggregatorName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteAggregator(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchConfigurationAggregatorException", "Aggregator not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func aggregatorToMap(a *Aggregator) map[string]any {
	var acctSrc, orgSrc any
	_ = json.Unmarshal([]byte(a.AccountSources), &acctSrc)
	_ = json.Unmarshal([]byte(a.OrgSource), &orgSrc)
	return map[string]any{
		"ConfigurationAggregatorName":   a.Name,
		"ConfigurationAggregatorArn":    a.ARN,
		"AccountAggregationSources":     acctSrc,
		"OrganizationAggregationSource": orgSrc,
	}
}

// ---- StoredQuery handlers ----

func (p *Provider) putStoredQuery(params map[string]any) (*plugin.Response, error) {
	input, _ := params["StoredQuery"].(map[string]any)
	if input == nil {
		return shared.JSONError("InvalidParameterValueException", "StoredQuery is required", http.StatusBadRequest), nil
	}
	name := strParam(input, "QueryName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "QueryName is required", http.StatusBadRequest), nil
	}
	q := &StoredQuery{
		Name:        name,
		ID:          shared.GenerateUUID(),
		ARN:         buildStoredQueryARN(name),
		Expression:  strParam(input, "Expression"),
		Description: strParam(input, "Description"),
	}
	// Keep existing ID if present
	existing, _ := p.store.GetStoredQuery(name)
	if existing != nil {
		q.ID = existing.ID
		q.ARN = existing.ARN
	}
	if err := p.store.PutStoredQuery(q); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"QueryArn": q.ARN})
}

func (p *Provider) getStoredQuery(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "QueryName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "QueryName is required", http.StatusBadRequest), nil
	}
	q, err := p.store.GetStoredQuery(name)
	if err != nil {
		if err == errStoredQueryNotFound {
			return shared.JSONError("ResourceNotFoundException", "Stored query not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"StoredQuery": storedQueryToMap(q)})
}

func (p *Provider) listStoredQueries(_ map[string]any) (*plugin.Response, error) {
	queries, err := p.store.ListStoredQueries()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(queries))
	for _, q := range queries {
		list = append(list, map[string]any{
			"QueryId":   q.ID,
			"QueryArn":  q.ARN,
			"QueryName": q.Name,
		})
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"StoredQueryMetadata": list})
}

func (p *Provider) deleteStoredQuery(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "QueryName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "QueryName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteStoredQuery(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("ResourceNotFoundException", "Stored query not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func storedQueryToMap(q *StoredQuery) map[string]any {
	return map[string]any{
		"QueryId":     q.ID,
		"QueryArn":    q.ARN,
		"QueryName":   q.Name,
		"Expression":  q.Expression,
		"Description": q.Description,
	}
}

// ---- RetentionConfiguration handlers ----

func (p *Provider) putRetentionConfiguration(params map[string]any) (*plugin.Response, error) {
	days := intParam(params, "RetentionPeriodInDays", 2557)
	r := &RetentionConfig{
		Name:          "default",
		RetentionDays: days,
	}
	if err := p.store.PutRetentionConfig(r); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"RetentionConfiguration": retentionConfigToMap(r),
	})
}

func (p *Provider) describeRetentionConfigurations(_ map[string]any) (*plugin.Response, error) {
	configs, err := p.store.ListRetentionConfigs()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(configs))
	for _, r := range configs {
		list = append(list, retentionConfigToMap(r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RetentionConfigurations": list})
}

func (p *Provider) deleteRetentionConfiguration(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "RetentionConfigurationName")
	if name == "" {
		name = "default"
	}
	found, err := p.store.DeleteRetentionConfig(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchRetentionConfigurationException", "Retention configuration not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func retentionConfigToMap(r *RetentionConfig) map[string]any {
	return map[string]any{
		"Name":                  r.Name,
		"RetentionPeriodInDays": r.RetentionDays,
	}
}

// ---- AggregationAuthorization handlers ----

func (p *Provider) putAggregationAuthorization(params map[string]any) (*plugin.Response, error) {
	account := strParam(params, "AuthorizedAccountId")
	region := strParam(params, "AuthorizedAwsRegion")
	if account == "" || region == "" {
		return shared.JSONError("InvalidParameterValueException", "AuthorizedAccountId and AuthorizedAwsRegion are required", http.StatusBadRequest), nil
	}
	a := &AggregationAuthorization{
		ARN:               buildAggAuthARN(account, region),
		AuthorizedAccount: account,
		AuthorizedRegion:  region,
		CreatedAt:         now(),
	}
	if err := p.store.PutAggregationAuthorization(a); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{
		"AggregationAuthorization": aggAuthToMap(a),
	})
}

func (p *Provider) describeAggregationAuthorizations(_ map[string]any) (*plugin.Response, error) {
	auths, err := p.store.ListAggregationAuthorizations()
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(auths))
	for _, a := range auths {
		list = append(list, aggAuthToMap(a))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"AggregationAuthorizations": list})
}

func (p *Provider) deleteAggregationAuthorization(params map[string]any) (*plugin.Response, error) {
	account := strParam(params, "AuthorizedAccountId")
	region := strParam(params, "AuthorizedAwsRegion")
	if account == "" || region == "" {
		return shared.JSONError("InvalidParameterValueException", "AuthorizedAccountId and AuthorizedAwsRegion are required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteAggregationAuthorization(account, region)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchAggregationAuthorizationException", "Aggregation authorization not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func aggAuthToMap(a *AggregationAuthorization) map[string]any {
	return map[string]any{
		"AggregationAuthorizationArn": a.ARN,
		"AuthorizedAccountId":         a.AuthorizedAccount,
		"AuthorizedAwsRegion":         a.AuthorizedRegion,
	}
}

// ---- RemediationConfiguration handlers ----

func (p *Provider) putRemediationConfigurations(params map[string]any) (*plugin.Response, error) {
	items, _ := params["RemediationConfigurations"].([]any)
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ruleName := strParam(m, "ConfigRuleName")
		if ruleName == "" {
			continue
		}
		r := &RemediationConfig{
			ConfigRuleName: ruleName,
			TargetType:     strParam(m, "TargetType"),
			TargetID:       strParam(m, "TargetId"),
			Parameters:     marshalParam(m, "Parameters"),
		}
		if r.TargetType == "" {
			r.TargetType = "SSM_DOCUMENT"
		}
		if boolParam(m, "Automatic") {
			r.Automatic = 1
		}
		if err := p.store.PutRemediationConfig(r); err != nil {
			return nil, err
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"FailedBatches": []any{}})
}

func (p *Provider) describeRemediationConfigurations(params map[string]any) (*plugin.Response, error) {
	names := stringsParam(params, "ConfigRuleNames")
	configs, err := p.store.ListRemediationConfigs(names)
	if err != nil {
		return nil, err
	}
	list := make([]any, 0, len(configs))
	for _, r := range configs {
		list = append(list, remediationConfigToMap(r))
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"RemediationConfigurations": list})
}

func (p *Provider) deleteRemediationConfiguration(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "ConfigRuleName")
	if name == "" {
		return shared.JSONError("InvalidParameterValueException", "ConfigRuleName is required", http.StatusBadRequest), nil
	}
	found, err := p.store.DeleteRemediationConfig(name)
	if err != nil {
		return nil, err
	}
	if !found {
		return shared.JSONError("NoSuchRemediationConfigurationException", "Remediation configuration not found", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func remediationConfigToMap(r *RemediationConfig) map[string]any {
	var params any
	_ = json.Unmarshal([]byte(r.Parameters), &params)
	return map[string]any{
		"ConfigRuleName": r.ConfigRuleName,
		"TargetType":     r.TargetType,
		"TargetId":       r.TargetID,
		"Parameters":     params,
		"Automatic":      r.Automatic == 1,
	}
}

// ---- Tags handlers ----

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags := tagsFromParams(params, "Tags")
	if err := p.store.AddTags(arn, tags); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	keys := stringsParam(params, "TagKeys")
	if err := p.store.RemoveTags(arn, keys); err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	arn := strParam(params, "ResourceArn")
	if arn == "" {
		return shared.JSONError("InvalidParameterValueException", "ResourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTags(arn)
	if err != nil {
		return nil, err
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"Tags": tagsToList(tags)})
}
