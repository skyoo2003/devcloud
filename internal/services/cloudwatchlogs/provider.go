// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatchlogs/provider.go
package cloudwatchlogs

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

type Provider struct {
	store      *LogsStore
	serverPort int
}

func (p *Provider) ServiceID() string             { return "cloudwatchlogs" }
func (p *Provider) ServiceName() string           { return "Logs" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	if port, ok := cfg.Options["server_port"].(int); ok {
		p.serverPort = port
	}
	var err error
	p.store, err = NewLogsStore(filepath.Join(dataDir, "cloudwatchlogs"))
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
		return cwlError("InvalidParameterException", "failed to read body", http.StatusBadRequest), nil
	}
	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			return cwlError("InvalidParameterException", "invalid JSON", http.StatusBadRequest), nil
		}
	} else {
		params = map[string]any{}
	}

	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		} else {
			action = target
		}
	}

	switch action {
	case "CreateLogGroup":
		return p.createLogGroup(params)
	case "DeleteLogGroup":
		return p.deleteLogGroup(params)
	case "DescribeLogGroups":
		return p.describeLogGroups(params)
	case "CreateLogStream":
		return p.createLogStream(params)
	case "DeleteLogStream":
		return p.deleteLogStream(params)
	case "DescribeLogStreams":
		return p.describeLogStreams(params)
	case "PutLogEvents":
		return p.putLogEvents(params)
	case "GetLogEvents":
		return p.getLogEvents(params)
	case "FilterLogEvents":
		return p.filterLogEvents(params)
	// Retention
	case "PutRetentionPolicy":
		return p.putRetentionPolicy(params)
	case "DeleteRetentionPolicy":
		return p.deleteRetentionPolicy(params)
	// Metric Filters
	case "PutMetricFilter":
		return p.putMetricFilter(params)
	case "DeleteMetricFilter":
		return p.deleteMetricFilter(params)
	case "DescribeMetricFilters":
		return p.describeMetricFilters(params)
	// Subscription Filters
	case "PutSubscriptionFilter":
		return p.putSubscriptionFilter(params)
	case "DeleteSubscriptionFilter":
		return p.deleteSubscriptionFilter(params)
	case "DescribeSubscriptionFilters":
		return p.describeSubscriptionFilters(params)
	// Tags (new and deprecated APIs)
	case "TagLogGroup", "TagResource":
		return p.tagResource(params)
	case "UntagLogGroup", "UntagResource":
		return p.untagResource(params)
	case "ListTagsLogGroup", "ListTagsForResource":
		return p.listTagsForResource(params)
	default:
		return cwlError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	groups, err := p.store.DescribeLogGroups(defaultAccountID, "")
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(groups))
	for _, g := range groups {
		resources = append(resources, plugin.Resource{Type: "log-group", ID: g.Name, Name: g.Name})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

func (p *Provider) createLogGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["logGroupName"].(string)
	if name == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateLogGroup(name, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteLogGroup(params map[string]any) (*plugin.Response, error) {
	name, _ := params["logGroupName"].(string)
	if name == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLogGroup(name, defaultAccountID); err != nil {
		return cwlError("ResourceNotFoundException", "log group not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeLogGroups(params map[string]any) (*plugin.Response, error) {
	prefix, _ := params["logGroupNamePrefix"].(string)
	groups, err := p.store.DescribeLogGroups(defaultAccountID, prefix)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		item := map[string]any{
			"logGroupName": g.Name,
			"creationTime": g.CreatedAt.UnixMilli(),
		}
		if g.RetentionDays > 0 {
			item["retentionInDays"] = g.RetentionDays
		}
		list = append(list, item)
	}
	return jsonResp(http.StatusOK, map[string]any{"logGroups": list})
}

func (p *Provider) createLogStream(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	streamName, _ := params["logStreamName"].(string)
	if groupName == "" || streamName == "" {
		return cwlError("InvalidParameterException", "logGroupName and logStreamName are required", http.StatusBadRequest), nil
	}
	if err := p.store.CreateLogStream(groupName, streamName, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteLogStream(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	streamName, _ := params["logStreamName"].(string)
	if groupName == "" || streamName == "" {
		return cwlError("InvalidParameterException", "logGroupName and logStreamName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteLogStream(groupName, streamName, defaultAccountID); err != nil {
		return cwlError("ResourceNotFoundException", "log stream not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeLogStreams(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	if groupName == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	streams, err := p.store.DescribeLogStreams(groupName, defaultAccountID)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		list = append(list, map[string]any{
			"logStreamName":       s.StreamName,
			"firstEventTimestamp": s.FirstEventTS,
			"lastEventTimestamp":  s.LastEventTS,
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"logStreams": list})
}

func (p *Provider) putLogEvents(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	streamName, _ := params["logStreamName"].(string)
	eventsRaw, _ := params["logEvents"].([]any)
	if groupName == "" || streamName == "" {
		return cwlError("InvalidParameterException", "logGroupName and logStreamName are required", http.StatusBadRequest), nil
	}
	// Check that the log group exists before inserting events.
	groups, err := p.store.DescribeLogGroups(defaultAccountID, groupName)
	if err != nil {
		return nil, err
	}
	groupFound := false
	for _, g := range groups {
		if g.Name == groupName {
			groupFound = true
			break
		}
	}
	if !groupFound {
		return cwlError("ResourceNotFoundException", "The specified log group does not exist.", http.StatusBadRequest), nil
	}

	events := make([]InputLogEvent, 0, len(eventsRaw))
	for _, e := range eventsRaw {
		if m, ok := e.(map[string]any); ok {
			var ts int64
			if t, ok := m["timestamp"].(float64); ok {
				ts = int64(t)
			}
			msg, _ := m["message"].(string)
			events = append(events, InputLogEvent{Timestamp: ts, Message: msg})
		}
	}
	if err := p.store.PutLogEvents(groupName, streamName, defaultAccountID, events); err != nil {
		return nil, err
	}

	// Async integrations: metric filters and subscription filters.
	if p.serverPort > 0 && len(events) > 0 {
		go p.dispatchMetricFilters(groupName, events)
		go p.dispatchSubscriptionFilters(groupName, streamName, events)
	}

	return jsonResp(http.StatusOK, map[string]any{"nextSequenceToken": "1"})
}

func (p *Provider) getLogEvents(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	streamName, _ := params["logStreamName"].(string)
	if groupName == "" || streamName == "" {
		return cwlError("InvalidParameterException", "logGroupName and logStreamName are required", http.StatusBadRequest), nil
	}
	var startTime, endTime int64
	if st, ok := params["startTime"].(float64); ok {
		startTime = int64(st)
	}
	if et, ok := params["endTime"].(float64); ok {
		endTime = int64(et)
	}
	limit := 0
	if l, ok := params["limit"].(float64); ok {
		limit = int(l)
	}
	events, err := p.store.GetLogEvents(groupName, streamName, defaultAccountID, startTime, endTime, limit)
	if err != nil {
		return nil, err
	}
	list := eventsToMap(events)
	return jsonResp(http.StatusOK, map[string]any{"events": list})
}

func (p *Provider) filterLogEvents(params map[string]any) (*plugin.Response, error) {
	groupName, _ := params["logGroupName"].(string)
	if groupName == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	var startTime, endTime int64
	if st, ok := params["startTime"].(float64); ok {
		startTime = int64(st)
	}
	if et, ok := params["endTime"].(float64); ok {
		endTime = int64(et)
	}
	filterPattern, _ := params["filterPattern"].(string)
	limit := 0
	if l, ok := params["limit"].(float64); ok {
		limit = int(l)
	}
	events, err := p.store.FilterLogEvents(groupName, defaultAccountID, startTime, endTime, filterPattern, limit)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"events": eventsToMap(events)})
}

// ── Retention ─────────────────────────────────────────────────────────────────

func (p *Provider) putRetentionPolicy(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	days, ok := params["retentionInDays"].(float64)
	if !ok {
		return cwlError("InvalidParameterException", "retentionInDays is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutRetentionPolicy(logGroupName, defaultAccountID, int(days)); err != nil {
		return cwlError("ResourceNotFoundException", "log group not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteRetentionPolicy(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRetentionPolicy(logGroupName, defaultAccountID); err != nil {
		return cwlError("ResourceNotFoundException", "log group not found", http.StatusBadRequest), nil
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

// ── Metric Filters ────────────────────────────────────────────────────────────

func (p *Provider) putMetricFilter(params map[string]any) (*plugin.Response, error) {
	filterName, _ := params["filterName"].(string)
	logGroupName, _ := params["logGroupName"].(string)
	filterPattern, _ := params["filterPattern"].(string)
	if filterName == "" || logGroupName == "" {
		return cwlError("InvalidParameterException", "filterName and logGroupName are required", http.StatusBadRequest), nil
	}
	transformsRaw, _ := params["metricTransformations"].([]any)
	if len(transformsRaw) == 0 {
		return cwlError("InvalidParameterException", "metricTransformations is required", http.StatusBadRequest), nil
	}
	t0, _ := transformsRaw[0].(map[string]any)
	metricName, _ := t0["metricName"].(string)
	metricNamespace, _ := t0["metricNamespace"].(string)
	metricValue, _ := t0["metricValue"].(string)
	var defaultValue *float64
	if dv, ok := t0["defaultValue"].(float64); ok {
		defaultValue = &dv
	}

	f := MetricFilter{
		FilterName:      filterName,
		LogGroupName:    logGroupName,
		AccountID:       defaultAccountID,
		FilterPattern:   filterPattern,
		MetricName:      metricName,
		MetricNamespace: metricNamespace,
		MetricValue:     metricValue,
		DefaultValue:    defaultValue,
		CreatedAt:       time.Now(),
	}
	if err := p.store.PutMetricFilter(f); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteMetricFilter(params map[string]any) (*plugin.Response, error) {
	filterName, _ := params["filterName"].(string)
	logGroupName, _ := params["logGroupName"].(string)
	if filterName == "" || logGroupName == "" {
		return cwlError("InvalidParameterException", "filterName and logGroupName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteMetricFilter(filterName, logGroupName, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeMetricFilters(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	filterNamePrefix, _ := params["filterNamePrefix"].(string)
	filters, err := p.store.DescribeMetricFilters(defaultAccountID, logGroupName, filterNamePrefix)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		transform := map[string]any{
			"metricName":      f.MetricName,
			"metricNamespace": f.MetricNamespace,
			"metricValue":     f.MetricValue,
		}
		if f.DefaultValue != nil {
			transform["defaultValue"] = *f.DefaultValue
		}
		list = append(list, map[string]any{
			"filterName":            f.FilterName,
			"logGroupName":          f.LogGroupName,
			"filterPattern":         f.FilterPattern,
			"metricTransformations": []any{transform},
			"creationTime":          f.CreatedAt.UnixMilli(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"metricFilters": list})
}

// ── Subscription Filters ──────────────────────────────────────────────────────

func (p *Provider) putSubscriptionFilter(params map[string]any) (*plugin.Response, error) {
	filterName, _ := params["filterName"].(string)
	logGroupName, _ := params["logGroupName"].(string)
	filterPattern, _ := params["filterPattern"].(string)
	destinationARN, _ := params["destinationArn"].(string)
	if filterName == "" || logGroupName == "" || destinationARN == "" {
		return cwlError("InvalidParameterException", "filterName, logGroupName, and destinationArn are required", http.StatusBadRequest), nil
	}
	roleARN, _ := params["roleArn"].(string)
	distribution, _ := params["distribution"].(string)

	f := SubscriptionFilter{
		FilterName:     filterName,
		LogGroupName:   logGroupName,
		AccountID:      defaultAccountID,
		FilterPattern:  filterPattern,
		DestinationARN: destinationARN,
		RoleARN:        roleARN,
		Distribution:   distribution,
		CreatedAt:      time.Now(),
	}
	if err := p.store.PutSubscriptionFilter(f); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) deleteSubscriptionFilter(params map[string]any) (*plugin.Response, error) {
	filterName, _ := params["filterName"].(string)
	logGroupName, _ := params["logGroupName"].(string)
	if filterName == "" || logGroupName == "" {
		return cwlError("InvalidParameterException", "filterName and logGroupName are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSubscriptionFilter(filterName, logGroupName, defaultAccountID); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) describeSubscriptionFilters(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName is required", http.StatusBadRequest), nil
	}
	filters, err := p.store.DescribeSubscriptionFilters(defaultAccountID, logGroupName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(filters))
	for _, f := range filters {
		list = append(list, map[string]any{
			"filterName":     f.FilterName,
			"logGroupName":   f.LogGroupName,
			"filterPattern":  f.FilterPattern,
			"destinationArn": f.DestinationARN,
			"roleArn":        f.RoleARN,
			"distribution":   f.Distribution,
			"creationTime":   f.CreatedAt.UnixMilli(),
		})
	}
	return jsonResp(http.StatusOK, map[string]any{"subscriptionFilters": list})
}

// ── Tags ──────────────────────────────────────────────────────────────────────

// logGroupNameFromARN extracts the log group name from an ARN like:
// arn:aws:logs:us-east-1:000000000000:log-group:my-group:*
func logGroupNameFromARN(arn string) string {
	// Try arn:aws:logs:region:account:log-group:NAME or :NAME:*
	parts := strings.Split(arn, ":")
	// parts: [arn, aws, logs, region, account, log-group, name, ...]
	if len(parts) >= 7 && parts[5] == "log-group" {
		return parts[6]
	}
	return ""
}

func (p *Provider) tagResource(params map[string]any) (*plugin.Response, error) {
	// Support both TagLogGroup (logGroupName + tags) and TagResource (resourceArn + tags)
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		arn, _ := params["resourceArn"].(string)
		logGroupName = logGroupNameFromARN(arn)
	}
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName or resourceArn is required", http.StatusBadRequest), nil
	}
	tagsRaw, _ := params["tags"].(map[string]any)
	tags := make(map[string]string, len(tagsRaw))
	for k, v := range tagsRaw {
		if sv, ok := v.(string); ok {
			tags[k] = sv
		}
	}
	if err := p.store.TagLogGroup(logGroupName, defaultAccountID, tags); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) untagResource(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		arn, _ := params["resourceArn"].(string)
		logGroupName = logGroupNameFromARN(arn)
	}
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName or resourceArn is required", http.StatusBadRequest), nil
	}

	// UntagLogGroup uses "tags" (list), UntagResource uses "tagKeys" (list)
	var tagKeys []string
	if raw, ok := params["tagKeys"].([]any); ok {
		for _, k := range raw {
			if s, ok := k.(string); ok {
				tagKeys = append(tagKeys, s)
			}
		}
	} else if raw, ok := params["tags"].([]any); ok {
		for _, k := range raw {
			if s, ok := k.(string); ok {
				tagKeys = append(tagKeys, s)
			}
		}
	}

	if err := p.store.UntagLogGroup(logGroupName, defaultAccountID, tagKeys); err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *Provider) listTagsForResource(params map[string]any) (*plugin.Response, error) {
	logGroupName, _ := params["logGroupName"].(string)
	if logGroupName == "" {
		arn, _ := params["resourceArn"].(string)
		logGroupName = logGroupNameFromARN(arn)
	}
	if logGroupName == "" {
		return cwlError("InvalidParameterException", "logGroupName or resourceArn is required", http.StatusBadRequest), nil
	}
	tags, err := p.store.ListTagsForLogGroup(logGroupName, defaultAccountID)
	if err != nil {
		return nil, err
	}
	return jsonResp(http.StatusOK, map[string]any{"tags": tags})
}

// ── Integration: Metric Filters → CloudWatch ──────────────────────────────────

func (p *Provider) dispatchMetricFilters(logGroupName string, events []InputLogEvent) {
	filters, err := p.store.GetMetricFiltersForGroup(logGroupName, defaultAccountID)
	if err != nil || len(filters) == 0 {
		return
	}
	for _, f := range filters {
		// Simple substring match for filter pattern.
		matched := false
		for _, e := range events {
			if f.FilterPattern == "" || strings.Contains(e.Message, f.FilterPattern) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		p.putCloudWatchMetric(f.MetricNamespace, f.MetricName, f.MetricValue)
	}
}

func (p *Provider) putCloudWatchMetric(namespace, metricName, metricValue string) {
	url := fmt.Sprintf("http://localhost:%d/", p.serverPort)
	payload := map[string]any{
		"Namespace": namespace,
		"MetricData": []any{
			map[string]any{
				"MetricName": metricName,
				"Value":      metricValue,
				"Unit":       "Count",
			},
		},
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "GraniteServiceVersion20100801.PutMetricData")
	http.DefaultClient.Do(req) //nolint:errcheck
}

// ── Integration: Subscription Filters → Lambda ────────────────────────────────

func (p *Provider) dispatchSubscriptionFilters(logGroupName, streamName string, events []InputLogEvent) {
	filters, err := p.store.GetSubscriptionFiltersForGroup(logGroupName, defaultAccountID)
	if err != nil || len(filters) == 0 {
		return
	}
	for _, f := range filters {
		// Collect matching events.
		var matchedEvents []InputLogEvent
		for _, e := range events {
			if f.FilterPattern == "" || strings.Contains(e.Message, f.FilterPattern) {
				matchedEvents = append(matchedEvents, e)
			}
		}
		if len(matchedEvents) == 0 {
			continue
		}
		if strings.HasPrefix(f.DestinationARN, "arn:aws:lambda:") {
			go p.invokeLambdaSubscription(f, logGroupName, streamName, matchedEvents)
		}
	}
}

func (p *Provider) invokeLambdaSubscription(f SubscriptionFilter, logGroupName, streamName string, events []InputLogEvent) {
	logEventsPayload := make([]map[string]any, 0, len(events))
	for _, e := range events {
		logEventsPayload = append(logEventsPayload, map[string]any{
			"id":        fmt.Sprintf("%d", e.Timestamp),
			"timestamp": e.Timestamp,
			"message":   e.Message,
		})
	}
	payload := map[string]any{
		"messageType":         "DATA_MESSAGE",
		"owner":               defaultAccountID,
		"logGroup":            logGroupName,
		"logStream":           streamName,
		"subscriptionFilters": []string{f.FilterName},
		"logEvents":           logEventsPayload,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return
	}

	// Gzip + base64 encode per AWS standard.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(payloadJSON)
	_ = gz.Close()
	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	lambdaPayload, _ := json.Marshal(map[string]any{
		"awslogs": map[string]any{
			"data": encoded,
		},
	})

	// Extract function name from ARN: arn:aws:lambda:region:account:function:NAME
	parts := strings.Split(f.DestinationARN, ":")
	if len(parts) < 7 {
		return
	}
	funcName := parts[len(parts)-1]
	url := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/%s/invocations", p.serverPort, funcName)

	req, err := http.NewRequest("POST", url, bytes.NewReader(lambdaPayload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	http.DefaultClient.Do(req) //nolint:errcheck
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func eventsToMap(events []LogEvent) []map[string]any {
	list := make([]map[string]any, 0, len(events))
	for _, e := range events {
		list = append(list, map[string]any{
			"timestamp":     e.Timestamp,
			"message":       e.Message,
			"ingestionTime": e.IngestionTime,
		})
	}
	return list
}

func cwlError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{"__type": code, "message": message})
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.1", Body: body}, nil
}
