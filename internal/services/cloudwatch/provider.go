// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatch/provider.go
package cloudwatch

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

type Provider struct {
	store *CWStore
}

func (p *Provider) ServiceID() string             { return "cloudwatch" }
func (p *Provider) ServiceName() string           { return "monitoring" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewCWStore(filepath.Join(dataDir, "cloudwatch"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	jm := isJSONMode(req)
	if err := parseBodyToForm(req); err != nil {
		return cwError(jm, "InvalidParameterValue", "failed to parse body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = req.FormValue("Action")
	}
	switch action {
	case "PutMetricData":
		return p.putMetricData(jm, req)
	case "GetMetricData":
		return p.getMetricData(jm, req)
	case "GetMetricStatistics":
		return p.getMetricStatistics(jm, req)
	case "ListMetrics":
		return p.listMetrics(jm, req)
	case "PutMetricAlarm":
		return p.putMetricAlarm(jm, req)
	case "DescribeAlarms":
		return p.describeAlarms(jm, req)
	case "DeleteAlarms":
		return p.deleteAlarms(jm, req)
	case "SetAlarmState":
		return p.setAlarmState(jm, req)
	case "DisableAlarmActions":
		return p.disableAlarmActions(jm, req)
	case "EnableAlarmActions":
		return p.enableAlarmActions(jm, req)
	// Dashboards
	case "PutDashboard":
		return p.putDashboard(jm, req)
	case "GetDashboard":
		return p.getDashboard(jm, req)
	case "DeleteDashboards":
		return p.deleteDashboards(jm, req)
	case "ListDashboards":
		return p.listDashboards(jm, req)
	// Composite alarms
	case "PutCompositeAlarm":
		return p.putCompositeAlarm(jm, req)
	// Alarm history
	case "DescribeAlarmHistory":
		return p.describeAlarmHistory(jm, req)
	case "DescribeAlarmsForMetric":
		return p.describeAlarmsForMetric(jm, req)
	// Anomaly detectors
	case "PutAnomalyDetector":
		return p.putAnomalyDetector(jm, req)
	case "DeleteAnomalyDetector":
		return p.deleteAnomalyDetector(jm, req)
	case "DescribeAnomalyDetectors":
		return p.describeAnomalyDetectors(jm, req)
	default:
		return cwError(jm, "InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	metrics, err := p.store.ListMetrics(defaultAccountID, "", "")
	if err != nil {
		return nil, err
	}
	resources := make([]plugin.Resource, 0, len(metrics))
	for _, m := range metrics {
		id := m.Namespace + "/" + m.MetricName
		resources = append(resources, plugin.Resource{Type: "metric", ID: id, Name: m.MetricName})
	}
	return resources, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// putMetricData parses MetricData.member.N.* form fields.
func (p *Provider) putMetricData(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	if namespace == "" {
		return cwError(jm, "MissingParameter", "Namespace is required", http.StatusBadRequest), nil
	}
	var data []MetricDatum
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MetricData.member.%d.", i)
		name := req.FormValue(prefix + "MetricName")
		if name == "" {
			break
		}
		val, _ := strconv.ParseFloat(req.FormValue(prefix+"Value"), 64)
		unit := req.FormValue(prefix + "Unit")
		if unit == "" {
			unit = "None"
		}
		data = append(data, MetricDatum{
			Namespace:  namespace,
			MetricName: name,
			Value:      val,
			Unit:       unit,
		})
	}
	if err := p.store.PutMetricData(defaultAccountID, data); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "PutMetricDataResponse", nil)
}

func (p *Provider) getMetricData(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	startTimeStr := req.FormValue("StartTime")
	endTimeStr := req.FormValue("EndTime")
	periodStr := req.FormValue("Period")

	var startTime, endTime int64
	var period int
	fmt.Sscanf(startTimeStr, "%d", &startTime)
	fmt.Sscanf(endTimeStr, "%d", &endTime)
	fmt.Sscanf(periodStr, "%d", &period)
	if period == 0 {
		period = 60
	}

	points, err := p.store.GetMetricData(defaultAccountID, namespace, metricName, startTime, endTime, period)
	if err != nil {
		return nil, err
	}

	dps := make([]map[string]any, 0, len(points))
	for _, pt := range points {
		dps = append(dps, map[string]any{
			"Timestamp":   pt.Timestamp,
			"SampleCount": 1.0,
			"Average":     pt.Value,
			"Unit":        pt.Unit,
		})
	}
	return cwResp(jm, http.StatusOK, "GetMetricDataResponse", map[string]any{
		"Datapoints": dps,
	})
}

func (p *Provider) getMetricStatistics(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	startTimeStr := req.FormValue("StartTime")
	endTimeStr := req.FormValue("EndTime")
	periodStr := req.FormValue("Period")

	var startTime, endTime int64
	var period int
	fmt.Sscanf(startTimeStr, "%d", &startTime)
	fmt.Sscanf(endTimeStr, "%d", &endTime)
	fmt.Sscanf(periodStr, "%d", &period)
	if period == 0 {
		period = 60
	}

	points, err := p.store.GetMetricData(defaultAccountID, namespace, metricName, startTime, endTime, period)
	if err != nil {
		return nil, err
	}

	dps := make([]map[string]any, 0, len(points))
	for _, pt := range points {
		dps = append(dps, map[string]any{
			"Timestamp":   pt.Timestamp,
			"SampleCount": 1.0,
			"Average":     pt.Value,
			"Sum":         pt.Value,
			"Minimum":     pt.Value,
			"Maximum":     pt.Value,
			"Unit":        pt.Unit,
		})
	}
	return cwResp(jm, http.StatusOK, "GetMetricStatisticsResponse", map[string]any{
		"Datapoints": dps,
		"Label":      metricName,
	})
}

func (p *Provider) listMetrics(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	metrics, err := p.store.ListMetrics(defaultAccountID, namespace, metricName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(metrics))
	for _, m := range metrics {
		list = append(list, map[string]any{
			"Namespace":  m.Namespace,
			"MetricName": m.MetricName,
		})
	}
	return cwResp(jm, http.StatusOK, "ListMetricsResponse", map[string]any{
		"Metrics": list,
	})
}

func (p *Provider) putMetricAlarm(jm bool, req *http.Request) (*plugin.Response, error) {
	alarmName := req.FormValue("AlarmName")
	if alarmName == "" {
		return cwError(jm, "MissingParameter", "AlarmName is required", http.StatusBadRequest), nil
	}
	threshold, _ := strconv.ParseFloat(req.FormValue("Threshold"), 64)
	period, _ := strconv.Atoi(req.FormValue("Period"))
	if period == 0 {
		period = 60
	}
	evalPeriods, _ := strconv.Atoi(req.FormValue("EvaluationPeriods"))
	if evalPeriods == 0 {
		evalPeriods = 1
	}
	actionsEnabled := req.FormValue("ActionsEnabled") != "false"
	alarm := Alarm{
		AlarmName:      alarmName,
		AccountID:      defaultAccountID,
		Namespace:      req.FormValue("Namespace"),
		MetricName:     req.FormValue("MetricName"),
		Statistic:      req.FormValue("Statistic"),
		Period:         period,
		EvalPeriods:    evalPeriods,
		Threshold:      threshold,
		Comparison:     req.FormValue("ComparisonOperator"),
		ActionsEnabled: actionsEnabled,
	}
	if err := p.store.PutMetricAlarm(alarm); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "PutMetricAlarmResponse", nil)
}

func (p *Provider) describeAlarms(jm bool, req *http.Request) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := req.FormValue(fmt.Sprintf("AlarmNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}

	// Collect AlarmTypes filter
	alarmTypes := map[string]bool{}
	for i := 1; ; i++ {
		t := req.FormValue(fmt.Sprintf("AlarmTypes.member.%d", i))
		if t == "" {
			break
		}
		alarmTypes[t] = true
	}
	includeMetric := len(alarmTypes) == 0 || alarmTypes["MetricAlarm"]
	includeComposite := len(alarmTypes) == 0 || alarmTypes["CompositeAlarm"]

	metricList := []map[string]any{}
	if includeMetric {
		alarms, err := p.store.DescribeAlarms(defaultAccountID, names)
		if err != nil {
			return nil, err
		}
		for _, a := range alarms {
			metricList = append(metricList, map[string]any{
				"AlarmName":          a.AlarmName,
				"Namespace":          a.Namespace,
				"MetricName":         a.MetricName,
				"Statistic":          a.Statistic,
				"Threshold":          a.Threshold,
				"StateValue":         a.State,
				"ComparisonOperator": a.Comparison,
				"Period":             a.Period,
				"EvaluationPeriods":  a.EvalPeriods,
				"ActionsEnabled":     a.ActionsEnabled,
			})
		}
	}

	compositeList := []map[string]any{}
	if includeComposite {
		compAlarms, err := p.store.DescribeCompositeAlarms(defaultAccountID, names)
		if err != nil {
			return nil, err
		}
		for _, a := range compAlarms {
			compositeList = append(compositeList, map[string]any{
				"AlarmName":      a.AlarmName,
				"AlarmRule":      a.AlarmRule,
				"StateValue":     a.State,
				"StateReason":    a.StateReason,
				"ActionsEnabled": a.ActionsEnabled,
			})
		}
	}

	return cwResp(jm, http.StatusOK, "DescribeAlarmsResponse", map[string]any{
		"MetricAlarms":    metricList,
		"CompositeAlarms": compositeList,
	})
}

func (p *Provider) deleteAlarms(jm bool, req *http.Request) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := req.FormValue(fmt.Sprintf("AlarmNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	if err := p.store.DeleteAlarms(defaultAccountID, names); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "DeleteAlarmsResponse", nil)
}

func (p *Provider) setAlarmState(jm bool, req *http.Request) (*plugin.Response, error) {
	alarmName := req.FormValue("AlarmName")
	state := req.FormValue("StateValue")
	reason := req.FormValue("StateReason")
	if alarmName == "" || state == "" {
		return cwError(jm, "MissingParameter", "AlarmName and StateValue are required", http.StatusBadRequest), nil
	}
	if err := p.store.SetAlarmState(defaultAccountID, alarmName, state, reason); err != nil {
		return cwError(jm, "ResourceNotFound", "alarm not found", http.StatusBadRequest), nil
	}
	summary := fmt.Sprintf("Alarm updated from unknown to %s", state)
	_ = p.store.AddAlarmHistory(defaultAccountID, alarmName, "StateUpdate", summary, "")
	return cwResp(jm, http.StatusOK, "SetAlarmStateResponse", nil)
}

func (p *Provider) disableAlarmActions(jm bool, req *http.Request) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := req.FormValue(fmt.Sprintf("AlarmNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	for _, name := range names {
		_ = p.store.SetAlarmActionsEnabled(defaultAccountID, name, false)
	}
	return cwResp(jm, http.StatusOK, "DisableAlarmActionsResponse", nil)
}

func (p *Provider) enableAlarmActions(jm bool, req *http.Request) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := req.FormValue(fmt.Sprintf("AlarmNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	for _, name := range names {
		_ = p.store.SetAlarmActionsEnabled(defaultAccountID, name, true)
	}
	return cwResp(jm, http.StatusOK, "EnableAlarmActionsResponse", nil)
}

// --- Dashboard handlers ---

func (p *Provider) putDashboard(jm bool, req *http.Request) (*plugin.Response, error) {
	name := req.FormValue("DashboardName")
	body := req.FormValue("DashboardBody")
	if name == "" {
		return cwError(jm, "MissingParameter", "DashboardName is required", http.StatusBadRequest), nil
	}
	if err := p.store.PutDashboard(defaultAccountID, name, body); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "PutDashboardResponse", nil)
}

func (p *Provider) getDashboard(jm bool, req *http.Request) (*plugin.Response, error) {
	name := req.FormValue("DashboardName")
	if name == "" {
		return cwError(jm, "MissingParameter", "DashboardName is required", http.StatusBadRequest), nil
	}
	d, err := p.store.GetDashboard(defaultAccountID, name)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return cwError(jm, "ResourceNotFound", "dashboard not found", http.StatusBadRequest), nil
	}
	return cwResp(jm, http.StatusOK, "GetDashboardResponse", map[string]any{
		"DashboardName": d.DashboardName,
		"DashboardBody": d.DashboardBody,
		"DashboardArn":  fmt.Sprintf("arn:aws:cloudwatch::123456789012:dashboard/%s", d.DashboardName),
	})
}

func (p *Provider) deleteDashboards(jm bool, req *http.Request) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		n := req.FormValue(fmt.Sprintf("DashboardNames.member.%d", i))
		if n == "" {
			break
		}
		names = append(names, n)
	}
	if err := p.store.DeleteDashboards(defaultAccountID, names); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "DeleteDashboardsResponse", nil)
}

func (p *Provider) listDashboards(jm bool, req *http.Request) (*plugin.Response, error) {
	prefix := req.FormValue("DashboardNamePrefix")
	dashboards, err := p.store.ListDashboards(defaultAccountID, prefix)
	if err != nil {
		return nil, err
	}
	entries := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		entries = append(entries, map[string]any{
			"DashboardName": d.DashboardName,
			"DashboardArn":  fmt.Sprintf("arn:aws:cloudwatch::123456789012:dashboard/%s", d.DashboardName),
			"LastModified":  d.CreatedAt,
			"Size":          len(d.DashboardBody),
		})
	}
	return cwResp(jm, http.StatusOK, "ListDashboardsResponse", map[string]any{
		"DashboardEntries": entries,
	})
}

// --- Composite alarm handlers ---

func (p *Provider) putCompositeAlarm(jm bool, req *http.Request) (*plugin.Response, error) {
	alarmName := req.FormValue("AlarmName")
	alarmRule := req.FormValue("AlarmRule")
	if alarmName == "" || alarmRule == "" {
		return cwError(jm, "MissingParameter", "AlarmName and AlarmRule are required", http.StatusBadRequest), nil
	}
	actionsEnabled := req.FormValue("ActionsEnabled") != "false"
	ca := CompositeAlarm{
		AlarmName:      alarmName,
		AlarmRule:      alarmRule,
		ActionsEnabled: actionsEnabled,
	}
	if err := p.store.PutCompositeAlarm(defaultAccountID, ca); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "PutCompositeAlarmResponse", nil)
}

// --- Alarm history handlers ---

func (p *Provider) describeAlarmHistory(jm bool, req *http.Request) (*plugin.Response, error) {
	alarmName := req.FormValue("AlarmName")
	historyItemType := req.FormValue("HistoryItemType")
	startDate := req.FormValue("StartDate")
	endDate := req.FormValue("EndDate")
	items, err := p.store.DescribeAlarmHistory(defaultAccountID, alarmName, historyItemType, startDate, endDate)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(items))
	for _, item := range items {
		list = append(list, map[string]any{
			"AlarmName":       item.AlarmName,
			"Timestamp":       item.Timestamp,
			"HistoryItemType": item.HistoryItemType,
			"HistorySummary":  item.Summary,
			"HistoryData":     item.HistoryData,
		})
	}
	return cwResp(jm, http.StatusOK, "DescribeAlarmHistoryResponse", map[string]any{
		"AlarmHistoryItems": list,
	})
}

func (p *Provider) describeAlarmsForMetric(jm bool, req *http.Request) (*plugin.Response, error) {
	metricName := req.FormValue("MetricName")
	namespace := req.FormValue("Namespace")
	if metricName == "" || namespace == "" {
		return cwError(jm, "MissingParameter", "MetricName and Namespace are required", http.StatusBadRequest), nil
	}
	alarms, err := p.store.DescribeAlarmsForMetric(defaultAccountID, metricName, namespace)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(alarms))
	for _, a := range alarms {
		list = append(list, map[string]any{
			"AlarmName":          a.AlarmName,
			"Namespace":          a.Namespace,
			"MetricName":         a.MetricName,
			"Statistic":          a.Statistic,
			"Threshold":          a.Threshold,
			"StateValue":         a.State,
			"ComparisonOperator": a.Comparison,
			"Period":             a.Period,
			"EvaluationPeriods":  a.EvalPeriods,
			"ActionsEnabled":     a.ActionsEnabled,
		})
	}
	return cwResp(jm, http.StatusOK, "DescribeAlarmsForMetricResponse", map[string]any{
		"MetricAlarms": list,
	})
}

// --- Anomaly detector handlers ---

func (p *Provider) putAnomalyDetector(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	stat := req.FormValue("Stat")
	if namespace == "" || metricName == "" || stat == "" {
		return cwError(jm, "MissingParameter", "Namespace, MetricName, and Stat are required", http.StatusBadRequest), nil
	}
	dimensions := req.FormValue("Dimensions")
	configuration := req.FormValue("Configuration")
	d := AnomalyDetector{
		Namespace:     namespace,
		MetricName:    metricName,
		Stat:          stat,
		Dimensions:    dimensions,
		Configuration: configuration,
	}
	if err := p.store.PutAnomalyDetector(defaultAccountID, d); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "PutAnomalyDetectorResponse", nil)
}

func (p *Provider) deleteAnomalyDetector(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	stat := req.FormValue("Stat")
	dimensions := req.FormValue("Dimensions")
	if err := p.store.DeleteAnomalyDetector(defaultAccountID, namespace, metricName, stat, dimensions); err != nil {
		return nil, err
	}
	return cwResp(jm, http.StatusOK, "DeleteAnomalyDetectorResponse", nil)
}

func (p *Provider) describeAnomalyDetectors(jm bool, req *http.Request) (*plugin.Response, error) {
	namespace := req.FormValue("Namespace")
	metricName := req.FormValue("MetricName")
	detectors, err := p.store.DescribeAnomalyDetectors(defaultAccountID, namespace, metricName)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(detectors))
	for _, d := range detectors {
		list = append(list, map[string]any{
			"Namespace":     d.Namespace,
			"MetricName":    d.MetricName,
			"Stat":          d.Stat,
			"Configuration": d.Configuration,
		})
	}
	return cwResp(jm, http.StatusOK, "DescribeAnomalyDetectorsResponse", map[string]any{
		"AnomalyDetectors": list,
	})
}

// --- Response helpers ---

func isJSONMode(req *http.Request) bool {
	ct := req.Header.Get("Content-Type")
	return strings.Contains(ct, "json")
}

func cwResp(jsonMode bool, status int, xmlRoot string, data map[string]any) (*plugin.Response, error) {
	if data == nil {
		data = map[string]any{}
	}
	if jsonMode {
		body, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.0", Body: body}, nil
	}
	// XML mode: wrap in standard CloudWatch XML envelope
	return xmlMapResp(status, xmlRoot, data)
}

func cwError(jsonMode bool, code, message string, status int) *plugin.Response {
	if jsonMode {
		body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
		return &plugin.Response{StatusCode: status, ContentType: "application/x-amz-json-1.0", Body: body}
	}
	type errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	e := errResp{}
	e.Error.Code = code
	e.Error.Message = message
	body, _ := xml.Marshal(e)
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: body}
}

// xmlMapResp produces a minimal XML response for query-protocol clients.
func xmlMapResp(status int, root string, data map[string]any) (*plugin.Response, error) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("<%s>", root))
	resultKey := strings.TrimSuffix(root, "Response") + "Result"
	buf.WriteString(fmt.Sprintf("<%s>", resultKey))
	writeXMLMap(&buf, data)
	buf.WriteString(fmt.Sprintf("</%s>", resultKey))
	buf.WriteString(fmt.Sprintf("</%s>", root))
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: buf.Bytes()}, nil
}

func writeXMLMap(buf *bytes.Buffer, data map[string]any) {
	for k, v := range data {
		switch val := v.(type) {
		case []map[string]any:
			buf.WriteString(fmt.Sprintf("<%s>", k))
			for _, item := range val {
				buf.WriteString("<member>")
				writeXMLMap(buf, item)
				buf.WriteString("</member>")
			}
			buf.WriteString(fmt.Sprintf("</%s>", k))
		case []any:
			buf.WriteString(fmt.Sprintf("<%s>", k))
			for _, item := range val {
				if m, ok := item.(map[string]any); ok {
					buf.WriteString("<member>")
					writeXMLMap(buf, m)
					buf.WriteString("</member>")
				}
			}
			buf.WriteString(fmt.Sprintf("</%s>", k))
		case string:
			buf.WriteString(fmt.Sprintf("<%s>%s</%s>", k, val, k))
		case float64:
			buf.WriteString(fmt.Sprintf("<%s>%s</%s>", k, strconv.FormatFloat(val, 'f', -1, 64), k))
		case int:
			buf.WriteString(fmt.Sprintf("<%s>%d</%s>", k, val, k))
		case bool:
			buf.WriteString(fmt.Sprintf("<%s>%t</%s>", k, val, k))
		}
	}
}

// --- Body parsing ---

// parseBodyToForm reads the request body and populates req.Form.
// If the Content-Type is JSON (awsQuery-compatible mode used by newer boto3),
// the JSON body is flattened into query-protocol form values so that existing
// FormValue() calls continue to work.
func parseBodyToForm(req *http.Request) error {
	ct := req.Header.Get("Content-Type")
	if !strings.Contains(ct, "json") {
		return req.ParseForm()
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))

	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return err
	}

	req.Form = make(url.Values)
	flattenJSON("", data, req.Form)
	return nil
}

// flattenJSON converts a nested JSON structure into AWS query-protocol
// form values.  e.g. {"MetricData": [{"MetricName": "cpu"}]}
// becomes MetricData.member.1.MetricName=cpu
func flattenJSON(prefix string, data any, values url.Values) {
	switch v := data.(type) {
	case map[string]any:
		for key, val := range v {
			p := key
			if prefix != "" {
				p = prefix + "." + key
			}
			flattenJSON(p, val, values)
		}
	case []any:
		for i, item := range v {
			flattenJSON(fmt.Sprintf("%s.member.%d", prefix, i+1), item, values)
		}
	case string:
		values.Set(prefix, v)
	case float64:
		values.Set(prefix, strconv.FormatFloat(v, 'f', -1, 64))
	case bool:
		values.Set(prefix, strconv.FormatBool(v))
	}
}
