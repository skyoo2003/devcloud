// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatch/provider_test.go
package cloudwatch

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func handle(t *testing.T, p *Provider, formBody string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(formBody))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestPutAndListMetrics(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p, "Action=PutMetricData&Namespace=MyApp&MetricData.member.1.MetricName=Requests&MetricData.member.1.Value=42&MetricData.member.1.Unit=Count")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "PutMetricDataResponse")

	handle(t, p, "Action=PutMetricData&Namespace=NS1&MetricData.member.1.MetricName=M1&MetricData.member.1.Value=1")
	handle(t, p, "Action=PutMetricData&Namespace=NS1&MetricData.member.1.MetricName=M2&MetricData.member.1.Value=2")

	listResp := handle(t, p, "Action=ListMetrics&Namespace=NS1")
	assert.Equal(t, 200, listResp.StatusCode)
	body := string(listResp.Body)
	assert.Contains(t, body, "M1")
	assert.Contains(t, body, "M2")
	assert.Contains(t, body, "ListMetricsResponse")
}

func TestPutMetricAlarm(t *testing.T) {
	p := newTestProvider(t)
	resp := handle(t, p,
		"Action=PutMetricAlarm&AlarmName=HighCPU&Namespace=AWS/EC2&MetricName=CPUUtilization"+
			"&Statistic=Average&Period=60&EvaluationPeriods=1&Threshold=80&ComparisonOperator=GreaterThanThreshold")
	assert.Equal(t, 200, resp.StatusCode)

	descResp := handle(t, p, "Action=DescribeAlarms")
	assert.Equal(t, 200, descResp.StatusCode)
	body := string(descResp.Body)
	assert.Contains(t, body, "HighCPU")
	assert.Contains(t, body, "INSUFFICIENT_DATA")
}

func TestSetAlarmState(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p,
		"Action=PutMetricAlarm&AlarmName=StateAlarm&Namespace=NS&MetricName=M"+
			"&Statistic=Average&Period=60&EvaluationPeriods=1&Threshold=5&ComparisonOperator=GreaterThanThreshold")

	resp := handle(t, p, "Action=SetAlarmState&AlarmName=StateAlarm&StateValue=ALARM&StateReason=test")
	assert.Equal(t, 200, resp.StatusCode)

	descResp := handle(t, p, "Action=DescribeAlarms&AlarmNames.member.1=StateAlarm")
	assert.Contains(t, string(descResp.Body), "ALARM")
}

func TestCW_DeleteAlarms(t *testing.T) {
	p := newTestProvider(t)
	handle(t, p,
		"Action=PutMetricAlarm&AlarmName=DelAlarm&Namespace=NS&MetricName=M"+
			"&Statistic=Average&Period=60&EvaluationPeriods=1&Threshold=5&ComparisonOperator=GreaterThanThreshold")

	resp := handle(t, p, "Action=DeleteAlarms&AlarmNames.member.1=DelAlarm")
	assert.Equal(t, 200, resp.StatusCode)

	descResp := handle(t, p, "Action=DescribeAlarms")
	assert.NotContains(t, string(descResp.Body), "DelAlarm")
}

func TestDashboardLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := handle(t, p, `Action=PutDashboard&DashboardName=my-dash&DashboardBody={"widgets":[]}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get
	getResp := handle(t, p, "Action=GetDashboard&DashboardName=my-dash")
	assert.Equal(t, 200, getResp.StatusCode)
	assert.Contains(t, string(getResp.Body), "my-dash")

	// List
	listResp := handle(t, p, "Action=ListDashboards")
	assert.Equal(t, 200, listResp.StatusCode)
	assert.Contains(t, string(listResp.Body), "my-dash")

	// Delete
	delResp := handle(t, p, "Action=DeleteDashboards&DashboardNames.member.1=my-dash")
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	listResp2 := handle(t, p, "Action=ListDashboards")
	assert.NotContains(t, string(listResp2.Body), "my-dash")
}

func TestCompositeAlarm(t *testing.T) {
	p := newTestProvider(t)

	// Create a metric alarm first
	handle(t, p,
		"Action=PutMetricAlarm&AlarmName=child-alarm&Namespace=AWS/EC2&MetricName=CPU"+
			"&Statistic=Average&Period=60&EvaluationPeriods=1&Threshold=80&ComparisonOperator=GreaterThanThreshold")

	// Put composite alarm
	resp := handle(t, p, "Action=PutCompositeAlarm&AlarmName=comp-alarm&AlarmRule=ALARM(child-alarm)&ActionsEnabled=true")
	assert.Equal(t, 200, resp.StatusCode)

	// Describe with AlarmTypes filter - should include composite alarms but not MetricAlarms list
	descResp := handle(t, p, "Action=DescribeAlarms&AlarmTypes.member.1=CompositeAlarm")
	assert.Equal(t, 200, descResp.StatusCode)
	body := string(descResp.Body)
	assert.Contains(t, body, "comp-alarm")
	// MetricAlarms list should be empty (no <member> inside MetricAlarms)
	assert.Contains(t, body, "<MetricAlarms></MetricAlarms>")
}

func TestAlarmHistory(t *testing.T) {
	p := newTestProvider(t)

	handle(t, p,
		"Action=PutMetricAlarm&AlarmName=hist-alarm&Namespace=N&MetricName=M"+
			"&Statistic=Sum&Period=60&EvaluationPeriods=1&Threshold=1&ComparisonOperator=GreaterThanThreshold")

	handle(t, p, "Action=SetAlarmState&AlarmName=hist-alarm&StateValue=ALARM&StateReason=test")

	histResp := handle(t, p, "Action=DescribeAlarmHistory&AlarmName=hist-alarm")
	assert.Equal(t, 200, histResp.StatusCode)
	assert.Contains(t, string(histResp.Body), "AlarmHistoryItems")
}

func TestDescribeAlarmsForMetric(t *testing.T) {
	p := newTestProvider(t)

	handle(t, p,
		"Action=PutMetricAlarm&AlarmName=m-alarm&Namespace=MyApp&MetricName=SpecialMetric"+
			"&Statistic=Sum&Period=60&EvaluationPeriods=1&Threshold=1&ComparisonOperator=GreaterThanThreshold")

	resp := handle(t, p, "Action=DescribeAlarmsForMetric&MetricName=SpecialMetric&Namespace=MyApp")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "m-alarm")
}

func TestAnomalyDetector(t *testing.T) {
	p := newTestProvider(t)

	// Put
	resp := handle(t, p, "Action=PutAnomalyDetector&Namespace=MyApp&MetricName=Req&Stat=Average")
	assert.Equal(t, 200, resp.StatusCode)

	// Describe
	descResp := handle(t, p, "Action=DescribeAnomalyDetectors&Namespace=MyApp")
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), "MyApp")

	// Delete
	delResp := handle(t, p, "Action=DeleteAnomalyDetector&Namespace=MyApp&MetricName=Req&Stat=Average")
	assert.Equal(t, 200, delResp.StatusCode)

	// Verify gone
	descResp2 := handle(t, p, "Action=DescribeAnomalyDetectors&Namespace=MyApp")
	assert.NotContains(t, string(descResp2.Body), "Req")
}
