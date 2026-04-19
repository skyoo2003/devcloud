// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatchlogs/provider_test.go
package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func call(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Logs."+target)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestCreateLogGroupAndStream(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "CreateLogGroup", `{"logGroupName":"/my/app"}`)
	assert.Equal(t, 200, resp.StatusCode)

	call(t, p, "CreateLogStream", `{"logGroupName":"/my/app","logStreamName":"stream-1"}`)

	listResp := call(t, p, "DescribeLogGroups", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	m := parseJSON(t, listResp)
	groups := m["logGroups"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, "/my/app", groups[0].(map[string]any)["logGroupName"])

	streamResp := call(t, p, "DescribeLogStreams", `{"logGroupName":"/my/app"}`)
	sm := parseJSON(t, streamResp)
	streams := sm["logStreams"].([]any)
	require.Len(t, streams, 1)
	assert.Equal(t, "stream-1", streams[0].(map[string]any)["logStreamName"])
}

func TestPutAndGetLogEvents(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"/events/group"}`)
	call(t, p, "CreateLogStream", `{"logGroupName":"/events/group","logStreamName":"s1"}`)

	now := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"logGroupName":"/events/group","logStreamName":"s1","logEvents":[{"timestamp":%d,"message":"hello world"},{"timestamp":%d,"message":"second event"}]}`, now, now+1000)
	resp := call(t, p, "PutLogEvents", body)
	assert.Equal(t, 200, resp.StatusCode)

	getResp := call(t, p, "GetLogEvents", `{"logGroupName":"/events/group","logStreamName":"s1"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	m := parseJSON(t, getResp)
	events := m["events"].([]any)
	assert.Len(t, events, 2)
	assert.Equal(t, "hello world", events[0].(map[string]any)["message"])
}

func TestFilterLogEvents(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"/filter/group"}`)
	call(t, p, "CreateLogStream", `{"logGroupName":"/filter/group","logStreamName":"s1"}`)

	now := time.Now().UnixMilli()
	body := fmt.Sprintf(`{"logGroupName":"/filter/group","logStreamName":"s1","logEvents":[{"timestamp":%d,"message":"ERROR: something failed"},{"timestamp":%d,"message":"INFO: all good"}]}`, now, now+1000)
	call(t, p, "PutLogEvents", body)

	filterResp := call(t, p, "FilterLogEvents", `{"logGroupName":"/filter/group","filterPattern":"ERROR"}`)
	assert.Equal(t, 200, filterResp.StatusCode)
	m := parseJSON(t, filterResp)
	events := m["events"].([]any)
	assert.Len(t, events, 1)
	assert.Contains(t, events[0].(map[string]any)["message"], "ERROR")
}

func TestCWL_DeleteLogGroup(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"/del/group"}`)
	resp := call(t, p, "DeleteLogGroup", `{"logGroupName":"/del/group"}`)
	assert.Equal(t, 200, resp.StatusCode)

	listResp := call(t, p, "DescribeLogGroups", `{}`)
	m := parseJSON(t, listResp)
	assert.Len(t, m["logGroups"], 0)
}

func TestCWL_ListResources(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"/resource/group"}`)
	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	require.Len(t, resources, 1)
	assert.Equal(t, "log-group", resources[0].Type)
}

func TestRetentionPolicy(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"retain-group"}`)
	resp := call(t, p, "PutRetentionPolicy", `{"logGroupName":"retain-group","retentionInDays":7}`)
	assert.Equal(t, 200, resp.StatusCode)

	listResp := call(t, p, "DescribeLogGroups", `{"logGroupNamePrefix":"retain-group"}`)
	m := parseJSON(t, listResp)
	groups := m["logGroups"].([]any)
	require.Len(t, groups, 1)
	assert.Equal(t, float64(7), groups[0].(map[string]any)["retentionInDays"])

	call(t, p, "DeleteRetentionPolicy", `{"logGroupName":"retain-group"}`)
	listResp2 := call(t, p, "DescribeLogGroups", `{"logGroupNamePrefix":"retain-group"}`)
	m2 := parseJSON(t, listResp2)
	groups2 := m2["logGroups"].([]any)
	_, hasRetention := groups2[0].(map[string]any)["retentionInDays"]
	assert.False(t, hasRetention)
}

func TestMetricFilter(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"mf-group"}`)

	body := `{"filterName":"error-filter","logGroupName":"mf-group","filterPattern":"ERROR","metricTransformations":[{"metricName":"ErrorCount","metricNamespace":"MyApp","metricValue":"1"}]}`
	resp := call(t, p, "PutMetricFilter", body)
	assert.Equal(t, 200, resp.StatusCode)

	descResp := call(t, p, "DescribeMetricFilters", `{"logGroupName":"mf-group"}`)
	m := parseJSON(t, descResp)
	filters := m["metricFilters"].([]any)
	require.Len(t, filters, 1)
	assert.Equal(t, "error-filter", filters[0].(map[string]any)["filterName"])

	delResp := call(t, p, "DeleteMetricFilter", `{"filterName":"error-filter","logGroupName":"mf-group"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	descResp2 := call(t, p, "DescribeMetricFilters", `{"logGroupName":"mf-group"}`)
	m2 := parseJSON(t, descResp2)
	assert.Len(t, m2["metricFilters"], 0)
}

func TestSubscriptionFilter(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"sf-group"}`)

	body := `{"filterName":"to-lambda","logGroupName":"sf-group","filterPattern":"","destinationArn":"arn:aws:lambda:us-east-1:000000000000:function:processor"}`
	resp := call(t, p, "PutSubscriptionFilter", body)
	assert.Equal(t, 200, resp.StatusCode)

	descResp := call(t, p, "DescribeSubscriptionFilters", `{"logGroupName":"sf-group"}`)
	m := parseJSON(t, descResp)
	filters := m["subscriptionFilters"].([]any)
	require.Len(t, filters, 1)
	assert.Equal(t, "to-lambda", filters[0].(map[string]any)["filterName"])

	delResp := call(t, p, "DeleteSubscriptionFilter", `{"filterName":"to-lambda","logGroupName":"sf-group"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	descResp2 := call(t, p, "DescribeSubscriptionFilters", `{"logGroupName":"sf-group"}`)
	m2 := parseJSON(t, descResp2)
	assert.Len(t, m2["subscriptionFilters"], 0)
}

func TestLogGroupTags(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateLogGroup", `{"logGroupName":"tagged-group"}`)
	arn := "arn:aws:logs:us-east-1:000000000000:log-group:tagged-group:*"

	tagResp := call(t, p, "TagResource", fmt.Sprintf(`{"resourceArn":%q,"tags":{"env":"test"}}`, arn))
	assert.Equal(t, 200, tagResp.StatusCode)

	listResp := call(t, p, "ListTagsForResource", fmt.Sprintf(`{"resourceArn":%q}`, arn))
	m := parseJSON(t, listResp)
	tags, _ := m["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])

	untagResp := call(t, p, "UntagResource", fmt.Sprintf(`{"resourceArn":%q,"tagKeys":["env"]}`, arn))
	assert.Equal(t, 200, untagResp.StatusCode)

	listResp2 := call(t, p, "ListTagsForResource", fmt.Sprintf(`{"resourceArn":%q}`, arn))
	m2 := parseJSON(t, listResp2)
	tags2, _ := m2["tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
}
