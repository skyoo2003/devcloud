// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesisanalyticsv2/provider_test.go
package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
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

func TestCreateAndDescribeApplication(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"my-app","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"arn:aws:iam::000000000000:role/test"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	detail := m["ApplicationDetail"].(map[string]any)
	assert.Equal(t, "my-app", detail["ApplicationName"])
	assert.Contains(t, detail["ApplicationARN"], "arn:aws:kinesisanalytics:")
	assert.Equal(t, "READY", detail["ApplicationStatus"])
	assert.Equal(t, "FLINK-1_18", detail["RuntimeEnvironment"])

	desc := callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"my-app"}`)
	assert.Equal(t, 200, desc.StatusCode)
	dm := parseJSON(t, desc)
	ddetail := dm["ApplicationDetail"].(map[string]any)
	assert.Equal(t, "my-app", ddetail["ApplicationName"])
	assert.Equal(t, "READY", ddetail["ApplicationStatus"])
}

func TestListApplications(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"app-1","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)
	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"app-2","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)

	resp := callJSON(t, p, "KinesisAnalytics_20180523.ListApplications", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	summaries := m["ApplicationSummaries"].([]any)
	assert.Len(t, summaries, 2)
}

func TestDeleteApplication(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"del-me","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)

	resp := callJSON(t, p, "KinesisAnalytics_20180523.DeleteApplication",
		`{"ApplicationName":"del-me"}`)
	assert.Equal(t, 200, resp.StatusCode)

	desc := callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"del-me"}`)
	assert.Equal(t, 400, desc.StatusCode)
}

func TestStartStopApplication(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"runner","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)

	startResp := callJSON(t, p, "KinesisAnalytics_20180523.StartApplication",
		`{"ApplicationName":"runner","RunConfiguration":{}}`)
	assert.Equal(t, 200, startResp.StatusCode)

	desc := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"runner"}`))
	assert.Equal(t, "RUNNING", desc["ApplicationDetail"].(map[string]any)["ApplicationStatus"])

	stopResp := callJSON(t, p, "KinesisAnalytics_20180523.StopApplication",
		`{"ApplicationName":"runner"}`)
	assert.Equal(t, 200, stopResp.StatusCode)

	desc2 := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"runner"}`))
	assert.Equal(t, "READY", desc2["ApplicationDetail"].(map[string]any)["ApplicationStatus"])
}

func TestCreateAndListSnapshots(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"snap-app","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)

	cr1 := callJSON(t, p, "KinesisAnalytics_20180523.CreateApplicationSnapshot",
		`{"ApplicationName":"snap-app","SnapshotName":"snap-1"}`)
	assert.Equal(t, 200, cr1.StatusCode)

	cr2 := callJSON(t, p, "KinesisAnalytics_20180523.CreateApplicationSnapshot",
		`{"ApplicationName":"snap-app","SnapshotName":"snap-2"}`)
	assert.Equal(t, 200, cr2.StatusCode)

	list := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.ListApplicationSnapshots",
		`{"ApplicationName":"snap-app"}`))
	summaries := list["SnapshotSummaries"].([]any)
	assert.Len(t, summaries, 2)

	delResp := callJSON(t, p, "KinesisAnalytics_20180523.DeleteApplicationSnapshot",
		`{"ApplicationName":"snap-app","SnapshotName":"snap-1"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	list2 := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.ListApplicationSnapshots",
		`{"ApplicationName":"snap-app"}`))
	summaries2 := list2["SnapshotSummaries"].([]any)
	assert.Len(t, summaries2, 1)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"tagged-app","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role","Tags":[{"Key":"env","Value":"prod"}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	arn := m["ApplicationDetail"].(map[string]any)["ApplicationARN"].(string)

	listResp := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.ListTagsForResource",
		`{"ResourceARN":"`+arn+`"}`))
	tags := listResp["Tags"].([]any)
	assert.Len(t, tags, 1)

	callJSON(t, p, "KinesisAnalytics_20180523.TagResource",
		`{"ResourceARN":"`+arn+`","Tags":[{"Key":"team","Value":"data"}]}`)
	listResp2 := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.ListTagsForResource",
		`{"ResourceARN":"`+arn+`"}`))
	tags2 := listResp2["Tags"].([]any)
	assert.Len(t, tags2, 2)

	callJSON(t, p, "KinesisAnalytics_20180523.UntagResource",
		`{"ResourceARN":"`+arn+`","TagKeys":["env","team"]}`)
	listResp3 := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.ListTagsForResource",
		`{"ResourceARN":"`+arn+`"}`))
	assert.Empty(t, listResp3["Tags"])
}

func TestUpdateApplication(t *testing.T) {
	p := newTestProvider(t)

	callJSON(t, p, "KinesisAnalytics_20180523.CreateApplication",
		`{"ApplicationName":"update-app","RuntimeEnvironment":"FLINK-1_18","ServiceExecutionRole":"role"}`)

	desc := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"update-app"}`))
	versionBefore := desc["ApplicationDetail"].(map[string]any)["ApplicationVersionId"].(float64)

	updateResp := callJSON(t, p, "KinesisAnalytics_20180523.UpdateApplication",
		`{"ApplicationName":"update-app","ApplicationConfigurationUpdate":{"FlinkApplicationConfigurationUpdate":{}}}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	desc2 := parseJSON(t, callJSON(t, p, "KinesisAnalytics_20180523.DescribeApplication",
		`{"ApplicationName":"update-app"}`))
	versionAfter := desc2["ApplicationDetail"].(map[string]any)["ApplicationVersionId"].(float64)
	assert.Greater(t, versionAfter, versionBefore)
}
