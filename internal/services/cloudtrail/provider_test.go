// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudtrail/provider_test.go
package cloudtrail

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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, action, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CloudTrail_20131101."+action)
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

func TestTrailCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "CreateTrail", `{"Name":"my-trail","S3BucketName":"my-bucket"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "my-trail", m["Name"])
	assert.Contains(t, m["TrailARN"], "arn:aws:cloudtrail:")

	// Get
	get := callJSON(t, p, "GetTrail", `{"Name":"my-trail"}`)
	assert.Equal(t, 200, get.StatusCode)
	gm := parseJSON(t, get)
	trail := gm["Trail"].(map[string]any)
	assert.Equal(t, "my-trail", trail["Name"])
	assert.Equal(t, "my-bucket", trail["S3BucketName"])

	// DescribeTrails
	desc := callJSON(t, p, "DescribeTrails", `{}`)
	assert.Equal(t, 200, desc.StatusCode)
	dm := parseJSON(t, desc)
	list := dm["trailList"].([]any)
	assert.Len(t, list, 1)

	// ListTrails
	lt := callJSON(t, p, "ListTrails", `{}`)
	assert.Equal(t, 200, lt.StatusCode)
	lm := parseJSON(t, lt)
	trails := lm["Trails"].([]any)
	assert.Len(t, trails, 1)

	// Update
	upd := callJSON(t, p, "UpdateTrail", `{"Name":"my-trail","S3BucketName":"new-bucket"}`)
	assert.Equal(t, 200, upd.StatusCode)
	um := parseJSON(t, upd)
	assert.Equal(t, "new-bucket", um["S3BucketName"])

	// Delete
	del := callJSON(t, p, "DeleteTrail", `{"Name":"my-trail"}`)
	assert.Equal(t, 200, del.StatusCode)

	// Get after delete
	gone := callJSON(t, p, "GetTrail", `{"Name":"my-trail"}`)
	assert.Equal(t, 400, gone.StatusCode)
}

func TestStartStopLogging(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "CreateTrail", `{"Name":"log-trail","S3BucketName":"bucket"}`)

	// GetTrailStatus - should be logging by default
	status := callJSON(t, p, "GetTrailStatus", `{"Name":"log-trail"}`)
	assert.Equal(t, 200, status.StatusCode)
	sm := parseJSON(t, status)
	assert.Equal(t, true, sm["IsLogging"])

	// StopLogging
	stop := callJSON(t, p, "StopLogging", `{"Name":"log-trail"}`)
	assert.Equal(t, 200, stop.StatusCode)
	sm2 := parseJSON(t, callJSON(t, p, "GetTrailStatus", `{"Name":"log-trail"}`))
	assert.Equal(t, false, sm2["IsLogging"])

	// StartLogging
	start := callJSON(t, p, "StartLogging", `{"Name":"log-trail"}`)
	assert.Equal(t, 200, start.StatusCode)
	sm3 := parseJSON(t, callJSON(t, p, "GetTrailStatus", `{"Name":"log-trail"}`))
	assert.Equal(t, true, sm3["IsLogging"])
}

func TestEventDataStoreCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "CreateEventDataStore", `{"Name":"my-eds","RetentionPeriod":365}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "my-eds", m["Name"])
	assert.Contains(t, m["EventDataStoreArn"], "arn:aws:cloudtrail:")
	edsARN := m["EventDataStoreArn"].(string)

	// Get
	get := callJSON(t, p, "GetEventDataStore", `{"EventDataStore":"`+edsARN+`"}`)
	assert.Equal(t, 200, get.StatusCode)
	gm := parseJSON(t, get)
	assert.Equal(t, "my-eds", gm["Name"])
	assert.Equal(t, float64(365), gm["RetentionPeriod"])

	// List
	list := callJSON(t, p, "ListEventDataStores", `{}`)
	assert.Equal(t, 200, list.StatusCode)
	lm := parseJSON(t, list)
	stores := lm["EventDataStores"].([]any)
	assert.Len(t, stores, 1)

	// Update
	upd := callJSON(t, p, "UpdateEventDataStore", `{"EventDataStore":"`+edsARN+`","RetentionPeriod":730}`)
	assert.Equal(t, 200, upd.StatusCode)
	um := parseJSON(t, upd)
	assert.Equal(t, float64(730), um["RetentionPeriod"])

	// StopIngestion
	stop := callJSON(t, p, "StopEventDataStoreIngestion", `{"EventDataStore":"`+edsARN+`"}`)
	assert.Equal(t, 200, stop.StatusCode)
	gm2 := parseJSON(t, callJSON(t, p, "GetEventDataStore", `{"EventDataStore":"`+edsARN+`"}`))
	assert.Equal(t, "STOPPED_INGESTION", gm2["Status"])

	// StartIngestion
	start := callJSON(t, p, "StartEventDataStoreIngestion", `{"EventDataStore":"`+edsARN+`"}`)
	assert.Equal(t, 200, start.StatusCode)
	gm3 := parseJSON(t, callJSON(t, p, "GetEventDataStore", `{"EventDataStore":"`+edsARN+`"}`))
	assert.Equal(t, "ENABLED", gm3["Status"])

	// Delete
	del := callJSON(t, p, "DeleteEventDataStore", `{"EventDataStore":"`+edsARN+`"}`)
	assert.Equal(t, 200, del.StatusCode)

	// Get after delete
	gone := callJSON(t, p, "GetEventDataStore", `{"EventDataStore":"`+edsARN+`"}`)
	assert.Equal(t, 400, gone.StatusCode)
}

func TestChannelCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "CreateChannel", `{"Name":"my-channel","Source":"Custom"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "my-channel", m["Name"])
	chARN := m["ChannelArn"].(string)

	// Get
	get := callJSON(t, p, "GetChannel", `{"Channel":"`+chARN+`"}`)
	assert.Equal(t, 200, get.StatusCode)
	gm := parseJSON(t, get)
	assert.Equal(t, "my-channel", gm["Name"])
	assert.Equal(t, "Custom", gm["Source"])

	// List
	list := callJSON(t, p, "ListChannels", `{}`)
	assert.Equal(t, 200, list.StatusCode)
	lm := parseJSON(t, list)
	channels := lm["Channels"].([]any)
	assert.Len(t, channels, 1)

	// Update
	upd := callJSON(t, p, "UpdateChannel", `{"Channel":"`+chARN+`","Name":"renamed-channel"}`)
	assert.Equal(t, 200, upd.StatusCode)
	um := parseJSON(t, upd)
	assert.Equal(t, "renamed-channel", um["Name"])

	// Delete
	del := callJSON(t, p, "DeleteChannel", `{"Channel":"`+chARN+`"}`)
	assert.Equal(t, 200, del.StatusCode)

	// Get after delete
	gone := callJSON(t, p, "GetChannel", `{"Channel":"`+chARN+`"}`)
	assert.Equal(t, 400, gone.StatusCode)
}

func TestDashboardCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "CreateDashboard", `{"Name":"my-dashboard"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "my-dashboard", m["Name"])
	dashARN := m["DashboardArn"].(string)

	// Get
	get := callJSON(t, p, "GetDashboard", `{"DashboardId":"`+dashARN+`"}`)
	assert.Equal(t, 200, get.StatusCode)
	gm := parseJSON(t, get)
	assert.Equal(t, "my-dashboard", gm["Name"])
	assert.Equal(t, "ACTIVE", gm["Status"])

	// List
	list := callJSON(t, p, "ListDashboards", `{}`)
	assert.Equal(t, 200, list.StatusCode)
	lm := parseJSON(t, list)
	dashboards := lm["Dashboards"].([]any)
	assert.Len(t, dashboards, 1)

	// Update
	upd := callJSON(t, p, "UpdateDashboard", `{"DashboardId":"`+dashARN+`","Widgets":[]}`)
	assert.Equal(t, 200, upd.StatusCode)

	// StartDashboardRefresh
	refresh := callJSON(t, p, "StartDashboardRefresh", `{"DashboardId":"`+dashARN+`"}`)
	assert.Equal(t, 200, refresh.StatusCode)
	rm := parseJSON(t, refresh)
	assert.NotEmpty(t, rm["RefreshId"])

	// Delete
	del := callJSON(t, p, "DeleteDashboard", `{"DashboardId":"`+dashARN+`"}`)
	assert.Equal(t, 200, del.StatusCode)

	// Get after delete
	gone := callJSON(t, p, "GetDashboard", `{"DashboardId":"`+dashARN+`"}`)
	assert.Equal(t, 400, gone.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create trail with tags
	resp := callJSON(t, p, "CreateTrail", `{"Name":"tagged-trail","S3BucketName":"bucket","TagsList":[{"Key":"env","Value":"prod"}]}`)
	require.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	trailARN := m["TrailARN"].(string)

	// ListTags
	lt := callJSON(t, p, "ListTags", `{"ResourceIdList":["`+trailARN+`"]}`)
	assert.Equal(t, 200, lt.StatusCode)
	lm := parseJSON(t, lt)
	resList := lm["ResourceTagList"].([]any)
	assert.Len(t, resList, 1)
	entry := resList[0].(map[string]any)
	tagsList := entry["TagsList"].([]any)
	assert.Len(t, tagsList, 1)

	// AddTags
	add := callJSON(t, p, "AddTags", `{"ResourceId":"`+trailARN+`","TagsList":[{"Key":"team","Value":"ops"}]}`)
	assert.Equal(t, 200, add.StatusCode)

	lt2 := callJSON(t, p, "ListTags", `{"ResourceIdList":["`+trailARN+`"]}`)
	lm2 := parseJSON(t, lt2)
	resList2 := lm2["ResourceTagList"].([]any)
	entry2 := resList2[0].(map[string]any)
	tagsList2 := entry2["TagsList"].([]any)
	assert.Len(t, tagsList2, 2)

	// RemoveTags
	rem := callJSON(t, p, "RemoveTags", `{"ResourceId":"`+trailARN+`","TagsList":[{"Key":"env"}]}`)
	assert.Equal(t, 200, rem.StatusCode)

	lt3 := callJSON(t, p, "ListTags", `{"ResourceIdList":["`+trailARN+`"]}`)
	lm3 := parseJSON(t, lt3)
	resList3 := lm3["ResourceTagList"].([]any)
	entry3 := resList3[0].(map[string]any)
	tagsList3 := entry3["TagsList"].([]any)
	assert.Len(t, tagsList3, 1)
}

func TestEventSelectors(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "CreateTrail", `{"Name":"sel-trail","S3BucketName":"bucket"}`)

	// GetEventSelectors (empty)
	get := callJSON(t, p, "GetEventSelectors", `{"TrailName":"sel-trail"}`)
	assert.Equal(t, 200, get.StatusCode)

	// PutEventSelectors
	put := callJSON(t, p, "PutEventSelectors", `{"TrailName":"sel-trail","EventSelectors":[{"ReadWriteType":"All","IncludeManagementEvents":true}]}`)
	assert.Equal(t, 200, put.StatusCode)
	pm := parseJSON(t, put)
	selectors := pm["EventSelectors"].([]any)
	assert.Len(t, selectors, 1)

	// GetInsightSelectors
	gi := callJSON(t, p, "GetInsightSelectors", `{"TrailName":"sel-trail"}`)
	assert.Equal(t, 200, gi.StatusCode)

	// PutInsightSelectors
	pi := callJSON(t, p, "PutInsightSelectors", `{"TrailName":"sel-trail","InsightSelectors":[{"InsightType":"ApiCallRateInsight"}]}`)
	assert.Equal(t, 200, pi.StatusCode)
	im := parseJSON(t, pi)
	insights := im["InsightSelectors"].([]any)
	assert.Len(t, insights, 1)
}

func TestQueryOperations(t *testing.T) {
	p := newTestProvider(t)

	// LookupEvents - always empty
	le := callJSON(t, p, "LookupEvents", `{}`)
	assert.Equal(t, 200, le.StatusCode)
	lm := parseJSON(t, le)
	events := lm["Events"].([]any)
	assert.Empty(t, events)

	// StartQuery
	sq := callJSON(t, p, "StartQuery", `{"QueryStatement":"SELECT * FROM events LIMIT 10"}`)
	assert.Equal(t, 200, sq.StatusCode)
	sm := parseJSON(t, sq)
	queryID := sm["QueryId"].(string)
	assert.NotEmpty(t, queryID)

	// DescribeQuery
	dq := callJSON(t, p, "DescribeQuery", `{"QueryId":"`+queryID+`"}`)
	assert.Equal(t, 200, dq.StatusCode)
	dm := parseJSON(t, dq)
	assert.Equal(t, queryID, dm["QueryId"])

	// GetQueryResults
	gqr := callJSON(t, p, "GetQueryResults", `{"QueryId":"`+queryID+`"}`)
	assert.Equal(t, 200, gqr.StatusCode)

	// ListQueries
	lq := callJSON(t, p, "ListQueries", `{}`)
	assert.Equal(t, 200, lq.StatusCode)
	lqm := parseJSON(t, lq)
	queries := lqm["Queries"].([]any)
	assert.Len(t, queries, 1)

	// CancelQuery
	cq := callJSON(t, p, "CancelQuery", `{"QueryId":"`+queryID+`"}`)
	assert.Equal(t, 200, cq.StatusCode)
	cm := parseJSON(t, cq)
	assert.Equal(t, "CANCELLED", cm["QueryStatus"])
}
