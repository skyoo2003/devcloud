// SPDX-License-Identifier: Apache-2.0

// internal/services/costexplorer/provider_test.go
package costexplorer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	if err := p.Init(plugin.PluginConfig{DataDir: dir}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		p.Shutdown(context.Background())
		os.RemoveAll(dir)
	})
	return p
}

func doRequest(t *testing.T, p *Provider, action string, body map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := p.HandleRequest(context.Background(), action, req)
	if err != nil {
		t.Fatalf("HandleRequest(%s): %v", action, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("action %s: unexpected status %d: %s", action, resp.StatusCode, string(resp.Body))
	}
	var out map[string]any
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return out
}

func TestCostCategoryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	res := doRequest(t, p, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "MyCategory",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []any{},
	})
	arn, ok := res["CostCategoryArn"].(string)
	if !ok || arn == "" {
		t.Fatal("expected CostCategoryArn in create response")
	}

	// Describe
	res = doRequest(t, p, "DescribeCostCategoryDefinition", map[string]any{
		"CostCategoryArn": arn,
	})
	cat, ok := res["CostCategory"].(map[string]any)
	if !ok {
		t.Fatal("expected CostCategory in describe response")
	}
	if cat["Name"] != "MyCategory" {
		t.Errorf("expected name MyCategory, got %v", cat["Name"])
	}

	// List
	res = doRequest(t, p, "ListCostCategoryDefinitions", map[string]any{})
	refs, ok := res["CostCategoryReferences"].([]any)
	if !ok || len(refs) != 1 {
		t.Fatalf("expected 1 cost category reference, got %v", res)
	}

	// Update
	doRequest(t, p, "UpdateCostCategoryDefinition", map[string]any{
		"CostCategoryArn": arn,
		"RuleVersion":     "CostCategoryExpression.v1",
		"Rules":           []any{},
	})

	// Delete
	res = doRequest(t, p, "DeleteCostCategoryDefinition", map[string]any{
		"CostCategoryArn": arn,
	})
	if res["CostCategoryArn"] != arn {
		t.Errorf("expected CostCategoryArn %s in delete response", arn)
	}

	// List should be empty now
	res = doRequest(t, p, "ListCostCategoryDefinitions", map[string]any{})
	refs = res["CostCategoryReferences"].([]any)
	if len(refs) != 0 {
		t.Fatalf("expected 0 cost category references after delete, got %d", len(refs))
	}
}

func TestAnomalyMonitorCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	res := doRequest(t, p, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName":      "MyMonitor",
			"MonitorType":      "DIMENSIONAL",
			"MonitorDimension": "SERVICE",
		},
	})
	arn, ok := res["MonitorArn"].(string)
	if !ok || arn == "" {
		t.Fatal("expected MonitorArn in create response")
	}

	// Get
	res = doRequest(t, p, "GetAnomalyMonitors", map[string]any{
		"MonitorArnList": []any{arn},
	})
	monitors, ok := res["AnomalyMonitors"].([]any)
	if !ok || len(monitors) != 1 {
		t.Fatalf("expected 1 monitor, got %v", res)
	}
	mon := monitors[0].(map[string]any)
	if mon["MonitorName"] != "MyMonitor" {
		t.Errorf("expected MonitorName MyMonitor, got %v", mon["MonitorName"])
	}

	// Update
	doRequest(t, p, "UpdateAnomalyMonitor", map[string]any{
		"MonitorArn":  arn,
		"MonitorName": "UpdatedMonitor",
	})

	// Get after update
	res = doRequest(t, p, "GetAnomalyMonitors", map[string]any{})
	monitors = res["AnomalyMonitors"].([]any)
	if len(monitors) != 1 {
		t.Fatalf("expected 1 monitor after update, got %d", len(monitors))
	}
	mon = monitors[0].(map[string]any)
	if mon["MonitorName"] != "UpdatedMonitor" {
		t.Errorf("expected UpdatedMonitor, got %v", mon["MonitorName"])
	}

	// Delete
	doRequest(t, p, "DeleteAnomalyMonitor", map[string]any{
		"MonitorArn": arn,
	})

	// Get should return empty
	res = doRequest(t, p, "GetAnomalyMonitors", map[string]any{})
	monitors = res["AnomalyMonitors"].([]any)
	if len(monitors) != 0 {
		t.Fatalf("expected 0 monitors after delete, got %d", len(monitors))
	}
}

func TestAnomalySubscriptionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	res := doRequest(t, p, "CreateAnomalySubscription", map[string]any{
		"AnomalySubscription": map[string]any{
			"SubscriptionName": "MySub",
			"Frequency":        "WEEKLY",
			"Threshold":        float64(100),
			"MonitorArnList":   []any{},
			"Subscribers":      []any{},
		},
	})
	arn, ok := res["SubscriptionArn"].(string)
	if !ok || arn == "" {
		t.Fatal("expected SubscriptionArn in create response")
	}

	// Get
	res = doRequest(t, p, "GetAnomalySubscriptions", map[string]any{})
	subs, ok := res["AnomalySubscriptions"].([]any)
	if !ok || len(subs) != 1 {
		t.Fatalf("expected 1 subscription, got %v", res)
	}
	sub := subs[0].(map[string]any)
	if sub["SubscriptionName"] != "MySub" {
		t.Errorf("expected MySub, got %v", sub["SubscriptionName"])
	}

	// Update
	doRequest(t, p, "UpdateAnomalySubscription", map[string]any{
		"SubscriptionArn":  arn,
		"SubscriptionName": "UpdatedSub",
		"Frequency":        "DAILY",
	})

	// Get after update
	res = doRequest(t, p, "GetAnomalySubscriptions", map[string]any{})
	subs = res["AnomalySubscriptions"].([]any)
	if len(subs) != 1 {
		t.Fatalf("expected 1 subscription after update, got %d", len(subs))
	}
	sub = subs[0].(map[string]any)
	if sub["Frequency"] != "DAILY" {
		t.Errorf("expected DAILY frequency, got %v", sub["Frequency"])
	}

	// Delete
	doRequest(t, p, "DeleteAnomalySubscription", map[string]any{
		"SubscriptionArn": arn,
	})

	// Get should return empty
	res = doRequest(t, p, "GetAnomalySubscriptions", map[string]any{})
	subs = res["AnomalySubscriptions"].([]any)
	if len(subs) != 0 {
		t.Fatalf("expected 0 subscriptions after delete, got %d", len(subs))
	}
}

func TestGetCostAndUsage(t *testing.T) {
	p := newTestProvider(t)

	res := doRequest(t, p, "GetCostAndUsage", map[string]any{
		"TimePeriod": map[string]any{
			"Start": "2024-01-01",
			"End":   "2024-02-01",
		},
		"Granularity": "MONTHLY",
		"Metrics":     []any{"BlendedCost"},
	})

	if _, ok := res["ResultsByTime"]; !ok {
		t.Error("expected ResultsByTime in response")
	}
	tp, ok := res["TimePeriod"].(map[string]any)
	if !ok {
		t.Fatal("expected TimePeriod in response")
	}
	if tp["Start"] != "2024-01-01" {
		t.Errorf("expected Start 2024-01-01, got %v", tp["Start"])
	}

	// Also test GetCostAndUsageWithResources
	res = doRequest(t, p, "GetCostAndUsageWithResources", map[string]any{
		"TimePeriod": map[string]any{
			"Start": "2024-01-01",
			"End":   "2024-02-01",
		},
		"Granularity": "MONTHLY",
	})
	if _, ok := res["ResultsByTime"]; !ok {
		t.Error("expected ResultsByTime in GetCostAndUsageWithResources response")
	}

	// Test GetCostForecast
	res = doRequest(t, p, "GetCostForecast", map[string]any{
		"TimePeriod": map[string]any{
			"Start": "2024-02-01",
			"End":   "2024-03-01",
		},
		"Metric":      "BLENDED_COST",
		"Granularity": "MONTHLY",
	})
	if _, ok := res["Total"]; !ok {
		t.Error("expected Total in GetCostForecast response")
	}
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a cost category to tag
	res := doRequest(t, p, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "TaggedCategory",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []any{},
	})
	arn := res["CostCategoryArn"].(string)

	// Tag
	doRequest(t, p, "TagResource", map[string]any{
		"ResourceArn": arn,
		"ResourceTags": []any{
			map[string]any{"Key": "env", "Value": "test"},
			map[string]any{"Key": "team", "Value": "platform"},
		},
	})

	// ListTags
	res = doRequest(t, p, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	tagList, ok := res["ResourceTags"].([]any)
	if !ok {
		t.Fatal("expected ResourceTags in response")
	}
	if len(tagList) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(tagList))
	}

	// Untag
	doRequest(t, p, "UntagResource", map[string]any{
		"ResourceArn":     arn,
		"ResourceTagKeys": []any{"env"},
	})

	// ListTags again — should have 1
	res = doRequest(t, p, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	tagList = res["ResourceTags"].([]any)
	if len(tagList) != 1 {
		t.Fatalf("expected 1 tag after untag, got %d", len(tagList))
	}
	tag := tagList[0].(map[string]any)
	if tag["Key"] != "team" || tag["Value"] != "platform" {
		t.Errorf("expected remaining tag team=platform, got %v", tag)
	}
}
