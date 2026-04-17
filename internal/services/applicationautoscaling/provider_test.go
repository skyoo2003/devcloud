// SPDX-License-Identifier: Apache-2.0

// internal/services/applicationautoscaling/provider_test.go
package applicationautoscaling

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

const svcTarget = "AnyScaleFrontendService"

func TestScalableTargetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Register
	resp := callJSON(t, p, svcTarget+".RegisterScalableTarget", `{
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity": 1,
		"MaxCapacity": 5
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ScalableTargetARN"], "arn:aws:")

	// Describe
	resp2 := callJSON(t, p, svcTarget+".DescribeScalableTargets", `{
		"ServiceNamespace": "ecs"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	targets := m2["ScalableTargets"].([]any)
	assert.Len(t, targets, 1)
	target := targets[0].(map[string]any)
	assert.Equal(t, "service/my-cluster/my-service", target["ResourceId"])
	assert.Equal(t, float64(1), target["MinCapacity"])
	assert.Equal(t, float64(5), target["MaxCapacity"])

	// Update (upsert)
	resp3 := callJSON(t, p, svcTarget+".RegisterScalableTarget", `{
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MinCapacity": 2,
		"MaxCapacity": 10
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callJSON(t, p, svcTarget+".DescribeScalableTargets", `{
		"ServiceNamespace": "ecs"
	}`)
	m4 := parseJSON(t, resp4)
	targets4 := m4["ScalableTargets"].([]any)
	assert.Len(t, targets4, 1)
	t4 := targets4[0].(map[string]any)
	assert.Equal(t, float64(2), t4["MinCapacity"])
	assert.Equal(t, float64(10), t4["MaxCapacity"])

	// Deregister
	resp5 := callJSON(t, p, svcTarget+".DeregisterScalableTarget", `{
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount"
	}`)
	assert.Equal(t, 200, resp5.StatusCode)

	resp6 := callJSON(t, p, svcTarget+".DescribeScalableTargets", `{"ServiceNamespace": "ecs"}`)
	m6 := parseJSON(t, resp6)
	targets6 := m6["ScalableTargets"].([]any)
	assert.Empty(t, targets6)
}

func TestScalingPolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put policy
	resp := callJSON(t, p, svcTarget+".PutScalingPolicy", `{
		"PolicyName": "my-policy",
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyType": "TargetTrackingScaling"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["PolicyARN"], "arn:aws:")

	// Describe
	resp2 := callJSON(t, p, svcTarget+".DescribeScalingPolicies", `{
		"ServiceNamespace": "ecs"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	policies := m2["ScalingPolicies"].([]any)
	assert.Len(t, policies, 1)
	pol := policies[0].(map[string]any)
	assert.Equal(t, "my-policy", pol["PolicyName"])

	// Delete
	resp3 := callJSON(t, p, svcTarget+".DeleteScalingPolicy", `{
		"PolicyName": "my-policy",
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount"
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callJSON(t, p, svcTarget+".DescribeScalingPolicies", `{"ServiceNamespace": "ecs"}`)
	m4 := parseJSON(t, resp4)
	policies4 := m4["ScalingPolicies"].([]any)
	assert.Empty(t, policies4)

	// Delete non-existent
	resp5 := callJSON(t, p, svcTarget+".DeleteScalingPolicy", `{
		"PolicyName": "no-such-policy",
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount"
	}`)
	assert.Equal(t, 400, resp5.StatusCode)
}

func TestScheduledActionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Put action
	resp := callJSON(t, p, svcTarget+".PutScheduledAction", `{
		"ScheduledActionName": "my-action",
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount",
		"Schedule": "rate(5 minutes)"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ScheduledActionARN"], "arn:aws:")

	// Describe
	resp2 := callJSON(t, p, svcTarget+".DescribeScheduledActions", `{
		"ServiceNamespace": "ecs"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	actions := m2["ScheduledActions"].([]any)
	assert.Len(t, actions, 1)
	action := actions[0].(map[string]any)
	assert.Equal(t, "my-action", action["ScheduledActionName"])
	assert.Equal(t, "rate(5 minutes)", action["Schedule"])

	// Delete
	resp3 := callJSON(t, p, svcTarget+".DeleteScheduledAction", `{
		"ScheduledActionName": "my-action",
		"ServiceNamespace": "ecs",
		"ResourceId": "service/my-cluster/my-service",
		"ScalableDimension": "ecs:service:DesiredCount"
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callJSON(t, p, svcTarget+".DescribeScheduledActions", `{"ServiceNamespace": "ecs"}`)
	m4 := parseJSON(t, resp4)
	actions4 := m4["ScheduledActions"].([]any)
	assert.Empty(t, actions4)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)
	arn := "arn:aws:application-autoscaling:us-east-1:000000000000:scalableTarget/test"

	// Tag
	resp := callJSON(t, p, svcTarget+".TagResource", `{
		"ResourceARN": "`+arn+`",
		"Tags": {"env": "prod", "team": "platform"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	// List tags
	resp2 := callJSON(t, p, svcTarget+".ListTagsForResource", `{
		"ResourceARN": "`+arn+`"
	}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	tags := m2["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag
	resp3 := callJSON(t, p, svcTarget+".UntagResource", `{
		"ResourceARN": "`+arn+`",
		"TagKeys": ["env"]
	}`)
	assert.Equal(t, 200, resp3.StatusCode)

	resp4 := callJSON(t, p, svcTarget+".ListTagsForResource", `{
		"ResourceARN": "`+arn+`"
	}`)
	m4 := parseJSON(t, resp4)
	tags4 := m4["Tags"].(map[string]any)
	assert.NotContains(t, tags4, "env")
	assert.Equal(t, "platform", tags4["team"])
}

func TestStatistics(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, svcTarget+".DescribeStatistics", `{
		"ServiceNamespace": "ecs",
		"ResourceId": "service/c1/s1",
		"ScalableDimension": "ecs:service:DesiredCount"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	stats := m["Statistics"].([]any)
	assert.GreaterOrEqual(t, len(stats), 1)
}

func TestCustomizedMetrics(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, svcTarget+".PutCustomizedMetricStatistics", `{
		"ServiceNamespace": "ecs",
		"ResourceId": "service/c1/s1",
		"ScalableDimension": "ecs:service:DesiredCount",
		"MetricName": "CPUUtilization",
		"Namespace": "AWS/ECS",
		"Statistic": "Average",
		"Unit": "Percent"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["MetricId"])

	descResp := callJSON(t, p, svcTarget+".DescribeCustomizedMetricStatistics", `{
		"ServiceNamespace": "ecs"
	}`)
	assert.Equal(t, 200, descResp.StatusCode)
	d := parseJSON(t, descResp)
	metrics := d["Metrics"].([]any)
	assert.Len(t, metrics, 1)
}

func TestAccountLimits(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, svcTarget+".DescribeAccountLimits", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, float64(2000), m["MaxNumberOfScalableTargets"])
}

func TestBatchOperations(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, svcTarget+".BatchRegisterScalableTarget", `{
		"ScalableTargets": [
			{"ServiceNamespace": "ecs", "ResourceId": "service/c1/s1", "ScalableDimension": "ecs:service:DesiredCount", "MinCapacity": 1, "MaxCapacity": 5},
			{"ServiceNamespace": "ecs", "ResourceId": "service/c1/s2", "ScalableDimension": "ecs:service:DesiredCount", "MinCapacity": 2, "MaxCapacity": 10}
		]
	}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	targets := m["ScalableTargets"].([]any)
	assert.Len(t, targets, 2)
}

func TestScalingHealth(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, svcTarget+".GetScalingHealth", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "Healthy", m["Status"])
}
