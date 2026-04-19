// SPDX-License-Identifier: Apache-2.0

// internal/services/shield/provider_test.go
package shield

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

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

const svc = "Shield_20160616"

func TestProtectionCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create subscription first
	callJSON(t, p, svc+".CreateSubscription", `{}`)

	// Create protection
	resp := callJSON(t, p, svc+".CreateProtection",
		`{"Name":"my-protection","ResourceArn":"arn:aws:ec2:us-east-1:000000000000:instance/i-1234"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	protID, _ := rb["ProtectionId"].(string)
	assert.NotEmpty(t, protID)

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateProtection",
		`{"Name":"my-protection","ResourceArn":"arn:aws:ec2:us-east-1:000000000000:instance/i-1234"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe by ID
	descBody, _ := json.Marshal(map[string]any{"ProtectionId": protID})
	resp3 := callJSON(t, p, svc+".DescribeProtection", string(descBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	prot := rb3["Protection"].(map[string]any)
	assert.Equal(t, "my-protection", prot["Name"])
	assert.Equal(t, protID, prot["Id"])

	// List
	resp4 := callJSON(t, p, svc+".ListProtections", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	list, _ := rb4["Protections"].([]any)
	assert.Len(t, list, 1)

	// Associate health check
	assocBody, _ := json.Marshal(map[string]any{
		"ProtectionId":   protID,
		"HealthCheckArn": "arn:aws:route53:::healthcheck/abc123",
	})
	resp5 := callJSON(t, p, svc+".AssociateHealthCheck", string(assocBody))
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify health check associated
	resp6 := callJSON(t, p, svc+".DescribeProtection", string(descBody))
	rb6 := parseBody(t, resp6)
	prot6 := rb6["Protection"].(map[string]any)
	healthChecks, _ := prot6["HealthCheckIds"].([]any)
	assert.Len(t, healthChecks, 1)

	// Disassociate health check
	resp7 := callJSON(t, p, svc+".DisassociateHealthCheck", string(assocBody))
	assert.Equal(t, 200, resp7.StatusCode)

	// Verify removed
	resp8 := callJSON(t, p, svc+".DescribeProtection", string(descBody))
	rb8 := parseBody(t, resp8)
	prot8 := rb8["Protection"].(map[string]any)
	healthChecks8, _ := prot8["HealthCheckIds"].([]any)
	assert.Empty(t, healthChecks8)

	// Delete
	delBody, _ := json.Marshal(map[string]any{"ProtectionId": protID})
	resp9 := callJSON(t, p, svc+".DeleteProtection", string(delBody))
	assert.Equal(t, 200, resp9.StatusCode)

	// Describe after delete -> 400
	resp10 := callJSON(t, p, svc+".DescribeProtection", string(descBody))
	assert.Equal(t, 400, resp10.StatusCode)

	// Delete non-existent
	resp11 := callJSON(t, p, svc+".DeleteProtection", `{"ProtectionId":"nonexistent"}`)
	assert.Equal(t, 400, resp11.StatusCode)
}

func TestProtectionGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	resp := callJSON(t, p, svc+".CreateProtectionGroup",
		`{"ProtectionGroupId":"my-group","Aggregation":"SUM","Pattern":"ALL"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Duplicate
	resp2 := callJSON(t, p, svc+".CreateProtectionGroup",
		`{"ProtectionGroupId":"my-group","Aggregation":"SUM","Pattern":"ALL"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// Describe
	resp3 := callJSON(t, p, svc+".DescribeProtectionGroup",
		`{"ProtectionGroupId":"my-group"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	g := rb3["ProtectionGroup"].(map[string]any)
	assert.Equal(t, "my-group", g["ProtectionGroupId"])
	assert.Equal(t, "SUM", g["Aggregation"])

	// List
	resp4 := callJSON(t, p, svc+".ListProtectionGroups", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)
	rb4 := parseBody(t, resp4)
	list, _ := rb4["ProtectionGroups"].([]any)
	assert.Len(t, list, 1)

	// Update
	resp5 := callJSON(t, p, svc+".UpdateProtectionGroup",
		`{"ProtectionGroupId":"my-group","Aggregation":"MAX","Pattern":"ARBITRARY","Members":["arn:aws:ec2:us-east-1:000000000000:instance/i-1"]}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify update
	resp6 := callJSON(t, p, svc+".DescribeProtectionGroup", `{"ProtectionGroupId":"my-group"}`)
	rb6 := parseBody(t, resp6)
	g6 := rb6["ProtectionGroup"].(map[string]any)
	assert.Equal(t, "MAX", g6["Aggregation"])
	assert.Equal(t, "ARBITRARY", g6["Pattern"])

	// ListResourcesInProtectionGroup
	resp7 := callJSON(t, p, svc+".ListResourcesInProtectionGroup", `{"ProtectionGroupId":"my-group"}`)
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	arns, _ := rb7["ResourceArns"].([]any)
	assert.Len(t, arns, 1)

	// Delete
	resp8 := callJSON(t, p, svc+".DeleteProtectionGroup", `{"ProtectionGroupId":"my-group"}`)
	assert.Equal(t, 200, resp8.StatusCode)

	// Describe after delete
	resp9 := callJSON(t, p, svc+".DescribeProtectionGroup", `{"ProtectionGroupId":"my-group"}`)
	assert.Equal(t, 400, resp9.StatusCode)
}

func TestSubscription(t *testing.T) {
	p := newTestProvider(t)

	// Initial state
	resp := callJSON(t, p, svc+".GetSubscriptionState", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "ACTIVE", rb["SubscriptionState"])

	// Create subscription
	resp2 := callJSON(t, p, svc+".CreateSubscription", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// Describe
	resp3 := callJSON(t, p, svc+".DescribeSubscription", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	sub := rb3["Subscription"].(map[string]any)
	assert.Equal(t, "ENABLED", sub["AutoRenew"])

	// Update
	resp4 := callJSON(t, p, svc+".UpdateSubscription", `{"AutoRenew":"DISABLED"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callJSON(t, p, svc+".DescribeSubscription", `{}`)
	rb5 := parseBody(t, resp5)
	sub5 := rb5["Subscription"].(map[string]any)
	assert.Equal(t, "DISABLED", sub5["AutoRenew"])

	// Proactive engagement
	resp6 := callJSON(t, p, svc+".EnableProactiveEngagement", `{}`)
	assert.Equal(t, 200, resp6.StatusCode)

	resp7 := callJSON(t, p, svc+".DescribeSubscription", `{}`)
	rb7 := parseBody(t, resp7)
	sub7 := rb7["Subscription"].(map[string]any)
	assert.Equal(t, "ENABLED", sub7["ProactiveEngagementStatus"])

	resp8 := callJSON(t, p, svc+".DisableProactiveEngagement", `{}`)
	assert.Equal(t, 200, resp8.StatusCode)

	// Delete subscription
	resp9 := callJSON(t, p, svc+".DeleteSubscription", `{}`)
	assert.Equal(t, 200, resp9.StatusCode)

	// State after delete
	resp10 := callJSON(t, p, svc+".GetSubscriptionState", `{}`)
	rb10 := parseBody(t, resp10)
	assert.Equal(t, "INACTIVE", rb10["SubscriptionState"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create protection
	resp := callJSON(t, p, svc+".CreateProtection",
		`{"Name":"tagged-prot","ResourceArn":"arn:aws:ec2:us-east-1:000000000000:instance/i-tagged"}`)
	rb := parseBody(t, resp)
	protID, _ := rb["ProtectionId"].(string)
	require.NotEmpty(t, protID)

	prot, _ := p.store.GetProtection(protID)
	protARN := prot.ARN

	// Tag
	tagBody, _ := json.Marshal(map[string]any{
		"ResourceARN": protARN,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	resp2 := callJSON(t, p, svc+".TagResource", string(tagBody))
	assert.Equal(t, 200, resp2.StatusCode)

	// ListTagsForResource
	listBody, _ := json.Marshal(map[string]any{"ResourceARN": protARN})
	resp3 := callJSON(t, p, svc+".ListTagsForResource", string(listBody))
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	tags, _ := rb3["Tags"].([]any)
	assert.Len(t, tags, 2)

	// Untag
	untagBody, _ := json.Marshal(map[string]any{
		"ResourceARN": protARN,
		"TagKeys":     []string{"env"},
	})
	resp4 := callJSON(t, p, svc+".UntagResource", string(untagBody))
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify 1 tag remains
	resp5 := callJSON(t, p, svc+".ListTagsForResource", string(listBody))
	rb5 := parseBody(t, resp5)
	tags5, _ := rb5["Tags"].([]any)
	assert.Len(t, tags5, 1)
}
