// SPDX-License-Identifier: Apache-2.0

// internal/services/wafv2/provider_test.go
package wafv2

import (
	"context"
	"encoding/json"
	"net/http"
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

func call(t *testing.T, p *Provider, action string, body map[string]any) *plugin.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(b)))
	req.Header.Set("X-Amz-Target", "WAF_20190729."+action)
	resp, err := p.HandleRequest(context.Background(), action, req)
	require.NoError(t, err)
	return resp
}

func parse(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestWebACLCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateWebACL", map[string]any{
		"Name":          "test-acl",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   true,
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "test-acl",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	summary := rb["Summary"].(map[string]any)
	assert.Equal(t, "test-acl", summary["Name"])
	lockToken, _ := summary["LockToken"].(string)
	assert.NotEmpty(t, lockToken)

	// Duplicate create
	resp2 := call(t, p, "CreateWebACL", map[string]any{
		"Name":             "test-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)

	// Get
	resp3 := call(t, p, "GetWebACL", map[string]any{"Name": "test-acl", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	acl := rb3["WebACL"].(map[string]any)
	assert.Equal(t, "test-acl", acl["Name"])

	// List
	resp4 := call(t, p, "ListWebACLs", map[string]any{"Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)
	rb4 := parse(t, resp4)
	acls := rb4["WebACLs"].([]any)
	assert.Len(t, acls, 1)

	// Update
	resp5 := call(t, p, "UpdateWebACL", map[string]any{
		"Name":             "test-acl",
		"Scope":            "REGIONAL",
		"LockToken":        lockToken,
		"Description":      "updated",
		"DefaultAction":    map[string]any{"Block": map[string]any{}},
		"VisibilityConfig": map[string]any{},
	})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)
	rb5 := parse(t, resp5)
	newToken, _ := rb5["NextLockToken"].(string)
	assert.NotEmpty(t, newToken)
	assert.NotEqual(t, lockToken, newToken)

	// Associate and disassociate
	aclARN, _ := acl["ARN"].(string)
	respAssoc := call(t, p, "AssociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:apigateway:us-east-1::/restapis/abc",
		"WebACLArn":   aclARN,
	})
	assert.Equal(t, http.StatusOK, respAssoc.StatusCode)

	respForResource := call(t, p, "GetWebACLForResource", map[string]any{
		"ResourceArn": "arn:aws:apigateway:us-east-1::/restapis/abc",
	})
	assert.Equal(t, http.StatusOK, respForResource.StatusCode)
	rbForRes := parse(t, respForResource)
	aclInRes := rbForRes["WebACL"].(map[string]any)
	assert.Equal(t, "test-acl", aclInRes["Name"])

	respListRes := call(t, p, "ListResourcesForWebACL", map[string]any{"WebACLArn": aclARN})
	assert.Equal(t, http.StatusOK, respListRes.StatusCode)
	rbListRes := parse(t, respListRes)
	resourceARNs := rbListRes["ResourceArns"].([]any)
	assert.Len(t, resourceARNs, 1)

	respDisassoc := call(t, p, "DisassociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:apigateway:us-east-1::/restapis/abc",
	})
	assert.Equal(t, http.StatusOK, respDisassoc.StatusCode)

	// Delete
	resp6 := call(t, p, "DeleteWebACL", map[string]any{"Name": "test-acl", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Get after delete
	resp7 := call(t, p, "GetWebACL", map[string]any{"Name": "test-acl", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestIPSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateIPSet", map[string]any{
		"Name":             "my-ipset",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{"192.0.2.0/24", "198.51.100.0/24"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	summary := rb["Summary"].(map[string]any)
	assert.Equal(t, "my-ipset", summary["Name"])
	lockToken, _ := summary["LockToken"].(string)
	assert.NotEmpty(t, lockToken)

	// Get
	resp2 := call(t, p, "GetIPSet", map[string]any{"Name": "my-ipset", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	ipset := rb2["IPSet"].(map[string]any)
	addresses := ipset["Addresses"].([]any)
	assert.Len(t, addresses, 2)

	// List
	resp3 := call(t, p, "ListIPSets", map[string]any{"Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	sets := rb3["IPSets"].([]any)
	assert.Len(t, sets, 1)

	// Update
	resp4 := call(t, p, "UpdateIPSet", map[string]any{
		"Name":      "my-ipset",
		"Scope":     "REGIONAL",
		"LockToken": lockToken,
		"Addresses": []string{"10.0.0.0/8"},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Verify update
	resp5 := call(t, p, "GetIPSet", map[string]any{"Name": "my-ipset", "Scope": "REGIONAL"})
	rb5 := parse(t, resp5)
	ipset5 := rb5["IPSet"].(map[string]any)
	addresses5 := ipset5["Addresses"].([]any)
	assert.Len(t, addresses5, 1)

	// Delete
	resp6 := call(t, p, "DeleteIPSet", map[string]any{"Name": "my-ipset", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Get after delete
	resp7 := call(t, p, "GetIPSet", map[string]any{"Name": "my-ipset", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestRegexPatternSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateRegexPatternSet", map[string]any{
		"Name":  "my-regex",
		"Scope": "REGIONAL",
		"RegularExpressionList": []map[string]string{
			{"RegexString": "^admin$"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	summary := rb["Summary"].(map[string]any)
	assert.Equal(t, "my-regex", summary["Name"])

	// Get
	resp2 := call(t, p, "GetRegexPatternSet", map[string]any{"Name": "my-regex", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	rps := rb2["RegexPatternSet"].(map[string]any)
	patterns := rps["RegularExpressionList"].([]any)
	assert.Len(t, patterns, 1)

	// List
	resp3 := call(t, p, "ListRegexPatternSets", map[string]any{"Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	sets := rb3["RegexPatternSets"].([]any)
	assert.Len(t, sets, 1)

	// Update
	resp4 := call(t, p, "UpdateRegexPatternSet", map[string]any{
		"Name":      "my-regex",
		"Scope":     "REGIONAL",
		"LockToken": rb["Summary"].(map[string]any)["LockToken"],
		"RegularExpressionList": []map[string]string{
			{"RegexString": "^admin$"},
			{"RegexString": "^root$"},
		},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Delete
	resp5 := call(t, p, "DeleteRegexPatternSet", map[string]any{"Name": "my-regex", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)

	// Get after delete
	resp6 := call(t, p, "GetRegexPatternSet", map[string]any{"Name": "my-regex", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, resp6.StatusCode)
}

func TestRuleGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateRuleGroup", map[string]any{
		"Name":     "my-rulegroup",
		"Scope":    "REGIONAL",
		"Capacity": float64(100),
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   true,
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "my-rulegroup",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	summary := rb["Summary"].(map[string]any)
	assert.Equal(t, "my-rulegroup", summary["Name"])
	lockToken, _ := summary["LockToken"].(string)
	assert.NotEmpty(t, lockToken)

	// Get
	resp2 := call(t, p, "GetRuleGroup", map[string]any{"Name": "my-rulegroup", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	rg := rb2["RuleGroup"].(map[string]any)
	assert.Equal(t, "my-rulegroup", rg["Name"])

	// List
	resp3 := call(t, p, "ListRuleGroups", map[string]any{"Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	groups := rb3["RuleGroups"].([]any)
	assert.Len(t, groups, 1)

	// Update
	resp4 := call(t, p, "UpdateRuleGroup", map[string]any{
		"Name":             "my-rulegroup",
		"Scope":            "REGIONAL",
		"LockToken":        lockToken,
		"Description":      "updated rule group",
		"VisibilityConfig": map[string]any{},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Delete
	resp5 := call(t, p, "DeleteRuleGroup", map[string]any{"Name": "my-rulegroup", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)

	// Get after delete
	resp6 := call(t, p, "GetRuleGroup", map[string]any{"Name": "my-rulegroup", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, resp6.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a WebACL with tags
	resp := call(t, p, "CreateWebACL", map[string]any{
		"Name":             "tagged-acl",
		"Scope":            "REGIONAL",
		"DefaultAction":    map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{},
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	arn, _ := rb["Summary"].(map[string]any)["ARN"].(string)
	require.NotEmpty(t, arn)

	// ListTagsForResource
	resp2 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	tagInfo := rb2["TagInfoForResource"].(map[string]any)
	tagList := tagInfo["TagList"].([]any)
	assert.Len(t, tagList, 1)
	assert.Equal(t, "env", tagList[0].(map[string]any)["Key"])

	// TagResource
	resp3 := call(t, p, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	resp4 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	rb4 := parse(t, resp4)
	tagList4 := rb4["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	assert.Len(t, tagList4, 2)

	// UntagResource
	resp5 := call(t, p, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, resp5.StatusCode)

	resp6 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	rb6 := parse(t, resp6)
	tagList6 := rb6["TagInfoForResource"].(map[string]any)["TagList"].([]any)
	assert.Len(t, tagList6, 1)
	assert.Equal(t, "owner", tagList6[0].(map[string]any)["Key"])
}
