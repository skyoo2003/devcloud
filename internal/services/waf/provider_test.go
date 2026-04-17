// SPDX-License-Identifier: Apache-2.0

// internal/services/waf/provider_test.go
package waf

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func call(t *testing.T, p *Provider, action string, body map[string]any) *plugin.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(b)))
	req.Header.Set("X-Amz-Target", "WAF_20150824."+action)
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

func TestGetChangeToken(t *testing.T) {
	p := newTestProvider(t)

	resp := call(t, p, "GetChangeToken", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	token, _ := rb["ChangeToken"].(string)
	assert.NotEmpty(t, token)

	// Two calls return different tokens
	resp2 := call(t, p, "GetChangeToken", map[string]any{})
	rb2 := parse(t, resp2)
	token2, _ := rb2["ChangeToken"].(string)
	assert.NotEqual(t, token, token2)

	// Status is always INSYNC
	resp3 := call(t, p, "GetChangeTokenStatus", map[string]any{"ChangeToken": token})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	assert.Equal(t, "INSYNC", rb3["ChangeTokenStatus"])
}

func TestWebACLCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateWebACL", map[string]any{
		"Name":          "test-acl",
		"ChangeToken":   "token-1",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
		"MetricName":    "testacl",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	acl := rb["WebACL"].(map[string]any)
	aclID, _ := acl["WebACLId"].(string)
	assert.NotEmpty(t, aclID)
	assert.Equal(t, "test-acl", acl["Name"])

	// Get
	resp2 := call(t, p, "GetWebACL", map[string]any{"WebACLId": aclID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	acl2 := rb2["WebACL"].(map[string]any)
	assert.Equal(t, "test-acl", acl2["Name"])
	assert.Equal(t, "ALLOW", acl2["DefaultAction"].(map[string]any)["Type"])

	// List
	resp3 := call(t, p, "ListWebACLs", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	acls := rb3["WebACLs"].([]any)
	assert.Len(t, acls, 1)

	// Update
	resp4 := call(t, p, "UpdateWebACL", map[string]any{
		"WebACLId":      aclID,
		"ChangeToken":   "token-2",
		"DefaultAction": map[string]any{"Type": "BLOCK"},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Get after update
	resp5 := call(t, p, "GetWebACL", map[string]any{"WebACLId": aclID})
	rb5 := parse(t, resp5)
	acl5 := rb5["WebACL"].(map[string]any)
	assert.Equal(t, "BLOCK", acl5["DefaultAction"].(map[string]any)["Type"])

	// Delete
	resp6 := call(t, p, "DeleteWebACL", map[string]any{"WebACLId": aclID, "ChangeToken": "token-3"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Get after delete → not found
	resp7 := call(t, p, "GetWebACL", map[string]any{"WebACLId": aclID})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestIPSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateIPSet", map[string]any{
		"Name":        "my-ip-set",
		"ChangeToken": "tok1",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	ipSet := rb["IPSet"].(map[string]any)
	ipID, _ := ipSet["IPSetId"].(string)
	assert.NotEmpty(t, ipID)
	assert.Equal(t, "my-ip-set", ipSet["Name"])

	// Get
	resp2 := call(t, p, "GetIPSet", map[string]any{"IPSetId": ipID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	ipSet2 := rb2["IPSet"].(map[string]any)
	assert.Equal(t, "my-ip-set", ipSet2["Name"])
	descs := ipSet2["IPSetDescriptors"].([]any)
	assert.Len(t, descs, 0)

	// List
	resp3 := call(t, p, "ListIPSets", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	sets := rb3["IPSets"].([]any)
	assert.Len(t, sets, 1)

	// Update
	resp4 := call(t, p, "UpdateIPSet", map[string]any{
		"IPSetId":     ipID,
		"ChangeToken": "tok2",
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"IPSetDescriptor": map[string]any{
					"Type":  "IPV4",
					"Value": "192.0.2.0/24",
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Verify update
	resp5 := call(t, p, "GetIPSet", map[string]any{"IPSetId": ipID})
	rb5 := parse(t, resp5)
	ipSet5 := rb5["IPSet"].(map[string]any)
	descs5 := ipSet5["IPSetDescriptors"].([]any)
	assert.Len(t, descs5, 1)

	// Delete
	resp6 := call(t, p, "DeleteIPSet", map[string]any{"IPSetId": ipID, "ChangeToken": "tok3"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Not found after delete
	resp7 := call(t, p, "GetIPSet", map[string]any{"IPSetId": ipID})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateRule", map[string]any{
		"Name":        "my-rule",
		"MetricName":  "myrule",
		"ChangeToken": "tok1",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	rule := rb["Rule"].(map[string]any)
	ruleID, _ := rule["RuleId"].(string)
	assert.NotEmpty(t, ruleID)
	assert.Equal(t, "my-rule", rule["Name"])

	// Get
	resp2 := call(t, p, "GetRule", map[string]any{"RuleId": ruleID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	rule2 := rb2["Rule"].(map[string]any)
	assert.Equal(t, "my-rule", rule2["Name"])
	assert.Equal(t, "myrule", rule2["MetricName"])

	// List
	resp3 := call(t, p, "ListRules", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	rules := rb3["Rules"].([]any)
	assert.Len(t, rules, 1)

	// Update
	resp4 := call(t, p, "UpdateRule", map[string]any{
		"RuleId":      ruleID,
		"ChangeToken": "tok2",
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"Predicate": map[string]any{
					"Negated": false,
					"Type":    "IPMatch",
					"DataId":  "some-ip-set-id",
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Verify
	resp5 := call(t, p, "GetRule", map[string]any{"RuleId": ruleID})
	rb5 := parse(t, resp5)
	rule5 := rb5["Rule"].(map[string]any)
	preds := rule5["Predicates"].([]any)
	assert.Len(t, preds, 1)

	// Delete
	resp6 := call(t, p, "DeleteRule", map[string]any{"RuleId": ruleID, "ChangeToken": "tok3"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Not found
	resp7 := call(t, p, "GetRule", map[string]any{"RuleId": ruleID})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestByteMatchSetCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateByteMatchSet", map[string]any{
		"Name":        "my-bms",
		"ChangeToken": "tok1",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	bms := rb["ByteMatchSet"].(map[string]any)
	bmsID, _ := bms["ByteMatchSetId"].(string)
	assert.NotEmpty(t, bmsID)
	assert.Equal(t, "my-bms", bms["Name"])

	// Get
	resp2 := call(t, p, "GetByteMatchSet", map[string]any{"ByteMatchSetId": bmsID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	rb2 := parse(t, resp2)
	bms2 := rb2["ByteMatchSet"].(map[string]any)
	tuples := bms2["ByteMatchTuples"].([]any)
	assert.Len(t, tuples, 0)

	// List
	resp3 := call(t, p, "ListByteMatchSets", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	sets := rb3["ByteMatchSets"].([]any)
	assert.Len(t, sets, 1)

	// Update
	resp4 := call(t, p, "UpdateByteMatchSet", map[string]any{
		"ByteMatchSetId": bmsID,
		"ChangeToken":    "tok2",
		"Updates": []map[string]any{
			{
				"Action": "INSERT",
				"ByteMatchTuple": map[string]any{
					"FieldToMatch":         map[string]any{"Type": "URI"},
					"TargetString":         "badpath",
					"TextTransformation":   "NONE",
					"PositionalConstraint": "CONTAINS",
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Verify
	resp5 := call(t, p, "GetByteMatchSet", map[string]any{"ByteMatchSetId": bmsID})
	rb5 := parse(t, resp5)
	bms5 := rb5["ByteMatchSet"].(map[string]any)
	tuples5 := bms5["ByteMatchTuples"].([]any)
	assert.Len(t, tuples5, 1)

	// Delete
	resp6 := call(t, p, "DeleteByteMatchSet", map[string]any{"ByteMatchSetId": bmsID, "ChangeToken": "tok3"})
	assert.Equal(t, http.StatusOK, resp6.StatusCode)

	// Not found
	resp7 := call(t, p, "GetByteMatchSet", map[string]any{"ByteMatchSetId": bmsID})
	assert.Equal(t, http.StatusBadRequest, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a WebACL to get an ARN
	resp := call(t, p, "CreateWebACL", map[string]any{
		"Name":          "tagged-acl",
		"ChangeToken":   "tok1",
		"DefaultAction": map[string]any{"Type": "ALLOW"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	acl := rb["WebACL"].(map[string]any)
	arn, _ := acl["WebACLArn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	resp2 := call(t, p, "TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// ListTagsForResource
	resp3 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	rb3 := parse(t, resp3)
	info := rb3["TagInfoForResource"].(map[string]any)
	tagList := info["TagList"].([]any)
	assert.Len(t, tagList, 2)

	keys := make([]string, 0, 2)
	for _, tag := range tagList {
		if m, ok := tag.(map[string]any); ok {
			keys = append(keys, m["Key"].(string))
		}
	}
	assert.ElementsMatch(t, []string{"env", "team"}, keys)

	// UntagResource
	resp4 := call(t, p, "UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []any{"team"},
	})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Verify removal
	resp5 := call(t, p, "ListTagsForResource", map[string]any{"ResourceARN": arn})
	rb5 := parse(t, resp5)
	info5 := rb5["TagInfoForResource"].(map[string]any)
	tagList5 := info5["TagList"].([]any)
	assert.Len(t, tagList5, 1)
	assert.Equal(t, "env", tagList5[0].(map[string]any)["Key"])
}

func TestRuleGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := call(t, p, "CreateRuleGroup", map[string]any{
		"Name":        "my-rg",
		"MetricName":  "myrg",
		"ChangeToken": "tok1",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	rb := parse(t, resp)
	rg := rb["RuleGroup"].(map[string]any)
	rgID, _ := rg["RuleGroupId"].(string)
	assert.NotEmpty(t, rgID)

	// Get
	resp2 := call(t, p, "GetRuleGroup", map[string]any{"RuleGroupId": rgID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// List
	resp3 := call(t, p, "ListRuleGroups", map[string]any{})
	rb3 := parse(t, resp3)
	groups := rb3["RuleGroups"].([]any)
	assert.Len(t, groups, 1)

	// Delete
	resp4 := call(t, p, "DeleteRuleGroup", map[string]any{"RuleGroupId": rgID, "ChangeToken": "tok2"})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)

	// Not found
	resp5 := call(t, p, "GetRuleGroup", map[string]any{"RuleGroupId": rgID})
	assert.Equal(t, http.StatusBadRequest, resp5.StatusCode)
}
