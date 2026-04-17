// SPDX-License-Identifier: Apache-2.0

// internal/services/iot/provider_test.go
package iot

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
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func callRESTWithHeader(t *testing.T, p *Provider, method, path, op, body string, headers map[string]string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestThingCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/things/my-thing", "CreateThing",
		`{"thingName":"my-thing","thingTypeName":"myType"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "my-thing", rb["thingName"])
	assert.NotEmpty(t, rb["thingArn"])

	// Describe
	resp2 := callREST(t, p, "GET", "/things/my-thing", "DescribeThing", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "my-thing", rb2["thingName"])
	assert.Equal(t, "myType", rb2["thingTypeName"])

	// List
	resp3 := callREST(t, p, "GET", "/things", "ListThings", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	things, ok := rb3["things"].([]any)
	require.True(t, ok)
	assert.Len(t, things, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/things/my-thing", "UpdateThing",
		`{"thingTypeName":"newType"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/things/my-thing", "DescribeThing", "")
	rb5 := parseBody(t, resp5)
	assert.Equal(t, "newType", rb5["thingTypeName"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/things/my-thing", "DeleteThing", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callREST(t, p, "GET", "/things/my-thing", "DescribeThing", "")
	assert.Equal(t, 404, resp7.StatusCode)

	// Duplicate create after delete succeeds
	resp8 := callREST(t, p, "POST", "/things/my-thing", "CreateThing", `{"thingName":"my-thing"}`)
	assert.Equal(t, 200, resp8.StatusCode)

	// Duplicate create conflict
	resp9 := callREST(t, p, "POST", "/things/my-thing", "CreateThing", `{"thingName":"my-thing"}`)
	assert.Equal(t, 409, resp9.StatusCode)
}

func TestThingTypeCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/thing-types/myThingType", "CreateThingType",
		`{"thingTypeProperties":{"thingTypeDescription":"a test type"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "myThingType", rb["thingTypeName"])

	// Describe
	resp2 := callREST(t, p, "GET", "/thing-types/myThingType", "DescribeThingType", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "myThingType", rb2["thingTypeName"])
	props, ok := rb2["thingTypeProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "a test type", props["thingTypeDescription"])

	// List
	resp3 := callREST(t, p, "GET", "/thing-types", "ListThingTypes", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	types, ok := rb3["thingTypes"].([]any)
	require.True(t, ok)
	assert.Len(t, types, 1)

	// Deprecate
	resp4 := callREST(t, p, "POST", "/thing-types/myThingType/deprecate", "DeprecateThingType", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify deprecated
	resp5 := callREST(t, p, "GET", "/thing-types/myThingType", "DescribeThingType", "")
	rb5 := parseBody(t, resp5)
	meta, ok := rb5["thingTypeMetadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, meta["deprecated"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/thing-types/myThingType", "DeleteThingType", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callREST(t, p, "GET", "/thing-types/myThingType", "DescribeThingType", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestThingGroupCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create group
	resp := callREST(t, p, "POST", "/thing-groups/myGroup", "CreateThingGroup",
		`{"thingGroupProperties":{"thingGroupDescription":"my group"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "myGroup", rb["thingGroupName"])

	// Describe
	resp2 := callREST(t, p, "GET", "/thing-groups/myGroup", "DescribeThingGroup", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "myGroup", rb2["thingGroupName"])
	props, ok := rb2["thingGroupProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my group", props["thingGroupDescription"])

	// List
	resp3 := callREST(t, p, "GET", "/thing-groups", "ListThingGroups", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	groups, ok := rb3["thingGroups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 1)

	// Update
	resp4 := callREST(t, p, "PATCH", "/thing-groups/myGroup", "UpdateThingGroup",
		`{"thingGroupProperties":{"thingGroupDescription":"updated"}}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify update
	resp5 := callREST(t, p, "GET", "/thing-groups/myGroup", "DescribeThingGroup", "")
	rb5 := parseBody(t, resp5)
	props5, ok := rb5["thingGroupProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated", props5["thingGroupDescription"])

	// Create thing and add to group
	callREST(t, p, "POST", "/things/myThing", "CreateThing", `{"thingName":"myThing"}`)
	resp6 := callREST(t, p, "PUT", "/thing-groups/addThingToThingGroup", "AddThingToThingGroup",
		`{"thingGroupName":"myGroup","thingName":"myThing"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// List things in group
	resp7 := callREST(t, p, "GET", "/thing-groups/myGroup/things", "ListThingsInThingGroup", "")
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	groupThings, ok := rb7["things"].([]any)
	require.True(t, ok)
	assert.Len(t, groupThings, 1)

	// List groups for thing
	resp8 := callREST(t, p, "GET", "/things/myThing/thing-groups", "ListThingGroupsForThing", "")
	assert.Equal(t, 200, resp8.StatusCode)
	rb8 := parseBody(t, resp8)
	thingGroups, ok := rb8["thingGroups"].([]any)
	require.True(t, ok)
	assert.Len(t, thingGroups, 1)

	// Remove from group
	resp9 := callREST(t, p, "PUT", "/thing-groups/removeThingFromThingGroup", "RemoveThingFromThingGroup",
		`{"thingGroupName":"myGroup","thingName":"myThing"}`)
	assert.Equal(t, 200, resp9.StatusCode)

	// Verify removed
	resp10 := callREST(t, p, "GET", "/thing-groups/myGroup/things", "ListThingsInThingGroup", "")
	rb10 := parseBody(t, resp10)
	groupThings10, _ := rb10["things"].([]any)
	assert.Len(t, groupThings10, 0)

	// Delete group
	resp11 := callREST(t, p, "DELETE", "/thing-groups/myGroup", "DeleteThingGroup", "")
	assert.Equal(t, 200, resp11.StatusCode)

	// Verify deleted
	resp12 := callREST(t, p, "GET", "/thing-groups/myGroup", "DescribeThingGroup", "")
	assert.Equal(t, 404, resp12.StatusCode)
}

func TestPolicyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/policies/myPolicy", "CreatePolicy",
		`{"policyName":"myPolicy","policyDocument":"{\"Version\":\"2012-10-17\"}"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "myPolicy", rb["policyName"])
	assert.NotEmpty(t, rb["policyArn"])

	// Get
	resp2 := callREST(t, p, "GET", "/policies/myPolicy", "GetPolicy", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "myPolicy", rb2["policyName"])

	// List
	resp3 := callREST(t, p, "GET", "/policies", "ListPolicies", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	policies, ok := rb3["policies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 1)

	// Create policy version
	resp4 := callREST(t, p, "POST", "/policies/myPolicy/versions", "CreatePolicyVersion",
		`{"policyDocument":"{\"Version\":\"2012-10-17\",\"Statement\":[]}","setAsDefault":true}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// List policy versions
	resp5 := callREST(t, p, "GET", "/policies/myPolicy/versions", "ListPolicyVersions", "")
	assert.Equal(t, 200, resp5.StatusCode)
	rb5 := parseBody(t, resp5)
	versions, ok := rb5["policyVersions"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(versions), 1)

	// Attach policy to a target
	resp6 := callREST(t, p, "PUT", "/policies/myPolicy/targets", "AttachPolicy",
		`{"target":"arn:aws:iot:us-east-1:000000000000:cert/abc123"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// List targets for policy
	resp7 := callREST(t, p, "GET", "/policies/myPolicy/targets", "ListTargetsForPolicy", "")
	assert.Equal(t, 200, resp7.StatusCode)
	rb7 := parseBody(t, resp7)
	targets, ok := rb7["targets"].([]any)
	require.True(t, ok)
	assert.Len(t, targets, 1)

	// Detach policy
	resp8 := callREST(t, p, "DELETE", "/policies/myPolicy/targets", "DetachPolicy",
		`{"target":"arn:aws:iot:us-east-1:000000000000:cert/abc123"}`)
	assert.Equal(t, 200, resp8.StatusCode)

	// Delete policy
	resp9 := callREST(t, p, "DELETE", "/policies/myPolicy", "DeletePolicy", "")
	assert.Equal(t, 200, resp9.StatusCode)

	// Verify deleted
	resp10 := callREST(t, p, "GET", "/policies/myPolicy", "GetPolicy", "")
	assert.Equal(t, 404, resp10.StatusCode)
}

func TestCertificateCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create keys and certificate
	resp := callREST(t, p, "POST", "/keys-and-certificate", "CreateKeysAndCertificate",
		`{"setAsActive":true}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotEmpty(t, rb["certificateId"])
	assert.NotEmpty(t, rb["certificateArn"])
	certID := rb["certificateId"].(string)

	// Describe
	resp2 := callREST(t, p, "GET", "/certificates/"+certID, "DescribeCertificate", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	desc, ok := rb2["certificateDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", desc["status"])

	// List
	resp3 := callREST(t, p, "GET", "/certificates", "ListCertificates", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	certs, ok := rb3["certificates"].([]any)
	require.True(t, ok)
	assert.Len(t, certs, 1)

	// Update status
	resp4 := callREST(t, p, "PUT", "/certificates/"+certID+"?newStatus=INACTIVE", "UpdateCertificate", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify updated
	resp5 := callREST(t, p, "GET", "/certificates/"+certID, "DescribeCertificate", "")
	rb5 := parseBody(t, resp5)
	desc5, ok := rb5["certificateDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "INACTIVE", desc5["status"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/certificates/"+certID, "DeleteCertificate", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callREST(t, p, "GET", "/certificates/"+certID, "DescribeCertificate", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestTopicRuleCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "POST", "/rules/myRule", "CreateTopicRule",
		`{"topicRulePayload":{"sql":"SELECT * FROM 'topic'","actions":[]}}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get
	resp2 := callREST(t, p, "GET", "/rules/myRule", "GetTopicRule", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	assert.Equal(t, "myRule", rb2["ruleName"])

	// List
	resp3 := callREST(t, p, "GET", "/rules", "ListTopicRules", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	rules, ok := rb3["rules"].([]any)
	require.True(t, ok)
	assert.Len(t, rules, 1)

	// Disable
	resp4 := callREST(t, p, "POST", "/rules/myRule/disable", "DisableTopicRule", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify disabled
	resp5 := callREST(t, p, "GET", "/rules/myRule", "GetTopicRule", "")
	rb5 := parseBody(t, resp5)
	payload, ok := rb5["topicRulePayload"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, payload["ruleDisabled"])

	// Enable
	resp6 := callREST(t, p, "POST", "/rules/myRule/enable", "EnableTopicRule", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Replace
	resp7 := callREST(t, p, "PATCH", "/rules/myRule", "ReplaceTopicRule",
		`{"topicRulePayload":{"sql":"SELECT temperature FROM 'sensor'"}}`)
	assert.Equal(t, 200, resp7.StatusCode)

	// Verify replaced
	resp8 := callREST(t, p, "GET", "/rules/myRule", "GetTopicRule", "")
	rb8 := parseBody(t, resp8)
	payload8, ok := rb8["topicRulePayload"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "SELECT temperature FROM 'sensor'", payload8["sql"])

	// Delete
	resp9 := callREST(t, p, "DELETE", "/rules/myRule", "DeleteTopicRule", "")
	assert.Equal(t, 200, resp9.StatusCode)

	// Verify deleted
	resp10 := callREST(t, p, "GET", "/rules/myRule", "GetTopicRule", "")
	assert.Equal(t, 404, resp10.StatusCode)
}

func TestJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callREST(t, p, "PUT", "/jobs/job-001", "CreateJob",
		`{"jobId":"job-001","targets":["arn:aws:iot:us-east-1:000000000000:thing/myThing"],"document":"{}","description":"test job"}`)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.Equal(t, "job-001", rb["jobId"])

	// Describe
	resp2 := callREST(t, p, "GET", "/jobs/job-001", "DescribeJob", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	job, ok := rb2["job"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "job-001", job["jobId"])
	assert.Equal(t, "IN_PROGRESS", job["status"])

	// List
	resp3 := callREST(t, p, "GET", "/jobs", "ListJobs", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	jobs, ok := rb3["jobs"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs, 1)

	// Cancel
	resp4 := callREST(t, p, "PUT", "/jobs/job-001/cancel", "CancelJob",
		`{"jobId":"job-001"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify canceled
	resp5 := callREST(t, p, "GET", "/jobs/job-001", "DescribeJob", "")
	rb5 := parseBody(t, resp5)
	job5, ok := rb5["job"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CANCELED", job5["status"])

	// Delete
	resp6 := callREST(t, p, "DELETE", "/jobs/job-001", "DeleteJob", "")
	assert.Equal(t, 200, resp6.StatusCode)

	// Verify deleted
	resp7 := callREST(t, p, "GET", "/jobs/job-001", "DescribeJob", "")
	assert.Equal(t, 404, resp7.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create a thing to tag
	callREST(t, p, "POST", "/things/tagThing", "CreateThing", `{"thingName":"tagThing"}`)
	resp := callREST(t, p, "GET", "/things/tagThing", "DescribeThing", "")
	rb := parseBody(t, resp)
	arn := rb["thingArn"].(string)

	// Tag resource
	resp2 := callREST(t, p, "POST", "/tags?resourceArn="+arn, "TagResource",
		`{"tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"iot"}]}`)
	assert.Equal(t, 200, resp2.StatusCode)

	// List tags
	resp3 := callREST(t, p, "GET", "/tags?resourceArn="+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, resp3.StatusCode)
	rb3 := parseBody(t, resp3)
	tags, ok := rb3["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// Untag
	resp4 := callREST(t, p, "DELETE", "/tags?resourceArn="+arn+"&tagKeys=env", "UntagResource", "")
	assert.Equal(t, 200, resp4.StatusCode)

	// Verify untagged
	resp5 := callREST(t, p, "GET", "/tags?resourceArn="+arn, "ListTagsForResource", "")
	rb5 := parseBody(t, resp5)
	tags5, ok := rb5["tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags5, 1)
}

func TestDefaultHandler(t *testing.T) {
	p := newTestProvider(t)
	// Unknown ops should return 200 with empty JSON
	resp := callREST(t, p, "GET", "/some/unknown/path", "SomeUnknownOperation", "")
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	assert.NotNil(t, rb)
}
