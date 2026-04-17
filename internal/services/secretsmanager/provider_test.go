// SPDX-License-Identifier: Apache-2.0

// internal/services/secretsmanager/provider_test.go
package secretsmanager

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

func call(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "secretsmanager."+target)
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

func TestSM_CreateSecret(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "CreateSecret", `{"Name":"my-secret","SecretString":"supersecret"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["ARN"].(string), "my-secret")
}

func TestSM_GetSecretValue(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"get-secret","SecretString":"hello"}`)
	resp := call(t, p, "GetSecretValue", `{"SecretId":"get-secret"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "hello", m["SecretString"])
}

func TestSM_PutSecretValue_Rotation(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"rotate-secret","SecretString":"v1"}`)
	call(t, p, "PutSecretValue", `{"SecretId":"rotate-secret","SecretString":"v2"}`)

	// AWSCURRENT should be v2.
	resp := call(t, p, "GetSecretValue", `{"SecretId":"rotate-secret"}`)
	m := parseJSON(t, resp)
	assert.Equal(t, "v2", m["SecretString"])

	// AWSPREVIOUS should be v1.
	prevResp := call(t, p, "GetSecretValue", `{"SecretId":"rotate-secret","VersionStage":"AWSPREVIOUS"}`)
	pm := parseJSON(t, prevResp)
	assert.Equal(t, "v1", pm["SecretString"])
}

func TestSM_DeleteSecret(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"del-secret","SecretString":"bye"}`)
	resp := call(t, p, "DeleteSecret", `{"SecretId":"del-secret"}`)
	assert.Equal(t, 200, resp.StatusCode)

	list := call(t, p, "ListSecrets", `{}`)
	lm := parseJSON(t, list)
	assert.Len(t, lm["SecretList"], 0)
}

func TestSM_ListSecrets(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"sec-a","SecretString":"a"}`)
	call(t, p, "CreateSecret", `{"Name":"sec-b","SecretString":"b"}`)
	resp := call(t, p, "ListSecrets", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Len(t, m["SecretList"], 2)
}

func TestSM_DescribeSecret(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"desc-secret","Description":"a test secret","SecretString":"x"}`)
	resp := call(t, p, "DescribeSecret", `{"SecretId":"desc-secret"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "a test secret", m["Description"])
}

func TestSM_UntagResource(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"untag-secret","SecretString":"x"}`)
	call(t, p, "TagResource", `{"SecretId":"untag-secret","Tags":[{"Key":"env","Value":"test"},{"Key":"app","Value":"myapp"}]}`)
	call(t, p, "UntagResource", `{"SecretId":"untag-secret","TagKeys":["env"]}`)
	resp := call(t, p, "DescribeSecret", `{"SecretId":"untag-secret"}`)
	m := parseJSON(t, resp)
	tags, _ := m["Tags"].([]any)
	for _, raw := range tags {
		t2 := raw.(map[string]any)
		assert.NotEqual(t, "env", t2["Key"])
	}
}

func TestSM_GetRandomPassword(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "GetRandomPassword", `{"PasswordLength":16}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	pw, _ := m["RandomPassword"].(string)
	assert.Len(t, pw, 16)
}

func TestSM_RotateSecret(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"rot-secret","SecretString":"initial"}`)
	resp := call(t, p, "RotateSecret", `{"SecretId":"rot-secret","RotationLambdaARN":"arn:aws:lambda:us-east-1:000000000000:function:rotator","RotationRules":{"AutomaticallyAfterDays":30}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["VersionId"])

	desc := call(t, p, "DescribeSecret", `{"SecretId":"rot-secret"}`)
	dm := parseJSON(t, desc)
	assert.Equal(t, true, dm["RotationEnabled"])

	cancelResp := call(t, p, "CancelRotateSecret", `{"SecretId":"rot-secret"}`)
	assert.Equal(t, 200, cancelResp.StatusCode)

	desc2 := call(t, p, "DescribeSecret", `{"SecretId":"rot-secret"}`)
	dm2 := parseJSON(t, desc2)
	assert.Equal(t, false, dm2["RotationEnabled"])
}

func TestSM_BatchGetSecretValue(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"batch-1","SecretString":"v1"}`)
	call(t, p, "CreateSecret", `{"Name":"batch-2","SecretString":"v2"}`)
	resp := call(t, p, "BatchGetSecretValue", `{"SecretIdList":["batch-1","batch-2"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	vals, _ := m["SecretValues"].([]any)
	assert.Len(t, vals, 2)
	found := map[string]string{}
	for _, raw := range vals {
		item := raw.(map[string]any)
		found[item["Name"].(string)] = item["SecretString"].(string)
	}
	assert.Equal(t, "v1", found["batch-1"])
	assert.Equal(t, "v2", found["batch-2"])
}

func TestSM_ListSecretVersionIds(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"ver-secret","SecretString":"v1"}`)
	call(t, p, "PutSecretValue", `{"SecretId":"ver-secret","SecretString":"v2"}`)
	resp := call(t, p, "ListSecretVersionIds", `{"SecretId":"ver-secret"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	versions, _ := m["Versions"].([]any)
	assert.GreaterOrEqual(t, len(versions), 2)
}

func TestSM_ResourcePolicy(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"rp-secret","SecretString":"s"}`)
	// Use properly escaped JSON string within the outer JSON.
	putResp := call(t, p, "PutResourcePolicy", `{"SecretId":"rp-secret","ResourcePolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`)
	assert.Equal(t, 200, putResp.StatusCode)

	getResp := call(t, p, "GetResourcePolicy", `{"SecretId":"rp-secret"}`)
	gm := parseJSON(t, getResp)
	assert.Contains(t, gm["ResourcePolicy"], "2012-10-17")

	delResp := call(t, p, "DeleteResourcePolicy", `{"SecretId":"rp-secret"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	valResp := call(t, p, "ValidateResourcePolicy", `{"ResourcePolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`)
	vm := parseJSON(t, valResp)
	assert.Equal(t, true, vm["PolicyValidationPassed"])
}

func TestSM_UpdateSecretVersionStage(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateSecret", `{"Name":"stage-secret","SecretString":"v1"}`)
	putResp := call(t, p, "PutSecretValue", `{"SecretId":"stage-secret","SecretString":"v2"}`)
	pm := parseJSON(t, putResp)
	v2ID := pm["VersionId"].(string)

	stageResp := call(t, p, "UpdateSecretVersionStage", `{"SecretId":"stage-secret","VersionStage":"AWSPREVIOUS","MoveToVersionId":"`+v2ID+`"}`)
	assert.Equal(t, 200, stageResp.StatusCode)
}
