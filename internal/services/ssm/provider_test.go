// SPDX-License-Identifier: Apache-2.0

// internal/services/ssm/provider_test.go
package ssm

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
	req.Header.Set("X-Amz-Target", "AmazonSSM."+target)
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

func TestSSM_PutAndGetParameter(t *testing.T) {
	p := newTestProvider(t)
	resp := call(t, p, "PutParameter", `{"Name":"/my/param","Value":"hello","Type":"String"}`)
	assert.Equal(t, 200, resp.StatusCode)

	getResp := call(t, p, "GetParameter", `{"Name":"/my/param"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	m := parseJSON(t, getResp)
	param := m["Parameter"].(map[string]any)
	assert.Equal(t, "hello", param["Value"])
	assert.Equal(t, "/my/param", param["Name"])
}

func TestSSM_PutParameter_NoOverwrite(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/dup/param","Value":"v1","Type":"String"}`)
	resp := call(t, p, "PutParameter", `{"Name":"/dup/param","Value":"v2","Type":"String","Overwrite":false}`)
	assert.Equal(t, 400, resp.StatusCode)
}

func TestSSM_PutParameter_Overwrite(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/ow/param","Value":"v1","Type":"String"}`)
	resp := call(t, p, "PutParameter", `{"Name":"/ow/param","Value":"v2","Type":"String","Overwrite":true}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, float64(2), m["Version"])
}

func TestSSM_GetParameters(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/a","Value":"1","Type":"String"}`)
	call(t, p, "PutParameter", `{"Name":"/b","Value":"2","Type":"String"}`)
	resp := call(t, p, "GetParameters", `{"Names":["/a","/b","/missing"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	params := m["Parameters"].([]any)
	assert.Len(t, params, 2)
	invalid := m["InvalidParameters"].([]any)
	assert.Len(t, invalid, 1)
	assert.Equal(t, "/missing", invalid[0])
}

func TestSSM_GetParametersByPath(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/app/db/host","Value":"localhost","Type":"String"}`)
	call(t, p, "PutParameter", `{"Name":"/app/db/port","Value":"5432","Type":"String"}`)
	call(t, p, "PutParameter", `{"Name":"/other/param","Value":"x","Type":"String"}`)

	resp := call(t, p, "GetParametersByPath", `{"Path":"/app/","Recursive":true}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	params := m["Parameters"].([]any)
	assert.Len(t, params, 2)
}

func TestSSM_DeleteParameter(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/del/param","Value":"bye","Type":"String"}`)
	resp := call(t, p, "DeleteParameter", `{"Name":"/del/param"}`)
	assert.Equal(t, 200, resp.StatusCode)

	getResp := call(t, p, "GetParameter", `{"Name":"/del/param"}`)
	assert.Equal(t, 400, getResp.StatusCode)
}

func TestSSM_DescribeParameters(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/desc/p1","Value":"a","Type":"String","Description":"first"}`)
	resp := call(t, p, "DescribeParameters", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	params := m["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/desc/p1", params[0].(map[string]any)["Name"])
}

func TestSSM_AddRemoveTags(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/tagged/p","Value":"v","Type":"String"}`)

	resp := call(t, p, "AddTagsToResource", `{"ResourceType":"Parameter","ResourceId":"/tagged/p","Tags":[{"Key":"env","Value":"test"}]}`)
	assert.Equal(t, 200, resp.StatusCode)

	listResp := call(t, p, "ListTagsForResource", `{"ResourceType":"Parameter","ResourceId":"/tagged/p"}`)
	m := parseJSON(t, listResp)
	tags := m["TagList"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["Key"])

	resp = call(t, p, "RemoveTagsFromResource", `{"ResourceType":"Parameter","ResourceId":"/tagged/p","TagKeys":["env"]}`)
	assert.Equal(t, 200, resp.StatusCode)

	listResp2 := call(t, p, "ListTagsForResource", `{"ResourceType":"Parameter","ResourceId":"/tagged/p"}`)
	m2 := parseJSON(t, listResp2)
	tags2 := m2["TagList"].([]any)
	assert.Len(t, tags2, 0)
}

func TestSSM_GetParameterHistory(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/hist/p","Value":"v1","Type":"String"}`)
	call(t, p, "PutParameter", `{"Name":"/hist/p","Value":"v2","Type":"String","Overwrite":true}`)
	call(t, p, "PutParameter", `{"Name":"/hist/p","Value":"v3","Type":"String","Overwrite":true}`)

	resp := call(t, p, "GetParameterHistory", `{"Name":"/hist/p"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	history := m["Parameters"].([]any)
	assert.Len(t, history, 3)
	assert.Equal(t, float64(1), history[0].(map[string]any)["Version"])
	assert.Equal(t, float64(3), history[2].(map[string]any)["Version"])
}

func TestSSM_LabelParameterVersion(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "PutParameter", `{"Name":"/lbl/p","Value":"v1","Type":"String"}`)

	resp := call(t, p, "LabelParameterVersion", `{"Name":"/lbl/p","Labels":["stable"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, float64(1), m["ParameterVersion"])
}

func TestSSM_DocumentLifecycle(t *testing.T) {
	p := newTestProvider(t)
	content := `{"schemaVersion":"2.2","description":"test","mainSteps":[]}`

	// Create
	resp := call(t, p, "CreateDocument", `{"Name":"MyDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// DescribeDocument
	descResp := call(t, p, "DescribeDocument", `{"Name":"MyDoc"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	m := parseJSON(t, descResp)
	doc := m["Document"].(map[string]any)
	assert.Equal(t, "MyDoc", doc["Name"])

	// GetDocument
	getResp := call(t, p, "GetDocument", `{"Name":"MyDoc"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	gm := parseJSON(t, getResp)
	assert.NotEmpty(t, gm["Content"])

	// ListDocuments
	listResp := call(t, p, "ListDocuments", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)
	lm := parseJSON(t, listResp)
	docs := lm["DocumentIdentifiers"].([]any)
	assert.Len(t, docs, 1)
	assert.Equal(t, "MyDoc", docs[0].(map[string]any)["Name"])

	// DeleteDocument
	_ = content
	delResp := call(t, p, "DeleteDocument", `{"Name":"MyDoc"}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// After delete, DescribeDocument should fail
	afterResp := call(t, p, "DescribeDocument", `{"Name":"MyDoc"}`)
	assert.Equal(t, 400, afterResp.StatusCode)
}

func TestSSM_UpdateDocument(t *testing.T) {
	p := newTestProvider(t)
	call(t, p, "CreateDocument", `{"Name":"UpdDoc","Content":"{\"schemaVersion\":\"2.2\"}","DocumentType":"Command"}`)

	resp := call(t, p, "UpdateDocument", `{"Name":"UpdDoc","Content":"{\"schemaVersion\":\"2.2\",\"mainSteps\":[]}","DocumentVersion":"$LATEST"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	doc := m["DocumentDescription"].(map[string]any)
	assert.Equal(t, "2", doc["DocumentVersion"])
}

func TestSSM_SessionStubs(t *testing.T) {
	p := newTestProvider(t)

	startResp := call(t, p, "StartSession", `{"Target":"i-1234567890abcdef0"}`)
	assert.Equal(t, 200, startResp.StatusCode)
	m := parseJSON(t, startResp)
	sessionID, ok := m["SessionId"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, sessionID)
	assert.NotEmpty(t, m["TokenValue"])

	termResp := call(t, p, "TerminateSession", `{"SessionId":"`+sessionID+`"}`)
	assert.Equal(t, 200, termResp.StatusCode)
}
