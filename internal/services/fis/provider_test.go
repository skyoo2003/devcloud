// SPDX-License-Identifier: Apache-2.0

// internal/services/fis/provider_test.go
package fis

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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
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

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestExperimentTemplateCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	createBody := `{
		"description": "my template",
		"roleArn": "arn:aws:iam::000000000000:role/fis-role",
		"stopConditions": [{"source": "none"}],
		"actions": {},
		"targets": {}
	}`
	resp := callREST(t, p, "POST", "/experimentTemplates", "CreateExperimentTemplate", createBody)
	assert.Equal(t, 200, resp.StatusCode)
	rb := parseBody(t, resp)
	tmpl, ok := rb["experimentTemplate"].(map[string]any)
	require.True(t, ok, "expected experimentTemplate key")
	id, ok := tmpl["id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, id)
	arn, _ := tmpl["arn"].(string)
	assert.NotEmpty(t, arn)
	assert.Equal(t, "my template", tmpl["description"])

	// Get
	resp2 := callREST(t, p, "GET", "/experimentTemplates/"+id, "GetExperimentTemplate", "")
	assert.Equal(t, 200, resp2.StatusCode)
	rb2 := parseBody(t, resp2)
	tmpl2 := rb2["experimentTemplate"].(map[string]any)
	assert.Equal(t, id, tmpl2["id"])
	assert.Equal(t, "my template", tmpl2["description"])

	// Get non-existent
	resp3 := callREST(t, p, "GET", "/experimentTemplates/doesnotexist", "GetExperimentTemplate", "")
	assert.Equal(t, 404, resp3.StatusCode)

	// List
	callREST(t, p, "POST", "/experimentTemplates", "CreateExperimentTemplate",
		`{"description":"template2","roleArn":"arn:aws:iam::000000000000:role/r"}`)
	listResp := callREST(t, p, "GET", "/experimentTemplates", "ListExperimentTemplates", "")
	assert.Equal(t, 200, listResp.StatusCode)
	listBody := parseBody(t, listResp)
	templates, ok := listBody["experimentTemplates"].([]any)
	require.True(t, ok)
	assert.Len(t, templates, 2)

	// Update
	updateResp := callREST(t, p, "PATCH", "/experimentTemplates/"+id, "UpdateExperimentTemplate",
		`{"description":"updated description","roleArn":"arn:aws:iam::000000000000:role/new-role"}`)
	assert.Equal(t, 200, updateResp.StatusCode)
	updateBody := parseBody(t, updateResp)
	updTmpl := updateBody["experimentTemplate"].(map[string]any)
	assert.Equal(t, "updated description", updTmpl["description"])

	// Update non-existent
	updateResp2 := callREST(t, p, "PATCH", "/experimentTemplates/nonexistent", "UpdateExperimentTemplate", `{}`)
	assert.Equal(t, 404, updateResp2.StatusCode)

	// Delete
	deleteResp := callREST(t, p, "DELETE", "/experimentTemplates/"+id, "DeleteExperimentTemplate", "")
	assert.Equal(t, 200, deleteResp.StatusCode)
	deleteBody := parseBody(t, deleteResp)
	deletedTmpl := deleteBody["experimentTemplate"].(map[string]any)
	assert.Equal(t, id, deletedTmpl["id"])

	// Get after delete
	resp4 := callREST(t, p, "GET", "/experimentTemplates/"+id, "GetExperimentTemplate", "")
	assert.Equal(t, 404, resp4.StatusCode)

	// Delete non-existent
	resp5 := callREST(t, p, "DELETE", "/experimentTemplates/nonexistent", "DeleteExperimentTemplate", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestStartAndGetExperiment(t *testing.T) {
	p := newTestProvider(t)

	// Create template first
	createBody := `{"description":"tpl","roleArn":"arn:aws:iam::000000000000:role/r","stopConditions":[{"source":"none"}],"actions":{},"targets":{}}`
	cr := callREST(t, p, "POST", "/experimentTemplates", "CreateExperimentTemplate", createBody)
	assert.Equal(t, 200, cr.StatusCode)
	crb := parseBody(t, cr)
	tmpl := crb["experimentTemplate"].(map[string]any)
	templateID := tmpl["id"].(string)

	// Start experiment
	startBody := `{"experimentTemplateId":"` + templateID + `"}`
	sr := callREST(t, p, "POST", "/experiments", "StartExperiment", startBody)
	assert.Equal(t, 200, sr.StatusCode)
	srb := parseBody(t, sr)
	exp, ok := srb["experiment"].(map[string]any)
	require.True(t, ok)
	expID, ok := exp["id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, expID)
	assert.Equal(t, templateID, exp["experimentTemplateId"])
	state, ok := exp["state"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "completed", state["status"])

	// Get experiment
	gr := callREST(t, p, "GET", "/experiments/"+expID, "GetExperiment", "")
	assert.Equal(t, 200, gr.StatusCode)
	grb := parseBody(t, gr)
	exp2 := grb["experiment"].(map[string]any)
	assert.Equal(t, expID, exp2["id"])
	assert.Equal(t, templateID, exp2["experimentTemplateId"])

	// Get non-existent
	gr2 := callREST(t, p, "GET", "/experiments/nonexistent", "GetExperiment", "")
	assert.Equal(t, 404, gr2.StatusCode)

	// List experiments
	lr := callREST(t, p, "GET", "/experiments", "ListExperiments", "")
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	exps, ok := lrb["experiments"].([]any)
	require.True(t, ok)
	assert.Len(t, exps, 1)

	// List experiments by template
	lr2 := callREST(t, p, "GET", "/experiments?experimentTemplateId="+templateID, "ListExperiments", "")
	assert.Equal(t, 200, lr2.StatusCode)
	lrb2 := parseBody(t, lr2)
	exps2, ok := lrb2["experiments"].([]any)
	require.True(t, ok)
	assert.Len(t, exps2, 1)

	// Start with non-existent template
	sr2 := callREST(t, p, "POST", "/experiments", "StartExperiment",
		`{"experimentTemplateId":"nonexistent"}`)
	assert.Equal(t, 404, sr2.StatusCode)
}

func TestStopExperiment(t *testing.T) {
	p := newTestProvider(t)

	// Create template
	cr := callREST(t, p, "POST", "/experimentTemplates", "CreateExperimentTemplate",
		`{"description":"tpl","roleArn":"arn:aws:iam::000000000000:role/r","stopConditions":[{"source":"none"}]}`)
	crb := parseBody(t, cr)
	templateID := crb["experimentTemplate"].(map[string]any)["id"].(string)

	// Start experiment
	sr := callREST(t, p, "POST", "/experiments", "StartExperiment",
		`{"experimentTemplateId":"`+templateID+`"}`)
	srb := parseBody(t, sr)
	expID := srb["experiment"].(map[string]any)["id"].(string)

	// Stop experiment
	stopResp := callREST(t, p, "DELETE", "/experiments/"+expID, "StopExperiment", "")
	assert.Equal(t, 200, stopResp.StatusCode)
	stopBody := parseBody(t, stopResp)
	stoppedExp := stopBody["experiment"].(map[string]any)
	assert.Equal(t, expID, stoppedExp["id"])
	stoppedState := stoppedExp["state"].(map[string]any)
	assert.Equal(t, "stopped", stoppedState["status"])

	// Stop non-existent
	stopResp2 := callREST(t, p, "DELETE", "/experiments/nonexistent", "StopExperiment", "")
	assert.Equal(t, 404, stopResp2.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create template
	cr := callREST(t, p, "POST", "/experimentTemplates", "CreateExperimentTemplate",
		`{"description":"tpl","roleArn":"arn:aws:iam::000000000000:role/r","stopConditions":[{"source":"none"}],"tags":{"Env":"prod"}}`)
	assert.Equal(t, 200, cr.StatusCode)
	crb := parseBody(t, cr)
	tmpl := crb["experimentTemplate"].(map[string]any)
	arn := tmpl["arn"].(string)
	require.NotEmpty(t, arn)

	// ListTagsForResource
	lr := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	assert.Equal(t, 200, lr.StatusCode)
	lrb := parseBody(t, lr)
	tags, ok := lrb["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tags["Env"])

	// TagResource — add more tags
	tagBody, _ := json.Marshal(map[string]any{
		"tags": map[string]string{"Team": "platform", "Owner": "alice"},
	})
	tr := callREST(t, p, "POST", "/tags/"+arn, "TagResource", string(tagBody))
	assert.Equal(t, 200, tr.StatusCode)

	// Verify
	lr2 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	lrb2 := parseBody(t, lr2)
	tags2 := lrb2["tags"].(map[string]any)
	assert.Len(t, tags2, 3)
	assert.Equal(t, "prod", tags2["Env"])
	assert.Equal(t, "platform", tags2["Team"])

	// UntagResource
	req := httptest.NewRequest("DELETE", "/tags/"+arn+"?tagKeys=Env&tagKeys=Team", strings.NewReader(""))
	untagResp, err := p.HandleRequest(context.Background(), "UntagResource", req)
	require.NoError(t, err)
	assert.Equal(t, 200, untagResp.StatusCode)

	// Verify only Owner remains
	lr3 := callREST(t, p, "GET", "/tags/"+arn, "ListTagsForResource", "")
	lrb3 := parseBody(t, lr3)
	tags3 := lrb3["tags"].(map[string]any)
	assert.Len(t, tags3, 1)
	assert.Equal(t, "alice", tags3["Owner"])
}
