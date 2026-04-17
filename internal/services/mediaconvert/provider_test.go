// SPDX-License-Identifier: Apache-2.0

// internal/services/mediaconvert/provider_test.go
package mediaconvert

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *MediaConvertProvider {
	t.Helper()
	dir := t.TempDir()
	p := &MediaConvertProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func mcReq(t *testing.T, p *MediaConvertProvider, method, path string, body map[string]any) (map[string]any, int) {
	t.Helper()
	var rawBody []byte
	if body != nil {
		rawBody, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal(resp.Body, &result)
	return result, resp.StatusCode
}

func TestJobTemplateLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Create job template
	res, status := mcReq(t, p, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name":        "my-template",
		"description": "test template",
		"settings":    map[string]any{"outputGroups": []any{}},
	})
	assert.Equal(t, http.StatusCreated, status)
	tmpl, ok := res["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-template", tmpl["name"])

	// Get job template
	res2, status2 := mcReq(t, p, http.MethodGet, "/2017-08-29/jobTemplates/my-template", nil)
	assert.Equal(t, http.StatusOK, status2)
	tmpl2, ok := res2["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-template", tmpl2["name"])
	assert.Equal(t, "test template", tmpl2["description"])

	// List job templates
	res3, status3 := mcReq(t, p, http.MethodGet, "/2017-08-29/jobTemplates", nil)
	assert.Equal(t, http.StatusOK, status3)
	templates, ok := res3["jobTemplates"].([]any)
	require.True(t, ok)
	assert.Len(t, templates, 1)

	// Delete job template
	_, status4 := mcReq(t, p, http.MethodDelete, "/2017-08-29/jobTemplates/my-template", nil)
	assert.Equal(t, http.StatusAccepted, status4)

	// Verify deletion
	_, status5 := mcReq(t, p, http.MethodGet, "/2017-08-29/jobTemplates/my-template", nil)
	assert.Equal(t, http.StatusNotFound, status5)
}

func TestQueueLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Create queue
	res, status := mcReq(t, p, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name":        "my-queue",
		"description": "test queue",
		"pricingPlan": "ON_DEMAND",
	})
	assert.Equal(t, http.StatusCreated, status)
	q, ok := res["queue"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-queue", q["name"])
	assert.Equal(t, "ACTIVE", q["status"])
	assert.Equal(t, "ON_DEMAND", q["pricingPlan"])

	// Get queue
	res2, status2 := mcReq(t, p, http.MethodGet, "/2017-08-29/queues/my-queue", nil)
	assert.Equal(t, http.StatusOK, status2)
	q2, ok := res2["queue"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-queue", q2["name"])

	// List queues
	res3, status3 := mcReq(t, p, http.MethodGet, "/2017-08-29/queues", nil)
	assert.Equal(t, http.StatusOK, status3)
	queues, ok := res3["queues"].([]any)
	require.True(t, ok)
	assert.Len(t, queues, 1)

	// Delete queue
	_, status4 := mcReq(t, p, http.MethodDelete, "/2017-08-29/queues/my-queue", nil)
	assert.Equal(t, http.StatusAccepted, status4)

	// Verify deletion
	_, status5 := mcReq(t, p, http.MethodGet, "/2017-08-29/queues/my-queue", nil)
	assert.Equal(t, http.StatusNotFound, status5)
}

func TestJobLifecycle(t *testing.T) {
	p := newTestProvider(t)

	// Create job
	res, status := mcReq(t, p, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"settings": map[string]any{"outputGroups": []any{}},
		"queue":    "arn:aws:mediaconvert:us-east-1:000000000000:queues/Default",
	})
	assert.Equal(t, http.StatusCreated, status)
	job, ok := res["job"].(map[string]any)
	require.True(t, ok)
	jobID, _ := job["id"].(string)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "SUBMITTED", job["status"])

	// Get job
	res2, status2 := mcReq(t, p, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, status2)
	job2, ok := res2["job"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, jobID, job2["id"])

	// List jobs
	res3, status3 := mcReq(t, p, http.MethodGet, "/2017-08-29/jobs", nil)
	assert.Equal(t, http.StatusOK, status3)
	jobs, ok := res3["jobs"].([]any)
	require.True(t, ok)
	assert.Len(t, jobs, 1)
}

func TestListPresets(t *testing.T) {
	p := newTestProvider(t)
	res, status := mcReq(t, p, http.MethodGet, "/2017-08-29/presets", nil)
	assert.Equal(t, http.StatusOK, status)
	presets, ok := res["presets"].([]any)
	require.True(t, ok)
	assert.Len(t, presets, 0)
}
