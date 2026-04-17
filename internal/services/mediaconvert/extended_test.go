// SPDX-License-Identifier: Apache-2.0

// internal/services/mediaconvert/extended_test.go
package mediaconvert

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPresetLifecycle(t *testing.T) {
	p := newTestProvider(t)

	res, status := mcReq(t, p, http.MethodPost, "/2017-08-29/presets", map[string]any{
		"name":        "hd-preset",
		"description": "high def",
		"settings":    map[string]any{"video": "h264"},
	})
	assert.Equal(t, http.StatusCreated, status)
	preset, ok := res["preset"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hd-preset", preset["name"])

	// Get
	res2, s2 := mcReq(t, p, http.MethodGet, "/2017-08-29/presets/hd-preset", nil)
	assert.Equal(t, http.StatusOK, s2)
	p2 := res2["preset"].(map[string]any)
	assert.Equal(t, "hd-preset", p2["name"])

	// List
	res3, s3 := mcReq(t, p, http.MethodGet, "/2017-08-29/presets", nil)
	assert.Equal(t, http.StatusOK, s3)
	lst := res3["presets"].([]any)
	assert.Len(t, lst, 1)

	// Update
	_, s4 := mcReq(t, p, http.MethodPut, "/2017-08-29/presets/hd-preset", map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, s4)

	// Delete
	_, s5 := mcReq(t, p, http.MethodDelete, "/2017-08-29/presets/hd-preset", nil)
	assert.Equal(t, http.StatusAccepted, s5)
}

func TestCancelJob(t *testing.T) {
	p := newTestProvider(t)

	// Create job
	res, _ := mcReq(t, p, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"settings": map[string]any{},
		"queue":    "default",
	})
	job := res["job"].(map[string]any)
	id := job["id"].(string)

	// Cancel
	_, status := mcReq(t, p, http.MethodDelete, "/2017-08-29/jobs/"+id, nil)
	assert.Equal(t, http.StatusAccepted, status)

	// Get job shows CANCELED
	res2, _ := mcReq(t, p, http.MethodGet, "/2017-08-29/jobs/"+id, nil)
	job2 := res2["job"].(map[string]any)
	assert.Equal(t, "CANCELED", job2["status"])
}

func TestEndpointsAndVersionsAndPolicy(t *testing.T) {
	p := newTestProvider(t)

	res, status := mcReq(t, p, http.MethodGet, "/2017-08-29/endpoints", nil)
	assert.Equal(t, http.StatusOK, status)
	endpoints := res["endpoints"].([]any)
	assert.NotEmpty(t, endpoints)

	res2, status2 := mcReq(t, p, http.MethodGet, "/2017-08-29/versions", nil)
	assert.Equal(t, http.StatusOK, status2)
	assert.NotEmpty(t, res2["versions"])

	// PutPolicy and GetPolicy
	_, ps := mcReq(t, p, http.MethodPut, "/2017-08-29/policy", map[string]any{
		"policy": map[string]any{"HttpInputs": "ALLOWED"},
	})
	assert.Equal(t, http.StatusOK, ps)

	gres, gs := mcReq(t, p, http.MethodGet, "/2017-08-29/policy", nil)
	assert.Equal(t, http.StatusOK, gs)
	pol := gres["policy"].(map[string]any)
	assert.Equal(t, "ALLOWED", pol["HttpInputs"])

	_, ds := mcReq(t, p, http.MethodDelete, "/2017-08-29/policy", nil)
	assert.Equal(t, http.StatusOK, ds)
}
