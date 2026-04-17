// SPDX-License-Identifier: Apache-2.0

// internal/services/textract/extended_test.go
package textract

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartAdapterTrainingAndStop(t *testing.T) {
	p := newTestProvider(t)

	// Create adapter first
	resp := callJSON(t, p, "Textract.CreateAdapter",
		`{"AdapterName":"train-adapter","FeatureTypes":["TABLES"]}`)
	require.Equal(t, 200, resp.StatusCode)
	adapterID := parseJSON(t, resp)["AdapterId"].(string)

	// Start training
	train := callJSON(t, p, "Textract.StartAdapterTraining",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, train.StatusCode)
	tm := parseJSON(t, train)
	assert.Equal(t, "TRAINING", tm["Status"])

	// Stop training
	stop := callJSON(t, p, "Textract.StopAdapterTraining",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, stop.StatusCode)
}

func TestLabelAndEntitiesJobs(t *testing.T) {
	p := newTestProvider(t)

	// Start label detection
	resp := callJSON(t, p, "Textract.StartLabelDetection", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	jobID := parseJSON(t, resp)["JobId"].(string)
	assert.NotEmpty(t, jobID)

	// Get label results
	res := callJSON(t, p, "Textract.GetLabelDetectionJobResults",
		`{"JobId":"`+jobID+`"}`)
	assert.Equal(t, 200, res.StatusCode)
	rm := parseJSON(t, res)
	assert.Equal(t, "SUCCEEDED", rm["JobStatus"])

	// Start entities detection
	eresp := callJSON(t, p, "Textract.StartEntitiesDetection", `{}`)
	assert.Equal(t, 200, eresp.StatusCode)
	ejobID := parseJSON(t, eresp)["JobId"].(string)

	// Get entities results
	eres := callJSON(t, p, "Textract.GetEntitiesDetectionJobResults",
		`{"JobId":"`+ejobID+`"}`)
	assert.Equal(t, 200, eres.StatusCode)
}

func TestCancelAndListJobs(t *testing.T) {
	p := newTestProvider(t)
	c := callJSON(t, p, "Textract.CancelJob", `{"JobId":"job-123"}`)
	assert.Equal(t, 200, c.StatusCode)

	l := callJSON(t, p, "Textract.ListJobs", `{}`)
	assert.Equal(t, 200, l.StatusCode)
}
