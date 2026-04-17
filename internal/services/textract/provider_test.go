// SPDX-License-Identifier: Apache-2.0

// internal/services/textract/provider_test.go
package textract

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

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
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

func TestAdapterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "Textract.CreateAdapter",
		`{"AdapterName":"my-adapter","FeatureTypes":["TABLES"],"AutoUpdate":"ENABLED"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	adapterID, _ := m["AdapterId"].(string)
	assert.NotEmpty(t, adapterID)

	// Get
	resp2 := callJSON(t, p, "Textract.GetAdapter",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, adapterID, m2["AdapterId"])
	assert.Equal(t, "my-adapter", m2["AdapterName"])
	assert.Equal(t, "ENABLED", m2["AutoUpdate"])

	// List
	resp3 := callJSON(t, p, "Textract.ListAdapters", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	adapters, _ := m3["Adapters"].([]any)
	assert.Len(t, adapters, 1)

	// Update
	resp4 := callJSON(t, p, "Textract.UpdateAdapter",
		`{"AdapterId":"`+adapterID+`","AutoUpdate":"DISABLED"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "DISABLED", m4["AutoUpdate"])

	// CreateAdapterVersion
	resp5 := callJSON(t, p, "Textract.CreateAdapterVersion",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	version, _ := m5["AdapterVersion"].(string)
	assert.NotEmpty(t, version)

	// GetAdapterVersion
	resp6 := callJSON(t, p, "Textract.GetAdapterVersion",
		`{"AdapterId":"`+adapterID+`","AdapterVersion":"`+version+`"}`)
	assert.Equal(t, 200, resp6.StatusCode)
	m6 := parseJSON(t, resp6)
	assert.Equal(t, version, m6["AdapterVersion"])
	assert.Equal(t, "ACTIVE", m6["Status"])

	// ListAdapterVersions
	resp7 := callJSON(t, p, "Textract.ListAdapterVersions",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, resp7.StatusCode)
	m7 := parseJSON(t, resp7)
	versions, _ := m7["AdapterVersions"].([]any)
	assert.Len(t, versions, 1)

	// DeleteAdapterVersion
	resp8 := callJSON(t, p, "Textract.DeleteAdapterVersion",
		`{"AdapterId":"`+adapterID+`","AdapterVersion":"`+version+`"}`)
	assert.Equal(t, 200, resp8.StatusCode)

	// Delete
	resp9 := callJSON(t, p, "Textract.DeleteAdapter",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 200, resp9.StatusCode)

	// Get after delete
	resp10 := callJSON(t, p, "Textract.GetAdapter",
		`{"AdapterId":"`+adapterID+`"}`)
	assert.Equal(t, 400, resp10.StatusCode)
}

func TestAnalyzeDocument(t *testing.T) {
	p := newTestProvider(t)

	// AnalyzeDocument
	resp := callJSON(t, p, "Textract.AnalyzeDocument",
		`{"Document":{"Bytes":"dGVzdA=="},"FeatureTypes":["TABLES"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	blocks, _ := m["Blocks"].([]any)
	assert.NotEmpty(t, blocks)
	meta, _ := m["DocumentMetadata"].(map[string]any)
	assert.Equal(t, float64(1), meta["Pages"])

	// AnalyzeExpense
	resp2 := callJSON(t, p, "Textract.AnalyzeExpense",
		`{"Document":{"Bytes":"dGVzdA=="}}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Contains(t, m2, "ExpenseDocuments")

	// AnalyzeID
	resp3 := callJSON(t, p, "Textract.AnalyzeID",
		`{"DocumentPages":[{"Bytes":"dGVzdA=="}]}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	assert.Contains(t, m3, "IdentityDocuments")

	// DetectDocumentText
	resp4 := callJSON(t, p, "Textract.DetectDocumentText",
		`{"Document":{"Bytes":"dGVzdA=="}}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	blocks4, _ := m4["Blocks"].([]any)
	assert.NotEmpty(t, blocks4)
}

func TestAsyncJobFlow(t *testing.T) {
	p := newTestProvider(t)

	// StartDocumentAnalysis
	resp := callJSON(t, p, "Textract.StartDocumentAnalysis",
		`{"DocumentLocation":{"S3Object":{"Bucket":"b","Name":"f.pdf"}}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	jobID, _ := m["JobId"].(string)
	assert.NotEmpty(t, jobID)

	// GetDocumentAnalysis
	resp2 := callJSON(t, p, "Textract.GetDocumentAnalysis",
		`{"JobId":"`+jobID+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "SUCCEEDED", m2["JobStatus"])

	// StartDocumentTextDetection
	resp3 := callJSON(t, p, "Textract.StartDocumentTextDetection",
		`{"DocumentLocation":{"S3Object":{"Bucket":"b","Name":"f.pdf"}}}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	jobID3, _ := m3["JobId"].(string)
	assert.NotEmpty(t, jobID3)

	// GetDocumentTextDetection
	resp4 := callJSON(t, p, "Textract.GetDocumentTextDetection",
		`{"JobId":"`+jobID3+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "SUCCEEDED", m4["JobStatus"])

	// StartExpenseAnalysis
	resp5 := callJSON(t, p, "Textract.StartExpenseAnalysis",
		`{"DocumentLocation":{"S3Object":{"Bucket":"b","Name":"f.pdf"}}}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	jobID5, _ := m5["JobId"].(string)

	// GetExpenseAnalysis
	resp6 := callJSON(t, p, "Textract.GetExpenseAnalysis",
		`{"JobId":"`+jobID5+`"}`)
	assert.Equal(t, 200, resp6.StatusCode)
	m6 := parseJSON(t, resp6)
	assert.Equal(t, "SUCCEEDED", m6["JobStatus"])

	// StartLendingAnalysis
	resp7 := callJSON(t, p, "Textract.StartLendingAnalysis",
		`{"DocumentLocation":{"S3Object":{"Bucket":"b","Name":"f.pdf"}}}`)
	assert.Equal(t, 200, resp7.StatusCode)
	m7 := parseJSON(t, resp7)
	jobID7, _ := m7["JobId"].(string)

	// GetLendingAnalysis
	resp8 := callJSON(t, p, "Textract.GetLendingAnalysis",
		`{"JobId":"`+jobID7+`"}`)
	assert.Equal(t, 200, resp8.StatusCode)
	m8 := parseJSON(t, resp8)
	assert.Equal(t, "SUCCEEDED", m8["JobStatus"])

	// GetLendingAnalysisSummary
	resp9 := callJSON(t, p, "Textract.GetLendingAnalysisSummary",
		`{"JobId":"`+jobID7+`"}`)
	assert.Equal(t, 200, resp9.StatusCode)
	m9 := parseJSON(t, resp9)
	assert.Equal(t, "SUCCEEDED", m9["JobStatus"])

	// Unknown job
	resp10 := callJSON(t, p, "Textract.GetDocumentAnalysis",
		`{"JobId":"nonexistent-id"}`)
	assert.Equal(t, 400, resp10.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create adapter
	resp := callJSON(t, p, "Textract.CreateAdapter",
		`{"AdapterName":"tagged-adapter"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	adapterID, _ := m["AdapterId"].(string)

	// Get ARN
	resp2 := callJSON(t, p, "Textract.GetAdapter",
		`{"AdapterId":"`+adapterID+`"}`)
	m2 := parseJSON(t, resp2)
	arn, _ := m2["AdapterArn"].(string)
	assert.NotEmpty(t, arn)

	// TagResource
	resp3 := callJSON(t, p, "Textract.TagResource",
		`{"ResourceARN":"`+arn+`","Tags":{"env":"prod","team":"ml"}}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// ListTagsForResource
	resp4 := callJSON(t, p, "Textract.ListTagsForResource",
		`{"ResourceARN":"`+arn+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	tags, _ := m4["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "ml", tags["team"])

	// UntagResource
	resp5 := callJSON(t, p, "Textract.UntagResource",
		`{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Verify tag removed
	resp6 := callJSON(t, p, "Textract.ListTagsForResource",
		`{"ResourceARN":"`+arn+`"}`)
	m6 := parseJSON(t, resp6)
	tags2, _ := m6["Tags"].(map[string]any)
	assert.NotContains(t, tags2, "env")
	assert.Equal(t, "ml", tags2["team"])
}
