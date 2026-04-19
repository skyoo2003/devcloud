// SPDX-License-Identifier: Apache-2.0

// internal/services/transcribe/provider_test.go
package transcribe

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
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
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

func TestTranscriptionJobCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Start
	resp := callJSON(t, p, "Transcribe.StartTranscriptionJob",
		`{"TranscriptionJobName":"job1","LanguageCode":"en-US","Media":{"MediaFileUri":"s3://bucket/audio.mp4"},"MediaFormat":"mp4"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	job := m["TranscriptionJob"].(map[string]any)
	assert.Equal(t, "job1", job["TranscriptionJobName"])
	assert.Equal(t, "COMPLETED", job["TranscriptionJobStatus"])

	// Get (with transcript)
	resp2 := callJSON(t, p, "Transcribe.GetTranscriptionJob",
		`{"TranscriptionJobName":"job1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	job2 := m2["TranscriptionJob"].(map[string]any)
	assert.Equal(t, "job1", job2["TranscriptionJobName"])
	assert.Contains(t, job2, "Transcript")

	// List
	callJSON(t, p, "Transcribe.StartTranscriptionJob",
		`{"TranscriptionJobName":"job2","LanguageCode":"en-US"}`)
	resp3 := callJSON(t, p, "Transcribe.ListTranscriptionJobs", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	summaries := m3["TranscriptionJobSummaries"].([]any)
	assert.GreaterOrEqual(t, len(summaries), 2)

	// Delete
	resp4 := callJSON(t, p, "Transcribe.DeleteTranscriptionJob",
		`{"TranscriptionJobName":"job1"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Get after delete
	resp5 := callJSON(t, p, "Transcribe.GetTranscriptionJob",
		`{"TranscriptionJobName":"job1"}`)
	assert.Equal(t, 400, resp5.StatusCode)

	// Duplicate
	callJSON(t, p, "Transcribe.StartTranscriptionJob",
		`{"TranscriptionJobName":"job2","LanguageCode":"en-US"}`)
	resp6 := callJSON(t, p, "Transcribe.StartTranscriptionJob",
		`{"TranscriptionJobName":"job2","LanguageCode":"en-US"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestVocabularyCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "Transcribe.CreateVocabulary",
		`{"VocabularyName":"vocab1","LanguageCode":"en-US","Phrases":["hello","world"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "vocab1", m["VocabularyName"])
	assert.Equal(t, "READY", m["VocabularyState"])

	// Get
	resp2 := callJSON(t, p, "Transcribe.GetVocabulary",
		`{"VocabularyName":"vocab1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "vocab1", m2["VocabularyName"])
	assert.Equal(t, "en-US", m2["LanguageCode"])

	// List
	callJSON(t, p, "Transcribe.CreateVocabulary",
		`{"VocabularyName":"vocab2","LanguageCode":"en-US"}`)
	resp3 := callJSON(t, p, "Transcribe.ListVocabularies", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	vocabs := m3["Vocabularies"].([]any)
	assert.GreaterOrEqual(t, len(vocabs), 2)

	// Update
	resp4 := callJSON(t, p, "Transcribe.UpdateVocabulary",
		`{"VocabularyName":"vocab1","LanguageCode":"en-US","Phrases":["foo","bar"]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "vocab1", m4["VocabularyName"])

	// Delete
	resp5 := callJSON(t, p, "Transcribe.DeleteVocabulary",
		`{"VocabularyName":"vocab1"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete
	resp6 := callJSON(t, p, "Transcribe.GetVocabulary",
		`{"VocabularyName":"vocab1"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestVocabularyFilterCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "Transcribe.CreateVocabularyFilter",
		`{"VocabularyFilterName":"filter1","LanguageCode":"en-US","Words":["bad","word"]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "filter1", m["VocabularyFilterName"])
	assert.Equal(t, "en-US", m["LanguageCode"])

	// Get
	resp2 := callJSON(t, p, "Transcribe.GetVocabularyFilter",
		`{"VocabularyFilterName":"filter1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "filter1", m2["VocabularyFilterName"])

	// List
	callJSON(t, p, "Transcribe.CreateVocabularyFilter",
		`{"VocabularyFilterName":"filter2","LanguageCode":"en-US","Words":["test"]}`)
	resp3 := callJSON(t, p, "Transcribe.ListVocabularyFilters", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	filters := m3["VocabularyFilters"].([]any)
	assert.GreaterOrEqual(t, len(filters), 2)

	// Update
	resp4 := callJSON(t, p, "Transcribe.UpdateVocabularyFilter",
		`{"VocabularyFilterName":"filter1","Words":["updated"]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	assert.Equal(t, "filter1", m4["VocabularyFilterName"])

	// Delete
	resp5 := callJSON(t, p, "Transcribe.DeleteVocabularyFilter",
		`{"VocabularyFilterName":"filter1"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete
	resp6 := callJSON(t, p, "Transcribe.GetVocabularyFilter",
		`{"VocabularyFilterName":"filter1"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestLanguageModelCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "Transcribe.CreateLanguageModel",
		`{"ModelName":"model1","LanguageCode":"en-US","BaseModelName":"NarrowBand","InputDataConfig":{"S3Uri":"s3://bucket/","DataAccessRoleArn":"arn:aws:iam::000000000000:role/role"}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "model1", m["ModelName"])
	assert.Equal(t, "COMPLETED", m["ModelStatus"])

	// Describe
	resp2 := callJSON(t, p, "Transcribe.DescribeLanguageModel",
		`{"ModelName":"model1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	lm := m2["LanguageModel"].(map[string]any)
	assert.Equal(t, "model1", lm["ModelName"])
	assert.Equal(t, "NarrowBand", lm["BaseModelName"])

	// List
	callJSON(t, p, "Transcribe.CreateLanguageModel",
		`{"ModelName":"model2","LanguageCode":"en-US","BaseModelName":"WideBand","InputDataConfig":{"S3Uri":"s3://bucket/","DataAccessRoleArn":"arn:aws:iam::000000000000:role/role"}}`)
	resp3 := callJSON(t, p, "Transcribe.ListLanguageModels", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	models := m3["Models"].([]any)
	assert.GreaterOrEqual(t, len(models), 2)

	// Delete
	resp4 := callJSON(t, p, "Transcribe.DeleteLanguageModel",
		`{"ModelName":"model1"}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// Describe after delete
	resp5 := callJSON(t, p, "Transcribe.DescribeLanguageModel",
		`{"ModelName":"model1"}`)
	assert.Equal(t, 400, resp5.StatusCode)
}

func TestCallAnalyticsCategoryCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callJSON(t, p, "Transcribe.CreateCallAnalyticsCategory",
		`{"CategoryName":"cat1","InputType":"REAL_TIME","Rules":[]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	cat := m["CategoryProperties"].(map[string]any)
	assert.Equal(t, "cat1", cat["CategoryName"])
	assert.Equal(t, "REAL_TIME", cat["InputType"])

	// Get
	resp2 := callJSON(t, p, "Transcribe.GetCallAnalyticsCategory",
		`{"CategoryName":"cat1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	cat2 := m2["CategoryProperties"].(map[string]any)
	assert.Equal(t, "cat1", cat2["CategoryName"])

	// List
	callJSON(t, p, "Transcribe.CreateCallAnalyticsCategory",
		`{"CategoryName":"cat2","Rules":[]}`)
	resp3 := callJSON(t, p, "Transcribe.ListCallAnalyticsCategories", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	cats := m3["Categories"].([]any)
	assert.GreaterOrEqual(t, len(cats), 2)

	// Update
	resp4 := callJSON(t, p, "Transcribe.UpdateCallAnalyticsCategory",
		`{"CategoryName":"cat1","InputType":"POST_CALL","Rules":[]}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	cat4 := m4["CategoryProperties"].(map[string]any)
	assert.Equal(t, "POST_CALL", cat4["InputType"])

	// Delete
	resp5 := callJSON(t, p, "Transcribe.DeleteCallAnalyticsCategory",
		`{"CategoryName":"cat1"}`)
	assert.Equal(t, 200, resp5.StatusCode)

	// Get after delete
	resp6 := callJSON(t, p, "Transcribe.GetCallAnalyticsCategory",
		`{"CategoryName":"cat1"}`)
	assert.Equal(t, 400, resp6.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Start a transcription job to get an ARN
	callJSON(t, p, "Transcribe.StartTranscriptionJob",
		`{"TranscriptionJobName":"tagged-job","LanguageCode":"en-US"}`)

	arn := "arn:aws:transcribe:us-east-1:000000000000:transcription-job/tagged-job"

	// TagResource
	resp := callJSON(t, p, "Transcribe.TagResource",
		`{"ResourceArn":"`+arn+`","Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"ml"}]}`)
	assert.Equal(t, 200, resp.StatusCode)

	// ListTagsForResource
	resp2 := callJSON(t, p, "Transcribe.ListTagsForResource",
		`{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	tags, _ := m2["Tags"].([]any)
	assert.Len(t, tags, 2)

	// UntagResource
	resp3 := callJSON(t, p, "Transcribe.UntagResource",
		`{"ResourceArn":"`+arn+`","TagKeys":["env"]}`)
	assert.Equal(t, 200, resp3.StatusCode)

	// Verify tag removed
	resp4 := callJSON(t, p, "Transcribe.ListTagsForResource",
		`{"ResourceArn":"`+arn+`"}`)
	m4 := parseJSON(t, resp4)
	tags2, _ := m4["Tags"].([]any)
	assert.Len(t, tags2, 1)
	remaining := tags2[0].(map[string]any)
	assert.Equal(t, "team", remaining["Key"])
	assert.Equal(t, "ml", remaining["Value"])
}
