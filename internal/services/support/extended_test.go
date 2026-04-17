// SPDX-License-Identifier: Apache-2.0

// internal/services/support/extended_test.go
package support

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDescribeServiceHealth(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "Support_20130415.DescribeServiceHealth", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	services := m["services"].([]any)
	assert.NotEmpty(t, services)
}

func TestDescribeTopicsAndPaths(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "Support_20130415.DescribeTopics", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	topics := parseJSON(t, resp)["topics"].([]any)
	assert.NotEmpty(t, topics)

	resp2 := callJSON(t, p, "Support_20130415.DescribeTopicPaths",
		`{"topicArn":"arn:aws:support:us-east-1::topic/1"}`)
	assert.Equal(t, 200, resp2.StatusCode)
}

func TestAttachmentLifecycle(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Support_20130415.AddAttachmentsToSet",
		`{"attachments":[{"fileName":"hello.txt","data":"aGk="}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	setID, _ := m["attachmentSetId"].(string)
	assert.NotEmpty(t, setID)
}

func TestTagNoops(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "Support_20130415.TagResource", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	resp2 := callJSON(t, p, "Support_20130415.UntagResource", `{}`)
	assert.Equal(t, 200, resp2.StatusCode)
	resp3 := callJSON(t, p, "Support_20130415.ListTagsForResource", `{}`)
	assert.Equal(t, 200, resp3.StatusCode)
}

func TestReopenAndEscalateCase(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "Support_20130415.CreateCase", `{"subject":"Reopen test"}`)
	caseID := parseJSON(t, resp)["caseId"].(string)

	reop := callJSON(t, p, "Support_20130415.ReopenCase", `{"caseId":"`+caseID+`"}`)
	assert.Equal(t, 200, reop.StatusCode)

	esc := callJSON(t, p, "Support_20130415.EscalateCase", `{"caseId":"`+caseID+`"}`)
	assert.Equal(t, 200, esc.StatusCode)
	assert.Equal(t, true, parseJSON(t, esc)["escalated"])
}
