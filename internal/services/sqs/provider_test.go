// SPDX-License-Identifier: Apache-2.0

package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSQSProvider(t *testing.T) *SQSProvider {
	t.Helper()
	p := &SQSProvider{}
	err := p.Init(plugin.PluginConfig{})
	require.NoError(t, err)
	return p
}

func formRequest(body string) *httptest.ResponseRecorder {
	return httptest.NewRecorder()
}

// makeFormReq builds a POST request with the given form-encoded body.
func makeFormReq(body string) *httptest.ResponseRecorder {
	_ = body
	return httptest.NewRecorder()
}

// handleForm is a helper that sends a form-encoded body through HandleRequest and
// returns the response body string.
func handleForm(t *testing.T, p *SQSProvider, formBody string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(formBody))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestSQSProvider_CreateQueue(t *testing.T) {
	p := newTestSQSProvider(t)

	resp := handleForm(t, p, "Action=CreateQueue&QueueName=test-queue")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "test-queue")
	assert.Contains(t, string(resp.Body), "CreateQueueResponse")
	assert.Contains(t, string(resp.Body), "QueueUrl")
}

func TestSQSProvider_CreateQueue_Duplicate(t *testing.T) {
	p := newTestSQSProvider(t)

	resp := handleForm(t, p, "Action=CreateQueue&QueueName=dup-queue")
	assert.Equal(t, 200, resp.StatusCode)

	// Creating a duplicate should still succeed (idempotent per AWS spec).
	resp2 := handleForm(t, p, "Action=CreateQueue&QueueName=dup-queue")
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Contains(t, string(resp2.Body), "dup-queue")
}

func TestSQSProvider_GetQueueUrl(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=url-queue")

	resp := handleForm(t, p, "Action=GetQueueUrl&QueueName=url-queue")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "url-queue")
	assert.Contains(t, string(resp.Body), "GetQueueUrlResponse")
}

func TestSQSProvider_GetQueueUrl_NotFound(t *testing.T) {
	p := newTestSQSProvider(t)

	resp := handleForm(t, p, "Action=GetQueueUrl&QueueName=no-such-queue")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NonExistentQueue")
}

func TestSQSProvider_ListQueues(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=alpha-queue")
	handleForm(t, p, "Action=CreateQueue&QueueName=beta-queue")

	resp := handleForm(t, p, "Action=ListQueues")
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "alpha-queue")
	assert.Contains(t, body, "beta-queue")
	assert.Contains(t, body, "ListQueuesResponse")
}

func TestSQSProvider_ListQueues_WithPrefix(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=prefix-queue-a")
	handleForm(t, p, "Action=CreateQueue&QueueName=prefix-queue-b")
	handleForm(t, p, "Action=CreateQueue&QueueName=other-queue")

	resp := handleForm(t, p, "Action=ListQueues&QueueNamePrefix=prefix-")
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "prefix-queue-a")
	assert.Contains(t, body, "prefix-queue-b")
	assert.NotContains(t, body, "other-queue")
}

func TestSQSProvider_SendAndReceive(t *testing.T) {
	p := newTestSQSProvider(t)

	// Create queue first.
	createResp := handleForm(t, p, "Action=CreateQueue&QueueName=send-recv-queue")
	require.Equal(t, 200, createResp.StatusCode)

	queueURL := "http://localhost:4747/000000000000/send-recv-queue"

	// Send a message.
	sendResp := handleForm(t, p,
		"Action=SendMessage&QueueUrl="+queueURL+"&MessageBody=hello+world")
	assert.Equal(t, 200, sendResp.StatusCode)
	sendBody := string(sendResp.Body)
	assert.Contains(t, sendBody, "SendMessageResponse")
	assert.Contains(t, sendBody, "MessageId")
	assert.Contains(t, sendBody, "MD5OfMessageBody")

	// Receive the message.
	recvResp := handleForm(t, p,
		"Action=ReceiveMessage&QueueUrl="+queueURL+"&MaxNumberOfMessages=1")
	assert.Equal(t, 200, recvResp.StatusCode)
	recvBody := string(recvResp.Body)
	assert.Contains(t, recvBody, "ReceiveMessageResponse")
	assert.Contains(t, recvBody, "hello world")
	assert.Contains(t, recvBody, "ReceiptHandle")
}

func TestSQSProvider_DeleteMessage(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=del-msg-queue")
	queueURL := "http://localhost:4747/000000000000/del-msg-queue"

	// Send a message.
	handleForm(t, p, "Action=SendMessage&QueueUrl="+queueURL+"&MessageBody=to+delete")

	// Receive to get receipt handle (use visibility timeout 0 so re-receive works immediately).
	recvResp := handleForm(t, p,
		"Action=ReceiveMessage&QueueUrl="+queueURL+"&MaxNumberOfMessages=1&VisibilityTimeout=0")
	require.Equal(t, 200, recvResp.StatusCode)

	// Extract receipt handle from XML response.
	recvBody := string(recvResp.Body)
	start := strings.Index(recvBody, "<ReceiptHandle>")
	end := strings.Index(recvBody, "</ReceiptHandle>")
	require.True(t, start >= 0 && end > start, "expected ReceiptHandle in response")
	receiptHandle := recvBody[start+len("<ReceiptHandle>") : end]

	// Delete the message.
	delResp := handleForm(t, p,
		"Action=DeleteMessage&QueueUrl="+queueURL+"&ReceiptHandle="+receiptHandle)
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteMessageResponse")

	// Re-receive with VisibilityTimeout=0 — message should be gone.
	recvResp2 := handleForm(t, p,
		"Action=ReceiveMessage&QueueUrl="+queueURL+"&MaxNumberOfMessages=1&VisibilityTimeout=0")
	assert.Equal(t, 200, recvResp2.StatusCode)
	assert.NotContains(t, string(recvResp2.Body), "ReceiptHandle")
}

func TestSQSProvider_DeleteQueue(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=gone-queue")
	queueURL := "http://localhost:4747/000000000000/gone-queue"

	delResp := handleForm(t, p, "Action=DeleteQueue&QueueUrl="+queueURL)
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteQueueResponse")

	// Queue should no longer appear in list.
	listResp := handleForm(t, p, "Action=ListQueues")
	assert.Equal(t, 200, listResp.StatusCode)
	assert.NotContains(t, string(listResp.Body), "gone-queue")
}

func TestSQSProvider_UnknownAction(t *testing.T) {
	p := newTestSQSProvider(t)

	resp := handleForm(t, p, "Action=UnknownAction")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "InvalidAction")
}

func TestSQSProvider_ActionFromOp(t *testing.T) {
	p := newTestSQSProvider(t)

	// When op is provided by the gateway, it should be used instead of the form field.
	req := httptest.NewRequest("POST", "/",
		strings.NewReader("QueueName=op-queue"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "CreateQueue", req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "op-queue")
}

func TestSQSProvider_ListResources(t *testing.T) {
	p := newTestSQSProvider(t)

	handleForm(t, p, "Action=CreateQueue&QueueName=resource-queue")

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	require.Len(t, resources, 1)
	assert.Equal(t, "resource-queue", resources[0].Name)
	assert.Equal(t, "queue", resources[0].Type)
}

// --- JSON protocol helpers ---

// handleJSON sends a JSON request through HandleRequest and returns the response.
func handleJSON(t *testing.T, p *SQSProvider, action string, params map[string]any) *plugin.Response {
	t.Helper()
	body, err := json.Marshal(params)
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

// jsonBody unmarshals the response body into a map.
func jsonBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestSQSProvider_JSON_SendMessageBatch(t *testing.T) {
	p := newTestSQSProvider(t)

	handleJSON(t, p, "CreateQueue", map[string]any{"QueueName": "batch-q"})
	queueURL := "http://localhost:4747/000000000000/batch-q"

	resp := handleJSON(t, p, "SendMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries": []map[string]any{
			{"Id": "1", "MessageBody": "msg1"},
			{"Id": "2", "MessageBody": "msg2"},
		},
	})
	assert.Equal(t, 200, resp.StatusCode)
	m := jsonBody(t, resp)
	successful, ok := m["Successful"].([]any)
	require.True(t, ok)
	assert.Len(t, successful, 2)
}

func TestSQSProvider_JSON_DeleteMessageBatch(t *testing.T) {
	p := newTestSQSProvider(t)

	handleJSON(t, p, "CreateQueue", map[string]any{"QueueName": "delbatch-q"})
	queueURL := "http://localhost:4747/000000000000/delbatch-q"

	handleJSON(t, p, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "a"})
	handleJSON(t, p, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "b"})

	recvResp := handleJSON(t, p, "ReceiveMessage", map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 2,
		"VisibilityTimeout":   0,
	})
	recvBody := jsonBody(t, recvResp)
	msgs, ok := recvBody["Messages"].([]any)
	require.True(t, ok)
	require.Len(t, msgs, 2)

	entries := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		msgMap := m.(map[string]any)
		entries[i] = map[string]any{
			"Id":            fmt.Sprintf("%d", i),
			"ReceiptHandle": msgMap["ReceiptHandle"],
		}
	}

	resp := handleJSON(t, p, "DeleteMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	assert.Equal(t, 200, resp.StatusCode)
	m2 := jsonBody(t, resp)
	successful, ok := m2["Successful"].([]any)
	require.True(t, ok)
	assert.Len(t, successful, 2)
}

func TestSQSProvider_JSON_ChangeMessageVisibility(t *testing.T) {
	p := newTestSQSProvider(t)

	handleJSON(t, p, "CreateQueue", map[string]any{"QueueName": "vis-q"})
	queueURL := "http://localhost:4747/000000000000/vis-q"

	handleJSON(t, p, "SendMessage", map[string]any{"QueueUrl": queueURL, "MessageBody": "test"})

	recvResp := handleJSON(t, p, "ReceiveMessage", map[string]any{
		"QueueUrl":          queueURL,
		"VisibilityTimeout": 30,
	})
	recvBody := jsonBody(t, recvResp)
	msgs := recvBody["Messages"].([]any)
	require.Len(t, msgs, 1)
	rh := msgs[0].(map[string]any)["ReceiptHandle"].(string)

	resp := handleJSON(t, p, "ChangeMessageVisibility", map[string]any{
		"QueueUrl":          queueURL,
		"ReceiptHandle":     rh,
		"VisibilityTimeout": 0,
	})
	assert.Equal(t, 200, resp.StatusCode)

	recvResp2 := handleJSON(t, p, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})
	recvBody2 := jsonBody(t, recvResp2)
	msgs2 := recvBody2["Messages"].([]any)
	assert.GreaterOrEqual(t, len(msgs2), 1)
}

func TestSQSProvider_JSON_QueueTags(t *testing.T) {
	p := newTestSQSProvider(t)

	handleJSON(t, p, "CreateQueue", map[string]any{"QueueName": "tag-q"})
	queueURL := "http://localhost:4747/000000000000/tag-q"

	resp := handleJSON(t, p, "TagQueue", map[string]any{
		"QueueUrl": queueURL,
		"Tags":     map[string]any{"env": "test"},
	})
	assert.Equal(t, 200, resp.StatusCode)

	listResp := handleJSON(t, p, "ListQueueTags", map[string]any{"QueueUrl": queueURL})
	assert.Equal(t, 200, listResp.StatusCode)
	m := jsonBody(t, listResp)
	tags := m["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])

	untagResp := handleJSON(t, p, "UntagQueue", map[string]any{
		"QueueUrl": queueURL,
		"TagKeys":  []any{"env"},
	})
	assert.Equal(t, 200, untagResp.StatusCode)

	listResp2 := handleJSON(t, p, "ListQueueTags", map[string]any{"QueueUrl": queueURL})
	m2 := jsonBody(t, listResp2)
	tags2 := m2["Tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
}

func TestSQSProvider_JSON_FIFOQueue(t *testing.T) {
	p := newTestSQSProvider(t)

	resp := handleJSON(t, p, "CreateQueue", map[string]any{
		"QueueName": "test.fifo",
		"Attributes": map[string]any{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	assert.Equal(t, 200, resp.StatusCode)
	queueURL := "http://localhost:4747/000000000000/test.fifo"

	sendResp := handleJSON(t, p, "SendMessage", map[string]any{
		"QueueUrl":       queueURL,
		"MessageBody":    "fifo-msg",
		"MessageGroupId": "group1",
	})
	assert.Equal(t, 200, sendResp.StatusCode)

	recvResp := handleJSON(t, p, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})
	recvBody := jsonBody(t, recvResp)
	msgs := recvBody["Messages"].([]any)
	assert.Len(t, msgs, 1)
	assert.Equal(t, "fifo-msg", msgs[0].(map[string]any)["Body"])
}

func TestSQSProvider_JSON_FIFODedup(t *testing.T) {
	p := newTestSQSProvider(t)

	handleJSON(t, p, "CreateQueue", map[string]any{
		"QueueName":  "dedup.fifo",
		"Attributes": map[string]any{"FifoQueue": "true"},
	})
	queueURL := "http://localhost:4747/000000000000/dedup.fifo"

	handleJSON(t, p, "SendMessage", map[string]any{
		"QueueUrl":               queueURL,
		"MessageBody":            "msg1",
		"MessageGroupId":         "g1",
		"MessageDeduplicationId": "dup1",
	})
	handleJSON(t, p, "SendMessage", map[string]any{
		"QueueUrl":               queueURL,
		"MessageBody":            "msg1-dup",
		"MessageGroupId":         "g1",
		"MessageDeduplicationId": "dup1",
	})

	recvResp := handleJSON(t, p, "ReceiveMessage", map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
	})
	recvBody := jsonBody(t, recvResp)
	msgs := recvBody["Messages"].([]any)
	assert.Len(t, msgs, 1)
}

// Ensure unused helper stubs don't cause compile errors by discarding them.
var _ = formRequest
var _ = makeFormReq

// TestSQSProvider_Init_PortFromOptions verifies that SQSProvider.Init reads
// the `server_port` key from cfg.Options and threads it into the underlying
// QueueStore, so queue URLs in responses reflect the dialled-in port rather
// than a hardcoded default. Regression test for the opts-based port
// propagation refactor.
func TestSQSProvider_Init_PortFromOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     map[string]any
		wantPort int
	}{
		{"nil opts falls back to 4747", nil, 4747},
		{"empty opts falls back to 4747", map[string]any{}, 4747},
		{"wrong type (string) falls back to 4747", map[string]any{"server_port": "5858"}, 4747},
		{"explicit int port 5858", map[string]any{"server_port": 5858}, 5858},
		{"explicit int port 8080", map[string]any{"server_port": 8080}, 8080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQSProvider{}
			require.NoError(t, p.Init(plugin.PluginConfig{Options: tt.opts}))

			// Create a queue via the Query protocol; the response embeds the
			// constructed URL, which must reflect the configured port.
			resp := handleForm(t, p, "Action=CreateQueue&QueueName=port-check")
			require.Equal(t, 200, resp.StatusCode)
			body := string(resp.Body)
			wantFragment := fmt.Sprintf(":%d/", tt.wantPort)
			assert.Contains(t, body, wantFragment,
				"expected response body to contain %q; got %s", wantFragment, body)
		})
	}
}
