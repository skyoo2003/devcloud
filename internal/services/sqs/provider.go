// SPDX-License-Identifier: Apache-2.0

package sqs

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID

// SQSProvider implements plugin.ServicePlugin for the SQS Query protocol.
type SQSProvider struct {
	store *QueueStore
}

// ServiceID returns the unique identifier for this plugin.
func (p *SQSProvider) ServiceID() string { return "sqs" }

// ServiceName returns the human-readable name for this plugin.
func (p *SQSProvider) ServiceName() string { return "Amazon SQS" }

// Protocol returns the wire protocol used by this plugin.
func (p *SQSProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

// Init initialises the QueueStore using the configured HTTP port so queue
// URLs returned in responses match the server the client dialed in on.
func (p *SQSProvider) Init(cfg plugin.PluginConfig) error {
	port := 0
	if v, ok := cfg.Options["server_port"].(int); ok {
		port = v
	}
	p.store = NewQueueStore(port)
	return nil
}

// Shutdown is a no-op for the in-memory store.
func (p *SQSProvider) Shutdown(_ context.Context) error { return nil }

// isJSONProtocol returns true when the request uses the AWS JSON 1.0 protocol
// (modern boto3 sends Content-Type: application/x-amz-json-1.0).
func isJSONProtocol(req *http.Request) bool {
	return strings.Contains(req.Header.Get("Content-Type"), "application/x-amz-json")
}

// HandleRequest routes the incoming SQS request to the correct handler.
// It supports both the legacy Query (form-encoded) protocol and the modern
// JSON 1.0 protocol used by boto3 1.42+.
// op is the Action already extracted by the gateway; if empty it is read from
// the form body (Query protocol) or the X-Amz-Target header (JSON protocol).
func (p *SQSProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if isJSONProtocol(req) {
		return p.handleJSONRequest(op, req)
	}
	return p.handleQueryRequest(op, req)
}

// handleQueryRequest handles the legacy form-encoded (Query) protocol.
func (p *SQSProvider) handleQueryRequest(op string, req *http.Request) (*plugin.Response, error) {
	if err := req.ParseForm(); err != nil {
		return sqsError("InvalidParameterValue", "failed to parse form body", http.StatusBadRequest), nil
	}

	action := op
	if action == "" {
		action = req.FormValue("Action")
	}

	switch action {
	case "CreateQueue":
		return p.createQueue(req)
	case "ListQueues":
		return p.listQueues(req)
	case "GetQueueUrl":
		return p.getQueueUrl(req)
	case "SendMessage":
		return p.sendMessage(req)
	case "ReceiveMessage":
		return p.receiveMessage(req)
	case "DeleteMessage":
		return p.deleteMessage(req)
	case "DeleteQueue":
		return p.deleteQueue(req)
	case "GetQueueAttributes":
		return p.getQueueAttributes(req)
	case "SetQueueAttributes":
		return p.setQueueAttributes(req)
	case "PurgeQueue":
		return p.purgeQueue(req)
	default:
		return sqsError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

// handleJSONRequest handles the modern JSON 1.0 protocol used by boto3 1.42+.
func (p *SQSProvider) handleJSONRequest(op string, req *http.Request) (*plugin.Response, error) {
	// Read and parse body once.
	rawBody, err := io.ReadAll(req.Body)
	if err != nil {
		return jsonError("InvalidParameterValue", "failed to read request body", http.StatusBadRequest), nil
	}

	var params map[string]any
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &params); err != nil {
			return jsonError("InvalidParameterValue", "failed to parse JSON body", http.StatusBadRequest), nil
		}
	} else {
		params = make(map[string]any)
	}

	// Resolve action: prefer op arg, then X-Amz-Target header (strip "AmazonSQS." prefix).
	action := op
	if action == "" {
		target := req.Header.Get("X-Amz-Target")
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			action = target[idx+1:]
		} else {
			action = target
		}
	}

	switch action {
	case "CreateQueue":
		return p.createQueueJSON(params)
	case "ListQueues":
		return p.listQueuesJSON(params)
	case "GetQueueUrl":
		return p.getQueueUrlJSON(params)
	case "SendMessage":
		return p.sendMessageJSON(params)
	case "ReceiveMessage":
		return p.receiveMessageJSON(params)
	case "DeleteMessage":
		return p.deleteMessageJSON(params)
	case "DeleteQueue":
		return p.deleteQueueJSON(params)
	case "GetQueueAttributes":
		return p.getQueueAttributesJSON(params)
	case "SetQueueAttributes":
		return p.setQueueAttributesJSON(params)
	case "PurgeQueue":
		return p.purgeQueueJSON(params)
	// Batch operations
	case "SendMessageBatch":
		return p.sendMessageBatchJSON(params)
	case "DeleteMessageBatch":
		return p.deleteMessageBatchJSON(params)
	case "ChangeMessageVisibilityBatch":
		return p.changeMessageVisibilityBatchJSON(params)
	case "ChangeMessageVisibility":
		return p.changeMessageVisibilityJSON(params)
	// Tags
	case "TagQueue":
		return p.tagQueueJSON(params)
	case "UntagQueue":
		return p.untagQueueJSON(params)
	case "ListQueueTags":
		return p.listQueueTagsJSON(params)
	// Permissions (stubs)
	case "AddPermission":
		return jsonResp(http.StatusOK, map[string]any{})
	case "RemovePermission":
		return jsonResp(http.StatusOK, map[string]any{})
	default:
		return jsonError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

// ListResources returns all queues as plugin resources.
func (p *SQSProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	queues := p.store.ListQueues(defaultAccountID, "")
	resources := make([]plugin.Resource, 0, len(queues))
	for _, q := range queues {
		resources = append(resources, plugin.Resource{
			Type: "queue",
			ID:   q.URL,
			Name: q.Name,
		})
	}
	return resources, nil
}

// GetMetrics returns empty metrics.
func (p *SQSProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- XML response helpers ---

type sqsErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

func sqsError(code, message string, status int) *plugin.Response {
	e := sqsErrorResponse{}
	e.Error.Code = code
	e.Error.Message = message
	body, _ := xml.Marshal(e)
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}
}

func xmlResp(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "text/xml",
		Body:        body,
	}, nil
}

// --- XML response structs ---

type createQueueResponse struct {
	XMLName xml.Name `xml:"CreateQueueResponse"`
	Result  struct {
		QueueUrl string `xml:"QueueUrl"`
	} `xml:"CreateQueueResult"`
}

type listQueuesResponse struct {
	XMLName xml.Name `xml:"ListQueuesResponse"`
	Result  struct {
		QueueUrls []string `xml:"QueueUrl"`
	} `xml:"ListQueuesResult"`
}

type getQueueUrlResponse struct {
	XMLName xml.Name `xml:"GetQueueUrlResponse"`
	Result  struct {
		QueueUrl string `xml:"QueueUrl"`
	} `xml:"GetQueueUrlResult"`
}

type sendMessageResponse struct {
	XMLName xml.Name `xml:"SendMessageResponse"`
	Result  struct {
		MessageId        string `xml:"MessageId"`
		MD5OfMessageBody string `xml:"MD5OfMessageBody"`
	} `xml:"SendMessageResult"`
}

type receiveMessageResponse struct {
	XMLName xml.Name      `xml:"ReceiveMessageResponse"`
	Result  receiveResult `xml:"ReceiveMessageResult"`
}

type receiveResult struct {
	Messages []messageXML `xml:"Message"`
}

type messageXML struct {
	MessageId         string                `xml:"MessageId"`
	ReceiptHandle     string                `xml:"ReceiptHandle"`
	Body              string                `xml:"Body"`
	MD5OfBody         string                `xml:"MD5OfBody"`
	MessageAttributes []messageAttributeXML `xml:"MessageAttribute,omitempty"`
}

type deleteMessageResponse struct {
	XMLName xml.Name `xml:"DeleteMessageResponse"`
}

type deleteQueueResponse struct {
	XMLName xml.Name `xml:"DeleteQueueResponse"`
}

type getQueueAttributesResponse struct {
	XMLName xml.Name `xml:"GetQueueAttributesResponse"`
	Result  struct {
		Attributes []xmlAttribute `xml:"Attribute"`
	} `xml:"GetQueueAttributesResult"`
}

type xmlAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type setQueueAttributesResponse struct {
	XMLName xml.Name `xml:"SetQueueAttributesResponse"`
}

type purgeQueueResponse struct {
	XMLName xml.Name `xml:"PurgeQueueResponse"`
}

type messageAttributeXML struct {
	Name  string                   `xml:"Name"`
	Value messageAttributeValueXML `xml:"Value"`
}

type messageAttributeValueXML struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue,omitempty"`
}

// --- JSON response helpers ---

func jsonError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]any{
		"__type":  code,
		"message": message,
	})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/x-amz-json-1.0",
		Body:        body,
	}
}

func jsonResp(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/x-amz-json-1.0",
		Body:        body,
	}, nil
}

// strParam extracts a string value from the JSON params map.
func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// intParam extracts an int value from the JSON params map.
func intParam(params map[string]any, key string, defaultVal int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return defaultVal
}

// --- JSON protocol operation implementations ---

func (p *SQSProvider) createQueueJSON(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "QueueName")
	if name == "" {
		return jsonError("MissingParameter", "QueueName is required", http.StatusBadRequest), nil
	}

	attrs := make(map[string]string)
	if a, ok := params["Attributes"]; ok {
		if m, ok := a.(map[string]any); ok {
			for k, v := range m {
				if s, ok := v.(string); ok {
					attrs[k] = s
				}
			}
		}
	}

	err := p.store.CreateQueueWithAttributes(name, defaultAccountID, attrs)
	if err != nil && err != ErrQueueAlreadyExists {
		return nil, err
	}

	qURL := p.store.QueueURL(defaultAccountID, name)
	return jsonResp(http.StatusOK, map[string]string{"QueueUrl": qURL})
}

func (p *SQSProvider) listQueuesJSON(params map[string]any) (*plugin.Response, error) {
	prefix := strParam(params, "QueueNamePrefix")
	queues := p.store.ListQueues(defaultAccountID, prefix)

	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, p.store.QueueURL(defaultAccountID, q.Name))
	}
	return jsonResp(http.StatusOK, map[string]any{"QueueUrls": urls})
}

func (p *SQSProvider) getQueueUrlJSON(params map[string]any) (*plugin.Response, error) {
	name := strParam(params, "QueueName")
	if name == "" {
		return jsonError("MissingParameter", "QueueName is required", http.StatusBadRequest), nil
	}

	_, err := p.store.GetQueueUrl(name, defaultAccountID)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	qURL := p.store.QueueURL(defaultAccountID, name)
	return jsonResp(http.StatusOK, map[string]string{"QueueUrl": qURL})
}

func (p *SQSProvider) sendMessageJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	body := strParam(params, "MessageBody")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}
	if body == "" {
		return jsonError("MissingParameter", "MessageBody is required", http.StatusBadRequest), nil
	}

	attrs := parseJSONMessageAttributes(params)
	fifoOpts := SendMessageFIFOOptions{
		MessageGroupID:         strParam(params, "MessageGroupId"),
		MessageDeduplicationID: strParam(params, "MessageDeduplicationId"),
	}

	name := queueNameFromURL(queueURL)
	msgID, err := p.store.SendMessageFull(name, defaultAccountID, body, attrs, fifoOpts)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]string{
		"MessageId":        msgID,
		"MD5OfMessageBody": md5Hex(body),
	})
}

func (p *SQSProvider) receiveMessageJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	maxMessages := intParam(params, "MaxNumberOfMessages", 1)
	if maxMessages <= 0 {
		maxMessages = 1
	}
	visibilityTimeout := intParam(params, "VisibilityTimeout", 30)

	name := queueNameFromURL(queueURL)
	msgs, err := p.store.ReceiveMessage(name, defaultAccountID, maxMessages, visibilityTimeout)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	type msgAttrJSON struct {
		DataType    string `json:"DataType"`
		StringValue string `json:"StringValue,omitempty"`
	}
	type msgJSON struct {
		MessageId         string                 `json:"MessageId"`
		ReceiptHandle     string                 `json:"ReceiptHandle"`
		Body              string                 `json:"Body"`
		MD5OfBody         string                 `json:"MD5OfBody"`
		MessageAttributes map[string]msgAttrJSON `json:"MessageAttributes,omitempty"`
	}

	result := make([]msgJSON, 0, len(msgs))
	for _, m := range msgs {
		mj := msgJSON{
			MessageId:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			MD5OfBody:     m.MD5OfBody,
		}
		if len(m.MessageAttributes) > 0 {
			mj.MessageAttributes = make(map[string]msgAttrJSON)
			for k, v := range m.MessageAttributes {
				mj.MessageAttributes[k] = msgAttrJSON{
					DataType:    v.DataType,
					StringValue: v.StringValue,
				}
			}
		}
		result = append(result, mj)
	}
	return jsonResp(http.StatusOK, map[string]any{"Messages": result})
}

func (p *SQSProvider) deleteMessageJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	receiptHandle := strParam(params, "ReceiptHandle")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}
	if receiptHandle == "" {
		return jsonError("MissingParameter", "ReceiptHandle is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.DeleteMessage(name, defaultAccountID, receiptHandle)
	if err != nil {
		return jsonError("ReceiptHandleIsInvalid", "receipt handle is invalid", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *SQSProvider) deleteQueueJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.DeleteQueue(name, defaultAccountID)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *SQSProvider) getQueueAttributesJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	var attrNames []string
	if names, ok := params["AttributeNames"]; ok {
		if arr, ok := names.([]any); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					attrNames = append(attrNames, s)
				}
			}
		}
	}
	if len(attrNames) == 0 {
		attrNames = []string{"All"}
	}

	name := queueNameFromURL(queueURL)
	attrs, err := p.store.GetQueueAttributes(name, defaultAccountID, attrNames)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{"Attributes": attrs})
}

func (p *SQSProvider) setQueueAttributesJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	attrs := make(map[string]string)
	if a, ok := params["Attributes"]; ok {
		if m, ok := a.(map[string]any); ok {
			for k, v := range m {
				if s, ok := v.(string); ok {
					attrs[k] = s
				}
			}
		}
	}

	name := queueNameFromURL(queueURL)
	err := p.store.SetQueueAttributes(name, defaultAccountID, attrs)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *SQSProvider) purgeQueueJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.PurgeQueue(name, defaultAccountID)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

// parseJSONMessageAttributes extracts MessageAttributes from JSON params.
func parseJSONMessageAttributes(params map[string]any) map[string]MessageAttribute {
	raw, ok := params["MessageAttributes"]
	if !ok {
		return nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	attrs := make(map[string]MessageAttribute)
	for k, v := range m {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		attr := MessageAttribute{}
		if dt, ok := vm["DataType"].(string); ok {
			attr.DataType = dt
		}
		if sv, ok := vm["StringValue"].(string); ok {
			attr.StringValue = sv
		}
		attrs[k] = attr
	}
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// --- helpers ---

// queueNameFromURL extracts the last path segment of a queue URL.
func queueNameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.TrimRight(u.Path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// --- SQS operation implementations ---

func (p *SQSProvider) createQueue(req *http.Request) (*plugin.Response, error) {
	name := req.FormValue("QueueName")
	if name == "" {
		return sqsError("MissingParameter", "QueueName is required", http.StatusBadRequest), nil
	}

	// Parse Attribute.N.Name / Attribute.N.Value parameters
	attrs := make(map[string]string)
	for i := 1; ; i++ {
		k := req.FormValue(fmt.Sprintf("Attribute.%d.Name", i))
		if k == "" {
			break
		}
		v := req.FormValue(fmt.Sprintf("Attribute.%d.Value", i))
		attrs[k] = v
	}

	err := p.store.CreateQueueWithAttributes(name, defaultAccountID, attrs)
	if err != nil && err != ErrQueueAlreadyExists {
		return nil, err
	}

	qURL := p.store.QueueURL(defaultAccountID, name)

	resp := createQueueResponse{}
	resp.Result.QueueUrl = qURL
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) listQueues(req *http.Request) (*plugin.Response, error) {
	prefix := req.FormValue("QueueNamePrefix")
	queues := p.store.ListQueues(defaultAccountID, prefix)

	resp := listQueuesResponse{}
	for _, q := range queues {
		// Use our canonical URL format.
		qURL := p.store.QueueURL(defaultAccountID, q.Name)
		resp.Result.QueueUrls = append(resp.Result.QueueUrls, qURL)
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) getQueueUrl(req *http.Request) (*plugin.Response, error) {
	name := req.FormValue("QueueName")
	if name == "" {
		return sqsError("MissingParameter", "QueueName is required", http.StatusBadRequest), nil
	}

	_, err := p.store.GetQueueUrl(name, defaultAccountID)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	qURL := p.store.QueueURL(defaultAccountID, name)
	resp := getQueueUrlResponse{}
	resp.Result.QueueUrl = qURL
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) sendMessage(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	body := req.FormValue("MessageBody")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}
	if body == "" {
		return sqsError("MissingParameter", "MessageBody is required", http.StatusBadRequest), nil
	}

	attrs := parseFormMessageAttributes(req)

	name := queueNameFromURL(queueURL)
	msgID, err := p.store.SendMessageWithAttributes(name, defaultAccountID, body, attrs)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	resp := sendMessageResponse{}
	resp.Result.MessageId = msgID
	resp.Result.MD5OfMessageBody = md5Hex(body)
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) receiveMessage(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	maxMessages := 1
	if s := req.FormValue("MaxNumberOfMessages"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxMessages = n
		}
	}

	visibilityTimeout := 30
	if s := req.FormValue("VisibilityTimeout"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 0 {
			visibilityTimeout = n
		}
	}

	name := queueNameFromURL(queueURL)
	msgs, err := p.store.ReceiveMessage(name, defaultAccountID, maxMessages, visibilityTimeout)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	resp := receiveMessageResponse{}
	for _, m := range msgs {
		mx := messageXML{
			MessageId:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			MD5OfBody:     m.MD5OfBody,
		}
		for attrName, attr := range m.MessageAttributes {
			mx.MessageAttributes = append(mx.MessageAttributes, messageAttributeXML{
				Name: attrName,
				Value: messageAttributeValueXML{
					DataType:    attr.DataType,
					StringValue: attr.StringValue,
				},
			})
		}
		resp.Result.Messages = append(resp.Result.Messages, mx)
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) deleteMessage(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	receiptHandle := req.FormValue("ReceiptHandle")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}
	if receiptHandle == "" {
		return sqsError("MissingParameter", "ReceiptHandle is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.DeleteMessage(name, defaultAccountID, receiptHandle)
	if err != nil {
		return sqsError("ReceiptHandleIsInvalid", "receipt handle is invalid", http.StatusBadRequest), nil
	}

	return xmlResp(http.StatusOK, deleteMessageResponse{})
}

func (p *SQSProvider) deleteQueue(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.DeleteQueue(name, defaultAccountID)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return xmlResp(http.StatusOK, deleteQueueResponse{})
}

func (p *SQSProvider) getQueueAttributes(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	// Parse AttributeName.N parameters
	var attrNames []string
	for i := 1; ; i++ {
		v := req.FormValue(fmt.Sprintf("AttributeName.%d", i))
		if v == "" {
			break
		}
		attrNames = append(attrNames, v)
	}
	if len(attrNames) == 0 {
		attrNames = []string{"All"}
	}

	name := queueNameFromURL(queueURL)
	attrs, err := p.store.GetQueueAttributes(name, defaultAccountID, attrNames)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	resp := getQueueAttributesResponse{}
	for k, v := range attrs {
		resp.Result.Attributes = append(resp.Result.Attributes, xmlAttribute{Name: k, Value: v})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *SQSProvider) setQueueAttributes(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	// Parse Attribute.N.Name / Attribute.N.Value parameters
	attrs := make(map[string]string)
	for i := 1; ; i++ {
		k := req.FormValue(fmt.Sprintf("Attribute.%d.Name", i))
		if k == "" {
			break
		}
		v := req.FormValue(fmt.Sprintf("Attribute.%d.Value", i))
		attrs[k] = v
	}

	name := queueNameFromURL(queueURL)
	err := p.store.SetQueueAttributes(name, defaultAccountID, attrs)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return xmlResp(http.StatusOK, setQueueAttributesResponse{})
}

func (p *SQSProvider) purgeQueue(req *http.Request) (*plugin.Response, error) {
	queueURL := req.FormValue("QueueUrl")
	if queueURL == "" {
		return sqsError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	err := p.store.PurgeQueue(name, defaultAccountID)
	if err != nil {
		return sqsError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return xmlResp(http.StatusOK, purgeQueueResponse{})
}

// parseFormMessageAttributes extracts MessageAttribute.N.* from form values.
func parseFormMessageAttributes(req *http.Request) map[string]MessageAttribute {
	attrs := make(map[string]MessageAttribute)
	for i := 1; ; i++ {
		name := req.FormValue(fmt.Sprintf("MessageAttribute.%d.Name", i))
		if name == "" {
			break
		}
		dataType := req.FormValue(fmt.Sprintf("MessageAttribute.%d.Value.DataType", i))
		stringValue := req.FormValue(fmt.Sprintf("MessageAttribute.%d.Value.StringValue", i))
		attrs[name] = MessageAttribute{
			DataType:    dataType,
			StringValue: stringValue,
		}
	}
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// --- Batch operations ---

func (p *SQSProvider) sendMessageBatchJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	entries, ok := params["Entries"].([]any)
	if !ok {
		return jsonError("MissingParameter", "Entries is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)

	type successEntry struct {
		Id               string `json:"Id"`
		MessageId        string `json:"MessageId"`
		MD5OfMessageBody string `json:"MD5OfMessageBody"`
	}
	type failEntry struct {
		Id          string `json:"Id"`
		Code        string `json:"Code"`
		Message     string `json:"Message"`
		SenderFault bool   `json:"SenderFault"`
	}

	var successful []successEntry
	var failed []failEntry

	for _, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			continue
		}
		id := strParam(entry, "Id")
		body := strParam(entry, "MessageBody")
		if body == "" {
			failed = append(failed, failEntry{
				Id:          id,
				Code:        "MissingParameter",
				Message:     "MessageBody is required",
				SenderFault: true,
			})
			continue
		}
		attrs := parseJSONMessageAttributes(entry)
		batchFIFOOpts := SendMessageFIFOOptions{
			MessageGroupID:         strParam(entry, "MessageGroupId"),
			MessageDeduplicationID: strParam(entry, "MessageDeduplicationId"),
		}
		msgID, err := p.store.SendMessageFull(name, defaultAccountID, body, attrs, batchFIFOOpts)
		if err != nil {
			failed = append(failed, failEntry{
				Id:          id,
				Code:        "AWS.SimpleQueueService.NonExistentQueue",
				Message:     "queue not found",
				SenderFault: false,
			})
			continue
		}
		successful = append(successful, successEntry{
			Id:               id,
			MessageId:        msgID,
			MD5OfMessageBody: md5Hex(body),
		})
	}

	if successful == nil {
		successful = []successEntry{}
	}
	if failed == nil {
		failed = []failEntry{}
	}

	return jsonResp(http.StatusOK, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

func (p *SQSProvider) deleteMessageBatchJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	entries, ok := params["Entries"].([]any)
	if !ok {
		return jsonError("MissingParameter", "Entries is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)

	type successEntry struct {
		Id string `json:"Id"`
	}
	type failEntry struct {
		Id          string `json:"Id"`
		Code        string `json:"Code"`
		Message     string `json:"Message"`
		SenderFault bool   `json:"SenderFault"`
	}

	var successful []successEntry
	var failed []failEntry

	for _, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			continue
		}
		id := strParam(entry, "Id")
		receiptHandle := strParam(entry, "ReceiptHandle")
		err := p.store.DeleteMessage(name, defaultAccountID, receiptHandle)
		if err != nil {
			failed = append(failed, failEntry{
				Id:          id,
				Code:        "ReceiptHandleIsInvalid",
				Message:     "receipt handle is invalid",
				SenderFault: true,
			})
			continue
		}
		successful = append(successful, successEntry{Id: id})
	}

	if successful == nil {
		successful = []successEntry{}
	}
	if failed == nil {
		failed = []failEntry{}
	}

	return jsonResp(http.StatusOK, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

func (p *SQSProvider) changeMessageVisibilityBatchJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	entries, ok := params["Entries"].([]any)
	if !ok {
		return jsonError("MissingParameter", "Entries is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)

	type successEntry struct {
		Id string `json:"Id"`
	}
	type failEntry struct {
		Id          string `json:"Id"`
		Code        string `json:"Code"`
		Message     string `json:"Message"`
		SenderFault bool   `json:"SenderFault"`
	}

	var successful []successEntry
	var failed []failEntry

	for _, e := range entries {
		entry, ok := e.(map[string]any)
		if !ok {
			continue
		}
		id := strParam(entry, "Id")
		receiptHandle := strParam(entry, "ReceiptHandle")
		vt := intParam(entry, "VisibilityTimeout", 30)
		err := p.store.ChangeMessageVisibility(defaultAccountID, name, receiptHandle, vt)
		if err != nil {
			failed = append(failed, failEntry{
				Id:          id,
				Code:        "ReceiptHandleIsInvalid",
				Message:     "receipt handle is invalid",
				SenderFault: true,
			})
			continue
		}
		successful = append(successful, successEntry{Id: id})
	}

	if successful == nil {
		successful = []successEntry{}
	}
	if failed == nil {
		failed = []failEntry{}
	}

	return jsonResp(http.StatusOK, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

func (p *SQSProvider) changeMessageVisibilityJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	receiptHandle := strParam(params, "ReceiptHandle")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}
	if receiptHandle == "" {
		return jsonError("MissingParameter", "ReceiptHandle is required", http.StatusBadRequest), nil
	}
	vt := intParam(params, "VisibilityTimeout", 30)

	name := queueNameFromURL(queueURL)
	err := p.store.ChangeMessageVisibility(defaultAccountID, name, receiptHandle, vt)
	if err != nil {
		return jsonError("ReceiptHandleIsInvalid", "receipt handle is invalid", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

// --- Tags ---

func (p *SQSProvider) tagQueueJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	tags := make(map[string]string)
	if t, ok := params["Tags"]; ok {
		if m, ok := t.(map[string]any); ok {
			for k, v := range m {
				if s, ok := v.(string); ok {
					tags[k] = s
				}
			}
		}
	}

	name := queueNameFromURL(queueURL)
	err := p.store.TagQueue(name, defaultAccountID, tags)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *SQSProvider) untagQueueJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	var tagKeys []string
	if k, ok := params["TagKeys"]; ok {
		if arr, ok := k.([]any); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					tagKeys = append(tagKeys, s)
				}
			}
		}
	}

	name := queueNameFromURL(queueURL)
	err := p.store.UntagQueue(name, defaultAccountID, tagKeys)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{})
}

func (p *SQSProvider) listQueueTagsJSON(params map[string]any) (*plugin.Response, error) {
	queueURL := strParam(params, "QueueUrl")
	if queueURL == "" {
		return jsonError("MissingParameter", "QueueUrl is required", http.StatusBadRequest), nil
	}

	name := queueNameFromURL(queueURL)
	tags, err := p.store.ListQueueTags(name, defaultAccountID)
	if err != nil {
		return jsonError("AWS.SimpleQueueService.NonExistentQueue", "queue not found", http.StatusBadRequest), nil
	}

	return jsonResp(http.StatusOK, map[string]any{"Tags": tags})
}
