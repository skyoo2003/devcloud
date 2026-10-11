// SPDX-License-Identifier: Apache-2.0

package azurequeue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/auth"
	generated "github.com/skyoo2003/devcloud/internal/generated/azurequeue"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *Store
}

func (p *Provider) ServiceID() string             { return "azurequeue" }
func (p *Provider) ServiceName() string           { return "Azure Queue Storage" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTXML }
func (p *Provider) Provider() string              { return "azure" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/azure-queue"
	}
	p.store = NewStore(p.dataDir)
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	queues, err := p.store.ListQueues("devstoreaccount1", "")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, q := range queues {
		res = append(res, plugin.Resource{
			Type: "Queue",
			ID:   q.Name,
			Name: q.Name,
		})
	}
	return res, nil
}

func randomRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (p *Provider) reply(req *http.Request, status int, headers map[string]string, body []byte) *plugin.Response {
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["x-ms-request-id"] = randomRequestID()
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version
	headers["Date"] = time.Now().UTC().Format(http.TimeFormat)

	ct := headers["Content-Type"]
	if ct == "" {
		ct = "application/xml"
	}
	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        body,
		ContentType: ct,
	}
}

func (p *Provider) errorReply(req *http.Request, status int, code, message string) *plugin.Response {
	headers := map[string]string{
		"x-ms-error-code": code,
		"Content-Type":    "application/xml",
		"x-ms-request-id": randomRequestID(),
		"Date":            time.Now().UTC().Format(http.TimeFormat),
	}
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version

	body := fmt.Sprintf("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Error><Code>%s</Code><Message>%s\nRequestId:%s\nTime:%s</Message></Error>",
		code, message, headers["x-ms-request-id"], time.Now().UTC().Format(time.RFC3339Nano))

	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        []byte(body),
		ContentType: "application/xml",
	}
}

func (p *Provider) HandleRequest(ctx context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	account := "devstoreaccount1"
	if id, ok := auth.FromContext(ctx); ok && id.AccessKeyID != "" {
		account = id.AccessKeyID
	}

	uri := req.URL.RequestURI()
	for _, accPrefix := range []string{"/" + account + "/queue", "/devstoreaccount1/queue", "/" + account, "/devstoreaccount1", "/queue"} {
		if strings.HasPrefix(uri, accPrefix+"/") {
			uri = uri[len(accPrefix):]
			break
		} else if strings.HasPrefix(uri, accPrefix+"?") {
			uri = "/" + uri[len(accPrefix):]
			break
		} else if uri == accPrefix {
			uri = "/"
			break
		}
	}

	op, params := generated.MatchOperation(req.Method, uri)
	if op == "" {
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
	}

	switch op {
	case "ListQueues":
		return p.handleListQueues(req, account)
	case "CreateQueue":
		return p.handleCreateQueue(req, account, params["queue"])
	case "DeleteQueue":
		return p.handleDeleteQueue(req, account, params["queue"])
	case "GetQueueMetadata":
		return p.handleGetQueueMetadata(req, account, params["queue"])
	case "SetQueueMetadata":
		return p.handleSetQueueMetadata(req, account, params["queue"])
	case "PutMessage":
		return p.handlePutMessage(req, account, params["queue"])
	case "GetMessages":
		return p.handleGetMessages(req, account, params["queue"], false)
	case "PeekMessages":
		return p.handleGetMessages(req, account, params["queue"], true)
	case "DeleteMessage":
		return p.handleDeleteMessage(req, account, params["queue"], params["messageid"])
	case "ClearMessages":
		return p.handleClearMessages(req, account, params["queue"])
	default:
		return p.errorReply(req, http.StatusBadRequest, "InvalidAction", "operation not handled: "+req.Method+" "+req.URL.Path), nil
	}
}

type XMLQueueItem struct {
	Name string `xml:"Name"`
}

type XMLQueuesResult struct {
	XMLName         xml.Name       `xml:"EnumerationResults"`
	ServiceEndpoint string         `xml:"ServiceEndpoint,attr"`
	Queues          []XMLQueueItem `xml:"Queues>Queue"`
}

func (p *Provider) handleListQueues(req *http.Request, account string) (*plugin.Response, error) {
	prefix := req.URL.Query().Get("prefix")
	queues, err := p.store.ListQueues(account, prefix)
	if err != nil {
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	var items []XMLQueueItem
	for _, q := range queues {
		items = append(items, XMLQueueItem{Name: q.Name})
	}

	result := XMLQueuesResult{
		ServiceEndpoint: fmt.Sprintf("http://127.0.0.1:4747/%s/", account),
		Queues:          items,
	}
	raw, _ := xml.MarshalIndent(result, "", "  ")
	body := []byte(xml.Header + string(raw))
	return p.reply(req, http.StatusOK, map[string]string{"Content-Type": "application/xml"}, body), nil
}

func (p *Provider) handleCreateQueue(req *http.Request, account, queue string) (*plugin.Response, error) {
	meta := extractMetadata(req.Header)
	err := p.store.CreateQueue(account, queue, meta)
	if err != nil {
		if errors.Is(err, ErrQueueAlreadyExists) {
			return p.reply(req, http.StatusNoContent, nil, nil), nil
		}
		if errors.Is(err, ErrInvalidQueueName) {
			return p.errorReply(req, http.StatusBadRequest, "InvalidResourceName", "invalid queue name"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusCreated, nil, nil), nil
}

func (p *Provider) handleDeleteQueue(req *http.Request, account, queue string) (*plugin.Response, error) {
	err := p.store.DeleteQueue(account, queue)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleGetQueueMetadata(req *http.Request, account, queue string) (*plugin.Response, error) {
	meta, count, err := p.store.GetQueueMetadata(account, queue)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	headers := map[string]string{
		"x-ms-approximate-messages-count": fmt.Sprintf("%d", count),
	}
	writeMetadataHeaders(headers, meta)
	return p.reply(req, http.StatusOK, headers, nil), nil
}

func (p *Provider) handleSetQueueMetadata(req *http.Request, account, queue string) (*plugin.Response, error) {
	meta := extractMetadata(req.Header)
	err := p.store.SetQueueMetadata(account, queue, meta)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

type XMLPutMessageRequest struct {
	XMLName xml.Name `xml:"QueueMessage"`
	Text    string   `xml:"MessageText"`
}

type XMLMessageResponse struct {
	MessageID       string `xml:"MessageId"`
	InsertionTime   string `xml:"InsertionTime"`
	ExpirationTime  string `xml:"ExpirationTime"`
	PopReceipt      string `xml:"PopReceipt"`
	TimeNextVisible string `xml:"TimeNextVisible"`
	DequeueCount    int    `xml:"DequeueCount,omitempty"`
	MessageText     string `xml:"MessageText,omitempty"`
}

type XMLQueueMessagesList struct {
	XMLName  xml.Name             `xml:"QueueMessagesList"`
	Messages []XMLMessageResponse `xml:"QueueMessage"`
}

func (p *Provider) handlePutMessage(req *http.Request, account, queue string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidXmlDocument", "failed to read body"), nil
	}
	var putReq XMLPutMessageRequest
	if err := xml.Unmarshal(body, &putReq); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidXmlDocument", "invalid message XML"), nil
	}

	visTimeoutSec, _ := strconv.Atoi(req.URL.Query().Get("visibilitytimeout"))
	ttlSec, _ := strconv.Atoi(req.URL.Query().Get("messagettl"))

	msg, err := p.store.PutMessage(account, queue, putReq.Text, time.Duration(visTimeoutSec)*time.Second, time.Duration(ttlSec)*time.Second)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	res := XMLQueueMessagesList{
		Messages: []XMLMessageResponse{{
			MessageID:       msg.ID,
			InsertionTime:   msg.InsertionTime.Format(http.TimeFormat),
			ExpirationTime:  msg.ExpirationTime.Format(http.TimeFormat),
			PopReceipt:      msg.PopReceipt,
			TimeNextVisible: msg.TimeNextVisible.Format(http.TimeFormat),
		}},
	}
	raw, _ := xml.MarshalIndent(res, "", "  ")
	out := []byte(xml.Header + string(raw))
	return p.reply(req, http.StatusCreated, map[string]string{"Content-Type": "application/xml"}, out), nil
}

func (p *Provider) handleGetMessages(req *http.Request, account, queue string, peek bool) (*plugin.Response, error) {
	numMessages := 1
	if n := req.URL.Query().Get("numofmessages"); n != "" {
		if parsed, err := strconv.Atoi(n); err == nil && parsed > 0 {
			numMessages = parsed
		}
	}
	visTimeout := 30 * time.Second
	if vt := req.URL.Query().Get("visibilitytimeout"); vt != "" {
		if parsed, err := strconv.Atoi(vt); err == nil && parsed >= 0 {
			visTimeout = time.Duration(parsed) * time.Second
		}
	}

	var msgs []*QueueMessage
	var err error
	if peek {
		msgs, err = p.store.PeekMessages(account, queue, numMessages)
	} else {
		msgs, err = p.store.GetMessages(account, queue, numMessages, visTimeout)
	}
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	var resMsgs []XMLMessageResponse
	for _, m := range msgs {
		resMsgs = append(resMsgs, XMLMessageResponse{
			MessageID:       m.ID,
			InsertionTime:   m.InsertionTime.Format(http.TimeFormat),
			ExpirationTime:  m.ExpirationTime.Format(http.TimeFormat),
			PopReceipt:      m.PopReceipt,
			TimeNextVisible: m.TimeNextVisible.Format(http.TimeFormat),
			DequeueCount:    m.DequeueCount,
			MessageText:     m.Text,
		})
	}

	result := XMLQueueMessagesList{Messages: resMsgs}
	raw, _ := xml.MarshalIndent(result, "", "  ")
	out := []byte(xml.Header + string(raw))
	return p.reply(req, http.StatusOK, map[string]string{"Content-Type": "application/xml"}, out), nil
}

func (p *Provider) handleDeleteMessage(req *http.Request, account, queue, messageID string) (*plugin.Response, error) {
	popReceipt := req.URL.Query().Get("popreceipt")
	err := p.store.DeleteMessage(account, queue, messageID, popReceipt)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		if errors.Is(err, ErrMessageNotFound) {
			return p.errorReply(req, http.StatusNotFound, "MessageNotFound", "message not found"), nil
		}
		if errors.Is(err, ErrPopReceiptMismatch) {
			return p.errorReply(req, http.StatusBadRequest, "PopReceiptMismatch", "pop receipt mismatch"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleClearMessages(req *http.Request, account, queue string) (*plugin.Response, error) {
	err := p.store.ClearMessages(account, queue)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return p.errorReply(req, http.StatusNotFound, "QueueNotFound", "queue not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func extractMetadata(h http.Header) map[string]string {
	meta := make(map[string]string)
	for k, vv := range h {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-ms-meta-") && len(vv) > 0 {
			metaKey := strings.TrimPrefix(lower, "x-ms-meta-")
			meta[metaKey] = vv[0]
		}
	}
	return meta
}

func writeMetadataHeaders(headers map[string]string, meta map[string]string) {
	for k, v := range meta {
		headers["x-ms-meta-"+strings.ToLower(k)] = v
	}
}

func init() {
	plugin.DefaultRegistry.Register("azurequeue", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
