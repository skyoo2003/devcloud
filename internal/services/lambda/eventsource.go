// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EventSourcePoller polls SQS queues (and DynamoDB streams) that are registered
// as event source mappings, and invokes the configured Lambda functions.
type EventSourcePoller struct {
	store    *LambdaStore
	port     int
	stopCh   chan struct{}
	stopOnce sync.Once
	states   map[string]*streamPollState
}

// NewEventSourcePoller creates a new EventSourcePoller backed by the given store.
func NewEventSourcePoller(store *LambdaStore, port int) *EventSourcePoller {
	return &EventSourcePoller{
		store:  store,
		port:   port,
		stopCh: make(chan struct{}),
		states: make(map[string]*streamPollState),
	}
}

// Start runs the polling loop until ctx is cancelled or Stop is called.
func (p *EventSourcePoller) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

// Stop signals the poller to stop. Safe to call multiple times.
func (p *EventSourcePoller) Stop() { p.stopOnce.Do(func() { close(p.stopCh) }) }

func (p *EventSourcePoller) poll(ctx context.Context) {
	mappings, err := p.store.ListEventSourceMappings(defaultAccountID, "")
	if err != nil {
		return
	}
	active := make(map[string]bool, len(mappings))
	for _, m := range mappings {
		active[m.UUID] = true
	}
	for key := range p.states {
		id, _, _ := strings.Cut(key, "/")
		if !active[id] {
			delete(p.states, key)
		}
	}
	for _, m := range mappings {
		if !m.Enabled || m.State != "Enabled" {
			continue
		}
		if isSQSArn(m.EventSourceARN) {
			p.pollSQS(ctx, m)
		} else if isDynamoDBStreamArn(m.EventSourceARN) {
			p.pollDynamoDBStream(ctx, m)
		}
	}
}

func isSQSArn(arn string) bool {
	return strings.Contains(arn, ":sqs:")
}

func isDynamoDBStreamArn(arn string) bool {
	return strings.Contains(arn, ":dynamodb:") && strings.Contains(arn, "/stream/")
}

func (p *EventSourcePoller) pollSQS(ctx context.Context, m EventSourceMapping) {
	baseURL := fmt.Sprintf("http://localhost:%d", p.port)
	parts := strings.Split(m.EventSourceARN, ":")
	if len(parts) != 6 || parts[4] != defaultAccountID {
		return
	}
	var queueOut struct {
		QueueURL string `json:"QueueUrl"`
	}
	if err := p.sourceJSON(ctx, "AmazonSQS.GetQueueUrl", map[string]any{"QueueName": parts[5], "QueueOwnerAWSAccountId": parts[4]}, &queueOut); err != nil || queueOut.QueueURL == "" {
		return
	}
	queueURL := queueOut.QueueURL

	batchSize := m.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}

	visibility := 30
	var attrOut struct {
		Attributes map[string]string `json:"Attributes"`
	}
	if err := p.sourceJSON(ctx, "AmazonSQS.GetQueueAttributes", map[string]any{"QueueUrl": queueURL, "AttributeNames": []string{"VisibilityTimeout"}}, &attrOut); err == nil {
		if value, err := strconv.Atoi(attrOut.Attributes["VisibilityTimeout"]); err == nil && value >= 0 {
			visibility = value
		}
	}
	// Receive messages from SQS.
	receiveBody, _ := json.Marshal(map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": batchSize,
		"WaitTimeSeconds":     0,
		"VisibilityTimeout":   visibility,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(receiveBody))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Messages []struct {
			MessageId     string `json:"MessageId"`
			Body          string `json:"Body"`
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}
	if len(result.Messages) == 0 {
		return
	}

	// Build SQS event payload for Lambda.
	records := make([]map[string]any, 0, len(result.Messages))
	for _, msg := range result.Messages {
		records = append(records, map[string]any{
			"messageId":      msg.MessageId,
			"body":           msg.Body,
			"receiptHandle":  msg.ReceiptHandle,
			"eventSource":    "aws:sqs",
			"eventSourceARN": m.EventSourceARN,
		})
	}
	event, _ := json.Marshal(map[string]any{"Records": records})

	// Invoke Lambda.
	invokeURL := fmt.Sprintf("%s/2015-03-31/functions/%s/invocations", baseURL, url.PathEscape(m.FunctionName))
	invokeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, invokeURL, bytes.NewReader(event))
	if err != nil {
		return
	}
	invokeReq.Header.Set("Content-Type", "application/json")
	invokeReq.Header.Set("X-Amz-Target", "Lambda.Invoke")
	invokeResp, err := http.DefaultClient.Do(invokeReq)
	if err != nil || invokeResp.StatusCode != http.StatusOK || invokeResp.Header.Get("X-Amz-Function-Error") != "" {
		if invokeResp != nil {
			_ = invokeResp.Body.Close()
		}
		slog.Warn("lambda invoke failed for SQS event", "function", m.FunctionName, "err", err)
		return
	}
	_ = invokeResp.Body.Close()

	// Delete messages on successful invocation.
	for _, msg := range result.Messages {
		delBody, _ := json.Marshal(map[string]any{
			"QueueUrl":      queueURL,
			"ReceiptHandle": msg.ReceiptHandle,
		})
		delReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(delBody))
		if err != nil {
			continue
		}
		delReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
		delReq.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
		delResp, err := http.DefaultClient.Do(delReq)
		if err == nil {
			_ = delResp.Body.Close()
		}
	}

	slog.Debug("processed SQS messages for lambda", "function", m.FunctionName, "count", len(result.Messages))
}

type streamPollState struct {
	generation, iterator, next, sequence string
	pending                              []byte
}
type shardDescription struct {
	StreamDescription struct {
		Shards []struct {
			ShardID string `json:"ShardId"`
		} `json:"Shards"`
	} `json:"StreamDescription"`
}
type shardBoundary interface {
	ShardBoundary(string, string) (string, string, error)
}

func (p *EventSourcePoller) sourceJSON(ctx context.Context, target string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://localhost:%d", p.port), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s returned %d", target, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(output)
}
func sourceBoundary(arn, shard string) (string, string, error) {
	svc, ok := plugin.DefaultRegistry.Get("dynamodbstreams")
	if !ok {
		return "", "0", nil
	}
	source, ok := svc.(shardBoundary)
	if !ok {
		return "", "0", nil
	}
	return source.ShardBoundary(arn, shard)
}
func (p *EventSourcePoller) initializeLatestCheckpoints(ctx context.Context, m EventSourceMapping) error {
	svc, ok := plugin.DefaultRegistry.Get("dynamodbstreams")
	if !ok {
		return fmt.Errorf("streams provider unavailable")
	}
	source, ok := svc.(shardBoundary)
	if !ok {
		return fmt.Errorf("streams boundary unavailable")
	}
	var desc shardDescription
	if err := p.sourceJSON(ctx, "DynamoDBStreams_20120810.DescribeStream", map[string]any{"StreamArn": m.EventSourceARN}, &desc); err != nil {
		return err
	}
	for _, shard := range desc.StreamDescription.Shards {
		g, seq, err := source.ShardBoundary(m.EventSourceARN, shard.ShardID)
		if err != nil {
			return err
		}
		if err := p.store.SaveStreamCheckpoint(m.UUID, shard.ShardID, StreamCheckpoint{g, seq}); err != nil {
			return err
		}
	}
	return nil
}
func (p *EventSourcePoller) pollDynamoDBStream(ctx context.Context, m EventSourceMapping) {
	var desc shardDescription
	if err := p.sourceJSON(ctx, "DynamoDBStreams_20120810.DescribeStream", map[string]any{"StreamArn": m.EventSourceARN}, &desc); err != nil {
		return
	}
	for _, shard := range desc.StreamDescription.Shards {
		p.pollStreamShard(ctx, m, shard.ShardID)
	}
}
func (p *EventSourcePoller) pollStreamShard(ctx context.Context, m EventSourceMapping, shard string) {
	generation, _, err := sourceBoundary(m.EventSourceARN, shard)
	if err != nil {
		return
	}
	key := m.UUID + "/" + shard
	state := p.states[key]
	if state == nil || state.generation != generation {
		state = &streamPollState{generation: generation}
		p.states[key] = state
	}
	if len(state.pending) == 0 {
		if state.iterator == "" {
			checkpoint, err := p.store.GetStreamCheckpoint(m.UUID, shard)
			if err != nil {
				return
			}
			kind := m.StartingPosition
			if kind == "" {
				kind = "TRIM_HORIZON"
			}
			input := map[string]any{"StreamArn": m.EventSourceARN, "ShardId": shard}
			if checkpoint != nil {
				if checkpoint.Generation == generation && checkpoint.SequenceNumber != "0" {
					kind = "AFTER_SEQUENCE_NUMBER"
					input["SequenceNumber"] = checkpoint.SequenceNumber
				} else {
					kind = "TRIM_HORIZON"
				}
			}
			input["ShardIteratorType"] = kind
			var out struct {
				Iterator string `json:"ShardIterator"`
			}
			if err := p.sourceJSON(ctx, "DynamoDBStreams_20120810.GetShardIterator", input, &out); err != nil {
				return
			}
			state.iterator = out.Iterator
		}
		limit := m.BatchSize
		if limit <= 0 {
			limit = 100
		}
		var out struct {
			Records []map[string]any `json:"Records"`
			Next    string           `json:"NextShardIterator"`
		}
		if err := p.sourceJSON(ctx, "DynamoDBStreams_20120810.GetRecords", map[string]any{"ShardIterator": state.iterator, "Limit": limit}, &out); err != nil {
			state.iterator = ""
			return
		}
		state.next = out.Next
		if len(out.Records) == 0 {
			state.iterator = out.Next
			return
		}
		for _, record := range out.Records {
			record["eventSourceARN"] = m.EventSourceARN
		}
		dynamo, ok := out.Records[len(out.Records)-1]["dynamodb"].(map[string]any)
		if !ok {
			return
		}
		sequence, ok := dynamo["SequenceNumber"].(string)
		if !ok || sequence == "" {
			return
		}
		state.sequence = sequence
		state.pending, _ = json.Marshal(map[string]any{"Records": out.Records})
	}
	invokeURL := fmt.Sprintf("http://localhost:%d/2015-03-31/functions/%s/invocations", p.port, url.PathEscape(m.FunctionName))
	req, err := http.NewRequestWithContext(ctx, "POST", invokeURL, bytes.NewReader(state.pending))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "Lambda.Invoke")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Amz-Function-Error") != "" {
		slog.Warn("Lambda stream batch failed", "function", m.FunctionName)
		return
	}
	if err := p.store.SaveStreamCheckpoint(m.UUID, shard, StreamCheckpoint{generation, state.sequence}); err != nil {
		return
	}
	state.pending = nil
	state.iterator = state.next
}
func extractQueueNameFromArn(arn string) string {
	parts := strings.Split(arn, ":")
	return parts[len(parts)-1]
}
