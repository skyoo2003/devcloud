// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// EventSourcePoller polls SQS queues (and DynamoDB streams) that are registered
// as event source mappings, and invokes the configured Lambda functions.
type EventSourcePoller struct {
	store  *LambdaStore
	port   int
	stopCh chan struct{}
}

// NewEventSourcePoller creates a new EventSourcePoller backed by the given store.
func NewEventSourcePoller(store *LambdaStore, port int) *EventSourcePoller {
	return &EventSourcePoller{
		store:  store,
		port:   port,
		stopCh: make(chan struct{}),
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
func (p *EventSourcePoller) Stop() {
	select {
	case <-p.stopCh:
		// already closed
	default:
		close(p.stopCh)
	}
}

func (p *EventSourcePoller) poll(ctx context.Context) {
	mappings, err := p.store.ListEventSourceMappings(defaultAccountID, "")
	if err != nil || len(mappings) == 0 {
		return
	}
	for _, m := range mappings {
		if m.State != "Enabled" {
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
	queueName := extractQueueNameFromArn(m.EventSourceARN)
	queueURL := fmt.Sprintf("%s/000000000000/%s", baseURL, queueName)

	batchSize := m.BatchSize
	if batchSize <= 0 {
		batchSize = 10
	}

	// Receive messages from SQS.
	receiveBody, _ := json.Marshal(map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": batchSize,
		"WaitTimeSeconds":     0,
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
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

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
	invokeURL := fmt.Sprintf("%s/2015-03-31/functions/%s/invocations", baseURL, m.FunctionName)
	invokeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, invokeURL, bytes.NewReader(event))
	if err != nil {
		return
	}
	invokeReq.Header.Set("Content-Type", "application/json")
	invokeResp, err := http.DefaultClient.Do(invokeReq)
	if err != nil || invokeResp.StatusCode != http.StatusOK {
		if invokeResp != nil {
			invokeResp.Body.Close()
		}
		slog.Warn("lambda invoke failed for SQS event", "function", m.FunctionName, "err", err)
		return
	}
	invokeResp.Body.Close()

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
			delResp.Body.Close()
		}
	}

	slog.Debug("processed SQS messages for lambda", "function", m.FunctionName, "count", len(result.Messages))
}

// pollDynamoDBStream polls a DynamoDB stream and invokes the mapped Lambda
// function with the records. Uses the JSON 1.0 protocol via the local gateway.
func (p *EventSourcePoller) pollDynamoDBStream(ctx context.Context, m EventSourceMapping) {
	baseURL := fmt.Sprintf("http://localhost:%d", p.port)
	streamARN := m.EventSourceARN

	// Obtain a shard iterator. Cache the latest iterator per mapping on the store
	// via the EventSourceMapping's UUID key; simple approach: use DescribeStream
	// then GetShardIterator(TRIM_HORIZON) for the first shard each poll cycle.
	descBody, _ := json.Marshal(map[string]any{"StreamArn": streamARN})
	descReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(descBody))
	if err != nil {
		return
	}
	descReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	descReq.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810.DescribeStream")
	descResp, err := http.DefaultClient.Do(descReq)
	if err != nil || descResp.StatusCode != http.StatusOK {
		if descResp != nil {
			descResp.Body.Close()
		}
		return
	}
	var descOut struct {
		StreamDescription struct {
			Shards []struct {
				ShardId string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	_ = json.NewDecoder(descResp.Body).Decode(&descOut)
	descResp.Body.Close()
	if len(descOut.StreamDescription.Shards) == 0 {
		return
	}
	shardID := descOut.StreamDescription.Shards[0].ShardId

	iterBody, _ := json.Marshal(map[string]any{
		"StreamArn":         streamARN,
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	iterReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(iterBody))
	if err != nil {
		return
	}
	iterReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	iterReq.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810.GetShardIterator")
	iterResp, err := http.DefaultClient.Do(iterReq)
	if err != nil || iterResp.StatusCode != http.StatusOK {
		if iterResp != nil {
			iterResp.Body.Close()
		}
		return
	}
	var iterOut struct {
		ShardIterator string `json:"ShardIterator"`
	}
	_ = json.NewDecoder(iterResp.Body).Decode(&iterOut)
	iterResp.Body.Close()
	if iterOut.ShardIterator == "" {
		return
	}

	batchSize := m.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	recBody, _ := json.Marshal(map[string]any{
		"ShardIterator": iterOut.ShardIterator,
		"Limit":         batchSize,
	})
	recReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(recBody))
	if err != nil {
		return
	}
	recReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	recReq.Header.Set("X-Amz-Target", "DynamoDBStreams_20120810.GetRecords")
	recResp, err := http.DefaultClient.Do(recReq)
	if err != nil || recResp.StatusCode != http.StatusOK {
		if recResp != nil {
			recResp.Body.Close()
		}
		return
	}
	var recOut struct {
		Records []map[string]any `json:"Records"`
	}
	_ = json.NewDecoder(recResp.Body).Decode(&recOut)
	recResp.Body.Close()
	if len(recOut.Records) == 0 {
		return
	}

	// Build Lambda event payload with eventSourceARN on each record.
	for i := range recOut.Records {
		recOut.Records[i]["eventSourceARN"] = streamARN
	}
	event, _ := json.Marshal(map[string]any{"Records": recOut.Records})

	invokeURL := fmt.Sprintf("%s/2015-03-31/functions/%s/invocations", baseURL, m.FunctionName)
	invokeReq, err := http.NewRequestWithContext(ctx, http.MethodPost, invokeURL, bytes.NewReader(event))
	if err != nil {
		return
	}
	invokeReq.Header.Set("Content-Type", "application/json")
	invokeResp, err := http.DefaultClient.Do(invokeReq)
	if err != nil || invokeResp.StatusCode != http.StatusOK {
		if invokeResp != nil {
			invokeResp.Body.Close()
		}
		slog.Warn("lambda invoke failed for DynamoDB stream", "function", m.FunctionName, "err", err)
		return
	}
	invokeResp.Body.Close()
	slog.Debug("processed DynamoDB stream records for lambda", "function", m.FunctionName, "count", len(recOut.Records))
}

func extractQueueNameFromArn(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 6 {
		return parts[len(parts)-1]
	}
	return arn
}
