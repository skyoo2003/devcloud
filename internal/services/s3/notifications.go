// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// notificationConfig mirrors the S3 NotificationConfiguration XML.
type notificationConfig struct {
	QueueConfigurations  []queueNotificationConfig  `xml:"QueueConfiguration"`
	LambdaConfigurations []lambdaNotificationConfig `xml:"CloudFunctionConfiguration"`
	TopicConfigurations  []topicNotificationConfig  `xml:"TopicConfiguration"`
}

type queueNotificationConfig struct {
	ID     string   `xml:"Id"`
	Queue  string   `xml:"Queue"`
	Events []string `xml:"Event"`
}

type lambdaNotificationConfig struct {
	ID            string   `xml:"Id"`
	CloudFunction string   `xml:"CloudFunction"`
	Events        []string `xml:"Event"`
}

type topicNotificationConfig struct {
	ID     string   `xml:"Id"`
	Topic  string   `xml:"Topic"`
	Events []string `xml:"Event"`
}

// s3EventRecord is the JSON representation of a single S3 event record.
type s3EventRecord struct {
	EventSource string   `json:"eventSource"`
	EventTime   string   `json:"eventTime"`
	EventName   string   `json:"eventName"`
	S3          s3Detail `json:"s3"`
}

type s3Detail struct {
	Bucket s3BucketRef `json:"bucket"`
	Object s3ObjectRef `json:"object"`
}

type s3BucketRef struct {
	Name string `json:"name"`
}

type s3ObjectRef struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// emitS3Event sends notifications to all configured SQS queues and Lambda
// functions that have subscribed to the given eventName pattern on bucket.
// The call is fire-and-forget: failures are only logged, never returned.
func (p *S3Provider) emitS3Event(ctx context.Context, bucket, key string, size int64, eventName string) {
	if p.serverPort == 0 {
		return
	}

	configXML, err := p.metaStore.GetBucketNotification(bucket, defaultAccountID)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return // no config — normal case
		}
		slog.Debug("s3 notification: could not read config", "bucket", bucket, "err", err)
		return
	}

	var cfg notificationConfig
	if xmlErr := xml.Unmarshal([]byte(configXML), &cfg); xmlErr != nil {
		slog.Debug("s3 notification: could not parse config", "bucket", bucket, "err", xmlErr)
		return
	}

	payload, _ := json.Marshal(map[string]any{
		"Records": []s3EventRecord{
			{
				EventSource: "aws:s3",
				EventTime:   time.Now().UTC().Format(time.RFC3339),
				EventName:   eventName,
				S3: s3Detail{
					Bucket: s3BucketRef{Name: bucket},
					Object: s3ObjectRef{Key: key, Size: size},
				},
			},
		},
	})

	baseURL := fmt.Sprintf("http://localhost:%d", p.serverPort)

	for _, qc := range cfg.QueueConfigurations {
		if !matchesEvent(qc.Events, eventName) {
			continue
		}
		queueName := extractNameFromARN(qc.Queue)
		go func(queueName string, payload []byte) {
			if err := sendToSQS(ctx, baseURL, queueName, payload); err != nil {
				slog.Debug("s3 notification: sqs send failed", "queue", queueName, "err", err)
			}
		}(queueName, payload)
	}

	for _, lc := range cfg.LambdaConfigurations {
		if !matchesEvent(lc.Events, eventName) {
			continue
		}
		fnName := extractNameFromARN(lc.CloudFunction)
		go func(fnName string, payload []byte) {
			if err := invokeLambda(ctx, baseURL, fnName, payload); err != nil {
				slog.Debug("s3 notification: lambda invoke failed", "function", fnName, "err", err)
			}
		}(fnName, payload)
	}
}

// matchesEvent returns true if any pattern in events matches the given eventName.
// Patterns may end with "*" as a wildcard suffix.
func matchesEvent(patterns []string, eventName string) bool {
	for _, p := range patterns {
		if p == eventName {
			return true
		}
		// e.g. "s3:ObjectCreated:*" matches "ObjectCreated:Put"
		if len(p) > 1 && p[len(p)-1] == '*' {
			prefix := p[:len(p)-1]
			if len(eventName) >= len(prefix) && eventName[:len(prefix)] == prefix {
				return true
			}
			// also compare against the full AWS event name format
			// patterns like "s3:ObjectCreated:*" vs event name "ObjectCreated:Put"
			const s3Prefix = "s3:"
			if len(prefix) > len(s3Prefix) && prefix[:len(s3Prefix)] == s3Prefix {
				shortPrefix := prefix[len(s3Prefix):]
				if len(eventName) >= len(shortPrefix) && eventName[:len(shortPrefix)] == shortPrefix {
					return true
				}
			}
		}
	}
	return false
}

// extractNameFromARN extracts the last segment of an ARN (queue name or function name).
func extractNameFromARN(arn string) string {
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == ':' || arn[i] == '/' {
			return arn[i+1:]
		}
	}
	return arn
}

// sendToSQS sends the JSON payload as a message to the named SQS queue.
func sendToSQS(ctx context.Context, baseURL, queueName string, payload []byte) error {
	body, _ := json.Marshal(map[string]any{
		"QueueUrl":    fmt.Sprintf("%s/000000000000/%s", baseURL, queueName),
		"MessageBody": string(payload),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sqs send: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// invokeLambda invokes a Lambda function with the given JSON payload.
func invokeLambda(ctx context.Context, baseURL, fnName string, payload []byte) error {
	url := fmt.Sprintf("%s/2015-03-31/functions/%s/invocations", baseURL, fnName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("lambda invoke: unexpected status %d", resp.StatusCode)
	}
	return nil
}
