// SPDX-License-Identifier: Apache-2.0

package sqs

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Sentinel errors.
var (
	ErrQueueAlreadyExists = errors.New("queue already exists")
	ErrQueueNotFound      = errors.New("queue not found")
	ErrReceiptInvalid     = errors.New("receipt handle is invalid")
)

// QueueInfo holds metadata about an SQS queue.
type QueueInfo struct {
	Name      string
	URL       string
	AccountID string
	CreatedAt time.Time
}

// MessageAttribute represents a single SQS message attribute.
type MessageAttribute struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// Message represents a single SQS message.
type Message struct {
	MessageID         string
	Body              string
	MD5OfBody         string
	ReceiptHandle     string
	SentTimestamp     time.Time
	MessageAttributes map[string]MessageAttribute

	// FIFO fields
	MessageGroupID         string
	MessageDeduplicationID string
	SequenceNumber         string

	// invisibleUntil tracks when the message becomes visible again after a receive.
	invisibleUntil time.Time
	// deleted marks the message as permanently removed.
	deleted bool
	// receiveCount tracks how many times the message has been received.
	receiveCount int
}

// queue is an internal per-queue data structure.
type queue struct {
	info       QueueInfo
	messages   []*Message
	attributes map[string]string
	mu         sync.Mutex

	// Tags support
	tags map[string]string

	// DLQ support
	MaxReceiveCount     int
	DeadLetterTargetArn string

	// FIFO support
	IsFIFO            bool
	ContentBasedDedup bool
	dedupCache        map[string]time.Time // deduplicationID -> sent time
	seqCounter        int64
}

// QueueStore is a thread-safe in-memory store for SQS queues.
type QueueStore struct {
	mu     sync.RWMutex
	queues map[string]*queue // key: "accountID/queueName"
	port   int               // HTTP port for queue-URL construction
}

// NewQueueStore creates and returns an empty QueueStore bound to the given
// HTTP port, which is used when building queue URLs. Pass 0 to accept the
// default (4747).
func NewQueueStore(port int) *QueueStore {
	if port <= 0 {
		port = 4747
	}
	return &QueueStore{
		queues: make(map[string]*queue),
		port:   port,
	}
}

func queueKey(accountID, name string) string {
	return accountID + "/" + name
}

// QueueURL returns the canonical queue URL for the given account and queue
// name, honoring the configured server port.
func (s *QueueStore) QueueURL(accountID, name string) string {
	return fmt.Sprintf("http://localhost:%d/%s/%s", s.port, accountID, name)
}

// CreateQueue creates a new queue with the given name under accountID.
// Returns ErrQueueAlreadyExists if the queue already exists.
func (s *QueueStore) CreateQueue(name, accountID string) error {
	return s.CreateQueueWithAttributes(name, accountID, nil)
}

// CreateQueueWithAttributes creates a new queue with the given name, accountID, and initial attributes.
// Returns ErrQueueAlreadyExists if the queue already exists.
func (s *QueueStore) CreateQueueWithAttributes(name, accountID string, attrs map[string]string) error {
	key := queueKey(accountID, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[key]; exists {
		return ErrQueueAlreadyExists
	}

	isFIFO := strings.HasSuffix(name, ".fifo")
	contentBasedDedup := false
	if attrs != nil {
		if v, ok := attrs["FifoQueue"]; ok && v == "true" {
			isFIFO = true
		}
		if v, ok := attrs["ContentBasedDeduplication"]; ok && v == "true" {
			contentBasedDedup = true
		}
	}

	q := &queue{
		info: QueueInfo{
			Name:      name,
			URL:       s.QueueURL(accountID, name),
			AccountID: accountID,
			CreatedAt: time.Now(),
		},
		messages:          make([]*Message, 0),
		attributes:        make(map[string]string),
		tags:              make(map[string]string),
		IsFIFO:            isFIFO,
		ContentBasedDedup: contentBasedDedup,
		dedupCache:        make(map[string]time.Time),
	}

	// Store FIFO attributes so GetQueueAttributes returns them
	if isFIFO {
		q.attributes["FifoQueue"] = "true"
	}
	if contentBasedDedup {
		q.attributes["ContentBasedDeduplication"] = "true"
	}

	s.queues[key] = q
	return nil
}

// DeleteQueue removes the queue identified by name and accountID.
// Returns ErrQueueNotFound if it does not exist.
func (s *QueueStore) DeleteQueue(name, accountID string) error {
	key := queueKey(accountID, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[key]; !exists {
		return ErrQueueNotFound
	}

	delete(s.queues, key)
	return nil
}

// ListQueues returns the QueueInfo for all queues belonging to accountID whose
// name starts with prefix. An empty prefix matches all queues.
func (s *QueueStore) ListQueues(accountID, prefix string) []QueueInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []QueueInfo
	for _, q := range s.queues {
		if q.info.AccountID != accountID {
			continue
		}
		if prefix != "" && !strings.HasPrefix(q.info.Name, prefix) {
			continue
		}
		result = append(result, q.info)
	}
	return result
}

// GetQueueUrl returns the URL for the named queue under accountID.
// Returns ErrQueueNotFound if it does not exist.
func (s *QueueStore) GetQueueUrl(name, accountID string) (string, error) {
	key := queueKey(accountID, name)

	s.mu.RLock()
	defer s.mu.RUnlock()

	q, exists := s.queues[key]
	if !exists {
		return "", ErrQueueNotFound
	}
	return q.info.URL, nil
}

// randomID generates a cryptographically random hex string of n bytes.
func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

// md5Hex returns the hex-encoded MD5 of s.
func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", sum)
}

// sha256Hex returns the hex-encoded SHA-256 of s.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// SendMessage enqueues a message onto the named queue.
// Returns the generated MessageID and ErrQueueNotFound if the queue does not exist.
func (s *QueueStore) SendMessage(queueName, accountID, body string) (string, error) {
	return s.SendMessageWithAttributes(queueName, accountID, body, nil)
}

// SendMessageFIFOOptions holds FIFO-specific send options.
type SendMessageFIFOOptions struct {
	MessageGroupID         string
	MessageDeduplicationID string
}

// SendMessageWithAttributes enqueues a message with optional message attributes.
func (s *QueueStore) SendMessageWithAttributes(queueName, accountID, body string, attrs map[string]MessageAttribute) (string, error) {
	return s.SendMessageFull(queueName, accountID, body, attrs, SendMessageFIFOOptions{})
}

// SendMessageFull enqueues a message with all options (attributes + FIFO fields).
func (s *QueueStore) SendMessageFull(queueName, accountID, body string, attrs map[string]MessageAttribute, fifoOpts SendMessageFIFOOptions) (string, error) {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return "", ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// FIFO deduplication logic
	if q.IsFIFO {
		dedupID := fifoOpts.MessageDeduplicationID
		if dedupID == "" && q.ContentBasedDedup {
			dedupID = sha256Hex(body)
		}
		if dedupID != "" {
			// Clean up expired dedup cache entries (5 min window)
			now := time.Now()
			for k, t := range q.dedupCache {
				if now.Sub(t) > 5*time.Minute {
					delete(q.dedupCache, k)
				}
			}
			if sentAt, found := q.dedupCache[dedupID]; found {
				if time.Since(sentAt) < 5*time.Minute {
					// Duplicate: return success without enqueuing
					return randomID(16), nil
				}
			}
			q.dedupCache[dedupID] = now
		}
	}

	msgID := randomID(16)
	q.seqCounter++
	seqNum := fmt.Sprintf("%020d", q.seqCounter)

	msg := &Message{
		MessageID:              msgID,
		Body:                   body,
		MD5OfBody:              md5Hex(body),
		ReceiptHandle:          randomID(32),
		SentTimestamp:          time.Now(),
		MessageAttributes:      attrs,
		MessageGroupID:         fifoOpts.MessageGroupID,
		MessageDeduplicationID: fifoOpts.MessageDeduplicationID,
		SequenceNumber:         seqNum,
	}

	q.messages = append(q.messages, msg)

	return msgID, nil
}

// GetQueueAttributes returns attributes for the named queue.
func (s *QueueStore) GetQueueAttributes(queueName, accountID string, attributeNames []string) (map[string]string, error) {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Count visible messages
	now := time.Now()
	approxVisible := 0
	approxNotVisible := 0
	for _, msg := range q.messages {
		if msg.deleted {
			continue
		}
		if now.Before(msg.invisibleUntil) {
			approxNotVisible++
		} else {
			approxVisible++
		}
	}

	all := false
	requested := make(map[string]bool)
	for _, n := range attributeNames {
		if n == "All" {
			all = true
			break
		}
		requested[n] = true
	}

	result := make(map[string]string)

	// Built-in computed attributes
	builtIn := map[string]string{
		"QueueArn":                              fmt.Sprintf("arn:aws:sqs:us-east-1:%s:%s", accountID, queueName),
		"ApproximateNumberOfMessages":           fmt.Sprintf("%d", approxVisible),
		"ApproximateNumberOfMessagesNotVisible": fmt.Sprintf("%d", approxNotVisible),
		"CreatedTimestamp":                      fmt.Sprintf("%d", q.info.CreatedAt.Unix()),
		"LastModifiedTimestamp":                 fmt.Sprintf("%d", q.info.CreatedAt.Unix()),
		"VisibilityTimeout":                     "30",
		"MaximumMessageSize":                    "262144",
		"MessageRetentionPeriod":                "345600",
		"DelaySeconds":                          "0",
		"ReceiveMessageWaitTimeSeconds":         "0",
	}

	// Override built-in defaults with stored attributes
	for k, v := range q.attributes {
		builtIn[k] = v
	}

	for k, v := range builtIn {
		if all || requested[k] {
			result[k] = v
		}
	}

	return result, nil
}

// SetQueueAttributes sets attributes on the named queue.
func (s *QueueStore) SetQueueAttributes(queueName, accountID string, attrs map[string]string) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for k, v := range attrs {
		q.attributes[k] = v
		// Parse RedrivePolicy for DLQ support
		if k == "RedrivePolicy" {
			var policy struct {
				MaxReceiveCount     interface{} `json:"maxReceiveCount"`
				DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
			}
			if err := json.Unmarshal([]byte(v), &policy); err == nil {
				q.DeadLetterTargetArn = policy.DeadLetterTargetArn
				switch n := policy.MaxReceiveCount.(type) {
				case float64:
					q.MaxReceiveCount = int(n)
				case string:
					_, _ = fmt.Sscanf(n, "%d", &q.MaxReceiveCount)
				}
			}
		}
	}
	return nil
}

// PurgeQueue removes all messages from the named queue.
func (s *QueueStore) PurgeQueue(queueName, accountID string) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.messages = make([]*Message, 0)
	return nil
}

// ReceiveMessage returns up to maxMessages visible messages from the queue,
// making them invisible for visibilityTimeout seconds.
// Returns ErrQueueNotFound if the queue does not exist.
func (s *QueueStore) ReceiveMessage(queueName, accountID string, maxMessages, visibilityTimeout int) ([]*Message, error) {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrQueueNotFound
	}

	now := time.Now()
	invisibleUntil := now.Add(time.Duration(visibilityTimeout) * time.Second)

	// Look up DLQ if configured
	var dlqQueue *queue
	if q.MaxReceiveCount > 0 && q.DeadLetterTargetArn != "" {
		dlqName := arnToQueueName(q.DeadLetterTargetArn)
		if dlqName != "" {
			dlqKey := queueKey(accountID, dlqName)
			s.mu.RLock()
			dlqQueue = s.queues[dlqKey]
			s.mu.RUnlock()
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var result []*Message
	for _, msg := range q.messages {
		if len(result) >= maxMessages {
			break
		}
		if msg.deleted {
			continue
		}
		if now.Before(msg.invisibleUntil) {
			// Still within an active visibility window.
			continue
		}

		// Increment receive count
		msg.receiveCount++

		// Check DLQ threshold
		if q.MaxReceiveCount > 0 && msg.receiveCount > q.MaxReceiveCount && dlqQueue != nil {
			// Move to DLQ
			dlqQueue.mu.Lock()
			dlqMsg := &Message{
				MessageID:         msg.MessageID,
				Body:              msg.Body,
				MD5OfBody:         msg.MD5OfBody,
				ReceiptHandle:     randomID(32),
				SentTimestamp:     msg.SentTimestamp,
				MessageAttributes: msg.MessageAttributes,
			}
			dlqQueue.messages = append(dlqQueue.messages, dlqMsg)
			dlqQueue.mu.Unlock()
			msg.deleted = true
			continue
		}

		// Make the message invisible for the requested duration.
		msg.invisibleUntil = invisibleUntil
		// Generate a fresh receipt handle each time the message is delivered.
		msg.ReceiptHandle = randomID(32)
		result = append(result, msg)
	}

	return result, nil
}

// arnToQueueName extracts queue name from an ARN like arn:aws:sqs:region:accountID:queueName.
func arnToQueueName(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	return parts[5]
}

// ChangeMessageVisibility changes the visibility timeout of a message.
func (s *QueueStore) ChangeMessageVisibility(accountID, queueName, receiptHandle string, visibilityTimeout int) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, msg := range q.messages {
		if msg.ReceiptHandle == receiptHandle && !msg.deleted {
			msg.invisibleUntil = time.Now().Add(time.Duration(visibilityTimeout) * time.Second)
			return nil
		}
	}
	return errors.New("message not found")
}

// DeleteMessage permanently removes the message identified by receiptHandle
// from the named queue.
// Returns ErrQueueNotFound or ErrReceiptInvalid on failure.
func (s *QueueStore) DeleteMessage(queueName, accountID, receiptHandle string) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	found := false
	for _, msg := range q.messages {
		if msg.ReceiptHandle == receiptHandle {
			msg.deleted = true
			found = true
			break
		}
	}

	if !found {
		return ErrReceiptInvalid
	}

	// Compact: remove all deleted messages to prevent unbounded growth.
	alive := q.messages[:0]
	for _, msg := range q.messages {
		if !msg.deleted {
			alive = append(alive, msg)
		}
	}
	// Nil out tail entries so deleted messages can be garbage collected.
	for i := len(alive); i < len(q.messages); i++ {
		q.messages[i] = nil
	}
	q.messages = alive

	return nil
}

// TagQueue adds or updates tags on the named queue.
func (s *QueueStore) TagQueue(queueName, accountID string, tags map[string]string) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for k, v := range tags {
		q.tags[k] = v
	}
	return nil
}

// UntagQueue removes specified tag keys from the named queue.
func (s *QueueStore) UntagQueue(queueName, accountID string, tagKeys []string) error {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, k := range tagKeys {
		delete(q.tags, k)
	}
	return nil
}

// ListQueueTags returns all tags for the named queue.
func (s *QueueStore) ListQueueTags(queueName, accountID string) (map[string]string, error) {
	key := queueKey(accountID, queueName)

	s.mu.RLock()
	q, exists := s.queues[key]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	result := make(map[string]string, len(q.tags))
	for k, v := range q.tags {
		result[k] = v
	}
	return result, nil
}
