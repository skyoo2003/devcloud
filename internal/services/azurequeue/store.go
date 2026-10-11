// SPDX-License-Identifier: Apache-2.0

package azurequeue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrQueueNotFound      = errors.New("QueueNotFound")
	ErrQueueAlreadyExists = errors.New("QueueAlreadyExists")
	ErrMessageNotFound    = errors.New("MessageNotFound")
	ErrPopReceiptMismatch = errors.New("PopReceiptMismatch")
	ErrInvalidQueueName   = errors.New("InvalidQueueName")
)

type QueueMessage struct {
	ID              string
	Text            string
	InsertionTime   time.Time
	ExpirationTime  time.Time
	PopReceipt      string
	TimeNextVisible time.Time
	DequeueCount    int
}

type Queue struct {
	Name     string
	Account  string
	Metadata map[string]string
	Messages []*QueueMessage
}

type Store struct {
	mu      sync.RWMutex
	baseDir string
	queues  map[string]*Queue // key: "account/queue"
}

func NewStore(baseDir string) *Store {
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		abs = filepath.Clean(baseDir)
	}
	cleaned := filepath.Clean(abs)
	_ = os.MkdirAll(cleaned, 0755)
	return &Store{
		baseDir: cleaned,
		queues:  make(map[string]*Queue),
	}
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Store) queueKey(account, queue string) string {
	if account == "" {
		account = "devstoreaccount1"
	}
	return account + "/" + queue
}

func (s *Store) CreateQueue(account, queue string, metadata map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if queue == "" || strings.Contains(queue, "/") || strings.Contains(queue, "\\") || strings.Contains(queue, "..") {
		return ErrInvalidQueueName
	}

	key := s.queueKey(account, queue)
	if _, exists := s.queues[key]; exists {
		return ErrQueueAlreadyExists
	}

	if metadata == nil {
		metadata = make(map[string]string)
	}
	s.queues[key] = &Queue{
		Name:     queue,
		Account:  account,
		Metadata: metadata,
		Messages: make([]*QueueMessage, 0),
	}
	return nil
}

func (s *Store) DeleteQueue(account, queue string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	if _, exists := s.queues[key]; !exists {
		return ErrQueueNotFound
	}
	delete(s.queues, key)
	return nil
}

func (s *Store) ListQueues(account, prefix string) ([]*Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}
	var res []*Queue
	accPrefix := account + "/"
	for k, q := range s.queues {
		if strings.HasPrefix(k, accPrefix) {
			if prefix == "" || strings.HasPrefix(q.Name, prefix) {
				res = append(res, q)
			}
		}
	}
	return res, nil
}

func (s *Store) GetQueueMetadata(account, queue string) (map[string]string, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return nil, 0, ErrQueueNotFound
	}

	now := time.Now().UTC()
	count := 0
	for _, m := range q.Messages {
		if m.ExpirationTime.After(now) {
			count++
		}
	}
	return q.Metadata, count, nil
}

func (s *Store) SetQueueMetadata(account, queue string, meta map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return ErrQueueNotFound
	}
	q.Metadata = meta
	return nil
}

func (s *Store) PutMessage(account, queue, text string, visibilityTimeout, ttl time.Duration) (*QueueMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return nil, ErrQueueNotFound
	}

	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	now := time.Now().UTC()
	msg := &QueueMessage{
		ID:              randomID(),
		Text:            text,
		InsertionTime:   now,
		ExpirationTime:  now.Add(ttl),
		PopReceipt:      randomID(),
		TimeNextVisible: now.Add(visibilityTimeout),
		DequeueCount:    0,
	}
	q.Messages = append(q.Messages, msg)
	return msg, nil
}

func (s *Store) GetMessages(account, queue string, numMessages int, visibilityTimeout time.Duration) ([]*QueueMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return nil, ErrQueueNotFound
	}

	if numMessages <= 0 {
		numMessages = 1
	}
	if numMessages > 32 {
		numMessages = 32
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}

	now := time.Now().UTC()
	var visible []*QueueMessage
	for _, m := range q.Messages {
		if m.ExpirationTime.After(now) && m.TimeNextVisible.Before(now) {
			m.DequeueCount++
			m.PopReceipt = randomID()
			m.TimeNextVisible = now.Add(visibilityTimeout)
			visible = append(visible, m)
			if len(visible) >= numMessages {
				break
			}
		}
	}
	return visible, nil
}

func (s *Store) PeekMessages(account, queue string, numMessages int) ([]*QueueMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return nil, ErrQueueNotFound
	}

	if numMessages <= 0 {
		numMessages = 1
	}
	if numMessages > 32 {
		numMessages = 32
	}

	now := time.Now().UTC()
	var visible []*QueueMessage
	for _, m := range q.Messages {
		if m.ExpirationTime.After(now) && m.TimeNextVisible.Before(now) {
			visible = append(visible, m)
			if len(visible) >= numMessages {
				break
			}
		}
	}
	return visible, nil
}

func (s *Store) DeleteMessage(account, queue, messageID, popReceipt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return ErrQueueNotFound
	}

	idx := -1
	for i, m := range q.Messages {
		if m.ID == messageID {
			if popReceipt != "" && m.PopReceipt != popReceipt {
				return ErrPopReceiptMismatch
			}
			idx = i
			break
		}
	}
	if idx == -1 {
		return ErrMessageNotFound
	}

	q.Messages = append(q.Messages[:idx], q.Messages[idx+1:]...)
	return nil
}

func (s *Store) ClearMessages(account, queue string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.queueKey(account, queue)
	q, exists := s.queues[key]
	if !exists {
		return ErrQueueNotFound
	}
	q.Messages = make([]*QueueMessage, 0)
	return nil
}
