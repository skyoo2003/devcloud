// SPDX-License-Identifier: Apache-2.0

package gcppubsub

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTopicNotFound        = errors.New("TopicNotFound")
	ErrTopicAlreadyExists   = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound = errors.New("SubscriptionNotFound")
	ErrSubAlreadyExists     = errors.New("SubscriptionAlreadyExists")
)

type PubsubMessage struct {
	MessageID   string            `json:"messageId"`
	Data        string            `json:"data"` // base64
	Attributes  map[string]string `json:"attributes,omitempty"`
	PublishTime string            `json:"publishTime"`
}

type Topic struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

type Subscription struct {
	Name               string            `json:"name"`
	Topic              string            `json:"topic"`
	AckDeadlineSeconds int               `json:"ackDeadlineSeconds,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`

	mu              sync.Mutex
	unackedMessages map[string]*ReceivedMessage // ackID -> message
	pendingQueue    []*PubsubMessage
}

type ReceivedMessage struct {
	AckID        string        `json:"ackId"`
	Message      PubsubMessage `json:"message"`
	DeliveryTime time.Time     `json:"-"`
}

type Store struct {
	mu            sync.RWMutex
	topics        map[string]*Topic        // key: "projects/{project}/topics/{topic}"
	subscriptions map[string]*Subscription // key: "projects/{project}/subscriptions/{sub}"
}

func NewStore() *Store {
	return &Store{
		topics:        make(map[string]*Topic),
		subscriptions: make(map[string]*Subscription),
	}
}

func topicKey(project, topicId string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, topicId)
}

func subKey(project, subId string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", project, subId)
}

func (s *Store) CreateTopic(project, topicId string, labels map[string]string) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := topicKey(project, topicId)
	if _, exists := s.topics[key]; exists {
		return nil, ErrTopicAlreadyExists
	}

	t := &Topic{
		Name:   key,
		Labels: labels,
	}
	s.topics[key] = t
	return t, nil
}

func (s *Store) GetTopic(project, topicId string) (*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := topicKey(project, topicId)
	t, exists := s.topics[key]
	if !exists {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

func (s *Store) ListTopics(project string) ([]*Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := fmt.Sprintf("projects/%s/topics/", project)
	var list []*Topic
	for k, t := range s.topics {
		if project == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			list = append(list, t)
		}
	}
	return list, nil
}

func (s *Store) DeleteTopic(project, topicId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := topicKey(project, topicId)
	if _, exists := s.topics[key]; !exists {
		return ErrTopicNotFound
	}
	delete(s.topics, key)
	return nil
}

func (s *Store) Publish(project, topicId string, messages []PubsubMessage) ([]string, error) {
	s.mu.RLock()
	key := topicKey(project, topicId)
	_, exists := s.topics[key]
	if !exists {
		s.mu.RUnlock()
		return nil, ErrTopicNotFound
	}

	// Find all subscriptions matching this topic
	var matchedSubs []*Subscription
	for _, sub := range s.subscriptions {
		if sub.Topic == key {
			matchedSubs = append(matchedSubs, sub)
		}
	}
	s.mu.RUnlock()

	var messageIDs []string
	now := time.Now().UTC().Format(time.RFC3339)

	for _, m := range messages {
		id := randomMessageID()
		m.MessageID = id
		m.PublishTime = now
		messageIDs = append(messageIDs, id)

		for _, sub := range matchedSubs {
			sub.mu.Lock()
			msgCopy := m
			sub.pendingQueue = append(sub.pendingQueue, &msgCopy)
			sub.mu.Unlock()
		}
	}

	return messageIDs, nil
}

func (s *Store) CreateSubscription(project, subId, topicFullName string, ackDeadline int, labels map[string]string) (*Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := subKey(project, subId)
	if _, exists := s.subscriptions[key]; exists {
		return nil, ErrSubAlreadyExists
	}

	if ackDeadline <= 0 {
		ackDeadline = 10
	}

	sub := &Subscription{
		Name:               key,
		Topic:              topicFullName,
		AckDeadlineSeconds: ackDeadline,
		Labels:             labels,
		unackedMessages:    make(map[string]*ReceivedMessage),
	}
	s.subscriptions[key] = sub
	return sub, nil
}

func (s *Store) GetSubscription(project, subId string) (*Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := subKey(project, subId)
	sub, exists := s.subscriptions[key]
	if !exists {
		return nil, ErrSubscriptionNotFound
	}
	return sub, nil
}

func (s *Store) ListSubscriptions(project string) ([]*Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := fmt.Sprintf("projects/%s/subscriptions/", project)
	var list []*Subscription
	for k, sub := range s.subscriptions {
		if project == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			list = append(list, sub)
		}
	}
	return list, nil
}

func (s *Store) DeleteSubscription(project, subId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := subKey(project, subId)
	if _, exists := s.subscriptions[key]; !exists {
		return ErrSubscriptionNotFound
	}
	delete(s.subscriptions, key)
	return nil
}

func (s *Store) Pull(project, subId string, maxMessages int) ([]*ReceivedMessage, error) {
	s.mu.RLock()
	key := subKey(project, subId)
	sub, exists := s.subscriptions[key]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrSubscriptionNotFound
	}

	sub.mu.Lock()
	defer sub.mu.Unlock()

	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > len(sub.pendingQueue) {
		maxMessages = len(sub.pendingQueue)
	}

	var received []*ReceivedMessage
	for i := 0; i < maxMessages; i++ {
		msg := sub.pendingQueue[i]
		ackID := randomAckID()
		rec := &ReceivedMessage{
			AckID:        ackID,
			Message:      *msg,
			DeliveryTime: time.Now(),
		}
		sub.unackedMessages[ackID] = rec
		received = append(received, rec)
	}
	sub.pendingQueue = sub.pendingQueue[maxMessages:]

	return received, nil
}

func (s *Store) Acknowledge(project, subId string, ackIDs []string) error {
	s.mu.RLock()
	key := subKey(project, subId)
	sub, exists := s.subscriptions[key]
	s.mu.RUnlock()

	if !exists {
		return ErrSubscriptionNotFound
	}

	sub.mu.Lock()
	defer sub.mu.Unlock()

	for _, id := range ackIDs {
		delete(sub.unackedMessages, id)
	}
	return nil
}

func randomMessageID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d", time.Now().UnixNano()/1e6) + hex.EncodeToString(b[:4])
}

func randomAckID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
