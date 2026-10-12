// SPDX-License-Identifier: Apache-2.0

package azureappconfig

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("KeyNotFound")
)

type KeyValue struct {
	ETag         string            `json:"etag"`
	Key          string            `json:"key"`
	Label        *string           `json:"label"`
	ContentType  string            `json:"content_type,omitempty"`
	Value        string            `json:"value"`
	LastModified string            `json:"last_modified"`
	Locked       bool              `json:"locked"`
	Tags         map[string]string `json:"tags,omitempty"`
}

type Store struct {
	mu    sync.RWMutex
	items map[string]map[string]*KeyValue // store -> "key\x00label" -> KeyValue
}

func NewStore() *Store {
	return &Store{
		items: make(map[string]map[string]*KeyValue),
	}
}

func itemID(key, label string) string {
	return fmt.Sprintf("%s\x00%s", key, label)
}

func randomETag() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Store) SetKeyValue(store, key string, label *string, value, contentType string, tags map[string]string) (*KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.items[store]; !ok {
		s.items[store] = make(map[string]*KeyValue)
	}

	lblVal := ""
	if label != nil {
		lblVal = *label
	}

	id := itemID(key, lblVal)
	kv := &KeyValue{
		ETag:         randomETag(),
		Key:          key,
		Label:        label,
		ContentType:  contentType,
		Value:        value,
		LastModified: time.Now().UTC().Format(time.RFC3339),
		Locked:       false,
		Tags:         tags,
	}

	s.items[store][id] = kv
	return kv, nil
}

func (s *Store) GetKeyValue(store, key string, label *string) (*KeyValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeMap, ok := s.items[store]
	if !ok {
		return nil, ErrKeyNotFound
	}

	lblVal := ""
	if label != nil {
		lblVal = *label
	}

	kv, ok := storeMap[itemID(key, lblVal)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return kv, nil
}

func (s *Store) DeleteKeyValue(store, key string, label *string) (*KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storeMap, ok := s.items[store]
	if !ok {
		return nil, ErrKeyNotFound
	}

	lblVal := ""
	if label != nil {
		lblVal = *label
	}

	id := itemID(key, lblVal)
	kv, ok := storeMap[id]
	if !ok {
		return nil, ErrKeyNotFound
	}

	delete(storeMap, id)
	return kv, nil
}

func (s *Store) ListKeyValues(store, keyFilter, labelFilter string) ([]*KeyValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	storeMap, ok := s.items[store]
	if !ok {
		return []*KeyValue{}, nil
	}

	var results []*KeyValue
	for _, kv := range storeMap {
		if !matchFilter(kv.Key, keyFilter) {
			continue
		}
		kvLabel := ""
		if kv.Label != nil {
			kvLabel = *kv.Label
		}
		if labelFilter != "" && labelFilter != "*" && !matchFilter(kvLabel, labelFilter) {
			continue
		}
		results = append(results, kv)
	}
	return results, nil
}

func matchFilter(val, filter string) bool {
	if filter == "" || filter == "*" {
		return true
	}
	if strings.HasSuffix(filter, "*") {
		prefix := strings.TrimSuffix(filter, "*")
		return strings.HasPrefix(val, prefix)
	}
	return val == filter
}
