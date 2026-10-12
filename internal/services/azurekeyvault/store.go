// SPDX-License-Identifier: Apache-2.0

package azurekeyvault

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrSecretNotFound = errors.New("SecretNotFound")
	ErrKeyNotFound    = errors.New("KeyNotFound")
)

type SecretAttributes struct {
	Enabled int64 `json:"enabled,omitempty"` // 1 or 0 in boolean terms, or bool in JSON
	Created int64 `json:"created,omitempty"`
	Updated int64 `json:"updated,omitempty"`
}

type SecretBundle struct {
	ID          string            `json:"id"`
	Value       string            `json:"value,omitempty"`
	ContentType string            `json:"contentType,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Attributes  map[string]any    `json:"attributes,omitempty"`
}

type KeyBundle struct {
	Key struct {
		Kid    string   `json:"kid"`
		Kty    string   `json:"kty"`
		KeyOps []string `json:"key_ops,omitempty"`
		N      string   `json:"n,omitempty"`
		E      string   `json:"e,omitempty"`
	} `json:"key"`
	Attributes map[string]any    `json:"attributes,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
}

type SecretItem struct {
	ID         string            `json:"id"`
	Attributes map[string]any    `json:"attributes,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
}

type KeyItem struct {
	Kid        string            `json:"kid"`
	Attributes map[string]any    `json:"attributes,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
}

type Store struct {
	mu      sync.RWMutex
	secrets map[string]map[string]*SecretBundle // vault -> secretName -> latest
	history map[string][]*SecretBundle          // vault/secretName -> versions
	keys    map[string]map[string]*KeyBundle    // vault -> keyName -> key
}

func NewStore() *Store {
	return &Store{
		secrets: make(map[string]map[string]*SecretBundle),
		history: make(map[string][]*SecretBundle),
		keys:    make(map[string]map[string]*KeyBundle),
	}
}

func (s *Store) SetSecret(vault, name, value, contentType string, tags map[string]string) (*SecretBundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.secrets[vault]; !exists {
		s.secrets[vault] = make(map[string]*SecretBundle)
	}

	version := randomHex(16)
	now := time.Now().Unix()
	id := fmt.Sprintf("https://%s.vault.azure.net/secrets/%s/%s", vault, name, version)

	bundle := &SecretBundle{
		ID:          id,
		Value:       value,
		ContentType: contentType,
		Tags:        tags,
		Attributes: map[string]any{
			"enabled": true,
			"created": now,
			"updated": now,
		},
	}

	s.secrets[vault][name] = bundle
	histKey := vault + "/" + name
	s.history[histKey] = append(s.history[histKey], bundle)

	return bundle, nil
}

func (s *Store) GetSecret(vault, name, version string) (*SecretBundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vaultSecrets, ok := s.secrets[vault]
	if !ok {
		return nil, ErrSecretNotFound
	}

	if version == "" {
		sec, ok := vaultSecrets[name]
		if !ok {
			return nil, ErrSecretNotFound
		}
		return sec, nil
	}

	histKey := vault + "/" + name
	for _, v := range s.history[histKey] {
		if fmt.Sprintf("https://%s.vault.azure.net/secrets/%s/%s", vault, name, version) == v.ID {
			return v, nil
		}
	}
	return nil, ErrSecretNotFound
}

func (s *Store) ListSecrets(vault string) ([]*SecretItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vaultSecrets, ok := s.secrets[vault]
	if !ok {
		return []*SecretItem{}, nil
	}

	var items []*SecretItem
	for _, sec := range vaultSecrets {
		items = append(items, &SecretItem{
			ID:         sec.ID,
			Attributes: sec.Attributes,
			Tags:       sec.Tags,
		})
	}
	return items, nil
}

func (s *Store) GetSecretVersions(vault, name string) ([]*SecretItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	histKey := vault + "/" + name
	history, ok := s.history[histKey]
	if !ok || len(history) == 0 {
		return nil, ErrSecretNotFound
	}

	var items []*SecretItem
	for _, sec := range history {
		items = append(items, &SecretItem{
			ID:         sec.ID,
			Attributes: sec.Attributes,
			Tags:       sec.Tags,
		})
	}
	return items, nil
}

func (s *Store) DeleteSecret(vault, name string) (*SecretBundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	vaultSecrets, ok := s.secrets[vault]
	if !ok {
		return nil, ErrSecretNotFound
	}
	sec, ok := vaultSecrets[name]
	if !ok {
		return nil, ErrSecretNotFound
	}

	delete(vaultSecrets, name)
	delete(s.history, vault+"/"+name)
	return sec, nil
}

func (s *Store) CreateKey(vault, name, kty string, keySize int) (*KeyBundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.keys[vault]; !exists {
		s.keys[vault] = make(map[string]*KeyBundle)
	}

	version := randomHex(16)
	kid := fmt.Sprintf("https://%s.vault.azure.net/keys/%s/%s", vault, name, version)

	if kty == "" {
		kty = "RSA"
	}

	kb := &KeyBundle{
		Attributes: map[string]any{
			"enabled": true,
			"created": time.Now().Unix(),
		},
	}
	kb.Key.Kid = kid
	kb.Key.Kty = kty
	kb.Key.KeyOps = []string{"encrypt", "decrypt", "sign", "verify", "wrapKey", "unwrapKey"}
	kb.Key.N = randomHex(32)
	kb.Key.E = "AQAB"

	s.keys[vault][name] = kb
	return kb, nil
}

func (s *Store) GetKey(vault, name string) (*KeyBundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vaultKeys, ok := s.keys[vault]
	if !ok {
		return nil, ErrKeyNotFound
	}
	k, ok := vaultKeys[name]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return k, nil
}

func (s *Store) ListKeys(vault string) ([]*KeyItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	vaultKeys, ok := s.keys[vault]
	if !ok {
		return []*KeyItem{}, nil
	}

	var items []*KeyItem
	for _, k := range vaultKeys {
		items = append(items, &KeyItem{
			Kid:        k.Key.Kid,
			Attributes: k.Attributes,
		})
	}
	return items, nil
}

func (s *Store) DeleteKey(vault, name string) (*KeyBundle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	vaultKeys, ok := s.keys[vault]
	if !ok {
		return nil, ErrKeyNotFound
	}
	k, ok := vaultKeys[name]
	if !ok {
		return nil, ErrKeyNotFound
	}
	delete(vaultKeys, name)
	return k, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
