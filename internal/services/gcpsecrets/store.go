// SPDX-License-Identifier: Apache-2.0

package gcpsecrets

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	ErrSecretNotFound      = errors.New("SecretNotFound")
	ErrSecretAlreadyExists = errors.New("SecretAlreadyExists")
	ErrVersionNotFound     = errors.New("VersionNotFound")
	ErrVersionDestroyed    = errors.New("VersionDestroyed")
)

type SecretVersion struct {
	Name        string `json:"name"`
	CreateTime  string `json:"createTime"`
	DestroyTime string `json:"destroyTime,omitempty"`
	State       string `json:"state"` // "ENABLED", "DISABLED", "DESTROYED"
	Payload     []byte `json:"-"`
}

type Secret struct {
	Name        string                 `json:"name"`
	CreateTime  string                 `json:"createTime"`
	Labels      map[string]string      `json:"labels,omitempty"`
	Versions    map[int]*SecretVersion `json:"-"`
	NextVersion int                    `json:"-"`
}

// Store is in-memory; nothing is written under the service data dir.
type Store struct {
	mu      sync.RWMutex
	secrets map[string]*Secret // key: "projects/{project}/secrets/{secretId}"
}

func NewStore() *Store {
	return &Store{
		secrets: make(map[string]*Secret),
	}
}

func (s *Store) secretKey(project, secretId string) string {
	return fmt.Sprintf("projects/%s/secrets/%s", project, secretId)
}

func (s *Store) CreateSecret(project, secretId string, labels map[string]string) (*Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.secretKey(project, secretId)
	if _, exists := s.secrets[key]; exists {
		return nil, ErrSecretAlreadyExists
	}

	sec := &Secret{
		Name:        key,
		CreateTime:  time.Now().UTC().Format(time.RFC3339),
		Labels:      labels,
		Versions:    make(map[int]*SecretVersion),
		NextVersion: 1,
	}
	s.secrets[key] = sec
	return sec, nil
}

func (s *Store) GetSecret(project, secretId string) (*Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.secretKey(project, secretId)
	sec, exists := s.secrets[key]
	if !exists {
		return nil, ErrSecretNotFound
	}
	return sec, nil
}

func (s *Store) ListSecrets(project string) ([]*Secret, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := fmt.Sprintf("projects/%s/secrets/", project)
	var list []*Secret
	for k, sec := range s.secrets {
		if project == "" || len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			list = append(list, sec)
		}
	}
	return list, nil
}

func (s *Store) DeleteSecret(project, secretId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.secretKey(project, secretId)
	if _, exists := s.secrets[key]; !exists {
		return ErrSecretNotFound
	}
	delete(s.secrets, key)
	return nil
}

func (s *Store) AddSecretVersion(project, secretId string, payload []byte) (*SecretVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.secretKey(project, secretId)
	sec, exists := s.secrets[key]
	if !exists {
		return nil, ErrSecretNotFound
	}

	vNum := sec.NextVersion
	sec.NextVersion++

	v := &SecretVersion{
		Name:       fmt.Sprintf("%s/versions/%d", key, vNum),
		CreateTime: time.Now().UTC().Format(time.RFC3339),
		State:      "ENABLED",
		Payload:    payload,
	}
	sec.Versions[vNum] = v
	cp := *v
	return &cp, nil
}

func (s *Store) ListSecretVersions(project, secretId string) ([]*SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.secretKey(project, secretId)
	sec, exists := s.secrets[key]
	if !exists {
		return nil, ErrSecretNotFound
	}

	var list []*SecretVersion
	for i := 1; i < sec.NextVersion; i++ {
		if v, ok := sec.Versions[i]; ok {
			cp := *v // copied under the lock, as in GetSecretVersion
			list = append(list, &cp)
		}
	}
	return list, nil
}

// GetSecretVersion returns a copy taken under the lock: DestroySecretVersion
// mutates the stored version, and callers read and marshal it unlocked. Every
// method handing out a *SecretVersion does the same.
func (s *Store) GetSecretVersion(project, secretId, versionId string) (*SecretVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, err := s.lookupVersion(project, secretId, versionId)
	if err != nil {
		return nil, err
	}
	cp := *v
	return &cp, nil
}

// lookupVersion expects s.mu held.
func (s *Store) lookupVersion(project, secretId, versionId string) (*SecretVersion, error) {
	sec, exists := s.secrets[s.secretKey(project, secretId)]
	if !exists {
		return nil, ErrSecretNotFound
	}

	vNum, err := s.resolveVersionNumber(sec, versionId)
	if err != nil {
		return nil, err
	}

	v, exists := sec.Versions[vNum]
	if !exists {
		return nil, ErrVersionNotFound
	}
	return v, nil
}

func (s *Store) AccessSecretVersion(project, secretId, versionId string) (*SecretVersion, []byte, error) {
	v, err := s.GetSecretVersion(project, secretId, versionId)
	if err != nil {
		return nil, nil, err
	}
	if v.State == "DESTROYED" {
		return nil, nil, ErrVersionDestroyed
	}
	return v, v.Payload, nil
}

func (s *Store) DestroySecretVersion(project, secretId, versionId string) (*SecretVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, err := s.lookupVersion(project, secretId, versionId)
	if err != nil {
		return nil, err
	}

	v.State = "DESTROYED"
	v.DestroyTime = time.Now().UTC().Format(time.RFC3339)
	v.Payload = nil
	cp := *v
	return &cp, nil
}

func (s *Store) resolveVersionNumber(sec *Secret, versionId string) (int, error) {
	// "latest" is the most recently created version whatever its state, as in
	// Secret Manager: once it is destroyed, access fails rather than quietly
	// serving the older value it replaced.
	if versionId == "latest" {
		if sec.NextVersion <= 1 {
			return 0, ErrVersionNotFound
		}
		return sec.NextVersion - 1, nil
	}

	vNum, err := strconv.Atoi(versionId)
	if err != nil {
		return 0, ErrVersionNotFound
	}
	return vNum, nil
}
