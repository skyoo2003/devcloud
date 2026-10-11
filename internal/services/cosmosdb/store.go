// SPDX-License-Identifier: Apache-2.0

package cosmosdb

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrDatabaseNotFound       = errors.New("DatabaseNotFound")
	ErrDatabaseAlreadyExists  = errors.New("DatabaseAlreadyExists")
	ErrContainerNotFound      = errors.New("ContainerNotFound")
	ErrContainerAlreadyExists = errors.New("ContainerAlreadyExists")
	ErrDocumentNotFound       = errors.New("DocumentNotFound")
	ErrDocumentAlreadyExists  = errors.New("DocumentAlreadyExists")
)

type DatabaseInfo struct {
	ID   string `json:"id"`
	RID  string `json:"_rid"`
	TS   int64  `json:"_ts"`
	ETag string `json:"_etag"`
}

type ContainerInfo struct {
	ID           string `json:"id"`
	RID          string `json:"_rid"`
	TS           int64  `json:"_ts"`
	ETag         string `json:"_etag"`
	PartitionKey any    `json:"partitionKey,omitempty"`
}

type Document = map[string]any

type Store struct {
	mu    sync.RWMutex
	store *sqlite.Store
}

var migrations = []sqlite.Migration{
	{
		Version: 1,
		SQL: `
CREATE TABLE IF NOT EXISTS databases (
    account TEXT NOT NULL,
    id TEXT NOT NULL,
    rid TEXT NOT NULL,
    ts INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (account, id)
);

CREATE TABLE IF NOT EXISTS containers (
    account TEXT NOT NULL,
    database_id TEXT NOT NULL,
    id TEXT NOT NULL,
    rid TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    ts INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (account, database_id, id)
);

CREATE TABLE IF NOT EXISTS documents (
    account TEXT NOT NULL,
    database_id TEXT NOT NULL,
    container_id TEXT NOT NULL,
    id TEXT NOT NULL,
    rid TEXT NOT NULL,
    data TEXT NOT NULL,
    ts INTEGER NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (account, database_id, container_id, id)
);
CREATE INDEX IF NOT EXISTS idx_docs_container ON documents (account, database_id, container_id);
`,
	},
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "cosmos.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error {
	return s.store.Close()
}

func randomRID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Store) CreateDatabase(account, id string) (*DatabaseInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}
	if id == "" {
		return nil, errors.New("database id is required")
	}

	var count int
	_ = s.store.DB().QueryRow("SELECT COUNT(*) FROM databases WHERE account = ? AND id = ?", account, id).Scan(&count)
	if count > 0 {
		return nil, ErrDatabaseAlreadyExists
	}

	now := time.Now().UTC()
	rid := randomRID()
	etag := fmt.Sprintf("\"%s\"", randomRID())
	ts := now.Unix()

	_, err := s.store.DB().Exec("INSERT INTO databases (account, id, rid, ts, etag) VALUES (?, ?, ?, ?, ?)",
		account, id, rid, ts, etag)
	if err != nil {
		return nil, err
	}
	return &DatabaseInfo{ID: id, RID: rid, TS: ts, ETag: etag}, nil
}

func (s *Store) GetDatabase(account, id string) (*DatabaseInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var rid, etag string
	var ts int64
	err := s.store.DB().QueryRow("SELECT rid, ts, etag FROM databases WHERE account = ? AND id = ?", account, id).Scan(&rid, &ts, &etag)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDatabaseNotFound
		}
		return nil, err
	}
	return &DatabaseInfo{ID: id, RID: rid, TS: ts, ETag: etag}, nil
}

func (s *Store) DeleteDatabase(account, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	res, err := s.store.DB().Exec("DELETE FROM databases WHERE account = ? AND id = ?", account, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return ErrDatabaseNotFound
	}

	_, _ = s.store.DB().Exec("DELETE FROM containers WHERE account = ? AND database_id = ?", account, id)
	_, _ = s.store.DB().Exec("DELETE FROM documents WHERE account = ? AND database_id = ?", account, id)
	return nil
}

func (s *Store) ListDatabases(account string) ([]DatabaseInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	rows, err := s.store.DB().Query("SELECT id, rid, ts, etag FROM databases WHERE account = ? ORDER BY id ASC", account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []DatabaseInfo
	for rows.Next() {
		var id, rid, etag string
		var ts int64
		if err := rows.Scan(&id, &rid, &ts, &etag); err == nil {
			list = append(list, DatabaseInfo{ID: id, RID: rid, TS: ts, ETag: etag})
		}
	}
	return list, nil
}

func (s *Store) CreateContainer(account, dbID, id string, partitionKey any) (*ContainerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var dbCount int
	_ = s.store.DB().QueryRow("SELECT COUNT(*) FROM databases WHERE account = ? AND id = ?", account, dbID).Scan(&dbCount)
	if dbCount == 0 {
		return nil, ErrDatabaseNotFound
	}

	var count int
	_ = s.store.DB().QueryRow("SELECT COUNT(*) FROM containers WHERE account = ? AND database_id = ? AND id = ?",
		account, dbID, id).Scan(&count)
	if count > 0 {
		return nil, ErrContainerAlreadyExists
	}

	pkBytes, _ := json.Marshal(partitionKey)
	now := time.Now().UTC()
	rid := randomRID()
	etag := fmt.Sprintf("\"%s\"", randomRID())
	ts := now.Unix()

	_, err := s.store.DB().Exec("INSERT INTO containers (account, database_id, id, rid, partition_key, ts, etag) VALUES (?, ?, ?, ?, ?, ?, ?)",
		account, dbID, id, rid, string(pkBytes), ts, etag)
	if err != nil {
		return nil, err
	}
	return &ContainerInfo{ID: id, RID: rid, TS: ts, ETag: etag, PartitionKey: partitionKey}, nil
}

func (s *Store) GetContainer(account, dbID, id string) (*ContainerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var rid, pkStr, etag string
	var ts int64
	err := s.store.DB().QueryRow("SELECT rid, partition_key, ts, etag FROM containers WHERE account = ? AND database_id = ? AND id = ?",
		account, dbID, id).Scan(&rid, &pkStr, &ts, &etag)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrContainerNotFound
		}
		return nil, err
	}

	var pk any
	_ = json.Unmarshal([]byte(pkStr), &pk)
	return &ContainerInfo{ID: id, RID: rid, TS: ts, ETag: etag, PartitionKey: pk}, nil
}

func (s *Store) DeleteContainer(account, dbID, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	res, err := s.store.DB().Exec("DELETE FROM containers WHERE account = ? AND database_id = ? AND id = ?", account, dbID, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return ErrContainerNotFound
	}

	_, _ = s.store.DB().Exec("DELETE FROM documents WHERE account = ? AND database_id = ? AND container_id = ?", account, dbID, id)
	return nil
}

func (s *Store) ListContainers(account, dbID string) ([]ContainerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	rows, err := s.store.DB().Query("SELECT id, rid, partition_key, ts, etag FROM containers WHERE account = ? AND database_id = ? ORDER BY id ASC", account, dbID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ContainerInfo
	for rows.Next() {
		var id, rid, pkStr, etag string
		var ts int64
		if err := rows.Scan(&id, &rid, &pkStr, &ts, &etag); err == nil {
			var pk any
			_ = json.Unmarshal([]byte(pkStr), &pk)
			list = append(list, ContainerInfo{ID: id, RID: rid, TS: ts, ETag: etag, PartitionKey: pk})
		}
	}
	return list, nil
}

func (s *Store) CreateDocument(account, dbID, collID string, doc Document) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	id, _ := doc["id"].(string)
	if id == "" {
		return nil, errors.New("document id is required")
	}

	var count int
	_ = s.store.DB().QueryRow("SELECT COUNT(*) FROM documents WHERE account = ? AND database_id = ? AND container_id = ? AND id = ?",
		account, dbID, collID, id).Scan(&count)
	if count > 0 {
		return nil, ErrDocumentAlreadyExists
	}

	now := time.Now().UTC()
	rid := randomRID()
	etag := fmt.Sprintf("\"%s\"", randomRID())
	ts := now.Unix()

	doc["_rid"] = rid
	doc["_ts"] = ts
	doc["_etag"] = etag

	dataBytes, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	_, err = s.store.DB().Exec(
		"INSERT INTO documents (account, database_id, container_id, id, rid, data, ts, etag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		account, dbID, collID, id, rid, string(dataBytes), ts, etag)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (s *Store) GetDocument(account, dbID, collID, docID string) (Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var dataStr string
	err := s.store.DB().QueryRow(
		"SELECT data FROM documents WHERE account = ? AND database_id = ? AND container_id = ? AND id = ?",
		account, dbID, collID, docID).Scan(&dataStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDocumentNotFound
		}
		return nil, err
	}

	var doc Document
	if err := json.Unmarshal([]byte(dataStr), &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func (s *Store) ReplaceDocument(account, dbID, collID, docID string, doc Document) (Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	doc["id"] = docID
	now := time.Now().UTC()
	etag := fmt.Sprintf("\"%s\"", randomRID())
	ts := now.Unix()

	doc["_ts"] = ts
	doc["_etag"] = etag

	dataBytes, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	res, err := s.store.DB().Exec(
		"UPDATE documents SET data = ?, ts = ?, etag = ? WHERE account = ? AND database_id = ? AND container_id = ? AND id = ?",
		string(dataBytes), ts, etag, account, dbID, collID, docID)
	if err != nil {
		return nil, err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return nil, ErrDocumentNotFound
	}
	return doc, nil
}

func (s *Store) DeleteDocument(account, dbID, collID, docID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	res, err := s.store.DB().Exec(
		"DELETE FROM documents WHERE account = ? AND database_id = ? AND container_id = ? AND id = ?",
		account, dbID, collID, docID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return ErrDocumentNotFound
	}
	return nil
}

func (s *Store) ListDocuments(account, dbID, collID string) ([]Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	rows, err := s.store.DB().Query(
		"SELECT data FROM documents WHERE account = ? AND database_id = ? AND container_id = ? ORDER BY id ASC",
		account, dbID, collID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Document
	for rows.Next() {
		var dataStr string
		if err := rows.Scan(&dataStr); err == nil {
			var doc Document
			if json.Unmarshal([]byte(dataStr), &doc) == nil {
				list = append(list, doc)
			}
		}
	}
	return list, nil
}
