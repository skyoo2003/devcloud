// SPDX-License-Identifier: Apache-2.0

package azuretable

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrTableNotFound       = errors.New("TableNotFound")
	ErrTableAlreadyExists  = errors.New("TableAlreadyExists")
	ErrEntityNotFound      = errors.New("EntityNotFound")
	ErrEntityAlreadyExists = errors.New("EntityAlreadyExists")
	ErrInvalidTableName    = errors.New("InvalidTableName")
)

type TableInfo struct {
	TableName string `json:"TableName"`
}

type Entity = map[string]any

type Store struct {
	mu    sync.RWMutex
	store *sqlite.Store
}

var migrations = []sqlite.Migration{
	{
		Version: 1,
		SQL: `
CREATE TABLE IF NOT EXISTS tables (
    account TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (account, name)
);

CREATE TABLE IF NOT EXISTS entities (
    account TEXT NOT NULL,
    table_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    row_key TEXT NOT NULL,
    data TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (account, table_name, partition_key, row_key)
);
CREATE INDEX IF NOT EXISTS idx_entities_table ON entities (account, table_name);
`,
	},
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "table.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error {
	return s.store.Close()
}

func (s *Store) CreateTable(account, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" || strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return ErrInvalidTableName
	}
	if account == "" {
		account = "devstoreaccount1"
	}

	var exists int
	err := s.store.DB().QueryRow("SELECT COUNT(*) FROM tables WHERE account = ? AND name = ?", account, name).Scan(&exists)
	if err != nil {
		return err
	}
	if exists > 0 {
		return ErrTableAlreadyExists
	}

	_, err = s.store.DB().Exec("INSERT INTO tables (account, name, created_at) VALUES (?, ?, ?)",
		account, name, time.Now().UTC())
	return err
}

func (s *Store) DeleteTable(account, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	res, err := s.store.DB().Exec("DELETE FROM tables WHERE account = ? AND name = ?", account, name)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return ErrTableNotFound
	}

	_, _ = s.store.DB().Exec("DELETE FROM entities WHERE account = ? AND table_name = ?", account, name)
	return nil
}

func (s *Store) ListTables(account string) ([]TableInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	rows, err := s.store.DB().Query("SELECT name FROM tables WHERE account = ? ORDER BY name ASC", account)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var list []TableInfo
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			list = append(list, TableInfo{TableName: name})
		}
	}
	return list, nil
}

func (s *Store) InsertEntity(account, tableName string, entity Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	// Verify table exists
	var tableCount int
	err := s.store.DB().QueryRow("SELECT COUNT(*) FROM tables WHERE account = ? AND name = ?", account, tableName).Scan(&tableCount)
	if err != nil || tableCount == 0 {
		return ErrTableNotFound
	}

	pk, _ := entity["PartitionKey"].(string)
	rk, _ := entity["RowKey"].(string)
	if pk == "" || rk == "" {
		return errors.New("PartitionKey and RowKey are required")
	}

	now := time.Now().UTC()
	etag := fmt.Sprintf("W/\"datetime'%s'\"", now.Format(time.RFC3339Nano))
	entity["Timestamp"] = now.Format(time.RFC3339)
	entity["odata.etag"] = etag

	dataBytes, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	var exists int
	_ = s.store.DB().QueryRow("SELECT COUNT(*) FROM entities WHERE account = ? AND table_name = ? AND partition_key = ? AND row_key = ?",
		account, tableName, pk, rk).Scan(&exists)
	if exists > 0 {
		return ErrEntityAlreadyExists
	}

	_, err = s.store.DB().Exec(
		"INSERT INTO entities (account, table_name, partition_key, row_key, data, timestamp, etag) VALUES (?, ?, ?, ?, ?, ?, ?)",
		account, tableName, pk, rk, string(dataBytes), now, etag)
	return err
}

func (s *Store) GetEntity(account, tableName, pk, rk string) (Entity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var dataStr string
	err := s.store.DB().QueryRow(
		"SELECT data FROM entities WHERE account = ? AND table_name = ? AND partition_key = ? AND row_key = ?",
		account, tableName, pk, rk).Scan(&dataStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEntityNotFound
		}
		return nil, err
	}

	var entity Entity
	if err := json.Unmarshal([]byte(dataStr), &entity); err != nil {
		return nil, err
	}
	return entity, nil
}

func (s *Store) UpdateEntity(account, tableName, pk, rk string, entity Entity, merge bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	var existingDataStr string
	err := s.store.DB().QueryRow(
		"SELECT data FROM entities WHERE account = ? AND table_name = ? AND partition_key = ? AND row_key = ?",
		account, tableName, pk, rk).Scan(&existingDataStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrEntityNotFound
		}
		return err
	}

	targetEntity := make(Entity)
	if merge {
		_ = json.Unmarshal([]byte(existingDataStr), &targetEntity)
	}
	for k, v := range entity {
		targetEntity[k] = v
	}
	targetEntity["PartitionKey"] = pk
	targetEntity["RowKey"] = rk
	now := time.Now().UTC()
	etag := fmt.Sprintf("W/\"datetime'%s'\"", now.Format(time.RFC3339Nano))
	targetEntity["Timestamp"] = now.Format(time.RFC3339)
	targetEntity["odata.etag"] = etag

	dataBytes, err := json.Marshal(targetEntity)
	if err != nil {
		return err
	}

	_, err = s.store.DB().Exec(
		"UPDATE entities SET data = ?, timestamp = ?, etag = ? WHERE account = ? AND table_name = ? AND partition_key = ? AND row_key = ?",
		string(dataBytes), now, etag, account, tableName, pk, rk)
	return err
}

func (s *Store) DeleteEntity(account, tableName, pk, rk string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	res, err := s.store.DB().Exec(
		"DELETE FROM entities WHERE account = ? AND table_name = ? AND partition_key = ? AND row_key = ?",
		account, tableName, pk, rk)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil || rows == 0 {
		return ErrEntityNotFound
	}
	return nil
}

func (s *Store) QueryEntities(account, tableName string) ([]Entity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if account == "" {
		account = "devstoreaccount1"
	}

	rows, err := s.store.DB().Query(
		"SELECT data FROM entities WHERE account = ? AND table_name = ? ORDER BY partition_key, row_key ASC",
		account, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var list []Entity
	for rows.Next() {
		var dataStr string
		if err := rows.Scan(&dataStr); err == nil {
			var ent Entity
			if json.Unmarshal([]byte(dataStr), &ent) == nil {
				list = append(list, ent)
			}
		}
	}
	return list, nil
}
