// SPDX-License-Identifier: Apache-2.0

package dynamodb

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

// Sentinel errors for DynamoStore operations.
var (
	ErrTableAlreadyExists = errors.New("table already exists")
	ErrTableNotFound      = errors.New("table not found")
	ErrItemNotFound       = errors.New("item not found")
)

// KeyDef describes a DynamoDB key attribute definition.
// Type must be "S" (string), "N" (number), or "B" (binary).
// KeyType is "HASH" or "RANGE" (used for index key schemas).
type KeyDef struct {
	Name    string `json:"Name"`
	Type    string `json:"Type"`    // attribute type: S, N, B
	KeyType string `json:"KeyType"` // HASH or RANGE
}

// IndexDef describes a Global or Local Secondary Index.
type IndexDef struct {
	IndexName      string   `json:"IndexName"`
	KeySchema      []KeyDef `json:"KeySchema"`
	ProjectionType string   `json:"ProjectionType"` // ALL, KEYS_ONLY, INCLUDE
}

// TTLConfig stores TTL configuration for a table.
type TTLConfig struct {
	AttributeName string `json:"AttributeName"`
	Enabled       bool   `json:"Enabled"`
}

// StreamSpec captures the DynamoDB StreamSpecification attached to a table.
type StreamSpec struct {
	Enabled  bool   `json:"Enabled"`
	ViewType string `json:"ViewType,omitempty"`
}

// TableInfo holds metadata for a DynamoDB table.
type TableInfo struct {
	Name                   string     `json:"Name"`
	PartitionKey           KeyDef     `json:"PartitionKey"`
	SortKey                *KeyDef    `json:"SortKey,omitempty"`
	Status                 string     `json:"Status"`
	CreatedAt              time.Time  `json:"CreatedAt"`
	GlobalSecondaryIndexes []IndexDef `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []IndexDef `json:"LocalSecondaryIndexes,omitempty"`
	BillingMode            string     `json:"BillingMode,omitempty"`
	TableArn               string     `json:"TableArn,omitempty"`
	StreamSpec             StreamSpec `json:"StreamSpecification,omitempty"`
	LatestStreamArn        string     `json:"LatestStreamArn,omitempty"`
	LatestStreamLabel      string     `json:"LatestStreamLabel,omitempty"`
}

// AttributeValue mirrors the DynamoDB AttributeValue union type.
type AttributeValue struct {
	S    *string                    `json:"S,omitempty"`
	N    *string                    `json:"N,omitempty"`
	B    []byte                     `json:"B,omitempty"`
	BOOL *bool                      `json:"BOOL,omitempty"`
	NULL *bool                      `json:"NULL,omitempty"`
	L    []*AttributeValue          `json:"L,omitempty"`
	M    map[string]*AttributeValue `json:"M,omitempty"`
	SS   []string                   `json:"SS,omitempty"` // String Set
	NS   []string                   `json:"NS,omitempty"` // Number Set
	BS   [][]byte                   `json:"BS,omitempty"` // Binary Set
}

// Item is a DynamoDB item: a map of attribute names to values.
type Item map[string]*AttributeValue

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS ddb_tables (
			name TEXT PRIMARY KEY,
			info TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ddb_items (
			table_name TEXT NOT NULL,
			pk         TEXT NOT NULL,
			sk         TEXT NOT NULL DEFAULT '',
			data       TEXT NOT NULL,
			PRIMARY KEY (table_name, pk, sk)
		);
		CREATE TABLE IF NOT EXISTS ddb_ttl (
			table_name TEXT PRIMARY KEY,
			config     TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ddb_tags (
			resource_arn TEXT PRIMARY KEY,
			tags         TEXT NOT NULL
		);
	`},
}

// DynamoStore is a SQLite-backed store for DynamoDB tables and items. Table
// metadata is cached in memory (loaded on open); items live in SQLite.
type DynamoStore struct {
	db     *sqlite.Store
	tables map[string]*TableInfo
	mu     sync.RWMutex
}

// NewDynamoStore opens (or creates) a SQLite database under dir and loads
// existing table metadata.
func NewDynamoStore(dir string) (*DynamoStore, error) {
	db, err := sqlite.Open(filepath.Join(dir, "dynamodb.db"), migrations)
	if err != nil {
		return nil, fmt.Errorf("open dynamodb store: %w", err)
	}

	s := &DynamoStore{
		db:     db,
		tables: make(map[string]*TableInfo),
	}

	if err := s.loadTableMeta(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load table metadata: %w", err)
	}

	return s, nil
}

// loadTableMeta reads all persisted table metadata into the in-memory map.
func (s *DynamoStore) loadTableMeta() error {
	rows, err := s.db.DB().Query(`SELECT info FROM ddb_tables`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var info TableInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			return err
		}
		s.tables[info.Name] = &info
	}
	return rows.Err()
}

// Close closes the underlying database.
func (s *DynamoStore) Close() error {
	return s.db.Close()
}

// attributeStringValue extracts the string representation of an AttributeValue key field.
func attributeStringValue(av *AttributeValue) (string, error) {
	if av == nil {
		return "", fmt.Errorf("attribute value is nil")
	}
	if av.S != nil {
		return *av.S, nil
	}
	if av.N != nil {
		return *av.N, nil
	}
	if av.B != nil {
		return string(av.B), nil
	}
	return "", fmt.Errorf("unsupported attribute value type for key")
}

// keyValues derives the partition and (optional) sort key string values for an
// item or key map, given its table's schema.
func keyValues(table *TableInfo, item Item) (pk, sk string, err error) {
	pkAttr, ok := item[table.PartitionKey.Name]
	if !ok {
		return "", "", fmt.Errorf("missing partition key attribute %q", table.PartitionKey.Name)
	}
	pk, err = attributeStringValue(pkAttr)
	if err != nil {
		return "", "", fmt.Errorf("partition key value: %w", err)
	}
	if table.SortKey != nil {
		skAttr, ok := item[table.SortKey.Name]
		if !ok {
			return "", "", fmt.Errorf("missing sort key attribute %q", table.SortKey.Name)
		}
		sk, err = attributeStringValue(skAttr)
		if err != nil {
			return "", "", fmt.Errorf("sort key value: %w", err)
		}
	}
	return pk, sk, nil
}

// CreateTable creates a new table. Returns ErrTableAlreadyExists if the table exists.
func (s *DynamoStore) CreateTable(info TableInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[info.Name]; exists {
		return ErrTableAlreadyExists
	}

	if info.Status == "" {
		info.Status = "ACTIVE"
	}
	if info.CreatedAt.IsZero() {
		info.CreatedAt = time.Now().UTC()
	}
	if info.TableArn == "" {
		info.TableArn = fmt.Sprintf("arn:aws:dynamodb:us-east-1:123456789012:table/%s", info.Name)
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal table info: %w", err)
	}

	if _, err := s.db.DB().Exec(
		`INSERT OR REPLACE INTO ddb_tables (name, info) VALUES (?, ?)`, info.Name, string(data)); err != nil {
		return fmt.Errorf("persist table metadata: %w", err)
	}

	s.tables[info.Name] = &info
	return nil
}

// DeleteTable removes a table and all its items. Returns ErrTableNotFound if missing.
func (s *DynamoStore) DeleteTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[name]; !exists {
		return ErrTableNotFound
	}

	// Delete items and metadata atomically so a mid-way failure can't leave
	// orphaned rows.
	tx, err := s.db.DB().Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM ddb_items WHERE table_name = ?`, name); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete table items: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM ddb_tables WHERE name = ?`, name); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete table metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete table: %w", err)
	}

	delete(s.tables, name)
	return nil
}

// ListTables returns the names of all tables.
func (s *DynamoStore) ListTables() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	return names
}

// GetTable returns metadata for the named table. Returns ErrTableNotFound if missing.
func (s *DynamoStore) GetTable(name string) (*TableInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, exists := s.tables[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	cp := *info
	return &cp, nil
}

// UpdateTable applies updates to a table's metadata and persists them.
func (s *DynamoStore) UpdateTable(name string, updates func(*TableInfo)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, exists := s.tables[name]
	if !exists {
		return ErrTableNotFound
	}
	updates(info)
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal table info: %w", err)
	}
	_, err = s.db.DB().Exec(`UPDATE ddb_tables SET info = ? WHERE name = ?`, string(data), name)
	return err
}

// PutItem stores (upserts) an item in the specified table. The item key is
// derived from the table's partition key (and optional sort key) values.
func (s *DynamoStore) PutItem(tableName string, item Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return ErrTableNotFound
	}

	pk, sk, err := keyValues(table, item)
	if err != nil {
		return err
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}

	_, err = s.db.DB().Exec(
		`INSERT OR REPLACE INTO ddb_items (table_name, pk, sk, data) VALUES (?, ?, ?, ?)`,
		tableName, pk, sk, string(data))
	return err
}

// GetItem retrieves an item by its key attributes. Returns ErrItemNotFound if missing.
func (s *DynamoStore) GetItem(tableName string, key Item) (*Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}

	pk, sk, err := keyValues(table, key)
	if err != nil {
		return nil, err
	}

	var raw string
	err = s.db.DB().QueryRow(
		`SELECT data FROM ddb_items WHERE table_name = ? AND pk = ? AND sk = ?`,
		tableName, pk, sk).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrItemNotFound
	}
	if err != nil {
		return nil, err
	}

	var result Item
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteItem removes an item by its key attributes. Returns ErrItemNotFound if missing.
func (s *DynamoStore) DeleteItem(tableName string, key Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return ErrTableNotFound
	}

	pk, sk, err := keyValues(table, key)
	if err != nil {
		return err
	}

	res, err := s.db.DB().Exec(
		`DELETE FROM ddb_items WHERE table_name = ? AND pk = ? AND sk = ?`, tableName, pk, sk)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrItemNotFound
	}
	return nil
}

// Query returns items with the given partition key value, ordered by sort key.
// If sortKeyPrefix is non-empty, only items whose sort key starts with it are returned.
func (s *DynamoStore) Query(tableName string, partitionKeyValue string, sortKeyPrefix string) ([]Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}

	query := `SELECT data FROM ddb_items WHERE table_name = ? AND pk = ?`
	args := []any{tableName, partitionKeyValue}
	if table.SortKey != nil && sortKeyPrefix != "" {
		query += ` AND sk LIKE ? ESCAPE '\'`
		args = append(args, escapeLike(sortKeyPrefix)+"%")
	}
	query += ` ORDER BY sk`
	return s.queryItems(query, args)
}

// Scan returns all items in a table.
func (s *DynamoStore) Scan(tableName string) ([]Item, error) {
	s.mu.RLock()
	_, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}
	return s.queryItems(`SELECT data FROM ddb_items WHERE table_name = ? ORDER BY pk, sk`, []any{tableName})
}

// QueryGSI returns items whose value for the index's HASH key attribute equals
// pkValue. Secondary indexes are projected on the fly from the base table.
//
// ponytail: O(n) full-table scan filtered in memory — fine at emulator scale.
// If a table ever holds enough items to matter, index the GSI hash attribute
// with a generated column (json_extract) and query it directly.
func (s *DynamoStore) QueryGSI(tableName, indexName, pkValue string) ([]Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}

	hashAttr := indexHashKey(table, indexName)
	if hashAttr == "" {
		// Unknown index → empty result, not an error. This matches the prior
		// behavior and how the provider surfaces it (an empty Query response).
		// Returning an error would become an opaque 500; a real
		// ValidationException belongs in the provider, not the store.
		return nil, nil
	}

	all, err := s.queryItems(`SELECT data FROM ddb_items WHERE table_name = ?`, []any{tableName})
	if err != nil {
		return nil, err
	}
	var items []Item
	for _, item := range all {
		av, ok := item[hashAttr]
		if !ok {
			continue
		}
		if v, err := attributeStringValue(av); err == nil && v == pkValue {
			items = append(items, item)
		}
	}
	return items, nil
}

// indexHashKey returns the HASH key attribute name for the named GSI or LSI,
// or "" if no such index exists.
func indexHashKey(table *TableInfo, indexName string) string {
	for _, idx := range append(table.GlobalSecondaryIndexes, table.LocalSecondaryIndexes...) {
		if idx.IndexName != indexName {
			continue
		}
		for _, ks := range idx.KeySchema {
			if ks.KeyType == "HASH" {
				return ks.Name
			}
		}
	}
	return ""
}

// queryItems runs a SELECT that returns a single "data" column of JSON items.
func (s *DynamoStore) queryItems(query string, args []any) ([]Item, error) {
	rows, err := s.db.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var items []Item
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var item Item
		if err := json.Unmarshal([]byte(raw), &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// escapeLike escapes LIKE metacharacters so a prefix matches literally.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// PutTTLConfig stores TTL config for a table.
func (s *DynamoStore) PutTTLConfig(tableName string, cfg TTLConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = s.db.DB().Exec(
		`INSERT OR REPLACE INTO ddb_ttl (table_name, config) VALUES (?, ?)`, tableName, string(data))
	return err
}

// GetTTLConfig retrieves TTL config for a table. Returns an empty config if unset.
func (s *DynamoStore) GetTTLConfig(tableName string) (*TTLConfig, error) {
	var raw string
	err := s.db.DB().QueryRow(`SELECT config FROM ddb_ttl WHERE table_name = ?`, tableName).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return &TTLConfig{}, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg TTLConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// GetTags retrieves tags for a resource ARN.
func (s *DynamoStore) GetTags(resourceArn string) (map[string]string, error) {
	tags := make(map[string]string)
	var raw string
	err := s.db.DB().QueryRow(`SELECT tags FROM ddb_tags WHERE resource_arn = ?`, resourceArn).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return tags, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(raw), &tags); err != nil {
		return nil, err
	}
	return tags, nil
}

// PutTags stores tags for a resource ARN (merges with existing).
func (s *DynamoStore) PutTags(resourceArn string, newTags map[string]string) error {
	existing, err := s.GetTags(resourceArn)
	if err != nil {
		return err
	}
	for k, v := range newTags {
		existing[k] = v
	}
	return s.writeTags(resourceArn, existing)
}

// RemoveTags removes specified tag keys for a resource ARN.
func (s *DynamoStore) RemoveTags(resourceArn string, tagKeys []string) error {
	existing, err := s.GetTags(resourceArn)
	if err != nil {
		return err
	}
	for _, k := range tagKeys {
		delete(existing, k)
	}
	return s.writeTags(resourceArn, existing)
}

func (s *DynamoStore) writeTags(resourceArn string, tags map[string]string) error {
	data, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	_, err = s.db.DB().Exec(
		`INSERT OR REPLACE INTO ddb_tags (resource_arn, tags) VALUES (?, ?)`, resourceArn, string(data))
	return err
}
