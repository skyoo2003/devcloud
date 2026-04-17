// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
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

// DynamoStore is a BadgerDB-backed store for DynamoDB tables and items.
type DynamoStore struct {
	db     *badger.DB
	tables map[string]*TableInfo
	mu     sync.RWMutex
}

const metaPrefix = "_meta/"
const itemPrefix = "_item/"
const gsiPrefix = "_gsi/"
const ttlPrefix = "_ttl/"
const tagsPrefix = "_tags/"
const streamPrefix = "_stream/"

// NewDynamoStore opens (or creates) a BadgerDB at dir and loads existing table metadata.
func NewDynamoStore(dir string) (*DynamoStore, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	s := &DynamoStore{
		db:     db,
		tables: make(map[string]*TableInfo),
	}

	// Load existing table metadata from the database.
	if err := s.loadTableMeta(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load table metadata: %w", err)
	}

	return s, nil
}

// loadTableMeta reads all _meta/ keys and populates the in-memory tables map.
func (s *DynamoStore) loadTableMeta() error {
	return s.db.View(func(txn *badger.Txn) error {
		prefix := []byte(metaPrefix)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				var info TableInfo
				if err := json.Unmarshal(v, &info); err != nil {
					return err
				}
				s.tables[info.Name] = &info
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close closes the underlying BadgerDB.
func (s *DynamoStore) Close() error {
	return s.db.Close()
}

// metaKey returns the BadgerDB key for a table's metadata record.
func metaKey(tableName string) []byte {
	return []byte(metaPrefix + tableName)
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

	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(info.Name), data)
	}); err != nil {
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

	// Collect all item keys belonging to this table.
	tableItemPrefix := []byte(itemPrefix + name + "/")
	var keysToDelete [][]byte

	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = tableItemPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, k)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan items for deletion: %w", err)
	}

	// Delete metadata and all items in a single batch.
	keysToDelete = append(keysToDelete, metaKey(name))

	if err := s.db.Update(func(txn *badger.Txn) error {
		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("delete table records: %w", err)
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
	copy := *info
	return &copy, nil
}

// itemKey builds the BadgerDB key for an item.
// Format: _item/tableName/partitionKeyValue  or  _item/tableName/partitionKeyValue/sortKeyValue
func itemKey(tableName, pk string, sk ...string) []byte {
	key := itemPrefix + tableName + "/" + pk
	if len(sk) > 0 && sk[0] != "" {
		key += "/" + sk[0]
	}
	return []byte(key)
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

// PutItem stores an item in the specified table.
// The item key is derived from the table's partition key (and optional sort key) values.
func (s *DynamoStore) PutItem(tableName string, item Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()

	if !exists {
		return ErrTableNotFound
	}

	pkAttr, ok := item[table.PartitionKey.Name]
	if !ok {
		return fmt.Errorf("item missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return fmt.Errorf("partition key value: %w", err)
	}

	var key []byte
	if table.SortKey != nil {
		skAttr, ok := item[table.SortKey.Name]
		if !ok {
			return fmt.Errorf("item missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return fmt.Errorf("sort key value: %w", err)
		}
		key = itemKey(tableName, pkVal, skVal)
	} else {
		key = itemKey(tableName, pkVal)
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, data); err != nil {
			return err
		}
		// Also update GSI/LSI entries.
		for _, idx := range table.GlobalSecondaryIndexes {
			if err := writeGSIEntry(txn, tableName, idx, item, data); err != nil {
				return err
			}
		}
		for _, idx := range table.LocalSecondaryIndexes {
			if err := writeGSIEntry(txn, tableName, idx, item, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetItem retrieves an item by its key attributes. Returns ErrItemNotFound if missing.
func (s *DynamoStore) GetItem(tableName string, key Item) (*Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	pkAttr, ok := key[table.PartitionKey.Name]
	if !ok {
		return nil, fmt.Errorf("key missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("partition key value: %w", err)
	}

	var dbKey []byte
	if table.SortKey != nil {
		skAttr, ok := key[table.SortKey.Name]
		if !ok {
			return nil, fmt.Errorf("key missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return nil, fmt.Errorf("sort key value: %w", err)
		}
		dbKey = itemKey(tableName, pkVal, skVal)
	} else {
		dbKey = itemKey(tableName, pkVal)
	}

	var result Item
	err = s.db.View(func(txn *badger.Txn) error {
		dbItem, err := txn.Get(dbKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrItemNotFound
			}
			return err
		}
		return dbItem.Value(func(v []byte) error {
			return json.Unmarshal(v, &result)
		})
	})
	if err != nil {
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

	pkAttr, ok := key[table.PartitionKey.Name]
	if !ok {
		return fmt.Errorf("key missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return fmt.Errorf("partition key value: %w", err)
	}

	var dbKey []byte
	if table.SortKey != nil {
		skAttr, ok := key[table.SortKey.Name]
		if !ok {
			return fmt.Errorf("key missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return fmt.Errorf("sort key value: %w", err)
		}
		dbKey = itemKey(tableName, pkVal, skVal)
	} else {
		dbKey = itemKey(tableName, pkVal)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(dbKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrItemNotFound
			}
			return err
		}
		return txn.Delete(dbKey)
	})
}

// Query performs a prefix scan for items with the given partitionKeyValue.
// If sortKeyPrefix is non-empty, only items whose sort key starts with sortKeyPrefix are returned.
func (s *DynamoStore) Query(tableName string, partitionKeyValue string, sortKeyPrefix string) ([]Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	// For partition-key-only tables, do an exact key lookup since items
	// are stored as _item/tableName/pkValue (no trailing slash).
	if table.SortKey == nil {
		var items []Item
		err := s.db.View(func(txn *badger.Txn) error {
			key := itemKey(tableName, partitionKeyValue)
			dbItem, err := txn.Get(key)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return nil // no match is not an error
				}
				return err
			}
			return dbItem.Value(func(v []byte) error {
				var item Item
				if err := json.Unmarshal(v, &item); err != nil {
					return err
				}
				items = append(items, item)
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
		return items, nil
	}

	// For tables with sort key, prefix scan: _item/tableName/partitionKeyValue/
	prefix := itemPrefix + tableName + "/" + partitionKeyValue + "/"
	if sortKeyPrefix != "" {
		prefix += sortKeyPrefix
	}

	var items []Item
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(v []byte) error {
				var item Item
				if err := json.Unmarshal(v, &item); err != nil {
					return err
				}
				items = append(items, item)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// Scan returns all items in a table.
func (s *DynamoStore) Scan(tableName string) ([]Item, error) {
	s.mu.RLock()
	_, exists := s.tables[tableName]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrTableNotFound
	}

	prefix := []byte(itemPrefix + tableName + "/")

	var items []Item
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Skip sub-prefix keys that aren't direct items (none expected, but be safe).
			keyStr := string(it.Item().Key())
			// Only include items that belong directly to this table prefix.
			// Strip the table item prefix and check there's actual content.
			rest := strings.TrimPrefix(keyStr, string(prefix))
			if rest == "" {
				continue
			}

			if err := it.Item().Value(func(v []byte) error {
				var item Item
				if err := json.Unmarshal(v, &item); err != nil {
					return err
				}
				items = append(items, item)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// UpdateTable updates billing mode and throughput stored in metadata.
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
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(name), data)
	})
}

// PutTTLConfig stores TTL config for a table.
func (s *DynamoStore) PutTTLConfig(tableName string, cfg TTLConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(ttlPrefix+tableName), data)
	})
}

// GetTTLConfig retrieves TTL config for a table.
func (s *DynamoStore) GetTTLConfig(tableName string) (*TTLConfig, error) {
	var cfg TTLConfig
	err := s.db.View(func(txn *badger.Txn) error {
		dbItem, err := txn.Get([]byte(ttlPrefix + tableName))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				cfg = TTLConfig{}
				return nil
			}
			return err
		}
		return dbItem.Value(func(v []byte) error {
			return json.Unmarshal(v, &cfg)
		})
	})
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// GetTags retrieves tags for a resource ARN.
func (s *DynamoStore) GetTags(resourceArn string) (map[string]string, error) {
	tags := make(map[string]string)
	err := s.db.View(func(txn *badger.Txn) error {
		dbItem, err := txn.Get([]byte(tagsPrefix + resourceArn))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return dbItem.Value(func(v []byte) error {
			return json.Unmarshal(v, &tags)
		})
	})
	if err != nil {
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
	data, err := json.Marshal(existing)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(tagsPrefix+resourceArn), data)
	})
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
	data, err := json.Marshal(existing)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(tagsPrefix+resourceArn), data)
	})
}

// QueryGSI scans items under the GSI prefix for a given index and partition key value.
func (s *DynamoStore) QueryGSI(tableName, indexName, pkValue string) ([]Item, error) {
	s.mu.RLock()
	_, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}

	prefix := []byte(gsiPrefix + tableName + "/" + indexName + "/" + pkValue + "/")
	var items []Item
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(v []byte) error {
				var item Item
				if err := json.Unmarshal(v, &item); err != nil {
					return err
				}
				items = append(items, item)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return items, err
}

// PutItemWithGSI stores an item and updates GSI/LSI entries.
func (s *DynamoStore) PutItemWithGSI(tableName string, item Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return ErrTableNotFound
	}

	pkAttr, ok := item[table.PartitionKey.Name]
	if !ok {
		return fmt.Errorf("item missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return fmt.Errorf("partition key value: %w", err)
	}

	var key []byte
	if table.SortKey != nil {
		skAttr, ok := item[table.SortKey.Name]
		if !ok {
			return fmt.Errorf("item missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return fmt.Errorf("sort key value: %w", err)
		}
		key = itemKey(tableName, pkVal, skVal)
	} else {
		key = itemKey(tableName, pkVal)
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, data); err != nil {
			return err
		}
		for _, idx := range table.GlobalSecondaryIndexes {
			if err := writeGSIEntry(txn, tableName, idx, item, data); err != nil {
				return err
			}
		}
		for _, idx := range table.LocalSecondaryIndexes {
			if err := writeGSIEntry(txn, tableName, idx, item, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// writeGSIEntry writes an index entry for the item. itemPKVal and itemSKVal
// are the main table primary key values — used to ensure uniqueness within
// the GSI partition even when multiple items share the same GSI partition key.
func writeGSIEntry(txn *badger.Txn, tableName string, idx IndexDef, item Item, data []byte) error {
	var gsiPK, gsiSK string
	for _, ks := range idx.KeySchema {
		av, ok := item[ks.Name]
		if !ok {
			return nil
		}
		val, err := attributeStringValue(av)
		if err != nil {
			return nil
		}
		if ks.KeyType == "HASH" {
			gsiPK = val
		} else {
			gsiSK = val
		}
	}
	if gsiPK == "" {
		return nil // no GSI partition key found in item, skip
	}

	// Build a unique key within the GSI using the full item key bytes as a suffix.
	// Key format: _gsi/{table}/{index}/{gsiPK}/{gsiSK or _}/{itemKey}
	// To derive itemKey, look at all fields in item to build a deterministic suffix.
	// We use a hash of the serialized item key attributes for uniqueness.
	// Simpler approach: store by gsiPK/gsiSK, but append a unique sub-key based on
	// all item key fields to avoid collision.
	//
	// We encode the item's partition key value from whatever fields exist.
	// Since we don't have table schema here, use the raw data bytes hash.
	// Actually, build a suffix from the data content (or just the first 32 bytes of hash).
	//
	// Simplest unique approach: use gsiPK/gsiSK/itemPrimaryKeyEncoded where we
	// compute a deterministic suffix from the item data.
	// Use a simple approach: take gsiPK and gsiSK, then append a unique per-item suffix
	// by hashing. For emulator purposes, use the JSON data length + first bytes.
	//
	// Best approach for correctness: caller passes the item's main table key.
	// We already marshal item data, just use the full data hash.
	// For simplicity in the emulator, use a fixed hash from item data.
	suffix := uniqueSuffix(data)
	var gsiKey string
	if gsiSK != "" {
		gsiKey = gsiPrefix + tableName + "/" + idx.IndexName + "/" + gsiPK + "/" + gsiSK + "/" + suffix
	} else {
		gsiKey = gsiPrefix + tableName + "/" + idx.IndexName + "/" + gsiPK + "/_/" + suffix
	}
	return txn.Set([]byte(gsiKey), data)
}

// uniqueSuffix creates a short unique string from item data to prevent GSI key collisions.
func uniqueSuffix(data []byte) string {
	// Simple FNV-like hash for uniqueness.
	var h uint32 = 2166136261
	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619
	}
	return fmt.Sprintf("%08x", h)
}

// ExecTransaction executes a function within a BadgerDB write transaction.
func (s *DynamoStore) ExecTransaction(fn func(txn *badger.Txn) error) error {
	return s.db.Update(fn)
}

// PutItemTxn stores an item within an existing BadgerDB transaction.
func (s *DynamoStore) PutItemTxn(txn *badger.Txn, tableName string, item Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return ErrTableNotFound
	}

	pkAttr, ok := item[table.PartitionKey.Name]
	if !ok {
		return fmt.Errorf("item missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return fmt.Errorf("partition key value: %w", err)
	}

	var key []byte
	if table.SortKey != nil {
		skAttr, ok := item[table.SortKey.Name]
		if !ok {
			return fmt.Errorf("item missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return fmt.Errorf("sort key value: %w", err)
		}
		key = itemKey(tableName, pkVal, skVal)
	} else {
		key = itemKey(tableName, pkVal)
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal item: %w", err)
	}
	return txn.Set(key, data)
}

// DeleteItemTxn removes an item within an existing BadgerDB transaction.
func (s *DynamoStore) DeleteItemTxn(txn *badger.Txn, tableName string, key Item) error {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return ErrTableNotFound
	}

	pkAttr, ok := key[table.PartitionKey.Name]
	if !ok {
		return fmt.Errorf("key missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return fmt.Errorf("partition key value: %w", err)
	}

	var dbKey []byte
	if table.SortKey != nil {
		skAttr, ok := key[table.SortKey.Name]
		if !ok {
			return fmt.Errorf("key missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return fmt.Errorf("sort key value: %w", err)
		}
		dbKey = itemKey(tableName, pkVal, skVal)
	} else {
		dbKey = itemKey(tableName, pkVal)
	}
	return txn.Delete(dbKey)
}

// GetItemTxn retrieves an item within an existing BadgerDB transaction.
func (s *DynamoStore) GetItemTxn(txn *badger.Txn, tableName string, key Item) (*Item, error) {
	s.mu.RLock()
	table, exists := s.tables[tableName]
	s.mu.RUnlock()
	if !exists {
		return nil, ErrTableNotFound
	}

	pkAttr, ok := key[table.PartitionKey.Name]
	if !ok {
		return nil, fmt.Errorf("key missing partition key attribute %q", table.PartitionKey.Name)
	}
	pkVal, err := attributeStringValue(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("partition key value: %w", err)
	}

	var dbKey []byte
	if table.SortKey != nil {
		skAttr, ok := key[table.SortKey.Name]
		if !ok {
			return nil, fmt.Errorf("key missing sort key attribute %q", table.SortKey.Name)
		}
		skVal, err := attributeStringValue(skAttr)
		if err != nil {
			return nil, fmt.Errorf("sort key value: %w", err)
		}
		dbKey = itemKey(tableName, pkVal, skVal)
	} else {
		dbKey = itemKey(tableName, pkVal)
	}

	var result Item
	dbItem, err := txn.Get(dbKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrItemNotFound
		}
		return nil, err
	}
	if err := dbItem.Value(func(v []byte) error {
		return json.Unmarshal(v, &result)
	}); err != nil {
		return nil, err
	}
	return &result, nil
}
