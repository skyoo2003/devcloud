// SPDX-License-Identifier: Apache-2.0

// internal/services/dynamodbstreams/store.go
package dynamodbstreams

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errStreamNotFound = errors.New("stream not found")

// ErrStreamNotFound is exported so external packages can identify missing streams.
var ErrStreamNotFound = errStreamNotFound

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS streams (
            arn             TEXT PRIMARY KEY,
            table_name      TEXT NOT NULL,
            label           TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'ENABLED',
            view_type       TEXT NOT NULL DEFAULT 'NEW_AND_OLD_IMAGES',
            created_at      INTEGER NOT NULL
        );
    `},
	{Version: 2, SQL: `
        CREATE TABLE IF NOT EXISTS stream_tags (
            stream_arn TEXT NOT NULL,
            tag_key    TEXT NOT NULL,
            tag_value  TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (stream_arn, tag_key)
        );
    `},
	{Version: 3, SQL: `
        CREATE INDEX IF NOT EXISTS idx_streams_table_name ON streams(table_name);
    `},
}

// StreamMeta holds DynamoDB Streams metadata stored in SQLite.
type StreamMeta struct {
	ARN       string
	TableName string
	Label     string
	Status    string
	ViewType  string
	CreatedAt time.Time
}

// StreamRecord represents a change record in the emulated stream.
type StreamRecord struct {
	EventID     string
	EventName   string // INSERT, MODIFY, REMOVE
	Keys        map[string]any
	NewImage    map[string]any
	OldImage    map[string]any
	SequenceNum string
	Timestamp   time.Time
	ViewType    string
	SizeBytes   int
}

// ShardBuffer holds records for a single shard.
type ShardBuffer struct {
	ID      string
	Records []StreamRecord
}

// Iterator tracks a consumer's read position in a shard.
type Iterator struct {
	StreamARN string
	ShardID   string
	Position  int
}

// StreamBuffer holds in-memory shard and iterator state.
type StreamBuffer struct {
	mu        sync.RWMutex
	shards    map[string]*ShardBuffer // shardID -> ShardBuffer
	iterators map[string]*Iterator    // iteratorID -> Iterator
	iterSeq   int64
	seqCtr    uint64
}

func newStreamBuffer() *StreamBuffer {
	return &StreamBuffer{
		shards:    make(map[string]*ShardBuffer),
		iterators: make(map[string]*Iterator),
	}
}

// streamShardID returns the canonical default shard ID for a stream ARN.
func streamShardID(streamARN string) string {
	return fmt.Sprintf("shardId-00000000000000000000-00000001")
}

// Store combines SQLite persistence with in-memory shard state.
type Store struct {
	sqlStore *sqlite.Store
	buf      *StreamBuffer
	// tableIndex provides a fast lookup of latest stream by table name for write-path integration.
	tableIndex sync.Map // map[tableName]string (ARN)
}

// globalStore exposes the most recently initialized Store so other packages
// (e.g. dynamodb) can publish change records without a circular import. The
// dynamodbstreams provider never imports dynamodb, so dynamodb may freely
// import dynamodbstreams.
var (
	globalStoreMu sync.RWMutex
	globalStore   *Store
)

// SetGlobalStore records the active store for cross-package publishing.
func SetGlobalStore(s *Store) {
	globalStoreMu.Lock()
	globalStore = s
	globalStoreMu.Unlock()
}

// GetGlobalStore returns the active global store (or nil if streams is not loaded).
func GetGlobalStore() *Store {
	globalStoreMu.RLock()
	defer globalStoreMu.RUnlock()
	return globalStore
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "dynamodbstreams.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	store := &Store{sqlStore: s, buf: newStreamBuffer()}
	// Rehydrate the table index from persistent metadata so tables created
	// before restart still publish correctly.
	if err := store.rehydrateIndex(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.sqlStore.Close() }

// rehydrateIndex loads table->ARN mappings from the streams table for ENABLED streams.
func (s *Store) rehydrateIndex() error {
	rows, err := s.sqlStore.DB().Query(
		`SELECT arn, table_name FROM streams WHERE status = 'ENABLED' ORDER BY created_at`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var arn, table string
		if err := rows.Scan(&arn, &table); err != nil {
			return err
		}
		s.tableIndex.Store(table, arn)
		// Also allocate an in-memory shard for the stream.
		shardID := streamShardID(arn)
		s.buf.mu.Lock()
		if _, ok := s.buf.shards[shardID]; !ok {
			s.buf.shards[shardID] = &ShardBuffer{ID: shardID}
		}
		s.buf.mu.Unlock()
	}
	return rows.Err()
}

// CreateStream inserts a new stream and allocates its default shard.
func (s *Store) CreateStream(arn, tableName, label, viewType string) (*StreamMeta, error) {
	now := time.Now().Unix()
	if viewType == "" {
		viewType = "NEW_AND_OLD_IMAGES"
	}
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO streams (arn, table_name, label, view_type, created_at)
         VALUES (?, ?, ?, ?, ?)`,
		arn, tableName, label, viewType, now,
	)
	if err != nil {
		return nil, err
	}
	// Allocate default shard in-memory.
	shardID := streamShardID(arn)
	s.buf.mu.Lock()
	s.buf.shards[shardID] = &ShardBuffer{ID: shardID}
	s.buf.mu.Unlock()

	// Track the active stream for this table (replaces any prior mapping).
	s.tableIndex.Store(tableName, arn)

	return &StreamMeta{
		ARN: arn, TableName: tableName, Label: label,
		Status: "ENABLED", ViewType: viewType,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetStreamByARN(arn string) (*StreamMeta, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT arn, table_name, label, status, view_type, created_at
         FROM streams WHERE arn = ?`, arn)
	return scanStream(row)
}

func (s *Store) ListStreams(tableName string) ([]StreamMeta, error) {
	query := `SELECT arn, table_name, label, status, view_type, created_at FROM streams`
	var args []any
	if tableName != "" {
		query += ` WHERE table_name = ?`
		args = append(args, tableName)
	}
	query += ` ORDER BY created_at`
	rows, err := s.sqlStore.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var streams []StreamMeta
	for rows.Next() {
		st, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		streams = append(streams, *st)
	}
	return streams, rows.Err()
}

// GetLatestStreamForTable returns the most recent stream ARN for a table name,
// or empty string when no stream exists / stream is disabled.
func (s *Store) GetLatestStreamForTable(tableName string) (string, bool) {
	v, ok := s.tableIndex.Load(tableName)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// SetStreamStatus updates the ENABLED/DISABLING/DISABLED status for a stream.
func (s *Store) SetStreamStatus(arn, status string) error {
	res, err := s.sqlStore.DB().Exec(`UPDATE streams SET status = ? WHERE arn = ?`, status, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	// Keep the table index aligned: remove mapping when disabled.
	st, err := s.GetStreamByARN(arn)
	if err == nil {
		if status == "ENABLED" {
			s.tableIndex.Store(st.TableName, arn)
		} else {
			// Only clear when the current mapping still points at this ARN.
			if cur, ok := s.tableIndex.Load(st.TableName); ok && cur.(string) == arn {
				s.tableIndex.Delete(st.TableName)
			}
		}
	}
	return nil
}

// CreateIterator creates a shard iterator for a given stream/shard.
func (s *Store) CreateIterator(streamARN, shardID, iteratorType, startSeq string) (string, error) {
	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()

	shard, ok := s.buf.shards[shardID]
	if !ok {
		return "", fmt.Errorf("shard not found: %s", shardID)
	}

	var position int
	switch iteratorType {
	case "TRIM_HORIZON":
		position = 0
	case "LATEST":
		position = len(shard.Records)
	case "AT_SEQUENCE_NUMBER":
		position = findSeqPosition(shard.Records, startSeq, false)
	case "AFTER_SEQUENCE_NUMBER":
		position = findSeqPosition(shard.Records, startSeq, true)
	default:
		// Unknown type defaults to LATEST
		position = len(shard.Records)
	}

	s.buf.iterSeq++
	iterID := fmt.Sprintf("iter-%s-%s-%d", streamARN, shardID, s.buf.iterSeq)
	s.buf.iterators[iterID] = &Iterator{
		StreamARN: streamARN,
		ShardID:   shardID,
		Position:  position,
	}
	return iterID, nil
}

// GetRecords reads records from a shard iterator, returning records and next iterator.
func (s *Store) GetRecords(iteratorID string, limit int) ([]StreamRecord, string, error) {
	s.buf.mu.Lock()
	defer s.buf.mu.Unlock()

	iter, ok := s.buf.iterators[iteratorID]
	if !ok {
		return nil, "", fmt.Errorf("iterator not found or expired")
	}

	shard, ok := s.buf.shards[iter.ShardID]
	if !ok {
		return nil, "", fmt.Errorf("shard not found: %s", iter.ShardID)
	}

	if limit <= 0 {
		limit = 1000
	}
	start := iter.Position
	if start > len(shard.Records) {
		start = len(shard.Records)
	}
	end := start + limit
	if end > len(shard.Records) {
		end = len(shard.Records)
	}

	records := make([]StreamRecord, end-start)
	copy(records, shard.Records[start:end])

	delete(s.buf.iterators, iteratorID)
	s.buf.iterSeq++
	nextID := fmt.Sprintf("iter-%s-%s-%d", iter.StreamARN, iter.ShardID, s.buf.iterSeq)
	s.buf.iterators[nextID] = &Iterator{
		StreamARN: iter.StreamARN,
		ShardID:   iter.ShardID,
		Position:  end,
	}

	return records, nextID, nil
}

// PublishRecord appends a change event to the default shard of the stream
// associated with the given table. It is safe to call from any goroutine and
// is a no-op when the table has no active stream.
//
// The caller supplies the key attributes plus the pre- and post-write images
// already encoded as DynamoDB AttributeValue maps. Image fields not applicable
// for the stream's view type are trimmed here to match AWS semantics.
func (s *Store) PublishRecord(tableName, eventName string, keys, newImage, oldImage map[string]any) error {
	arn, ok := s.GetLatestStreamForTable(tableName)
	if !ok {
		return nil // no active stream, silently ignore
	}
	meta, err := s.GetStreamByARN(arn)
	if err != nil {
		return nil
	}
	if meta.Status != "ENABLED" {
		return nil
	}

	// Apply view type projection.
	var keepNew, keepOld bool
	switch meta.ViewType {
	case "KEYS_ONLY":
		keepNew, keepOld = false, false
	case "NEW_IMAGE":
		keepNew, keepOld = true, false
	case "OLD_IMAGE":
		keepNew, keepOld = false, true
	case "NEW_AND_OLD_IMAGES":
		keepNew, keepOld = true, true
	default:
		keepNew, keepOld = true, true
	}

	rec := StreamRecord{
		EventID:     fmt.Sprintf("%x", atomic.AddUint64(&s.buf.seqCtr, 1)),
		EventName:   eventName,
		Keys:        cloneMap(keys),
		SequenceNum: fmt.Sprintf("%021d", atomic.AddUint64(&s.buf.seqCtr, 1)),
		Timestamp:   time.Now().UTC(),
		ViewType:    meta.ViewType,
	}
	if keepNew {
		rec.NewImage = cloneMap(newImage)
	}
	if keepOld {
		rec.OldImage = cloneMap(oldImage)
	}
	// Approximate SizeBytes from the JSON encoding of the key+images.
	if b, err := json.Marshal(struct {
		K, N, O map[string]any
	}{rec.Keys, rec.NewImage, rec.OldImage}); err == nil {
		rec.SizeBytes = len(b)
	}

	shardID := streamShardID(arn)
	s.buf.mu.Lock()
	shard, ok := s.buf.shards[shardID]
	if !ok {
		shard = &ShardBuffer{ID: shardID}
		s.buf.shards[shardID] = shard
	}
	shard.Records = append(shard.Records, rec)
	s.buf.mu.Unlock()
	return nil
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// AddTags merges new tags for a stream ARN.
func (s *Store) AddTags(arn string, tags map[string]string) error {
	if _, err := s.GetStreamByARN(arn); err != nil {
		return err
	}
	tx, err := s.sqlStore.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(
		`INSERT INTO stream_tags(stream_arn, tag_key, tag_value) VALUES(?,?,?)
         ON CONFLICT(stream_arn, tag_key) DO UPDATE SET tag_value = excluded.tag_value`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for k, v := range tags {
		if _, err := stmt.Exec(arn, k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RemoveTags deletes tag keys for a stream ARN.
func (s *Store) RemoveTags(arn string, keys []string) error {
	if _, err := s.GetStreamByARN(arn); err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := s.sqlStore.DB().Exec(
			`DELETE FROM stream_tags WHERE stream_arn = ? AND tag_key = ?`, arn, k); err != nil {
			return err
		}
	}
	return nil
}

// ListTags returns all tags for a stream ARN.
func (s *Store) ListTags(arn string) (map[string]string, error) {
	if _, err := s.GetStreamByARN(arn); err != nil {
		return nil, err
	}
	rows, err := s.sqlStore.DB().Query(
		`SELECT tag_key, tag_value FROM stream_tags WHERE stream_arn = ? ORDER BY tag_key`, arn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// DefaultShardID returns the single default shard for a stream.
func DefaultShardID() string {
	return "shardId-00000000000000000000-00000001"
}

type scanner interface{ Scan(dest ...any) error }

func scanStream(sc scanner) (*StreamMeta, error) {
	var st StreamMeta
	var createdAt int64
	err := sc.Scan(&st.ARN, &st.TableName, &st.Label, &st.Status, &st.ViewType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStreamNotFound
		}
		return nil, err
	}
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

func findSeqPosition(records []StreamRecord, seqNum string, after bool) int {
	for i, r := range records {
		if r.SequenceNum == seqNum {
			if after {
				return i + 1
			}
			return i
		}
	}
	return len(records)
}
