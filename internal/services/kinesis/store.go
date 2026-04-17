// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesis/store.go
package kinesis

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errStreamNotFound = errors.New("stream not found")
var errConsumerNotFound = errors.New("consumer not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS streams (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'ACTIVE',
			shard_count     INTEGER NOT NULL DEFAULT 1,
			retention_hours INTEGER NOT NULL DEFAULT 24,
			mode            TEXT NOT NULL DEFAULT 'PROVISIONED',
			encryption      TEXT NOT NULL DEFAULT 'NONE',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS stream_consumers (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			stream_arn      TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'ACTIVE',
			created_at      INTEGER NOT NULL,
			UNIQUE(name, stream_arn)
		);
	`},
}

// StreamMeta holds the metadata stored in SQLite.
type StreamMeta struct {
	Name           string
	ARN            string
	Status         string
	ShardCount     int
	RetentionHours int
	Mode           string
	Encryption     string
	CreatedAt      time.Time
}

// ConsumerMeta holds consumer metadata stored in SQLite.
type ConsumerMeta struct {
	ARN       string
	Name      string
	StreamARN string
	Status    string
	CreatedAt time.Time
}

// BufferRecord is a single record stored in the in-memory shard buffer.
type BufferRecord struct {
	PartitionKey   string
	Data           []byte
	SequenceNumber string
	Timestamp      time.Time
}

// ShardBuffer represents a single shard's in-memory record buffer.
type ShardBuffer struct {
	ID      string
	Records []BufferRecord
	SeqNum  int64 // next sequence number counter
}

// Iterator tracks a consumer's read position in a shard.
type Iterator struct {
	StreamName string
	ShardID    string
	Position   int
}

// StreamBuffer holds the in-memory record storage for all streams.
type StreamBuffer struct {
	mu        sync.RWMutex
	shards    map[string]map[string]*ShardBuffer // streamName -> shardID -> ShardBuffer
	iterators map[string]*Iterator
	iterSeq   int64
}

func newStreamBuffer() *StreamBuffer {
	return &StreamBuffer{
		shards:    make(map[string]map[string]*ShardBuffer),
		iterators: make(map[string]*Iterator),
	}
}

func (b *StreamBuffer) allocateShards(streamName string, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shardMap := make(map[string]*ShardBuffer, count)
	for i := 0; i < count; i++ {
		shardID := fmt.Sprintf("shardId-%012d", i)
		shardMap[shardID] = &ShardBuffer{ID: shardID}
	}
	b.shards[streamName] = shardMap
}

func (b *StreamBuffer) removeStream(streamName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.shards, streamName)
	// Clean up iterators for this stream
	for id, it := range b.iterators {
		if it.StreamName == streamName {
			delete(b.iterators, id)
		}
	}
}

func (b *StreamBuffer) getShards(streamName string) map[string]*ShardBuffer {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shards[streamName]
}

func (b *StreamBuffer) putRecord(streamName, partitionKey string, data []byte, shardCount int) (string, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shardMap, ok := b.shards[streamName]
	if !ok {
		return "", "", errStreamNotFound
	}

	shardIndex := hashPartitionKey(partitionKey) % shardCount
	shardID := fmt.Sprintf("shardId-%012d", shardIndex)
	shard, ok := shardMap[shardID]
	if !ok {
		return "", "", fmt.Errorf("shard %s not found", shardID)
	}

	shard.SeqNum++
	seqNum := fmt.Sprintf("%021d", shard.SeqNum)
	shard.Records = append(shard.Records, BufferRecord{
		PartitionKey:   partitionKey,
		Data:           data,
		SequenceNumber: seqNum,
		Timestamp:      time.Now(),
	})
	return shardID, seqNum, nil
}

func (b *StreamBuffer) createIterator(streamName, shardID, iteratorType, startingSeqNum string, timestamp time.Time) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	shardMap, ok := b.shards[streamName]
	if !ok {
		return "", errStreamNotFound
	}
	shard, ok := shardMap[shardID]
	if !ok {
		return "", fmt.Errorf("shard %s not found", shardID)
	}

	var position int
	switch iteratorType {
	case "TRIM_HORIZON":
		position = 0
	case "LATEST":
		position = len(shard.Records)
	case "AT_SEQUENCE_NUMBER":
		position = findSequencePosition(shard.Records, startingSeqNum, false)
	case "AFTER_SEQUENCE_NUMBER":
		position = findSequencePosition(shard.Records, startingSeqNum, true)
	case "AT_TIMESTAMP":
		position = findTimestampPosition(shard.Records, timestamp)
	default:
		return "", fmt.Errorf("invalid iterator type: %s", iteratorType)
	}

	b.iterSeq++
	iterID := fmt.Sprintf("iter-%s-%s-%d", streamName, shardID, b.iterSeq)
	b.iterators[iterID] = &Iterator{
		StreamName: streamName,
		ShardID:    shardID,
		Position:   position,
	}
	return iterID, nil
}

func (b *StreamBuffer) getRecords(iteratorID string, limit int) ([]BufferRecord, string, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	iter, ok := b.iterators[iteratorID]
	if !ok {
		return nil, "", 0, fmt.Errorf("iterator not found or expired")
	}

	shardMap, ok := b.shards[iter.StreamName]
	if !ok {
		return nil, "", 0, errStreamNotFound
	}
	shard, ok := shardMap[iter.ShardID]
	if !ok {
		return nil, "", 0, fmt.Errorf("shard %s not found", iter.ShardID)
	}

	if limit <= 0 {
		limit = 10000
	}

	start := iter.Position
	if start > len(shard.Records) {
		start = len(shard.Records)
	}
	end := start + limit
	if end > len(shard.Records) {
		end = len(shard.Records)
	}

	records := make([]BufferRecord, end-start)
	copy(records, shard.Records[start:end])

	// Calculate millis behind latest
	var millisBehind int64
	if end < len(shard.Records) {
		millisBehind = time.Since(shard.Records[end-1].Timestamp).Milliseconds()
		if millisBehind < 0 {
			millisBehind = 0
		}
	}

	// Remove old iterator, create new one
	delete(b.iterators, iteratorID)

	b.iterSeq++
	nextIterID := fmt.Sprintf("iter-%s-%s-%d", iter.StreamName, iter.ShardID, b.iterSeq)
	b.iterators[nextIterID] = &Iterator{
		StreamName: iter.StreamName,
		ShardID:    iter.ShardID,
		Position:   end,
	}

	return records, nextIterID, millisBehind, nil
}

func (b *StreamBuffer) addShard(streamName string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shardMap, ok := b.shards[streamName]
	if !ok {
		return "", errStreamNotFound
	}
	newID := fmt.Sprintf("shardId-%012d", len(shardMap))
	shardMap[newID] = &ShardBuffer{ID: newID}
	return newID, nil
}

func (b *StreamBuffer) removeShard(streamName, shardID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	shardMap, ok := b.shards[streamName]
	if !ok {
		return errStreamNotFound
	}
	if _, ok := shardMap[shardID]; !ok {
		return fmt.Errorf("shard %s not found", shardID)
	}
	delete(shardMap, shardID)
	return nil
}

func (b *StreamBuffer) resizeShards(streamName string, targetCount int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	shardMap, ok := b.shards[streamName]
	if !ok {
		return errStreamNotFound
	}
	currentCount := len(shardMap)
	if targetCount > currentCount {
		for i := currentCount; i < targetCount; i++ {
			id := fmt.Sprintf("shardId-%012d", i)
			shardMap[id] = &ShardBuffer{ID: id}
		}
	} else if targetCount < currentCount {
		for i := targetCount; i < currentCount; i++ {
			id := fmt.Sprintf("shardId-%012d", i)
			delete(shardMap, id)
		}
	}
	return nil
}

func hashPartitionKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

func findSequencePosition(records []BufferRecord, seqNum string, after bool) int {
	for i, r := range records {
		if r.SequenceNumber == seqNum {
			if after {
				return i + 1
			}
			return i
		}
	}
	return len(records)
}

func findTimestampPosition(records []BufferRecord, ts time.Time) int {
	for i, r := range records {
		if !r.Timestamp.Before(ts) {
			return i
		}
	}
	return len(records)
}

// Store combines SQLite persistence with the in-memory buffer.
type Store struct {
	sqlStore *sqlite.Store
	tags     *shared.TagStore
	buffer   *StreamBuffer
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "kinesis.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{
		sqlStore: s,
		tags:     shared.NewTagStore(s),
		buffer:   newStreamBuffer(),
	}, nil
}

func (s *Store) Close() error { return s.sqlStore.Close() }

func (s *Store) CreateStream(name, arn string, shardCount int, mode string) (*StreamMeta, error) {
	if shardCount <= 0 {
		shardCount = 1
	}
	if mode == "" {
		mode = "PROVISIONED"
	}
	now := time.Now().Unix()
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO streams (name, arn, shard_count, mode, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, arn, shardCount, mode, now,
	)
	if err != nil {
		return nil, err
	}
	s.buffer.allocateShards(name, shardCount)
	return &StreamMeta{
		Name: name, ARN: arn, Status: "ACTIVE", ShardCount: shardCount,
		RetentionHours: 24, Mode: mode, Encryption: "NONE",
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetStream(name string) (*StreamMeta, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT name, arn, status, shard_count, retention_hours, mode, encryption, created_at
		 FROM streams WHERE name = ?`, name)
	return scanStreamMeta(row)
}

func (s *Store) GetStreamByARN(arn string) (*StreamMeta, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT name, arn, status, shard_count, retention_hours, mode, encryption, created_at
		 FROM streams WHERE arn = ?`, arn)
	return scanStreamMeta(row)
}

func (s *Store) ListStreams() ([]StreamMeta, error) {
	rows, err := s.sqlStore.DB().Query(
		`SELECT name, arn, status, shard_count, retention_hours, mode, encryption, created_at
		 FROM streams ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var streams []StreamMeta
	for rows.Next() {
		st, err := scanStreamMeta(rows)
		if err != nil {
			return nil, err
		}
		streams = append(streams, *st)
	}
	return streams, rows.Err()
}

func (s *Store) DeleteStream(name string) error {
	st, err := s.GetStream(name)
	if err != nil {
		return err
	}
	if _, err := s.sqlStore.DB().Exec(`DELETE FROM stream_consumers WHERE stream_arn = ?`, st.ARN); err != nil {
		return err
	}
	res, err := s.sqlStore.DB().Exec(`DELETE FROM streams WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	s.buffer.removeStream(name)
	return nil
}

func (s *Store) UpdateRetention(name string, hours int) error {
	res, err := s.sqlStore.DB().Exec(
		`UPDATE streams SET retention_hours = ? WHERE name = ?`, hours, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

func (s *Store) UpdateEncryption(name, encryption string) error {
	res, err := s.sqlStore.DB().Exec(
		`UPDATE streams SET encryption = ? WHERE name = ?`, encryption, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

func (s *Store) UpdateMode(name, mode string) error {
	res, err := s.sqlStore.DB().Exec(
		`UPDATE streams SET mode = ? WHERE name = ?`, mode, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

func (s *Store) UpdateShardCount(name string, count int) error {
	res, err := s.sqlStore.DB().Exec(
		`UPDATE streams SET shard_count = ? WHERE name = ?`, count, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return s.buffer.resizeShards(name, count)
}

// Consumer operations

func (s *Store) RegisterConsumer(name, streamARN string) (*ConsumerMeta, error) {
	now := time.Now().Unix()
	consumerARN := fmt.Sprintf("%s/consumer/%s:%d", streamARN, name, now)
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO stream_consumers (arn, name, stream_arn, created_at) VALUES (?, ?, ?, ?)`,
		consumerARN, name, streamARN, now,
	)
	if err != nil {
		return nil, err
	}
	return &ConsumerMeta{
		ARN: consumerARN, Name: name, StreamARN: streamARN,
		Status: "ACTIVE", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) DeregisterConsumer(consumerARN string) error {
	res, err := s.sqlStore.DB().Exec(`DELETE FROM stream_consumers WHERE arn = ?`, consumerARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConsumerNotFound
	}
	return nil
}

func (s *Store) DeregisterConsumerByName(name, streamARN string) error {
	res, err := s.sqlStore.DB().Exec(
		`DELETE FROM stream_consumers WHERE name = ? AND stream_arn = ?`, name, streamARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConsumerNotFound
	}
	return nil
}

func (s *Store) GetConsumer(consumerARN string) (*ConsumerMeta, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT arn, name, stream_arn, status, created_at FROM stream_consumers WHERE arn = ?`,
		consumerARN)
	return scanConsumer(row)
}

func (s *Store) GetConsumerByName(name, streamARN string) (*ConsumerMeta, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT arn, name, stream_arn, status, created_at
		 FROM stream_consumers WHERE name = ? AND stream_arn = ?`,
		name, streamARN)
	return scanConsumer(row)
}

func (s *Store) ListConsumers(streamARN string) ([]ConsumerMeta, error) {
	rows, err := s.sqlStore.DB().Query(
		`SELECT arn, name, stream_arn, status, created_at
		 FROM stream_consumers WHERE stream_arn = ? ORDER BY created_at`,
		streamARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var consumers []ConsumerMeta
	for rows.Next() {
		c, err := scanConsumer(rows)
		if err != nil {
			return nil, err
		}
		consumers = append(consumers, *c)
	}
	return consumers, rows.Err()
}

type scanner interface{ Scan(dest ...any) error }

func scanStreamMeta(s scanner) (*StreamMeta, error) {
	var st StreamMeta
	var createdAt int64
	err := s.Scan(&st.Name, &st.ARN, &st.Status, &st.ShardCount, &st.RetentionHours,
		&st.Mode, &st.Encryption, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStreamNotFound
		}
		return nil, err
	}
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

func scanConsumer(s scanner) (*ConsumerMeta, error) {
	var c ConsumerMeta
	var createdAt int64
	err := s.Scan(&c.ARN, &c.Name, &c.StreamARN, &c.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConsumerNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
