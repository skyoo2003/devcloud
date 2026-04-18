// SPDX-License-Identifier: Apache-2.0

// internal/services/ebs/store.go
package ebs

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errSnapshotNotFound = errors.New("snapshot not found")
var errBlockNotFound = errors.New("block not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS snapshots (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			volume_id   TEXT NOT NULL DEFAULT '',
			volume_size INTEGER NOT NULL DEFAULT 1,
			status      TEXT NOT NULL DEFAULT 'completed',
			block_size  INTEGER NOT NULL DEFAULT 524288,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS snapshot_blocks (
			snapshot_id  TEXT NOT NULL,
			block_index  INTEGER NOT NULL,
			block_token  TEXT NOT NULL DEFAULT '',
			checksum     TEXT NOT NULL DEFAULT '',
			data         BLOB,
			PRIMARY KEY (snapshot_id, block_index)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS volume_modifications (
			id                  TEXT PRIMARY KEY,
			volume_id           TEXT NOT NULL,
			modification_state  TEXT NOT NULL DEFAULT 'completed',
			target_size         INTEGER,
			target_type         TEXT,
			target_iops         INTEGER,
			progress            INTEGER NOT NULL DEFAULT 100,
			start_time          INTEGER NOT NULL,
			account_id          TEXT NOT NULL DEFAULT '000000000000'
		);
	`},
}

type Snapshot struct {
	ID         string
	ARN        string
	VolumeID   string
	VolumeSize int64
	Status     string
	BlockSize  int32
	CreatedAt  time.Time
}

type SnapshotBlock struct {
	SnapshotID string
	BlockIndex int32
	BlockToken string
	Checksum   string
	Data       []byte
}

type VolumeModification struct {
	ID                string
	VolumeID          string
	ModificationState string
	TargetSize        int64
	TargetType        string
	TargetIops        int64
	Progress          int64
	StartTime         time.Time
	AccountID         string
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "ebs.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateSnapshot(snap *Snapshot) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO snapshots (id, arn, volume_id, volume_size, status, block_size, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		snap.ID, snap.ARN, snap.VolumeID, snap.VolumeSize, snap.Status, snap.BlockSize, now,
	)
	return err
}

func (s *Store) GetSnapshot(id string) (*Snapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, volume_id, volume_size, status, block_size, created_at
		 FROM snapshots WHERE id = ?`, id)
	return scanSnapshot(row)
}

func (s *Store) UpdateSnapshotStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE snapshots SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSnapshotNotFound
	}
	return nil
}

func (s *Store) PutBlock(block *SnapshotBlock) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO snapshot_blocks (snapshot_id, block_index, block_token, checksum, data)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(snapshot_id, block_index) DO UPDATE SET
		   block_token=excluded.block_token,
		   checksum=excluded.checksum,
		   data=excluded.data`,
		block.SnapshotID, block.BlockIndex, block.BlockToken, block.Checksum, block.Data,
	)
	return err
}

func (s *Store) GetBlock(snapshotID string, blockIndex int32) (*SnapshotBlock, error) {
	row := s.store.DB().QueryRow(
		`SELECT snapshot_id, block_index, block_token, checksum, data
		 FROM snapshot_blocks WHERE snapshot_id = ? AND block_index = ?`,
		snapshotID, blockIndex,
	)
	var b SnapshotBlock
	err := row.Scan(&b.SnapshotID, &b.BlockIndex, &b.BlockToken, &b.Checksum, &b.Data)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBlockNotFound
		}
		return nil, err
	}
	return &b, nil
}

func (s *Store) ListBlocks(snapshotID string, startIndex int32) ([]SnapshotBlock, error) {
	rows, err := s.store.DB().Query(
		`SELECT snapshot_id, block_index, block_token, checksum
		 FROM snapshot_blocks WHERE snapshot_id = ? AND block_index >= ?
		 ORDER BY block_index`,
		snapshotID, startIndex,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var blocks []SnapshotBlock
	for rows.Next() {
		var b SnapshotBlock
		if err := rows.Scan(&b.SnapshotID, &b.BlockIndex, &b.BlockToken, &b.Checksum); err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func (s *Store) ListChangedBlocks(firstSnapshotID, secondSnapshotID string, startIndex int32) ([]SnapshotBlock, error) {
	// Return blocks in second snapshot that differ from or don't exist in the first.
	rows, err := s.store.DB().Query(
		`SELECT b2.snapshot_id, b2.block_index, b2.block_token,
		        COALESCE(b1.block_token, '') as first_token
		 FROM snapshot_blocks b2
		 LEFT JOIN snapshot_blocks b1
		   ON b1.snapshot_id = ? AND b1.block_index = b2.block_index
		 WHERE b2.snapshot_id = ? AND b2.block_index >= ?
		   AND (b1.block_token IS NULL OR b1.checksum != b2.checksum)
		 ORDER BY b2.block_index`,
		firstSnapshotID, secondSnapshotID, startIndex,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var blocks []SnapshotBlock
	for rows.Next() {
		var b SnapshotBlock
		var firstToken string
		if err := rows.Scan(&b.SnapshotID, &b.BlockIndex, &b.BlockToken, &firstToken); err != nil {
			return nil, err
		}
		// Store first snapshot token in Checksum field for reuse
		b.Checksum = firstToken
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func (s *Store) ListSnapshots() ([]Snapshot, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, volume_id, volume_size, status, block_size, created_at
		 FROM snapshots ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var snaps []Snapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *snap)
	}
	return snaps, rows.Err()
}

func (s *Store) CreateVolumeModification(mod *VolumeModification) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO volume_modifications (id, volume_id, modification_state, target_size, target_type, target_iops, progress, start_time, account_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		mod.ID, mod.VolumeID, mod.ModificationState, mod.TargetSize, mod.TargetType, mod.TargetIops, mod.Progress, mod.StartTime.Unix(), mod.AccountID,
	)
	return err
}

func (s *Store) ListVolumeModifications(volumeID string) ([]VolumeModification, error) {
	q := `SELECT id, volume_id, modification_state, target_size, target_type, target_iops, progress, start_time, account_id
		  FROM volume_modifications`
	args := []any{}
	if volumeID != "" {
		q += ` WHERE volume_id = ?`
		args = append(args, volumeID)
	}
	rows, err := s.store.DB().Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var mods []VolumeModification
	for rows.Next() {
		var m VolumeModification
		var startTime int64
		if err := rows.Scan(&m.ID, &m.VolumeID, &m.ModificationState, &m.TargetSize, &m.TargetType, &m.TargetIops, &m.Progress, &startTime, &m.AccountID); err != nil {
			return nil, err
		}
		m.StartTime = time.Unix(startTime, 0)
		mods = append(mods, m)
	}
	return mods, rows.Err()
}

func (s *Store) CopySnapshot(srcID, newID, newARN string) error {
	row := s.store.DB().QueryRow(
		`SELECT volume_size, block_size FROM snapshots WHERE id = ?`, srcID)
	var volumeSize int64
	var blockSize int32
	if err := row.Scan(&volumeSize, &blockSize); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errSnapshotNotFound
		}
		return err
	}
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO snapshots (id, arn, volume_id, volume_size, status, block_size, created_at)
		 VALUES (?, ?, '', ?, 'completed', ?, ?)`,
		newID, newARN, volumeSize, blockSize, now,
	)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanSnapshot(sc scanner) (*Snapshot, error) {
	var snap Snapshot
	var createdAt int64
	err := sc.Scan(&snap.ID, &snap.ARN, &snap.VolumeID, &snap.VolumeSize, &snap.Status, &snap.BlockSize, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	snap.CreatedAt = time.Unix(createdAt, 0)
	return &snap, nil
}
