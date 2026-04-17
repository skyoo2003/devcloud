// SPDX-License-Identifier: Apache-2.0

// internal/services/glacier/store.go
package glacier

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errVaultNotFound   = errors.New("vault not found")
	errArchiveNotFound = errors.New("archive not found")
	errJobNotFound     = errors.New("job not found")
	errUploadNotFound  = errors.New("upload not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS vaults (
			name          TEXT PRIMARY KEY,
			arn           TEXT NOT NULL UNIQUE,
			created_at    INTEGER NOT NULL,
			archive_count INTEGER NOT NULL DEFAULT 0,
			size_bytes    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS archives (
			id          TEXT PRIMARY KEY,
			vault_name  TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			size_bytes  INTEGER NOT NULL DEFAULT 0,
			checksum    TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS jobs (
			id           TEXT PRIMARY KEY,
			vault_name   TEXT NOT NULL,
			type         TEXT NOT NULL DEFAULT 'inventory-retrieval',
			description  TEXT NOT NULL DEFAULT '',
			status       TEXT NOT NULL DEFAULT 'Succeeded',
			archive_id   TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS vault_tags (
			vault_name TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (vault_name, key)
		);
		CREATE TABLE IF NOT EXISTS vault_policies (
			vault_name TEXT PRIMARY KEY,
			policy     TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS vault_notifications (
			vault_name TEXT PRIMARY KEY,
			sns_topic  TEXT NOT NULL DEFAULT '',
			events     TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS vault_locks (
			vault_name TEXT PRIMARY KEY,
			lock_id    TEXT NOT NULL DEFAULT '',
			policy     TEXT NOT NULL DEFAULT '{}',
			state      TEXT NOT NULL DEFAULT 'InProgress'
		);
		CREATE TABLE IF NOT EXISTS multipart_uploads (
			id          TEXT PRIMARY KEY,
			vault_name  TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			part_size   INTEGER NOT NULL DEFAULT 0,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS multipart_parts (
			upload_id   TEXT NOT NULL,
			range_start INTEGER NOT NULL,
			range_end   INTEGER NOT NULL,
			checksum    TEXT NOT NULL DEFAULT '',
			data        BLOB,
			PRIMARY KEY (upload_id, range_start)
		);
	`},
}

type Vault struct {
	Name         string
	ARN          string
	CreatedAt    time.Time
	ArchiveCount int64
	SizeBytes    int64
}

type Archive struct {
	ID          string
	VaultName   string
	Description string
	SizeBytes   int64
	Checksum    string
	CreatedAt   time.Time
}

type Job struct {
	ID          string
	VaultName   string
	Type        string
	Description string
	Status      string
	ArchiveID   string
	CreatedAt   time.Time
	CompletedAt time.Time
}

type MultipartUpload struct {
	ID          string
	VaultName   string
	Description string
	PartSize    int64
	CreatedAt   time.Time
}

type MultipartPart struct {
	UploadID   string
	RangeStart int64
	RangeEnd   int64
	Checksum   string
	Data       []byte
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "glacier.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Vault ---

func (s *Store) CreateVault(v *Vault) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO vaults (name, arn, created_at, archive_count, size_bytes)
		 VALUES (?, ?, ?, 0, 0)`,
		v.Name, v.ARN, now,
	)
	return err
}

func (s *Store) GetVault(name string) (*Vault, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, created_at, archive_count, size_bytes FROM vaults WHERE name = ?`, name)
	return scanVault(row)
}

func (s *Store) ListVaults() ([]Vault, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, created_at, archive_count, size_bytes FROM vaults ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vaults []Vault
	for rows.Next() {
		v, err := scanVault(rows)
		if err != nil {
			return nil, err
		}
		vaults = append(vaults, *v)
	}
	return vaults, rows.Err()
}

func (s *Store) DeleteVault(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM vaults WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errVaultNotFound
	}
	return nil
}

func (s *Store) updateVaultStats(vaultName string, sizeDelta int64, countDelta int64) error {
	_, err := s.store.DB().Exec(
		`UPDATE vaults SET size_bytes = size_bytes + ?, archive_count = archive_count + ?
		 WHERE name = ?`,
		sizeDelta, countDelta, vaultName,
	)
	return err
}

// --- Archive ---

func (s *Store) CreateArchive(a *Archive) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO archives (id, vault_name, description, size_bytes, checksum, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		a.ID, a.VaultName, a.Description, a.SizeBytes, a.Checksum, now,
	)
	if err != nil {
		return err
	}
	return s.updateVaultStats(a.VaultName, a.SizeBytes, 1)
}

func (s *Store) GetArchive(id string) (*Archive, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, vault_name, description, size_bytes, checksum, created_at
		 FROM archives WHERE id = ?`, id)
	return scanArchive(row)
}

func (s *Store) DeleteArchive(id string) error {
	a, err := s.GetArchive(id)
	if err != nil {
		return err
	}
	res, err := s.store.DB().Exec(`DELETE FROM archives WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errArchiveNotFound
	}
	return s.updateVaultStats(a.VaultName, -a.SizeBytes, -1)
}

func (s *Store) ListArchives(vaultName string) ([]Archive, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, vault_name, description, size_bytes, checksum, created_at
		 FROM archives WHERE vault_name = ? ORDER BY created_at`,
		vaultName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var archives []Archive
	for rows.Next() {
		a, err := scanArchive(rows)
		if err != nil {
			return nil, err
		}
		archives = append(archives, *a)
	}
	return archives, rows.Err()
}

// --- Job ---

func (s *Store) CreateJob(j *Job) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO jobs (id, vault_name, type, description, status, archive_id, created_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.VaultName, j.Type, j.Description, j.Status, j.ArchiveID, now, now,
	)
	return err
}

func (s *Store) GetJob(id string) (*Job, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, vault_name, type, description, status, archive_id, created_at, completed_at
		 FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

func (s *Store) ListJobs(vaultName string) ([]Job, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, vault_name, type, description, status, archive_id, created_at, completed_at
		 FROM jobs WHERE vault_name = ? ORDER BY created_at`,
		vaultName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

// --- Tags ---

func (s *Store) AddTags(vaultName string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO vault_tags (vault_name, key, value) VALUES (?, ?, ?)
			 ON CONFLICT(vault_name, key) DO UPDATE SET value=excluded.value`,
			vaultName, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RemoveTags(vaultName string, keys []string) error {
	for _, k := range keys {
		if _, err := s.store.DB().Exec(
			`DELETE FROM vault_tags WHERE vault_name = ? AND key = ?`, vaultName, k); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListTags(vaultName string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT key, value FROM vault_tags WHERE vault_name = ? ORDER BY key`, vaultName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// --- Vault Policy ---

func (s *Store) SetVaultPolicy(vaultName, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO vault_policies (vault_name, policy) VALUES (?, ?)
		 ON CONFLICT(vault_name) DO UPDATE SET policy=excluded.policy`,
		vaultName, policy,
	)
	return err
}

func (s *Store) GetVaultPolicy(vaultName string) (string, error) {
	var policy string
	err := s.store.DB().QueryRow(
		`SELECT policy FROM vault_policies WHERE vault_name = ?`, vaultName).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return policy, err
}

func (s *Store) DeleteVaultPolicy(vaultName string) error {
	_, err := s.store.DB().Exec(`DELETE FROM vault_policies WHERE vault_name = ?`, vaultName)
	return err
}

// --- Vault Notifications ---

func (s *Store) SetVaultNotifications(vaultName, snsTopic string, events []string) error {
	eventsJSON, _ := json.Marshal(events)
	_, err := s.store.DB().Exec(
		`INSERT INTO vault_notifications (vault_name, sns_topic, events) VALUES (?, ?, ?)
		 ON CONFLICT(vault_name) DO UPDATE SET sns_topic=excluded.sns_topic, events=excluded.events`,
		vaultName, snsTopic, string(eventsJSON),
	)
	return err
}

func (s *Store) GetVaultNotifications(vaultName string) (string, []string, error) {
	var snsTopic, eventsJSON string
	err := s.store.DB().QueryRow(
		`SELECT sns_topic, events FROM vault_notifications WHERE vault_name = ?`, vaultName).
		Scan(&snsTopic, &eventsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}
	var events []string
	json.Unmarshal([]byte(eventsJSON), &events)
	return snsTopic, events, nil
}

func (s *Store) DeleteVaultNotifications(vaultName string) error {
	_, err := s.store.DB().Exec(`DELETE FROM vault_notifications WHERE vault_name = ?`, vaultName)
	return err
}

// --- Vault Lock ---

func (s *Store) InitVaultLock(vaultName, lockID, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO vault_locks (vault_name, lock_id, policy, state) VALUES (?, ?, ?, 'InProgress')
		 ON CONFLICT(vault_name) DO UPDATE SET lock_id=excluded.lock_id, policy=excluded.policy, state='InProgress'`,
		vaultName, lockID, policy,
	)
	return err
}

func (s *Store) GetVaultLock(vaultName string) (string, string, string, error) {
	var lockID, policy, state string
	err := s.store.DB().QueryRow(
		`SELECT lock_id, policy, state FROM vault_locks WHERE vault_name = ?`, vaultName).
		Scan(&lockID, &policy, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", nil
	}
	return lockID, policy, state, err
}

func (s *Store) CompleteVaultLock(vaultName, lockID string) error {
	res, err := s.store.DB().Exec(
		`UPDATE vault_locks SET state = 'Locked' WHERE vault_name = ? AND lock_id = ?`,
		vaultName, lockID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("lock not found or lock ID mismatch")
	}
	return nil
}

func (s *Store) AbortVaultLock(vaultName string) error {
	_, err := s.store.DB().Exec(`DELETE FROM vault_locks WHERE vault_name = ?`, vaultName)
	return err
}

// --- Multipart Upload ---

func (s *Store) CreateMultipartUpload(u *MultipartUpload) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO multipart_uploads (id, vault_name, description, part_size, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		u.ID, u.VaultName, u.Description, u.PartSize, now,
	)
	return err
}

func (s *Store) GetMultipartUpload(id string) (*MultipartUpload, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, vault_name, description, part_size, created_at FROM multipart_uploads WHERE id = ?`, id)
	var u MultipartUpload
	var createdAt int64
	err := row.Scan(&u.ID, &u.VaultName, &u.Description, &u.PartSize, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errUploadNotFound
	}
	if err != nil {
		return nil, err
	}
	u.CreatedAt = time.Unix(createdAt, 0)
	return &u, nil
}

func (s *Store) ListMultipartUploads(vaultName string) ([]MultipartUpload, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, vault_name, description, part_size, created_at
		 FROM multipart_uploads WHERE vault_name = ? ORDER BY created_at`,
		vaultName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var createdAt int64
		if err := rows.Scan(&u.ID, &u.VaultName, &u.Description, &u.PartSize, &createdAt); err != nil {
			return nil, err
		}
		u.CreatedAt = time.Unix(createdAt, 0)
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

func (s *Store) AbortMultipartUpload(id string) error {
	s.store.DB().Exec(`DELETE FROM multipart_parts WHERE upload_id = ?`, id)
	res, err := s.store.DB().Exec(`DELETE FROM multipart_uploads WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUploadNotFound
	}
	return nil
}

func (s *Store) PutPart(p *MultipartPart) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO multipart_parts (upload_id, range_start, range_end, checksum, data)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(upload_id, range_start) DO UPDATE SET
		   range_end=excluded.range_end, checksum=excluded.checksum, data=excluded.data`,
		p.UploadID, p.RangeStart, p.RangeEnd, p.Checksum, p.Data,
	)
	return err
}

func (s *Store) ListParts(uploadID string) ([]MultipartPart, error) {
	rows, err := s.store.DB().Query(
		`SELECT upload_id, range_start, range_end, checksum FROM multipart_parts
		 WHERE upload_id = ? ORDER BY range_start`,
		uploadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []MultipartPart
	for rows.Next() {
		var p MultipartPart
		if err := rows.Scan(&p.UploadID, &p.RangeStart, &p.RangeEnd, &p.Checksum); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (s *Store) AssembleMultipartUpload(uploadID string) ([]byte, int64, error) {
	rows, err := s.store.DB().Query(
		`SELECT data FROM multipart_parts WHERE upload_id = ? ORDER BY range_start`, uploadID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var combined []byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, 0, err
		}
		combined = append(combined, data...)
	}
	return combined, int64(len(combined)), rows.Err()
}

func (s *Store) DeleteMultipartParts(uploadID string) error {
	_, err := s.store.DB().Exec(`DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID)
	return err
}

func (s *Store) DeleteMultipartUploadRecord(uploadID string) error {
	_, err := s.store.DB().Exec(`DELETE FROM multipart_uploads WHERE id = ?`, uploadID)
	return err
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanVault(sc scanner) (*Vault, error) {
	var v Vault
	var createdAt int64
	err := sc.Scan(&v.Name, &v.ARN, &createdAt, &v.ArchiveCount, &v.SizeBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errVaultNotFound
	}
	if err != nil {
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

func scanArchive(sc scanner) (*Archive, error) {
	var a Archive
	var createdAt int64
	err := sc.Scan(&a.ID, &a.VaultName, &a.Description, &a.SizeBytes, &a.Checksum, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errArchiveNotFound
	}
	if err != nil {
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanJob(sc scanner) (*Job, error) {
	var j Job
	var createdAt, completedAt int64
	err := sc.Scan(&j.ID, &j.VaultName, &j.Type, &j.Description, &j.Status, &j.ArchiveID, &createdAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errJobNotFound
	}
	if err != nil {
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.CompletedAt = time.Unix(completedAt, 0)
	return &j, nil
}
