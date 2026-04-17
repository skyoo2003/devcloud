// SPDX-License-Identifier: Apache-2.0

package efs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrFileSystemNotFound  = errors.New("file system not found")
	ErrMountTargetNotFound = errors.New("mount target not found")
	ErrAccessPointNotFound = errors.New("access point not found")
	ErrConflict            = errors.New("resource already exists")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `CREATE TABLE IF NOT EXISTS file_systems (
		file_system_id   TEXT PRIMARY KEY,
		creation_token   TEXT UNIQUE,
		account_id       TEXT NOT NULL,
		performance_mode TEXT DEFAULT 'generalPurpose',
		throughput_mode  TEXT DEFAULT 'bursting',
		encrypted        INTEGER DEFAULT 0,
		kms_key_id       TEXT,
		lifecycle_state  TEXT DEFAULT 'available',
		number_of_mount_targets INTEGER DEFAULT 0,
		size_bytes       INTEGER DEFAULT 0,
		name             TEXT,
		backup_policy    TEXT DEFAULT 'DISABLED',
		lifecycle_config TEXT,
		file_system_policy TEXT,
		created_at       DATETIME NOT NULL
	);`},
	{Version: 2, SQL: `CREATE TABLE IF NOT EXISTS mount_targets (
		mount_target_id TEXT PRIMARY KEY,
		file_system_id  TEXT NOT NULL,
		subnet_id       TEXT NOT NULL,
		ip_address      TEXT,
		security_groups TEXT,
		lifecycle_state TEXT DEFAULT 'available',
		account_id      TEXT NOT NULL,
		created_at      DATETIME NOT NULL
	);`},
	{Version: 3, SQL: `CREATE TABLE IF NOT EXISTS access_points (
		access_point_id TEXT PRIMARY KEY,
		file_system_id  TEXT NOT NULL,
		name            TEXT,
		client_token    TEXT,
		posix_user      TEXT,
		root_directory  TEXT,
		account_id      TEXT NOT NULL,
		created_at      DATETIME NOT NULL
	);`},
	{Version: 4, SQL: `CREATE TABLE IF NOT EXISTS efs_tags (
		resource_id TEXT NOT NULL,
		tag_key     TEXT NOT NULL,
		tag_value   TEXT NOT NULL,
		PRIMARY KEY (resource_id, tag_key)
	);`},
}

type Store struct {
	s *sqlite.Store
}

func NewStore(dir string) (*Store, error) {
	s, err := sqlite.Open(filepath.Join(dir, "efs.db"), migrations)
	if err != nil {
		return nil, fmt.Errorf("efs: open store: %w", err)
	}
	return &Store{s: s}, nil
}

func (s *Store) Close() error { return s.s.Close() }

func (s *Store) db() *sql.DB { return s.s.DB() }

// --- FileSystem ---

type fileSystemRow struct {
	FileSystemID         string
	CreationToken        string
	AccountID            string
	PerformanceMode      string
	ThroughputMode       string
	Encrypted            bool
	KMSKeyID             string
	LifecycleState       string
	NumberOfMountTargets int32
	SizeBytes            int64
	Name                 string
	BackupPolicy         string
	LifecycleConfig      string
	FileSystemPolicy     string
	CreatedAt            time.Time
}

func (s *Store) CreateFileSystem(r *fileSystemRow) error {
	_, err := s.db().Exec(
		`INSERT INTO file_systems
		 (file_system_id, creation_token, account_id, performance_mode, throughput_mode,
		  encrypted, kms_key_id, lifecycle_state, number_of_mount_targets, size_bytes,
		  name, backup_policy, lifecycle_config, file_system_policy, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.FileSystemID, r.CreationToken, r.AccountID, r.PerformanceMode, r.ThroughputMode,
		boolInt(r.Encrypted), r.KMSKeyID, r.LifecycleState, r.NumberOfMountTargets, r.SizeBytes,
		r.Name, r.BackupPolicy, r.LifecycleConfig, r.FileSystemPolicy, r.CreatedAt,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return ErrConflict
		}
		return fmt.Errorf("create file system: %w", err)
	}
	return nil
}

func (s *Store) GetFileSystem(id string) (*fileSystemRow, error) {
	row := s.db().QueryRow(
		`SELECT file_system_id, creation_token, account_id, performance_mode, throughput_mode,
		        encrypted, COALESCE(kms_key_id,''), lifecycle_state, number_of_mount_targets, size_bytes,
		        COALESCE(name,''), backup_policy, COALESCE(lifecycle_config,''), COALESCE(file_system_policy,''), created_at
		 FROM file_systems WHERE file_system_id=?`, id)
	return scanFileSystem(row)
}

func (s *Store) GetFileSystemByToken(token string) (*fileSystemRow, error) {
	row := s.db().QueryRow(
		`SELECT file_system_id, creation_token, account_id, performance_mode, throughput_mode,
		        encrypted, COALESCE(kms_key_id,''), lifecycle_state, number_of_mount_targets, size_bytes,
		        COALESCE(name,''), backup_policy, COALESCE(lifecycle_config,''), COALESCE(file_system_policy,''), created_at
		 FROM file_systems WHERE creation_token=?`, token)
	return scanFileSystem(row)
}

func (s *Store) ListFileSystems(accountID string) ([]*fileSystemRow, error) {
	rows, err := s.db().Query(
		`SELECT file_system_id, creation_token, account_id, performance_mode, throughput_mode,
		        encrypted, COALESCE(kms_key_id,''), lifecycle_state, number_of_mount_targets, size_bytes,
		        COALESCE(name,''), backup_policy, COALESCE(lifecycle_config,''), COALESCE(file_system_policy,''), created_at
		 FROM file_systems WHERE account_id=? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*fileSystemRow
	for rows.Next() {
		r, err := scanFileSystem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) UpdateFileSystem(id, throughputMode string) error {
	res, err := s.db().Exec(`UPDATE file_systems SET throughput_mode=? WHERE file_system_id=?`, throughputMode, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileSystemNotFound
	}
	return nil
}

func (s *Store) DeleteFileSystem(id string) error {
	res, err := s.db().Exec(`DELETE FROM file_systems WHERE file_system_id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileSystemNotFound
	}
	return nil
}

func (s *Store) PutFileSystemPolicy(id, policy string) error {
	res, err := s.db().Exec(`UPDATE file_systems SET file_system_policy=? WHERE file_system_id=?`, policy, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileSystemNotFound
	}
	return nil
}

func (s *Store) DeleteFileSystemPolicy(id string) error {
	return s.PutFileSystemPolicy(id, "")
}

func (s *Store) PutBackupPolicy(id, status string) error {
	res, err := s.db().Exec(`UPDATE file_systems SET backup_policy=? WHERE file_system_id=?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileSystemNotFound
	}
	return nil
}

func (s *Store) PutLifecycleConfig(id string, policies []map[string]any) error {
	b, _ := json.Marshal(policies)
	res, err := s.db().Exec(`UPDATE file_systems SET lifecycle_config=? WHERE file_system_id=?`, string(b), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrFileSystemNotFound
	}
	return nil
}

func scanFileSystem(s interface{ Scan(...any) error }) (*fileSystemRow, error) {
	r := &fileSystemRow{}
	var encInt int
	var createdStr string
	err := s.Scan(
		&r.FileSystemID, &r.CreationToken, &r.AccountID, &r.PerformanceMode, &r.ThroughputMode,
		&encInt, &r.KMSKeyID, &r.LifecycleState, &r.NumberOfMountTargets, &r.SizeBytes,
		&r.Name, &r.BackupPolicy, &r.LifecycleConfig, &r.FileSystemPolicy, &createdStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFileSystemNotFound
		}
		return nil, err
	}
	r.Encrypted = encInt != 0
	r.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if r.CreatedAt.IsZero() {
		r.CreatedAt, _ = time.Parse("2006-01-02 15:04:05.999999999-07:00", createdStr)
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	return r, nil
}

// --- MountTarget ---

type mountTargetRow struct {
	MountTargetID  string
	FileSystemID   string
	SubnetID       string
	IPAddress      string
	SecurityGroups []string
	LifecycleState string
	AccountID      string
	CreatedAt      time.Time
}

func (s *Store) CreateMountTarget(r *mountTargetRow) error {
	sg, _ := json.Marshal(r.SecurityGroups)
	_, err := s.db().Exec(
		`INSERT INTO mount_targets
		 (mount_target_id, file_system_id, subnet_id, ip_address, security_groups, lifecycle_state, account_id, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		r.MountTargetID, r.FileSystemID, r.SubnetID, r.IPAddress, string(sg), r.LifecycleState, r.AccountID, r.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create mount target: %w", err)
	}
	_, _ = s.db().Exec(`UPDATE file_systems SET number_of_mount_targets = number_of_mount_targets + 1 WHERE file_system_id=?`, r.FileSystemID)
	return nil
}

func (s *Store) GetMountTarget(id string) (*mountTargetRow, error) {
	row := s.db().QueryRow(
		`SELECT mount_target_id, file_system_id, subnet_id, COALESCE(ip_address,''), COALESCE(security_groups,'[]'), lifecycle_state, account_id, created_at
		 FROM mount_targets WHERE mount_target_id=?`, id)
	return scanMountTarget(row)
}

func (s *Store) ListMountTargetsByFS(fsID string) ([]*mountTargetRow, error) {
	rows, err := s.db().Query(
		`SELECT mount_target_id, file_system_id, subnet_id, COALESCE(ip_address,''), COALESCE(security_groups,'[]'), lifecycle_state, account_id, created_at
		 FROM mount_targets WHERE file_system_id=?`, fsID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectMountTargets(rows)
}

func (s *Store) ListAllMountTargets(accountID string) ([]*mountTargetRow, error) {
	rows, err := s.db().Query(
		`SELECT mount_target_id, file_system_id, subnet_id, COALESCE(ip_address,''), COALESCE(security_groups,'[]'), lifecycle_state, account_id, created_at
		 FROM mount_targets WHERE account_id=?`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectMountTargets(rows)
}

func (s *Store) DeleteMountTarget(id string) error {
	mt, err := s.GetMountTarget(id)
	if err != nil {
		return err
	}
	res, err := s.db().Exec(`DELETE FROM mount_targets WHERE mount_target_id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrMountTargetNotFound
	}
	_, _ = s.db().Exec(`UPDATE file_systems SET number_of_mount_targets = number_of_mount_targets - 1 WHERE file_system_id=?`, mt.FileSystemID)
	return nil
}

func (s *Store) UpdateMountTargetSecurityGroups(id string, sgs []string) error {
	b, _ := json.Marshal(sgs)
	res, err := s.db().Exec(`UPDATE mount_targets SET security_groups=? WHERE mount_target_id=?`, string(b), id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrMountTargetNotFound
	}
	return nil
}

func collectMountTargets(rows *sql.Rows) ([]*mountTargetRow, error) {
	var out []*mountTargetRow
	for rows.Next() {
		r, err := scanMountTarget(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanMountTarget(s interface{ Scan(...any) error }) (*mountTargetRow, error) {
	r := &mountTargetRow{}
	var sgJSON string
	var createdStr string
	err := s.Scan(&r.MountTargetID, &r.FileSystemID, &r.SubnetID, &r.IPAddress, &sgJSON, &r.LifecycleState, &r.AccountID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrMountTargetNotFound
		}
		return nil, err
	}
	json.Unmarshal([]byte(sgJSON), &r.SecurityGroups)
	r.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	return r, nil
}

// --- AccessPoint ---

type accessPointRow struct {
	AccessPointID string
	FileSystemID  string
	Name          string
	ClientToken   string
	PosixUser     string
	RootDirectory string
	AccountID     string
	CreatedAt     time.Time
}

func (s *Store) CreateAccessPoint(r *accessPointRow) error {
	_, err := s.db().Exec(
		`INSERT INTO access_points
		 (access_point_id, file_system_id, name, client_token, posix_user, root_directory, account_id, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		r.AccessPointID, r.FileSystemID, r.Name, r.ClientToken, r.PosixUser, r.RootDirectory, r.AccountID, r.CreatedAt,
	)
	return err
}

func (s *Store) GetAccessPoint(id string) (*accessPointRow, error) {
	row := s.db().QueryRow(
		`SELECT access_point_id, file_system_id, COALESCE(name,''), COALESCE(client_token,''), COALESCE(posix_user,''), COALESCE(root_directory,''), account_id, created_at
		 FROM access_points WHERE access_point_id=?`, id)
	return scanAccessPoint(row)
}

func (s *Store) ListAccessPoints(accountID, fsID string) ([]*accessPointRow, error) {
	var rows *sql.Rows
	var err error
	if fsID != "" {
		rows, err = s.db().Query(
			`SELECT access_point_id, file_system_id, COALESCE(name,''), COALESCE(client_token,''), COALESCE(posix_user,''), COALESCE(root_directory,''), account_id, created_at
			 FROM access_points WHERE account_id=? AND file_system_id=?`, accountID, fsID)
	} else {
		rows, err = s.db().Query(
			`SELECT access_point_id, file_system_id, COALESCE(name,''), COALESCE(client_token,''), COALESCE(posix_user,''), COALESCE(root_directory,''), account_id, created_at
			 FROM access_points WHERE account_id=?`, accountID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*accessPointRow
	for rows.Next() {
		r, err := scanAccessPoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAccessPoint(id string) error {
	res, err := s.db().Exec(`DELETE FROM access_points WHERE access_point_id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrAccessPointNotFound
	}
	return nil
}

func scanAccessPoint(s interface{ Scan(...any) error }) (*accessPointRow, error) {
	r := &accessPointRow{}
	var createdStr string
	err := s.Scan(&r.AccessPointID, &r.FileSystemID, &r.Name, &r.ClientToken, &r.PosixUser, &r.RootDirectory, &r.AccountID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAccessPointNotFound
		}
		return nil, err
	}
	r.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	return r, nil
}

// --- Tags ---

func (s *Store) TagResource(resourceID string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(
			`INSERT INTO efs_tags (resource_id, tag_key, tag_value) VALUES (?,?,?)
			 ON CONFLICT(resource_id, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			resourceID, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListTags(resourceID string) (map[string]string, error) {
	rows, err := s.db().Query(`SELECT tag_key, tag_value FROM efs_tags WHERE resource_id=?`, resourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *Store) UntagResource(resourceID string, keys []string) error {
	for _, k := range keys {
		if _, err := s.db().Exec(`DELETE FROM efs_tags WHERE resource_id=? AND tag_key=?`, resourceID, k); err != nil {
			return err
		}
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
