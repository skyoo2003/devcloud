// SPDX-License-Identifier: Apache-2.0

// internal/services/s3tables/store.go
package s3tables

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errBucketNotFound      = errors.New("table bucket not found")
	errNamespaceNotFound   = errors.New("namespace not found")
	errTableNotFound       = errors.New("table not found")
	errPolicyNotFound      = errors.New("policy not found")
	errEncryptionNotFound  = errors.New("encryption not found")
	errMaintenanceNotFound = errors.New("maintenance config not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS s3tables_buckets (
			arn        TEXT PRIMARY KEY,
			name       TEXT UNIQUE NOT NULL,
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS s3tables_namespaces (
			id         TEXT PRIMARY KEY,
			bucket_arn TEXT NOT NULL,
			namespace  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			UNIQUE(bucket_arn, namespace)
		);
		CREATE TABLE IF NOT EXISTS s3tables_tables (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL,
			bucket_arn       TEXT NOT NULL,
			namespace        TEXT NOT NULL,
			type             TEXT NOT NULL DEFAULT 'customer',
			format           TEXT NOT NULL DEFAULT 'ICEBERG',
			metadata_location TEXT NOT NULL DEFAULT '',
			version_token    TEXT NOT NULL DEFAULT '',
			account_id       TEXT NOT NULL,
			created_at       INTEGER NOT NULL,
			UNIQUE(bucket_arn, namespace, name)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS s3tables_policies (
			resource_arn TEXT PRIMARY KEY,
			policy       TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS s3tables_encryption (
			resource_arn TEXT PRIMARY KEY,
			config       TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS s3tables_maintenance (
			resource_arn TEXT NOT NULL,
			config_type  TEXT NOT NULL,
			value        TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (resource_arn, config_type)
		);
	`},
}

type Bucket struct {
	ARN       string
	Name      string
	AccountID string
	CreatedAt time.Time
}

type Namespace struct {
	ID        string
	BucketARN string
	Namespace string
	AccountID string
	CreatedAt time.Time
}

type Table struct {
	ARN              string
	Name             string
	BucketARN        string
	Namespace        string
	Type             string
	Format           string
	MetadataLocation string
	VersionToken     string
	AccountID        string
	CreatedAt        time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "s3tables.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// Bucket operations

func (s *Store) CreateBucket(name, accountID string) (*Bucket, error) {
	arn := "arn:aws:s3tables:us-east-1:" + accountID + ":bucket/" + name
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_buckets (arn, name, account_id, created_at) VALUES (?, ?, ?, ?)`,
		arn, name, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Bucket{ARN: arn, Name: name, AccountID: accountID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetBucket(name string) (*Bucket, error) {
	var row *sql.Row
	if strings.Contains(name, "arn:") {
		row = s.store.DB().QueryRow(
			`SELECT arn, name, account_id, created_at FROM s3tables_buckets WHERE arn = ?`, name)
	} else {
		row = s.store.DB().QueryRow(
			`SELECT arn, name, account_id, created_at FROM s3tables_buckets WHERE name = ?`, name)
	}
	var b Bucket
	var createdAt int64
	err := row.Scan(&b.ARN, &b.Name, &b.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBucketNotFound
		}
		return nil, err
	}
	b.CreatedAt = time.Unix(createdAt, 0)
	return &b, nil
}

func (s *Store) GetBucketByARN(arn string) (*Bucket, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, account_id, created_at FROM s3tables_buckets WHERE arn = ?`, arn)
	var b Bucket
	var createdAt int64
	err := row.Scan(&b.ARN, &b.Name, &b.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBucketNotFound
		}
		return nil, err
	}
	b.CreatedAt = time.Unix(createdAt, 0)
	return &b, nil
}

func (s *Store) DeleteBucket(name string) error {
	var res sql.Result
	var err error
	if strings.Contains(name, "arn:") {
		res, err = s.store.DB().Exec(`DELETE FROM s3tables_buckets WHERE arn = ?`, name)
	} else {
		res, err = s.store.DB().Exec(`DELETE FROM s3tables_buckets WHERE name = ?`, name)
	}
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBucketNotFound
	}
	return nil
}

func (s *Store) ListBuckets() ([]Bucket, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, account_id, created_at FROM s3tables_buckets ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var buckets []Bucket
	for rows.Next() {
		var b Bucket
		var createdAt int64
		if err := rows.Scan(&b.ARN, &b.Name, &b.AccountID, &createdAt); err != nil {
			return nil, err
		}
		b.CreatedAt = time.Unix(createdAt, 0)
		buckets = append(buckets, b)
	}
	return buckets, rows.Err()
}

// Namespace operations

func (s *Store) CreateNamespace(bucketARN, namespace, accountID string) (*Namespace, error) {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_namespaces (id, bucket_arn, namespace, account_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		id, bucketARN, namespace, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Namespace{ID: id, BucketARN: bucketARN, Namespace: namespace, AccountID: accountID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetNamespace(bucketARN, namespace string) (*Namespace, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, bucket_arn, namespace, account_id, created_at FROM s3tables_namespaces WHERE bucket_arn = ? AND namespace = ?`,
		bucketARN, namespace)
	var ns Namespace
	var createdAt int64
	err := row.Scan(&ns.ID, &ns.BucketARN, &ns.Namespace, &ns.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNamespaceNotFound
		}
		return nil, err
	}
	ns.CreatedAt = time.Unix(createdAt, 0)
	return &ns, nil
}

func (s *Store) DeleteNamespace(bucketARN, namespace string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM s3tables_namespaces WHERE bucket_arn = ? AND namespace = ?`, bucketARN, namespace)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNamespaceNotFound
	}
	return nil
}

func (s *Store) ListNamespaces(bucketARN string) ([]Namespace, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, bucket_arn, namespace, account_id, created_at FROM s3tables_namespaces WHERE bucket_arn = ? ORDER BY created_at`,
		bucketARN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var nsList []Namespace
	for rows.Next() {
		var ns Namespace
		var createdAt int64
		if err := rows.Scan(&ns.ID, &ns.BucketARN, &ns.Namespace, &ns.AccountID, &createdAt); err != nil {
			return nil, err
		}
		ns.CreatedAt = time.Unix(createdAt, 0)
		nsList = append(nsList, ns)
	}
	return nsList, rows.Err()
}

// Table operations

func (s *Store) CreateTable(bucketARN, namespace, name, format, accountID string) (*Table, error) {
	arn := "arn:aws:s3tables:us-east-1:" + accountID + ":bucket/" + bucketARN + "/table/" + namespace + "/" + name
	now := time.Now().Unix()
	if format == "" {
		format = "ICEBERG"
	}
	versionToken := shared.GenerateUUID()
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_tables (arn, name, bucket_arn, namespace, type, format, metadata_location, version_token, account_id, created_at)
		 VALUES (?, ?, ?, ?, 'customer', ?, '', ?, ?, ?)`,
		arn, name, bucketARN, namespace, format, versionToken, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Table{ARN: arn, Name: name, BucketARN: bucketARN, Namespace: namespace, Type: "customer", Format: format, VersionToken: versionToken, AccountID: accountID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetTable(bucketARN, namespace, name string) (*Table, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, bucket_arn, namespace, type, format, metadata_location, version_token, account_id, created_at FROM s3tables_tables WHERE bucket_arn = ? AND namespace = ? AND name = ?`,
		bucketARN, namespace, name)
	var t Table
	var createdAt int64
	err := row.Scan(&t.ARN, &t.Name, &t.BucketARN, &t.Namespace, &t.Type, &t.Format, &t.MetadataLocation, &t.VersionToken, &t.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTableNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

func (s *Store) DeleteTable(bucketARN, namespace, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM s3tables_tables WHERE bucket_arn = ? AND namespace = ? AND name = ?`,
		bucketARN, namespace, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTableNotFound
	}
	return nil
}

func (s *Store) ListTables(bucketARN, namespace string) ([]Table, error) {
	query := `SELECT arn, name, bucket_arn, namespace, type, format, metadata_location, version_token, account_id, created_at FROM s3tables_tables WHERE bucket_arn = ?`
	args := []any{bucketARN}
	if namespace != "" {
		query += ` AND namespace = ?`
		args = append(args, namespace)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tables []Table
	for rows.Next() {
		var t Table
		var createdAt int64
		if err := rows.Scan(&t.ARN, &t.Name, &t.BucketARN, &t.Namespace, &t.Type, &t.Format, &t.MetadataLocation, &t.VersionToken, &t.AccountID, &createdAt); err != nil {
			return nil, err
		}
		t.CreatedAt = time.Unix(createdAt, 0)
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (s *Store) RenameTable(bucketARN, namespace, oldName, newNamespace, newName string) error {
	t, err := s.GetTable(bucketARN, namespace, oldName)
	if err != nil {
		return err
	}
	if newNamespace == "" {
		newNamespace = namespace
	}
	if newName == "" {
		newName = oldName
	}
	newARN := "arn:aws:s3tables:us-east-1:" + t.AccountID + ":bucket/" + bucketARN + "/table/" + newNamespace + "/" + newName
	_, err = s.store.DB().Exec(
		`UPDATE s3tables_tables SET namespace = ?, name = ?, arn = ? WHERE bucket_arn = ? AND namespace = ? AND name = ?`,
		newNamespace, newName, newARN, bucketARN, namespace, oldName,
	)
	return err
}

func (s *Store) UpdateTableMetadataLocation(bucketARN, namespace, name, metadataLocation, versionToken string) (*Table, error) {
	t, err := s.GetTable(bucketARN, namespace, name)
	if err != nil {
		return nil, err
	}
	if versionToken != "" && versionToken != t.VersionToken {
		return nil, errors.New("version token mismatch")
	}
	newToken := shared.GenerateUUID()
	_, err = s.store.DB().Exec(
		`UPDATE s3tables_tables SET metadata_location = ?, version_token = ? WHERE bucket_arn = ? AND namespace = ? AND name = ?`,
		metadataLocation, newToken, bucketARN, namespace, name,
	)
	if err != nil {
		return nil, err
	}
	t.MetadataLocation = metadataLocation
	t.VersionToken = newToken
	return t, nil
}

// Policy operations

func (s *Store) PutPolicy(resourceARN, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_policies (resource_arn, policy) VALUES (?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET policy = excluded.policy`,
		resourceARN, policy,
	)
	return err
}

func (s *Store) GetPolicy(resourceARN string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT policy FROM s3tables_policies WHERE resource_arn = ?`, resourceARN)
	var p string
	if err := row.Scan(&p); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errPolicyNotFound
		}
		return "", err
	}
	return p, nil
}

func (s *Store) DeletePolicy(resourceARN string) error {
	res, err := s.store.DB().Exec(`DELETE FROM s3tables_policies WHERE resource_arn = ?`, resourceARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

// Encryption operations

func (s *Store) PutEncryption(resourceARN, config string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_encryption (resource_arn, config) VALUES (?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET config = excluded.config`,
		resourceARN, config,
	)
	return err
}

func (s *Store) GetEncryption(resourceARN string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT config FROM s3tables_encryption WHERE resource_arn = ?`, resourceARN)
	var c string
	if err := row.Scan(&c); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errEncryptionNotFound
		}
		return "", err
	}
	return c, nil
}

func (s *Store) DeleteEncryption(resourceARN string) error {
	_, err := s.store.DB().Exec(`DELETE FROM s3tables_encryption WHERE resource_arn = ?`, resourceARN)
	return err
}

// Maintenance operations

func (s *Store) PutMaintenance(resourceARN, configType, value string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO s3tables_maintenance (resource_arn, config_type, value) VALUES (?, ?, ?)
		 ON CONFLICT(resource_arn, config_type) DO UPDATE SET value = excluded.value`,
		resourceARN, configType, value,
	)
	return err
}

func (s *Store) GetMaintenance(resourceARN, configType string) (string, error) {
	row := s.store.DB().QueryRow(
		`SELECT value FROM s3tables_maintenance WHERE resource_arn = ? AND config_type = ?`,
		resourceARN, configType,
	)
	var v string
	if err := row.Scan(&v); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errMaintenanceNotFound
		}
		return "", err
	}
	return v, nil
}

func (s *Store) ListMaintenance(resourceARN string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT config_type, value FROM s3tables_maintenance WHERE resource_arn = ?`, resourceARN,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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
