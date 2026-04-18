// SPDX-License-Identifier: Apache-2.0

// internal/services/timestreamwrite/store.go
package timestreamwrite

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errDatabaseNotFound  = errors.New("database not found")
	errTableNotFound     = errors.New("table not found")
	errBatchLoadNotFound = errors.New("batch load task not found")
	errDatabaseExists    = errors.New("database already exists")
	errTableExists       = errors.New("table already exists")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS databases (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			kms_key_id  TEXT NOT NULL DEFAULT '',
			table_count INTEGER NOT NULL DEFAULT 0,
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS tables (
			name          TEXT NOT NULL,
			database_name TEXT NOT NULL,
			arn           TEXT NOT NULL UNIQUE,
			status        TEXT NOT NULL DEFAULT 'ACTIVE',
			retention_mem TEXT NOT NULL DEFAULT '6',
			retention_mag TEXT NOT NULL DEFAULT '73000',
			schema_def    TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL,
			updated_at    INTEGER NOT NULL,
			PRIMARY KEY (name, database_name)
		);
		CREATE TABLE IF NOT EXISTS batch_load_tasks (
			id            TEXT PRIMARY KEY,
			database_name TEXT NOT NULL,
			table_name    TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'CREATED',
			data_source   TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL
		);
	`},
}

// DatabaseRow represents a database record.
type DatabaseRow struct {
	Name       string
	ARN        string
	KMSKeyID   string
	TableCount int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// TableRow represents a table record.
type TableRow struct {
	Name         string
	DatabaseName string
	ARN          string
	Status       string
	RetentionMem string
	RetentionMag string
	SchemaDef    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// BatchLoadTaskRow represents a batch load task record.
type BatchLoadTaskRow struct {
	ID           string
	DatabaseName string
	TableName    string
	Status       string
	DataSource   string
	CreatedAt    time.Time
}

// Store wraps SQLite for Timestream Write persistence.
type Store struct {
	sqlStore *sqlite.Store
	tags     *shared.TagStore
}

// NewStore creates a new Store backed by SQLite.
func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "timestreamwrite.db")
	allMigrations := append(migrations, shared.TagMigrations...)
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{sqlStore: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.sqlStore.Close() }

// ---- Database CRUD ----

func (s *Store) CreateDatabase(name, kmsKeyID string) (*DatabaseRow, error) {
	arn := shared.BuildARN("timestream", "database", name)
	now := time.Now().Unix()
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO databases (name, arn, kms_key_id, table_count, created_at, updated_at)
		 VALUES (?, ?, ?, 0, ?, ?)`,
		name, arn, kmsKeyID, now, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, errDatabaseExists
		}
		return nil, err
	}
	return &DatabaseRow{
		Name: name, ARN: arn, KMSKeyID: kmsKeyID,
		TableCount: 0,
		CreatedAt:  time.Unix(now, 0),
		UpdatedAt:  time.Unix(now, 0),
	}, nil
}

func (s *Store) GetDatabase(name string) (*DatabaseRow, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT name, arn, kms_key_id, table_count, created_at, updated_at
		 FROM databases WHERE name = ?`, name)
	return scanDatabase(row)
}

func (s *Store) ListDatabases() ([]*DatabaseRow, error) {
	rows, err := s.sqlStore.DB().Query(
		`SELECT name, arn, kms_key_id, table_count, created_at, updated_at
		 FROM databases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var dbs []*DatabaseRow
	for rows.Next() {
		db, err := scanDatabase(rows)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, rows.Err()
}

func (s *Store) UpdateDatabase(name, kmsKeyID string) (*DatabaseRow, error) {
	now := time.Now().Unix()
	res, err := s.sqlStore.DB().Exec(
		`UPDATE databases SET kms_key_id = ?, updated_at = ? WHERE name = ?`,
		kmsKeyID, now, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errDatabaseNotFound
	}
	return s.GetDatabase(name)
}

func (s *Store) DeleteDatabase(name string) error {
	res, err := s.sqlStore.DB().Exec(`DELETE FROM databases WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDatabaseNotFound
	}
	return nil
}

// ---- Table CRUD ----

func (s *Store) CreateTable(dbName, tableName string) (*TableRow, error) {
	arn := shared.BuildARN("timestream", "database", dbName+"/table/"+tableName)
	now := time.Now().Unix()
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO tables (name, database_name, arn, status, retention_mem, retention_mag, schema_def, created_at, updated_at)
		 VALUES (?, ?, ?, 'ACTIVE', '6', '73000', '{}', ?, ?)`,
		tableName, dbName, arn, now, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, errTableExists
		}
		return nil, err
	}
	// Update table count.
	_, _ = s.sqlStore.DB().Exec(
		`UPDATE databases SET table_count = table_count + 1, updated_at = ? WHERE name = ?`,
		now, dbName)
	return &TableRow{
		Name: tableName, DatabaseName: dbName, ARN: arn,
		Status: "ACTIVE", RetentionMem: "6", RetentionMag: "73000",
		SchemaDef: "{}",
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTable(dbName, tableName string) (*TableRow, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT name, database_name, arn, status, retention_mem, retention_mag, schema_def, created_at, updated_at
		 FROM tables WHERE name = ? AND database_name = ?`, tableName, dbName)
	return scanTable(row)
}

func (s *Store) ListTables(dbName string) ([]*TableRow, error) {
	query := `SELECT name, database_name, arn, status, retention_mem, retention_mag, schema_def, created_at, updated_at FROM tables`
	var args []any
	if dbName != "" {
		query += ` WHERE database_name = ?`
		args = append(args, dbName)
	}
	query += ` ORDER BY name`
	rows, err := s.sqlStore.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tables []*TableRow
	for rows.Next() {
		t, err := scanTable(rows)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (s *Store) UpdateTable(dbName, tableName, retentionMem, retentionMag string) (*TableRow, error) {
	now := time.Now().Unix()
	res, err := s.sqlStore.DB().Exec(
		`UPDATE tables SET retention_mem = ?, retention_mag = ?, updated_at = ?
		 WHERE name = ? AND database_name = ?`,
		retentionMem, retentionMag, now, tableName, dbName)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errTableNotFound
	}
	return s.GetTable(dbName, tableName)
}

func (s *Store) DeleteTable(dbName, tableName string) error {
	now := time.Now().Unix()
	res, err := s.sqlStore.DB().Exec(
		`DELETE FROM tables WHERE name = ? AND database_name = ?`, tableName, dbName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTableNotFound
	}
	_, _ = s.sqlStore.DB().Exec(
		`UPDATE databases SET table_count = table_count - 1, updated_at = ? WHERE name = ?`,
		now, dbName)
	return nil
}

// ---- BatchLoadTask CRUD ----

func (s *Store) CreateBatchLoadTask(dbName, tableName, dataSource string) (*BatchLoadTaskRow, error) {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.sqlStore.DB().Exec(
		`INSERT INTO batch_load_tasks (id, database_name, table_name, status, data_source, created_at)
		 VALUES (?, ?, ?, 'CREATED', ?, ?)`,
		id, dbName, tableName, dataSource, now,
	)
	if err != nil {
		return nil, err
	}
	return &BatchLoadTaskRow{
		ID: id, DatabaseName: dbName, TableName: tableName,
		Status: "CREATED", DataSource: dataSource,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetBatchLoadTask(id string) (*BatchLoadTaskRow, error) {
	row := s.sqlStore.DB().QueryRow(
		`SELECT id, database_name, table_name, status, data_source, created_at
		 FROM batch_load_tasks WHERE id = ?`, id)
	return scanBatchLoadTask(row)
}

func (s *Store) ListBatchLoadTasks(statusFilter string) ([]*BatchLoadTaskRow, error) {
	query := `SELECT id, database_name, table_name, status, data_source, created_at FROM batch_load_tasks`
	var args []any
	if statusFilter != "" {
		query += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.sqlStore.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tasks []*BatchLoadTaskRow
	for rows.Next() {
		t, err := scanBatchLoadTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *Store) ResumeBatchLoadTask(id string) error {
	res, err := s.sqlStore.DB().Exec(
		`UPDATE batch_load_tasks SET status = 'IN_PROGRESS' WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBatchLoadNotFound
	}
	return nil
}

// ---- Tag helpers ----

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanDatabase(sc scanner) (*DatabaseRow, error) {
	var r DatabaseRow
	var createdAt, updatedAt int64
	err := sc.Scan(&r.Name, &r.ARN, &r.KMSKeyID, &r.TableCount, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDatabaseNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.UpdatedAt = time.Unix(updatedAt, 0)
	return &r, nil
}

func scanTable(sc scanner) (*TableRow, error) {
	var r TableRow
	var createdAt, updatedAt int64
	err := sc.Scan(&r.Name, &r.DatabaseName, &r.ARN, &r.Status,
		&r.RetentionMem, &r.RetentionMag, &r.SchemaDef, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTableNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.UpdatedAt = time.Unix(updatedAt, 0)
	return &r, nil
}

func scanBatchLoadTask(sc scanner) (*BatchLoadTaskRow, error) {
	var r BatchLoadTaskRow
	var createdAt int64
	err := sc.Scan(&r.ID, &r.DatabaseName, &r.TableName, &r.Status, &r.DataSource, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBatchLoadNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func dbRowToMap(r *DatabaseRow) map[string]any {
	return map[string]any{
		"Arn":             r.ARN,
		"DatabaseName":    r.Name,
		"KmsKeyId":        r.KMSKeyID,
		"TableCount":      r.TableCount,
		"CreationTime":    r.CreatedAt.Unix(),
		"LastUpdatedTime": r.UpdatedAt.Unix(),
	}
}

func tableRowToMap(r *TableRow) map[string]any {
	memHours := int64(6)
	magDays := int64(73000)
	_, _ = fmt.Sscanf(r.RetentionMem, "%d", &memHours)
	_, _ = fmt.Sscanf(r.RetentionMag, "%d", &magDays)
	return map[string]any{
		"Arn":          r.ARN,
		"TableName":    r.Name,
		"DatabaseName": r.DatabaseName,
		"TableStatus":  r.Status,
		"RetentionProperties": map[string]any{
			"MemoryStoreRetentionPeriodInHours":  memHours,
			"MagneticStoreRetentionPeriodInDays": magDays,
		},
		"CreationTime":    r.CreatedAt.Unix(),
		"LastUpdatedTime": r.UpdatedAt.Unix(),
	}
}

func batchTaskToMap(r *BatchLoadTaskRow) map[string]any {
	return map[string]any{
		"TaskId":       r.ID,
		"DatabaseName": r.DatabaseName,
		"TableName":    r.TableName,
		"TaskStatus":   r.Status,
		"CreationTime": r.CreatedAt.Unix(),
	}
}
