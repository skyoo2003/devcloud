// SPDX-License-Identifier: Apache-2.0

// internal/services/glue/store.go
package glue

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
	errDatabaseNotFound       = errors.New("database not found")
	errTableNotFound          = errors.New("table not found")
	errPartitionNotFound      = errors.New("partition not found")
	errCrawlerNotFound        = errors.New("crawler not found")
	errJobNotFound            = errors.New("job not found")
	errJobRunNotFound         = errors.New("job run not found")
	errConnectionNotFound     = errors.New("connection not found")
	errTriggerNotFound        = errors.New("trigger not found")
	errSecurityConfigNotFound = errors.New("security configuration not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS databases (
			catalog_id      TEXT NOT NULL DEFAULT '000000000000',
			name            TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			location_uri    TEXT NOT NULL DEFAULT '',
			parameters      TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (catalog_id, name)
		);
		CREATE TABLE IF NOT EXISTS tables (
			catalog_id      TEXT NOT NULL DEFAULT '000000000000',
			database_name   TEXT NOT NULL,
			name            TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			table_type      TEXT NOT NULL DEFAULT 'EXTERNAL_TABLE',
			parameters      TEXT NOT NULL DEFAULT '{}',
			storage_desc    TEXT NOT NULL DEFAULT '{}',
			columns         TEXT NOT NULL DEFAULT '[]',
			partition_keys  TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL,
			PRIMARY KEY (catalog_id, database_name, name)
		);
		CREATE TABLE IF NOT EXISTS partitions (
			catalog_id      TEXT NOT NULL DEFAULT '000000000000',
			database_name   TEXT NOT NULL,
			table_name      TEXT NOT NULL,
			values_key      TEXT NOT NULL,
			parameters      TEXT NOT NULL DEFAULT '{}',
			storage_desc    TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (catalog_id, database_name, table_name, values_key)
		);
		CREATE TABLE IF NOT EXISTS crawlers (
			name            TEXT PRIMARY KEY,
			role            TEXT NOT NULL DEFAULT '',
			database_name   TEXT NOT NULL DEFAULT '',
			targets         TEXT NOT NULL DEFAULT '{}',
			status          TEXT NOT NULL DEFAULT 'READY',
			schedule        TEXT NOT NULL DEFAULT '',
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS jobs (
			name            TEXT PRIMARY KEY,
			role            TEXT NOT NULL DEFAULT '',
			command         TEXT NOT NULL DEFAULT '{}',
			max_retries     INTEGER NOT NULL DEFAULT 0,
			timeout         INTEGER NOT NULL DEFAULT 2880,
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS job_runs (
			id              TEXT PRIMARY KEY,
			job_name        TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'SUCCEEDED',
			started_at      INTEGER NOT NULL,
			completed_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS connections (
			catalog_id      TEXT NOT NULL DEFAULT '000000000000',
			name            TEXT NOT NULL,
			type            TEXT NOT NULL DEFAULT 'JDBC',
			properties      TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (catalog_id, name)
		);
		CREATE TABLE IF NOT EXISTS triggers (
			name            TEXT PRIMARY KEY,
			type            TEXT NOT NULL DEFAULT 'ON_DEMAND',
			state           TEXT NOT NULL DEFAULT 'CREATED',
			actions         TEXT NOT NULL DEFAULT '[]',
			predicate       TEXT NOT NULL DEFAULT '{}',
			schedule        TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS security_configs (
			name            TEXT PRIMARY KEY,
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
	`},
}

// ---- model types ----

type Database struct {
	CatalogID   string
	Name        string
	Description string
	LocationURI string
	Parameters  string
	CreatedAt   time.Time
}

type Table struct {
	CatalogID     string
	DatabaseName  string
	Name          string
	Description   string
	TableType     string
	Parameters    string
	StorageDesc   string
	Columns       string
	PartitionKeys string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Partition struct {
	CatalogID    string
	DatabaseName string
	TableName    string
	ValuesKey    string
	Parameters   string
	StorageDesc  string
	CreatedAt    time.Time
}

type Crawler struct {
	Name         string
	Role         string
	DatabaseName string
	Targets      string
	Status       string
	Schedule     string
	Config       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Job struct {
	Name       string
	Role       string
	Command    string
	MaxRetries int
	Timeout    int
	Config     string
	CreatedAt  time.Time
}

type JobRun struct {
	ID          string
	JobName     string
	Status      string
	StartedAt   time.Time
	CompletedAt time.Time
}

type Connection struct {
	CatalogID  string
	Name       string
	Type       string
	Properties string
	CreatedAt  time.Time
}

type Trigger struct {
	Name      string
	Type      string
	State     string
	Actions   string
	Predicate string
	Schedule  string
}

type SecurityConfig struct {
	Name      string
	Config    string
	CreatedAt time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "glue.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

type scanner interface{ Scan(dest ...any) error }

// ---- Database ----

func (s *Store) CreateDatabase(catalogID, name, description, locationURI, parameters string) (*Database, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO databases (catalog_id, name, description, location_uri, parameters, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		catalogID, name, description, locationURI, parameters, now,
	)
	if err != nil {
		return nil, err
	}
	return &Database{CatalogID: catalogID, Name: name, Description: description, LocationURI: locationURI, Parameters: parameters, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetDatabase(catalogID, name string) (*Database, error) {
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, name, description, location_uri, parameters, created_at FROM databases WHERE catalog_id = ? AND name = ?`,
		catalogID, name)
	return scanDatabase(row)
}

func (s *Store) ListDatabases(catalogID string) ([]Database, error) {
	rows, err := s.store.DB().Query(
		`SELECT catalog_id, name, description, location_uri, parameters, created_at FROM databases WHERE catalog_id = ? ORDER BY name`,
		catalogID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var dbs []Database
	for rows.Next() {
		db, err := scanDatabase(rows)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, *db)
	}
	return dbs, rows.Err()
}

func (s *Store) UpdateDatabase(catalogID, name, description, locationURI, parameters string) error {
	res, err := s.store.DB().Exec(
		`UPDATE databases SET description = ?, location_uri = ?, parameters = ? WHERE catalog_id = ? AND name = ?`,
		description, locationURI, parameters, catalogID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDatabaseNotFound
	}
	return nil
}

func (s *Store) DeleteDatabase(catalogID, name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM databases WHERE catalog_id = ? AND name = ?`, catalogID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDatabaseNotFound
	}
	return nil
}

func scanDatabase(sc scanner) (*Database, error) {
	var d Database
	var createdAt int64
	err := sc.Scan(&d.CatalogID, &d.Name, &d.Description, &d.LocationURI, &d.Parameters, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDatabaseNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

// ---- Table ----

func (s *Store) CreateTable(catalogID, databaseName, name, description, tableType, parameters, storageDesc, columns, partitionKeys string) (*Table, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO tables (catalog_id, database_name, name, description, table_type, parameters, storage_desc, columns, partition_keys, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		catalogID, databaseName, name, description, tableType, parameters, storageDesc, columns, partitionKeys, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Table{
		CatalogID: catalogID, DatabaseName: databaseName, Name: name,
		Description: description, TableType: tableType, Parameters: parameters,
		StorageDesc: storageDesc, Columns: columns, PartitionKeys: partitionKeys,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTable(catalogID, databaseName, name string) (*Table, error) {
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, database_name, name, description, table_type, parameters, storage_desc, columns, partition_keys, created_at, updated_at FROM tables WHERE catalog_id = ? AND database_name = ? AND name = ?`,
		catalogID, databaseName, name)
	return scanTable(row)
}

func (s *Store) ListTables(catalogID, databaseName string) ([]Table, error) {
	rows, err := s.store.DB().Query(
		`SELECT catalog_id, database_name, name, description, table_type, parameters, storage_desc, columns, partition_keys, created_at, updated_at FROM tables WHERE catalog_id = ? AND database_name = ? ORDER BY name`,
		catalogID, databaseName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tables []Table
	for rows.Next() {
		t, err := scanTable(rows)
		if err != nil {
			return nil, err
		}
		tables = append(tables, *t)
	}
	return tables, rows.Err()
}

func (s *Store) UpdateTable(catalogID, databaseName, name, description, tableType, parameters, storageDesc, columns, partitionKeys string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE tables SET description = ?, table_type = ?, parameters = ?, storage_desc = ?, columns = ?, partition_keys = ?, updated_at = ? WHERE catalog_id = ? AND database_name = ? AND name = ?`,
		description, tableType, parameters, storageDesc, columns, partitionKeys, now, catalogID, databaseName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTableNotFound
	}
	return nil
}

func (s *Store) DeleteTable(catalogID, databaseName, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM tables WHERE catalog_id = ? AND database_name = ? AND name = ?`,
		catalogID, databaseName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTableNotFound
	}
	return nil
}

func scanTable(sc scanner) (*Table, error) {
	var t Table
	var createdAt, updatedAt int64
	err := sc.Scan(&t.CatalogID, &t.DatabaseName, &t.Name, &t.Description, &t.TableType, &t.Parameters, &t.StorageDesc, &t.Columns, &t.PartitionKeys, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTableNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	t.UpdatedAt = time.Unix(updatedAt, 0)
	return &t, nil
}

// ---- Partition ----

func partitionValuesKey(values []string) string {
	return strings.Join(values, "\x00")
}

func (s *Store) CreatePartition(catalogID, databaseName, tableName string, values []string, parameters, storageDesc string) (*Partition, error) {
	now := time.Now().Unix()
	vk := partitionValuesKey(values)
	_, err := s.store.DB().Exec(
		`INSERT INTO partitions (catalog_id, database_name, table_name, values_key, parameters, storage_desc, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		catalogID, databaseName, tableName, vk, parameters, storageDesc, now,
	)
	if err != nil {
		return nil, err
	}
	return &Partition{CatalogID: catalogID, DatabaseName: databaseName, TableName: tableName, ValuesKey: vk, Parameters: parameters, StorageDesc: storageDesc, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetPartition(catalogID, databaseName, tableName string, values []string) (*Partition, error) {
	vk := partitionValuesKey(values)
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, database_name, table_name, values_key, parameters, storage_desc, created_at FROM partitions WHERE catalog_id = ? AND database_name = ? AND table_name = ? AND values_key = ?`,
		catalogID, databaseName, tableName, vk)
	return scanPartition(row)
}

func (s *Store) ListPartitions(catalogID, databaseName, tableName string) ([]Partition, error) {
	rows, err := s.store.DB().Query(
		`SELECT catalog_id, database_name, table_name, values_key, parameters, storage_desc, created_at FROM partitions WHERE catalog_id = ? AND database_name = ? AND table_name = ? ORDER BY values_key`,
		catalogID, databaseName, tableName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var parts []Partition
	for rows.Next() {
		p, err := scanPartition(rows)
		if err != nil {
			return nil, err
		}
		parts = append(parts, *p)
	}
	return parts, rows.Err()
}

func (s *Store) UpdatePartition(catalogID, databaseName, tableName string, values []string, parameters, storageDesc string) error {
	vk := partitionValuesKey(values)
	res, err := s.store.DB().Exec(
		`UPDATE partitions SET parameters = ?, storage_desc = ? WHERE catalog_id = ? AND database_name = ? AND table_name = ? AND values_key = ?`,
		parameters, storageDesc, catalogID, databaseName, tableName, vk)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPartitionNotFound
	}
	return nil
}

func (s *Store) DeletePartition(catalogID, databaseName, tableName string, values []string) error {
	vk := partitionValuesKey(values)
	res, err := s.store.DB().Exec(
		`DELETE FROM partitions WHERE catalog_id = ? AND database_name = ? AND table_name = ? AND values_key = ?`,
		catalogID, databaseName, tableName, vk)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPartitionNotFound
	}
	return nil
}

func scanPartition(sc scanner) (*Partition, error) {
	var p Partition
	var createdAt int64
	err := sc.Scan(&p.CatalogID, &p.DatabaseName, &p.TableName, &p.ValuesKey, &p.Parameters, &p.StorageDesc, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPartitionNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

// ---- Crawler ----

func (s *Store) CreateCrawler(name, role, databaseName, targets, schedule, config string) (*Crawler, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO crawlers (name, role, database_name, targets, status, schedule, config, created_at, updated_at) VALUES (?, ?, ?, ?, 'READY', ?, ?, ?, ?)`,
		name, role, databaseName, targets, schedule, config, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Crawler{Name: name, Role: role, DatabaseName: databaseName, Targets: targets, Status: "READY", Schedule: schedule, Config: config, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetCrawler(name string) (*Crawler, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, role, database_name, targets, status, schedule, config, created_at, updated_at FROM crawlers WHERE name = ?`, name)
	return scanCrawler(row)
}

func (s *Store) ListCrawlers() ([]Crawler, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, role, database_name, targets, status, schedule, config, created_at, updated_at FROM crawlers ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var crawlers []Crawler
	for rows.Next() {
		c, err := scanCrawler(rows)
		if err != nil {
			return nil, err
		}
		crawlers = append(crawlers, *c)
	}
	return crawlers, rows.Err()
}

func (s *Store) UpdateCrawler(name, role, databaseName, targets, schedule, config string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE crawlers SET role = ?, database_name = ?, targets = ?, schedule = ?, config = ?, updated_at = ? WHERE name = ?`,
		role, databaseName, targets, schedule, config, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCrawlerNotFound
	}
	return nil
}

func (s *Store) UpdateCrawlerStatus(name, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE crawlers SET status = ?, updated_at = ? WHERE name = ?`, status, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCrawlerNotFound
	}
	return nil
}

func (s *Store) DeleteCrawler(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM crawlers WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCrawlerNotFound
	}
	return nil
}

func scanCrawler(sc scanner) (*Crawler, error) {
	var c Crawler
	var createdAt, updatedAt int64
	err := sc.Scan(&c.Name, &c.Role, &c.DatabaseName, &c.Targets, &c.Status, &c.Schedule, &c.Config, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errCrawlerNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	c.UpdatedAt = time.Unix(updatedAt, 0)
	return &c, nil
}

// ---- Job ----

func (s *Store) CreateJob(name, role, command string, maxRetries, timeout int, config string) (*Job, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO jobs (name, role, command, max_retries, timeout, config, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, role, command, maxRetries, timeout, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &Job{Name: name, Role: role, Command: command, MaxRetries: maxRetries, Timeout: timeout, Config: config, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetJob(name string) (*Job, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, role, command, max_retries, timeout, config, created_at FROM jobs WHERE name = ?`, name)
	return scanJob(row)
}

func (s *Store) ListJobs() ([]Job, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, role, command, max_retries, timeout, config, created_at FROM jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

func (s *Store) UpdateJob(name, role, command string, maxRetries, timeout int, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE jobs SET role = ?, command = ?, max_retries = ?, timeout = ?, config = ? WHERE name = ?`,
		role, command, maxRetries, timeout, config, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobNotFound
	}
	return nil
}

func (s *Store) DeleteJob(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM jobs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobNotFound
	}
	return nil
}

func scanJob(sc scanner) (*Job, error) {
	var j Job
	var createdAt int64
	err := sc.Scan(&j.Name, &j.Role, &j.Command, &j.MaxRetries, &j.Timeout, &j.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errJobNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	return &j, nil
}

// ---- JobRun ----

func (s *Store) CreateJobRun(id, jobName string) (*JobRun, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO job_runs (id, job_name, status, started_at, completed_at) VALUES (?, ?, 'SUCCEEDED', ?, 0)`,
		id, jobName, now,
	)
	if err != nil {
		return nil, err
	}
	return &JobRun{ID: id, JobName: jobName, Status: "SUCCEEDED", StartedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetJobRun(id string) (*JobRun, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, job_name, status, started_at, completed_at FROM job_runs WHERE id = ?`, id)
	return scanJobRun(row)
}

func (s *Store) ListJobRuns(jobName string) ([]JobRun, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, job_name, status, started_at, completed_at FROM job_runs WHERE job_name = ? ORDER BY started_at DESC`,
		jobName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var runs []JobRun
	for rows.Next() {
		r, err := scanJobRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *r)
	}
	return runs, rows.Err()
}

func (s *Store) UpdateJobRunStatus(id, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE job_runs SET status = ?, completed_at = ? WHERE id = ?`, status, now, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobRunNotFound
	}
	return nil
}

func scanJobRun(sc scanner) (*JobRun, error) {
	var r JobRun
	var startedAt, completedAt int64
	err := sc.Scan(&r.ID, &r.JobName, &r.Status, &startedAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errJobRunNotFound
		}
		return nil, err
	}
	r.StartedAt = time.Unix(startedAt, 0)
	r.CompletedAt = time.Unix(completedAt, 0)
	return &r, nil
}

// ---- Connection ----

func (s *Store) CreateConnection(catalogID, name, connType, properties string) (*Connection, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO connections (catalog_id, name, type, properties, created_at) VALUES (?, ?, ?, ?, ?)`,
		catalogID, name, connType, properties, now,
	)
	if err != nil {
		return nil, err
	}
	return &Connection{CatalogID: catalogID, Name: name, Type: connType, Properties: properties, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetConnection(catalogID, name string) (*Connection, error) {
	row := s.store.DB().QueryRow(
		`SELECT catalog_id, name, type, properties, created_at FROM connections WHERE catalog_id = ? AND name = ?`,
		catalogID, name)
	return scanConnection(row)
}

func (s *Store) ListConnections(catalogID string) ([]Connection, error) {
	rows, err := s.store.DB().Query(
		`SELECT catalog_id, name, type, properties, created_at FROM connections WHERE catalog_id = ? ORDER BY name`,
		catalogID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var conns []Connection
	for rows.Next() {
		c, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		conns = append(conns, *c)
	}
	return conns, rows.Err()
}

func (s *Store) UpdateConnection(catalogID, name, connType, properties string) error {
	res, err := s.store.DB().Exec(
		`UPDATE connections SET type = ?, properties = ? WHERE catalog_id = ? AND name = ?`,
		connType, properties, catalogID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConnectionNotFound
	}
	return nil
}

func (s *Store) DeleteConnection(catalogID, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM connections WHERE catalog_id = ? AND name = ?`, catalogID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConnectionNotFound
	}
	return nil
}

func scanConnection(sc scanner) (*Connection, error) {
	var c Connection
	var createdAt int64
	err := sc.Scan(&c.CatalogID, &c.Name, &c.Type, &c.Properties, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConnectionNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

// ---- Trigger ----

func (s *Store) CreateTrigger(name, trigType, actions, predicate, schedule string) (*Trigger, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO triggers (name, type, state, actions, predicate, schedule) VALUES (?, ?, 'CREATED', ?, ?, ?)`,
		name, trigType, actions, predicate, schedule,
	)
	if err != nil {
		return nil, err
	}
	return &Trigger{Name: name, Type: trigType, State: "CREATED", Actions: actions, Predicate: predicate, Schedule: schedule}, nil
}

func (s *Store) GetTrigger(name string) (*Trigger, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, type, state, actions, predicate, schedule FROM triggers WHERE name = ?`, name)
	return scanTrigger(row)
}

func (s *Store) ListTriggers() ([]Trigger, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, type, state, actions, predicate, schedule FROM triggers ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var triggers []Trigger
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, *t)
	}
	return triggers, rows.Err()
}

func (s *Store) UpdateTrigger(name, trigType, actions, predicate, schedule string) error {
	res, err := s.store.DB().Exec(
		`UPDATE triggers SET type = ?, actions = ?, predicate = ?, schedule = ? WHERE name = ?`,
		trigType, actions, predicate, schedule, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTriggerNotFound
	}
	return nil
}

func (s *Store) UpdateTriggerState(name, state string) error {
	res, err := s.store.DB().Exec(`UPDATE triggers SET state = ? WHERE name = ?`, state, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTriggerNotFound
	}
	return nil
}

func (s *Store) DeleteTrigger(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM triggers WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTriggerNotFound
	}
	return nil
}

func scanTrigger(sc scanner) (*Trigger, error) {
	var t Trigger
	err := sc.Scan(&t.Name, &t.Type, &t.State, &t.Actions, &t.Predicate, &t.Schedule)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTriggerNotFound
		}
		return nil, err
	}
	return &t, nil
}

// ---- SecurityConfiguration ----

func (s *Store) CreateSecurityConfig(name, config string) (*SecurityConfig, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO security_configs (name, config, created_at) VALUES (?, ?, ?)`,
		name, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &SecurityConfig{Name: name, Config: config, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetSecurityConfig(name string) (*SecurityConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, config, created_at FROM security_configs WHERE name = ?`, name)
	return scanSecurityConfig(row)
}

func (s *Store) ListSecurityConfigs() ([]SecurityConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, config, created_at FROM security_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var configs []SecurityConfig
	for rows.Next() {
		c, err := scanSecurityConfig(rows)
		if err != nil {
			return nil, err
		}
		configs = append(configs, *c)
	}
	return configs, rows.Err()
}

func (s *Store) DeleteSecurityConfig(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM security_configs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSecurityConfigNotFound
	}
	return nil
}

func scanSecurityConfig(sc scanner) (*SecurityConfig, error) {
	var c SecurityConfig
	var createdAt int64
	err := sc.Scan(&c.Name, &c.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSecurityConfigNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
