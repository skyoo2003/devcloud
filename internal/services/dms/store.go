// SPDX-License-Identifier: Apache-2.0

// internal/services/dms/store.go
package dms

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errReplicationInstanceNotFound = errors.New("replication instance not found")
	errReplicationTaskNotFound     = errors.New("replication task not found")
	errEndpointNotFound            = errors.New("endpoint not found")
	errEventSubNotFound            = errors.New("event subscription not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS replication_instances (
			arn           TEXT PRIMARY KEY,
			identifier    TEXT NOT NULL UNIQUE,
			instance_class TEXT NOT NULL DEFAULT 'dms.t2.micro',
			engine_version TEXT NOT NULL DEFAULT '3.5.1',
			status        TEXT NOT NULL DEFAULT 'available',
			multi_az      INTEGER NOT NULL DEFAULT 0,
			allocated_storage INTEGER NOT NULL DEFAULT 20,
			publicly_accessible INTEGER NOT NULL DEFAULT 0,
			account_id    TEXT NOT NULL,
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS replication_tasks (
			arn            TEXT PRIMARY KEY,
			identifier     TEXT NOT NULL UNIQUE,
			instance_arn   TEXT NOT NULL DEFAULT '',
			source_arn     TEXT NOT NULL DEFAULT '',
			target_arn     TEXT NOT NULL DEFAULT '',
			migration_type TEXT NOT NULL DEFAULT 'full-load',
			table_mappings TEXT NOT NULL DEFAULT '{}',
			task_settings  TEXT NOT NULL DEFAULT '{}',
			status         TEXT NOT NULL DEFAULT 'ready',
			account_id     TEXT NOT NULL,
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS dms_endpoints (
			arn            TEXT PRIMARY KEY,
			identifier     TEXT NOT NULL UNIQUE,
			endpoint_type  TEXT NOT NULL DEFAULT 'source',
			engine_name    TEXT NOT NULL DEFAULT 'mysql',
			server_name    TEXT NOT NULL DEFAULT '',
			port           INTEGER NOT NULL DEFAULT 3306,
			database_name  TEXT NOT NULL DEFAULT '',
			username       TEXT NOT NULL DEFAULT '',
			status         TEXT NOT NULL DEFAULT 'active',
			ssl_mode       TEXT NOT NULL DEFAULT 'none',
			account_id     TEXT NOT NULL,
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS event_subscriptions (
			name           TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			sns_topic_arn  TEXT NOT NULL DEFAULT '',
			source_type    TEXT NOT NULL DEFAULT '',
			event_categories TEXT NOT NULL DEFAULT '[]',
			enabled        INTEGER NOT NULL DEFAULT 1,
			status         TEXT NOT NULL DEFAULT 'active',
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS dms_tags (
			resource_arn TEXT NOT NULL,
			tag_key      TEXT NOT NULL,
			tag_value    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (resource_arn, tag_key)
		);
	`},
}

type ReplicationInstance struct {
	ARN                string
	Identifier         string
	InstanceClass      string
	EngineVersion      string
	Status             string
	MultiAZ            bool
	AllocatedStorage   int
	PubliclyAccessible bool
	AccountID          string
	CreatedAt          time.Time
}

type ReplicationTask struct {
	ARN           string
	Identifier    string
	InstanceARN   string
	SourceARN     string
	TargetARN     string
	MigrationType string
	TableMappings string
	TaskSettings  string
	Status        string
	AccountID     string
	CreatedAt     time.Time
}

type DMSEndpoint struct {
	ARN          string
	Identifier   string
	EndpointType string
	EngineName   string
	ServerName   string
	Port         int
	DatabaseName string
	Username     string
	Status       string
	SSLMode      string
	AccountID    string
	CreatedAt    time.Time
}

type EventSubscription struct {
	Name            string
	AccountID       string
	SnsTopicARN     string
	SourceType      string
	EventCategories string
	Enabled         bool
	Status          string
	CreatedAt       time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "dms.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Replication Instances ---

func (s *Store) CreateReplicationInstance(ri *ReplicationInstance) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO replication_instances (arn, identifier, instance_class, engine_version, status, multi_az, allocated_storage, publicly_accessible, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ri.ARN, ri.Identifier, ri.InstanceClass, ri.EngineVersion, ri.Status,
		boolToInt(ri.MultiAZ), ri.AllocatedStorage, boolToInt(ri.PubliclyAccessible),
		ri.AccountID, ri.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetReplicationInstance(identifier string) (*ReplicationInstance, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, identifier, instance_class, engine_version, status, multi_az, allocated_storage, publicly_accessible, account_id, created_at
		 FROM replication_instances WHERE identifier = ? OR arn = ?`, identifier, identifier)
	return scanInstance(row)
}

func (s *Store) ListReplicationInstances() ([]ReplicationInstance, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, identifier, instance_class, engine_version, status, multi_az, allocated_storage, publicly_accessible, account_id, created_at
		 FROM replication_instances ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicationInstance
	for rows.Next() {
		r, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteReplicationInstance(identifier string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM replication_instances WHERE identifier = ? OR arn = ?`, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplicationInstanceNotFound
	}
	return nil
}

func (s *Store) ModifyReplicationInstance(identifier, instanceClass, engineVersion string) error {
	res, err := s.store.DB().Exec(
		`UPDATE replication_instances SET
		   instance_class = COALESCE(NULLIF(?, ''), instance_class),
		   engine_version = COALESCE(NULLIF(?, ''), engine_version)
		 WHERE identifier = ? OR arn = ?`,
		instanceClass, engineVersion, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplicationInstanceNotFound
	}
	return nil
}

// --- Replication Tasks ---

func (s *Store) CreateReplicationTask(rt *ReplicationTask) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO replication_tasks (arn, identifier, instance_arn, source_arn, target_arn, migration_type, table_mappings, task_settings, status, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rt.ARN, rt.Identifier, rt.InstanceARN, rt.SourceARN, rt.TargetARN, rt.MigrationType,
		rt.TableMappings, rt.TaskSettings, rt.Status, rt.AccountID, rt.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetReplicationTask(identifier string) (*ReplicationTask, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, identifier, instance_arn, source_arn, target_arn, migration_type, table_mappings, task_settings, status, account_id, created_at
		 FROM replication_tasks WHERE identifier = ? OR arn = ?`, identifier, identifier)
	return scanTask(row)
}

func (s *Store) ListReplicationTasks() ([]ReplicationTask, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, identifier, instance_arn, source_arn, target_arn, migration_type, table_mappings, task_settings, status, account_id, created_at
		 FROM replication_tasks ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicationTask
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateTaskStatus(identifier, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE replication_tasks SET status = ? WHERE identifier = ? OR arn = ?`,
		status, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplicationTaskNotFound
	}
	return nil
}

func (s *Store) ModifyReplicationTask(identifier, migrationType, tableMappings, taskSettings string) error {
	res, err := s.store.DB().Exec(
		`UPDATE replication_tasks SET
		   migration_type = COALESCE(NULLIF(?, ''), migration_type),
		   table_mappings = COALESCE(NULLIF(?, ''), table_mappings),
		   task_settings = COALESCE(NULLIF(?, ''), task_settings)
		 WHERE identifier = ? OR arn = ?`,
		migrationType, tableMappings, taskSettings, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplicationTaskNotFound
	}
	return nil
}

func (s *Store) DeleteReplicationTask(identifier string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM replication_tasks WHERE identifier = ? OR arn = ?`, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplicationTaskNotFound
	}
	return nil
}

// --- Endpoints ---

func (s *Store) CreateEndpoint(ep *DMSEndpoint) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO dms_endpoints (arn, identifier, endpoint_type, engine_name, server_name, port, database_name, username, status, ssl_mode, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ep.ARN, ep.Identifier, ep.EndpointType, ep.EngineName, ep.ServerName, ep.Port,
		ep.DatabaseName, ep.Username, ep.Status, ep.SSLMode, ep.AccountID, ep.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetEndpoint(identifier string) (*DMSEndpoint, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, identifier, endpoint_type, engine_name, server_name, port, database_name, username, status, ssl_mode, account_id, created_at
		 FROM dms_endpoints WHERE identifier = ? OR arn = ?`, identifier, identifier)
	return scanEndpoint(row)
}

func (s *Store) ListEndpoints() ([]DMSEndpoint, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, identifier, endpoint_type, engine_name, server_name, port, database_name, username, status, ssl_mode, account_id, created_at
		 FROM dms_endpoints ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DMSEndpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func (s *Store) DeleteEndpoint(identifier string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM dms_endpoints WHERE identifier = ? OR arn = ?`, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointNotFound
	}
	return nil
}

func (s *Store) ModifyEndpoint(identifier, engineName, serverName, databaseName, username string, port int) error {
	res, err := s.store.DB().Exec(
		`UPDATE dms_endpoints SET
		   engine_name = COALESCE(NULLIF(?, ''), engine_name),
		   server_name = COALESCE(NULLIF(?, ''), server_name),
		   database_name = COALESCE(NULLIF(?, ''), database_name),
		   username = COALESCE(NULLIF(?, ''), username),
		   port = CASE WHEN ? > 0 THEN ? ELSE port END
		 WHERE identifier = ? OR arn = ?`,
		engineName, serverName, databaseName, username, port, port, identifier, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointNotFound
	}
	return nil
}

// --- Event Subscriptions ---

func (s *Store) CreateEventSubscription(es *EventSubscription) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO event_subscriptions (name, account_id, sns_topic_arn, source_type, event_categories, enabled, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		es.Name, es.AccountID, es.SnsTopicARN, es.SourceType, es.EventCategories,
		boolToInt(es.Enabled), es.Status, es.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetEventSubscription(name, accountID string) (*EventSubscription, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, account_id, sns_topic_arn, source_type, event_categories, enabled, status, created_at
		 FROM event_subscriptions WHERE name = ? AND account_id = ?`, name, accountID)
	var es EventSubscription
	var createdAt int64
	var enabled int
	err := row.Scan(&es.Name, &es.AccountID, &es.SnsTopicARN, &es.SourceType, &es.EventCategories, &enabled, &es.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEventSubNotFound
		}
		return nil, err
	}
	es.Enabled = enabled != 0
	es.CreatedAt = time.Unix(createdAt, 0)
	return &es, nil
}

func (s *Store) ListEventSubscriptions(accountID string) ([]EventSubscription, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, sns_topic_arn, source_type, event_categories, enabled, status, created_at
		 FROM event_subscriptions WHERE account_id = ? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventSubscription
	for rows.Next() {
		var es EventSubscription
		var createdAt int64
		var enabled int
		if err := rows.Scan(&es.Name, &es.AccountID, &es.SnsTopicARN, &es.SourceType, &es.EventCategories, &enabled, &es.Status, &createdAt); err != nil {
			return nil, err
		}
		es.Enabled = enabled != 0
		es.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, es)
	}
	return out, rows.Err()
}

func (s *Store) DeleteEventSubscription(name, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM event_subscriptions WHERE name = ? AND account_id = ?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEventSubNotFound
	}
	return nil
}

// --- Tags ---

func (s *Store) PutTags(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO dms_tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)
			 ON CONFLICT(resource_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			arn, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetTags(arn string) (map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT tag_key, tag_value FROM dms_tags WHERE resource_arn = ?`, arn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

func (s *Store) DeleteTags(arn string, keys []string) error {
	for _, k := range keys {
		_, err := s.store.DB().Exec(`DELETE FROM dms_tags WHERE resource_arn = ? AND tag_key = ?`, arn, k)
		if err != nil {
			return err
		}
	}
	return nil
}

// --- scanners ---

type instanceScanner interface{ Scan(dest ...any) error }

func scanInstance(sc instanceScanner) (*ReplicationInstance, error) {
	var r ReplicationInstance
	var createdAt int64
	var multiAz, publiclyAccessible int
	err := sc.Scan(&r.ARN, &r.Identifier, &r.InstanceClass, &r.EngineVersion, &r.Status,
		&multiAz, &r.AllocatedStorage, &publiclyAccessible, &r.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationInstanceNotFound
		}
		return nil, err
	}
	r.MultiAZ = multiAz != 0
	r.PubliclyAccessible = publiclyAccessible != 0
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func scanTask(sc instanceScanner) (*ReplicationTask, error) {
	var t ReplicationTask
	var createdAt int64
	err := sc.Scan(&t.ARN, &t.Identifier, &t.InstanceARN, &t.SourceARN, &t.TargetARN,
		&t.MigrationType, &t.TableMappings, &t.TaskSettings, &t.Status, &t.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplicationTaskNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

func scanEndpoint(sc instanceScanner) (*DMSEndpoint, error) {
	var e DMSEndpoint
	var createdAt int64
	err := sc.Scan(&e.ARN, &e.Identifier, &e.EndpointType, &e.EngineName, &e.ServerName,
		&e.Port, &e.DatabaseName, &e.Username, &e.Status, &e.SSLMode, &e.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEndpointNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	return &e, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
