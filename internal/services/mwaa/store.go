// SPDX-License-Identifier: Apache-2.0

// internal/services/mwaa/store.go
package mwaa

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errEnvironmentNotFound = errors.New("environment not found")
	errDagNotFound         = errors.New("dag not found")
	errDagRunNotFound      = errors.New("dag run not found")
	errVariableNotFound    = errors.New("variable not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS environments (
            name              TEXT PRIMARY KEY,
            arn               TEXT NOT NULL UNIQUE,
            status            TEXT NOT NULL DEFAULT 'AVAILABLE',
            airflow_version   TEXT NOT NULL DEFAULT '2.8.1',
            environment_class TEXT NOT NULL DEFAULT 'mw1.small',
            max_workers       INTEGER NOT NULL DEFAULT 10,
            min_workers       INTEGER NOT NULL DEFAULT 1,
            source_bucket     TEXT NOT NULL DEFAULT '',
            dag_s3_path       TEXT NOT NULL DEFAULT 'dags',
            execution_role    TEXT NOT NULL DEFAULT '',
            webserver_url     TEXT NOT NULL DEFAULT '',
            config            TEXT NOT NULL DEFAULT '{}',
            created_at        INTEGER NOT NULL,
            updated_at        INTEGER NOT NULL
        );
    `},
	{Version: 2, SQL: `
        CREATE TABLE IF NOT EXISTS dags (
            id               TEXT PRIMARY KEY,
            environment_name TEXT NOT NULL,
            dag_id           TEXT NOT NULL,
            file_uri         TEXT NOT NULL DEFAULT '',
            status           TEXT NOT NULL DEFAULT 'active',
            paused           INTEGER NOT NULL DEFAULT 0,
            account_id       TEXT NOT NULL,
            created_at       INTEGER NOT NULL,
            UNIQUE(environment_name, dag_id)
        );
        CREATE TABLE IF NOT EXISTS dag_runs (
            id               TEXT PRIMARY KEY,
            environment_name TEXT NOT NULL,
            dag_id           TEXT NOT NULL,
            run_id           TEXT NOT NULL,
            state            TEXT NOT NULL DEFAULT 'queued',
            execution_date   TEXT NOT NULL,
            conf             TEXT NOT NULL DEFAULT '{}',
            account_id       TEXT NOT NULL,
            created_at       INTEGER NOT NULL,
            UNIQUE(environment_name, dag_id, run_id)
        );
        CREATE TABLE IF NOT EXISTS variables (
            environment_name TEXT NOT NULL,
            key              TEXT NOT NULL,
            value            TEXT NOT NULL DEFAULT '',
            is_encrypted     INTEGER NOT NULL DEFAULT 0,
            description      TEXT NOT NULL DEFAULT '',
            created_at       INTEGER NOT NULL,
            PRIMARY KEY (environment_name, key)
        );
    `},
}

type Environment struct {
	Name             string
	ARN              string
	Status           string
	AirflowVersion   string
	EnvironmentClass string
	MaxWorkers       int
	MinWorkers       int
	SourceBucket     string
	DagS3Path        string
	ExecutionRole    string
	WebserverURL     string
	Config           string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type DAG struct {
	ID              string
	EnvironmentName string
	DagID           string
	FileURI         string
	Status          string
	Paused          bool
	CreatedAt       time.Time
}

type DagRun struct {
	ID              string
	EnvironmentName string
	DagID           string
	RunID           string
	State           string
	ExecutionDate   string
	Conf            string
	CreatedAt       time.Time
}

type Variable struct {
	EnvironmentName string
	Key             string
	Value           string
	IsEncrypted     bool
	Description     string
	CreatedAt       time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append([]sqlite.Migration{}, migrations...)
	allMigrations = append(allMigrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "mwaa.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateEnvironment(e *Environment) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO environments
            (name, arn, status, airflow_version, environment_class, max_workers, min_workers,
             source_bucket, dag_s3_path, execution_role, webserver_url, config, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Name, e.ARN, e.Status, e.AirflowVersion, e.EnvironmentClass, e.MaxWorkers, e.MinWorkers,
		e.SourceBucket, e.DagS3Path, e.ExecutionRole, e.WebserverURL, e.Config, now, now,
	)
	return err
}

func (s *Store) GetEnvironment(name string) (*Environment, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, airflow_version, environment_class, max_workers, min_workers,
                source_bucket, dag_s3_path, execution_role, webserver_url, config, created_at, updated_at
         FROM environments WHERE name = ?`, name)
	return scanEnvironment(row)
}

func (s *Store) ListEnvironments() ([]Environment, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, airflow_version, environment_class, max_workers, min_workers,
                source_bucket, dag_s3_path, execution_role, webserver_url, config, created_at, updated_at
         FROM environments ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var envs []Environment
	for rows.Next() {
		e, err := scanEnvironment(rows)
		if err != nil {
			return nil, err
		}
		envs = append(envs, *e)
	}
	return envs, rows.Err()
}

func (s *Store) UpdateEnvironment(name string, fields map[string]any) error {
	e, err := s.GetEnvironment(name)
	if err != nil {
		return errEnvironmentNotFound
	}
	if v, ok := fields["AirflowVersion"].(string); ok && v != "" {
		e.AirflowVersion = v
	}
	if v, ok := fields["EnvironmentClass"].(string); ok && v != "" {
		e.EnvironmentClass = v
	}
	if v, ok := fields["MaxWorkers"].(float64); ok {
		e.MaxWorkers = int(v)
	}
	if v, ok := fields["MinWorkers"].(float64); ok {
		e.MinWorkers = int(v)
	}
	if v, ok := fields["SourceBucketArn"].(string); ok && v != "" {
		e.SourceBucket = v
	}
	if v, ok := fields["DagS3Path"].(string); ok && v != "" {
		e.DagS3Path = v
	}
	if v, ok := fields["ExecutionRoleArn"].(string); ok && v != "" {
		e.ExecutionRole = v
	}
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE environments SET
            airflow_version=?, environment_class=?, max_workers=?, min_workers=?,
            source_bucket=?, dag_s3_path=?, execution_role=?, updated_at=?
         WHERE name=?`,
		e.AirflowVersion, e.EnvironmentClass, e.MaxWorkers, e.MinWorkers,
		e.SourceBucket, e.DagS3Path, e.ExecutionRole, now, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEnvironmentNotFound
	}
	return nil
}

func (s *Store) DeleteEnvironment(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM environments WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEnvironmentNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanEnvironment(sc scanner) (*Environment, error) {
	var e Environment
	var createdAt, updatedAt int64
	err := sc.Scan(
		&e.Name, &e.ARN, &e.Status, &e.AirflowVersion, &e.EnvironmentClass,
		&e.MaxWorkers, &e.MinWorkers, &e.SourceBucket, &e.DagS3Path,
		&e.ExecutionRole, &e.WebserverURL, &e.Config, &createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEnvironmentNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

// DAG operations

func (s *Store) CreateDag(d *DAG) error {
	if d.ID == "" {
		d.ID = shared.GenerateUUID()
	}
	now := time.Now().Unix()
	pausedInt := 0
	if d.Paused {
		pausedInt = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO dags (id, environment_name, dag_id, file_uri, status, paused, account_id, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.EnvironmentName, d.DagID, d.FileURI, d.Status, pausedInt, shared.DefaultAccountID, now,
	)
	if err != nil {
		return err
	}
	d.CreatedAt = time.Unix(now, 0)
	return nil
}

func (s *Store) GetDag(env, dagID string) (*DAG, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, environment_name, dag_id, file_uri, status, paused, created_at FROM dags WHERE environment_name = ? AND dag_id = ?`,
		env, dagID,
	)
	return scanDag(row)
}

func (s *Store) ListDags(env string) ([]DAG, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, environment_name, dag_id, file_uri, status, paused, created_at FROM dags WHERE environment_name = ? ORDER BY dag_id`,
		env,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var dags []DAG
	for rows.Next() {
		d, err := scanDag(rows)
		if err != nil {
			return nil, err
		}
		dags = append(dags, *d)
	}
	return dags, rows.Err()
}

func (s *Store) DeleteDag(env, dagID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM dags WHERE environment_name = ? AND dag_id = ?`, env, dagID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDagNotFound
	}
	return nil
}

func (s *Store) PauseDag(env, dagID string, paused bool) error {
	pausedInt := 0
	if paused {
		pausedInt = 1
	}
	res, err := s.store.DB().Exec(
		`UPDATE dags SET paused = ? WHERE environment_name = ? AND dag_id = ?`,
		pausedInt, env, dagID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDagNotFound
	}
	return nil
}

func scanDag(sc scanner) (*DAG, error) {
	var d DAG
	var paused int
	var createdAt int64
	err := sc.Scan(&d.ID, &d.EnvironmentName, &d.DagID, &d.FileURI, &d.Status, &paused, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDagNotFound
		}
		return nil, err
	}
	d.Paused = paused != 0
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

// DAG Run operations

func (s *Store) CreateDagRun(r *DagRun) error {
	if r.ID == "" {
		r.ID = shared.GenerateUUID()
	}
	if r.RunID == "" {
		r.RunID = "manual__" + time.Now().UTC().Format("2006-01-02T15:04:05") + "+00:00"
	}
	if r.ExecutionDate == "" {
		r.ExecutionDate = time.Now().UTC().Format("2006-01-02T15:04:05Z")
	}
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO dag_runs (id, environment_name, dag_id, run_id, state, execution_date, conf, account_id, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.EnvironmentName, r.DagID, r.RunID, r.State, r.ExecutionDate, r.Conf, shared.DefaultAccountID, now,
	)
	if err != nil {
		return err
	}
	r.CreatedAt = time.Unix(now, 0)
	return nil
}

func (s *Store) GetDagRun(env, dagID, runID string) (*DagRun, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, environment_name, dag_id, run_id, state, execution_date, conf, created_at FROM dag_runs WHERE environment_name = ? AND dag_id = ? AND run_id = ?`,
		env, dagID, runID,
	)
	return scanDagRun(row)
}

func (s *Store) ListDagRuns(env, dagID string) ([]DagRun, error) {
	query := `SELECT id, environment_name, dag_id, run_id, state, execution_date, conf, created_at FROM dag_runs WHERE environment_name = ?`
	args := []any{env}
	if dagID != "" {
		query += ` AND dag_id = ?`
		args = append(args, dagID)
	}
	query += ` ORDER BY created_at DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var runs []DagRun
	for rows.Next() {
		r, err := scanDagRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *r)
	}
	return runs, rows.Err()
}

func (s *Store) DeleteDagRun(env, dagID, runID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM dag_runs WHERE environment_name = ? AND dag_id = ? AND run_id = ?`,
		env, dagID, runID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDagRunNotFound
	}
	return nil
}

func scanDagRun(sc scanner) (*DagRun, error) {
	var r DagRun
	var createdAt int64
	err := sc.Scan(&r.ID, &r.EnvironmentName, &r.DagID, &r.RunID, &r.State, &r.ExecutionDate, &r.Conf, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDagRunNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

// Variable operations

func (s *Store) SetVariable(v *Variable) error {
	now := time.Now().Unix()
	encInt := 0
	if v.IsEncrypted {
		encInt = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO variables (environment_name, key, value, is_encrypted, description, created_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(environment_name, key) DO UPDATE SET
           value = excluded.value,
           is_encrypted = excluded.is_encrypted,
           description = excluded.description`,
		v.EnvironmentName, v.Key, v.Value, encInt, v.Description, now,
	)
	return err
}

func (s *Store) GetVariable(env, key string) (*Variable, error) {
	row := s.store.DB().QueryRow(
		`SELECT environment_name, key, value, is_encrypted, description, created_at FROM variables WHERE environment_name = ? AND key = ?`,
		env, key,
	)
	var v Variable
	var enc int
	var createdAt int64
	err := row.Scan(&v.EnvironmentName, &v.Key, &v.Value, &enc, &v.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errVariableNotFound
		}
		return nil, err
	}
	v.IsEncrypted = enc != 0
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

func (s *Store) ListVariables(env string) ([]Variable, error) {
	rows, err := s.store.DB().Query(
		`SELECT environment_name, key, value, is_encrypted, description, created_at FROM variables WHERE environment_name = ? ORDER BY key`,
		env,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var list []Variable
	for rows.Next() {
		var v Variable
		var enc int
		var createdAt int64
		if err := rows.Scan(&v.EnvironmentName, &v.Key, &v.Value, &enc, &v.Description, &createdAt); err != nil {
			return nil, err
		}
		v.IsEncrypted = enc != 0
		v.CreatedAt = time.Unix(createdAt, 0)
		list = append(list, v)
	}
	return list, rows.Err()
}

func (s *Store) DeleteVariable(env, key string) error {
	res, err := s.store.DB().Exec(`DELETE FROM variables WHERE environment_name = ? AND key = ?`, env, key)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errVariableNotFound
	}
	return nil
}
