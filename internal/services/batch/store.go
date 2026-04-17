// SPDX-License-Identifier: Apache-2.0

// internal/services/batch/store.go
package batch

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS compute_environments (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL UNIQUE,
			type             TEXT NOT NULL DEFAULT 'MANAGED',
			state            TEXT NOT NULL DEFAULT 'ENABLED',
			status           TEXT NOT NULL DEFAULT 'VALID',
			service_role     TEXT NOT NULL DEFAULT '',
			compute_resources TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS job_queues (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL UNIQUE,
			state            TEXT NOT NULL DEFAULT 'ENABLED',
			status           TEXT NOT NULL DEFAULT 'VALID',
			priority         INTEGER NOT NULL DEFAULT 0,
			compute_envs     TEXT NOT NULL DEFAULT '[]',
			scheduling_policy TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS job_definitions (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL,
			revision         INTEGER NOT NULL DEFAULT 1,
			type             TEXT NOT NULL DEFAULT 'container',
			status           TEXT NOT NULL DEFAULT 'ACTIVE',
			container_props  TEXT NOT NULL DEFAULT '{}',
			parameters       TEXT NOT NULL DEFAULT '{}',
			timeout          TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS jobs (
			id               TEXT PRIMARY KEY,
			arn              TEXT NOT NULL UNIQUE,
			name             TEXT NOT NULL,
			queue            TEXT NOT NULL,
			definition       TEXT NOT NULL,
			status           TEXT NOT NULL DEFAULT 'SUCCEEDED',
			status_reason    TEXT NOT NULL DEFAULT '',
			parameters       TEXT NOT NULL DEFAULT '{}',
			container        TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL,
			started_at       INTEGER NOT NULL DEFAULT 0,
			stopped_at       INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS scheduling_policies (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL UNIQUE,
			fairshare        TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL
		);
	`},
}

// --- Model types ---

type ComputeEnvironment struct {
	ARN              string
	Name             string
	Type             string
	State            string
	Status           string
	ServiceRole      string
	ComputeResources string
	CreatedAt        int64
	UpdatedAt        int64
}

type JobQueue struct {
	ARN              string
	Name             string
	State            string
	Status           string
	Priority         int32
	ComputeEnvs      string
	SchedulingPolicy string
	CreatedAt        int64
	UpdatedAt        int64
}

type JobDefinition struct {
	ARN            string
	Name           string
	Revision       int32
	Type           string
	Status         string
	ContainerProps string
	Parameters     string
	Timeout        string
	CreatedAt      int64
}

type Job struct {
	ID           string
	ARN          string
	Name         string
	Queue        string
	Definition   string
	Status       string
	StatusReason string
	Parameters   string
	Container    string
	CreatedAt    int64
	StartedAt    int64
	StoppedAt    int64
}

type SchedulingPolicy struct {
	ARN       string
	Name      string
	Fairshare string
	CreatedAt int64
	UpdatedAt int64
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "batch.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- ComputeEnvironment ---

func (s *Store) CreateComputeEnvironment(ce *ComputeEnvironment) error {
	now := time.Now().Unix()
	ce.CreatedAt = now
	ce.UpdatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO compute_environments (arn, name, type, state, status, service_role, compute_resources, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ce.ARN, ce.Name, ce.Type, ce.State, ce.Status, ce.ServiceRole, ce.ComputeResources, now, now,
	)
	return err
}

func (s *Store) GetComputeEnvironment(nameOrARN string) (*ComputeEnvironment, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, type, state, status, service_role, compute_resources, created_at, updated_at
		 FROM compute_environments WHERE name = ? OR arn = ?`, nameOrARN, nameOrARN)
	return scanComputeEnvironment(row)
}

func (s *Store) ListComputeEnvironments() ([]*ComputeEnvironment, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, type, state, status, service_role, compute_resources, created_at, updated_at
		 FROM compute_environments ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*ComputeEnvironment
	for rows.Next() {
		ce, err := scanComputeEnvironment(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, ce)
	}
	return result, rows.Err()
}

func (s *Store) UpdateComputeEnvironment(nameOrARN string, state, serviceRole string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE compute_environments SET state=?, service_role=?, updated_at=? WHERE name=? OR arn=?`,
		state, serviceRole, now, nameOrARN, nameOrARN,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteComputeEnvironment(nameOrARN string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM compute_environments WHERE name=? OR arn=?`, nameOrARN, nameOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanComputeEnvironment(sc scanner) (*ComputeEnvironment, error) {
	var ce ComputeEnvironment
	err := sc.Scan(&ce.ARN, &ce.Name, &ce.Type, &ce.State, &ce.Status, &ce.ServiceRole, &ce.ComputeResources, &ce.CreatedAt, &ce.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &ce, nil
}

// --- JobQueue ---

func (s *Store) CreateJobQueue(jq *JobQueue) error {
	now := time.Now().Unix()
	jq.CreatedAt = now
	jq.UpdatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO job_queues (arn, name, state, status, priority, compute_envs, scheduling_policy, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jq.ARN, jq.Name, jq.State, jq.Status, jq.Priority, jq.ComputeEnvs, jq.SchedulingPolicy, now, now,
	)
	return err
}

func (s *Store) GetJobQueue(nameOrARN string) (*JobQueue, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, state, status, priority, compute_envs, scheduling_policy, created_at, updated_at
		 FROM job_queues WHERE name=? OR arn=?`, nameOrARN, nameOrARN)
	return scanJobQueue(row)
}

func (s *Store) ListJobQueues() ([]*JobQueue, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, state, status, priority, compute_envs, scheduling_policy, created_at, updated_at
		 FROM job_queues ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*JobQueue
	for rows.Next() {
		jq, err := scanJobQueue(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, jq)
	}
	return result, rows.Err()
}

func (s *Store) UpdateJobQueue(nameOrARN string, state string, priority int32, schedulingPolicy string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE job_queues SET state=?, priority=?, scheduling_policy=?, updated_at=? WHERE name=? OR arn=?`,
		state, priority, schedulingPolicy, now, nameOrARN, nameOrARN,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteJobQueue(nameOrARN string) error {
	res, err := s.store.DB().Exec(`DELETE FROM job_queues WHERE name=? OR arn=?`, nameOrARN, nameOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanJobQueue(sc scanner) (*JobQueue, error) {
	var jq JobQueue
	err := sc.Scan(&jq.ARN, &jq.Name, &jq.State, &jq.Status, &jq.Priority, &jq.ComputeEnvs, &jq.SchedulingPolicy, &jq.CreatedAt, &jq.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &jq, nil
}

// --- JobDefinition ---

func (s *Store) RegisterJobDefinition(jd *JobDefinition) error {
	// Find max revision for this name
	var maxRev int32
	row := s.store.DB().QueryRow(`SELECT COALESCE(MAX(revision), 0) FROM job_definitions WHERE name=?`, jd.Name)
	row.Scan(&maxRev)
	jd.Revision = maxRev + 1

	now := time.Now().Unix()
	jd.CreatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO job_definitions (arn, name, revision, type, status, container_props, parameters, timeout, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jd.ARN, jd.Name, jd.Revision, jd.Type, jd.Status, jd.ContainerProps, jd.Parameters, jd.Timeout, now,
	)
	return err
}

func (s *Store) UpdateJobDefinitionARN(name string, revision int32, arn string) error {
	_, err := s.store.DB().Exec(`UPDATE job_definitions SET arn=? WHERE name=? AND revision=?`, arn, name, revision)
	return err
}

func (s *Store) GetJobDefinition(nameOrARN string) (*JobDefinition, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, revision, type, status, container_props, parameters, timeout, created_at
		 FROM job_definitions WHERE arn=? OR (name=? AND status='ACTIVE') ORDER BY revision DESC LIMIT 1`,
		nameOrARN, nameOrARN)
	return scanJobDefinition(row)
}

func (s *Store) ListJobDefinitions(name, status string) ([]*JobDefinition, error) {
	query := `SELECT arn, name, revision, type, status, container_props, parameters, timeout, created_at FROM job_definitions WHERE 1=1`
	args := []any{}
	if name != "" {
		query += " AND name=?"
		args = append(args, name)
	}
	if status != "" {
		query += " AND status=?"
		args = append(args, status)
	}
	query += " ORDER BY name, revision"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*JobDefinition
	for rows.Next() {
		jd, err := scanJobDefinition(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, jd)
	}
	return result, rows.Err()
}

func (s *Store) DeregisterJobDefinition(nameOrARN string) error {
	res, err := s.store.DB().Exec(
		`UPDATE job_definitions SET status='INACTIVE' WHERE arn=? OR name=?`,
		nameOrARN, nameOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanJobDefinition(sc scanner) (*JobDefinition, error) {
	var jd JobDefinition
	err := sc.Scan(&jd.ARN, &jd.Name, &jd.Revision, &jd.Type, &jd.Status, &jd.ContainerProps, &jd.Parameters, &jd.Timeout, &jd.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &jd, nil
}

// --- Job ---

func (s *Store) CreateJob(j *Job) error {
	now := time.Now().Unix()
	j.CreatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO jobs (id, arn, name, queue, definition, status, status_reason, parameters, container, created_at, started_at, stopped_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.ARN, j.Name, j.Queue, j.Definition, j.Status, j.StatusReason,
		j.Parameters, j.Container, now, j.StartedAt, j.StoppedAt,
	)
	return err
}

func (s *Store) GetJob(idOrARN string) (*Job, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, queue, definition, status, status_reason, parameters, container, created_at, started_at, stopped_at
		 FROM jobs WHERE id=? OR arn=?`, idOrARN, idOrARN)
	return scanJob(row)
}

func (s *Store) ListJobs(queue, status string) ([]*Job, error) {
	query := `SELECT id, arn, name, queue, definition, status, status_reason, parameters, container, created_at, started_at, stopped_at FROM jobs WHERE 1=1`
	args := []any{}
	if queue != "" {
		query += " AND queue=?"
		args = append(args, queue)
	}
	if status != "" {
		query += " AND status=?"
		args = append(args, status)
	}
	query += " ORDER BY created_at"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

func (s *Store) UpdateJobStatus(idOrARN, status, reason string) error {
	res, err := s.store.DB().Exec(
		`UPDATE jobs SET status=?, status_reason=? WHERE id=? OR arn=?`,
		status, reason, idOrARN, idOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanJob(sc scanner) (*Job, error) {
	var j Job
	err := sc.Scan(&j.ID, &j.ARN, &j.Name, &j.Queue, &j.Definition, &j.Status, &j.StatusReason,
		&j.Parameters, &j.Container, &j.CreatedAt, &j.StartedAt, &j.StoppedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &j, nil
}

// --- SchedulingPolicy ---

func (s *Store) CreateSchedulingPolicy(sp *SchedulingPolicy) error {
	now := time.Now().Unix()
	sp.CreatedAt = now
	sp.UpdatedAt = now
	_, err := s.store.DB().Exec(
		`INSERT INTO scheduling_policies (arn, name, fairshare, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		sp.ARN, sp.Name, sp.Fairshare, now, now,
	)
	return err
}

func (s *Store) GetSchedulingPolicy(nameOrARN string) (*SchedulingPolicy, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, fairshare, created_at, updated_at FROM scheduling_policies WHERE name=? OR arn=?`,
		nameOrARN, nameOrARN)
	return scanSchedulingPolicy(row)
}

func (s *Store) ListSchedulingPolicies() ([]*SchedulingPolicy, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, fairshare, created_at, updated_at FROM scheduling_policies ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*SchedulingPolicy
	for rows.Next() {
		sp, err := scanSchedulingPolicy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, sp)
	}
	return result, rows.Err()
}

func (s *Store) UpdateSchedulingPolicy(nameOrARN, fairshare string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE scheduling_policies SET fairshare=?, updated_at=? WHERE name=? OR arn=?`,
		fairshare, now, nameOrARN, nameOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteSchedulingPolicy(nameOrARN string) error {
	res, err := s.store.DB().Exec(`DELETE FROM scheduling_policies WHERE name=? OR arn=?`, nameOrARN, nameOrARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanSchedulingPolicy(sc scanner) (*SchedulingPolicy, error) {
	var sp SchedulingPolicy
	err := sc.Scan(&sp.ARN, &sp.Name, &sp.Fairshare, &sp.CreatedAt, &sp.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &sp, nil
}

// --- Helpers ---

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
