// SPDX-License-Identifier: Apache-2.0

// internal/services/swf/store.go
package swf

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errDomainNotFound       = errors.New("domain not found")
	errWorkflowTypeNotFound = errors.New("workflow type not found")
	errActivityTypeNotFound = errors.New("activity type not found")
	errExecutionNotFound    = errors.New("workflow execution not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS domains (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'REGISTERED',
			description     TEXT NOT NULL DEFAULT '',
			retention       TEXT NOT NULL DEFAULT '30',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS workflow_types (
			domain          TEXT NOT NULL,
			name            TEXT NOT NULL,
			version         TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'REGISTERED',
			description     TEXT NOT NULL DEFAULT '',
			default_timeout TEXT NOT NULL DEFAULT 'NONE',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (domain, name, version)
		);
		CREATE TABLE IF NOT EXISTS activity_types (
			domain          TEXT NOT NULL,
			name            TEXT NOT NULL,
			version         TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'REGISTERED',
			description     TEXT NOT NULL DEFAULT '',
			default_timeout TEXT NOT NULL DEFAULT 'NONE',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (domain, name, version)
		);
		CREATE TABLE IF NOT EXISTS workflow_executions (
			domain          TEXT NOT NULL,
			workflow_id     TEXT NOT NULL,
			run_id          TEXT NOT NULL,
			workflow_name   TEXT NOT NULL DEFAULT '',
			workflow_version TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'OPEN',
			input           TEXT NOT NULL DEFAULT '',
			tag_list        TEXT NOT NULL DEFAULT '[]',
			start_time      INTEGER NOT NULL,
			close_time      INTEGER NOT NULL DEFAULT 0,
			close_status    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (domain, workflow_id, run_id)
		);
	`},
}

// Domain represents an SWF domain.
type Domain struct {
	Name        string
	ARN         string
	Status      string
	Description string
	Retention   string
	CreatedAt   time.Time
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Domain         string
	Name           string
	Version        string
	Status         string
	Description    string
	DefaultTimeout string
	CreatedAt      time.Time
}

// ActivityType represents an SWF activity type.
type ActivityType struct {
	Domain         string
	Name           string
	Version        string
	Status         string
	Description    string
	DefaultTimeout string
	CreatedAt      time.Time
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	Domain          string
	WorkflowID      string
	RunID           string
	WorkflowName    string
	WorkflowVersion string
	Status          string
	Input           string
	TagList         string
	StartTime       time.Time
	CloseTime       time.Time
	CloseStatus     string
}

// Store manages the SQLite state for the SWF service.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the SQLite DB for SWF.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "swf.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Domain ----

func (s *Store) CreateDomain(d *Domain) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (name, arn, status, description, retention, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		d.Name, d.ARN, d.Status, d.Description, d.Retention, d.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetDomain(name string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, description, retention, created_at FROM domains WHERE name = ?`, name)
	return scanDomain(row)
}

func (s *Store) ListDomains(statusFilter string) ([]Domain, error) {
	query := `SELECT name, arn, status, description, retention, created_at FROM domains`
	var args []any
	if statusFilter != "" {
		query += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var domains []Domain
	for rows.Next() {
		d, err := scanDomain(rows)
		if err != nil {
			return nil, err
		}
		domains = append(domains, *d)
	}
	return domains, rows.Err()
}

func (s *Store) UpdateDomainStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE domains SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNotFound
	}
	return nil
}

// ---- WorkflowType ----

func (s *Store) CreateWorkflowType(wt *WorkflowType) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO workflow_types (domain, name, version, status, description, default_timeout, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		wt.Domain, wt.Name, wt.Version, wt.Status, wt.Description, wt.DefaultTimeout, wt.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetWorkflowType(domain, name, version string) (*WorkflowType, error) {
	row := s.store.DB().QueryRow(
		`SELECT domain, name, version, status, description, default_timeout, created_at
		 FROM workflow_types WHERE domain = ? AND name = ? AND version = ?`,
		domain, name, version)
	return scanWorkflowType(row)
}

func (s *Store) ListWorkflowTypes(domain, statusFilter string) ([]WorkflowType, error) {
	query := `SELECT domain, name, version, status, description, default_timeout, created_at
		FROM workflow_types WHERE domain = ?`
	args := []any{domain}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var wts []WorkflowType
	for rows.Next() {
		wt, err := scanWorkflowType(rows)
		if err != nil {
			return nil, err
		}
		wts = append(wts, *wt)
	}
	return wts, rows.Err()
}

func (s *Store) UpdateWorkflowTypeStatus(domain, name, version, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE workflow_types SET status = ? WHERE domain = ? AND name = ? AND version = ?`,
		status, domain, name, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWorkflowTypeNotFound
	}
	return nil
}

func (s *Store) DeleteWorkflowType(domain, name, version string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM workflow_types WHERE domain = ? AND name = ? AND version = ?`,
		domain, name, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWorkflowTypeNotFound
	}
	return nil
}

// ---- ActivityType ----

func (s *Store) CreateActivityType(at *ActivityType) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO activity_types (domain, name, version, status, description, default_timeout, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		at.Domain, at.Name, at.Version, at.Status, at.Description, at.DefaultTimeout, at.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetActivityType(domain, name, version string) (*ActivityType, error) {
	row := s.store.DB().QueryRow(
		`SELECT domain, name, version, status, description, default_timeout, created_at
		 FROM activity_types WHERE domain = ? AND name = ? AND version = ?`,
		domain, name, version)
	return scanActivityType(row)
}

func (s *Store) ListActivityTypes(domain, statusFilter string) ([]ActivityType, error) {
	query := `SELECT domain, name, version, status, description, default_timeout, created_at
		FROM activity_types WHERE domain = ?`
	args := []any{domain}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ats []ActivityType
	for rows.Next() {
		at, err := scanActivityType(rows)
		if err != nil {
			return nil, err
		}
		ats = append(ats, *at)
	}
	return ats, rows.Err()
}

func (s *Store) UpdateActivityTypeStatus(domain, name, version, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE activity_types SET status = ? WHERE domain = ? AND name = ? AND version = ?`,
		status, domain, name, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errActivityTypeNotFound
	}
	return nil
}

func (s *Store) DeleteActivityType(domain, name, version string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM activity_types WHERE domain = ? AND name = ? AND version = ?`,
		domain, name, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errActivityTypeNotFound
	}
	return nil
}

// ---- WorkflowExecution ----

func (s *Store) CreateWorkflowExecution(we *WorkflowExecution) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO workflow_executions
		 (domain, workflow_id, run_id, workflow_name, workflow_version, status, input, tag_list, start_time, close_time, close_status)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		we.Domain, we.WorkflowID, we.RunID, we.WorkflowName, we.WorkflowVersion,
		we.Status, we.Input, we.TagList, we.StartTime.Unix(), we.CloseTime.Unix(), we.CloseStatus,
	)
	return err
}

func (s *Store) GetWorkflowExecution(domain, workflowID, runID string) (*WorkflowExecution, error) {
	row := s.store.DB().QueryRow(
		`SELECT domain, workflow_id, run_id, workflow_name, workflow_version, status, input, tag_list, start_time, close_time, close_status
		 FROM workflow_executions WHERE domain = ? AND workflow_id = ? AND run_id = ?`,
		domain, workflowID, runID)
	return scanWorkflowExecution(row)
}

func (s *Store) GetLatestWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error) {
	row := s.store.DB().QueryRow(
		`SELECT domain, workflow_id, run_id, workflow_name, workflow_version, status, input, tag_list, start_time, close_time, close_status
		 FROM workflow_executions WHERE domain = ? AND workflow_id = ? ORDER BY start_time DESC LIMIT 1`,
		domain, workflowID)
	return scanWorkflowExecution(row)
}

func (s *Store) ListWorkflowExecutions(domain, statusFilter string) ([]WorkflowExecution, error) {
	query := `SELECT domain, workflow_id, run_id, workflow_name, workflow_version, status, input, tag_list, start_time, close_time, close_status
		FROM workflow_executions WHERE domain = ?`
	args := []any{domain}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY start_time DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var execs []WorkflowExecution
	for rows.Next() {
		we, err := scanWorkflowExecution(rows)
		if err != nil {
			return nil, err
		}
		execs = append(execs, *we)
	}
	return execs, rows.Err()
}

func (s *Store) CloseWorkflowExecution(domain, workflowID, runID, closeStatus string, closeTime time.Time) error {
	res, err := s.store.DB().Exec(
		`UPDATE workflow_executions SET status = 'CLOSED', close_status = ?, close_time = ?
		 WHERE domain = ? AND workflow_id = ? AND run_id = ?`,
		closeStatus, closeTime.Unix(), domain, workflowID, runID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExecutionNotFound
	}
	return nil
}

func (s *Store) CountWorkflowExecutions(domain, statusFilter string) (int, error) {
	query := `SELECT COUNT(*) FROM workflow_executions WHERE domain = ?`
	args := []any{domain}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	var count int
	err := s.store.DB().QueryRow(query, args...).Scan(&count)
	return count, err
}

// ---- Scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanDomain(s scanner) (*Domain, error) {
	var d Domain
	var createdAt int64
	err := s.Scan(&d.Name, &d.ARN, &d.Status, &d.Description, &d.Retention, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanWorkflowType(s scanner) (*WorkflowType, error) {
	var wt WorkflowType
	var createdAt int64
	err := s.Scan(&wt.Domain, &wt.Name, &wt.Version, &wt.Status, &wt.Description, &wt.DefaultTimeout, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errWorkflowTypeNotFound
		}
		return nil, err
	}
	wt.CreatedAt = time.Unix(createdAt, 0)
	return &wt, nil
}

func scanActivityType(s scanner) (*ActivityType, error) {
	var at ActivityType
	var createdAt int64
	err := s.Scan(&at.Domain, &at.Name, &at.Version, &at.Status, &at.Description, &at.DefaultTimeout, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errActivityTypeNotFound
		}
		return nil, err
	}
	at.CreatedAt = time.Unix(createdAt, 0)
	return &at, nil
}

func scanWorkflowExecution(s scanner) (*WorkflowExecution, error) {
	var we WorkflowExecution
	var startTime, closeTime int64
	err := s.Scan(&we.Domain, &we.WorkflowID, &we.RunID, &we.WorkflowName, &we.WorkflowVersion,
		&we.Status, &we.Input, &we.TagList, &startTime, &closeTime, &we.CloseStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExecutionNotFound
		}
		return nil, err
	}
	we.StartTime = time.Unix(startTime, 0)
	if closeTime > 0 {
		we.CloseTime = time.Unix(closeTime, 0)
	}
	return &we, nil
}
