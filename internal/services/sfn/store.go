// SPDX-License-Identifier: Apache-2.0

// internal/services/sfn/store.go
package sfn

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errStateMachineNotFound = errors.New("state machine not found")
	errExecutionNotFound    = errors.New("execution not found")
	errActivityNotFound     = errors.New("activity not found")
	errAliasNotFound        = errors.New("alias not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS state_machines (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL UNIQUE,
			definition      TEXT NOT NULL DEFAULT '{}',
			role_arn        TEXT NOT NULL DEFAULT '',
			type            TEXT NOT NULL DEFAULT 'STANDARD',
			status          TEXT NOT NULL DEFAULT 'ACTIVE',
			revision_id     TEXT NOT NULL DEFAULT '',
			version         INTEGER NOT NULL DEFAULT 1,
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS executions (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			state_machine   TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'SUCCEEDED',
			input           TEXT NOT NULL DEFAULT '{}',
			output          TEXT NOT NULL DEFAULT '{}',
			start_date      INTEGER NOT NULL,
			stop_date       INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS activities (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL UNIQUE,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS state_machine_aliases (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			state_machine   TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			routing_config  TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL,
			UNIQUE(name, state_machine)
		);
	`},
}

// StateMachine represents a Step Functions state machine.
type StateMachine struct {
	ARN        string
	Name       string
	Definition string
	RoleARN    string
	Type       string
	Status     string
	RevisionID string
	Version    int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Execution represents a Step Functions execution.
type Execution struct {
	ARN          string
	Name         string
	StateMachine string
	Status       string
	Input        string
	Output       string
	StartDate    time.Time
	StopDate     time.Time
}

// Activity represents a Step Functions activity.
type Activity struct {
	ARN       string
	Name      string
	CreatedAt time.Time
}

// StateMachineAlias represents a Step Functions state machine alias.
type StateMachineAlias struct {
	ARN           string
	Name          string
	StateMachine  string
	Description   string
	RoutingConfig string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Store manages the SQLite state for the SFN service.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the SQLite DB for SFN.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "sfn.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- State Machine ----

func (s *Store) CreateStateMachine(sm *StateMachine) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO state_machines (arn, name, definition, role_arn, type, status, revision_id, version, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sm.ARN, sm.Name, sm.Definition, sm.RoleARN, sm.Type, sm.Status,
		sm.RevisionID, sm.Version, sm.CreatedAt.Unix(), sm.UpdatedAt.Unix(),
	)
	return err
}

func (s *Store) GetStateMachineByARN(arn string) (*StateMachine, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, definition, role_arn, type, status, revision_id, version, created_at, updated_at
		 FROM state_machines WHERE arn = ?`, arn)
	return scanStateMachine(row)
}

func (s *Store) GetStateMachineByName(name string) (*StateMachine, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, definition, role_arn, type, status, revision_id, version, created_at, updated_at
		 FROM state_machines WHERE name = ?`, name)
	return scanStateMachine(row)
}

func (s *Store) ListStateMachines() ([]StateMachine, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, definition, role_arn, type, status, revision_id, version, created_at, updated_at
		 FROM state_machines ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sms []StateMachine
	for rows.Next() {
		sm, err := scanStateMachine(rows)
		if err != nil {
			return nil, err
		}
		sms = append(sms, *sm)
	}
	return sms, rows.Err()
}

func (s *Store) UpdateStateMachine(arn, definition, roleARN, revisionID string, version int, updatedAt time.Time) error {
	res, err := s.store.DB().Exec(
		`UPDATE state_machines SET definition = ?, role_arn = ?, revision_id = ?, version = ?, updated_at = ?
		 WHERE arn = ?`,
		definition, roleARN, revisionID, version, updatedAt.Unix(), arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStateMachineNotFound
	}
	return nil
}

func (s *Store) DeleteStateMachine(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM state_machines WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStateMachineNotFound
	}
	return nil
}

// ---- Execution ----

func (s *Store) CreateExecution(ex *Execution) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO executions (arn, name, state_machine, status, input, output, start_date, stop_date)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ex.ARN, ex.Name, ex.StateMachine, ex.Status, ex.Input, ex.Output,
		ex.StartDate.Unix(), ex.StopDate.Unix(),
	)
	return err
}

func (s *Store) GetExecution(arn string) (*Execution, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, state_machine, status, input, output, start_date, stop_date
		 FROM executions WHERE arn = ?`, arn)
	return scanExecution(row)
}

func (s *Store) ListExecutions(stateMachineARN, statusFilter string) ([]Execution, error) {
	query := `SELECT arn, name, state_machine, status, input, output, start_date, stop_date
		FROM executions WHERE state_machine = ?`
	args := []any{stateMachineARN}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY start_date DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var execs []Execution
	for rows.Next() {
		ex, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		execs = append(execs, *ex)
	}
	return execs, rows.Err()
}

func (s *Store) UpdateExecutionStatus(arn, status string, stopDate time.Time) error {
	res, err := s.store.DB().Exec(
		`UPDATE executions SET status = ?, stop_date = ? WHERE arn = ?`,
		status, stopDate.Unix(), arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExecutionNotFound
	}
	return nil
}

// ---- Activity ----

func (s *Store) CreateActivity(act *Activity) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO activities (arn, name, created_at) VALUES (?, ?, ?)`,
		act.ARN, act.Name, act.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetActivityByARN(arn string) (*Activity, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, created_at FROM activities WHERE arn = ?`, arn)
	return scanActivity(row)
}

func (s *Store) GetActivityByName(name string) (*Activity, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, created_at FROM activities WHERE name = ?`, name)
	return scanActivity(row)
}

func (s *Store) ListActivities() ([]Activity, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, created_at FROM activities ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var acts []Activity
	for rows.Next() {
		act, err := scanActivity(rows)
		if err != nil {
			return nil, err
		}
		acts = append(acts, *act)
	}
	return acts, rows.Err()
}

func (s *Store) DeleteActivity(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM activities WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errActivityNotFound
	}
	return nil
}

// ---- StateMachineAlias ----

func (s *Store) CreateAlias(a *StateMachineAlias) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO state_machine_aliases (arn, name, state_machine, description, routing_config, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ARN, a.Name, a.StateMachine, a.Description, a.RoutingConfig,
		a.CreatedAt.Unix(), a.UpdatedAt.Unix(),
	)
	return err
}

func (s *Store) GetAliasByARN(arn string) (*StateMachineAlias, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, state_machine, description, routing_config, created_at, updated_at
		 FROM state_machine_aliases WHERE arn = ?`, arn)
	return scanAlias(row)
}

func (s *Store) ListAliases(stateMachineARN string) ([]StateMachineAlias, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, state_machine, description, routing_config, created_at, updated_at
		 FROM state_machine_aliases WHERE state_machine = ? ORDER BY created_at`,
		stateMachineARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var aliases []StateMachineAlias
	for rows.Next() {
		a, err := scanAlias(rows)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, *a)
	}
	return aliases, rows.Err()
}

func (s *Store) UpdateAlias(arn, description, routingConfig string, updatedAt time.Time) error {
	res, err := s.store.DB().Exec(
		`UPDATE state_machine_aliases SET description = ?, routing_config = ?, updated_at = ? WHERE arn = ?`,
		description, routingConfig, updatedAt.Unix(), arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAliasNotFound
	}
	return nil
}

func (s *Store) DeleteAlias(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM state_machine_aliases WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAliasNotFound
	}
	return nil
}

// ---- Scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanStateMachine(s scanner) (*StateMachine, error) {
	var sm StateMachine
	var createdAt, updatedAt int64
	err := s.Scan(&sm.ARN, &sm.Name, &sm.Definition, &sm.RoleARN, &sm.Type,
		&sm.Status, &sm.RevisionID, &sm.Version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStateMachineNotFound
		}
		return nil, err
	}
	sm.CreatedAt = time.Unix(createdAt, 0)
	sm.UpdatedAt = time.Unix(updatedAt, 0)
	return &sm, nil
}

func scanExecution(s scanner) (*Execution, error) {
	var ex Execution
	var startDate, stopDate int64
	err := s.Scan(&ex.ARN, &ex.Name, &ex.StateMachine, &ex.Status,
		&ex.Input, &ex.Output, &startDate, &stopDate)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExecutionNotFound
		}
		return nil, err
	}
	ex.StartDate = time.Unix(startDate, 0)
	if stopDate > 0 {
		ex.StopDate = time.Unix(stopDate, 0)
	}
	return &ex, nil
}

func scanActivity(s scanner) (*Activity, error) {
	var act Activity
	var createdAt int64
	err := s.Scan(&act.ARN, &act.Name, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errActivityNotFound
		}
		return nil, err
	}
	act.CreatedAt = time.Unix(createdAt, 0)
	return &act, nil
}

func scanAlias(s scanner) (*StateMachineAlias, error) {
	var a StateMachineAlias
	var createdAt, updatedAt int64
	err := s.Scan(&a.ARN, &a.Name, &a.StateMachine, &a.Description,
		&a.RoutingConfig, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAliasNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	a.UpdatedAt = time.Unix(updatedAt, 0)
	return &a, nil
}
