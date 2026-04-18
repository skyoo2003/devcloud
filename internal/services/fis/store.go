// SPDX-License-Identifier: Apache-2.0

// internal/services/fis/store.go
package fis

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errTemplateNotFound = errors.New("experiment template not found")
var errExperimentNotFound = errors.New("experiment not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS experiment_templates (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			role_arn        TEXT NOT NULL DEFAULT '',
			actions         TEXT NOT NULL DEFAULT '{}',
			targets         TEXT NOT NULL DEFAULT '{}',
			stop_conditions TEXT NOT NULL DEFAULT '[]',
			log_config      TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS experiments (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			template_id     TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'completed',
			role_arn        TEXT NOT NULL DEFAULT '',
			actions         TEXT NOT NULL DEFAULT '{}',
			targets         TEXT NOT NULL DEFAULT '{}',
			start_time      INTEGER NOT NULL,
			end_time        INTEGER NOT NULL DEFAULT 0
		);
	`},
}

// ExperimentTemplate holds a stored experiment template.
type ExperimentTemplate struct {
	ID             string
	ARN            string
	Description    string
	RoleARN        string
	Actions        string // JSON
	Targets        string // JSON
	StopConditions string // JSON
	LogConfig      string // JSON
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Experiment holds a stored experiment.
type Experiment struct {
	ID         string
	ARN        string
	TemplateID string
	Status     string
	RoleARN    string
	Actions    string // JSON
	Targets    string // JSON
	StartTime  time.Time
	EndTime    time.Time
}

// Store is the FIS SQLite store.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the FIS SQLite database.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "fis.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- ExperimentTemplate ---

func (s *Store) CreateExperimentTemplate(t *ExperimentTemplate) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO experiment_templates
			(id, arn, description, role_arn, actions, targets, stop_conditions, log_config, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.ARN, t.Description, t.RoleARN, t.Actions, t.Targets, t.StopConditions, t.LogConfig, now, now,
	)
	return err
}

func (s *Store) GetExperimentTemplate(id string) (*ExperimentTemplate, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, description, role_arn, actions, targets, stop_conditions, log_config, created_at, updated_at
		 FROM experiment_templates WHERE id = ?`, id)
	return scanTemplate(row)
}

func (s *Store) ListExperimentTemplates() ([]ExperimentTemplate, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, description, role_arn, actions, targets, stop_conditions, log_config, created_at, updated_at
		 FROM experiment_templates ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var templates []ExperimentTemplate
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		templates = append(templates, *t)
	}
	return templates, rows.Err()
}

func (s *Store) UpdateExperimentTemplate(id string, fields map[string]any) error {
	t, err := s.GetExperimentTemplate(id)
	if err != nil {
		return errTemplateNotFound
	}
	if v, ok := fields["description"].(string); ok {
		t.Description = v
	}
	if v, ok := fields["roleArn"].(string); ok && v != "" {
		t.RoleARN = v
	}
	if v, ok := fields["actions"]; ok {
		if b, err := json.Marshal(v); err == nil {
			t.Actions = string(b)
		}
	}
	if v, ok := fields["targets"]; ok {
		if b, err := json.Marshal(v); err == nil {
			t.Targets = string(b)
		}
	}
	if v, ok := fields["stopConditions"]; ok {
		if b, err := json.Marshal(v); err == nil {
			t.StopConditions = string(b)
		}
	}
	if v, ok := fields["logConfiguration"]; ok {
		if b, err := json.Marshal(v); err == nil {
			t.LogConfig = string(b)
		}
	}
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE experiment_templates SET
			description=?, role_arn=?, actions=?, targets=?, stop_conditions=?, log_config=?, updated_at=?
		 WHERE id=?`,
		t.Description, t.RoleARN, t.Actions, t.Targets, t.StopConditions, t.LogConfig, now, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTemplateNotFound
	}
	return nil
}

func (s *Store) DeleteExperimentTemplate(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM experiment_templates WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTemplateNotFound
	}
	return nil
}

// --- Experiment ---

func (s *Store) CreateExperiment(e *Experiment) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO experiments
			(id, arn, template_id, status, role_arn, actions, targets, start_time, end_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.ARN, e.TemplateID, e.Status, e.RoleARN, e.Actions, e.Targets, now, 0,
	)
	return err
}

func (s *Store) GetExperiment(id string) (*Experiment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, template_id, status, role_arn, actions, targets, start_time, end_time
		 FROM experiments WHERE id = ?`, id)
	return scanExperiment(row)
}

func (s *Store) ListExperiments(templateID string) ([]Experiment, error) {
	var rows *sql.Rows
	var err error
	if templateID != "" {
		rows, err = s.store.DB().Query(
			`SELECT id, arn, template_id, status, role_arn, actions, targets, start_time, end_time
			 FROM experiments WHERE template_id = ? ORDER BY start_time`, templateID)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT id, arn, template_id, status, role_arn, actions, targets, start_time, end_time
			 FROM experiments ORDER BY start_time`)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var exps []Experiment
	for rows.Next() {
		e, err := scanExperiment(rows)
		if err != nil {
			return nil, err
		}
		exps = append(exps, *e)
	}
	return exps, rows.Err()
}

func (s *Store) StopExperiment(id string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE experiments SET status='stopped', end_time=? WHERE id=?`, now, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExperimentNotFound
	}
	return nil
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanTemplate(sc scanner) (*ExperimentTemplate, error) {
	var t ExperimentTemplate
	var createdAt, updatedAt int64
	err := sc.Scan(
		&t.ID, &t.ARN, &t.Description, &t.RoleARN,
		&t.Actions, &t.Targets, &t.StopConditions, &t.LogConfig,
		&createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTemplateNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	t.UpdatedAt = time.Unix(updatedAt, 0)
	return &t, nil
}

func scanExperiment(sc scanner) (*Experiment, error) {
	var e Experiment
	var startTime, endTime int64
	err := sc.Scan(
		&e.ID, &e.ARN, &e.TemplateID, &e.Status, &e.RoleARN,
		&e.Actions, &e.Targets, &startTime, &endTime,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExperimentNotFound
		}
		return nil, err
	}
	e.StartTime = time.Unix(startTime, 0)
	if endTime > 0 {
		e.EndTime = time.Unix(endTime, 0)
	}
	return &e, nil
}
