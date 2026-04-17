// SPDX-License-Identifier: Apache-2.0

// internal/services/codepipeline/store.go
package codepipeline

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errPipelineNotFound = errors.New("pipeline not found")
var errExecutionNotFound = errors.New("pipeline execution not found")
var errWebhookNotFound = errors.New("webhook not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS pipelines (
			name       TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			role_arn   TEXT NOT NULL DEFAULT '',
			stages     TEXT NOT NULL DEFAULT '[]',
			version    INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS pipeline_executions (
			id            TEXT PRIMARY KEY,
			pipeline_name TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'Succeeded',
			started_at    INTEGER NOT NULL,
			updated_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS webhooks (
			name          TEXT PRIMARY KEY,
			arn           TEXT NOT NULL UNIQUE,
			url           TEXT NOT NULL DEFAULT '',
			pipeline      TEXT NOT NULL DEFAULT '',
			target_action TEXT NOT NULL DEFAULT '',
			filters       TEXT NOT NULL DEFAULT '[]',
			auth_type     TEXT NOT NULL DEFAULT 'GITHUB_HMAC',
			auth_config   TEXT NOT NULL DEFAULT '{}'
		);
	`},
}

type Pipeline struct {
	Name      string
	ARN       string
	RoleARN   string
	Stages    string
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type PipelineExecution struct {
	ID           string
	PipelineName string
	Status       string
	StartedAt    time.Time
	UpdatedAt    time.Time
}

type Webhook struct {
	Name         string
	ARN          string
	URL          string
	Pipeline     string
	TargetAction string
	Filters      string
	AuthType     string
	AuthConfig   string
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "codepipeline.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Pipeline ----

func (s *Store) CreatePipeline(name, arn, roleARN, stages string) (*Pipeline, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pipelines (name, arn, role_arn, stages, version, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 1, ?, ?)`,
		name, arn, roleARN, stages, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		Name: name, ARN: arn, RoleARN: roleARN, Stages: stages,
		Version: 1, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetPipeline(name string) (*Pipeline, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, role_arn, stages, version, created_at, updated_at
		 FROM pipelines WHERE name = ?`, name)
	return scanPipeline(row)
}

func (s *Store) ListPipelines() ([]Pipeline, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, role_arn, stages, version, created_at, updated_at
		 FROM pipelines ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pipelines []Pipeline
	for rows.Next() {
		p, err := scanPipeline(rows)
		if err != nil {
			return nil, err
		}
		pipelines = append(pipelines, *p)
	}
	return pipelines, rows.Err()
}

func (s *Store) UpdatePipeline(name, roleARN, stages string) (*Pipeline, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE pipelines SET role_arn = ?, stages = ?, version = version + 1, updated_at = ?
		 WHERE name = ?`,
		roleARN, stages, now, name,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errPipelineNotFound
	}
	return s.GetPipeline(name)
}

func (s *Store) DeletePipeline(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM pipelines WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipelineNotFound
	}
	return nil
}

// ---- PipelineExecution ----

func (s *Store) CreateExecution(id, pipelineName, status string) (*PipelineExecution, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pipeline_executions (id, pipeline_name, status, started_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		id, pipelineName, status, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &PipelineExecution{
		ID: id, PipelineName: pipelineName, Status: status,
		StartedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetExecution(pipelineName, executionID string) (*PipelineExecution, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, pipeline_name, status, started_at, updated_at
		 FROM pipeline_executions WHERE id = ? AND pipeline_name = ?`,
		executionID, pipelineName)
	return scanExecution(row)
}

func (s *Store) ListExecutions(pipelineName string) ([]PipelineExecution, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, pipeline_name, status, started_at, updated_at
		 FROM pipeline_executions WHERE pipeline_name = ? ORDER BY started_at DESC`,
		pipelineName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var execs []PipelineExecution
	for rows.Next() {
		e, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		execs = append(execs, *e)
	}
	return execs, rows.Err()
}

func (s *Store) UpdateExecutionStatus(pipelineName, executionID, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE pipeline_executions SET status = ?, updated_at = ?
		 WHERE id = ? AND pipeline_name = ?`,
		status, now, executionID, pipelineName,
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

// ---- Webhook ----

func (s *Store) PutWebhook(name, arn, url, pipeline, targetAction, filters, authType, authConfig string) (*Webhook, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO webhooks (name, arn, url, pipeline, target_action, filters, auth_type, auth_config)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET
		   arn=excluded.arn, url=excluded.url, pipeline=excluded.pipeline,
		   target_action=excluded.target_action, filters=excluded.filters,
		   auth_type=excluded.auth_type, auth_config=excluded.auth_config`,
		name, arn, url, pipeline, targetAction, filters, authType, authConfig,
	)
	if err != nil {
		return nil, err
	}
	return &Webhook{
		Name: name, ARN: arn, URL: url, Pipeline: pipeline,
		TargetAction: targetAction, Filters: filters,
		AuthType: authType, AuthConfig: authConfig,
	}, nil
}

func (s *Store) GetWebhook(name string) (*Webhook, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, url, pipeline, target_action, filters, auth_type, auth_config
		 FROM webhooks WHERE name = ?`, name)
	return scanWebhook(row)
}

func (s *Store) ListWebhooks() ([]Webhook, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, url, pipeline, target_action, filters, auth_type, auth_config
		 FROM webhooks ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var webhooks []Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, err
		}
		webhooks = append(webhooks, *w)
	}
	return webhooks, rows.Err()
}

func (s *Store) DeleteWebhook(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM webhooks WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWebhookNotFound
	}
	return nil
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanPipeline(s scanner) (*Pipeline, error) {
	var p Pipeline
	var createdAt, updatedAt int64
	err := s.Scan(&p.Name, &p.ARN, &p.RoleARN, &p.Stages, &p.Version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPipelineNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	p.UpdatedAt = time.Unix(updatedAt, 0)
	return &p, nil
}

func scanExecution(s scanner) (*PipelineExecution, error) {
	var e PipelineExecution
	var startedAt, updatedAt int64
	err := s.Scan(&e.ID, &e.PipelineName, &e.Status, &startedAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExecutionNotFound
		}
		return nil, err
	}
	e.StartedAt = time.Unix(startedAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

func scanWebhook(s scanner) (*Webhook, error) {
	var w Webhook
	err := s.Scan(&w.Name, &w.ARN, &w.URL, &w.Pipeline, &w.TargetAction, &w.Filters, &w.AuthType, &w.AuthConfig)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errWebhookNotFound
		}
		return nil, err
	}
	return &w, nil
}
