// SPDX-License-Identifier: Apache-2.0

// internal/services/sagemaker/store.go
package sagemaker

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotebookInstanceNotFound  = errors.New("notebook instance not found")
	errModelNotFound             = errors.New("model not found")
	errEndpointConfigNotFound    = errors.New("endpoint config not found")
	errEndpointNotFound          = errors.New("endpoint not found")
	errTrainingJobNotFound       = errors.New("training job not found")
	errProcessingJobNotFound     = errors.New("processing job not found")
	errTransformJobNotFound      = errors.New("transform job not found")
	errPipelineNotFound          = errors.New("pipeline not found")
	errPipelineExecutionNotFound = errors.New("pipeline execution not found")
	errExperimentNotFound        = errors.New("experiment not found")
	errTrialNotFound             = errors.New("trial not found")
	errDomainNotFound            = errors.New("domain not found")
	errUserProfileNotFound       = errors.New("user profile not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS notebook_instances (
			name          TEXT PRIMARY KEY,
			arn           TEXT NOT NULL UNIQUE,
			status        TEXT NOT NULL DEFAULT 'InService',
			instance_type TEXT NOT NULL DEFAULT 'ml.t3.medium',
			role_arn      TEXT NOT NULL DEFAULT '',
			url           TEXT NOT NULL DEFAULT '',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS models (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			execution_role  TEXT NOT NULL DEFAULT '',
			primary_container TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS endpoint_configs (
			name                TEXT PRIMARY KEY,
			arn                 TEXT NOT NULL UNIQUE,
			production_variants TEXT NOT NULL DEFAULT '[]',
			created_at          INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS endpoints (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			config_name TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT 'InService',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS training_jobs (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'Completed',
			role_arn        TEXT NOT NULL DEFAULT '',
			algorithm       TEXT NOT NULL DEFAULT '{}',
			input_config    TEXT NOT NULL DEFAULT '[]',
			output_config   TEXT NOT NULL DEFAULT '{}',
			resource_config TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS processing_jobs (
			name       TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			status     TEXT NOT NULL DEFAULT 'Completed',
			role_arn   TEXT NOT NULL DEFAULT '',
			app_spec   TEXT NOT NULL DEFAULT '{}',
			inputs     TEXT NOT NULL DEFAULT '[]',
			outputs    TEXT NOT NULL DEFAULT '[]',
			resources  TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS transform_jobs (
			name       TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			status     TEXT NOT NULL DEFAULT 'Completed',
			model_name TEXT NOT NULL DEFAULT '',
			input      TEXT NOT NULL DEFAULT '{}',
			output     TEXT NOT NULL DEFAULT '{}',
			resources  TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS pipelines (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			role_arn    TEXT NOT NULL DEFAULT '',
			definition  TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS pipeline_executions (
			arn           TEXT PRIMARY KEY,
			pipeline_name TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'Succeeded',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS experiments (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS trials (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			experiment_name TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS domains (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL DEFAULT '',
			status     TEXT NOT NULL DEFAULT 'InService',
			auth_mode  TEXT NOT NULL DEFAULT 'IAM',
			vpc_id     TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS user_profiles (
			domain_id  TEXT NOT NULL,
			name       TEXT NOT NULL,
			arn        TEXT NOT NULL UNIQUE,
			status     TEXT NOT NULL DEFAULT 'InService',
			created_at INTEGER NOT NULL,
			PRIMARY KEY (domain_id, name)
		);
	`},
}

// ---- model types ----

type NotebookInstance struct {
	Name         string
	ARN          string
	Status       string
	InstanceType string
	RoleARN      string
	URL          string
	CreatedAt    time.Time
}

type Model struct {
	Name             string
	ARN              string
	ExecutionRole    string
	PrimaryContainer string
	CreatedAt        time.Time
}

type EndpointConfig struct {
	Name               string
	ARN                string
	ProductionVariants string
	CreatedAt          time.Time
}

type Endpoint struct {
	Name       string
	ARN        string
	ConfigName string
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type TrainingJob struct {
	Name           string
	ARN            string
	Status         string
	RoleARN        string
	Algorithm      string
	InputConfig    string
	OutputConfig   string
	ResourceConfig string
	CreatedAt      time.Time
}

type ProcessingJob struct {
	Name      string
	ARN       string
	Status    string
	RoleARN   string
	AppSpec   string
	Inputs    string
	Outputs   string
	Resources string
	CreatedAt time.Time
}

type TransformJob struct {
	Name      string
	ARN       string
	Status    string
	ModelName string
	Input     string
	Output    string
	Resources string
	CreatedAt time.Time
}

type Pipeline struct {
	Name        string
	ARN         string
	RoleARN     string
	Definition  string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type PipelineExecution struct {
	ARN          string
	PipelineName string
	Status       string
	CreatedAt    time.Time
}

type Experiment struct {
	Name        string
	ARN         string
	Description string
	CreatedAt   time.Time
}

type Trial struct {
	Name           string
	ARN            string
	ExperimentName string
	CreatedAt      time.Time
}

type Domain struct {
	ID        string
	ARN       string
	Name      string
	Status    string
	AuthMode  string
	VpcID     string
	CreatedAt time.Time
}

type UserProfile struct {
	DomainID  string
	Name      string
	ARN       string
	Status    string
	CreatedAt time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "sagemaker.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

type scanner interface{ Scan(dest ...any) error }

// ---- NotebookInstance ----

func (s *Store) CreateNotebookInstance(name, arn, status, instanceType, roleARN, url string) (*NotebookInstance, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO notebook_instances (name, arn, status, instance_type, role_arn, url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, arn, status, instanceType, roleARN, url, now,
	)
	if err != nil {
		return nil, err
	}
	return &NotebookInstance{Name: name, ARN: arn, Status: status, InstanceType: instanceType, RoleARN: roleARN, URL: url, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetNotebookInstance(name string) (*NotebookInstance, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, instance_type, role_arn, url, created_at FROM notebook_instances WHERE name = ?`, name)
	return scanNotebookInstance(row)
}

func (s *Store) ListNotebookInstances() ([]NotebookInstance, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, status, instance_type, role_arn, url, created_at FROM notebook_instances ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []NotebookInstance
	for rows.Next() {
		nb, err := scanNotebookInstance(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *nb)
	}
	return items, rows.Err()
}

func (s *Store) UpdateNotebookInstance(name, instanceType, roleARN string) error {
	res, err := s.store.DB().Exec(
		`UPDATE notebook_instances SET instance_type = ?, role_arn = ? WHERE name = ?`,
		instanceType, roleARN, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotebookInstanceNotFound
	}
	return nil
}

func (s *Store) UpdateNotebookInstanceStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE notebook_instances SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotebookInstanceNotFound
	}
	return nil
}

func (s *Store) DeleteNotebookInstance(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM notebook_instances WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotebookInstanceNotFound
	}
	return nil
}

func scanNotebookInstance(sc scanner) (*NotebookInstance, error) {
	var nb NotebookInstance
	var createdAt int64
	err := sc.Scan(&nb.Name, &nb.ARN, &nb.Status, &nb.InstanceType, &nb.RoleARN, &nb.URL, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotebookInstanceNotFound
		}
		return nil, err
	}
	nb.CreatedAt = time.Unix(createdAt, 0)
	return &nb, nil
}

// ---- Model ----

func (s *Store) CreateModel(name, arn, executionRole, primaryContainer string) (*Model, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO models (name, arn, execution_role, primary_container, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, arn, executionRole, primaryContainer, now,
	)
	if err != nil {
		return nil, err
	}
	return &Model{Name: name, ARN: arn, ExecutionRole: executionRole, PrimaryContainer: primaryContainer, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetModel(name string) (*Model, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, execution_role, primary_container, created_at FROM models WHERE name = ?`, name)
	return scanModel(row)
}

func (s *Store) ListModels() ([]Model, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, execution_role, primary_container, created_at FROM models ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Model
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

func (s *Store) DeleteModel(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM models WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errModelNotFound
	}
	return nil
}

func scanModel(sc scanner) (*Model, error) {
	var m Model
	var createdAt int64
	err := sc.Scan(&m.Name, &m.ARN, &m.ExecutionRole, &m.PrimaryContainer, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errModelNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

// ---- EndpointConfig ----

func (s *Store) CreateEndpointConfig(name, arn, productionVariants string) (*EndpointConfig, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO endpoint_configs (name, arn, production_variants, created_at) VALUES (?, ?, ?, ?)`,
		name, arn, productionVariants, now,
	)
	if err != nil {
		return nil, err
	}
	return &EndpointConfig{Name: name, ARN: arn, ProductionVariants: productionVariants, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetEndpointConfig(name string) (*EndpointConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, production_variants, created_at FROM endpoint_configs WHERE name = ?`, name)
	return scanEndpointConfig(row)
}

func (s *Store) ListEndpointConfigs() ([]EndpointConfig, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, production_variants, created_at FROM endpoint_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []EndpointConfig
	for rows.Next() {
		ec, err := scanEndpointConfig(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *ec)
	}
	return items, rows.Err()
}

func (s *Store) DeleteEndpointConfig(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM endpoint_configs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointConfigNotFound
	}
	return nil
}

func scanEndpointConfig(sc scanner) (*EndpointConfig, error) {
	var ec EndpointConfig
	var createdAt int64
	err := sc.Scan(&ec.Name, &ec.ARN, &ec.ProductionVariants, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEndpointConfigNotFound
		}
		return nil, err
	}
	ec.CreatedAt = time.Unix(createdAt, 0)
	return &ec, nil
}

// ---- Endpoint ----

func (s *Store) CreateEndpoint(name, arn, configName, status string) (*Endpoint, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO endpoints (name, arn, config_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		name, arn, configName, status, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Endpoint{Name: name, ARN: arn, ConfigName: configName, Status: status, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetEndpoint(name string) (*Endpoint, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, config_name, status, created_at, updated_at FROM endpoints WHERE name = ?`, name)
	return scanEndpoint(row)
}

func (s *Store) ListEndpoints() ([]Endpoint, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, config_name, status, created_at, updated_at FROM endpoints ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Endpoint
	for rows.Next() {
		e, err := scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *e)
	}
	return items, rows.Err()
}

func (s *Store) UpdateEndpoint(name, configName string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE endpoints SET config_name = ?, updated_at = ? WHERE name = ?`,
		configName, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointNotFound
	}
	return nil
}

func (s *Store) DeleteEndpoint(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM endpoints WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointNotFound
	}
	return nil
}

func scanEndpoint(sc scanner) (*Endpoint, error) {
	var e Endpoint
	var createdAt, updatedAt int64
	err := sc.Scan(&e.Name, &e.ARN, &e.ConfigName, &e.Status, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEndpointNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

// ---- TrainingJob ----

func (s *Store) CreateTrainingJob(name, arn, status, roleARN, algorithm, inputConfig, outputConfig, resourceConfig string) (*TrainingJob, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO training_jobs (name, arn, status, role_arn, algorithm, input_config, output_config, resource_config, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, arn, status, roleARN, algorithm, inputConfig, outputConfig, resourceConfig, now,
	)
	if err != nil {
		return nil, err
	}
	return &TrainingJob{Name: name, ARN: arn, Status: status, RoleARN: roleARN, Algorithm: algorithm, InputConfig: inputConfig, OutputConfig: outputConfig, ResourceConfig: resourceConfig, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetTrainingJob(name string) (*TrainingJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, role_arn, algorithm, input_config, output_config, resource_config, created_at FROM training_jobs WHERE name = ?`, name)
	return scanTrainingJob(row)
}

func (s *Store) ListTrainingJobs() ([]TrainingJob, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, status, role_arn, algorithm, input_config, output_config, resource_config, created_at FROM training_jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TrainingJob
	for rows.Next() {
		tj, err := scanTrainingJob(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *tj)
	}
	return items, rows.Err()
}

func (s *Store) UpdateTrainingJobStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE training_jobs SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrainingJobNotFound
	}
	return nil
}

func scanTrainingJob(sc scanner) (*TrainingJob, error) {
	var tj TrainingJob
	var createdAt int64
	err := sc.Scan(&tj.Name, &tj.ARN, &tj.Status, &tj.RoleARN, &tj.Algorithm, &tj.InputConfig, &tj.OutputConfig, &tj.ResourceConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTrainingJobNotFound
		}
		return nil, err
	}
	tj.CreatedAt = time.Unix(createdAt, 0)
	return &tj, nil
}

// ---- ProcessingJob ----

func (s *Store) CreateProcessingJob(name, arn, status, roleARN, appSpec, inputs, outputs, resources string) (*ProcessingJob, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO processing_jobs (name, arn, status, role_arn, app_spec, inputs, outputs, resources, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, arn, status, roleARN, appSpec, inputs, outputs, resources, now,
	)
	if err != nil {
		return nil, err
	}
	return &ProcessingJob{Name: name, ARN: arn, Status: status, RoleARN: roleARN, AppSpec: appSpec, Inputs: inputs, Outputs: outputs, Resources: resources, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetProcessingJob(name string) (*ProcessingJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, role_arn, app_spec, inputs, outputs, resources, created_at FROM processing_jobs WHERE name = ?`, name)
	return scanProcessingJob(row)
}

func (s *Store) ListProcessingJobs() ([]ProcessingJob, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, status, role_arn, app_spec, inputs, outputs, resources, created_at FROM processing_jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ProcessingJob
	for rows.Next() {
		pj, err := scanProcessingJob(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *pj)
	}
	return items, rows.Err()
}

func (s *Store) UpdateProcessingJobStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE processing_jobs SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProcessingJobNotFound
	}
	return nil
}

func scanProcessingJob(sc scanner) (*ProcessingJob, error) {
	var pj ProcessingJob
	var createdAt int64
	err := sc.Scan(&pj.Name, &pj.ARN, &pj.Status, &pj.RoleARN, &pj.AppSpec, &pj.Inputs, &pj.Outputs, &pj.Resources, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errProcessingJobNotFound
		}
		return nil, err
	}
	pj.CreatedAt = time.Unix(createdAt, 0)
	return &pj, nil
}

// ---- TransformJob ----

func (s *Store) CreateTransformJob(name, arn, status, modelName, input, output, resources string) (*TransformJob, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO transform_jobs (name, arn, status, model_name, input, output, resources, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		name, arn, status, modelName, input, output, resources, now,
	)
	if err != nil {
		return nil, err
	}
	return &TransformJob{Name: name, ARN: arn, Status: status, ModelName: modelName, Input: input, Output: output, Resources: resources, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetTransformJob(name string) (*TransformJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, model_name, input, output, resources, created_at FROM transform_jobs WHERE name = ?`, name)
	return scanTransformJob(row)
}

func (s *Store) ListTransformJobs() ([]TransformJob, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, status, model_name, input, output, resources, created_at FROM transform_jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []TransformJob
	for rows.Next() {
		tj, err := scanTransformJob(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *tj)
	}
	return items, rows.Err()
}

func (s *Store) UpdateTransformJobStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE transform_jobs SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTransformJobNotFound
	}
	return nil
}

func scanTransformJob(sc scanner) (*TransformJob, error) {
	var tj TransformJob
	var createdAt int64
	err := sc.Scan(&tj.Name, &tj.ARN, &tj.Status, &tj.ModelName, &tj.Input, &tj.Output, &tj.Resources, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTransformJobNotFound
		}
		return nil, err
	}
	tj.CreatedAt = time.Unix(createdAt, 0)
	return &tj, nil
}

// ---- Pipeline ----

func (s *Store) CreatePipeline(name, arn, roleARN, definition, description string) (*Pipeline, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pipelines (name, arn, role_arn, definition, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, arn, roleARN, definition, description, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Pipeline{Name: name, ARN: arn, RoleARN: roleARN, Definition: definition, Description: description, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetPipeline(name string) (*Pipeline, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, role_arn, definition, description, created_at, updated_at FROM pipelines WHERE name = ?`, name)
	return scanPipeline(row)
}

func (s *Store) ListPipelines() ([]Pipeline, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, role_arn, definition, description, created_at, updated_at FROM pipelines ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Pipeline
	for rows.Next() {
		p, err := scanPipeline(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *p)
	}
	return items, rows.Err()
}

func (s *Store) UpdatePipeline(name, roleARN, definition, description string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE pipelines SET role_arn = ?, definition = ?, description = ?, updated_at = ? WHERE name = ?`,
		roleARN, definition, description, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipelineNotFound
	}
	return nil
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

func scanPipeline(sc scanner) (*Pipeline, error) {
	var p Pipeline
	var createdAt, updatedAt int64
	err := sc.Scan(&p.Name, &p.ARN, &p.RoleARN, &p.Definition, &p.Description, &createdAt, &updatedAt)
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

// ---- PipelineExecution ----

func (s *Store) CreatePipelineExecution(arn, pipelineName, status string) (*PipelineExecution, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pipeline_executions (arn, pipeline_name, status, created_at) VALUES (?, ?, ?, ?)`,
		arn, pipelineName, status, now,
	)
	if err != nil {
		return nil, err
	}
	return &PipelineExecution{ARN: arn, PipelineName: pipelineName, Status: status, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetPipelineExecution(arn string) (*PipelineExecution, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, pipeline_name, status, created_at FROM pipeline_executions WHERE arn = ?`, arn)
	return scanPipelineExecution(row)
}

func (s *Store) ListPipelineExecutions(pipelineName string) ([]PipelineExecution, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, pipeline_name, status, created_at FROM pipeline_executions WHERE pipeline_name = ? ORDER BY created_at DESC`,
		pipelineName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PipelineExecution
	for rows.Next() {
		pe, err := scanPipelineExecution(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *pe)
	}
	return items, rows.Err()
}

func (s *Store) UpdatePipelineExecutionStatus(arn, status string) error {
	res, err := s.store.DB().Exec(`UPDATE pipeline_executions SET status = ? WHERE arn = ?`, status, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipelineExecutionNotFound
	}
	return nil
}

func scanPipelineExecution(sc scanner) (*PipelineExecution, error) {
	var pe PipelineExecution
	var createdAt int64
	err := sc.Scan(&pe.ARN, &pe.PipelineName, &pe.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPipelineExecutionNotFound
		}
		return nil, err
	}
	pe.CreatedAt = time.Unix(createdAt, 0)
	return &pe, nil
}

// ---- Experiment ----

func (s *Store) CreateExperiment(name, arn, description string) (*Experiment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO experiments (name, arn, description, created_at) VALUES (?, ?, ?, ?)`,
		name, arn, description, now,
	)
	if err != nil {
		return nil, err
	}
	return &Experiment{Name: name, ARN: arn, Description: description, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetExperiment(name string) (*Experiment, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, created_at FROM experiments WHERE name = ?`, name)
	return scanExperiment(row)
}

func (s *Store) ListExperiments() ([]Experiment, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, description, created_at FROM experiments ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Experiment
	for rows.Next() {
		e, err := scanExperiment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *e)
	}
	return items, rows.Err()
}

func (s *Store) UpdateExperiment(name, description string) error {
	res, err := s.store.DB().Exec(`UPDATE experiments SET description = ? WHERE name = ?`, description, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExperimentNotFound
	}
	return nil
}

func (s *Store) DeleteExperiment(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM experiments WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errExperimentNotFound
	}
	return nil
}

func scanExperiment(sc scanner) (*Experiment, error) {
	var e Experiment
	var createdAt int64
	err := sc.Scan(&e.Name, &e.ARN, &e.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errExperimentNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	return &e, nil
}

// ---- Trial ----

func (s *Store) CreateTrial(name, arn, experimentName string) (*Trial, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO trials (name, arn, experiment_name, created_at) VALUES (?, ?, ?, ?)`,
		name, arn, experimentName, now,
	)
	if err != nil {
		return nil, err
	}
	return &Trial{Name: name, ARN: arn, ExperimentName: experimentName, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetTrial(name string) (*Trial, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, experiment_name, created_at FROM trials WHERE name = ?`, name)
	return scanTrial(row)
}

func (s *Store) ListTrials(experimentName string) ([]Trial, error) {
	var rows *sql.Rows
	var err error
	if experimentName != "" {
		rows, err = s.store.DB().Query(
			`SELECT name, arn, experiment_name, created_at FROM trials WHERE experiment_name = ? ORDER BY name`,
			experimentName)
	} else {
		rows, err = s.store.DB().Query(`SELECT name, arn, experiment_name, created_at FROM trials ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Trial
	for rows.Next() {
		tr, err := scanTrial(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *tr)
	}
	return items, rows.Err()
}

func (s *Store) UpdateTrial(name, experimentName string) error {
	res, err := s.store.DB().Exec(`UPDATE trials SET experiment_name = ? WHERE name = ?`, experimentName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrialNotFound
	}
	return nil
}

func (s *Store) DeleteTrial(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM trials WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrialNotFound
	}
	return nil
}

func scanTrial(sc scanner) (*Trial, error) {
	var tr Trial
	var createdAt int64
	err := sc.Scan(&tr.Name, &tr.ARN, &tr.ExperimentName, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTrialNotFound
		}
		return nil, err
	}
	tr.CreatedAt = time.Unix(createdAt, 0)
	return &tr, nil
}

// ---- Domain ----

func (s *Store) CreateDomain(id, arn, name, status, authMode, vpcID string) (*Domain, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (id, arn, name, status, auth_mode, vpc_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, arn, name, status, authMode, vpcID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Domain{ID: id, ARN: arn, Name: name, Status: status, AuthMode: authMode, VpcID: vpcID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetDomain(id string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, status, auth_mode, vpc_id, created_at FROM domains WHERE id = ?`, id)
	return scanDomain(row)
}

func (s *Store) ListDomains() ([]Domain, error) {
	rows, err := s.store.DB().Query(`SELECT id, arn, name, status, auth_mode, vpc_id, created_at FROM domains ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Domain
	for rows.Next() {
		d, err := scanDomain(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *d)
	}
	return items, rows.Err()
}

func (s *Store) UpdateDomain(id, vpcID string) error {
	res, err := s.store.DB().Exec(`UPDATE domains SET vpc_id = ? WHERE id = ?`, vpcID, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNotFound
	}
	return nil
}

func (s *Store) DeleteDomain(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM domains WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNotFound
	}
	return nil
}

func scanDomain(sc scanner) (*Domain, error) {
	var d Domain
	var createdAt int64
	err := sc.Scan(&d.ID, &d.ARN, &d.Name, &d.Status, &d.AuthMode, &d.VpcID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

// ---- UserProfile ----

func (s *Store) CreateUserProfile(domainID, name, arn, status string) (*UserProfile, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO user_profiles (domain_id, name, arn, status, created_at) VALUES (?, ?, ?, ?, ?)`,
		domainID, name, arn, status, now,
	)
	if err != nil {
		return nil, err
	}
	return &UserProfile{DomainID: domainID, Name: name, ARN: arn, Status: status, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetUserProfile(domainID, name string) (*UserProfile, error) {
	row := s.store.DB().QueryRow(
		`SELECT domain_id, name, arn, status, created_at FROM user_profiles WHERE domain_id = ? AND name = ?`,
		domainID, name)
	return scanUserProfile(row)
}

func (s *Store) ListUserProfiles(domainID string) ([]UserProfile, error) {
	var rows *sql.Rows
	var err error
	if domainID != "" {
		rows, err = s.store.DB().Query(
			`SELECT domain_id, name, arn, status, created_at FROM user_profiles WHERE domain_id = ? ORDER BY name`,
			domainID)
	} else {
		rows, err = s.store.DB().Query(`SELECT domain_id, name, arn, status, created_at FROM user_profiles ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []UserProfile
	for rows.Next() {
		up, err := scanUserProfile(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *up)
	}
	return items, rows.Err()
}

func (s *Store) UpdateUserProfile(domainID, name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE user_profiles SET status = ? WHERE domain_id = ? AND name = ?`, status, domainID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserProfileNotFound
	}
	return nil
}

func (s *Store) DeleteUserProfile(domainID, name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM user_profiles WHERE domain_id = ? AND name = ?`, domainID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserProfileNotFound
	}
	return nil
}

func scanUserProfile(sc scanner) (*UserProfile, error) {
	var up UserProfile
	var createdAt int64
	err := sc.Scan(&up.DomainID, &up.Name, &up.ARN, &up.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUserProfileNotFound
		}
		return nil, err
	}
	up.CreatedAt = time.Unix(createdAt, 0)
	return &up, nil
}

// ---- Tags ----

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTagsForResource(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}
