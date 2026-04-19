// SPDX-License-Identifier: Apache-2.0

package elasticbeanstalk

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errAppNotFound = errors.New("application not found")
var errVersionNotFound = errors.New("application version not found")
var errEnvNotFound = errors.New("environment not found")
var errTemplateNotFound = errors.New("configuration template not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS applications (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS app_versions (
			app_name        TEXT NOT NULL,
			version_label   TEXT NOT NULL,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			source_bundle   TEXT NOT NULL DEFAULT '{}',
			status          TEXT NOT NULL DEFAULT 'PROCESSED',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (app_name, version_label)
		);
		CREATE TABLE IF NOT EXISTS environments (
			id              TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			arn             TEXT NOT NULL UNIQUE,
			app_name        TEXT NOT NULL,
			version_label   TEXT NOT NULL DEFAULT '',
			template_name   TEXT NOT NULL DEFAULT '',
			solution_stack  TEXT NOT NULL DEFAULT '64bit Amazon Linux 2023 v4.0.0 running Docker',
			tier            TEXT NOT NULL DEFAULT 'WebServer',
			status          TEXT NOT NULL DEFAULT 'Ready',
			health          TEXT NOT NULL DEFAULT 'Green',
			cname           TEXT NOT NULL DEFAULT '',
			endpoint_url    TEXT NOT NULL DEFAULT '',
			description     TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS config_templates (
			app_name        TEXT NOT NULL,
			name            TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			solution_stack  TEXT NOT NULL DEFAULT '',
			options         TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (app_name, name)
		);
	`},
}

// Application represents a Beanstalk application.
type Application struct {
	Name        string
	ARN         string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// AppVersion represents an application version.
type AppVersion struct {
	AppName      string
	VersionLabel string
	ARN          string
	Description  string
	SourceBundle string
	Status       string
	CreatedAt    time.Time
}

// Environment represents a Beanstalk environment.
type Environment struct {
	ID            string
	Name          string
	ARN           string
	AppName       string
	VersionLabel  string
	TemplateName  string
	SolutionStack string
	Tier          string
	Status        string
	Health        string
	CNAME         string
	EndpointURL   string
	Description   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ConfigTemplate represents a configuration template.
type ConfigTemplate struct {
	AppName       string
	Name          string
	Description   string
	SolutionStack string
	Options       string
}

// Store provides Elastic Beanstalk data persistence.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the SQLite database for Elastic Beanstalk.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "elasticbeanstalk.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

// Close releases the database connection.
func (s *Store) Close() error { return s.store.Close() }

// --- Application CRUD ---

func (s *Store) CreateApplication(name, arn, description string) (*Application, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO applications (name, arn, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		name, arn, description, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Application{
		Name:        name,
		ARN:         arn,
		Description: description,
		CreatedAt:   time.Unix(now, 0),
		UpdatedAt:   time.Unix(now, 0),
	}, nil
}

func (s *Store) GetApplication(name string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, created_at, updated_at FROM applications WHERE name = ?`, name)
	return scanApplication(row)
}

func (s *Store) ListApplications(names []string) ([]Application, error) {
	query := `SELECT name, arn, description, created_at, updated_at FROM applications`
	var args []any
	if len(names) > 0 {
		placeholders := make([]byte, 0, len(names)*2)
		for i, n := range names {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args = append(args, n)
		}
		query += ` WHERE name IN (` + string(placeholders) + `)`
	}
	query += ` ORDER BY name`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var apps []Application
	for rows.Next() {
		a, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *a)
	}
	return apps, rows.Err()
}

func (s *Store) UpdateApplication(name, description string) (*Application, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE applications SET description = ?, updated_at = ? WHERE name = ?`,
		description, now, name,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errAppNotFound
	}
	return s.GetApplication(name)
}

func (s *Store) DeleteApplication(name string) (*Application, error) {
	a, err := s.GetApplication(name)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM app_versions WHERE app_name = ?`, name); err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM environments WHERE app_name = ?`, name); err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM config_templates WHERE app_name = ?`, name); err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM applications WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errAppNotFound
	}
	return a, nil
}

// --- AppVersion CRUD ---

func (s *Store) CreateAppVersion(appName, versionLabel, arn, description, sourceBundle string) (*AppVersion, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO app_versions (app_name, version_label, arn, description, source_bundle, status, created_at)
		 VALUES (?, ?, ?, ?, ?, 'PROCESSED', ?)`,
		appName, versionLabel, arn, description, sourceBundle, now,
	)
	if err != nil {
		return nil, err
	}
	return &AppVersion{
		AppName:      appName,
		VersionLabel: versionLabel,
		ARN:          arn,
		Description:  description,
		SourceBundle: sourceBundle,
		Status:       "PROCESSED",
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAppVersion(appName, versionLabel string) (*AppVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_name, version_label, arn, description, source_bundle, status, created_at
		 FROM app_versions WHERE app_name = ? AND version_label = ?`,
		appName, versionLabel)
	return scanAppVersion(row)
}

func (s *Store) ListAppVersions(appName string, labels []string) ([]AppVersion, error) {
	query := `SELECT app_name, version_label, arn, description, source_bundle, status, created_at FROM app_versions WHERE 1=1`
	var args []any
	if appName != "" {
		query += ` AND app_name = ?`
		args = append(args, appName)
	}
	if len(labels) > 0 {
		placeholders := make([]byte, 0, len(labels)*2)
		for i, l := range labels {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args = append(args, l)
		}
		query += ` AND version_label IN (` + string(placeholders) + `)`
	}
	query += ` ORDER BY created_at DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var versions []AppVersion
	for rows.Next() {
		v, err := scanAppVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, *v)
	}
	return versions, rows.Err()
}

func (s *Store) UpdateAppVersion(appName, versionLabel, description string) (*AppVersion, error) {
	res, err := s.store.DB().Exec(
		`UPDATE app_versions SET description = ? WHERE app_name = ? AND version_label = ?`,
		description, appName, versionLabel,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errVersionNotFound
	}
	return s.GetAppVersion(appName, versionLabel)
}

func (s *Store) DeleteAppVersion(appName, versionLabel string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM app_versions WHERE app_name = ? AND version_label = ?`,
		appName, versionLabel)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errVersionNotFound
	}
	return nil
}

// --- Environment CRUD ---

func (s *Store) CreateEnvironment(id, name, arn, appName, versionLabel, templateName, solutionStack, tier, description, cname string) (*Environment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO environments (id, name, arn, app_name, version_label, template_name, solution_stack, tier, status, health, cname, endpoint_url, description, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Ready', 'Green', ?, '', ?, ?, ?)`,
		id, name, arn, appName, versionLabel, templateName, solutionStack, tier, cname, description, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Environment{
		ID:            id,
		Name:          name,
		ARN:           arn,
		AppName:       appName,
		VersionLabel:  versionLabel,
		TemplateName:  templateName,
		SolutionStack: solutionStack,
		Tier:          tier,
		Status:        "Ready",
		Health:        "Green",
		CNAME:         cname,
		EndpointURL:   "",
		Description:   description,
		CreatedAt:     time.Unix(now, 0),
		UpdatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetEnvironment(id string) (*Environment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, app_name, version_label, template_name, solution_stack, tier, status, health, cname, endpoint_url, description, created_at, updated_at
		 FROM environments WHERE id = ?`, id)
	return scanEnvironment(row)
}

func (s *Store) GetEnvironmentByName(appName, envName string) (*Environment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, app_name, version_label, template_name, solution_stack, tier, status, health, cname, endpoint_url, description, created_at, updated_at
		 FROM environments WHERE app_name = ? AND name = ?`, appName, envName)
	return scanEnvironment(row)
}

func (s *Store) ListEnvironments(appName string, envNames []string, envIDs []string) ([]Environment, error) {
	query := `SELECT id, name, arn, app_name, version_label, template_name, solution_stack, tier, status, health, cname, endpoint_url, description, created_at, updated_at FROM environments WHERE 1=1`
	var args []any
	if appName != "" {
		query += ` AND app_name = ?`
		args = append(args, appName)
	}
	if len(envNames) > 0 {
		placeholders := make([]byte, 0, len(envNames)*2)
		for i, n := range envNames {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args = append(args, n)
		}
		query += ` AND name IN (` + string(placeholders) + `)`
	}
	if len(envIDs) > 0 {
		placeholders := make([]byte, 0, len(envIDs)*2)
		for i, id := range envIDs {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args = append(args, id)
		}
		query += ` AND id IN (` + string(placeholders) + `)`
	}
	query += ` ORDER BY name`
	rows, err := s.store.DB().Query(query, args...)
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

func (s *Store) UpdateEnvironment(id, versionLabel, description string) (*Environment, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE environments SET version_label = ?, description = ?, updated_at = ? WHERE id = ?`,
		versionLabel, description, now, id,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errEnvNotFound
	}
	return s.GetEnvironment(id)
}

func (s *Store) TerminateEnvironment(id string) (*Environment, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE environments SET status = 'Terminated', updated_at = ? WHERE id = ?`, now, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errEnvNotFound
	}
	return s.GetEnvironment(id)
}

// --- ConfigTemplate CRUD ---

func (s *Store) CreateConfigTemplate(appName, name, description, solutionStack, options string) (*ConfigTemplate, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO config_templates (app_name, name, description, solution_stack, options) VALUES (?, ?, ?, ?, ?)`,
		appName, name, description, solutionStack, options,
	)
	if err != nil {
		return nil, err
	}
	return &ConfigTemplate{
		AppName:       appName,
		Name:          name,
		Description:   description,
		SolutionStack: solutionStack,
		Options:       options,
	}, nil
}

func (s *Store) GetConfigTemplate(appName, name string) (*ConfigTemplate, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_name, name, description, solution_stack, options FROM config_templates WHERE app_name = ? AND name = ?`,
		appName, name)
	return scanConfigTemplate(row)
}

func (s *Store) ListConfigTemplates(appName string) ([]ConfigTemplate, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_name, name, description, solution_stack, options FROM config_templates WHERE app_name = ? ORDER BY name`,
		appName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var templates []ConfigTemplate
	for rows.Next() {
		t, err := scanConfigTemplate(rows)
		if err != nil {
			return nil, err
		}
		templates = append(templates, *t)
	}
	return templates, rows.Err()
}

func (s *Store) UpdateConfigTemplate(appName, name, description, options string) (*ConfigTemplate, error) {
	res, err := s.store.DB().Exec(
		`UPDATE config_templates SET description = ?, options = ? WHERE app_name = ? AND name = ?`,
		description, options, appName, name,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errTemplateNotFound
	}
	return s.GetConfigTemplate(appName, name)
}

func (s *Store) DeleteConfigTemplate(appName, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM config_templates WHERE app_name = ? AND name = ?`, appName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTemplateNotFound
	}
	return nil
}

// --- helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanApplication(s scanner) (*Application, error) {
	var a Application
	var createdAt, updatedAt int64
	err := s.Scan(&a.Name, &a.ARN, &a.Description, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAppNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	a.UpdatedAt = time.Unix(updatedAt, 0)
	return &a, nil
}

func scanAppVersion(s scanner) (*AppVersion, error) {
	var v AppVersion
	var createdAt int64
	err := s.Scan(&v.AppName, &v.VersionLabel, &v.ARN, &v.Description, &v.SourceBundle, &v.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errVersionNotFound
		}
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

func scanEnvironment(s scanner) (*Environment, error) {
	var e Environment
	var createdAt, updatedAt int64
	err := s.Scan(&e.ID, &e.Name, &e.ARN, &e.AppName, &e.VersionLabel, &e.TemplateName,
		&e.SolutionStack, &e.Tier, &e.Status, &e.Health, &e.CNAME, &e.EndpointURL, &e.Description,
		&createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEnvNotFound
		}
		return nil, err
	}
	e.CreatedAt = time.Unix(createdAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

func scanConfigTemplate(s scanner) (*ConfigTemplate, error) {
	var t ConfigTemplate
	err := s.Scan(&t.AppName, &t.Name, &t.Description, &t.SolutionStack, &t.Options)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTemplateNotFound
		}
		return nil, err
	}
	return &t, nil
}
