// SPDX-License-Identifier: Apache-2.0

// internal/services/codebuild/store.go
package codebuild

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errProjectNotFound     = errors.New("project not found")
	errBuildNotFound       = errors.New("build not found")
	errReportGroupNotFound = errors.New("report group not found")
	errFleetNotFound       = errors.New("fleet not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS projects (
			name         TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			description  TEXT NOT NULL DEFAULT '',
			source       TEXT NOT NULL DEFAULT '{}',
			artifacts    TEXT NOT NULL DEFAULT '{}',
			environment  TEXT NOT NULL DEFAULT '{}',
			service_role TEXT NOT NULL DEFAULT '',
			timeout      INTEGER NOT NULL DEFAULT 60,
			created_at   INTEGER NOT NULL,
			updated_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS builds (
			id             TEXT PRIMARY KEY,
			arn            TEXT NOT NULL UNIQUE,
			project_name   TEXT NOT NULL,
			status         TEXT NOT NULL DEFAULT 'SUCCEEDED',
			source_version TEXT NOT NULL DEFAULT '',
			start_time     INTEGER NOT NULL,
			end_time       INTEGER NOT NULL DEFAULT 0,
			phases         TEXT NOT NULL DEFAULT '[]',
			logs           TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS report_groups (
			arn           TEXT PRIMARY KEY,
			name          TEXT NOT NULL UNIQUE,
			type          TEXT NOT NULL DEFAULT 'TEST',
			export_config TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS fleets (
			arn              TEXT PRIMARY KEY,
			name             TEXT NOT NULL UNIQUE,
			base_capacity    INTEGER NOT NULL DEFAULT 1,
			compute_type     TEXT NOT NULL DEFAULT 'BUILD_GENERAL1_SMALL',
			environment_type TEXT NOT NULL DEFAULT 'LINUX_CONTAINER',
			status           TEXT NOT NULL DEFAULT 'ACTIVE',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS source_credentials (
			arn           TEXT PRIMARY KEY,
			server_type   TEXT NOT NULL,
			auth_type     TEXT NOT NULL,
			token         TEXT NOT NULL DEFAULT ''
		);
	`},
}

// ---- model structs ----

type Project struct {
	Name        string
	ARN         string
	Description string
	Source      string
	Artifacts   string
	Environment string
	ServiceRole string
	Timeout     int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Build struct {
	ID            string
	ARN           string
	ProjectName   string
	Status        string
	SourceVersion string
	StartTime     time.Time
	EndTime       int64
	Phases        string
	Logs          string
}

type ReportGroup struct {
	ARN          string
	Name         string
	Type         string
	ExportConfig string
	CreatedAt    time.Time
}

type Fleet struct {
	ARN             string
	Name            string
	BaseCapacity    int64
	ComputeType     string
	EnvironmentType string
	Status          string
	CreatedAt       time.Time
}

type SourceCredential struct {
	ARN        string
	ServerType string
	AuthType   string
	Token      string
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "codebuild.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Project ----

func (s *Store) CreateProject(name, arn, description, source, artifacts, environment, serviceRole string, timeout int64) (*Project, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO projects (name, arn, description, source, artifacts, environment, service_role, timeout, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, arn, description, source, artifacts, environment, serviceRole, timeout, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Project{
		Name: name, ARN: arn, Description: description, Source: source,
		Artifacts: artifacts, Environment: environment, ServiceRole: serviceRole,
		Timeout: timeout, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetProject(name string) (*Project, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, source, artifacts, environment, service_role, timeout, created_at, updated_at
		 FROM projects WHERE name = ?`, name)
	return scanProject(row)
}

func (s *Store) ListProjects() ([]Project, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, source, artifacts, environment, service_role, timeout, created_at, updated_at
		 FROM projects ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var projects []Project
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		projects = append(projects, *p)
	}
	return projects, rows.Err()
}

func (s *Store) UpdateProject(name, description, source, artifacts, environment, serviceRole string, timeout int64) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE projects SET description = ?, source = ?, artifacts = ?, environment = ?, service_role = ?, timeout = ?, updated_at = ?
		 WHERE name = ?`,
		description, source, artifacts, environment, serviceRole, timeout, now, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProjectNotFound
	}
	return nil
}

func (s *Store) DeleteProject(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM projects WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProjectNotFound
	}
	return nil
}

// ---- Build ----

func (s *Store) CreateBuild(id, arn, projectName, status, sourceVersion string) (*Build, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO builds (id, arn, project_name, status, source_version, start_time)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, arn, projectName, status, sourceVersion, now,
	)
	if err != nil {
		return nil, err
	}
	return &Build{
		ID: id, ARN: arn, ProjectName: projectName, Status: status,
		SourceVersion: sourceVersion, StartTime: time.Unix(now, 0),
		Phases: "[]", Logs: "{}",
	}, nil
}

func (s *Store) GetBuild(id string) (*Build, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, project_name, status, source_version, start_time, end_time, phases, logs
		 FROM builds WHERE id = ?`, id)
	return scanBuild(row)
}

func (s *Store) ListBuilds() ([]Build, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, project_name, status, source_version, start_time, end_time, phases, logs
		 FROM builds ORDER BY start_time DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var builds []Build
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, err
		}
		builds = append(builds, *b)
	}
	return builds, rows.Err()
}

func (s *Store) ListBuildsForProject(projectName string) ([]Build, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, project_name, status, source_version, start_time, end_time, phases, logs
		 FROM builds WHERE project_name = ? ORDER BY start_time DESC`, projectName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var builds []Build
	for rows.Next() {
		b, err := scanBuild(rows)
		if err != nil {
			return nil, err
		}
		builds = append(builds, *b)
	}
	return builds, rows.Err()
}

func (s *Store) UpdateBuildStatus(id, status string) error {
	endTime := int64(0)
	if status == "STOPPED" || status == "FAILED" || status == "SUCCEEDED" {
		endTime = time.Now().Unix()
	}
	res, err := s.store.DB().Exec(
		`UPDATE builds SET status = ?, end_time = ? WHERE id = ?`, status, endTime, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBuildNotFound
	}
	return nil
}

// ---- ReportGroup ----

func (s *Store) CreateReportGroup(arn, name, rgType, exportConfig string) (*ReportGroup, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO report_groups (arn, name, type, export_config, created_at) VALUES (?, ?, ?, ?, ?)`,
		arn, name, rgType, exportConfig, now,
	)
	if err != nil {
		return nil, err
	}
	return &ReportGroup{ARN: arn, Name: name, Type: rgType, ExportConfig: exportConfig, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetReportGroup(arn string) (*ReportGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, type, export_config, created_at FROM report_groups WHERE arn = ?`, arn)
	return scanReportGroup(row)
}

func (s *Store) GetReportGroupByName(name string) (*ReportGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, type, export_config, created_at FROM report_groups WHERE name = ?`, name)
	return scanReportGroup(row)
}

func (s *Store) ListReportGroups() ([]ReportGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, type, export_config, created_at FROM report_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []ReportGroup
	for rows.Next() {
		g, err := scanReportGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *Store) UpdateReportGroup(arn, exportConfig string) error {
	res, err := s.store.DB().Exec(
		`UPDATE report_groups SET export_config = ? WHERE arn = ?`, exportConfig, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReportGroupNotFound
	}
	return nil
}

func (s *Store) DeleteReportGroup(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM report_groups WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReportGroupNotFound
	}
	return nil
}

// ---- Fleet ----

func (s *Store) CreateFleet(arn, name string, baseCapacity int64, computeType, envType string) (*Fleet, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO fleets (arn, name, base_capacity, compute_type, environment_type, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, baseCapacity, computeType, envType, now,
	)
	if err != nil {
		return nil, err
	}
	return &Fleet{
		ARN: arn, Name: name, BaseCapacity: baseCapacity, ComputeType: computeType,
		EnvironmentType: envType, Status: "ACTIVE", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetFleet(arn string) (*Fleet, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, base_capacity, compute_type, environment_type, status, created_at
		 FROM fleets WHERE arn = ?`, arn)
	return scanFleet(row)
}

func (s *Store) GetFleetByName(name string) (*Fleet, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, base_capacity, compute_type, environment_type, status, created_at
		 FROM fleets WHERE name = ?`, name)
	return scanFleet(row)
}

func (s *Store) ListFleets() ([]Fleet, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, base_capacity, compute_type, environment_type, status, created_at
		 FROM fleets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fleets []Fleet
	for rows.Next() {
		f, err := scanFleet(rows)
		if err != nil {
			return nil, err
		}
		fleets = append(fleets, *f)
	}
	return fleets, rows.Err()
}

func (s *Store) UpdateFleet(arn string, baseCapacity int64, computeType, envType string) error {
	res, err := s.store.DB().Exec(
		`UPDATE fleets SET base_capacity = ?, compute_type = ?, environment_type = ? WHERE arn = ?`,
		baseCapacity, computeType, envType, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFleetNotFound
	}
	return nil
}

func (s *Store) DeleteFleet(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM fleets WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFleetNotFound
	}
	return nil
}

// ---- SourceCredentials ----

func (s *Store) UpsertSourceCredential(arn, serverType, authType, token string) (*SourceCredential, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO source_credentials (arn, server_type, auth_type, token) VALUES (?, ?, ?, ?)
		 ON CONFLICT(arn) DO UPDATE SET token=excluded.token`,
		arn, serverType, authType, token,
	)
	if err != nil {
		return nil, err
	}
	return &SourceCredential{ARN: arn, ServerType: serverType, AuthType: authType, Token: token}, nil
}

func (s *Store) ListSourceCredentials() ([]SourceCredential, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, server_type, auth_type, token FROM source_credentials ORDER BY server_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var creds []SourceCredential
	for rows.Next() {
		var c SourceCredential
		if err := rows.Scan(&c.ARN, &c.ServerType, &c.AuthType, &c.Token); err != nil {
			return nil, err
		}
		creds = append(creds, c)
	}
	return creds, rows.Err()
}

func (s *Store) DeleteSourceCredential(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM source_credentials WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("source credential not found")
	}
	return nil
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanProject(s scanner) (*Project, error) {
	var p Project
	var createdAt, updatedAt int64
	err := s.Scan(&p.Name, &p.ARN, &p.Description, &p.Source, &p.Artifacts, &p.Environment, &p.ServiceRole, &p.Timeout, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errProjectNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	p.UpdatedAt = time.Unix(updatedAt, 0)
	return &p, nil
}

func scanBuild(s scanner) (*Build, error) {
	var b Build
	var startTime int64
	err := s.Scan(&b.ID, &b.ARN, &b.ProjectName, &b.Status, &b.SourceVersion, &startTime, &b.EndTime, &b.Phases, &b.Logs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBuildNotFound
		}
		return nil, err
	}
	b.StartTime = time.Unix(startTime, 0)
	return &b, nil
}

func scanReportGroup(s scanner) (*ReportGroup, error) {
	var g ReportGroup
	var createdAt int64
	err := s.Scan(&g.ARN, &g.Name, &g.Type, &g.ExportConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReportGroupNotFound
		}
		return nil, err
	}
	g.CreatedAt = time.Unix(createdAt, 0)
	return &g, nil
}

func scanFleet(s scanner) (*Fleet, error) {
	var f Fleet
	var createdAt int64
	err := s.Scan(&f.ARN, &f.Name, &f.BaseCapacity, &f.ComputeType, &f.EnvironmentType, &f.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errFleetNotFound
		}
		return nil, err
	}
	f.CreatedAt = time.Unix(createdAt, 0)
	return &f, nil
}
