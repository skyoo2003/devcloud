// SPDX-License-Identifier: Apache-2.0

// internal/services/codedeploy/store.go
package codedeploy

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errAppNotFound    = errors.New("application not found")
	errGroupNotFound  = errors.New("deployment group not found")
	errDeployNotFound = errors.New("deployment not found")
	errConfigNotFound = errors.New("deployment config not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS applications (
			name             TEXT PRIMARY KEY,
			id               TEXT NOT NULL UNIQUE,
			compute_platform TEXT NOT NULL DEFAULT 'Server',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS deployment_groups (
			id                TEXT PRIMARY KEY,
			app_name          TEXT NOT NULL,
			name              TEXT NOT NULL,
			service_role      TEXT NOT NULL DEFAULT '',
			deployment_config TEXT NOT NULL DEFAULT 'CodeDeployDefault.OneAtATime',
			auto_rollback     TEXT NOT NULL DEFAULT '{}',
			deployment_style  TEXT NOT NULL DEFAULT '{}',
			UNIQUE(app_name, name)
		);
		CREATE TABLE IF NOT EXISTS deployments (
			id           TEXT PRIMARY KEY,
			app_name     TEXT NOT NULL,
			group_name   TEXT NOT NULL,
			status       TEXT NOT NULL DEFAULT 'Succeeded',
			revision     TEXT NOT NULL DEFAULT '{}',
			description  TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS deployment_configs (
			name             TEXT PRIMARY KEY,
			compute_platform TEXT NOT NULL DEFAULT 'Server',
			min_healthy      TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL
		);
	`},
}

// ---- model structs ----

type Application struct {
	Name            string
	ID              string
	ComputePlatform string
	CreatedAt       time.Time
}

type DeploymentGroup struct {
	ID               string
	AppName          string
	Name             string
	ServiceRole      string
	DeploymentConfig string
	AutoRollback     string
	DeploymentStyle  string
}

type Deployment struct {
	ID          string
	AppName     string
	GroupName   string
	Status      string
	Revision    string
	Description string
	CreatedAt   time.Time
	CompletedAt int64
}

type DeploymentConfig struct {
	Name            string
	ComputePlatform string
	MinHealthy      string
	CreatedAt       time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "codedeploy.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Application ----

func (s *Store) CreateApplication(name, id, platform string) (*Application, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO applications (name, id, compute_platform, created_at) VALUES (?, ?, ?, ?)`,
		name, id, platform, now,
	)
	if err != nil {
		return nil, err
	}
	return &Application{Name: name, ID: id, ComputePlatform: platform, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetApplication(name string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, id, compute_platform, created_at FROM applications WHERE name = ?`, name)
	return scanApplication(row)
}

func (s *Store) ListApplications() ([]Application, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, id, compute_platform, created_at FROM applications ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *Store) UpdateApplication(oldName, newName string) error {
	res, err := s.store.DB().Exec(
		`UPDATE applications SET name = ? WHERE name = ?`, newName, oldName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

func (s *Store) DeleteApplication(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM applications WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

// ---- DeploymentGroup ----

func (s *Store) CreateDeploymentGroup(id, appName, name, serviceRole, deployConfig, autoRollback, deployStyle string) (*DeploymentGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO deployment_groups (id, app_name, name, service_role, deployment_config, auto_rollback, deployment_style)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, appName, name, serviceRole, deployConfig, autoRollback, deployStyle,
	)
	if err != nil {
		return nil, err
	}
	return &DeploymentGroup{
		ID: id, AppName: appName, Name: name, ServiceRole: serviceRole,
		DeploymentConfig: deployConfig, AutoRollback: autoRollback, DeploymentStyle: deployStyle,
	}, nil
}

func (s *Store) GetDeploymentGroup(appName, name string) (*DeploymentGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_name, name, service_role, deployment_config, auto_rollback, deployment_style
		 FROM deployment_groups WHERE app_name = ? AND name = ?`, appName, name)
	return scanDeploymentGroup(row)
}

func (s *Store) GetDeploymentGroupByID(id string) (*DeploymentGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_name, name, service_role, deployment_config, auto_rollback, deployment_style
		 FROM deployment_groups WHERE id = ?`, id)
	return scanDeploymentGroup(row)
}

func (s *Store) ListDeploymentGroups(appName string) ([]DeploymentGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_name, name, service_role, deployment_config, auto_rollback, deployment_style
		 FROM deployment_groups WHERE app_name = ? ORDER BY name`, appName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []DeploymentGroup
	for rows.Next() {
		g, err := scanDeploymentGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *Store) UpdateDeploymentGroup(appName, name, serviceRole, deployConfig, autoRollback, deployStyle string) error {
	res, err := s.store.DB().Exec(
		`UPDATE deployment_groups SET service_role = ?, deployment_config = ?, auto_rollback = ?, deployment_style = ?
		 WHERE app_name = ? AND name = ?`,
		serviceRole, deployConfig, autoRollback, deployStyle, appName, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

func (s *Store) DeleteDeploymentGroup(appName, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM deployment_groups WHERE app_name = ? AND name = ?`, appName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

// ---- Deployment ----

func (s *Store) CreateDeployment(id, appName, groupName, status, revision, description string) (*Deployment, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO deployments (id, app_name, group_name, status, revision, description, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, appName, groupName, status, revision, description, now,
	)
	if err != nil {
		return nil, err
	}
	return &Deployment{
		ID: id, AppName: appName, GroupName: groupName, Status: status,
		Revision: revision, Description: description, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetDeployment(id string) (*Deployment, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_name, group_name, status, revision, description, created_at, completed_at
		 FROM deployments WHERE id = ?`, id)
	return scanDeployment(row)
}

func (s *Store) ListDeployments(appName, groupName string) ([]Deployment, error) {
	query := `SELECT id, app_name, group_name, status, revision, description, created_at, completed_at FROM deployments`
	var args []any
	if appName != "" && groupName != "" {
		query += ` WHERE app_name = ? AND group_name = ?`
		args = append(args, appName, groupName)
	} else if appName != "" {
		query += ` WHERE app_name = ?`
		args = append(args, appName)
	}
	query += ` ORDER BY created_at DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deployments []Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		deployments = append(deployments, *d)
	}
	return deployments, rows.Err()
}

func (s *Store) UpdateDeploymentStatus(id, status string) error {
	completedAt := int64(0)
	if status == "Stopped" || status == "Failed" || status == "Succeeded" {
		completedAt = time.Now().Unix()
	}
	res, err := s.store.DB().Exec(
		`UPDATE deployments SET status = ?, completed_at = ? WHERE id = ?`,
		status, completedAt, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDeployNotFound
	}
	return nil
}

// ---- DeploymentConfig ----

func (s *Store) CreateDeploymentConfig(name, platform, minHealthy string) (*DeploymentConfig, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO deployment_configs (name, compute_platform, min_healthy, created_at) VALUES (?, ?, ?, ?)`,
		name, platform, minHealthy, now,
	)
	if err != nil {
		return nil, err
	}
	return &DeploymentConfig{Name: name, ComputePlatform: platform, MinHealthy: minHealthy, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetDeploymentConfig(name string) (*DeploymentConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, compute_platform, min_healthy, created_at FROM deployment_configs WHERE name = ?`, name)
	return scanDeploymentConfig(row)
}

func (s *Store) ListDeploymentConfigs() ([]DeploymentConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, compute_platform, min_healthy, created_at FROM deployment_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cfgs []DeploymentConfig
	for rows.Next() {
		c, err := scanDeploymentConfig(rows)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, *c)
	}
	return cfgs, rows.Err()
}

func (s *Store) DeleteDeploymentConfig(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM deployment_configs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigNotFound
	}
	return nil
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanApplication(s scanner) (*Application, error) {
	var a Application
	var createdAt int64
	err := s.Scan(&a.Name, &a.ID, &a.ComputePlatform, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAppNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanDeploymentGroup(s scanner) (*DeploymentGroup, error) {
	var g DeploymentGroup
	err := s.Scan(&g.ID, &g.AppName, &g.Name, &g.ServiceRole, &g.DeploymentConfig, &g.AutoRollback, &g.DeploymentStyle)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupNotFound
		}
		return nil, err
	}
	return &g, nil
}

func scanDeployment(s scanner) (*Deployment, error) {
	var d Deployment
	var createdAt int64
	err := s.Scan(&d.ID, &d.AppName, &d.GroupName, &d.Status, &d.Revision, &d.Description, &createdAt, &d.CompletedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDeployNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanDeploymentConfig(s scanner) (*DeploymentConfig, error) {
	var c DeploymentConfig
	var createdAt int64
	err := s.Scan(&c.Name, &c.ComputePlatform, &c.MinHealthy, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConfigNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}
