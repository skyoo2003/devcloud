// SPDX-License-Identifier: Apache-2.0

// internal/services/kinesisanalyticsv2/store.go
package kinesisanalyticsv2

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errAppNotFound = errors.New("application not found")
var errSnapshotNotFound = errors.New("snapshot not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS applications (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'READY',
			runtime         TEXT NOT NULL DEFAULT 'FLINK-1_18',
			service_role    TEXT NOT NULL DEFAULT '',
			description     TEXT NOT NULL DEFAULT '',
			version_id      INTEGER NOT NULL DEFAULT 1,
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS application_snapshots (
			name            TEXT NOT NULL,
			app_name        TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'READY',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (name, app_name)
		);
	`},
}

type Application struct {
	Name        string
	ARN         string
	Status      string
	Runtime     string
	ServiceRole string
	Description string
	VersionID   int64
	Config      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type ApplicationSnapshot struct {
	Name      string
	AppName   string
	Status    string
	CreatedAt time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "kinesisanalyticsv2.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateApplication(name, arn, runtime, serviceRole, description, config string) (*Application, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO applications (name, arn, runtime, service_role, description, config, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		name, arn, runtime, serviceRole, description, config, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Application{
		Name: name, ARN: arn, Status: "READY", Runtime: runtime,
		ServiceRole: serviceRole, Description: description, VersionID: 1,
		Config: config, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetApplication(name string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, runtime, service_role, description, version_id, config, created_at, updated_at
         FROM applications WHERE name = ?`, name)
	return scanApplication(row)
}

func (s *Store) ListApplications() ([]Application, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, runtime, service_role, description, version_id, config, created_at, updated_at
         FROM applications ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var apps []Application
	for rows.Next() {
		app, err := scanApplication(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *app)
	}
	return apps, rows.Err()
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

func (s *Store) UpdateApplicationStatus(name, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE applications SET status = ?, updated_at = ? WHERE name = ?`, status, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

func (s *Store) UpdateApplicationConfig(name, config string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE applications SET config = ?, version_id = version_id + 1, updated_at = ? WHERE name = ?`,
		config, now, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

func (s *Store) CreateSnapshot(snapName, appName string) (*ApplicationSnapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO application_snapshots (name, app_name, created_at) VALUES (?, ?, ?)`,
		snapName, appName, now,
	)
	if err != nil {
		return nil, err
	}
	return &ApplicationSnapshot{
		Name: snapName, AppName: appName, Status: "READY", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSnapshot(appName, snapName string) (*ApplicationSnapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, app_name, status, created_at FROM application_snapshots WHERE name = ? AND app_name = ?`,
		snapName, appName)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(appName string) ([]ApplicationSnapshot, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, app_name, status, created_at FROM application_snapshots WHERE app_name = ? ORDER BY created_at`,
		appName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snaps []ApplicationSnapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *snap)
	}
	return snaps, rows.Err()
}

func (s *Store) DeleteSnapshot(appName, snapName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM application_snapshots WHERE name = ? AND app_name = ?`, snapName, appName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSnapshotNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanApplication(s scanner) (*Application, error) {
	var app Application
	var createdAt, updatedAt int64
	err := s.Scan(&app.Name, &app.ARN, &app.Status, &app.Runtime, &app.ServiceRole,
		&app.Description, &app.VersionID, &app.Config, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAppNotFound
		}
		return nil, err
	}
	app.CreatedAt = time.Unix(createdAt, 0)
	app.UpdatedAt = time.Unix(updatedAt, 0)
	return &app, nil
}

func scanSnapshot(s scanner) (*ApplicationSnapshot, error) {
	var snap ApplicationSnapshot
	var createdAt int64
	err := s.Scan(&snap.Name, &snap.AppName, &snap.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	snap.CreatedAt = time.Unix(createdAt, 0)
	return &snap, nil
}
