// SPDX-License-Identifier: Apache-2.0

// internal/services/opensearch/store.go
package opensearch

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errDomainNotFound = errors.New("domain not found")
var errAppNotFound = errors.New("application not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS domains (
            name            TEXT PRIMARY KEY,
            arn             TEXT NOT NULL UNIQUE,
            domain_id       TEXT NOT NULL UNIQUE,
            status          TEXT NOT NULL DEFAULT 'Active',
            engine_version  TEXT NOT NULL DEFAULT 'OpenSearch_2.11',
            endpoint        TEXT NOT NULL DEFAULT '',
            config          TEXT NOT NULL DEFAULT '{}',
            created_at      INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applications (
            id              TEXT PRIMARY KEY,
            arn             TEXT NOT NULL UNIQUE,
            name            TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'ACTIVE',
            config          TEXT NOT NULL DEFAULT '{}',
            created_at      INTEGER NOT NULL
        );
    `},
}

type Domain struct {
	Name          string
	ARN           string
	DomainID      string
	Status        string
	EngineVersion string
	Endpoint      string
	Config        string
	CreatedAt     time.Time
}

type Application struct {
	ID        string
	ARN       string
	Name      string
	Status    string
	Config    string
	CreatedAt time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "opensearch.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Domain ---

func (s *Store) CreateDomain(name, arn, domainID, engineVersion string) (*Domain, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (name, arn, domain_id, engine_version, created_at)
         VALUES (?, ?, ?, ?, ?)`,
		name, arn, domainID, engineVersion, now,
	)
	if err != nil {
		return nil, err
	}
	return &Domain{
		Name:          name,
		ARN:           arn,
		DomainID:      domainID,
		Status:        "Active",
		EngineVersion: engineVersion,
		Endpoint:      "",
		Config:        "{}",
		CreatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetDomain(name string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, domain_id, status, engine_version, endpoint, config, created_at
         FROM domains WHERE name = ?`, name)
	return scanDomain(row)
}

func (s *Store) ListDomains() ([]Domain, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, domain_id, status, engine_version, endpoint, config, created_at
         FROM domains ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *Store) DeleteDomain(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM domains WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNotFound
	}
	return nil
}

func (s *Store) UpdateDomainConfig(name, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE domains SET config = ? WHERE name = ?`, config, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainNotFound
	}
	return nil
}

// --- Application ---

func (s *Store) CreateApplication(id, arn, name string) (*Application, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO applications (id, arn, name, created_at) VALUES (?, ?, ?, ?)`,
		id, arn, name, now,
	)
	if err != nil {
		return nil, err
	}
	return &Application{
		ID:        id,
		ARN:       arn,
		Name:      name,
		Status:    "ACTIVE",
		Config:    "{}",
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetApplication(id string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, status, config, created_at FROM applications WHERE id = ?`, id)
	return scanApplication(row)
}

func (s *Store) ListApplications() ([]Application, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, status, config, created_at FROM applications ORDER BY created_at`)
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

func (s *Store) DeleteApplication(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM applications WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

func (s *Store) UpdateApplication(id, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE applications SET config = ? WHERE id = ?`, config, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanDomain(s scanner) (*Domain, error) {
	var d Domain
	var createdAt int64
	err := s.Scan(
		&d.Name, &d.ARN, &d.DomainID, &d.Status, &d.EngineVersion,
		&d.Endpoint, &d.Config, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanApplication(s scanner) (*Application, error) {
	var a Application
	var createdAt int64
	err := s.Scan(&a.ID, &a.ARN, &a.Name, &a.Status, &a.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAppNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}
