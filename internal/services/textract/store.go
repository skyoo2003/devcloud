// SPDX-License-Identifier: Apache-2.0

// internal/services/textract/store.go
package textract

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errAdapterNotFound = errors.New("adapter not found")
	errVersionNotFound = errors.New("adapter version not found")
	errJobNotFound     = errors.New("job not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS adapters (
			id           TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			name         TEXT NOT NULL,
			feature_types TEXT NOT NULL DEFAULT '[]',
			auto_update  TEXT NOT NULL DEFAULT 'ENABLED',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS adapter_versions (
			adapter_id     TEXT NOT NULL,
			version        TEXT NOT NULL,
			status         TEXT NOT NULL DEFAULT 'ACTIVE',
			dataset_config TEXT NOT NULL DEFAULT '{}',
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (adapter_id, version)
		);
		CREATE TABLE IF NOT EXISTS async_jobs (
			id         TEXT PRIMARY KEY,
			type       TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'SUCCEEDED',
			created_at INTEGER NOT NULL
		);
	`},
}

// Adapter represents a Textract adapter record.
type Adapter struct {
	ID           string
	ARN          string
	Name         string
	FeatureTypes string
	AutoUpdate   string
	CreatedAt    time.Time
}

// AdapterVersion represents a version of a Textract adapter.
type AdapterVersion struct {
	AdapterID     string
	Version       string
	Status        string
	DatasetConfig string
	CreatedAt     time.Time
}

// AsyncJob represents an async Textract job.
type AsyncJob struct {
	ID        string
	Type      string
	Status    string
	CreatedAt time.Time
}

// Store manages SQLite persistence for Textract.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "textract.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Adapter CRUD ---

func (s *Store) CreateAdapter(id, arn, name, featureTypes, autoUpdate string) (*Adapter, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO adapters (id, arn, name, feature_types, auto_update, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, arn, name, featureTypes, autoUpdate, now,
	)
	if err != nil {
		return nil, err
	}
	return &Adapter{
		ID: id, ARN: arn, Name: name,
		FeatureTypes: featureTypes, AutoUpdate: autoUpdate,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAdapter(id string) (*Adapter, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, feature_types, auto_update, created_at FROM adapters WHERE id = ?`, id)
	return scanAdapter(row)
}

func (s *Store) ListAdapters() ([]Adapter, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, feature_types, auto_update, created_at FROM adapters ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var adapters []Adapter
	for rows.Next() {
		a, err := scanAdapter(rows)
		if err != nil {
			return nil, err
		}
		adapters = append(adapters, *a)
	}
	return adapters, rows.Err()
}

func (s *Store) UpdateAdapter(id, autoUpdate, featureTypes string) error {
	res, err := s.store.DB().Exec(
		`UPDATE adapters SET auto_update = ?, feature_types = ? WHERE id = ?`,
		autoUpdate, featureTypes, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAdapterNotFound
	}
	return nil
}

func (s *Store) DeleteAdapter(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM adapters WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAdapterNotFound
	}
	return nil
}

// --- AdapterVersion CRUD ---

func (s *Store) CreateAdapterVersion(adapterID, version, datasetConfig string) (*AdapterVersion, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO adapter_versions (adapter_id, version, dataset_config, created_at) VALUES (?, ?, ?, ?)`,
		adapterID, version, datasetConfig, now,
	)
	if err != nil {
		return nil, err
	}
	return &AdapterVersion{
		AdapterID: adapterID, Version: version, Status: "ACTIVE",
		DatasetConfig: datasetConfig, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAdapterVersion(adapterID, version string) (*AdapterVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT adapter_id, version, status, dataset_config, created_at
		 FROM adapter_versions WHERE adapter_id = ? AND version = ?`,
		adapterID, version,
	)
	return scanAdapterVersion(row)
}

func (s *Store) ListAdapterVersions(adapterID string) ([]AdapterVersion, error) {
	rows, err := s.store.DB().Query(
		`SELECT adapter_id, version, status, dataset_config, created_at
		 FROM adapter_versions WHERE adapter_id = ? ORDER BY created_at`,
		adapterID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []AdapterVersion
	for rows.Next() {
		v, err := scanAdapterVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, *v)
	}
	return versions, rows.Err()
}

func (s *Store) DeleteAdapterVersion(adapterID, version string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM adapter_versions WHERE adapter_id = ? AND version = ?`, adapterID, version)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errVersionNotFound
	}
	return nil
}

// --- AsyncJob ---

func (s *Store) CreateJob(id, jobType string) (*AsyncJob, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO async_jobs (id, type, created_at) VALUES (?, ?, ?)`,
		id, jobType, now,
	)
	if err != nil {
		return nil, err
	}
	return &AsyncJob{ID: id, Type: jobType, Status: "SUCCEEDED", CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetJob(id string) (*AsyncJob, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, type, status, created_at FROM async_jobs WHERE id = ?`, id)
	return scanJob(row)
}

// --- scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanAdapter(sc scanner) (*Adapter, error) {
	var a Adapter
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.FeatureTypes, &a.AutoUpdate, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAdapterNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanAdapterVersion(sc scanner) (*AdapterVersion, error) {
	var v AdapterVersion
	var createdAt int64
	err := sc.Scan(&v.AdapterID, &v.Version, &v.Status, &v.DatasetConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errVersionNotFound
		}
		return nil, err
	}
	v.CreatedAt = time.Unix(createdAt, 0)
	return &v, nil
}

func scanJob(sc scanner) (*AsyncJob, error) {
	var j AsyncJob
	var createdAt int64
	err := sc.Scan(&j.ID, &j.Type, &j.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errJobNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	return &j, nil
}
