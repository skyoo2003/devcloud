// SPDX-License-Identifier: Apache-2.0

// internal/services/elasticsearchservice/store.go
package elasticsearchservice

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errDomainNotFound = errors.New("domain not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS domains (
            name            TEXT PRIMARY KEY,
            arn             TEXT NOT NULL UNIQUE,
            domain_id       TEXT NOT NULL UNIQUE,
            status          TEXT NOT NULL DEFAULT 'Active',
            engine_version  TEXT NOT NULL DEFAULT 'Elasticsearch_7.10',
            endpoint        TEXT NOT NULL DEFAULT '',
            instance_type   TEXT NOT NULL DEFAULT 'm5.large.elasticsearch',
            instance_count  INTEGER NOT NULL DEFAULT 1,
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
	InstanceType  string
	InstanceCount int
	Config        string
	CreatedAt     time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "elasticsearchservice.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateDomain(name, arn, domainID, engineVersion, instanceType string, instanceCount int) (*Domain, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (name, arn, domain_id, engine_version, instance_type, instance_count, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, arn, domainID, engineVersion, instanceType, instanceCount, now,
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
		InstanceType:  instanceType,
		InstanceCount: instanceCount,
		Config:        "{}",
		CreatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetDomain(name string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, domain_id, status, engine_version, endpoint, instance_type, instance_count, config, created_at
         FROM domains WHERE name = ?`, name)
	return scanDomain(row)
}

func (s *Store) ListDomains() ([]Domain, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, domain_id, status, engine_version, endpoint, instance_type, instance_count, config, created_at
         FROM domains ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

type scanner interface{ Scan(dest ...any) error }

func scanDomain(s scanner) (*Domain, error) {
	var d Domain
	var createdAt int64
	err := s.Scan(
		&d.Name, &d.ARN, &d.DomainID, &d.Status, &d.EngineVersion,
		&d.Endpoint, &d.InstanceType, &d.InstanceCount, &d.Config, &createdAt,
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
