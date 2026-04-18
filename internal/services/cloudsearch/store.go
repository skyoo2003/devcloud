// SPDX-License-Identifier: Apache-2.0

package cloudsearch

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errDomainNotFound = errors.New("domain not found")
var errFieldNotFound = errors.New("index field not found")
var errConfigNotFound = errors.New("config not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS domains (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			domain_id       TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'ACTIVE',
			search_endpoint TEXT NOT NULL DEFAULT '',
			doc_endpoint    TEXT NOT NULL DEFAULT '',
			created         INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS index_fields (
			domain_name     TEXT NOT NULL,
			name            TEXT NOT NULL,
			type            TEXT NOT NULL DEFAULT 'text',
			config          TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (domain_name, name)
		);
		CREATE TABLE IF NOT EXISTS domain_configs (
			domain_name     TEXT NOT NULL,
			config_type     TEXT NOT NULL,
			name            TEXT NOT NULL,
			data            TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (domain_name, config_type, name)
		);
	`},
}

// Domain represents a CloudSearch domain.
type Domain struct {
	Name           string
	ARN            string
	DomainID       string
	Status         string
	SearchEndpoint string
	DocEndpoint    string
	Created        time.Time
}

// IndexField represents an index field definition.
type IndexField struct {
	DomainName string
	Name       string
	Type       string
	Config     string
}

// DomainConfig stores generic config blobs (expressions, suggesters, analysis schemes).
type DomainConfig struct {
	DomainName string
	ConfigType string
	Name       string
	Data       string
}

// Store provides CloudSearch data persistence.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the SQLite database for CloudSearch.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "cloudsearch.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

// Close releases the database connection.
func (s *Store) Close() error { return s.store.Close() }

// --- Domain CRUD ---

// CreateDomain inserts a new domain record.
func (s *Store) CreateDomain(name, arn, domainID string) (*Domain, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (name, arn, domain_id, status, search_endpoint, doc_endpoint, created)
		 VALUES (?, ?, ?, 'ACTIVE', '', '', ?)`,
		name, arn, domainID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Domain{
		Name:     name,
		ARN:      arn,
		DomainID: domainID,
		Status:   "ACTIVE",
		Created:  time.Unix(now, 0),
	}, nil
}

// GetDomain fetches a domain by name.
func (s *Store) GetDomain(name string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, domain_id, status, search_endpoint, doc_endpoint, created
		 FROM domains WHERE name = ?`, name)
	return scanDomain(row)
}

// ListDomains returns all domains, optionally filtered by names.
func (s *Store) ListDomains(names []string) ([]Domain, error) {
	query := `SELECT name, arn, domain_id, status, search_endpoint, doc_endpoint, created FROM domains`
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

// DeleteDomain removes a domain and its associated records.
func (s *Store) DeleteDomain(name string) (*Domain, error) {
	d, err := s.GetDomain(name)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM index_fields WHERE domain_name = ?`, name); err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM domain_configs WHERE domain_name = ?`, name); err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM domains WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errDomainNotFound
	}
	return d, nil
}

// --- IndexField CRUD ---

// DefineIndexField upserts an index field for a domain.
func (s *Store) DefineIndexField(domainName, fieldName, fieldType, config string) (*IndexField, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO index_fields (domain_name, name, type, config) VALUES (?, ?, ?, ?)
		 ON CONFLICT(domain_name, name) DO UPDATE SET type=excluded.type, config=excluded.config`,
		domainName, fieldName, fieldType, config,
	)
	if err != nil {
		return nil, err
	}
	return &IndexField{DomainName: domainName, Name: fieldName, Type: fieldType, Config: config}, nil
}

// ListIndexFields returns all index fields for a domain.
func (s *Store) ListIndexFields(domainName string, fieldNames []string) ([]IndexField, error) {
	query := `SELECT domain_name, name, type, config FROM index_fields WHERE domain_name = ?`
	args := []any{domainName}
	if len(fieldNames) > 0 {
		placeholders := make([]byte, 0, len(fieldNames)*2)
		for i, n := range fieldNames {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args = append(args, n)
		}
		query += ` AND name IN (` + string(placeholders) + `)`
	}
	query += ` ORDER BY name`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var fields []IndexField
	for rows.Next() {
		var f IndexField
		if err := rows.Scan(&f.DomainName, &f.Name, &f.Type, &f.Config); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	return fields, rows.Err()
}

// DeleteIndexField removes a single index field.
func (s *Store) DeleteIndexField(domainName, fieldName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM index_fields WHERE domain_name = ? AND name = ?`, domainName, fieldName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFieldNotFound
	}
	return nil
}

// --- Generic domain config blobs (expressions, suggesters, analysis schemes) ---

// SetDomainConfig upserts a named config blob for a domain.
func (s *Store) SetDomainConfig(domainName, configType, name, data string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO domain_configs (domain_name, config_type, name, data) VALUES (?, ?, ?, ?)
		 ON CONFLICT(domain_name, config_type, name) DO UPDATE SET data=excluded.data`,
		domainName, configType, name, data,
	)
	return err
}

// ListDomainConfigs returns all config blobs of a given type for a domain.
func (s *Store) ListDomainConfigs(domainName, configType string) ([]DomainConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT domain_name, config_type, name, data FROM domain_configs
		 WHERE domain_name = ? AND config_type = ? ORDER BY name`,
		domainName, configType,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cfgs []DomainConfig
	for rows.Next() {
		var c DomainConfig
		if err := rows.Scan(&c.DomainName, &c.ConfigType, &c.Name, &c.Data); err != nil {
			return nil, err
		}
		cfgs = append(cfgs, c)
	}
	return cfgs, rows.Err()
}

// DeleteDomainConfig removes a named config blob.
func (s *Store) DeleteDomainConfig(domainName, configType, name string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM domain_configs WHERE domain_name = ? AND config_type = ? AND name = ?`,
		domainName, configType, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigNotFound
	}
	return nil
}

// --- helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanDomain(s scanner) (*Domain, error) {
	var d Domain
	var created int64
	err := s.Scan(&d.Name, &d.ARN, &d.DomainID, &d.Status, &d.SearchEndpoint, &d.DocEndpoint, &created)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNotFound
		}
		return nil, err
	}
	d.Created = time.Unix(created, 0)
	return &d, nil
}
