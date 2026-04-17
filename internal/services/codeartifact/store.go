// SPDX-License-Identifier: Apache-2.0

// internal/services/codeartifact/store.go
package codeartifact

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errDomainNotFound = errors.New("domain not found")
var errRepositoryNotFound = errors.New("repository not found")
var errPackageNotFound = errors.New("package not found")
var errPackageGroupNotFound = errors.New("package group not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS domains (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			owner           TEXT NOT NULL DEFAULT '000000000000',
			status          TEXT NOT NULL DEFAULT 'Active',
			encryption_key  TEXT NOT NULL DEFAULT '',
			repo_count      INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS repositories (
			name            TEXT NOT NULL,
			domain_name     TEXT NOT NULL,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			upstreams       TEXT NOT NULL DEFAULT '[]',
			external_conns  TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (name, domain_name)
		);
		CREATE TABLE IF NOT EXISTS packages (
			name            TEXT NOT NULL,
			namespace       TEXT NOT NULL DEFAULT '',
			format          TEXT NOT NULL DEFAULT 'npm',
			domain_name     TEXT NOT NULL,
			repo_name       TEXT NOT NULL,
			origin_config   TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (name, namespace, format, domain_name, repo_name)
		);
		CREATE TABLE IF NOT EXISTS package_groups (
			arn             TEXT PRIMARY KEY,
			pattern         TEXT NOT NULL,
			domain_name     TEXT NOT NULL,
			description     TEXT NOT NULL DEFAULT '',
			origin_config   TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
	`},
}

// Domain holds a stored domain.
type Domain struct {
	Name          string
	ARN           string
	Owner         string
	Status        string
	EncryptionKey string
	RepoCount     int
	CreatedAt     time.Time
}

// Repository holds a stored repository.
type Repository struct {
	Name          string
	DomainName    string
	ARN           string
	Description   string
	Upstreams     string // JSON
	ExternalConns string // JSON
	CreatedAt     time.Time
}

// Package holds a stored package.
type Package struct {
	Name         string
	Namespace    string
	Format       string
	DomainName   string
	RepoName     string
	OriginConfig string // JSON
}

// PackageGroup holds a stored package group.
type PackageGroup struct {
	ARN          string
	Pattern      string
	DomainName   string
	Description  string
	OriginConfig string // JSON
	CreatedAt    time.Time
}

// Store is the CodeArtifact SQLite store.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the CodeArtifact SQLite database.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "codeartifact.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Domain ---

func (s *Store) CreateDomain(d *Domain) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domains (name, arn, owner, status, encryption_key, repo_count, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		d.Name, d.ARN, d.Owner, d.Status, d.EncryptionKey, d.RepoCount, now,
	)
	return err
}

func (s *Store) GetDomain(name string) (*Domain, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, owner, status, encryption_key, repo_count, created_at
		 FROM domains WHERE name = ?`, name)
	return scanDomain(row)
}

func (s *Store) ListDomains() ([]Domain, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, owner, status, encryption_key, repo_count, created_at
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

func (s *Store) IncrementDomainRepoCount(domainName string, delta int) error {
	_, err := s.store.DB().Exec(
		`UPDATE domains SET repo_count = repo_count + ? WHERE name = ?`, delta, domainName)
	return err
}

// --- Repository ---

func (s *Store) CreateRepository(r *Repository) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO repositories (name, domain_name, arn, description, upstreams, external_conns, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.Name, r.DomainName, r.ARN, r.Description, r.Upstreams, r.ExternalConns, now,
	)
	return err
}

func (s *Store) GetRepository(name, domainName string) (*Repository, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, domain_name, arn, description, upstreams, external_conns, created_at
		 FROM repositories WHERE name = ? AND domain_name = ?`, name, domainName)
	return scanRepository(row)
}

func (s *Store) GetRepositoryByARN(arn string) (*Repository, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, domain_name, arn, description, upstreams, external_conns, created_at
		 FROM repositories WHERE arn = ?`, arn)
	return scanRepository(row)
}

func (s *Store) ListRepositories() ([]Repository, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, domain_name, arn, description, upstreams, external_conns, created_at
		 FROM repositories ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepositories(rows)
}

func (s *Store) ListRepositoriesInDomain(domainName string) ([]Repository, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, domain_name, arn, description, upstreams, external_conns, created_at
		 FROM repositories WHERE domain_name = ? ORDER BY created_at`, domainName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRepositories(rows)
}

func (s *Store) UpdateRepository(name, domainName, description string, upstreams string) error {
	res, err := s.store.DB().Exec(
		`UPDATE repositories SET description = ?, upstreams = ? WHERE name = ? AND domain_name = ?`,
		description, upstreams, name, domainName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

func (s *Store) UpdateRepositoryExternalConns(name, domainName, externalConns string) error {
	res, err := s.store.DB().Exec(
		`UPDATE repositories SET external_conns = ? WHERE name = ? AND domain_name = ?`,
		externalConns, name, domainName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

func (s *Store) DeleteRepository(name, domainName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM repositories WHERE name = ? AND domain_name = ?`, name, domainName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepositoryNotFound
	}
	return nil
}

// --- Package ---

func (s *Store) UpsertPackage(pkg *Package) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO packages (name, namespace, format, domain_name, repo_name, origin_config)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name, namespace, format, domain_name, repo_name) DO UPDATE SET origin_config = excluded.origin_config`,
		pkg.Name, pkg.Namespace, pkg.Format, pkg.DomainName, pkg.RepoName, pkg.OriginConfig,
	)
	return err
}

func (s *Store) GetPackage(name, namespace, format, domainName, repoName string) (*Package, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, namespace, format, domain_name, repo_name, origin_config
		 FROM packages WHERE name = ? AND namespace = ? AND format = ? AND domain_name = ? AND repo_name = ?`,
		name, namespace, format, domainName, repoName)
	return scanPackage(row)
}

func (s *Store) ListPackages(domainName, repoName, format, namespace string) ([]Package, error) {
	query := `SELECT name, namespace, format, domain_name, repo_name, origin_config FROM packages WHERE domain_name = ? AND repo_name = ?`
	args := []any{domainName, repoName}
	if format != "" {
		query += " AND format = ?"
		args = append(args, format)
	}
	if namespace != "" {
		query += " AND namespace = ?"
		args = append(args, namespace)
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pkgs []Package
	for rows.Next() {
		p, err := scanPackage(rows)
		if err != nil {
			return nil, err
		}
		pkgs = append(pkgs, *p)
	}
	return pkgs, rows.Err()
}

func (s *Store) DeletePackage(name, namespace, format, domainName, repoName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM packages WHERE name = ? AND namespace = ? AND format = ? AND domain_name = ? AND repo_name = ?`,
		name, namespace, format, domainName, repoName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPackageNotFound
	}
	return nil
}

// --- PackageGroup ---

func (s *Store) CreatePackageGroup(pg *PackageGroup) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO package_groups (arn, pattern, domain_name, description, origin_config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		pg.ARN, pg.Pattern, pg.DomainName, pg.Description, pg.OriginConfig, now,
	)
	return err
}

func (s *Store) GetPackageGroup(arn string) (*PackageGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, pattern, domain_name, description, origin_config, created_at
		 FROM package_groups WHERE arn = ?`, arn)
	return scanPackageGroup(row)
}

func (s *Store) GetPackageGroupByPattern(pattern, domainName string) (*PackageGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, pattern, domain_name, description, origin_config, created_at
		 FROM package_groups WHERE pattern = ? AND domain_name = ?`, pattern, domainName)
	return scanPackageGroup(row)
}

func (s *Store) ListPackageGroups(domainName string) ([]PackageGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, pattern, domain_name, description, origin_config, created_at
		 FROM package_groups WHERE domain_name = ? ORDER BY created_at`, domainName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []PackageGroup
	for rows.Next() {
		pg, err := scanPackageGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *pg)
	}
	return groups, rows.Err()
}

func (s *Store) UpdatePackageGroup(arn, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE package_groups SET description = ? WHERE arn = ?`, description, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPackageGroupNotFound
	}
	return nil
}

func (s *Store) UpdatePackageGroupOriginConfig(arn, originConfig string) error {
	res, err := s.store.DB().Exec(
		`UPDATE package_groups SET origin_config = ? WHERE arn = ?`, originConfig, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPackageGroupNotFound
	}
	return nil
}

func (s *Store) DeletePackageGroup(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM package_groups WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPackageGroupNotFound
	}
	return nil
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanDomain(sc scanner) (*Domain, error) {
	var d Domain
	var createdAt int64
	err := sc.Scan(&d.Name, &d.ARN, &d.Owner, &d.Status, &d.EncryptionKey, &d.RepoCount, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	return &d, nil
}

func scanRepository(sc scanner) (*Repository, error) {
	var r Repository
	var createdAt int64
	err := sc.Scan(&r.Name, &r.DomainName, &r.ARN, &r.Description, &r.Upstreams, &r.ExternalConns, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRepositoryNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func scanRepositories(rows *sql.Rows) ([]Repository, error) {
	var repos []Repository
	for rows.Next() {
		r, err := scanRepository(rows)
		if err != nil {
			return nil, err
		}
		repos = append(repos, *r)
	}
	return repos, rows.Err()
}

func scanPackage(sc scanner) (*Package, error) {
	var p Package
	err := sc.Scan(&p.Name, &p.Namespace, &p.Format, &p.DomainName, &p.RepoName, &p.OriginConfig)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPackageNotFound
		}
		return nil, err
	}
	return &p, nil
}

func scanPackageGroup(sc scanner) (*PackageGroup, error) {
	var pg PackageGroup
	var createdAt int64
	err := sc.Scan(&pg.ARN, &pg.Pattern, &pg.DomainName, &pg.Description, &pg.OriginConfig, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPackageGroupNotFound
		}
		return nil, err
	}
	pg.CreatedAt = time.Unix(createdAt, 0)
	return &pg, nil
}
