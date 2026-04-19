// SPDX-License-Identifier: Apache-2.0

// internal/services/serverlessrepo/store.go
package serverlessrepo

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errApplicationNotFound = errors.New("application not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS serverlessrepo_applications (
			application_id TEXT PRIMARY KEY,
			name           TEXT NOT NULL,
			description    TEXT NOT NULL DEFAULT '',
			author         TEXT NOT NULL DEFAULT '',
			home_page_url  TEXT NOT NULL DEFAULT '',
			readme_url     TEXT NOT NULL DEFAULT '',
			license_url    TEXT NOT NULL DEFAULT '',
			labels         TEXT NOT NULL DEFAULT '[]',
			created_at     INTEGER NOT NULL,
			UNIQUE(name)
		);
		CREATE TABLE IF NOT EXISTS serverlessrepo_versions (
			application_id   TEXT NOT NULL,
			semantic_version TEXT NOT NULL,
			template_url     TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			PRIMARY KEY (application_id, semantic_version)
		);
		CREATE TABLE IF NOT EXISTS serverlessrepo_policies (
			application_id TEXT PRIMARY KEY,
			statements     TEXT NOT NULL DEFAULT '[]'
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS serverlessrepo_change_sets (
			change_set_id    TEXT PRIMARY KEY,
			application_id   TEXT NOT NULL,
			stack_name       TEXT NOT NULL,
			semantic_version TEXT NOT NULL DEFAULT '',
			template_url     TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS serverlessrepo_templates (
			template_id      TEXT PRIMARY KEY,
			application_id   TEXT NOT NULL,
			semantic_version TEXT NOT NULL DEFAULT '',
			template_url     TEXT NOT NULL DEFAULT '',
			status           TEXT NOT NULL DEFAULT 'ACTIVE',
			expiration_time  INTEGER NOT NULL DEFAULT 0,
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS serverlessrepo_unshared (
			application_id   TEXT NOT NULL,
			principal        TEXT NOT NULL,
			PRIMARY KEY (application_id, principal)
		);
		CREATE TABLE IF NOT EXISTS serverlessrepo_dependencies (
			application_id   TEXT NOT NULL,
			dependent_app_id TEXT NOT NULL,
			semantic_version TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (application_id, dependent_app_id, semantic_version)
		);
	`},
}

// ChangeSet represents a CloudFormation change set stub.
type ChangeSet struct {
	ChangeSetID     string
	ApplicationID   string
	StackName       string
	SemanticVersion string
	TemplateURL     string
	CreatedAt       time.Time
}

// TemplateRecord represents a stored CloudFormation template stub.
type TemplateRecord struct {
	TemplateID      string
	ApplicationID   string
	SemanticVersion string
	TemplateURL     string
	Status          string
	CreatedAt       time.Time
}

// Application represents a serverless application.
type Application struct {
	ApplicationID string
	Name          string
	Description   string
	Author        string
	HomePageURL   string
	ReadmeURL     string
	LicenseURL    string
	Labels        string
	CreatedAt     time.Time
}

// ApplicationVersion represents a version of a serverless application.
type ApplicationVersion struct {
	ApplicationID   string
	SemanticVersion string
	TemplateURL     string
	CreatedAt       time.Time
}

// Store manages Serverless Application Repository data.
type Store struct {
	store *sqlite.Store
}

// NewStore opens (or creates) the SQLite database.
func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "serverlessrepo.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.store.Close() }

// --- Application operations ---

func (s *Store) CreateApplication(appID, name, description, author, homePageURL, readmeURL, licenseURL, labelsJSON string) (*Application, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO serverlessrepo_applications
		 (application_id, name, description, author, home_page_url, readme_url, license_url, labels, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		appID, name, description, author, homePageURL, readmeURL, licenseURL, labelsJSON, now,
	)
	if err != nil {
		return nil, err
	}
	return &Application{
		ApplicationID: appID, Name: name, Description: description,
		Author: author, HomePageURL: homePageURL, ReadmeURL: readmeURL,
		LicenseURL: licenseURL, Labels: labelsJSON,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetApplication(appID string) (*Application, error) {
	row := s.store.DB().QueryRow(
		`SELECT application_id, name, description, author, home_page_url, readme_url, license_url, labels, created_at
		 FROM serverlessrepo_applications WHERE application_id=?`,
		appID,
	)
	return scanApplication(row)
}

func (s *Store) UpdateApplication(appID, description, author, homePageURL, readmeURL string) error {
	res, err := s.store.DB().Exec(
		`UPDATE serverlessrepo_applications SET description=?, author=?, home_page_url=?, readme_url=? WHERE application_id=?`,
		description, author, homePageURL, readmeURL, appID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	return nil
}

func (s *Store) DeleteApplication(appID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM serverlessrepo_applications WHERE application_id=?`, appID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errApplicationNotFound
	}
	// Also delete versions and policy
	_, _ = s.store.DB().Exec(`DELETE FROM serverlessrepo_versions WHERE application_id=?`, appID)
	_, _ = s.store.DB().Exec(`DELETE FROM serverlessrepo_policies WHERE application_id=?`, appID)
	return nil
}

func (s *Store) ListApplications() ([]Application, error) {
	rows, err := s.store.DB().Query(
		`SELECT application_id, name, description, author, home_page_url, readme_url, license_url, labels, created_at
		 FROM serverlessrepo_applications ORDER BY created_at`,
	)
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

// --- Version operations ---

func (s *Store) CreateVersion(appID, semanticVersion, templateURL string) (*ApplicationVersion, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO serverlessrepo_versions (application_id, semantic_version, template_url, created_at)
		 VALUES (?, ?, ?, ?)`,
		appID, semanticVersion, templateURL, now,
	)
	if err != nil {
		return nil, err
	}
	return &ApplicationVersion{
		ApplicationID: appID, SemanticVersion: semanticVersion,
		TemplateURL: templateURL, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) ListVersions(appID string) ([]ApplicationVersion, error) {
	rows, err := s.store.DB().Query(
		`SELECT application_id, semantic_version, template_url, created_at
		 FROM serverlessrepo_versions WHERE application_id=? ORDER BY created_at`,
		appID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var versions []ApplicationVersion
	for rows.Next() {
		var v ApplicationVersion
		var createdAt int64
		if err := rows.Scan(&v.ApplicationID, &v.SemanticVersion, &v.TemplateURL, &createdAt); err != nil {
			return nil, err
		}
		v.CreatedAt = time.Unix(createdAt, 0)
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

// --- Policy operations ---

func (s *Store) GetPolicy(appID string) (string, error) {
	var statements string
	err := s.store.DB().QueryRow(
		`SELECT statements FROM serverlessrepo_policies WHERE application_id=?`, appID,
	).Scan(&statements)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "[]", nil
		}
		return "", err
	}
	return statements, nil
}

func (s *Store) PutPolicy(appID, statementsJSON string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO serverlessrepo_policies (application_id, statements) VALUES (?, ?)
		 ON CONFLICT(application_id) DO UPDATE SET statements=excluded.statements`,
		appID, statementsJSON,
	)
	return err
}

// --- ChangeSet operations ---

func (s *Store) CreateChangeSet(appID, stackName, semanticVersion, templateURL string) (*ChangeSet, error) {
	id := "changeSet-" + stackName
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO serverlessrepo_change_sets (change_set_id, application_id, stack_name, semantic_version, template_url, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, appID, stackName, semanticVersion, templateURL, now,
	)
	if err != nil {
		return nil, err
	}
	return &ChangeSet{
		ChangeSetID: id, ApplicationID: appID, StackName: stackName,
		SemanticVersion: semanticVersion, TemplateURL: templateURL,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

// --- Template operations ---

func (s *Store) CreateTemplate(appID, semanticVersion, templateURL string) (*TemplateRecord, error) {
	id := "template-" + appID + "-" + semanticVersion
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO serverlessrepo_templates (template_id, application_id, semantic_version, template_url, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		id, appID, semanticVersion, templateURL, now,
	)
	if err != nil {
		return nil, err
	}
	return &TemplateRecord{
		TemplateID: id, ApplicationID: appID,
		SemanticVersion: semanticVersion, TemplateURL: templateURL,
		Status: "ACTIVE", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTemplate(templateID string) (*TemplateRecord, error) {
	var t TemplateRecord
	var createdAt int64
	err := s.store.DB().QueryRow(
		`SELECT template_id, application_id, semantic_version, template_url, status, created_at FROM serverlessrepo_templates WHERE template_id=?`,
		templateID,
	).Scan(&t.TemplateID, &t.ApplicationID, &t.SemanticVersion, &t.TemplateURL, &t.Status, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("template not found")
	}
	if err != nil {
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

// --- Unshare/Dependency operations ---

func (s *Store) AddUnshared(appID, principal string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO serverlessrepo_unshared (application_id, principal) VALUES (?, ?)`,
		appID, principal,
	)
	return err
}

func (s *Store) AddDependency(appID, dependentAppID, semanticVersion string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO serverlessrepo_dependencies (application_id, dependent_app_id, semantic_version)
		 VALUES (?, ?, ?)`,
		appID, dependentAppID, semanticVersion,
	)
	return err
}

func (s *Store) ListDependencies(appID string) ([]map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT dependent_app_id, semantic_version FROM serverlessrepo_dependencies WHERE application_id=?`,
		appID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var deps []map[string]string
	for rows.Next() {
		var depID, ver string
		if err := rows.Scan(&depID, &ver); err != nil {
			return nil, err
		}
		deps = append(deps, map[string]string{"ApplicationId": depID, "SemanticVersion": ver})
	}
	return deps, rows.Err()
}

type storeScanner interface{ Scan(dest ...any) error }

func scanApplication(s storeScanner) (*Application, error) {
	var a Application
	var createdAt int64
	err := s.Scan(&a.ApplicationID, &a.Name, &a.Description, &a.Author,
		&a.HomePageURL, &a.ReadmeURL, &a.LicenseURL, &a.Labels, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errApplicationNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func isUniqueErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
