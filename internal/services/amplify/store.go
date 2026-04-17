// SPDX-License-Identifier: Apache-2.0

// internal/services/amplify/store.go
package amplify

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errAppNotFound               = errors.New("app not found")
	errBranchNotFound            = errors.New("branch not found")
	errDomainAssociationNotFound = errors.New("domain association not found")
	errWebhookNotFound           = errors.New("webhook not found")
	errBackendEnvNotFound        = errors.New("backend environment not found")
	errJobNotFound               = errors.New("job not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS apps (
			id           TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			name         TEXT NOT NULL,
			description  TEXT NOT NULL DEFAULT '',
			repository   TEXT NOT NULL DEFAULT '',
			platform     TEXT NOT NULL DEFAULT 'WEB',
			iam_role     TEXT NOT NULL DEFAULT '',
			default_domain TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL,
			updated_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS branches (
			app_id           TEXT NOT NULL,
			name             TEXT NOT NULL,
			arn              TEXT NOT NULL UNIQUE,
			display_name     TEXT NOT NULL DEFAULT '',
			description      TEXT NOT NULL DEFAULT '',
			stage            TEXT NOT NULL DEFAULT 'NONE',
			framework        TEXT NOT NULL DEFAULT '',
			enable_auto_build INTEGER NOT NULL DEFAULT 1,
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL,
			PRIMARY KEY (app_id, name)
		);
		CREATE TABLE IF NOT EXISTS domain_associations (
			app_id      TEXT NOT NULL,
			domain_name TEXT NOT NULL,
			arn         TEXT NOT NULL UNIQUE,
			status      TEXT NOT NULL DEFAULT 'AVAILABLE',
			sub_domains TEXT NOT NULL DEFAULT '[]',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL,
			PRIMARY KEY (app_id, domain_name)
		);
		CREATE TABLE IF NOT EXISTS webhooks (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			app_id      TEXT NOT NULL,
			branch_name TEXT NOT NULL DEFAULT '',
			url         TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS backend_environments (
			app_id      TEXT NOT NULL,
			name        TEXT NOT NULL,
			arn         TEXT NOT NULL UNIQUE,
			stack_name  TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (app_id, name)
		);
		CREATE TABLE IF NOT EXISTS jobs (
			id           TEXT NOT NULL,
			app_id       TEXT NOT NULL,
			branch_name  TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			job_type     TEXT NOT NULL DEFAULT 'RELEASE',
			status       TEXT NOT NULL DEFAULT 'SUCCEED',
			commit_id    TEXT NOT NULL DEFAULT '',
			commit_msg   TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL,
			updated_at   INTEGER NOT NULL,
			PRIMARY KEY (app_id, branch_name, id)
		);
	`},
}

// --- Model types ---

type App struct {
	ID            string
	ARN           string
	Name          string
	Description   string
	Repository    string
	Platform      string
	IAMRole       string
	DefaultDomain string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Branch struct {
	AppID           string
	Name            string
	ARN             string
	DisplayName     string
	Description     string
	Stage           string
	Framework       string
	EnableAutoBuild bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type DomainAssociation struct {
	AppID      string
	DomainName string
	ARN        string
	Status     string
	SubDomains string // JSON array
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Webhook struct {
	ID         string
	ARN        string
	AppID      string
	BranchName string
	URL        string
	CreatedAt  time.Time
}

type BackendEnvironment struct {
	AppID     string
	Name      string
	ARN       string
	StackName string
	CreatedAt time.Time
}

type Job struct {
	ID         string
	AppID      string
	BranchName string
	ARN        string
	JobType    string
	Status     string
	CommitID   string
	CommitMsg  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "amplify.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Apps ---

func (s *Store) CreateApp(a *App) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO apps (id, arn, name, description, repository, platform, iam_role, default_domain, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ARN, a.Name, a.Description, a.Repository, a.Platform, a.IAMRole, a.DefaultDomain, now, now,
	)
	return err
}

func (s *Store) GetApp(id string) (*App, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, repository, platform, iam_role, default_domain, created_at, updated_at
		 FROM apps WHERE id = ?`, id)
	return scanApp(row)
}

func (s *Store) ListApps() ([]App, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, description, repository, platform, iam_role, default_domain, created_at, updated_at
		 FROM apps ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var apps []App
	for rows.Next() {
		a, err := scanApp(rows)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *a)
	}
	return apps, rows.Err()
}

func (s *Store) UpdateApp(id string, fields map[string]any) error {
	a, err := s.GetApp(id)
	if err != nil {
		return errAppNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		a.Name = v
	}
	if v, ok := fields["description"].(string); ok {
		a.Description = v
	}
	if v, ok := fields["repository"].(string); ok {
		a.Repository = v
	}
	if v, ok := fields["platform"].(string); ok && v != "" {
		a.Platform = v
	}
	if v, ok := fields["iamServiceRoleArn"].(string); ok {
		a.IAMRole = v
	}
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE apps SET name=?, description=?, repository=?, platform=?, iam_role=?, updated_at=? WHERE id=?`,
		a.Name, a.Description, a.Repository, a.Platform, a.IAMRole, now, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAppNotFound
	}
	return nil
}

func (s *Store) DeleteApp(id string) (*App, error) {
	a, err := s.GetApp(id)
	if err != nil {
		return nil, errAppNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM apps WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return a, nil
}

// --- Branches ---

func (s *Store) CreateBranch(b *Branch) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO branches (app_id, name, arn, display_name, description, stage, framework, enable_auto_build, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.AppID, b.Name, b.ARN, b.DisplayName, b.Description, b.Stage, b.Framework, boolInt(b.EnableAutoBuild), now, now,
	)
	return err
}

func (s *Store) GetBranch(appID, name string) (*Branch, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, name, arn, display_name, description, stage, framework, enable_auto_build, created_at, updated_at
		 FROM branches WHERE app_id = ? AND name = ?`, appID, name)
	return scanBranch(row)
}

func (s *Store) ListBranches(appID string) ([]Branch, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_id, name, arn, display_name, description, stage, framework, enable_auto_build, created_at, updated_at
		 FROM branches WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var branches []Branch
	for rows.Next() {
		b, err := scanBranch(rows)
		if err != nil {
			return nil, err
		}
		branches = append(branches, *b)
	}
	return branches, rows.Err()
}

func (s *Store) UpdateBranch(appID, name string, fields map[string]any) error {
	b, err := s.GetBranch(appID, name)
	if err != nil {
		return errBranchNotFound
	}
	if v, ok := fields["displayName"].(string); ok {
		b.DisplayName = v
	}
	if v, ok := fields["description"].(string); ok {
		b.Description = v
	}
	if v, ok := fields["stage"].(string); ok && v != "" {
		b.Stage = v
	}
	if v, ok := fields["framework"].(string); ok {
		b.Framework = v
	}
	if v, ok := fields["enableAutoBuild"].(bool); ok {
		b.EnableAutoBuild = v
	}
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE branches SET display_name=?, description=?, stage=?, framework=?, enable_auto_build=?, updated_at=?
		 WHERE app_id=? AND name=?`,
		b.DisplayName, b.Description, b.Stage, b.Framework, boolInt(b.EnableAutoBuild), now, appID, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBranchNotFound
	}
	return nil
}

func (s *Store) DeleteBranch(appID, name string) (*Branch, error) {
	b, err := s.GetBranch(appID, name)
	if err != nil {
		return nil, errBranchNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM branches WHERE app_id = ? AND name = ?`, appID, name); err != nil {
		return nil, err
	}
	return b, nil
}

// --- Domain Associations ---

func (s *Store) CreateDomainAssociation(d *DomainAssociation) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO domain_associations (app_id, domain_name, arn, status, sub_domains, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		d.AppID, d.DomainName, d.ARN, d.Status, d.SubDomains, now, now,
	)
	return err
}

func (s *Store) GetDomainAssociation(appID, domainName string) (*DomainAssociation, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, domain_name, arn, status, sub_domains, created_at, updated_at
		 FROM domain_associations WHERE app_id = ? AND domain_name = ?`, appID, domainName)
	return scanDomainAssociation(row)
}

func (s *Store) ListDomainAssociations(appID string) ([]DomainAssociation, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_id, domain_name, arn, status, sub_domains, created_at, updated_at
		 FROM domain_associations WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var domains []DomainAssociation
	for rows.Next() {
		d, err := scanDomainAssociation(rows)
		if err != nil {
			return nil, err
		}
		domains = append(domains, *d)
	}
	return domains, rows.Err()
}

func (s *Store) UpdateDomainAssociation(appID, domainName string, fields map[string]any) error {
	d, err := s.GetDomainAssociation(appID, domainName)
	if err != nil {
		return errDomainAssociationNotFound
	}
	if v, ok := fields["subDomainSettings"]; ok {
		b, _ := json.Marshal(v)
		d.SubDomains = string(b)
	}
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE domain_associations SET sub_domains=?, updated_at=? WHERE app_id=? AND domain_name=?`,
		d.SubDomains, now, appID, domainName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDomainAssociationNotFound
	}
	return nil
}

func (s *Store) DeleteDomainAssociation(appID, domainName string) (*DomainAssociation, error) {
	d, err := s.GetDomainAssociation(appID, domainName)
	if err != nil {
		return nil, errDomainAssociationNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM domain_associations WHERE app_id = ? AND domain_name = ?`, appID, domainName); err != nil {
		return nil, err
	}
	return d, nil
}

// --- Webhooks ---

func (s *Store) CreateWebhook(w *Webhook) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO webhooks (id, arn, app_id, branch_name, url, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		w.ID, w.ARN, w.AppID, w.BranchName, w.URL, now,
	)
	return err
}

func (s *Store) GetWebhook(id string) (*Webhook, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, app_id, branch_name, url, created_at FROM webhooks WHERE id = ?`, id)
	return scanWebhook(row)
}

func (s *Store) ListWebhooks(appID string) ([]Webhook, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, app_id, branch_name, url, created_at FROM webhooks WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var webhooks []Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, err
		}
		webhooks = append(webhooks, *w)
	}
	return webhooks, rows.Err()
}

func (s *Store) UpdateWebhook(id string, fields map[string]any) error {
	w, err := s.GetWebhook(id)
	if err != nil {
		return errWebhookNotFound
	}
	if v, ok := fields["branchName"].(string); ok && v != "" {
		w.BranchName = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE webhooks SET branch_name=? WHERE id=?`, w.BranchName, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWebhookNotFound
	}
	return nil
}

func (s *Store) DeleteWebhook(id string) (*Webhook, error) {
	w, err := s.GetWebhook(id)
	if err != nil {
		return nil, errWebhookNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM webhooks WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return w, nil
}

// --- Backend Environments ---

func (s *Store) CreateBackendEnvironment(be *BackendEnvironment) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO backend_environments (app_id, name, arn, stack_name, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		be.AppID, be.Name, be.ARN, be.StackName, now,
	)
	return err
}

func (s *Store) GetBackendEnvironment(appID, name string) (*BackendEnvironment, error) {
	row := s.store.DB().QueryRow(
		`SELECT app_id, name, arn, stack_name, created_at FROM backend_environments WHERE app_id = ? AND name = ?`, appID, name)
	return scanBackendEnvironment(row)
}

func (s *Store) ListBackendEnvironments(appID string) ([]BackendEnvironment, error) {
	rows, err := s.store.DB().Query(
		`SELECT app_id, name, arn, stack_name, created_at FROM backend_environments WHERE app_id = ? ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var envs []BackendEnvironment
	for rows.Next() {
		be, err := scanBackendEnvironment(rows)
		if err != nil {
			return nil, err
		}
		envs = append(envs, *be)
	}
	return envs, rows.Err()
}

func (s *Store) DeleteBackendEnvironment(appID, name string) (*BackendEnvironment, error) {
	be, err := s.GetBackendEnvironment(appID, name)
	if err != nil {
		return nil, errBackendEnvNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM backend_environments WHERE app_id = ? AND name = ?`, appID, name); err != nil {
		return nil, err
	}
	return be, nil
}

// --- Jobs ---

func (s *Store) CreateJob(j *Job) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO jobs (id, app_id, branch_name, arn, job_type, status, commit_id, commit_msg, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.AppID, j.BranchName, j.ARN, j.JobType, j.Status, j.CommitID, j.CommitMsg, now, now,
	)
	return err
}

func (s *Store) GetJob(appID, branchName, id string) (*Job, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, app_id, branch_name, arn, job_type, status, commit_id, commit_msg, created_at, updated_at
		 FROM jobs WHERE app_id = ? AND branch_name = ? AND id = ?`, appID, branchName, id)
	return scanJob(row)
}

func (s *Store) ListJobs(appID, branchName string) ([]Job, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, app_id, branch_name, arn, job_type, status, commit_id, commit_msg, created_at, updated_at
		 FROM jobs WHERE app_id = ? AND branch_name = ? ORDER BY created_at DESC`, appID, branchName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func (s *Store) UpdateJobStatus(appID, branchName, id, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE jobs SET status=?, updated_at=? WHERE app_id=? AND branch_name=? AND id=?`,
		status, now, appID, branchName, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobNotFound
	}
	return nil
}

func (s *Store) DeleteJob(appID, branchName, id string) (*Job, error) {
	j, err := s.GetJob(appID, branchName, id)
	if err != nil {
		return nil, errJobNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM jobs WHERE app_id = ? AND branch_name = ? AND id = ?`, appID, branchName, id); err != nil {
		return nil, err
	}
	return j, nil
}

// --- Scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanApp(sc scanner) (*App, error) {
	var a App
	var createdAt, updatedAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.Description, &a.Repository, &a.Platform, &a.IAMRole, &a.DefaultDomain, &createdAt, &updatedAt)
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

func scanBranch(sc scanner) (*Branch, error) {
	var b Branch
	var createdAt, updatedAt int64
	var autoBuild int
	err := sc.Scan(&b.AppID, &b.Name, &b.ARN, &b.DisplayName, &b.Description, &b.Stage, &b.Framework, &autoBuild, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBranchNotFound
		}
		return nil, err
	}
	b.EnableAutoBuild = autoBuild != 0
	b.CreatedAt = time.Unix(createdAt, 0)
	b.UpdatedAt = time.Unix(updatedAt, 0)
	return &b, nil
}

func scanDomainAssociation(sc scanner) (*DomainAssociation, error) {
	var d DomainAssociation
	var createdAt, updatedAt int64
	err := sc.Scan(&d.AppID, &d.DomainName, &d.ARN, &d.Status, &d.SubDomains, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDomainAssociationNotFound
		}
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	d.UpdatedAt = time.Unix(updatedAt, 0)
	return &d, nil
}

func scanWebhook(sc scanner) (*Webhook, error) {
	var w Webhook
	var createdAt int64
	err := sc.Scan(&w.ID, &w.ARN, &w.AppID, &w.BranchName, &w.URL, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errWebhookNotFound
		}
		return nil, err
	}
	w.CreatedAt = time.Unix(createdAt, 0)
	return &w, nil
}

func scanBackendEnvironment(sc scanner) (*BackendEnvironment, error) {
	var be BackendEnvironment
	var createdAt int64
	err := sc.Scan(&be.AppID, &be.Name, &be.ARN, &be.StackName, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBackendEnvNotFound
		}
		return nil, err
	}
	be.CreatedAt = time.Unix(createdAt, 0)
	return &be, nil
}

func scanJob(sc scanner) (*Job, error) {
	var j Job
	var createdAt, updatedAt int64
	err := sc.Scan(&j.ID, &j.AppID, &j.BranchName, &j.ARN, &j.JobType, &j.Status, &j.CommitID, &j.CommitMsg, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errJobNotFound
		}
		return nil, err
	}
	j.CreatedAt = time.Unix(createdAt, 0)
	j.UpdatedAt = time.Unix(updatedAt, 0)
	return &j, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func isUniqueErr(err error) bool {
	return sqlite.IsUniqueConstraintError(err)
}
