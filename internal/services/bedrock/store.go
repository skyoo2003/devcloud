// SPDX-License-Identifier: Apache-2.0

package bedrock

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("resource already exists")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS custom_models (
			model_id    TEXT PRIMARY KEY,
			model_name  TEXT UNIQUE,
			base_model_id TEXT,
			job_id      TEXT,
			status      TEXT DEFAULT 'Creating',
			account_id  TEXT NOT NULL,
			created_at  DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS customization_jobs (
			job_id        TEXT PRIMARY KEY,
			job_name      TEXT,
			custom_model_name TEXT,
			base_model_id TEXT,
			status        TEXT DEFAULT 'InProgress',
			account_id    TEXT NOT NULL,
			created_at    DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS guardrails (
			guardrail_id TEXT PRIMARY KEY,
			name         TEXT UNIQUE,
			description  TEXT,
			version      TEXT DEFAULT 'DRAFT',
			status       TEXT DEFAULT 'READY',
			account_id   TEXT NOT NULL,
			created_at   DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS bedrock_tags (
			resource_arn TEXT NOT NULL,
			tag_key      TEXT NOT NULL,
			tag_value    TEXT NOT NULL,
			PRIMARY KEY (resource_arn, tag_key)
		);
	`},
}

type Store struct {
	s *sqlite.Store
}

func NewStore(dir string) (*Store, error) {
	s, err := sqlite.Open(filepath.Join(dir, "bedrock.db"), migrations)
	if err != nil {
		return nil, fmt.Errorf("bedrock: open store: %w", err)
	}
	return &Store{s: s}, nil
}

func (s *Store) Close() error { return s.s.Close() }
func (s *Store) db() *sql.DB  { return s.s.DB() }

// --- Customization Jobs ---

type CustomizationJob struct {
	JobID           string
	JobName         string
	CustomModelName string
	BaseModelID     string
	Status          string
	AccountID       string
	CreatedAt       time.Time
}

func (s *Store) CreateCustomizationJob(j *CustomizationJob) error {
	_, err := s.db().Exec(
		`INSERT INTO customization_jobs (job_id, job_name, custom_model_name, base_model_id, status, account_id, created_at)
		 VALUES (?,?,?,?,?,?,?)`,
		j.JobID, j.JobName, j.CustomModelName, j.BaseModelID, j.Status, j.AccountID, j.CreatedAt,
	)
	return err
}

func (s *Store) GetCustomizationJob(jobID string) (*CustomizationJob, error) {
	row := s.db().QueryRow(
		`SELECT job_id, job_name, custom_model_name, base_model_id, status, account_id, created_at
		 FROM customization_jobs WHERE job_id=?`, jobID)
	return scanCustomizationJob(row)
}

func (s *Store) ListCustomizationJobs(accountID string) ([]*CustomizationJob, error) {
	rows, err := s.db().Query(
		`SELECT job_id, job_name, custom_model_name, base_model_id, status, account_id, created_at
		 FROM customization_jobs WHERE account_id=? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*CustomizationJob
	for rows.Next() {
		j, err := scanCustomizationJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) StopCustomizationJob(jobID string) error {
	res, err := s.db().Exec(`UPDATE customization_jobs SET status='Stopping' WHERE job_id=?`, jobID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanCustomizationJob(row interface{ Scan(...any) error }) (*CustomizationJob, error) {
	j := &CustomizationJob{}
	var createdStr string
	err := row.Scan(&j.JobID, &j.JobName, &j.CustomModelName, &j.BaseModelID, &j.Status, &j.AccountID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	j.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	return j, nil
}

// --- Custom Models ---

type CustomModel struct {
	ModelID     string
	ModelName   string
	BaseModelID string
	JobID       string
	Status      string
	AccountID   string
	CreatedAt   time.Time
}

func (s *Store) CreateCustomModel(m *CustomModel) error {
	_, err := s.db().Exec(
		`INSERT INTO custom_models (model_id, model_name, base_model_id, job_id, status, account_id, created_at)
		 VALUES (?,?,?,?,?,?,?)`,
		m.ModelID, m.ModelName, m.BaseModelID, m.JobID, m.Status, m.AccountID, m.CreatedAt,
	)
	return err
}

func (s *Store) GetCustomModel(modelID string) (*CustomModel, error) {
	row := s.db().QueryRow(
		`SELECT model_id, model_name, base_model_id, COALESCE(job_id,''), status, account_id, created_at
		 FROM custom_models WHERE model_id=?`, modelID)
	return scanCustomModel(row)
}

func (s *Store) ListCustomModels(accountID string) ([]*CustomModel, error) {
	rows, err := s.db().Query(
		`SELECT model_id, model_name, base_model_id, COALESCE(job_id,''), status, account_id, created_at
		 FROM custom_models WHERE account_id=? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*CustomModel
	for rows.Next() {
		m, err := scanCustomModel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCustomModel(modelID string) error {
	res, err := s.db().Exec(`DELETE FROM custom_models WHERE model_id=?`, modelID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanCustomModel(row interface{ Scan(...any) error }) (*CustomModel, error) {
	m := &CustomModel{}
	var createdStr string
	err := row.Scan(&m.ModelID, &m.ModelName, &m.BaseModelID, &m.JobID, &m.Status, &m.AccountID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	m.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
	return m, nil
}

// --- Guardrails ---

type Guardrail struct {
	GuardrailID string
	Name        string
	Description string
	Version     string
	Status      string
	AccountID   string
	CreatedAt   time.Time
}

func (s *Store) CreateGuardrail(g *Guardrail) error {
	_, err := s.db().Exec(
		`INSERT INTO guardrails (guardrail_id, name, description, version, status, account_id, created_at)
		 VALUES (?,?,?,?,?,?,?)`,
		g.GuardrailID, g.Name, g.Description, g.Version, g.Status, g.AccountID, g.CreatedAt,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return ErrConflict
		}
	}
	return err
}

func (s *Store) GetGuardrail(guardrailID string) (*Guardrail, error) {
	row := s.db().QueryRow(
		`SELECT guardrail_id, name, COALESCE(description,''), version, status, account_id, created_at
		 FROM guardrails WHERE guardrail_id=?`, guardrailID)
	return scanGuardrail(row)
}

func (s *Store) ListGuardrails(accountID string) ([]*Guardrail, error) {
	rows, err := s.db().Query(
		`SELECT guardrail_id, name, COALESCE(description,''), version, status, account_id, created_at
		 FROM guardrails WHERE account_id=? ORDER BY created_at`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Guardrail
	for rows.Next() {
		g, err := scanGuardrail(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *Store) UpdateGuardrail(guardrailID, name, description string) error {
	res, err := s.db().Exec(
		`UPDATE guardrails SET name=?, description=? WHERE guardrail_id=?`,
		name, description, guardrailID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) DeleteGuardrail(guardrailID string) error {
	res, err := s.db().Exec(`DELETE FROM guardrails WHERE guardrail_id=?`, guardrailID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func scanGuardrail(row interface{ Scan(...any) error }) (*Guardrail, error) {
	g := &Guardrail{}
	var createdStr string
	err := row.Scan(&g.GuardrailID, &g.Name, &g.Description, &g.Version, &g.Status, &g.AccountID, &createdStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	g.CreatedAt, _ = time.Parse("2006-01-02T15:04:05Z07:00", createdStr)
	if g.CreatedAt.IsZero() {
		g.CreatedAt = time.Now()
	}
	return g, nil
}

// --- Tags ---

func (s *Store) TagResource(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(
			`INSERT INTO bedrock_tags (resource_arn, tag_key, tag_value) VALUES (?,?,?)
			 ON CONFLICT(resource_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			arn, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	rows, err := s.db().Query(`SELECT tag_key, tag_value FROM bedrock_tags WHERE resource_arn=?`, arn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		rows.Scan(&k, &v)
		out[k] = v
	}
	return out, rows.Err()
}

func (s *Store) UntagResource(arn string, keys []string) error {
	for _, k := range keys {
		if _, err := s.db().Exec(`DELETE FROM bedrock_tags WHERE resource_arn=? AND tag_key=?`, arn, k); err != nil {
			return err
		}
	}
	return nil
}
