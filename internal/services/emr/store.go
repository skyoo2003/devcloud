// SPDX-License-Identifier: Apache-2.0

// internal/services/emr/store.go
package emr

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errClusterNotFound       = errors.New("cluster not found")
	errStepNotFound          = errors.New("step not found")
	errSecurityConfigExists  = errors.New("security configuration already exists")
	errSecurityConfigMissing = errors.New("security configuration not found")
	errStudioNotFound        = errors.New("studio not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			id              TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'WAITING',
			release_label   TEXT NOT NULL DEFAULT 'emr-7.0.0',
			instance_type   TEXT NOT NULL DEFAULT 'm5.xlarge',
			instance_count  INTEGER NOT NULL DEFAULT 1,
			log_uri         TEXT NOT NULL DEFAULT '',
			service_role    TEXT NOT NULL DEFAULT '',
			auto_terminate  INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS steps (
			id                TEXT PRIMARY KEY,
			cluster_id        TEXT NOT NULL,
			name              TEXT NOT NULL,
			status            TEXT NOT NULL DEFAULT 'COMPLETED',
			action_on_failure TEXT NOT NULL DEFAULT 'CONTINUE',
			config            TEXT NOT NULL DEFAULT '{}',
			created_at        INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS security_configs (
			name       TEXT PRIMARY KEY,
			config     TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS studios (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			auth_mode   TEXT NOT NULL DEFAULT 'IAM',
			vpc_id      TEXT NOT NULL DEFAULT '',
			url         TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
	`},
}

// ---- model types ----

type Cluster struct {
	ID            string
	Name          string
	ARN           string
	Status        string
	ReleaseLabel  string
	InstanceType  string
	InstanceCount int
	LogURI        string
	ServiceRole   string
	AutoTerminate bool
	CreatedAt     time.Time
}

type Step struct {
	ID              string
	ClusterID       string
	Name            string
	Status          string
	ActionOnFailure string
	Config          string
	CreatedAt       time.Time
}

type SecurityConfig struct {
	Name      string
	Config    string
	CreatedAt time.Time
}

type Studio struct {
	ID          string
	ARN         string
	Name        string
	Description string
	AuthMode    string
	VpcID       string
	URL         string
	CreatedAt   time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "emr.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Cluster CRUD ----

func (s *Store) CreateCluster(c *Cluster) error {
	now := time.Now().Unix()
	c.CreatedAt = time.Unix(now, 0)
	_, err := s.store.DB().Exec(
		`INSERT INTO clusters (id, name, arn, status, release_label, instance_type, instance_count, log_uri, service_role, auto_terminate, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.Name, c.ARN, c.Status, c.ReleaseLabel, c.InstanceType, c.InstanceCount,
		c.LogURI, c.ServiceRole, boolToInt(c.AutoTerminate), now,
	)
	return err
}

func (s *Store) GetCluster(id string) (*Cluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, status, release_label, instance_type, instance_count, log_uri, service_role, auto_terminate, created_at
		 FROM clusters WHERE id = ?`, id)
	return scanCluster(row)
}

func (s *Store) ListClusters(statusFilter string) ([]Cluster, error) {
	query := `SELECT id, name, arn, status, release_label, instance_type, instance_count, log_uri, service_role, auto_terminate, created_at FROM clusters`
	var args []any
	if statusFilter != "" {
		query += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clusters []Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, *c)
	}
	return clusters, rows.Err()
}

func (s *Store) UpdateClusterStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE clusters SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

// ---- Step CRUD ----

func (s *Store) CreateStep(step *Step) error {
	now := time.Now().Unix()
	step.CreatedAt = time.Unix(now, 0)
	_, err := s.store.DB().Exec(
		`INSERT INTO steps (id, cluster_id, name, status, action_on_failure, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		step.ID, step.ClusterID, step.Name, step.Status, step.ActionOnFailure, step.Config, now,
	)
	return err
}

func (s *Store) GetStep(clusterID, stepID string) (*Step, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, cluster_id, name, status, action_on_failure, config, created_at
		 FROM steps WHERE id = ? AND cluster_id = ?`, stepID, clusterID)
	return scanStep(row)
}

func (s *Store) ListSteps(clusterID, statusFilter string) ([]Step, error) {
	query := `SELECT id, cluster_id, name, status, action_on_failure, config, created_at FROM steps WHERE cluster_id = ?`
	args := []any{clusterID}
	if statusFilter != "" {
		query += ` AND status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var steps []Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		steps = append(steps, *st)
	}
	return steps, rows.Err()
}

func (s *Store) UpdateStepStatus(clusterID, stepID, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE steps SET status = ? WHERE id = ? AND cluster_id = ?`, status, stepID, clusterID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStepNotFound
	}
	return nil
}

// ---- SecurityConfig CRUD ----

func (s *Store) CreateSecurityConfig(name, config string) (*SecurityConfig, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO security_configs (name, config, created_at) VALUES (?, ?, ?)`,
		name, config, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, errSecurityConfigExists
		}
		return nil, err
	}
	return &SecurityConfig{Name: name, Config: config, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetSecurityConfig(name string) (*SecurityConfig, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, config, created_at FROM security_configs WHERE name = ?`, name)
	var sc SecurityConfig
	var createdAt int64
	if err := row.Scan(&sc.Name, &sc.Config, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSecurityConfigMissing
		}
		return nil, err
	}
	sc.CreatedAt = time.Unix(createdAt, 0)
	return &sc, nil
}

func (s *Store) DeleteSecurityConfig(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM security_configs WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSecurityConfigMissing
	}
	return nil
}

func (s *Store) ListSecurityConfigs() ([]SecurityConfig, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, config, created_at FROM security_configs ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var configs []SecurityConfig
	for rows.Next() {
		var sc SecurityConfig
		var createdAt int64
		if err := rows.Scan(&sc.Name, &sc.Config, &createdAt); err != nil {
			return nil, err
		}
		sc.CreatedAt = time.Unix(createdAt, 0)
		configs = append(configs, sc)
	}
	return configs, rows.Err()
}

// ---- Studio CRUD ----

func (s *Store) CreateStudio(st *Studio) error {
	now := time.Now().Unix()
	st.CreatedAt = time.Unix(now, 0)
	_, err := s.store.DB().Exec(
		`INSERT INTO studios (id, arn, name, description, auth_mode, vpc_id, url, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		st.ID, st.ARN, st.Name, st.Description, st.AuthMode, st.VpcID, st.URL, now,
	)
	return err
}

func (s *Store) GetStudio(id string) (*Studio, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, description, auth_mode, vpc_id, url, created_at FROM studios WHERE id = ?`, id)
	return scanStudio(row)
}

func (s *Store) ListStudios() ([]Studio, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, description, auth_mode, vpc_id, url, created_at FROM studios ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var studios []Studio
	for rows.Next() {
		st, err := scanStudio(rows)
		if err != nil {
			return nil, err
		}
		studios = append(studios, *st)
	}
	return studios, rows.Err()
}

func (s *Store) UpdateStudio(id, name, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE studios SET name = ?, description = ? WHERE id = ?`, name, description, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStudioNotFound
	}
	return nil
}

func (s *Store) DeleteStudio(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM studios WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStudioNotFound
	}
	return nil
}

// ---- scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanCluster(s scanner) (*Cluster, error) {
	var c Cluster
	var createdAt int64
	var autoTerminate int
	err := s.Scan(&c.ID, &c.Name, &c.ARN, &c.Status, &c.ReleaseLabel, &c.InstanceType,
		&c.InstanceCount, &c.LogURI, &c.ServiceRole, &autoTerminate, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.AutoTerminate = autoTerminate != 0
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanStep(s scanner) (*Step, error) {
	var st Step
	var createdAt int64
	err := s.Scan(&st.ID, &st.ClusterID, &st.Name, &st.Status, &st.ActionOnFailure, &st.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStepNotFound
		}
		return nil, err
	}
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

func scanStudio(s scanner) (*Studio, error) {
	var st Studio
	var createdAt int64
	err := s.Scan(&st.ID, &st.ARN, &st.Name, &st.Description, &st.AuthMode, &st.VpcID, &st.URL, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStudioNotFound
		}
		return nil, err
	}
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
