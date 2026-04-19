// SPDX-License-Identifier: Apache-2.0

// internal/services/applicationautoscaling/store.go
package applicationautoscaling

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errTargetNotFound          = errors.New("scalable target not found")
	errPolicyNotFound          = errors.New("scaling policy not found")
	errScheduledActionNotFound = errors.New("scheduled action not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS scalable_targets (
			id                TEXT PRIMARY KEY,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			min_capacity      INTEGER NOT NULL DEFAULT 1,
			max_capacity      INTEGER NOT NULL DEFAULT 10,
			role_arn          TEXT NOT NULL DEFAULT '',
			created_at        INTEGER NOT NULL,
			UNIQUE(service_namespace, resource_id, dimension)
		);
		CREATE TABLE IF NOT EXISTS scaling_policies (
			arn               TEXT PRIMARY KEY,
			name              TEXT NOT NULL,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			policy_type       TEXT NOT NULL DEFAULT 'TargetTrackingScaling',
			config            TEXT NOT NULL DEFAULT '{}',
			created_at        INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS scheduled_actions (
			arn               TEXT PRIMARY KEY,
			name              TEXT NOT NULL,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			schedule          TEXT NOT NULL DEFAULT '',
			config            TEXT NOT NULL DEFAULT '{}',
			created_at        INTEGER NOT NULL
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS scaling_activities (
			id                TEXT PRIMARY KEY,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			description       TEXT NOT NULL DEFAULT '',
			cause             TEXT NOT NULL DEFAULT '',
			status_code       TEXT NOT NULL DEFAULT 'Successful',
			status_msg        TEXT NOT NULL DEFAULT '',
			start_time        INTEGER NOT NULL,
			end_time          INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS customized_metrics (
			id                TEXT PRIMARY KEY,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			metric_name       TEXT NOT NULL,
			namespace         TEXT NOT NULL,
			statistic         TEXT NOT NULL DEFAULT 'Average',
			unit              TEXT NOT NULL DEFAULT '',
			config            TEXT NOT NULL DEFAULT '{}',
			created_at        INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS predictive_forecasts (
			id                TEXT PRIMARY KEY,
			service_namespace TEXT NOT NULL,
			resource_id       TEXT NOT NULL,
			dimension         TEXT NOT NULL,
			policy_name       TEXT NOT NULL,
			forecast_data     TEXT NOT NULL DEFAULT '{}',
			created_at        INTEGER NOT NULL
		);
	`},
}

type ScalableTarget struct {
	ID               string
	ServiceNamespace string
	ResourceID       string
	Dimension        string
	MinCapacity      int
	MaxCapacity      int
	RoleARN          string
	CreatedAt        time.Time
}

type ScalingPolicy struct {
	ARN              string
	Name             string
	ServiceNamespace string
	ResourceID       string
	Dimension        string
	PolicyType       string
	Config           string
	CreatedAt        time.Time
}

type ScheduledAction struct {
	ARN              string
	Name             string
	ServiceNamespace string
	ResourceID       string
	Dimension        string
	Schedule         string
	Config           string
	CreatedAt        time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "applicationautoscaling.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ScalableTarget operations

func (s *Store) RegisterTarget(ns, resourceID, dimension, roleARN string, min, max int) (*ScalableTarget, error) {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO scalable_targets (id, service_namespace, resource_id, dimension, min_capacity, max_capacity, role_arn, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(service_namespace, resource_id, dimension)
		DO UPDATE SET min_capacity=excluded.min_capacity, max_capacity=excluded.max_capacity, role_arn=excluded.role_arn`,
		id, ns, resourceID, dimension, min, max, roleARN, now,
	)
	if err != nil {
		return nil, err
	}
	// Fetch the actual record (id may differ on upsert)
	return s.GetTarget(ns, resourceID, dimension)
}

func (s *Store) GetTarget(ns, resourceID, dimension string) (*ScalableTarget, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, service_namespace, resource_id, dimension, min_capacity, max_capacity, role_arn, created_at
		 FROM scalable_targets WHERE service_namespace=? AND resource_id=? AND dimension=?`,
		ns, resourceID, dimension)
	return scanTarget(row)
}

func (s *Store) ListTargets(ns, resourceID, dimension string) ([]ScalableTarget, error) {
	query := `SELECT id, service_namespace, resource_id, dimension, min_capacity, max_capacity, role_arn, created_at FROM scalable_targets WHERE 1=1`
	var args []any
	if ns != "" {
		query += ` AND service_namespace=?`
		args = append(args, ns)
	}
	if resourceID != "" {
		query += ` AND resource_id=?`
		args = append(args, resourceID)
	}
	if dimension != "" {
		query += ` AND dimension=?`
		args = append(args, dimension)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []ScalableTarget
	for rows.Next() {
		t, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		targets = append(targets, *t)
	}
	return targets, rows.Err()
}

func (s *Store) DeregisterTarget(ns, resourceID, dimension string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM scalable_targets WHERE service_namespace=? AND resource_id=? AND dimension=?`,
		ns, resourceID, dimension)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTargetNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanTarget(s scanner) (*ScalableTarget, error) {
	var t ScalableTarget
	var createdAt int64
	err := s.Scan(&t.ID, &t.ServiceNamespace, &t.ResourceID, &t.Dimension, &t.MinCapacity, &t.MaxCapacity, &t.RoleARN, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTargetNotFound
		}
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

// ScalingPolicy operations

func (s *Store) PutPolicy(name, ns, resourceID, dimension, policyType, config string) (*ScalingPolicy, error) {
	now := time.Now().Unix()
	arn := shared.BuildARN("application-autoscaling", "scalingPolicy", name)
	_, err := s.store.DB().Exec(`
		INSERT INTO scaling_policies (arn, name, service_namespace, resource_id, dimension, policy_type, config, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET policy_type=excluded.policy_type, config=excluded.config`,
		arn, name, ns, resourceID, dimension, policyType, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &ScalingPolicy{
		ARN: arn, Name: name, ServiceNamespace: ns,
		ResourceID: resourceID, Dimension: dimension,
		PolicyType: policyType, Config: config,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) ListPolicies(ns, resourceID, dimension string) ([]ScalingPolicy, error) {
	query := `SELECT arn, name, service_namespace, resource_id, dimension, policy_type, config, created_at FROM scaling_policies WHERE 1=1`
	var args []any
	if ns != "" {
		query += ` AND service_namespace=?`
		args = append(args, ns)
	}
	if resourceID != "" {
		query += ` AND resource_id=?`
		args = append(args, resourceID)
	}
	if dimension != "" {
		query += ` AND dimension=?`
		args = append(args, dimension)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var policies []ScalingPolicy
	for rows.Next() {
		var p ScalingPolicy
		var createdAt int64
		if err := rows.Scan(&p.ARN, &p.Name, &p.ServiceNamespace, &p.ResourceID, &p.Dimension, &p.PolicyType, &p.Config, &createdAt); err != nil {
			return nil, err
		}
		p.CreatedAt = time.Unix(createdAt, 0)
		policies = append(policies, p)
	}
	return policies, rows.Err()
}

func (s *Store) DeletePolicy(policyName, ns, resourceID, dimension string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM scaling_policies WHERE name=? AND service_namespace=? AND resource_id=? AND dimension=?`,
		policyName, ns, resourceID, dimension)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

// ScheduledAction operations

func (s *Store) PutScheduledAction(name, ns, resourceID, dimension, schedule, config string) (*ScheduledAction, error) {
	now := time.Now().Unix()
	arn := shared.BuildARN("application-autoscaling", "scheduledAction", name)
	_, err := s.store.DB().Exec(`
		INSERT INTO scheduled_actions (arn, name, service_namespace, resource_id, dimension, schedule, config, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET schedule=excluded.schedule, config=excluded.config`,
		arn, name, ns, resourceID, dimension, schedule, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &ScheduledAction{
		ARN: arn, Name: name, ServiceNamespace: ns,
		ResourceID: resourceID, Dimension: dimension,
		Schedule: schedule, Config: config,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) ListScheduledActions(ns, resourceID, dimension string) ([]ScheduledAction, error) {
	query := `SELECT arn, name, service_namespace, resource_id, dimension, schedule, config, created_at FROM scheduled_actions WHERE 1=1`
	var args []any
	if ns != "" {
		query += ` AND service_namespace=?`
		args = append(args, ns)
	}
	if resourceID != "" {
		query += ` AND resource_id=?`
		args = append(args, resourceID)
	}
	if dimension != "" {
		query += ` AND dimension=?`
		args = append(args, dimension)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var actions []ScheduledAction
	for rows.Next() {
		var a ScheduledAction
		var createdAt int64
		if err := rows.Scan(&a.ARN, &a.Name, &a.ServiceNamespace, &a.ResourceID, &a.Dimension, &a.Schedule, &a.Config, &createdAt); err != nil {
			return nil, err
		}
		a.CreatedAt = time.Unix(createdAt, 0)
		actions = append(actions, a)
	}
	return actions, rows.Err()
}

func (s *Store) DeleteScheduledAction(actionName, ns, resourceID, dimension string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM scheduled_actions WHERE name=? AND service_namespace=? AND resource_id=? AND dimension=?`,
		actionName, ns, resourceID, dimension)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errScheduledActionNotFound
	}
	return nil
}

// --- Scaling Activities ---

type ScalingActivity struct {
	ID               string
	ServiceNamespace string
	ResourceID       string
	Dimension        string
	Description      string
	Cause            string
	StatusCode       string
	StatusMessage    string
	StartTime        time.Time
	EndTime          time.Time
}

func (s *Store) RecordActivity(ns, resourceID, dimension, description, cause string) (*ScalingActivity, error) {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO scaling_activities (id, service_namespace, resource_id, dimension, description, cause, status_code, status_msg, start_time, end_time)
		VALUES (?, ?, ?, ?, ?, ?, 'Successful', '', ?, ?)`,
		id, ns, resourceID, dimension, description, cause, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &ScalingActivity{
		ID: id, ServiceNamespace: ns, ResourceID: resourceID, Dimension: dimension,
		Description: description, Cause: cause, StatusCode: "Successful",
		StartTime: time.Unix(now, 0), EndTime: time.Unix(now, 0),
	}, nil
}

func (s *Store) ListActivities(ns, resourceID, dimension string) ([]ScalingActivity, error) {
	query := `SELECT id, service_namespace, resource_id, dimension, description, cause, status_code, status_msg, start_time, end_time FROM scaling_activities WHERE 1=1`
	var args []any
	if ns != "" {
		query += ` AND service_namespace=?`
		args = append(args, ns)
	}
	if resourceID != "" {
		query += ` AND resource_id=?`
		args = append(args, resourceID)
	}
	if dimension != "" {
		query += ` AND dimension=?`
		args = append(args, dimension)
	}
	query += ` ORDER BY start_time DESC`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ScalingActivity
	for rows.Next() {
		var a ScalingActivity
		var start, end int64
		if err := rows.Scan(&a.ID, &a.ServiceNamespace, &a.ResourceID, &a.Dimension, &a.Description, &a.Cause, &a.StatusCode, &a.StatusMessage, &start, &end); err != nil {
			return nil, err
		}
		a.StartTime = time.Unix(start, 0)
		a.EndTime = time.Unix(end, 0)
		out = append(out, a)
	}
	return out, rows.Err()
}

// --- Customized Metrics ---

type CustomizedMetric struct {
	ID               string
	ServiceNamespace string
	ResourceID       string
	Dimension        string
	MetricName       string
	Namespace        string
	Statistic        string
	Unit             string
	Config           string
	CreatedAt        time.Time
}

func (s *Store) PutCustomizedMetric(ns, resourceID, dimension, metricName, namespace, statistic, unit, config string) (*CustomizedMetric, error) {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO customized_metrics (id, service_namespace, resource_id, dimension, metric_name, namespace, statistic, unit, config, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, ns, resourceID, dimension, metricName, namespace, statistic, unit, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &CustomizedMetric{
		ID: id, ServiceNamespace: ns, ResourceID: resourceID, Dimension: dimension,
		MetricName: metricName, Namespace: namespace, Statistic: statistic, Unit: unit,
		Config: config, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) DeleteCustomizedMetric(id string) error {
	_, err := s.store.DB().Exec(`DELETE FROM customized_metrics WHERE id=?`, id)
	return err
}

func (s *Store) ListCustomizedMetrics(ns, resourceID, dimension string) ([]CustomizedMetric, error) {
	query := `SELECT id, service_namespace, resource_id, dimension, metric_name, namespace, statistic, unit, config, created_at FROM customized_metrics WHERE 1=1`
	var args []any
	if ns != "" {
		query += ` AND service_namespace=?`
		args = append(args, ns)
	}
	if resourceID != "" {
		query += ` AND resource_id=?`
		args = append(args, resourceID)
	}
	if dimension != "" {
		query += ` AND dimension=?`
		args = append(args, dimension)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []CustomizedMetric
	for rows.Next() {
		var m CustomizedMetric
		var createdAt int64
		if err := rows.Scan(&m.ID, &m.ServiceNamespace, &m.ResourceID, &m.Dimension, &m.MetricName, &m.Namespace, &m.Statistic, &m.Unit, &m.Config, &createdAt); err != nil {
			return nil, err
		}
		m.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, m)
	}
	return out, rows.Err()
}

// --- Predictive Forecasts ---

func (s *Store) SaveForecast(ns, resourceID, dimension, policyName, forecastData string) error {
	id := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO predictive_forecasts (id, service_namespace, resource_id, dimension, policy_name, forecast_data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, ns, resourceID, dimension, policyName, forecastData, now,
	)
	return err
}

// Tags exposes the tag store.
func (s *Store) Tags() *shared.TagStore {
	return s.tags
}
