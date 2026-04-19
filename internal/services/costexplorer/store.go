// SPDX-License-Identifier: Apache-2.0

// internal/services/costexplorer/store.go
package costexplorer

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errCategoryNotFound     = errors.New("cost category not found")
	errMonitorNotFound      = errors.New("anomaly monitor not found")
	errSubscriptionNotFound = errors.New("anomaly subscription not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS cost_categories (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL UNIQUE,
			rule_version    TEXT NOT NULL DEFAULT 'CostCategoryExpression.v1',
			rules           TEXT NOT NULL DEFAULT '[]',
			effective_start TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS anomaly_monitors (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			type            TEXT NOT NULL DEFAULT 'DIMENSIONAL',
			dimension       TEXT NOT NULL DEFAULT 'SERVICE',
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS anomaly_subscriptions (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			monitor_arns    TEXT NOT NULL DEFAULT '[]',
			threshold       REAL NOT NULL DEFAULT 0,
			frequency       TEXT NOT NULL DEFAULT 'DAILY',
			subscribers     TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL
		);
	`},
}

// ----- model types -----

type CostCategory struct {
	ARN            string
	Name           string
	RuleVersion    string
	Rules          string
	EffectiveStart string
	CreatedAt      time.Time
}

type AnomalyMonitor struct {
	ARN       string
	Name      string
	Type      string
	Dimension string
	Config    string
	CreatedAt time.Time
}

type AnomalySubscription struct {
	ARN         string
	Name        string
	MonitorARNs string
	Threshold   float64
	Frequency   string
	Subscribers string
	CreatedAt   time.Time
}

// ----- Store -----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "costexplorer.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ----- CostCategory -----

func (s *Store) CreateCostCategory(arn, name, ruleVersion, rules, effectiveStart string) (*CostCategory, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cost_categories (arn, name, rule_version, rules, effective_start, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, ruleVersion, rules, effectiveStart, now,
	)
	if err != nil {
		return nil, err
	}
	return &CostCategory{
		ARN: arn, Name: name, RuleVersion: ruleVersion,
		Rules: rules, EffectiveStart: effectiveStart, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCostCategoryByARN(arn string) (*CostCategory, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, rule_version, rules, effective_start, created_at
		 FROM cost_categories WHERE arn = ?`, arn)
	return scanCostCategory(row)
}

func (s *Store) ListCostCategories() ([]CostCategory, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, rule_version, rules, effective_start, created_at
		 FROM cost_categories ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cats []CostCategory
	for rows.Next() {
		c, err := scanCostCategory(rows)
		if err != nil {
			return nil, err
		}
		cats = append(cats, *c)
	}
	return cats, rows.Err()
}

func (s *Store) UpdateCostCategory(arn, ruleVersion, rules string) error {
	res, err := s.store.DB().Exec(
		`UPDATE cost_categories SET rule_version = ?, rules = ? WHERE arn = ?`,
		ruleVersion, rules, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCategoryNotFound
	}
	return nil
}

func (s *Store) DeleteCostCategory(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM cost_categories WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCategoryNotFound
	}
	return nil
}

// ----- AnomalyMonitor -----

func (s *Store) CreateAnomalyMonitor(arn, name, monType, dimension, config string) (*AnomalyMonitor, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO anomaly_monitors (arn, name, type, dimension, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, monType, dimension, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &AnomalyMonitor{
		ARN: arn, Name: name, Type: monType,
		Dimension: dimension, Config: config, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAnomalyMonitorByARN(arn string) (*AnomalyMonitor, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, type, dimension, config, created_at
		 FROM anomaly_monitors WHERE arn = ?`, arn)
	return scanAnomalyMonitor(row)
}

func (s *Store) ListAnomalyMonitors(arns []string) ([]AnomalyMonitor, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, type, dimension, config, created_at
		 FROM anomaly_monitors ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	arnSet := make(map[string]bool, len(arns))
	for _, a := range arns {
		arnSet[a] = true
	}
	var monitors []AnomalyMonitor
	for rows.Next() {
		m, err := scanAnomalyMonitor(rows)
		if err != nil {
			return nil, err
		}
		if len(arnSet) == 0 || arnSet[m.ARN] {
			monitors = append(monitors, *m)
		}
	}
	return monitors, rows.Err()
}

func (s *Store) UpdateAnomalyMonitor(arn, name string) error {
	res, err := s.store.DB().Exec(
		`UPDATE anomaly_monitors SET name = ? WHERE arn = ?`, name, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errMonitorNotFound
	}
	return nil
}

func (s *Store) DeleteAnomalyMonitor(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM anomaly_monitors WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errMonitorNotFound
	}
	return nil
}

// ----- AnomalySubscription -----

func (s *Store) CreateAnomalySubscription(arn, name, monitorARNs string, threshold float64, frequency, subscribers string) (*AnomalySubscription, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO anomaly_subscriptions (arn, name, monitor_arns, threshold, frequency, subscribers, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		arn, name, monitorARNs, threshold, frequency, subscribers, now,
	)
	if err != nil {
		return nil, err
	}
	return &AnomalySubscription{
		ARN: arn, Name: name, MonitorARNs: monitorARNs,
		Threshold: threshold, Frequency: frequency,
		Subscribers: subscribers, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAnomalySubscriptionByARN(arn string) (*AnomalySubscription, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, monitor_arns, threshold, frequency, subscribers, created_at
		 FROM anomaly_subscriptions WHERE arn = ?`, arn)
	return scanAnomalySubscription(row)
}

func (s *Store) ListAnomalySubscriptions(monitorARN string) ([]AnomalySubscription, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, monitor_arns, threshold, frequency, subscribers, created_at
		 FROM anomaly_subscriptions ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var subs []AnomalySubscription
	for rows.Next() {
		sub, err := scanAnomalySubscription(rows)
		if err != nil {
			return nil, err
		}
		subs = append(subs, *sub)
	}
	return subs, rows.Err()
}

func (s *Store) UpdateAnomalySubscription(arn, name, monitorARNs string, threshold float64, frequency, subscribers string) error {
	res, err := s.store.DB().Exec(
		`UPDATE anomaly_subscriptions SET name=?, monitor_arns=?, threshold=?, frequency=?, subscribers=? WHERE arn=?`,
		name, monitorARNs, threshold, frequency, subscribers, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSubscriptionNotFound
	}
	return nil
}

func (s *Store) DeleteAnomalySubscription(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM anomaly_subscriptions WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSubscriptionNotFound
	}
	return nil
}

// ----- scanner helpers -----

type scanner interface{ Scan(dest ...any) error }

func scanCostCategory(s scanner) (*CostCategory, error) {
	var c CostCategory
	var createdAt int64
	err := s.Scan(&c.ARN, &c.Name, &c.RuleVersion, &c.Rules, &c.EffectiveStart, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errCategoryNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanAnomalyMonitor(s scanner) (*AnomalyMonitor, error) {
	var m AnomalyMonitor
	var createdAt int64
	err := s.Scan(&m.ARN, &m.Name, &m.Type, &m.Dimension, &m.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errMonitorNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

func scanAnomalySubscription(s scanner) (*AnomalySubscription, error) {
	var sub AnomalySubscription
	var createdAt int64
	err := s.Scan(&sub.ARN, &sub.Name, &sub.MonitorARNs, &sub.Threshold, &sub.Frequency, &sub.Subscribers, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSubscriptionNotFound
		}
		return nil, err
	}
	sub.CreatedAt = time.Unix(createdAt, 0)
	return &sub, nil
}
