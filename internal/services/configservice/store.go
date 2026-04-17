// SPDX-License-Identifier: Apache-2.0

// internal/services/configservice/store.go
package configservice

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errConfigRuleNotFound      = errors.New("config rule not found")
	errRecorderNotFound        = errors.New("configuration recorder not found")
	errDeliveryChannelNotFound = errors.New("delivery channel not found")
	errConformancePackNotFound = errors.New("conformance pack not found")
	errAggregatorNotFound      = errors.New("aggregator not found")
	errStoredQueryNotFound     = errors.New("stored query not found")
	errRetentionConfigNotFound = errors.New("retention configuration not found")
	errAggAuthNotFound         = errors.New("aggregation authorization not found")
	errRemediationNotFound     = errors.New("remediation configuration not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS config_rules (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			source      TEXT NOT NULL DEFAULT '{}',
			scope       TEXT NOT NULL DEFAULT '{}',
			input_params TEXT NOT NULL DEFAULT '',
			state       TEXT NOT NULL DEFAULT 'ACTIVE',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS configuration_recorders (
			name           TEXT PRIMARY KEY,
			role_arn       TEXT NOT NULL DEFAULT '',
			recording_group TEXT NOT NULL DEFAULT '{}',
			recording_mode  TEXT NOT NULL DEFAULT '{}',
			status         TEXT NOT NULL DEFAULT 'STOPPED'
		);
		CREATE TABLE IF NOT EXISTS delivery_channels (
			name       TEXT PRIMARY KEY,
			s3_bucket  TEXT NOT NULL DEFAULT '',
			s3_prefix  TEXT NOT NULL DEFAULT '',
			sns_topic  TEXT NOT NULL DEFAULT '',
			frequency  TEXT NOT NULL DEFAULT 'TwentyFour_Hours'
		);
		CREATE TABLE IF NOT EXISTS conformance_packs (
			name           TEXT PRIMARY KEY,
			arn            TEXT NOT NULL UNIQUE,
			template_body  TEXT NOT NULL DEFAULT '',
			delivery_bucket TEXT NOT NULL DEFAULT '',
			status         TEXT NOT NULL DEFAULT 'CREATE_COMPLETE',
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS aggregators (
			name           TEXT PRIMARY KEY,
			arn            TEXT NOT NULL UNIQUE,
			account_sources TEXT NOT NULL DEFAULT '[]',
			org_source     TEXT NOT NULL DEFAULT '{}',
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS stored_queries (
			name        TEXT PRIMARY KEY,
			id          TEXT NOT NULL UNIQUE,
			arn         TEXT NOT NULL UNIQUE,
			expression  TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS retention_configs (
			name           TEXT PRIMARY KEY DEFAULT 'default',
			retention_days INTEGER NOT NULL DEFAULT 2557
		);
		CREATE TABLE IF NOT EXISTS aggregation_authorizations (
			arn               TEXT PRIMARY KEY,
			authorized_account TEXT NOT NULL,
			authorized_region  TEXT NOT NULL,
			created_at        INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS remediation_configs (
			config_rule_name TEXT PRIMARY KEY,
			target_type      TEXT NOT NULL DEFAULT 'SSM_DOCUMENT',
			target_id        TEXT NOT NULL DEFAULT '',
			parameters       TEXT NOT NULL DEFAULT '{}',
			automatic        INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS resource_tags (
			resource_arn TEXT NOT NULL,
			key          TEXT NOT NULL,
			value        TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (resource_arn, key)
		);
	`},
}

// ---- model types ----

type ConfigRule struct {
	Name        string
	ARN         string
	Source      string
	Scope       string
	InputParams string
	State       string
	CreatedAt   int64
}

type ConfigurationRecorder struct {
	Name           string
	RoleARN        string
	RecordingGroup string
	RecordingMode  string
	Status         string
}

type DeliveryChannel struct {
	Name      string
	S3Bucket  string
	S3Prefix  string
	SNSTopic  string
	Frequency string
}

type ConformancePack struct {
	Name           string
	ARN            string
	TemplateBody   string
	DeliveryBucket string
	Status         string
	CreatedAt      int64
}

type Aggregator struct {
	Name           string
	ARN            string
	AccountSources string
	OrgSource      string
	CreatedAt      int64
}

type StoredQuery struct {
	Name        string
	ID          string
	ARN         string
	Expression  string
	Description string
}

type RetentionConfig struct {
	Name          string
	RetentionDays int
}

type AggregationAuthorization struct {
	ARN               string
	AuthorizedAccount string
	AuthorizedRegion  string
	CreatedAt         int64
}

type RemediationConfig struct {
	ConfigRuleName string
	TargetType     string
	TargetID       string
	Parameters     string
	Automatic      int
}

// ---- Store ----

type Store struct {
	db   *sqlite.Store
	tags *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "configservice.db")
	db, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, tags: shared.NewTagStore(db)}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func sqliteUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

func now() int64 { return time.Now().Unix() }

// ---- ConfigRule ----

func (s *Store) PutConfigRule(r *ConfigRule) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO config_rules (name, arn, source, scope, input_params, state, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			arn=excluded.arn, source=excluded.source, scope=excluded.scope,
			input_params=excluded.input_params, state=excluded.state`,
		r.Name, r.ARN, r.Source, r.Scope, r.InputParams, r.State, r.CreatedAt)
	return err
}

func scanConfigRule(row shared.Scanner) (*ConfigRule, error) {
	r := &ConfigRule{}
	err := row.Scan(&r.Name, &r.ARN, &r.Source, &r.Scope, &r.InputParams, &r.State, &r.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, errConfigRuleNotFound
	}
	return r, err
}

func (s *Store) GetConfigRule(name string) (*ConfigRule, error) {
	row := s.db.DB().QueryRow(
		`SELECT name, arn, source, scope, input_params, state, created_at FROM config_rules WHERE name = ?`, name)
	return scanConfigRule(row)
}

func (s *Store) ListConfigRules(names []string) ([]*ConfigRule, error) {
	var rows *sql.Rows
	var err error
	if len(names) > 0 {
		placeholders := strings.Repeat("?,", len(names))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(names))
		for i, n := range names {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT name, arn, source, scope, input_params, state, created_at FROM config_rules WHERE name IN (`+placeholders+`) ORDER BY name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT name, arn, source, scope, input_params, state, created_at FROM config_rules ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ConfigRule
	for rows.Next() {
		r, err := scanConfigRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteConfigRule(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM config_rules WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- ConfigurationRecorder ----

func (s *Store) PutConfigurationRecorder(r *ConfigurationRecorder) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO configuration_recorders (name, role_arn, recording_group, recording_mode, status)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			role_arn=excluded.role_arn, recording_group=excluded.recording_group,
			recording_mode=excluded.recording_mode`,
		r.Name, r.RoleARN, r.RecordingGroup, r.RecordingMode, r.Status)
	return err
}

func scanRecorder(row shared.Scanner) (*ConfigurationRecorder, error) {
	r := &ConfigurationRecorder{}
	err := row.Scan(&r.Name, &r.RoleARN, &r.RecordingGroup, &r.RecordingMode, &r.Status)
	if err == sql.ErrNoRows {
		return nil, errRecorderNotFound
	}
	return r, err
}

func (s *Store) GetConfigurationRecorder(name string) (*ConfigurationRecorder, error) {
	row := s.db.DB().QueryRow(
		`SELECT name, role_arn, recording_group, recording_mode, status FROM configuration_recorders WHERE name = ?`, name)
	return scanRecorder(row)
}

func (s *Store) ListConfigurationRecorders(names []string) ([]*ConfigurationRecorder, error) {
	var rows *sql.Rows
	var err error
	if len(names) > 0 {
		placeholders := strings.Repeat("?,", len(names))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(names))
		for i, n := range names {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT name, role_arn, recording_group, recording_mode, status FROM configuration_recorders WHERE name IN (`+placeholders+`) ORDER BY name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT name, role_arn, recording_group, recording_mode, status FROM configuration_recorders ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ConfigurationRecorder
	for rows.Next() {
		r, err := scanRecorder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) UpdateRecorderStatus(name, status string) error {
	res, err := s.db.DB().Exec(`UPDATE configuration_recorders SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRecorderNotFound
	}
	return nil
}

func (s *Store) DeleteConfigurationRecorder(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM configuration_recorders WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- DeliveryChannel ----

func (s *Store) PutDeliveryChannel(d *DeliveryChannel) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO delivery_channels (name, s3_bucket, s3_prefix, sns_topic, frequency)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			s3_bucket=excluded.s3_bucket, s3_prefix=excluded.s3_prefix,
			sns_topic=excluded.sns_topic, frequency=excluded.frequency`,
		d.Name, d.S3Bucket, d.S3Prefix, d.SNSTopic, d.Frequency)
	return err
}

func scanDeliveryChannel(row shared.Scanner) (*DeliveryChannel, error) {
	d := &DeliveryChannel{}
	err := row.Scan(&d.Name, &d.S3Bucket, &d.S3Prefix, &d.SNSTopic, &d.Frequency)
	if err == sql.ErrNoRows {
		return nil, errDeliveryChannelNotFound
	}
	return d, err
}

func (s *Store) ListDeliveryChannels(names []string) ([]*DeliveryChannel, error) {
	var rows *sql.Rows
	var err error
	if len(names) > 0 {
		placeholders := strings.Repeat("?,", len(names))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(names))
		for i, n := range names {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT name, s3_bucket, s3_prefix, sns_topic, frequency FROM delivery_channels WHERE name IN (`+placeholders+`) ORDER BY name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT name, s3_bucket, s3_prefix, sns_topic, frequency FROM delivery_channels ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DeliveryChannel
	for rows.Next() {
		d, err := scanDeliveryChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DeleteDeliveryChannel(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM delivery_channels WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- ConformancePack ----

func (s *Store) PutConformancePack(c *ConformancePack) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO conformance_packs (name, arn, template_body, delivery_bucket, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			template_body=excluded.template_body, delivery_bucket=excluded.delivery_bucket,
			status=excluded.status`,
		c.Name, c.ARN, c.TemplateBody, c.DeliveryBucket, c.Status, c.CreatedAt)
	return err
}

func scanConformancePack(row shared.Scanner) (*ConformancePack, error) {
	c := &ConformancePack{}
	err := row.Scan(&c.Name, &c.ARN, &c.TemplateBody, &c.DeliveryBucket, &c.Status, &c.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, errConformancePackNotFound
	}
	return c, err
}

func (s *Store) ListConformancePacks(names []string) ([]*ConformancePack, error) {
	var rows *sql.Rows
	var err error
	if len(names) > 0 {
		placeholders := strings.Repeat("?,", len(names))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(names))
		for i, n := range names {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT name, arn, template_body, delivery_bucket, status, created_at FROM conformance_packs WHERE name IN (`+placeholders+`) ORDER BY name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT name, arn, template_body, delivery_bucket, status, created_at FROM conformance_packs ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ConformancePack
	for rows.Next() {
		c, err := scanConformancePack(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) DeleteConformancePack(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM conformance_packs WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- ConfigurationAggregator ----

func (s *Store) PutAggregator(a *Aggregator) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO aggregators (name, arn, account_sources, org_source, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			account_sources=excluded.account_sources, org_source=excluded.org_source`,
		a.Name, a.ARN, a.AccountSources, a.OrgSource, a.CreatedAt)
	return err
}

func scanAggregator(row shared.Scanner) (*Aggregator, error) {
	a := &Aggregator{}
	err := row.Scan(&a.Name, &a.ARN, &a.AccountSources, &a.OrgSource, &a.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, errAggregatorNotFound
	}
	return a, err
}

func (s *Store) GetAggregator(name string) (*Aggregator, error) {
	row := s.db.DB().QueryRow(
		`SELECT name, arn, account_sources, org_source, created_at FROM aggregators WHERE name = ?`, name)
	return scanAggregator(row)
}

func (s *Store) ListAggregators(names []string) ([]*Aggregator, error) {
	var rows *sql.Rows
	var err error
	if len(names) > 0 {
		placeholders := strings.Repeat("?,", len(names))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(names))
		for i, n := range names {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT name, arn, account_sources, org_source, created_at FROM aggregators WHERE name IN (`+placeholders+`) ORDER BY name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT name, arn, account_sources, org_source, created_at FROM aggregators ORDER BY name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Aggregator
	for rows.Next() {
		a, err := scanAggregator(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAggregator(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM aggregators WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- StoredQuery ----

func (s *Store) PutStoredQuery(q *StoredQuery) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO stored_queries (name, id, arn, expression, description)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			expression=excluded.expression, description=excluded.description`,
		q.Name, q.ID, q.ARN, q.Expression, q.Description)
	return err
}

func scanStoredQuery(row shared.Scanner) (*StoredQuery, error) {
	q := &StoredQuery{}
	err := row.Scan(&q.Name, &q.ID, &q.ARN, &q.Expression, &q.Description)
	if err == sql.ErrNoRows {
		return nil, errStoredQueryNotFound
	}
	return q, err
}

func (s *Store) GetStoredQuery(name string) (*StoredQuery, error) {
	row := s.db.DB().QueryRow(
		`SELECT name, id, arn, expression, description FROM stored_queries WHERE name = ?`, name)
	return scanStoredQuery(row)
}

func (s *Store) ListStoredQueries() ([]*StoredQuery, error) {
	rows, err := s.db.DB().Query(
		`SELECT name, id, arn, expression, description FROM stored_queries ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*StoredQuery
	for rows.Next() {
		q, err := scanStoredQuery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Store) DeleteStoredQuery(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM stored_queries WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- RetentionConfiguration ----

func (s *Store) PutRetentionConfig(r *RetentionConfig) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO retention_configs (name, retention_days) VALUES (?, ?)
		ON CONFLICT(name) DO UPDATE SET retention_days=excluded.retention_days`,
		r.Name, r.RetentionDays)
	return err
}

func scanRetentionConfig(row shared.Scanner) (*RetentionConfig, error) {
	r := &RetentionConfig{}
	err := row.Scan(&r.Name, &r.RetentionDays)
	if err == sql.ErrNoRows {
		return nil, errRetentionConfigNotFound
	}
	return r, err
}

func (s *Store) GetRetentionConfig(name string) (*RetentionConfig, error) {
	row := s.db.DB().QueryRow(
		`SELECT name, retention_days FROM retention_configs WHERE name = ?`, name)
	return scanRetentionConfig(row)
}

func (s *Store) ListRetentionConfigs() ([]*RetentionConfig, error) {
	rows, err := s.db.DB().Query(
		`SELECT name, retention_days FROM retention_configs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RetentionConfig
	for rows.Next() {
		r, err := scanRetentionConfig(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteRetentionConfig(name string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM retention_configs WHERE name = ?`, name)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- AggregationAuthorization ----

func (s *Store) PutAggregationAuthorization(a *AggregationAuthorization) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO aggregation_authorizations (arn, authorized_account, authorized_region, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(arn) DO UPDATE SET
			authorized_account=excluded.authorized_account, authorized_region=excluded.authorized_region`,
		a.ARN, a.AuthorizedAccount, a.AuthorizedRegion, a.CreatedAt)
	return err
}

func scanAggAuth(row shared.Scanner) (*AggregationAuthorization, error) {
	a := &AggregationAuthorization{}
	err := row.Scan(&a.ARN, &a.AuthorizedAccount, &a.AuthorizedRegion, &a.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, errAggAuthNotFound
	}
	return a, err
}

func (s *Store) ListAggregationAuthorizations() ([]*AggregationAuthorization, error) {
	rows, err := s.db.DB().Query(
		`SELECT arn, authorized_account, authorized_region, created_at FROM aggregation_authorizations ORDER BY arn`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AggregationAuthorization
	for rows.Next() {
		a, err := scanAggAuth(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAggregationAuthorization(account, region string) (bool, error) {
	arn := shared.BuildARNWithAccount("config", "aggregation-authorization", account+"/"+region, region, account)
	res, err := s.db.DB().Exec(`DELETE FROM aggregation_authorizations WHERE authorized_account = ? AND authorized_region = ?`, account, region)
	_ = arn
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- RemediationConfiguration ----

func (s *Store) PutRemediationConfig(r *RemediationConfig) error {
	_, err := s.db.DB().Exec(`
		INSERT INTO remediation_configs (config_rule_name, target_type, target_id, parameters, automatic)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(config_rule_name) DO UPDATE SET
			target_type=excluded.target_type, target_id=excluded.target_id,
			parameters=excluded.parameters, automatic=excluded.automatic`,
		r.ConfigRuleName, r.TargetType, r.TargetID, r.Parameters, r.Automatic)
	return err
}

func scanRemediationConfig(row shared.Scanner) (*RemediationConfig, error) {
	r := &RemediationConfig{}
	err := row.Scan(&r.ConfigRuleName, &r.TargetType, &r.TargetID, &r.Parameters, &r.Automatic)
	if err == sql.ErrNoRows {
		return nil, errRemediationNotFound
	}
	return r, err
}

func (s *Store) ListRemediationConfigs(ruleNames []string) ([]*RemediationConfig, error) {
	var rows *sql.Rows
	var err error
	if len(ruleNames) > 0 {
		placeholders := strings.Repeat("?,", len(ruleNames))
		placeholders = placeholders[:len(placeholders)-1]
		args := make([]any, len(ruleNames))
		for i, n := range ruleNames {
			args[i] = n
		}
		rows, err = s.db.DB().Query(
			`SELECT config_rule_name, target_type, target_id, parameters, automatic FROM remediation_configs WHERE config_rule_name IN (`+placeholders+`) ORDER BY config_rule_name`,
			args...)
	} else {
		rows, err = s.db.DB().Query(
			`SELECT config_rule_name, target_type, target_id, parameters, automatic FROM remediation_configs ORDER BY config_rule_name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RemediationConfig
	for rows.Next() {
		r, err := scanRemediationConfig(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteRemediationConfig(ruleName string) (bool, error) {
	res, err := s.db.DB().Exec(`DELETE FROM remediation_configs WHERE config_rule_name = ?`, ruleName)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- Tags ----

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}
