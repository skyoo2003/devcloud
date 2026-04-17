// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatchlogs/store.go
package cloudwatchlogs

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrLogGroupNotFound  = errors.New("log group not found")
	ErrLogStreamNotFound = errors.New("log stream not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS log_groups (
			name           TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			retention_days INTEGER NOT NULL DEFAULT 0,
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS log_streams (
			group_name     TEXT NOT NULL,
			stream_name    TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			first_event_ts INTEGER NOT NULL DEFAULT 0,
			last_event_ts  INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (group_name, stream_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS log_events (
			id             INTEGER PRIMARY KEY AUTOINCREMENT,
			group_name     TEXT NOT NULL,
			stream_name    TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			timestamp      INTEGER NOT NULL,
			message        TEXT NOT NULL,
			ingestion_time INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_log_events_lookup
			ON log_events (group_name, stream_name, account_id, timestamp);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS metric_filters (
			filter_name      TEXT NOT NULL,
			log_group_name   TEXT NOT NULL,
			account_id       TEXT NOT NULL,
			filter_pattern   TEXT NOT NULL,
			metric_name      TEXT NOT NULL,
			metric_namespace TEXT NOT NULL,
			metric_value     TEXT NOT NULL,
			default_value    REAL,
			created_at       DATETIME NOT NULL,
			PRIMARY KEY (filter_name, log_group_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS subscription_filters (
			filter_name     TEXT NOT NULL,
			log_group_name  TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			filter_pattern  TEXT NOT NULL,
			destination_arn TEXT NOT NULL,
			role_arn        TEXT,
			distribution    TEXT DEFAULT 'ByLogStream',
			created_at      DATETIME NOT NULL,
			PRIMARY KEY (filter_name, log_group_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS log_group_tags (
			log_group_name TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			tag_key        TEXT NOT NULL,
			tag_value      TEXT NOT NULL,
			PRIMARY KEY (log_group_name, tag_key, account_id)
		);
	`},
}

type LogGroup struct {
	Name          string
	AccountID     string
	RetentionDays int
	CreatedAt     time.Time
}

type LogStream struct {
	GroupName    string
	StreamName   string
	AccountID    string
	FirstEventTS int64
	LastEventTS  int64
}

type LogEvent struct {
	ID            int64
	GroupName     string
	StreamName    string
	AccountID     string
	Timestamp     int64
	Message       string
	IngestionTime int64
}

type LogsStore struct {
	store *sqlite.Store
}

func NewLogsStore(dataDir string) (*LogsStore, error) {
	dbPath := filepath.Join(dataDir, "cloudwatchlogs.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &LogsStore{store: s}, nil
}

func (s *LogsStore) Close() error { return s.store.Close() }

func (s *LogsStore) CreateLogGroup(name, accountID string) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO log_groups (name, account_id, retention_days, created_at) VALUES (?, ?, 0, ?)
		 ON CONFLICT(name, account_id) DO NOTHING`,
		name, accountID, now,
	)
	return err
}

func (s *LogsStore) DeleteLogGroup(name, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM log_groups WHERE name = ? AND account_id = ?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLogGroupNotFound
	}
	_, _ = s.store.DB().Exec(`DELETE FROM log_streams WHERE group_name = ? AND account_id = ?`, name, accountID)
	_, _ = s.store.DB().Exec(`DELETE FROM log_events WHERE group_name = ? AND account_id = ?`, name, accountID)
	return nil
}

func (s *LogsStore) DescribeLogGroups(accountID, prefix string) ([]LogGroup, error) {
	var rows *sql.Rows
	var err error
	if prefix != "" {
		rows, err = s.store.DB().Query(
			`SELECT name, account_id, retention_days, created_at FROM log_groups WHERE account_id = ? AND name LIKE ? ORDER BY name`,
			accountID, prefix+"%",
		)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT name, account_id, retention_days, created_at FROM log_groups WHERE account_id = ? ORDER BY name`,
			accountID,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []LogGroup
	for rows.Next() {
		var g LogGroup
		var createdAt int64
		if err := rows.Scan(&g.Name, &g.AccountID, &g.RetentionDays, &createdAt); err != nil {
			return nil, err
		}
		g.CreatedAt = time.Unix(createdAt, 0)
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

func (s *LogsStore) CreateLogStream(groupName, streamName, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO log_streams (group_name, stream_name, account_id, first_event_ts, last_event_ts) VALUES (?, ?, ?, 0, 0)
		 ON CONFLICT(group_name, stream_name, account_id) DO NOTHING`,
		groupName, streamName, accountID,
	)
	return err
}

func (s *LogsStore) DeleteLogStream(groupName, streamName, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM log_streams WHERE group_name = ? AND stream_name = ? AND account_id = ?`,
		groupName, streamName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLogStreamNotFound
	}
	_, _ = s.store.DB().Exec(
		`DELETE FROM log_events WHERE group_name = ? AND stream_name = ? AND account_id = ?`,
		groupName, streamName, accountID,
	)
	return nil
}

func (s *LogsStore) DescribeLogStreams(groupName, accountID string) ([]LogStream, error) {
	rows, err := s.store.DB().Query(
		`SELECT group_name, stream_name, account_id, first_event_ts, last_event_ts FROM log_streams WHERE group_name = ? AND account_id = ? ORDER BY stream_name`,
		groupName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var streams []LogStream
	for rows.Next() {
		var ls LogStream
		if err := rows.Scan(&ls.GroupName, &ls.StreamName, &ls.AccountID, &ls.FirstEventTS, &ls.LastEventTS); err != nil {
			return nil, err
		}
		streams = append(streams, ls)
	}
	return streams, rows.Err()
}

type InputLogEvent struct {
	Timestamp int64
	Message   string
}

func (s *LogsStore) PutLogEvents(groupName, streamName, accountID string, events []InputLogEvent) error {
	now := time.Now().UnixMilli()
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.Prepare(
		`INSERT INTO log_events (group_name, stream_name, account_id, timestamp, message, ingestion_time) VALUES (?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var minTS, maxTS int64
	for i, e := range events {
		if _, err := stmt.Exec(groupName, streamName, accountID, e.Timestamp, e.Message, now); err != nil {
			return err
		}
		if i == 0 || e.Timestamp < minTS {
			minTS = e.Timestamp
		}
		if i == 0 || e.Timestamp > maxTS {
			maxTS = e.Timestamp
		}
	}

	_, err = tx.Exec(
		`UPDATE log_streams SET
			first_event_ts = CASE WHEN first_event_ts = 0 OR ? < first_event_ts THEN ? ELSE first_event_ts END,
			last_event_ts  = CASE WHEN ? > last_event_ts THEN ? ELSE last_event_ts END
		 WHERE group_name = ? AND stream_name = ? AND account_id = ?`,
		minTS, minTS, maxTS, maxTS, groupName, streamName, accountID,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *LogsStore) GetLogEvents(groupName, streamName, accountID string, startTime, endTime int64, limit int) ([]LogEvent, error) {
	query := `SELECT id, group_name, stream_name, account_id, timestamp, message, ingestion_time FROM log_events WHERE group_name = ? AND stream_name = ? AND account_id = ?`
	args := []any{groupName, streamName, accountID}
	if startTime > 0 {
		query += " AND timestamp >= ?"
		args = append(args, startTime)
	}
	if endTime > 0 {
		query += " AND timestamp <= ?"
		args = append(args, endTime)
	}
	query += " ORDER BY timestamp"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (s *LogsStore) FilterLogEvents(groupName, accountID string, startTime, endTime int64, filterPattern string, limit int) ([]LogEvent, error) {
	query := `SELECT id, group_name, stream_name, account_id, timestamp, message, ingestion_time FROM log_events WHERE group_name = ? AND account_id = ?`
	args := []any{groupName, accountID}
	if startTime > 0 {
		query += " AND timestamp >= ?"
		args = append(args, startTime)
	}
	if endTime > 0 {
		query += " AND timestamp <= ?"
		args = append(args, endTime)
	}
	if filterPattern != "" {
		query += " AND message LIKE ?"
		args = append(args, "%"+filterPattern+"%")
	}
	query += " ORDER BY timestamp"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEvents(rows)
}

func scanEvents(rows *sql.Rows) ([]LogEvent, error) {
	var events []LogEvent
	for rows.Next() {
		var e LogEvent
		if err := rows.Scan(&e.ID, &e.GroupName, &e.StreamName, &e.AccountID, &e.Timestamp, &e.Message, &e.IngestionTime); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// ── Retention ────────────────────────────────────────────────────────────────

func (s *LogsStore) PutRetentionPolicy(logGroupName, accountID string, retentionDays int) error {
	res, err := s.store.DB().Exec(
		`UPDATE log_groups SET retention_days = ? WHERE name = ? AND account_id = ?`,
		retentionDays, logGroupName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrLogGroupNotFound
	}
	return nil
}

func (s *LogsStore) DeleteRetentionPolicy(logGroupName, accountID string) error {
	return s.PutRetentionPolicy(logGroupName, accountID, 0)
}

// ── Metric Filters ───────────────────────────────────────────────────────────

type MetricFilter struct {
	FilterName      string
	LogGroupName    string
	AccountID       string
	FilterPattern   string
	MetricName      string
	MetricNamespace string
	MetricValue     string
	DefaultValue    *float64
	CreatedAt       time.Time
}

func (s *LogsStore) PutMetricFilter(f MetricFilter) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO metric_filters (filter_name, log_group_name, account_id, filter_pattern, metric_name, metric_namespace, metric_value, default_value, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(filter_name, log_group_name, account_id) DO UPDATE SET
		   filter_pattern=excluded.filter_pattern,
		   metric_name=excluded.metric_name,
		   metric_namespace=excluded.metric_namespace,
		   metric_value=excluded.metric_value,
		   default_value=excluded.default_value,
		   created_at=excluded.created_at`,
		f.FilterName, f.LogGroupName, f.AccountID, f.FilterPattern,
		f.MetricName, f.MetricNamespace, f.MetricValue, f.DefaultValue, f.CreatedAt,
	)
	return err
}

func (s *LogsStore) DeleteMetricFilter(filterName, logGroupName, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM metric_filters WHERE filter_name = ? AND log_group_name = ? AND account_id = ?`,
		filterName, logGroupName, accountID,
	)
	return err
}

func (s *LogsStore) DescribeMetricFilters(accountID, logGroupName, filterNamePrefix string) ([]MetricFilter, error) {
	query := `SELECT filter_name, log_group_name, account_id, filter_pattern, metric_name, metric_namespace, metric_value, default_value, created_at FROM metric_filters WHERE account_id = ?`
	args := []any{accountID}
	if logGroupName != "" {
		query += " AND log_group_name = ?"
		args = append(args, logGroupName)
	}
	if filterNamePrefix != "" {
		query += " AND filter_name LIKE ?"
		args = append(args, filterNamePrefix+"%")
	}
	query += " ORDER BY filter_name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var filters []MetricFilter
	for rows.Next() {
		var f MetricFilter
		var createdAt string
		if err := rows.Scan(&f.FilterName, &f.LogGroupName, &f.AccountID, &f.FilterPattern,
			&f.MetricName, &f.MetricNamespace, &f.MetricValue, &f.DefaultValue, &createdAt); err != nil {
			return nil, err
		}
		f.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		filters = append(filters, f)
	}
	return filters, rows.Err()
}

func (s *LogsStore) GetMetricFiltersForGroup(logGroupName, accountID string) ([]MetricFilter, error) {
	return s.DescribeMetricFilters(accountID, logGroupName, "")
}

// ── Subscription Filters ─────────────────────────────────────────────────────

type SubscriptionFilter struct {
	FilterName     string
	LogGroupName   string
	AccountID      string
	FilterPattern  string
	DestinationARN string
	RoleARN        string
	Distribution   string
	CreatedAt      time.Time
}

func (s *LogsStore) PutSubscriptionFilter(f SubscriptionFilter) error {
	dist := f.Distribution
	if dist == "" {
		dist = "ByLogStream"
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO subscription_filters (filter_name, log_group_name, account_id, filter_pattern, destination_arn, role_arn, distribution, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(filter_name, log_group_name, account_id) DO UPDATE SET
		   filter_pattern=excluded.filter_pattern,
		   destination_arn=excluded.destination_arn,
		   role_arn=excluded.role_arn,
		   distribution=excluded.distribution,
		   created_at=excluded.created_at`,
		f.FilterName, f.LogGroupName, f.AccountID, f.FilterPattern,
		f.DestinationARN, f.RoleARN, dist, f.CreatedAt,
	)
	return err
}

func (s *LogsStore) DeleteSubscriptionFilter(filterName, logGroupName, accountID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM subscription_filters WHERE filter_name = ? AND log_group_name = ? AND account_id = ?`,
		filterName, logGroupName, accountID,
	)
	return err
}

func (s *LogsStore) DescribeSubscriptionFilters(accountID, logGroupName string) ([]SubscriptionFilter, error) {
	rows, err := s.store.DB().Query(
		`SELECT filter_name, log_group_name, account_id, filter_pattern, destination_arn, COALESCE(role_arn,''), distribution, created_at
		 FROM subscription_filters WHERE account_id = ? AND log_group_name = ? ORDER BY filter_name`,
		accountID, logGroupName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var filters []SubscriptionFilter
	for rows.Next() {
		var f SubscriptionFilter
		var createdAt string
		if err := rows.Scan(&f.FilterName, &f.LogGroupName, &f.AccountID, &f.FilterPattern,
			&f.DestinationARN, &f.RoleARN, &f.Distribution, &createdAt); err != nil {
			return nil, err
		}
		f.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		filters = append(filters, f)
	}
	return filters, rows.Err()
}

func (s *LogsStore) GetSubscriptionFiltersForGroup(logGroupName, accountID string) ([]SubscriptionFilter, error) {
	return s.DescribeSubscriptionFilters(accountID, logGroupName)
}

// ── Tags ──────────────────────────────────────────────────────────────────────

func (s *LogsStore) TagLogGroup(logGroupName, accountID string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO log_group_tags (log_group_name, account_id, tag_key, tag_value)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(log_group_name, tag_key, account_id) DO UPDATE SET tag_value=excluded.tag_value`,
			logGroupName, accountID, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *LogsStore) UntagLogGroup(logGroupName, accountID string, tagKeys []string) error {
	for _, k := range tagKeys {
		_, err := s.store.DB().Exec(
			`DELETE FROM log_group_tags WHERE log_group_name = ? AND account_id = ? AND tag_key = ?`,
			logGroupName, accountID, k,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *LogsStore) ListTagsForLogGroup(logGroupName, accountID string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM log_group_tags WHERE log_group_name = ? AND account_id = ? ORDER BY tag_key`,
		logGroupName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}
