// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudwatch/store.go
package cloudwatch

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrAlarmNotFound = errors.New("alarm not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS metrics (
			namespace   TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			dimensions  TEXT NOT NULL DEFAULT '[]',
			account_id  TEXT NOT NULL,
			PRIMARY KEY (namespace, metric_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS metric_data (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace   TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			timestamp   INTEGER NOT NULL,
			value       REAL NOT NULL,
			unit        TEXT NOT NULL DEFAULT 'None'
		);
		CREATE INDEX IF NOT EXISTS idx_metric_data_lookup
			ON metric_data (namespace, metric_name, account_id, timestamp);
		CREATE TABLE IF NOT EXISTS alarms (
			alarm_name      TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			namespace       TEXT NOT NULL,
			metric_name     TEXT NOT NULL,
			statistic       TEXT NOT NULL DEFAULT 'Average',
			period          INTEGER NOT NULL DEFAULT 60,
			eval_periods    INTEGER NOT NULL DEFAULT 1,
			threshold       REAL NOT NULL DEFAULT 0,
			comparison      TEXT NOT NULL,
			state           TEXT NOT NULL DEFAULT 'INSUFFICIENT_DATA',
			state_reason    TEXT NOT NULL DEFAULT '',
			actions_enabled INTEGER NOT NULL DEFAULT 1,
			updated_at      INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (alarm_name, account_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS dashboards (
			dashboard_name TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			dashboard_body TEXT NOT NULL,
			created_at     DATETIME NOT NULL,
			PRIMARY KEY (dashboard_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS composite_alarms (
			alarm_name      TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			alarm_rule      TEXT NOT NULL,
			state           TEXT NOT NULL DEFAULT 'OK',
			state_reason    TEXT NOT NULL DEFAULT '',
			actions_enabled INTEGER NOT NULL DEFAULT 1,
			created_at      DATETIME NOT NULL,
			PRIMARY KEY (alarm_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS alarm_history (
			id                INTEGER PRIMARY KEY AUTOINCREMENT,
			alarm_name        TEXT NOT NULL,
			account_id        TEXT NOT NULL,
			timestamp         DATETIME NOT NULL,
			history_item_type TEXT NOT NULL,
			summary           TEXT,
			history_data      TEXT
		);
		CREATE TABLE IF NOT EXISTS anomaly_detectors (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			namespace   TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			stat        TEXT NOT NULL,
			dimensions  TEXT NOT NULL DEFAULT '',
			account_id  TEXT NOT NULL,
			configuration TEXT NOT NULL DEFAULT '',
			created_at  DATETIME NOT NULL,
			UNIQUE (namespace, metric_name, stat, dimensions, account_id)
		);
	`},
}

type MetricDatum struct {
	Namespace  string
	MetricName string
	Dimensions []Dimension
	Timestamp  int64
	Value      float64
	Unit       string
}

type Dimension struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

type MetricIdentifier struct {
	Namespace  string
	MetricName string
	Dimensions []Dimension
}

type Alarm struct {
	AlarmName      string
	AccountID      string
	Namespace      string
	MetricName     string
	Statistic      string
	Period         int
	EvalPeriods    int
	Threshold      float64
	Comparison     string
	State          string
	StateReason    string
	ActionsEnabled bool
	UpdatedAt      int64
}

type MetricDataPoint struct {
	Timestamp float64
	Value     float64
	Unit      string
}

type CWStore struct {
	store *sqlite.Store
}

func NewCWStore(dataDir string) (*CWStore, error) {
	dbPath := filepath.Join(dataDir, "cloudwatch.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &CWStore{store: s}, nil
}

func (s *CWStore) Close() error { return s.store.Close() }

func (s *CWStore) PutMetricData(accountID string, data []MetricDatum) error {
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for _, d := range data {
		dimsJSON, _ := json.Marshal(d.Dimensions)
		ts := d.Timestamp
		if ts == 0 {
			ts = time.Now().Unix()
		}
		_, err := tx.Exec(
			`INSERT INTO metric_data (namespace, metric_name, account_id, timestamp, value, unit) VALUES (?, ?, ?, ?, ?, ?)`,
			d.Namespace, d.MetricName, accountID, ts, d.Value, d.Unit,
		)
		if err != nil {
			return err
		}
		_, _ = tx.Exec(
			`INSERT INTO metrics (namespace, metric_name, dimensions, account_id) VALUES (?, ?, ?, ?)
			 ON CONFLICT(namespace, metric_name, account_id) DO UPDATE SET dimensions = excluded.dimensions`,
			d.Namespace, d.MetricName, string(dimsJSON), accountID,
		)
	}
	return tx.Commit()
}

func (s *CWStore) ListMetrics(accountID, namespace, metricName string) ([]MetricIdentifier, error) {
	query := `SELECT namespace, metric_name, dimensions FROM metrics WHERE account_id = ?`
	args := []any{accountID}
	if namespace != "" {
		query += " AND namespace = ?"
		args = append(args, namespace)
	}
	if metricName != "" {
		query += " AND metric_name = ?"
		args = append(args, metricName)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []MetricIdentifier
	for rows.Next() {
		var m MetricIdentifier
		var dimsJSON string
		if err := rows.Scan(&m.Namespace, &m.MetricName, &dimsJSON); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(dimsJSON), &m.Dimensions)
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}

func (s *CWStore) GetMetricData(accountID, namespace, metricName string, startTime, endTime int64, period int) ([]MetricDataPoint, error) {
	rows, err := s.store.DB().Query(
		`SELECT timestamp, value, unit FROM metric_data WHERE namespace = ? AND metric_name = ? AND account_id = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp`,
		namespace, metricName, accountID, startTime, endTime,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []MetricDataPoint
	for rows.Next() {
		var dp MetricDataPoint
		if err := rows.Scan(&dp.Timestamp, &dp.Value, &dp.Unit); err != nil {
			return nil, err
		}
		points = append(points, dp)
	}
	return points, rows.Err()
}

func (s *CWStore) PutMetricAlarm(a Alarm) error {
	actionsEnabled := 0
	if a.ActionsEnabled {
		actionsEnabled = 1
	}
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO alarms (alarm_name, account_id, namespace, metric_name, statistic, period, eval_periods, threshold, comparison, state, state_reason, actions_enabled, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'INSUFFICIENT_DATA', '', ?, ?)
		 ON CONFLICT(alarm_name, account_id) DO UPDATE SET
			namespace = excluded.namespace, metric_name = excluded.metric_name,
			statistic = excluded.statistic, period = excluded.period,
			eval_periods = excluded.eval_periods,
			threshold = excluded.threshold, comparison = excluded.comparison,
			actions_enabled = excluded.actions_enabled,
			updated_at = excluded.updated_at`,
		a.AlarmName, a.AccountID, a.Namespace, a.MetricName, a.Statistic, a.Period, a.EvalPeriods, a.Threshold, a.Comparison, actionsEnabled, now,
	)
	return err
}

func (s *CWStore) DescribeAlarms(accountID string, alarmNames []string) ([]Alarm, error) {
	var rows *sql.Rows
	var err error
	if len(alarmNames) > 0 {
		placeholders := make([]string, len(alarmNames))
		args := make([]any, 0, len(alarmNames)+1)
		args = append(args, accountID)
		for i, n := range alarmNames {
			placeholders[i] = "?"
			args = append(args, n)
		}
		query := `SELECT alarm_name, account_id, namespace, metric_name, statistic, period, eval_periods, threshold, comparison, state, state_reason, actions_enabled FROM alarms WHERE account_id = ? AND alarm_name IN (` + strings.Join(placeholders, ",") + `)`
		rows, err = s.store.DB().Query(query, args...)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT alarm_name, account_id, namespace, metric_name, statistic, period, eval_periods, threshold, comparison, state, state_reason, actions_enabled FROM alarms WHERE account_id = ? ORDER BY alarm_name`,
			accountID,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAlarms(rows)
}

func (s *CWStore) DeleteAlarms(accountID string, alarmNames []string) error {
	for _, name := range alarmNames {
		_, err := s.store.DB().Exec(`DELETE FROM alarms WHERE alarm_name = ? AND account_id = ?`, name, accountID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CWStore) SetAlarmState(accountID, alarmName, state, reason string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE alarms SET state = ?, state_reason = ?, updated_at = ? WHERE alarm_name = ? AND account_id = ?`,
		state, reason, now, alarmName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrAlarmNotFound
	}
	return nil
}

func (s *CWStore) SetAlarmActionsEnabled(accountID, alarmName string, enabled bool) error {
	v := 0
	if enabled {
		v = 1
	}
	_, err := s.store.DB().Exec(
		`UPDATE alarms SET actions_enabled = ? WHERE alarm_name = ? AND account_id = ?`,
		v, alarmName, accountID,
	)
	return err
}

func scanAlarms(rows *sql.Rows) ([]Alarm, error) {
	var alarms []Alarm
	for rows.Next() {
		var a Alarm
		var actionsEnabled int
		if err := rows.Scan(&a.AlarmName, &a.AccountID, &a.Namespace, &a.MetricName, &a.Statistic, &a.Period, &a.EvalPeriods, &a.Threshold, &a.Comparison, &a.State, &a.StateReason, &actionsEnabled); err != nil {
			return nil, err
		}
		a.ActionsEnabled = actionsEnabled == 1
		alarms = append(alarms, a)
	}
	return alarms, rows.Err()
}

// --- Dashboard types and methods ---

type Dashboard struct {
	DashboardName string
	DashboardBody string
	CreatedAt     string
}

func (s *CWStore) PutDashboard(accountID, name, body string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.store.DB().Exec(
		`INSERT INTO dashboards (dashboard_name, account_id, dashboard_body, created_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(dashboard_name, account_id) DO UPDATE SET
		 	dashboard_body = excluded.dashboard_body`,
		name, accountID, body, now,
	)
	return err
}

func (s *CWStore) GetDashboard(accountID, name string) (*Dashboard, error) {
	row := s.store.DB().QueryRow(
		`SELECT dashboard_name, dashboard_body, created_at FROM dashboards WHERE dashboard_name = ? AND account_id = ?`,
		name, accountID,
	)
	var d Dashboard
	if err := row.Scan(&d.DashboardName, &d.DashboardBody, &d.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &d, nil
}

func (s *CWStore) DeleteDashboards(accountID string, names []string) error {
	for _, name := range names {
		if _, err := s.store.DB().Exec(
			`DELETE FROM dashboards WHERE dashboard_name = ? AND account_id = ?`,
			name, accountID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *CWStore) ListDashboards(accountID, prefix string) ([]Dashboard, error) {
	query := `SELECT dashboard_name, dashboard_body, created_at FROM dashboards WHERE account_id = ?`
	args := []any{accountID}
	if prefix != "" {
		query += " AND dashboard_name LIKE ?"
		args = append(args, prefix+"%")
	}
	query += " ORDER BY dashboard_name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dashboards []Dashboard
	for rows.Next() {
		var d Dashboard
		if err := rows.Scan(&d.DashboardName, &d.DashboardBody, &d.CreatedAt); err != nil {
			return nil, err
		}
		dashboards = append(dashboards, d)
	}
	return dashboards, rows.Err()
}

// --- Composite alarm types and methods ---

type CompositeAlarm struct {
	AlarmName      string
	AlarmRule      string
	State          string
	StateReason    string
	ActionsEnabled bool
	CreatedAt      string
}

func (s *CWStore) PutCompositeAlarm(accountID string, a CompositeAlarm) error {
	actionsEnabled := 0
	if a.ActionsEnabled {
		actionsEnabled = 1
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.store.DB().Exec(
		`INSERT INTO composite_alarms (alarm_name, account_id, alarm_rule, state, state_reason, actions_enabled, created_at)
		 VALUES (?, ?, ?, 'OK', '', ?, ?)
		 ON CONFLICT(alarm_name, account_id) DO UPDATE SET
		 	alarm_rule = excluded.alarm_rule,
		 	actions_enabled = excluded.actions_enabled`,
		a.AlarmName, accountID, a.AlarmRule, actionsEnabled, now,
	)
	return err
}

func (s *CWStore) DescribeCompositeAlarms(accountID string, alarmNames []string) ([]CompositeAlarm, error) {
	var rows *sql.Rows
	var err error
	if len(alarmNames) > 0 {
		placeholders := make([]string, len(alarmNames))
		args := make([]any, 0, len(alarmNames)+1)
		args = append(args, accountID)
		for i, n := range alarmNames {
			placeholders[i] = "?"
			args = append(args, n)
		}
		query := `SELECT alarm_name, alarm_rule, state, state_reason, actions_enabled, created_at FROM composite_alarms WHERE account_id = ? AND alarm_name IN (` + strings.Join(placeholders, ",") + `)`
		rows, err = s.store.DB().Query(query, args...)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT alarm_name, alarm_rule, state, state_reason, actions_enabled, created_at FROM composite_alarms WHERE account_id = ? ORDER BY alarm_name`,
			accountID,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var alarms []CompositeAlarm
	for rows.Next() {
		var a CompositeAlarm
		var actionsEnabled int
		if err := rows.Scan(&a.AlarmName, &a.AlarmRule, &a.State, &a.StateReason, &actionsEnabled, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.ActionsEnabled = actionsEnabled == 1
		alarms = append(alarms, a)
	}
	return alarms, rows.Err()
}

// --- Alarm history types and methods ---

type AlarmHistoryItem struct {
	AlarmName       string
	Timestamp       string
	HistoryItemType string
	Summary         string
	HistoryData     string
}

func (s *CWStore) AddAlarmHistory(accountID, alarmName, historyType, summary, data string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.store.DB().Exec(
		`INSERT INTO alarm_history (alarm_name, account_id, timestamp, history_item_type, summary, history_data)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		alarmName, accountID, now, historyType, summary, data,
	)
	return err
}

func (s *CWStore) DescribeAlarmHistory(accountID, alarmName, historyItemType, startDate, endDate string) ([]AlarmHistoryItem, error) {
	query := `SELECT alarm_name, timestamp, history_item_type, summary, history_data FROM alarm_history WHERE account_id = ?`
	args := []any{accountID}
	if alarmName != "" {
		query += " AND alarm_name = ?"
		args = append(args, alarmName)
	}
	if historyItemType != "" {
		query += " AND history_item_type = ?"
		args = append(args, historyItemType)
	}
	if startDate != "" {
		query += " AND timestamp >= ?"
		args = append(args, startDate)
	}
	if endDate != "" {
		query += " AND timestamp <= ?"
		args = append(args, endDate)
	}
	query += " ORDER BY timestamp DESC"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []AlarmHistoryItem
	for rows.Next() {
		var item AlarmHistoryItem
		if err := rows.Scan(&item.AlarmName, &item.Timestamp, &item.HistoryItemType, &item.Summary, &item.HistoryData); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *CWStore) DescribeAlarmsForMetric(accountID, metricName, namespace string) ([]Alarm, error) {
	rows, err := s.store.DB().Query(
		`SELECT alarm_name, account_id, namespace, metric_name, statistic, period, eval_periods, threshold, comparison, state, state_reason, actions_enabled FROM alarms WHERE account_id = ? AND metric_name = ? AND namespace = ? ORDER BY alarm_name`,
		accountID, metricName, namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAlarms(rows)
}

// --- Anomaly detector types and methods ---

type AnomalyDetector struct {
	Namespace     string
	MetricName    string
	Stat          string
	Dimensions    string
	Configuration string
	CreatedAt     string
}

func (s *CWStore) PutAnomalyDetector(accountID string, d AnomalyDetector) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.store.DB().Exec(
		`INSERT INTO anomaly_detectors (namespace, metric_name, stat, dimensions, account_id, configuration, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(namespace, metric_name, stat, dimensions, account_id) DO UPDATE SET
		 	configuration = excluded.configuration`,
		d.Namespace, d.MetricName, d.Stat, d.Dimensions, accountID, d.Configuration, now,
	)
	return err
}

func (s *CWStore) DeleteAnomalyDetector(accountID, namespace, metricName, stat, dimensions string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM anomaly_detectors WHERE account_id = ? AND namespace = ? AND metric_name = ? AND stat = ? AND dimensions = ?`,
		accountID, namespace, metricName, stat, dimensions,
	)
	return err
}

func (s *CWStore) DescribeAnomalyDetectors(accountID, namespace, metricName string) ([]AnomalyDetector, error) {
	query := `SELECT namespace, metric_name, stat, dimensions, configuration, created_at FROM anomaly_detectors WHERE account_id = ?`
	args := []any{accountID}
	if namespace != "" {
		query += " AND namespace = ?"
		args = append(args, namespace)
	}
	if metricName != "" {
		query += " AND metric_name = ?"
		args = append(args, metricName)
	}
	query += " ORDER BY namespace, metric_name, stat"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var detectors []AnomalyDetector
	for rows.Next() {
		var d AnomalyDetector
		if err := rows.Scan(&d.Namespace, &d.MetricName, &d.Stat, &d.Dimensions, &d.Configuration, &d.CreatedAt); err != nil {
			return nil, err
		}
		detectors = append(detectors, d)
	}
	return detectors, rows.Err()
}
