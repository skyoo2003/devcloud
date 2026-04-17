// SPDX-License-Identifier: Apache-2.0

// internal/services/scheduler/store.go
package scheduler

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errScheduleNotFound      = errors.New("schedule not found")
	errScheduleGroupNotFound = errors.New("schedule group not found")
	errRateLimitNotFound     = errors.New("rate limit not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS scheduler_groups (
			name       TEXT NOT NULL,
			arn        TEXT NOT NULL,
			state      TEXT NOT NULL DEFAULT 'ACTIVE',
			account_id TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS scheduler_schedules (
			name               TEXT NOT NULL,
			group_name         TEXT NOT NULL DEFAULT 'default',
			arn                TEXT NOT NULL,
			schedule_expression TEXT NOT NULL,
			target             TEXT NOT NULL DEFAULT '{}',
			state              TEXT NOT NULL DEFAULT 'ENABLED',
			flexible_time_window TEXT NOT NULL DEFAULT '{}',
			description        TEXT NOT NULL DEFAULT '',
			account_id         TEXT NOT NULL,
			created_at         INTEGER NOT NULL,
			PRIMARY KEY (name, group_name, account_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS scheduler_rate_limits (
			resource_arn TEXT PRIMARY KEY,
			max_calls    INTEGER NOT NULL DEFAULT 100,
			time_window  TEXT NOT NULL DEFAULT '1h',
			account_id   TEXT NOT NULL,
			created_at   INTEGER NOT NULL
		);
	`},
}

type ScheduleGroup struct {
	Name      string
	ARN       string
	State     string
	AccountID string
	CreatedAt time.Time
}

type Schedule struct {
	Name               string
	GroupName          string
	ARN                string
	ScheduleExpression string
	Target             string
	State              string
	FlexibleTimeWindow string
	Description        string
	AccountID          string
	CreatedAt          time.Time
}

type RateLimit struct {
	ResourceARN string
	MaxCalls    int
	TimeWindow  string
	AccountID   string
	CreatedAt   time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append([]sqlite.Migration{}, migrations...)
	allMigrations = append(allMigrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "scheduler.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	st := &Store{store: s, tags: shared.NewTagStore(s)}
	// Ensure default group exists
	st.ensureDefaultGroup()
	return st, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) ensureDefaultGroup() {
	arn := "arn:aws:scheduler:us-east-1:" + shared.DefaultAccountID + ":schedule-group/default"
	now := time.Now().Unix()
	s.store.DB().Exec(
		`INSERT OR IGNORE INTO scheduler_groups (name, arn, state, account_id, created_at) VALUES (?, ?, 'ACTIVE', ?, ?)`,
		"default", arn, shared.DefaultAccountID, now,
	)
}

// Group operations

func (s *Store) CreateGroup(name string) (*ScheduleGroup, error) {
	arn := "arn:aws:scheduler:us-east-1:" + shared.DefaultAccountID + ":schedule-group/" + name
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO scheduler_groups (name, arn, state, account_id, created_at) VALUES (?, ?, 'ACTIVE', ?, ?)`,
		name, arn, shared.DefaultAccountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &ScheduleGroup{Name: name, ARN: arn, State: "ACTIVE", AccountID: shared.DefaultAccountID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetGroup(name string) (*ScheduleGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, state, account_id, created_at FROM scheduler_groups WHERE name = ?`, name)
	return scanGroup(row)
}

func (s *Store) UpdateGroup(name, state string) (*ScheduleGroup, error) {
	if state == "" {
		state = "ACTIVE"
	}
	res, err := s.store.DB().Exec(`UPDATE scheduler_groups SET state = ? WHERE name = ?`, state, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errScheduleGroupNotFound
	}
	return s.GetGroup(name)
}

func (s *Store) DeleteGroup(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM scheduler_groups WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errScheduleGroupNotFound
	}
	return nil
}

func (s *Store) ListGroups(prefix string) ([]ScheduleGroup, error) {
	query := `SELECT name, arn, state, account_id, created_at FROM scheduler_groups WHERE 1=1`
	var args []any
	if prefix != "" {
		query += ` AND name LIKE ?`
		args = append(args, prefix+"%")
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []ScheduleGroup
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

// Schedule operations

func (s *Store) CreateSchedule(name, groupName, expr, target, state, flexWindow, description string) (*Schedule, error) {
	if groupName == "" {
		groupName = "default"
	}
	arn := "arn:aws:scheduler:us-east-1:" + shared.DefaultAccountID + ":schedule/" + groupName + "/" + name
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO scheduler_schedules (name, group_name, arn, schedule_expression, target, state, flexible_time_window, description, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, groupName, arn, expr, target, state, flexWindow, description, shared.DefaultAccountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Schedule{
		Name: name, GroupName: groupName, ARN: arn,
		ScheduleExpression: expr, Target: target, State: state,
		FlexibleTimeWindow: flexWindow, Description: description,
		AccountID: shared.DefaultAccountID, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSchedule(name, groupName string) (*Schedule, error) {
	if groupName == "" {
		groupName = "default"
	}
	row := s.store.DB().QueryRow(
		`SELECT name, group_name, arn, schedule_expression, target, state, flexible_time_window, description, account_id, created_at
		 FROM scheduler_schedules WHERE name = ? AND group_name = ?`, name, groupName)
	return scanSchedule(row)
}

func (s *Store) UpdateSchedule(name, groupName, expr, target, state, flexWindow, description string) error {
	if groupName == "" {
		groupName = "default"
	}
	res, err := s.store.DB().Exec(
		`UPDATE scheduler_schedules SET schedule_expression=?, target=?, state=?, flexible_time_window=?, description=?
		 WHERE name = ? AND group_name = ?`,
		expr, target, state, flexWindow, description, name, groupName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errScheduleNotFound
	}
	return nil
}

func (s *Store) DeleteSchedule(name, groupName string) error {
	if groupName == "" {
		groupName = "default"
	}
	res, err := s.store.DB().Exec(`DELETE FROM scheduler_schedules WHERE name = ? AND group_name = ?`, name, groupName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errScheduleNotFound
	}
	return nil
}

func (s *Store) ListSchedules(groupName, prefix, state string) ([]Schedule, error) {
	query := `SELECT name, group_name, arn, schedule_expression, target, state, flexible_time_window, description, account_id, created_at
	          FROM scheduler_schedules WHERE 1=1`
	var args []any
	if groupName != "" {
		query += ` AND group_name = ?`
		args = append(args, groupName)
	}
	if prefix != "" {
		query += ` AND name LIKE ?`
		args = append(args, prefix+"%")
	}
	if state != "" {
		query += ` AND state = ?`
		args = append(args, state)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var schedules []Schedule
	for rows.Next() {
		sc, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, *sc)
	}
	return schedules, rows.Err()
}

// Rate limit operations

func (s *Store) PutRateLimit(rl *RateLimit) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO scheduler_rate_limits (resource_arn, max_calls, time_window, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET
		   max_calls = excluded.max_calls,
		   time_window = excluded.time_window`,
		rl.ResourceARN, rl.MaxCalls, rl.TimeWindow, shared.DefaultAccountID, now,
	)
	return err
}

func (s *Store) GetRateLimit(resourceARN string) (*RateLimit, error) {
	row := s.store.DB().QueryRow(
		`SELECT resource_arn, max_calls, time_window, account_id, created_at FROM scheduler_rate_limits WHERE resource_arn = ?`,
		resourceARN,
	)
	var rl RateLimit
	var createdAt int64
	if err := row.Scan(&rl.ResourceARN, &rl.MaxCalls, &rl.TimeWindow, &rl.AccountID, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRateLimitNotFound
		}
		return nil, err
	}
	rl.CreatedAt = time.Unix(createdAt, 0)
	return &rl, nil
}

func (s *Store) DeleteRateLimit(resourceARN string) error {
	res, err := s.store.DB().Exec(`DELETE FROM scheduler_rate_limits WHERE resource_arn = ?`, resourceARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRateLimitNotFound
	}
	return nil
}

func (s *Store) ListRateLimits() ([]RateLimit, error) {
	rows, err := s.store.DB().Query(
		`SELECT resource_arn, max_calls, time_window, account_id, created_at FROM scheduler_rate_limits ORDER BY resource_arn`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []RateLimit
	for rows.Next() {
		var rl RateLimit
		var createdAt int64
		if err := rows.Scan(&rl.ResourceARN, &rl.MaxCalls, &rl.TimeWindow, &rl.AccountID, &createdAt); err != nil {
			return nil, err
		}
		rl.CreatedAt = time.Unix(createdAt, 0)
		list = append(list, rl)
	}
	return list, rows.Err()
}

type scanner interface{ Scan(dest ...any) error }

func scanGroup(s scanner) (*ScheduleGroup, error) {
	var g ScheduleGroup
	var createdAt int64
	err := s.Scan(&g.Name, &g.ARN, &g.State, &g.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errScheduleGroupNotFound
		}
		return nil, err
	}
	g.CreatedAt = time.Unix(createdAt, 0)
	return &g, nil
}

func scanSchedule(s scanner) (*Schedule, error) {
	var sc Schedule
	var createdAt int64
	err := s.Scan(&sc.Name, &sc.GroupName, &sc.ARN, &sc.ScheduleExpression, &sc.Target, &sc.State, &sc.FlexibleTimeWindow, &sc.Description, &sc.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errScheduleNotFound
		}
		return nil, err
	}
	sc.CreatedAt = time.Unix(createdAt, 0)
	return &sc, nil
}
