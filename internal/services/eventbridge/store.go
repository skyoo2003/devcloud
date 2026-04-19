// SPDX-License-Identifier: Apache-2.0

// internal/services/eventbridge/store.go
package eventbridge

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrBusNotFound     = errors.New("event bus not found")
	ErrRuleNotFound    = errors.New("rule not found")
	ErrArchiveNotFound = errors.New("archive not found")
	ErrReplayNotFound  = errors.New("replay not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS event_buses (
			name       TEXT NOT NULL,
			arn        TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS rules (
			name                TEXT NOT NULL,
			bus_name            TEXT NOT NULL,
			account_id          TEXT NOT NULL,
			event_pattern       TEXT NOT NULL DEFAULT '',
			state               TEXT NOT NULL DEFAULT 'ENABLED',
			schedule_expression TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (name, bus_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS targets (
			rule_name  TEXT NOT NULL,
			bus_name   TEXT NOT NULL,
			target_id  TEXT NOT NULL,
			arn        TEXT NOT NULL,
			account_id TEXT NOT NULL,
			input_path TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (rule_name, bus_name, target_id, account_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS archives (
			archive_name     TEXT NOT NULL,
			event_source_arn TEXT NOT NULL,
			description      TEXT NOT NULL DEFAULT '',
			event_pattern    TEXT NOT NULL DEFAULT '',
			retention_days   INTEGER NOT NULL DEFAULT 0,
			state            TEXT NOT NULL DEFAULT 'ENABLED',
			account_id       TEXT NOT NULL,
			created_at       DATETIME NOT NULL,
			PRIMARY KEY (archive_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS replays (
			replay_name      TEXT NOT NULL,
			archive_name     TEXT NOT NULL,
			event_source_arn TEXT NOT NULL,
			destination      TEXT NOT NULL,
			event_start_time DATETIME,
			event_end_time   DATETIME,
			state            TEXT NOT NULL DEFAULT 'COMPLETED',
			account_id       TEXT NOT NULL,
			created_at       DATETIME NOT NULL,
			PRIMARY KEY (replay_name, account_id)
		);
	`},
}

type EventBus struct {
	Name      string
	ARN       string
	AccountID string
}

type Rule struct {
	Name               string
	BusName            string
	AccountID          string
	EventPattern       map[string]any
	State              string
	ScheduleExpression string
}

type Target struct {
	RuleName  string
	BusName   string
	TargetID  string
	ARN       string
	AccountID string
	InputPath string
}

type EBStore struct {
	store *sqlite.Store
}

func NewEBStore(dataDir string) (*EBStore, error) {
	dbPath := filepath.Join(dataDir, "eventbridge.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	eb := &EBStore{store: s}
	// Seed the default event bus.
	_ = eb.CreateEventBus("default", "000000000000")
	return eb, nil
}

func (s *EBStore) Close() error { return s.store.Close() }

func (s *EBStore) CreateEventBus(name, accountID string) error {
	arn := "arn:aws:events:us-east-1:" + accountID + ":event-bus/" + name
	_, err := s.store.DB().Exec(
		`INSERT INTO event_buses (name, arn, account_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
		name, arn, accountID,
	)
	return err
}

func (s *EBStore) DeleteEventBus(name, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM event_buses WHERE name = ? AND account_id = ?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrBusNotFound
	}
	return nil
}

func (s *EBStore) GetEventBus(name, accountID string) (*EventBus, error) {
	var b EventBus
	err := s.store.DB().QueryRow(
		`SELECT name, arn, account_id FROM event_buses WHERE name = ? AND account_id = ?`,
		name, accountID,
	).Scan(&b.Name, &b.ARN, &b.AccountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrBusNotFound
		}
		return nil, err
	}
	return &b, nil
}

func (s *EBStore) GetRule(name, busName, accountID string) (*Rule, error) {
	var r Rule
	var patternJSON string
	err := s.store.DB().QueryRow(
		`SELECT name, bus_name, account_id, event_pattern, state, schedule_expression FROM rules WHERE name = ? AND bus_name = ? AND account_id = ?`,
		name, busName, accountID,
	).Scan(&r.Name, &r.BusName, &r.AccountID, &patternJSON, &r.State, &r.ScheduleExpression)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrRuleNotFound
		}
		return nil, err
	}
	if patternJSON != "" {
		_ = json.Unmarshal([]byte(patternJSON), &r.EventPattern)
	}
	return &r, nil
}

func (s *EBStore) ListEventBuses(accountID string) ([]EventBus, error) {
	rows, err := s.store.DB().Query(`SELECT name, arn, account_id FROM event_buses WHERE account_id = ? ORDER BY name`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var buses []EventBus
	for rows.Next() {
		var b EventBus
		if err := rows.Scan(&b.Name, &b.ARN, &b.AccountID); err != nil {
			return nil, err
		}
		buses = append(buses, b)
	}
	return buses, rows.Err()
}

func (s *EBStore) PutRule(name, busName, accountID string, eventPattern map[string]any, state, scheduleExpression string) error {
	patternJSON, _ := json.Marshal(eventPattern)
	_, err := s.store.DB().Exec(
		`INSERT INTO rules (name, bus_name, account_id, event_pattern, state, schedule_expression)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name, bus_name, account_id) DO UPDATE SET
			event_pattern = excluded.event_pattern,
			state = excluded.state,
			schedule_expression = excluded.schedule_expression`,
		name, busName, accountID, string(patternJSON), state, scheduleExpression,
	)
	return err
}

func (s *EBStore) DeleteRule(name, busName, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM rules WHERE name = ? AND bus_name = ? AND account_id = ?`,
		name, busName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrRuleNotFound
	}
	return nil
}

func (s *EBStore) ListRules(busName, accountID string) ([]Rule, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, bus_name, account_id, event_pattern, state, schedule_expression FROM rules WHERE bus_name = ? AND account_id = ?`,
		busName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanRules(rows)
}

func (s *EBStore) EnableRule(name, busName, accountID string) error {
	_, err := s.store.DB().Exec(
		`UPDATE rules SET state = 'ENABLED' WHERE name = ? AND bus_name = ? AND account_id = ?`,
		name, busName, accountID,
	)
	return err
}

func (s *EBStore) DisableRule(name, busName, accountID string) error {
	_, err := s.store.DB().Exec(
		`UPDATE rules SET state = 'DISABLED' WHERE name = ? AND bus_name = ? AND account_id = ?`,
		name, busName, accountID,
	)
	return err
}

func scanRules(rows *sql.Rows) ([]Rule, error) {
	var rules []Rule
	for rows.Next() {
		var r Rule
		var patternJSON string
		if err := rows.Scan(&r.Name, &r.BusName, &r.AccountID, &patternJSON, &r.State, &r.ScheduleExpression); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(patternJSON), &r.EventPattern)
		rules = append(rules, r)
	}
	return rules, rows.Err()
}

func (s *EBStore) PutTargets(ruleName, busName, accountID string, targets []Target) error {
	for _, t := range targets {
		_, err := s.store.DB().Exec(
			`INSERT INTO targets (rule_name, bus_name, target_id, arn, account_id, input_path) VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT(rule_name, bus_name, target_id, account_id) DO UPDATE SET arn = excluded.arn, input_path = excluded.input_path`,
			ruleName, busName, t.TargetID, t.ARN, accountID, t.InputPath,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *EBStore) RemoveTargets(ruleName, busName, accountID string, targetIDs []string) error {
	for _, id := range targetIDs {
		_, _ = s.store.DB().Exec(
			`DELETE FROM targets WHERE rule_name = ? AND bus_name = ? AND target_id = ? AND account_id = ?`,
			ruleName, busName, id, accountID,
		)
	}
	return nil
}

func (s *EBStore) ListTargetsByRule(ruleName, busName, accountID string) ([]Target, error) {
	rows, err := s.store.DB().Query(
		`SELECT rule_name, bus_name, target_id, arn, account_id, input_path FROM targets WHERE rule_name = ? AND bus_name = ? AND account_id = ?`,
		ruleName, busName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []Target
	for rows.Next() {
		var t Target
		if err := rows.Scan(&t.RuleName, &t.BusName, &t.TargetID, &t.ARN, &t.AccountID, &t.InputPath); err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	return targets, rows.Err()
}

// MatchingRules returns ENABLED rules whose event_pattern matches the given event.
// Pattern matching is source-based: if pattern has "source" array, event source must be in it.
// An empty pattern matches all events.
func (s *EBStore) MatchingRules(busName, accountID string, event map[string]any) ([]Rule, error) {
	rules, err := s.ListRules(busName, accountID)
	if err != nil {
		return nil, err
	}
	eventSource, _ := event["source"].(string)
	if eventSource == "" {
		eventSource, _ = event["Source"].(string)
	}
	var matched []Rule
	for _, r := range rules {
		if r.State != "ENABLED" {
			continue
		}
		if len(r.EventPattern) == 0 {
			matched = append(matched, r)
			continue
		}
		if sources, ok := r.EventPattern["source"].([]any); ok {
			for _, s := range sources {
				if src, ok := s.(string); ok && src == eventSource {
					matched = append(matched, r)
					break
				}
			}
		}
	}
	return matched, nil
}

// ──────────────────────────────────────────────
// Archive
// ──────────────────────────────────────────────

type Archive struct {
	ArchiveName    string
	EventSourceARN string
	Description    string
	EventPattern   string
	RetentionDays  int
	State          string
	AccountID      string
	CreatedAt      time.Time
}

func (s *EBStore) CreateArchive(a Archive) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO archives (archive_name, event_source_arn, description, event_pattern, retention_days, state, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		a.ArchiveName, a.EventSourceARN, a.Description, a.EventPattern, a.RetentionDays, a.State, a.AccountID, a.CreatedAt,
	)
	return err
}

func (s *EBStore) GetArchive(archiveName, accountID string) (*Archive, error) {
	var a Archive
	err := s.store.DB().QueryRow(
		`SELECT archive_name, event_source_arn, description, event_pattern, retention_days, state, account_id, created_at
		 FROM archives WHERE archive_name = ? AND account_id = ?`,
		archiveName, accountID,
	).Scan(&a.ArchiveName, &a.EventSourceARN, &a.Description, &a.EventPattern, &a.RetentionDays, &a.State, &a.AccountID, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrArchiveNotFound
		}
		return nil, err
	}
	return &a, nil
}

func (s *EBStore) ListArchives(namePrefix, eventSourceARN, accountID string) ([]Archive, error) {
	q := `SELECT archive_name, event_source_arn, description, event_pattern, retention_days, state, account_id, created_at
	      FROM archives WHERE account_id = ?`
	args := []any{accountID}
	if namePrefix != "" {
		q += ` AND archive_name LIKE ?`
		args = append(args, namePrefix+"%")
	}
	if eventSourceARN != "" {
		q += ` AND event_source_arn = ?`
		args = append(args, eventSourceARN)
	}
	rows, err := s.store.DB().Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var archives []Archive
	for rows.Next() {
		var a Archive
		if err := rows.Scan(&a.ArchiveName, &a.EventSourceARN, &a.Description, &a.EventPattern, &a.RetentionDays, &a.State, &a.AccountID, &a.CreatedAt); err != nil {
			return nil, err
		}
		archives = append(archives, a)
	}
	return archives, rows.Err()
}

func (s *EBStore) UpdateArchive(archiveName, accountID, description, eventPattern string, retentionDays int) error {
	res, err := s.store.DB().Exec(
		`UPDATE archives SET description = ?, event_pattern = ?, retention_days = ? WHERE archive_name = ? AND account_id = ?`,
		description, eventPattern, retentionDays, archiveName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrArchiveNotFound
	}
	return nil
}

func (s *EBStore) DeleteArchive(archiveName, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM archives WHERE archive_name = ? AND account_id = ?`, archiveName, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrArchiveNotFound
	}
	return nil
}

// ──────────────────────────────────────────────
// Replay
// ──────────────────────────────────────────────

type Replay struct {
	ReplayName     string
	ArchiveName    string
	EventSourceARN string
	Destination    string
	EventStartTime *time.Time
	EventEndTime   *time.Time
	State          string
	AccountID      string
	CreatedAt      time.Time
}

func (s *EBStore) CreateReplay(r Replay) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO replays (replay_name, archive_name, event_source_arn, destination, event_start_time, event_end_time, state, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ReplayName, r.ArchiveName, r.EventSourceARN, r.Destination, r.EventStartTime, r.EventEndTime, r.State, r.AccountID, r.CreatedAt,
	)
	return err
}

func (s *EBStore) GetReplay(replayName, accountID string) (*Replay, error) {
	var r Replay
	err := s.store.DB().QueryRow(
		`SELECT replay_name, archive_name, event_source_arn, destination, event_start_time, event_end_time, state, account_id, created_at
		 FROM replays WHERE replay_name = ? AND account_id = ?`,
		replayName, accountID,
	).Scan(&r.ReplayName, &r.ArchiveName, &r.EventSourceARN, &r.Destination, &r.EventStartTime, &r.EventEndTime, &r.State, &r.AccountID, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrReplayNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *EBStore) ListReplays(namePrefix, eventSourceARN, accountID string) ([]Replay, error) {
	q := `SELECT replay_name, archive_name, event_source_arn, destination, event_start_time, event_end_time, state, account_id, created_at
	      FROM replays WHERE account_id = ?`
	args := []any{accountID}
	if namePrefix != "" {
		q += ` AND replay_name LIKE ?`
		args = append(args, namePrefix+"%")
	}
	if eventSourceARN != "" {
		q += ` AND event_source_arn = ?`
		args = append(args, eventSourceARN)
	}
	rows, err := s.store.DB().Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var replays []Replay
	for rows.Next() {
		var r Replay
		if err := rows.Scan(&r.ReplayName, &r.ArchiveName, &r.EventSourceARN, &r.Destination, &r.EventStartTime, &r.EventEndTime, &r.State, &r.AccountID, &r.CreatedAt); err != nil {
			return nil, err
		}
		replays = append(replays, r)
	}
	return replays, rows.Err()
}

func (s *EBStore) CancelReplay(replayName, accountID string) error {
	res, err := s.store.DB().Exec(
		`UPDATE replays SET state = 'CANCELLED' WHERE replay_name = ? AND account_id = ?`,
		replayName, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrReplayNotFound
	}
	return nil
}

func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func nowUnix() int64 { return time.Now().Unix() }
