// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudtrail/store.go
package cloudtrail

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
	errTrailNotFound          = errors.New("trail not found")
	errEventDataStoreNotFound = errors.New("event data store not found")
	errChannelNotFound        = errors.New("channel not found")
	errDashboardNotFound      = errors.New("dashboard not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS trails (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			s3_bucket       TEXT NOT NULL DEFAULT '',
			s3_prefix       TEXT NOT NULL DEFAULT '',
			is_multi_region INTEGER NOT NULL DEFAULT 0,
			is_org_trail    INTEGER NOT NULL DEFAULT 0,
			is_logging      INTEGER NOT NULL DEFAULT 1,
			log_group       TEXT NOT NULL DEFAULT '',
			sns_topic       TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS event_data_stores (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			status     TEXT NOT NULL DEFAULT 'ENABLED',
			retention  INTEGER NOT NULL DEFAULT 2555,
			multi_region INTEGER NOT NULL DEFAULT 1,
			org_enabled  INTEGER NOT NULL DEFAULT 0,
			created_at   INTEGER NOT NULL,
			updated_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS channels (
			arn          TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			source       TEXT NOT NULL DEFAULT '',
			destinations TEXT NOT NULL DEFAULT '[]',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS dashboards (
			arn        TEXT PRIMARY KEY,
			name       TEXT NOT NULL UNIQUE,
			type       TEXT NOT NULL DEFAULT 'MANAGED',
			status     TEXT NOT NULL DEFAULT 'ACTIVE',
			widgets    TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS queries (
			query_id   TEXT PRIMARY KEY,
			query_sql  TEXT NOT NULL DEFAULT '',
			status     TEXT NOT NULL DEFAULT 'QUEUED',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS resource_policies (
			resource_arn TEXT PRIMARY KEY,
			policy       TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS event_selectors (
			trail_arn TEXT PRIMARY KEY,
			selectors TEXT NOT NULL DEFAULT '[]',
			advanced  TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS insight_selectors (
			trail_arn TEXT PRIMARY KEY,
			selectors TEXT NOT NULL DEFAULT '[]'
		);
	`},
}

// Trail represents a CloudTrail trail.
type Trail struct {
	Name          string
	ARN           string
	S3Bucket      string
	S3Prefix      string
	IsMultiRegion bool
	IsOrgTrail    bool
	IsLogging     bool
	LogGroup      string
	SNSTopic      string
	CreatedAt     time.Time
}

// EventDataStore represents a CloudTrail event data store.
type EventDataStore struct {
	ARN         string
	Name        string
	Status      string
	Retention   int
	MultiRegion bool
	OrgEnabled  bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Channel represents a CloudTrail channel.
type Channel struct {
	ARN          string
	Name         string
	Source       string
	Destinations string
	CreatedAt    time.Time
}

// Dashboard represents a CloudTrail dashboard.
type Dashboard struct {
	ARN       string
	Name      string
	Type      string
	Status    string
	Widgets   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Query represents a CloudTrail query.
type Query struct {
	QueryID   string
	QuerySQL  string
	Status    string
	CreatedAt time.Time
}

// Store is the CloudTrail data store.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore creates a new Store backed by SQLite.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "cloudtrail.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.store.Close() }

// ---- Trail ----

func (s *Store) CreateTrail(name, arn, s3Bucket, s3Prefix, logGroup, snsTopic string, multiRegion, orgTrail bool) (*Trail, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO trails (name, arn, s3_bucket, s3_prefix, is_multi_region, is_org_trail, is_logging, log_group, sns_topic, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)`,
		name, arn, s3Bucket, s3Prefix, boolToInt(multiRegion), boolToInt(orgTrail), logGroup, snsTopic, now,
	)
	if err != nil {
		return nil, err
	}
	return &Trail{
		Name: name, ARN: arn, S3Bucket: s3Bucket, S3Prefix: s3Prefix,
		IsMultiRegion: multiRegion, IsOrgTrail: orgTrail, IsLogging: true,
		LogGroup: logGroup, SNSTopic: snsTopic, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetTrail(name string) (*Trail, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, s3_bucket, s3_prefix, is_multi_region, is_org_trail, is_logging, log_group, sns_topic, created_at
		 FROM trails WHERE name = ? OR arn = ?`, name, name)
	return scanTrail(row)
}

func (s *Store) ListTrails() ([]Trail, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, s3_bucket, s3_prefix, is_multi_region, is_org_trail, is_logging, log_group, sns_topic, created_at
		 FROM trails ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var trails []Trail
	for rows.Next() {
		t, err := scanTrail(rows)
		if err != nil {
			return nil, err
		}
		trails = append(trails, *t)
	}
	return trails, rows.Err()
}

func (s *Store) UpdateTrail(name, s3Bucket, s3Prefix, logGroup, snsTopic string, multiRegion, orgTrail bool) (*Trail, error) {
	res, err := s.store.DB().Exec(
		`UPDATE trails SET s3_bucket=?, s3_prefix=?, is_multi_region=?, is_org_trail=?, log_group=?, sns_topic=?
		 WHERE name = ? OR arn = ?`,
		s3Bucket, s3Prefix, boolToInt(multiRegion), boolToInt(orgTrail), logGroup, snsTopic, name, name,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errTrailNotFound
	}
	return s.GetTrail(name)
}

func (s *Store) DeleteTrail(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM trails WHERE name = ? OR arn = ?`, name, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrailNotFound
	}
	return nil
}

func (s *Store) SetLogging(name string, logging bool) error {
	res, err := s.store.DB().Exec(
		`UPDATE trails SET is_logging=? WHERE name = ? OR arn = ?`,
		boolToInt(logging), name, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTrailNotFound
	}
	return nil
}

func (s *Store) GetEventSelectors(trailARN string) (string, string, error) {
	row := s.store.DB().QueryRow(`SELECT selectors, advanced FROM event_selectors WHERE trail_arn = ?`, trailARN)
	var selectors, advanced string
	err := row.Scan(&selectors, &advanced)
	if errors.Is(err, sql.ErrNoRows) {
		return "[]", "[]", nil
	}
	return selectors, advanced, err
}

func (s *Store) PutEventSelectors(trailARN, selectors, advanced string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO event_selectors (trail_arn, selectors, advanced) VALUES (?, ?, ?)
		 ON CONFLICT(trail_arn) DO UPDATE SET selectors=excluded.selectors, advanced=excluded.advanced`,
		trailARN, selectors, advanced,
	)
	return err
}

func (s *Store) GetInsightSelectors(trailARN string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT selectors FROM insight_selectors WHERE trail_arn = ?`, trailARN)
	var selectors string
	err := row.Scan(&selectors)
	if errors.Is(err, sql.ErrNoRows) {
		return "[]", nil
	}
	return selectors, err
}

func (s *Store) PutInsightSelectors(trailARN, selectors string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO insight_selectors (trail_arn, selectors) VALUES (?, ?)
		 ON CONFLICT(trail_arn) DO UPDATE SET selectors=excluded.selectors`,
		trailARN, selectors,
	)
	return err
}

// ---- EventDataStore ----

func (s *Store) CreateEventDataStore(arn, name string, retention int, multiRegion, orgEnabled bool) (*EventDataStore, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO event_data_stores (arn, name, status, retention, multi_region, org_enabled, created_at, updated_at)
		 VALUES (?, ?, 'ENABLED', ?, ?, ?, ?, ?)`,
		arn, name, retention, boolToInt(multiRegion), boolToInt(orgEnabled), now, now,
	)
	if err != nil {
		return nil, err
	}
	return &EventDataStore{
		ARN: arn, Name: name, Status: "ENABLED", Retention: retention,
		MultiRegion: multiRegion, OrgEnabled: orgEnabled,
		CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetEventDataStore(arn string) (*EventDataStore, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, status, retention, multi_region, org_enabled, created_at, updated_at
		 FROM event_data_stores WHERE arn = ?`, arn)
	return scanEDS(row)
}

func (s *Store) ListEventDataStores() ([]EventDataStore, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, status, retention, multi_region, org_enabled, created_at, updated_at
		 FROM event_data_stores ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventDataStore
	for rows.Next() {
		e, err := scanEDS(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func (s *Store) UpdateEventDataStore(arn string, retention int, multiRegion, orgEnabled bool) (*EventDataStore, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE event_data_stores SET retention=?, multi_region=?, org_enabled=?, updated_at=? WHERE arn = ?`,
		retention, boolToInt(multiRegion), boolToInt(orgEnabled), now, arn,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errEventDataStoreNotFound
	}
	return s.GetEventDataStore(arn)
}

func (s *Store) SetEventDataStoreStatus(arn, status string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE event_data_stores SET status=?, updated_at=? WHERE arn = ?`, status, now, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEventDataStoreNotFound
	}
	return nil
}

func (s *Store) DeleteEventDataStore(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM event_data_stores WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEventDataStoreNotFound
	}
	return nil
}

// ---- Channel ----

func (s *Store) CreateChannel(arn, name, source, destinations string) (*Channel, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO channels (arn, name, source, destinations, created_at) VALUES (?, ?, ?, ?, ?)`,
		arn, name, source, destinations, now,
	)
	if err != nil {
		return nil, err
	}
	return &Channel{ARN: arn, Name: name, Source: source, Destinations: destinations, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetChannel(arn string) (*Channel, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, source, destinations, created_at FROM channels WHERE arn = ?`, arn)
	return scanChannel(row)
}

func (s *Store) ListChannels() ([]Channel, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, source, destinations, created_at FROM channels ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateChannel(arn, name, destinations string) (*Channel, error) {
	res, err := s.store.DB().Exec(
		`UPDATE channels SET name=?, destinations=? WHERE arn = ?`, name, destinations, arn)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errChannelNotFound
	}
	return s.GetChannel(arn)
}

func (s *Store) DeleteChannel(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM channels WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errChannelNotFound
	}
	return nil
}

// ---- Dashboard ----

func (s *Store) CreateDashboard(arn, name, dashType, widgets string) (*Dashboard, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO dashboards (arn, name, type, status, widgets, created_at, updated_at) VALUES (?, ?, ?, 'ACTIVE', ?, ?, ?)`,
		arn, name, dashType, widgets, now, now,
	)
	if err != nil {
		return nil, err
	}
	return &Dashboard{ARN: arn, Name: name, Type: dashType, Status: "ACTIVE", Widgets: widgets, CreatedAt: time.Unix(now, 0), UpdatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetDashboard(arn string) (*Dashboard, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, type, status, widgets, created_at, updated_at FROM dashboards WHERE arn = ?`, arn)
	return scanDashboard(row)
}

func (s *Store) ListDashboards() ([]Dashboard, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, type, status, widgets, created_at, updated_at FROM dashboards ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Dashboard
	for rows.Next() {
		d, err := scanDashboard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

func (s *Store) UpdateDashboard(arn, widgets string) (*Dashboard, error) {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE dashboards SET widgets=?, updated_at=? WHERE arn = ?`, widgets, now, arn)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errDashboardNotFound
	}
	return s.GetDashboard(arn)
}

func (s *Store) DeleteDashboard(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM dashboards WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errDashboardNotFound
	}
	return nil
}

// ---- Query ----

func (s *Store) CreateQuery(queryID, querySQL string) (*Query, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO queries (query_id, query_sql, status, created_at) VALUES (?, ?, 'QUEUED', ?)`,
		queryID, querySQL, now,
	)
	if err != nil {
		return nil, err
	}
	return &Query{QueryID: queryID, QuerySQL: querySQL, Status: "QUEUED", CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetQuery(queryID string) (*Query, error) {
	row := s.store.DB().QueryRow(
		`SELECT query_id, query_sql, status, created_at FROM queries WHERE query_id = ?`, queryID)
	var q Query
	var createdAt int64
	err := row.Scan(&q.QueryID, &q.QuerySQL, &q.Status, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("query not found")
	}
	if err != nil {
		return nil, err
	}
	q.CreatedAt = time.Unix(createdAt, 0)
	return &q, nil
}

func (s *Store) ListQueries() ([]Query, error) {
	rows, err := s.store.DB().Query(
		`SELECT query_id, query_sql, status, created_at FROM queries ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Query
	for rows.Next() {
		var q Query
		var createdAt int64
		if err := rows.Scan(&q.QueryID, &q.QuerySQL, &q.Status, &createdAt); err != nil {
			return nil, err
		}
		q.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Store) CancelQuery(queryID string) error {
	res, err := s.store.DB().Exec(`UPDATE queries SET status='CANCELLED' WHERE query_id = ?`, queryID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("query not found")
	}
	return nil
}

// ---- ResourcePolicy ----

func (s *Store) GetResourcePolicy(arn string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT policy FROM resource_policies WHERE resource_arn = ?`, arn)
	var policy string
	err := row.Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return policy, err
}

func (s *Store) PutResourcePolicy(arn, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO resource_policies (resource_arn, policy) VALUES (?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET policy=excluded.policy`,
		arn, policy,
	)
	return err
}

func (s *Store) DeleteResourcePolicy(arn string) error {
	_, err := s.store.DB().Exec(`DELETE FROM resource_policies WHERE resource_arn = ?`, arn)
	return err
}

// ---- Scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanTrail(s scanner) (*Trail, error) {
	var t Trail
	var multiRegion, orgTrail, isLogging int
	var createdAt int64
	err := s.Scan(&t.Name, &t.ARN, &t.S3Bucket, &t.S3Prefix, &multiRegion, &orgTrail, &isLogging, &t.LogGroup, &t.SNSTopic, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errTrailNotFound
	}
	if err != nil {
		return nil, err
	}
	t.IsMultiRegion = multiRegion == 1
	t.IsOrgTrail = orgTrail == 1
	t.IsLogging = isLogging == 1
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

func scanEDS(s scanner) (*EventDataStore, error) {
	var e EventDataStore
	var multiRegion, orgEnabled int
	var createdAt, updatedAt int64
	err := s.Scan(&e.ARN, &e.Name, &e.Status, &e.Retention, &multiRegion, &orgEnabled, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errEventDataStoreNotFound
	}
	if err != nil {
		return nil, err
	}
	e.MultiRegion = multiRegion == 1
	e.OrgEnabled = orgEnabled == 1
	e.CreatedAt = time.Unix(createdAt, 0)
	e.UpdatedAt = time.Unix(updatedAt, 0)
	return &e, nil
}

func scanChannel(s scanner) (*Channel, error) {
	var c Channel
	var createdAt int64
	err := s.Scan(&c.ARN, &c.Name, &c.Source, &c.Destinations, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errChannelNotFound
	}
	if err != nil {
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanDashboard(s scanner) (*Dashboard, error) {
	var d Dashboard
	var createdAt, updatedAt int64
	err := s.Scan(&d.ARN, &d.Name, &d.Type, &d.Status, &d.Widgets, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errDashboardNotFound
	}
	if err != nil {
		return nil, err
	}
	d.CreatedAt = time.Unix(createdAt, 0)
	d.UpdatedAt = time.Unix(updatedAt, 0)
	return &d, nil
}

// ---- Helpers ----

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func sqlite_isUnique(err error) bool {
	return err != nil && containsStr(err.Error(), "UNIQUE constraint failed")
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}

func marshalJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
