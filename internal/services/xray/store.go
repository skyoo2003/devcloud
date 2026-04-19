// SPDX-License-Identifier: Apache-2.0

// internal/services/xray/store.go
package xray

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errGroupNotFound = errors.New("group not found")
var errSamplingRuleNotFound = errors.New("sampling rule not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			filter_expr     TEXT NOT NULL DEFAULT '',
			insights_config TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS sampling_rules (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			priority        INTEGER NOT NULL DEFAULT 1000,
			fixed_rate      REAL NOT NULL DEFAULT 0.05,
			reservoir_size  INTEGER NOT NULL DEFAULT 1,
			service_name    TEXT NOT NULL DEFAULT '*',
			service_type    TEXT NOT NULL DEFAULT '*',
			host            TEXT NOT NULL DEFAULT '*',
			http_method     TEXT NOT NULL DEFAULT '*',
			url_path        TEXT NOT NULL DEFAULT '*',
			resource_arn    TEXT NOT NULL DEFAULT '*',
			version         INTEGER NOT NULL DEFAULT 1
		);
		CREATE TABLE IF NOT EXISTS trace_segments (
			trace_id        TEXT NOT NULL,
			segment_id      TEXT NOT NULL,
			document        TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			PRIMARY KEY (trace_id, segment_id)
		);
	`},
}

// Group holds a stored X-Ray group.
type Group struct {
	Name           string
	ARN            string
	FilterExpr     string
	InsightsConfig string // JSON
}

// SamplingRule holds a stored sampling rule.
type SamplingRule struct {
	Name          string
	ARN           string
	Priority      int64
	FixedRate     float64
	ReservoirSize int64
	ServiceName   string
	ServiceType   string
	Host          string
	HTTPMethod    string
	URLPath       string
	ResourceARN   string
	Version       int64
}

// TraceSegment holds a stored trace segment.
type TraceSegment struct {
	TraceID   string
	SegmentID string
	Document  string // JSON
	CreatedAt int64
}

// Store is the X-Ray SQLite store.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens (or creates) the X-Ray SQLite database.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "xray.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Group ---

func (s *Store) CreateGroup(g *Group) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO groups (name, arn, filter_expr, insights_config) VALUES (?, ?, ?, ?)`,
		g.Name, g.ARN, g.FilterExpr, g.InsightsConfig,
	)
	return err
}

func (s *Store) GetGroup(name string) (*Group, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, filter_expr, insights_config FROM groups WHERE name = ?`, name)
	return scanGroup(row)
}

func (s *Store) ListGroups() ([]Group, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, filter_expr, insights_config FROM groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var groups []Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *Store) UpdateGroup(name string, filterExpr, insightsConfig string) error {
	res, err := s.store.DB().Exec(
		`UPDATE groups SET filter_expr=?, insights_config=? WHERE name=?`,
		filterExpr, insightsConfig, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

func (s *Store) DeleteGroup(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM groups WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

// --- SamplingRule ---

func (s *Store) CreateSamplingRule(r *SamplingRule) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO sampling_rules
			(name, arn, priority, fixed_rate, reservoir_size, service_name, service_type,
			 host, http_method, url_path, resource_arn, version)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Name, r.ARN, r.Priority, r.FixedRate, r.ReservoirSize,
		r.ServiceName, r.ServiceType, r.Host, r.HTTPMethod, r.URLPath, r.ResourceARN, r.Version,
	)
	return err
}

func (s *Store) GetSamplingRule(name string) (*SamplingRule, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, priority, fixed_rate, reservoir_size, service_name, service_type,
		        host, http_method, url_path, resource_arn, version
		 FROM sampling_rules WHERE name = ?`, name)
	return scanSamplingRule(row)
}

func (s *Store) ListSamplingRules() ([]SamplingRule, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, priority, fixed_rate, reservoir_size, service_name, service_type,
		        host, http_method, url_path, resource_arn, version
		 FROM sampling_rules ORDER BY priority, name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var rules []SamplingRule
	for rows.Next() {
		r, err := scanSamplingRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, *r)
	}
	return rules, rows.Err()
}

func (s *Store) UpdateSamplingRule(name string, fields map[string]any) error {
	r, err := s.GetSamplingRule(name)
	if err != nil {
		return errSamplingRuleNotFound
	}
	if v, ok := toFloat(fields["FixedRate"]); ok {
		r.FixedRate = v
	}
	if v, ok := toInt64(fields["ReservoirSize"]); ok {
		r.ReservoirSize = v
	}
	if v, ok := toInt64(fields["Priority"]); ok {
		r.Priority = v
	}
	if v, ok := fields["ServiceName"].(string); ok {
		r.ServiceName = v
	}
	if v, ok := fields["ServiceType"].(string); ok {
		r.ServiceType = v
	}
	if v, ok := fields["Host"].(string); ok {
		r.Host = v
	}
	if v, ok := fields["HTTPMethod"].(string); ok {
		r.HTTPMethod = v
	}
	if v, ok := fields["URLPath"].(string); ok {
		r.URLPath = v
	}
	if v, ok := fields["ResourceARN"].(string); ok {
		r.ResourceARN = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE sampling_rules SET
			priority=?, fixed_rate=?, reservoir_size=?, service_name=?, service_type=?,
			host=?, http_method=?, url_path=?, resource_arn=?
		 WHERE name=?`,
		r.Priority, r.FixedRate, r.ReservoirSize, r.ServiceName, r.ServiceType,
		r.Host, r.HTTPMethod, r.URLPath, r.ResourceARN, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSamplingRuleNotFound
	}
	return nil
}

func (s *Store) DeleteSamplingRule(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM sampling_rules WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSamplingRuleNotFound
	}
	return nil
}

// --- TraceSegment ---

func (s *Store) PutSegment(seg *TraceSegment) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO trace_segments (trace_id, segment_id, document, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(trace_id, segment_id) DO UPDATE SET document=excluded.document`,
		seg.TraceID, seg.SegmentID, seg.Document, seg.CreatedAt,
	)
	return err
}

func (s *Store) GetSegmentsByTraceID(traceID string) ([]TraceSegment, error) {
	rows, err := s.store.DB().Query(
		`SELECT trace_id, segment_id, document, created_at FROM trace_segments WHERE trace_id = ?`, traceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var segs []TraceSegment
	for rows.Next() {
		var seg TraceSegment
		if err := rows.Scan(&seg.TraceID, &seg.SegmentID, &seg.Document, &seg.CreatedAt); err != nil {
			return nil, err
		}
		segs = append(segs, seg)
	}
	return segs, rows.Err()
}

func (s *Store) ListDistinctTraceIDs() ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT DISTINCT trace_id FROM trace_segments ORDER BY trace_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanGroup(sc scanner) (*Group, error) {
	var g Group
	err := sc.Scan(&g.Name, &g.ARN, &g.FilterExpr, &g.InsightsConfig)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupNotFound
		}
		return nil, err
	}
	return &g, nil
}

func scanSamplingRule(sc scanner) (*SamplingRule, error) {
	var r SamplingRule
	err := sc.Scan(
		&r.Name, &r.ARN, &r.Priority, &r.FixedRate, &r.ReservoirSize,
		&r.ServiceName, &r.ServiceType, &r.Host, &r.HTTPMethod, &r.URLPath, &r.ResourceARN, &r.Version,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSamplingRuleNotFound
		}
		return nil, err
	}
	return &r, nil
}

// --- Numeric helpers ---

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	return 0, false
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}
