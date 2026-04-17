// SPDX-License-Identifier: Apache-2.0

// internal/services/shield/store.go
package shield

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errProtectionNotFound      = errors.New("protection not found")
	errProtectionGroupNotFound = errors.New("protection group not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS protections (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			name            TEXT NOT NULL DEFAULT '',
			resource_arn    TEXT NOT NULL UNIQUE,
			health_check_ids TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS protection_groups (
			id           TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			aggregation  TEXT NOT NULL DEFAULT 'SUM',
			pattern      TEXT NOT NULL DEFAULT 'ALL',
			resource_type TEXT NOT NULL DEFAULT '',
			members      TEXT NOT NULL DEFAULT '[]',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS subscription (
			id                  TEXT PRIMARY KEY DEFAULT 'default',
			state               TEXT NOT NULL DEFAULT 'ACTIVE',
			start_time          INTEGER NOT NULL DEFAULT 0,
			end_time            INTEGER NOT NULL DEFAULT 0,
			auto_renew          TEXT NOT NULL DEFAULT 'ENABLED',
			proactive_engagement TEXT NOT NULL DEFAULT 'DISABLED'
		);
	`},
}

type Protection struct {
	ID             string
	ARN            string
	Name           string
	ResourceARN    string
	HealthCheckIDs string
	CreatedAt      time.Time
}

type ProtectionGroup struct {
	ID           string
	ARN          string
	Aggregation  string
	Pattern      string
	ResourceType string
	Members      string
	CreatedAt    time.Time
}

type Subscription struct {
	ID                  string
	State               string
	StartTime           int64
	EndTime             int64
	AutoRenew           string
	ProactiveEngagement string
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "shield.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	st := &Store{store: s, tags: shared.NewTagStore(s)}
	// Ensure default subscription row exists
	st.store.DB().Exec(
		`INSERT OR IGNORE INTO subscription (id, state, start_time, end_time, auto_renew, proactive_engagement)
		 VALUES ('default', 'ACTIVE', 0, 0, 'ENABLED', 'DISABLED')`)
	return st, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Protection ---

func (s *Store) CreateProtection(p *Protection) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO protections (id, arn, name, resource_arn, health_check_ids, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.ARN, p.Name, p.ResourceARN, p.HealthCheckIDs, now,
	)
	return err
}

func (s *Store) GetProtection(id string) (*Protection, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, resource_arn, health_check_ids, created_at
		 FROM protections WHERE id = ?`, id)
	return scanProtection(row)
}

func (s *Store) GetProtectionByARN(arn string) (*Protection, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, resource_arn, health_check_ids, created_at
		 FROM protections WHERE arn = ?`, arn)
	return scanProtection(row)
}

func (s *Store) GetProtectionByResourceARN(resourceARN string) (*Protection, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, resource_arn, health_check_ids, created_at
		 FROM protections WHERE resource_arn = ?`, resourceARN)
	return scanProtection(row)
}

func (s *Store) ListProtections() ([]Protection, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, resource_arn, health_check_ids, created_at
		 FROM protections ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var protections []Protection
	for rows.Next() {
		p, err := scanProtection(rows)
		if err != nil {
			return nil, err
		}
		protections = append(protections, *p)
	}
	return protections, rows.Err()
}

func (s *Store) DeleteProtection(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM protections WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProtectionNotFound
	}
	return nil
}

func (s *Store) UpdateProtectionHealthChecks(id, healthCheckIDs string) error {
	res, err := s.store.DB().Exec(
		`UPDATE protections SET health_check_ids = ? WHERE id = ?`, healthCheckIDs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProtectionNotFound
	}
	return nil
}

// --- ProtectionGroup ---

func (s *Store) CreateProtectionGroup(g *ProtectionGroup) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO protection_groups (id, arn, aggregation, pattern, resource_type, members, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		g.ID, g.ARN, g.Aggregation, g.Pattern, g.ResourceType, g.Members, now,
	)
	return err
}

func (s *Store) GetProtectionGroup(id string) (*ProtectionGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, aggregation, pattern, resource_type, members, created_at
		 FROM protection_groups WHERE id = ?`, id)
	return scanProtectionGroup(row)
}

func (s *Store) ListProtectionGroups() ([]ProtectionGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, aggregation, pattern, resource_type, members, created_at
		 FROM protection_groups ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []ProtectionGroup
	for rows.Next() {
		g, err := scanProtectionGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *Store) UpdateProtectionGroup(id string, fields map[string]any) error {
	g, err := s.GetProtectionGroup(id)
	if err != nil {
		return errProtectionGroupNotFound
	}
	if v, ok := fields["Aggregation"].(string); ok && v != "" {
		g.Aggregation = v
	}
	if v, ok := fields["Pattern"].(string); ok && v != "" {
		g.Pattern = v
	}
	if v, ok := fields["ResourceType"].(string); ok {
		g.ResourceType = v
	}
	if v, ok := fields["Members"].(string); ok {
		g.Members = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE protection_groups SET aggregation=?, pattern=?, resource_type=?, members=? WHERE id=?`,
		g.Aggregation, g.Pattern, g.ResourceType, g.Members, id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProtectionGroupNotFound
	}
	return nil
}

func (s *Store) DeleteProtectionGroup(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM protection_groups WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProtectionGroupNotFound
	}
	return nil
}

// --- Subscription ---

func (s *Store) GetSubscription() (*Subscription, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, state, start_time, end_time, auto_renew, proactive_engagement
		 FROM subscription WHERE id = 'default'`)
	var sub Subscription
	err := row.Scan(&sub.ID, &sub.State, &sub.StartTime, &sub.EndTime, &sub.AutoRenew, &sub.ProactiveEngagement)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("subscription not found")
		}
		return nil, err
	}
	return &sub, nil
}

func (s *Store) UpdateSubscription(fields map[string]any) error {
	sub, err := s.GetSubscription()
	if err != nil {
		return err
	}
	if v, ok := fields["AutoRenew"].(string); ok && v != "" {
		sub.AutoRenew = v
	}
	_, err = s.store.DB().Exec(
		`UPDATE subscription SET auto_renew=? WHERE id='default'`, sub.AutoRenew)
	return err
}

func (s *Store) SetSubscriptionState(state string) error {
	_, err := s.store.DB().Exec(
		`UPDATE subscription SET state=? WHERE id='default'`, state)
	return err
}

func (s *Store) SetProactiveEngagement(status string) error {
	_, err := s.store.DB().Exec(
		`UPDATE subscription SET proactive_engagement=? WHERE id='default'`, status)
	return err
}

// --- scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanProtection(sc scanner) (*Protection, error) {
	var p Protection
	var createdAt int64
	err := sc.Scan(&p.ID, &p.ARN, &p.Name, &p.ResourceARN, &p.HealthCheckIDs, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errProtectionNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}

func scanProtectionGroup(sc scanner) (*ProtectionGroup, error) {
	var g ProtectionGroup
	var createdAt int64
	err := sc.Scan(&g.ID, &g.ARN, &g.Aggregation, &g.Pattern, &g.ResourceType, &g.Members, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errProtectionGroupNotFound
		}
		return nil, err
	}
	g.CreatedAt = time.Unix(createdAt, 0)
	return &g, nil
}
