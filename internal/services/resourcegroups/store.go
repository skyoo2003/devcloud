// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroups/store.go
package resourcegroups

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errGroupNotFound = errors.New("group not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS groups_table (
            name          TEXT PRIMARY KEY,
            arn           TEXT NOT NULL UNIQUE,
            description   TEXT NOT NULL DEFAULT '',
            resource_query TEXT NOT NULL DEFAULT '{}',
            config        TEXT NOT NULL DEFAULT '{}',
            created_at    INTEGER NOT NULL
        );
    `},
}

type Group struct {
	Name          string
	ARN           string
	Description   string
	ResourceQuery string
	Config        string
	CreatedAt     time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "resourcegroups.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateGroup(g *Group) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO groups_table (name, arn, description, resource_query, config, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
		g.Name, g.ARN, g.Description, g.ResourceQuery, g.Config, now,
	)
	return err
}

func (s *Store) GetGroup(name string) (*Group, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, resource_query, config, created_at FROM groups_table WHERE name = ?`, name)
	return scanGroup(row)
}

func (s *Store) ListGroups() ([]Group, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, resource_query, config, created_at FROM groups_table ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *Store) UpdateGroup(name string, fields map[string]any) error {
	g, err := s.GetGroup(name)
	if err != nil {
		return errGroupNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		g.Description = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE groups_table SET description=? WHERE name=?`,
		g.Description, name,
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
	res, err := s.store.DB().Exec(`DELETE FROM groups_table WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

func (s *Store) UpdateGroupQuery(name, resourceQuery string) error {
	res, err := s.store.DB().Exec(
		`UPDATE groups_table SET resource_query=? WHERE name=?`,
		resourceQuery, name,
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

func (s *Store) UpdateGroupConfig(name, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE groups_table SET config=? WHERE name=?`,
		config, name,
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

type scanner interface{ Scan(dest ...any) error }

func scanGroup(sc scanner) (*Group, error) {
	var g Group
	var createdAt int64
	err := sc.Scan(&g.Name, &g.ARN, &g.Description, &g.ResourceQuery, &g.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupNotFound
		}
		return nil, err
	}
	g.CreatedAt = time.Unix(createdAt, 0)
	return &g, nil
}
