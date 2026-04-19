// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"fmt"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var TagMigrations = []sqlite.Migration{
	{Version: 1000, SQL: `
		CREATE TABLE IF NOT EXISTS resource_tags (
			resource_arn TEXT NOT NULL,
			key          TEXT NOT NULL,
			value        TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (resource_arn, key)
		);
	`},
}

type TagStore struct {
	db *sqlite.Store
}

func NewTagStore(db *sqlite.Store) *TagStore {
	return &TagStore{db: db}
}

func (s *TagStore) AddTags(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db.DB().Exec(`
			INSERT INTO resource_tags (resource_arn, key, value) VALUES (?, ?, ?)
			ON CONFLICT(resource_arn, key) DO UPDATE SET value=excluded.value`,
			arn, k, v)
		if err != nil {
			return fmt.Errorf("add tag %q: %w", k, err)
		}
	}
	return nil
}

func (s *TagStore) RemoveTags(arn string, keys []string) error {
	for _, k := range keys {
		if _, err := s.db.DB().Exec(
			`DELETE FROM resource_tags WHERE resource_arn = ? AND key = ?`, arn, k); err != nil {
			return err
		}
	}
	return nil
}

func (s *TagStore) ListTags(arn string) (map[string]string, error) {
	rows, err := s.db.DB().Query(
		`SELECT key, value FROM resource_tags WHERE resource_arn = ? ORDER BY key`, arn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tags := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

func (s *TagStore) DeleteAllTags(arn string) error {
	_, err := s.db.DB().Exec(`DELETE FROM resource_tags WHERE resource_arn = ?`, arn)
	return err
}
