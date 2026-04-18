// SPDX-License-Identifier: Apache-2.0

// internal/services/resourcegroupstaggingapi/store.go
package resourcegroupstaggingapi

import (
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS resource_tags (
			resource_arn  TEXT NOT NULL,
			tag_key       TEXT NOT NULL,
			tag_value     TEXT NOT NULL,
			resource_type TEXT NOT NULL DEFAULT '',
			account_id    TEXT NOT NULL DEFAULT '000000000000',
			PRIMARY KEY (resource_arn, tag_key)
		);
	`},
}

// TagEntry holds a resource ARN + one tag pair.
type TagEntry struct {
	ResourceARN  string
	TagKey       string
	TagValue     string
	ResourceType string
	AccountID    string
}

// ResourceTagMapping groups all tags for one ARN.
type ResourceTagMapping struct {
	ResourceARN  string
	ResourceType string
	Tags         []TagEntry
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "resourcegroupstaggingapi.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// PutTags upserts tags for a resource ARN.
func (s *Store) PutTags(resourceARN string, tags map[string]string, accountID string) error {
	resourceType := arnToResourceType(resourceARN)
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO resource_tags (resource_arn, tag_key, tag_value, resource_type, account_id)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(resource_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			resourceARN, k, v, resourceType, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteTags removes specific tag keys from a resource ARN.
func (s *Store) DeleteTags(resourceARN string, tagKeys []string) error {
	for _, k := range tagKeys {
		_, err := s.store.DB().Exec(
			`DELETE FROM resource_tags WHERE resource_arn=? AND tag_key=?`, resourceARN, k)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetResources returns resource tag mappings filtered by optional tag filters and resource type filters.
func (s *Store) GetResources(tagFilters []TagFilter, resourceTypeFilters []string, accountID string) ([]ResourceTagMapping, error) {
	q := `SELECT resource_arn, tag_key, tag_value, resource_type FROM resource_tags WHERE account_id=?`
	args := []any{accountID}

	// Apply resource type filters
	if len(resourceTypeFilters) > 0 {
		placeholders := make([]string, len(resourceTypeFilters))
		for i, rt := range resourceTypeFilters {
			placeholders[i] = "?"
			args = append(args, rt)
		}
		q += ` AND resource_type IN (` + strings.Join(placeholders, ",") + `)`
	}

	q += ` ORDER BY resource_arn, tag_key`

	rows, err := s.store.DB().Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	// Group by ARN
	arnMap := map[string]*ResourceTagMapping{}
	var arns []string
	for rows.Next() {
		var arn, key, val, resType string
		if err := rows.Scan(&arn, &key, &val, &resType); err != nil {
			return nil, err
		}
		if _, ok := arnMap[arn]; !ok {
			arnMap[arn] = &ResourceTagMapping{ResourceARN: arn, ResourceType: resType}
			arns = append(arns, arn)
		}
		arnMap[arn].Tags = append(arnMap[arn].Tags, TagEntry{ResourceARN: arn, TagKey: key, TagValue: val})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Apply tag filters
	var result []ResourceTagMapping
	for _, arn := range arns {
		rm := arnMap[arn]
		if matchesTagFilters(rm.Tags, tagFilters) {
			result = append(result, *rm)
		}
	}
	return result, nil
}

// GetTagKeys returns all distinct tag keys for an account.
func (s *Store) GetTagKeys(accountID string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT DISTINCT tag_key FROM resource_tags WHERE account_id=? ORDER BY tag_key`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// GetTagValues returns all distinct values for a given key.
func (s *Store) GetTagValues(key, accountID string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT DISTINCT tag_value FROM resource_tags WHERE tag_key=? AND account_id=? ORDER BY tag_value`,
		key, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var vals []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return vals, rows.Err()
}

// TagFilter represents a single filter (key + optional values).
type TagFilter struct {
	Key    string
	Values []string
}

func matchesTagFilters(tags []TagEntry, filters []TagFilter) bool {
	if len(filters) == 0 {
		return true
	}
	tagMap := map[string]string{}
	for _, t := range tags {
		tagMap[t.TagKey] = t.TagValue
	}
	for _, f := range filters {
		val, ok := tagMap[f.Key]
		if !ok {
			return false
		}
		if len(f.Values) > 0 {
			found := false
			for _, v := range f.Values {
				if v == val {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// arnToResourceType extracts a resource type from an ARN like "arn:aws:s3:::bucket" -> "s3"
func arnToResourceType(arn string) string {
	// arn:aws:{service}:{region}:{account}:{resource}
	parts := strings.Split(arn, ":")
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

// GetAllResourceARNs returns all ARNs for an account (for compliance summaries).
func (s *Store) GetAllResourceARNs(accountID string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT DISTINCT resource_arn FROM resource_tags WHERE account_id=?`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var arns []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		arns = append(arns, a)
	}
	return arns, rows.Err()
}
