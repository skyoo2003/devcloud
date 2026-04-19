// SPDX-License-Identifier: Apache-2.0

// internal/services/wafv2/store.go
package wafv2

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errWebACLNotFound          = errors.New("web ACL not found")
	errIPSetNotFound           = errors.New("IP set not found")
	errRegexPatternSetNotFound = errors.New("regex pattern set not found")
	errRuleGroupNotFound       = errors.New("rule group not found")
	errAPIKeyNotFound          = errors.New("API key not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS web_acls (
			id               TEXT NOT NULL,
			name             TEXT NOT NULL,
			arn              TEXT NOT NULL UNIQUE,
			scope            TEXT NOT NULL DEFAULT 'REGIONAL',
			description      TEXT NOT NULL DEFAULT '',
			default_action   TEXT NOT NULL DEFAULT '{"Allow":{}}',
			rules            TEXT NOT NULL DEFAULT '[]',
			visibility_config TEXT NOT NULL DEFAULT '{}',
			lock_token       TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL,
			PRIMARY KEY (name, scope)
		);
		CREATE TABLE IF NOT EXISTS web_acl_associations (
			resource_arn  TEXT PRIMARY KEY,
			web_acl_arn   TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ip_sets (
			id          TEXT NOT NULL,
			name        TEXT NOT NULL,
			arn         TEXT NOT NULL UNIQUE,
			scope       TEXT NOT NULL DEFAULT 'REGIONAL',
			description TEXT NOT NULL DEFAULT '',
			ip_version  TEXT NOT NULL DEFAULT 'IPV4',
			addresses   TEXT NOT NULL DEFAULT '[]',
			lock_token  TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (name, scope)
		);
		CREATE TABLE IF NOT EXISTS regex_pattern_sets (
			id          TEXT NOT NULL,
			name        TEXT NOT NULL,
			arn         TEXT NOT NULL UNIQUE,
			scope       TEXT NOT NULL DEFAULT 'REGIONAL',
			description TEXT NOT NULL DEFAULT '',
			patterns    TEXT NOT NULL DEFAULT '[]',
			lock_token  TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (name, scope)
		);
		CREATE TABLE IF NOT EXISTS rule_groups (
			id               TEXT NOT NULL,
			name             TEXT NOT NULL,
			arn              TEXT NOT NULL UNIQUE,
			scope            TEXT NOT NULL DEFAULT 'REGIONAL',
			description      TEXT NOT NULL DEFAULT '',
			capacity         INTEGER NOT NULL DEFAULT 100,
			rules            TEXT NOT NULL DEFAULT '[]',
			visibility_config TEXT NOT NULL DEFAULT '{}',
			lock_token       TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			PRIMARY KEY (name, scope)
		);
		CREATE TABLE IF NOT EXISTS api_keys (
			key          TEXT PRIMARY KEY,
			scope        TEXT NOT NULL DEFAULT 'REGIONAL',
			token_domains TEXT NOT NULL DEFAULT '[]',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS logging_configs (
			resource_arn        TEXT PRIMARY KEY,
			log_destination_configs TEXT NOT NULL DEFAULT '[]',
			redacted_fields     TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS permission_policies (
			resource_arn TEXT PRIMARY KEY,
			policy       TEXT NOT NULL DEFAULT ''
		);
	`},
}

// --- model types ---

type WebACL struct {
	ID               string
	Name             string
	ARN              string
	Scope            string
	Description      string
	DefaultAction    string
	Rules            string
	VisibilityConfig string
	LockToken        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type IPSet struct {
	ID          string
	Name        string
	ARN         string
	Scope       string
	Description string
	IPVersion   string
	Addresses   string
	LockToken   string
	CreatedAt   time.Time
}

type RegexPatternSet struct {
	ID          string
	Name        string
	ARN         string
	Scope       string
	Description string
	Patterns    string
	LockToken   string
	CreatedAt   time.Time
}

type RuleGroup struct {
	ID               string
	Name             string
	ARN              string
	Scope            string
	Description      string
	Capacity         int64
	Rules            string
	VisibilityConfig string
	LockToken        string
	CreatedAt        time.Time
}

type APIKey struct {
	Key          string
	Scope        string
	TokenDomains string
	CreatedAt    time.Time
}

// --- store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "wafv2.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- WebACL ---

func (s *Store) CreateWebACL(w *WebACL) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO web_acls (id, name, arn, scope, description, default_action, rules, visibility_config, lock_token, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.Name, w.ARN, w.Scope, w.Description,
		w.DefaultAction, w.Rules, w.VisibilityConfig, w.LockToken, now, now,
	)
	return err
}

func (s *Store) GetWebACL(name, scope string) (*WebACL, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, name, arn, scope, description, default_action, rules, visibility_config, lock_token, created_at, updated_at
		FROM web_acls WHERE name = ? AND scope = ?`, name, scope)
	return scanWebACL(row)
}

func (s *Store) GetWebACLByARN(arn string) (*WebACL, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, name, arn, scope, description, default_action, rules, visibility_config, lock_token, created_at, updated_at
		FROM web_acls WHERE arn = ?`, arn)
	return scanWebACL(row)
}

func (s *Store) ListWebACLs(scope string) ([]WebACL, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, name, arn, scope, description, default_action, rules, visibility_config, lock_token, created_at, updated_at
		FROM web_acls WHERE scope = ? ORDER BY created_at`, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var acls []WebACL
	for rows.Next() {
		w, err := scanWebACL(rows)
		if err != nil {
			return nil, err
		}
		acls = append(acls, *w)
	}
	return acls, rows.Err()
}

func (s *Store) UpdateWebACL(name, scope string, fields map[string]any) error {
	w, err := s.GetWebACL(name, scope)
	if err != nil {
		return errWebACLNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		w.Description = v
	}
	if v, ok := fields["DefaultAction"].(string); ok {
		w.DefaultAction = v
	}
	if v, ok := fields["Rules"].(string); ok {
		w.Rules = v
	}
	if v, ok := fields["VisibilityConfig"].(string); ok {
		w.VisibilityConfig = v
	}
	newToken := shared.GenerateID("", 32)
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(`
		UPDATE web_acls SET description=?, default_action=?, rules=?, visibility_config=?, lock_token=?, updated_at=?
		WHERE name=? AND scope=?`,
		w.Description, w.DefaultAction, w.Rules, w.VisibilityConfig, newToken, now, name, scope,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWebACLNotFound
	}
	fields["LockToken"] = newToken
	return nil
}

func (s *Store) DeleteWebACL(name, scope string) error {
	res, err := s.store.DB().Exec(`DELETE FROM web_acls WHERE name = ? AND scope = ?`, name, scope)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errWebACLNotFound
	}
	return nil
}

// --- WebACL associations ---

func (s *Store) AssociateWebACL(resourceARN, webACLARN string) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO web_acl_associations (resource_arn, web_acl_arn) VALUES (?, ?)
		ON CONFLICT(resource_arn) DO UPDATE SET web_acl_arn=excluded.web_acl_arn`,
		resourceARN, webACLARN)
	return err
}

func (s *Store) DisassociateWebACL(resourceARN string) error {
	_, err := s.store.DB().Exec(`DELETE FROM web_acl_associations WHERE resource_arn = ?`, resourceARN)
	return err
}

func (s *Store) GetWebACLForResource(resourceARN string) (string, error) {
	var webACLARN string
	err := s.store.DB().QueryRow(
		`SELECT web_acl_arn FROM web_acl_associations WHERE resource_arn = ?`, resourceARN,
	).Scan(&webACLARN)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return webACLARN, nil
}

func (s *Store) ListResourcesForWebACL(webACLARN string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT resource_arn FROM web_acl_associations WHERE web_acl_arn = ?`, webACLARN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var arns []string
	for rows.Next() {
		var arn string
		if err := rows.Scan(&arn); err != nil {
			return nil, err
		}
		arns = append(arns, arn)
	}
	return arns, rows.Err()
}

// --- IPSet ---

func (s *Store) CreateIPSet(ip *IPSet) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO ip_sets (id, name, arn, scope, description, ip_version, addresses, lock_token, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ip.ID, ip.Name, ip.ARN, ip.Scope, ip.Description, ip.IPVersion, ip.Addresses, ip.LockToken, now,
	)
	return err
}

func (s *Store) GetIPSet(name, scope string) (*IPSet, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, name, arn, scope, description, ip_version, addresses, lock_token, created_at
		FROM ip_sets WHERE name = ? AND scope = ?`, name, scope)
	return scanIPSet(row)
}

func (s *Store) ListIPSets(scope string) ([]IPSet, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, name, arn, scope, description, ip_version, addresses, lock_token, created_at
		FROM ip_sets WHERE scope = ? ORDER BY created_at`, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sets []IPSet
	for rows.Next() {
		ip, err := scanIPSet(rows)
		if err != nil {
			return nil, err
		}
		sets = append(sets, *ip)
	}
	return sets, rows.Err()
}

func (s *Store) UpdateIPSet(name, scope string, fields map[string]any) error {
	ip, err := s.GetIPSet(name, scope)
	if err != nil {
		return errIPSetNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		ip.Description = v
	}
	if v, ok := fields["Addresses"].(string); ok {
		ip.Addresses = v
	}
	newToken := shared.GenerateID("", 32)
	res, err := s.store.DB().Exec(`
		UPDATE ip_sets SET description=?, addresses=?, lock_token=? WHERE name=? AND scope=?`,
		ip.Description, ip.Addresses, newToken, name, scope,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errIPSetNotFound
	}
	fields["LockToken"] = newToken
	return nil
}

func (s *Store) DeleteIPSet(name, scope string) error {
	res, err := s.store.DB().Exec(`DELETE FROM ip_sets WHERE name = ? AND scope = ?`, name, scope)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errIPSetNotFound
	}
	return nil
}

// --- RegexPatternSet ---

func (s *Store) CreateRegexPatternSet(r *RegexPatternSet) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO regex_pattern_sets (id, name, arn, scope, description, patterns, lock_token, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.Name, r.ARN, r.Scope, r.Description, r.Patterns, r.LockToken, now,
	)
	return err
}

func (s *Store) GetRegexPatternSet(name, scope string) (*RegexPatternSet, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, name, arn, scope, description, patterns, lock_token, created_at
		FROM regex_pattern_sets WHERE name = ? AND scope = ?`, name, scope)
	return scanRegexPatternSet(row)
}

func (s *Store) ListRegexPatternSets(scope string) ([]RegexPatternSet, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, name, arn, scope, description, patterns, lock_token, created_at
		FROM regex_pattern_sets WHERE scope = ? ORDER BY created_at`, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sets []RegexPatternSet
	for rows.Next() {
		r, err := scanRegexPatternSet(rows)
		if err != nil {
			return nil, err
		}
		sets = append(sets, *r)
	}
	return sets, rows.Err()
}

func (s *Store) UpdateRegexPatternSet(name, scope string, fields map[string]any) error {
	r, err := s.GetRegexPatternSet(name, scope)
	if err != nil {
		return errRegexPatternSetNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		r.Description = v
	}
	if v, ok := fields["Patterns"].(string); ok {
		r.Patterns = v
	}
	newToken := shared.GenerateID("", 32)
	res, err := s.store.DB().Exec(`
		UPDATE regex_pattern_sets SET description=?, patterns=?, lock_token=? WHERE name=? AND scope=?`,
		r.Description, r.Patterns, newToken, name, scope,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRegexPatternSetNotFound
	}
	fields["LockToken"] = newToken
	return nil
}

func (s *Store) DeleteRegexPatternSet(name, scope string) error {
	res, err := s.store.DB().Exec(`DELETE FROM regex_pattern_sets WHERE name = ? AND scope = ?`, name, scope)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRegexPatternSetNotFound
	}
	return nil
}

// --- RuleGroup ---

func (s *Store) CreateRuleGroup(rg *RuleGroup) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO rule_groups (id, name, arn, scope, description, capacity, rules, visibility_config, lock_token, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rg.ID, rg.Name, rg.ARN, rg.Scope, rg.Description, rg.Capacity, rg.Rules, rg.VisibilityConfig, rg.LockToken, now,
	)
	return err
}

func (s *Store) GetRuleGroup(name, scope string) (*RuleGroup, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, name, arn, scope, description, capacity, rules, visibility_config, lock_token, created_at
		FROM rule_groups WHERE name = ? AND scope = ?`, name, scope)
	return scanRuleGroup(row)
}

func (s *Store) ListRuleGroups(scope string) ([]RuleGroup, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, name, arn, scope, description, capacity, rules, visibility_config, lock_token, created_at
		FROM rule_groups WHERE scope = ? ORDER BY created_at`, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var groups []RuleGroup
	for rows.Next() {
		rg, err := scanRuleGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *rg)
	}
	return groups, rows.Err()
}

func (s *Store) UpdateRuleGroup(name, scope string, fields map[string]any) error {
	rg, err := s.GetRuleGroup(name, scope)
	if err != nil {
		return errRuleGroupNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		rg.Description = v
	}
	if v, ok := fields["Rules"].(string); ok {
		rg.Rules = v
	}
	if v, ok := fields["VisibilityConfig"].(string); ok {
		rg.VisibilityConfig = v
	}
	newToken := shared.GenerateID("", 32)
	res, err := s.store.DB().Exec(`
		UPDATE rule_groups SET description=?, rules=?, visibility_config=?, lock_token=? WHERE name=? AND scope=?`,
		rg.Description, rg.Rules, rg.VisibilityConfig, newToken, name, scope,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRuleGroupNotFound
	}
	fields["LockToken"] = newToken
	return nil
}

func (s *Store) DeleteRuleGroup(name, scope string) error {
	res, err := s.store.DB().Exec(`DELETE FROM rule_groups WHERE name = ? AND scope = ?`, name, scope)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRuleGroupNotFound
	}
	return nil
}

// --- APIKey ---

func (s *Store) CreateAPIKey(k *APIKey) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO api_keys (key, scope, token_domains, created_at)
		VALUES (?, ?, ?, ?)`,
		k.Key, k.Scope, k.TokenDomains, now,
	)
	return err
}

func (s *Store) ListAPIKeys(scope string) ([]APIKey, error) {
	rows, err := s.store.DB().Query(`
		SELECT key, scope, token_domains, created_at FROM api_keys
		WHERE scope = ? ORDER BY created_at`, scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []APIKey
	for rows.Next() {
		var k APIKey
		var createdAt int64
		if err := rows.Scan(&k.Key, &k.Scope, &k.TokenDomains, &createdAt); err != nil {
			return nil, err
		}
		k.CreatedAt = time.Unix(createdAt, 0)
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (s *Store) DeleteAPIKey(key string) error {
	res, err := s.store.DB().Exec(`DELETE FROM api_keys WHERE key = ?`, key)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAPIKeyNotFound
	}
	return nil
}

func (s *Store) GetAPIKey(key string) (*APIKey, error) {
	row := s.store.DB().QueryRow(
		`SELECT key, scope, token_domains, created_at FROM api_keys WHERE key = ?`, key)
	var k APIKey
	var createdAt int64
	if err := row.Scan(&k.Key, &k.Scope, &k.TokenDomains, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAPIKeyNotFound
		}
		return nil, err
	}
	k.CreatedAt = time.Unix(createdAt, 0)
	return &k, nil
}

// --- Logging config ---

func (s *Store) PutLoggingConfig(resourceARN, destinations, redactedFields string) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO logging_configs (resource_arn, log_destination_configs, redacted_fields)
		VALUES (?, ?, ?)
		ON CONFLICT(resource_arn) DO UPDATE SET log_destination_configs=excluded.log_destination_configs, redacted_fields=excluded.redacted_fields`,
		resourceARN, destinations, redactedFields)
	return err
}

func (s *Store) GetLoggingConfig(resourceARN string) (string, string, error) {
	var destinations, redactedFields string
	err := s.store.DB().QueryRow(
		`SELECT log_destination_configs, redacted_fields FROM logging_configs WHERE resource_arn = ?`,
		resourceARN,
	).Scan(&destinations, &redactedFields)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", errors.New("logging config not found")
		}
		return "", "", err
	}
	return destinations, redactedFields, nil
}

func (s *Store) ListLoggingConfigs() ([][3]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT resource_arn, log_destination_configs, redacted_fields FROM logging_configs`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var configs [][3]string
	for rows.Next() {
		var c [3]string
		if err := rows.Scan(&c[0], &c[1], &c[2]); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, rows.Err()
}

func (s *Store) DeleteLoggingConfig(resourceARN string) error {
	_, err := s.store.DB().Exec(`DELETE FROM logging_configs WHERE resource_arn = ?`, resourceARN)
	return err
}

// --- Permission policy ---

func (s *Store) PutPermissionPolicy(resourceARN, policy string) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO permission_policies (resource_arn, policy) VALUES (?, ?)
		ON CONFLICT(resource_arn) DO UPDATE SET policy=excluded.policy`,
		resourceARN, policy)
	return err
}

func (s *Store) GetPermissionPolicy(resourceARN string) (string, error) {
	var policy string
	err := s.store.DB().QueryRow(
		`SELECT policy FROM permission_policies WHERE resource_arn = ?`, resourceARN,
	).Scan(&policy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errors.New("permission policy not found")
		}
		return "", err
	}
	return policy, nil
}

func (s *Store) DeletePermissionPolicy(resourceARN string) error {
	_, err := s.store.DB().Exec(`DELETE FROM permission_policies WHERE resource_arn = ?`, resourceARN)
	return err
}

// --- scanners ---

type rowScanner interface{ Scan(dest ...any) error }

func scanWebACL(sc rowScanner) (*WebACL, error) {
	var w WebACL
	var createdAt, updatedAt int64
	err := sc.Scan(
		&w.ID, &w.Name, &w.ARN, &w.Scope, &w.Description,
		&w.DefaultAction, &w.Rules, &w.VisibilityConfig, &w.LockToken,
		&createdAt, &updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errWebACLNotFound
		}
		return nil, err
	}
	w.CreatedAt = time.Unix(createdAt, 0)
	w.UpdatedAt = time.Unix(updatedAt, 0)
	return &w, nil
}

func scanIPSet(sc rowScanner) (*IPSet, error) {
	var ip IPSet
	var createdAt int64
	err := sc.Scan(
		&ip.ID, &ip.Name, &ip.ARN, &ip.Scope, &ip.Description,
		&ip.IPVersion, &ip.Addresses, &ip.LockToken, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errIPSetNotFound
		}
		return nil, err
	}
	ip.CreatedAt = time.Unix(createdAt, 0)
	return &ip, nil
}

func scanRegexPatternSet(sc rowScanner) (*RegexPatternSet, error) {
	var r RegexPatternSet
	var createdAt int64
	err := sc.Scan(
		&r.ID, &r.Name, &r.ARN, &r.Scope, &r.Description,
		&r.Patterns, &r.LockToken, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRegexPatternSetNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func scanRuleGroup(sc rowScanner) (*RuleGroup, error) {
	var rg RuleGroup
	var createdAt int64
	err := sc.Scan(
		&rg.ID, &rg.Name, &rg.ARN, &rg.Scope, &rg.Description,
		&rg.Capacity, &rg.Rules, &rg.VisibilityConfig, &rg.LockToken, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRuleGroupNotFound
		}
		return nil, err
	}
	rg.CreatedAt = time.Unix(createdAt, 0)
	return &rg, nil
}
