// SPDX-License-Identifier: Apache-2.0

// internal/services/iot/store.go
package iot

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errThingNotFound      = errors.New("thing not found")
	errThingTypeNotFound  = errors.New("thing type not found")
	errThingGroupNotFound = errors.New("thing group not found")
	errPolicyNotFound     = errors.New("policy not found")
	errCertNotFound       = errors.New("certificate not found")
	errTopicRuleNotFound  = errors.New("topic rule not found")
	errJobNotFound        = errors.New("job not found")
	errRoleAliasNotFound  = errors.New("role alias not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS things (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			type_name   TEXT NOT NULL DEFAULT '',
			attributes  TEXT NOT NULL DEFAULT '{}',
			version     INTEGER NOT NULL DEFAULT 1,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS thing_types (
			name             TEXT PRIMARY KEY,
			arn              TEXT NOT NULL UNIQUE,
			description      TEXT NOT NULL DEFAULT '',
			searchable_attrs TEXT NOT NULL DEFAULT '[]',
			deprecated       INTEGER NOT NULL DEFAULT 0,
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS thing_groups (
			name        TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			parent      TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			attributes  TEXT NOT NULL DEFAULT '{}',
			version     INTEGER NOT NULL DEFAULT 1,
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS policies (
			name       TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			document   TEXT NOT NULL DEFAULT '{}',
			version    INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS certificates (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			status     TEXT NOT NULL DEFAULT 'ACTIVE',
			pem        TEXT NOT NULL DEFAULT '',
			ca_pem     TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS topic_rules (
			name       TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			sql_query  TEXT NOT NULL DEFAULT '',
			actions    TEXT NOT NULL DEFAULT '[]',
			enabled    INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS jobs (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			status      TEXT NOT NULL DEFAULT 'IN_PROGRESS',
			targets     TEXT NOT NULL DEFAULT '[]',
			document    TEXT NOT NULL DEFAULT '{}',
			description TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS role_aliases (
			name                TEXT PRIMARY KEY,
			arn                 TEXT NOT NULL UNIQUE,
			role_arn            TEXT NOT NULL DEFAULT '',
			credential_duration INTEGER NOT NULL DEFAULT 3600,
			created_at          INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS thing_group_members (
			group_name TEXT NOT NULL,
			thing_name TEXT NOT NULL,
			PRIMARY KEY (group_name, thing_name)
		);
		CREATE TABLE IF NOT EXISTS thing_principals (
			thing_name TEXT NOT NULL,
			principal  TEXT NOT NULL,
			PRIMARY KEY (thing_name, principal)
		);
		CREATE TABLE IF NOT EXISTS policy_targets (
			policy_name TEXT NOT NULL,
			target      TEXT NOT NULL,
			PRIMARY KEY (policy_name, target)
		);
		CREATE TABLE IF NOT EXISTS policy_versions (
			policy_name TEXT NOT NULL,
			version_id  TEXT NOT NULL,
			document    TEXT NOT NULL DEFAULT '{}',
			is_default  INTEGER NOT NULL DEFAULT 0,
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (policy_name, version_id)
		);
	`},
}

// --- Model types ---

type Thing struct {
	Name       string
	ARN        string
	TypeName   string
	Attributes string // JSON
	Version    int64
	CreatedAt  int64
}

type ThingType struct {
	Name            string
	ARN             string
	Description     string
	SearchableAttrs string // JSON array
	Deprecated      bool
	CreatedAt       int64
}

type ThingGroup struct {
	Name        string
	ARN         string
	Parent      string
	Description string
	Attributes  string // JSON
	Version     int64
	CreatedAt   int64
}

type Policy struct {
	Name      string
	ARN       string
	Document  string // JSON
	Version   int64
	CreatedAt int64
}

type PolicyVersion struct {
	PolicyName string
	VersionID  string
	Document   string // JSON
	IsDefault  bool
	CreatedAt  int64
}

type Certificate struct {
	ID        string
	ARN       string
	Status    string
	PEM       string
	CAPEM     string
	CreatedAt int64
}

type TopicRule struct {
	Name      string
	ARN       string
	SQLQuery  string
	Actions   string // JSON
	Enabled   bool
	CreatedAt int64
}

type Job struct {
	ID          string
	ARN         string
	Status      string
	Targets     string // JSON
	Document    string // JSON
	Description string
	CreatedAt   int64
}

type RoleAlias struct {
	Name               string
	ARN                string
	RoleARN            string
	CredentialDuration int64
	CreatedAt          int64
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "iot.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Things ---

func (s *Store) CreateThing(t *Thing) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO things (name, arn, type_name, attributes, version, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		t.Name, t.ARN, t.TypeName, t.Attributes, t.Version, t.CreatedAt,
	)
	return err
}

func (s *Store) GetThing(name string) (*Thing, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, type_name, attributes, version, created_at FROM things WHERE name = ?`, name)
	return scanThing(row)
}

func (s *Store) ListThings() ([]Thing, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, type_name, attributes, version, created_at FROM things ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Thing
	for rows.Next() {
		t, err := scanThing(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *t)
	}
	return result, rows.Err()
}

func (s *Store) UpdateThing(name string, fields map[string]any) error {
	t, err := s.GetThing(name)
	if err != nil {
		return errThingNotFound
	}
	if v, ok := fields["thingTypeName"].(string); ok {
		t.TypeName = v
	}
	if v, ok := fields["attributePayload"].(map[string]any); ok {
		if attrs, ok2 := v["attributes"].(map[string]any); ok2 {
			if b, err2 := json.Marshal(attrs); err2 == nil {
				t.Attributes = string(b)
			}
		}
	}
	t.Version++
	res, err := s.store.DB().Exec(
		`UPDATE things SET type_name=?, attributes=?, version=? WHERE name=?`,
		t.TypeName, t.Attributes, t.Version, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errThingNotFound
	}
	return nil
}

func (s *Store) DeleteThing(name string) (*Thing, error) {
	t, err := s.GetThing(name)
	if err != nil {
		return nil, errThingNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM things WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return t, nil
}

// --- Thing Types ---

func (s *Store) CreateThingType(tt *ThingType) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO thing_types (name, arn, description, searchable_attrs, deprecated, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		tt.Name, tt.ARN, tt.Description, tt.SearchableAttrs, boolToInt(tt.Deprecated), tt.CreatedAt,
	)
	return err
}

func (s *Store) GetThingType(name string) (*ThingType, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, searchable_attrs, deprecated, created_at FROM thing_types WHERE name = ?`, name)
	return scanThingType(row)
}

func (s *Store) ListThingTypes() ([]ThingType, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, searchable_attrs, deprecated, created_at FROM thing_types ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []ThingType
	for rows.Next() {
		tt, err := scanThingType(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *tt)
	}
	return result, rows.Err()
}

func (s *Store) DeprecateThingType(name string, deprecated bool) error {
	res, err := s.store.DB().Exec(
		`UPDATE thing_types SET deprecated=? WHERE name=?`, boolToInt(deprecated), name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errThingTypeNotFound
	}
	return nil
}

func (s *Store) DeleteThingType(name string) (*ThingType, error) {
	tt, err := s.GetThingType(name)
	if err != nil {
		return nil, errThingTypeNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM thing_types WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return tt, nil
}

// --- Thing Groups ---

func (s *Store) CreateThingGroup(tg *ThingGroup) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO thing_groups (name, arn, parent, description, attributes, version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		tg.Name, tg.ARN, tg.Parent, tg.Description, tg.Attributes, tg.Version, tg.CreatedAt,
	)
	return err
}

func (s *Store) GetThingGroup(name string) (*ThingGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, parent, description, attributes, version, created_at FROM thing_groups WHERE name = ?`, name)
	return scanThingGroup(row)
}

func (s *Store) ListThingGroups() ([]ThingGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, parent, description, attributes, version, created_at FROM thing_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []ThingGroup
	for rows.Next() {
		tg, err := scanThingGroup(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *tg)
	}
	return result, rows.Err()
}

func (s *Store) UpdateThingGroup(name string, fields map[string]any) error {
	tg, err := s.GetThingGroup(name)
	if err != nil {
		return errThingGroupNotFound
	}
	if v, ok := fields["thingGroupProperties"].(map[string]any); ok {
		if desc, ok2 := v["thingGroupDescription"].(string); ok2 {
			tg.Description = desc
		}
	}
	tg.Version++
	res, err := s.store.DB().Exec(
		`UPDATE thing_groups SET description=?, version=? WHERE name=?`,
		tg.Description, tg.Version, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errThingGroupNotFound
	}
	return nil
}

func (s *Store) DeleteThingGroup(name string) (*ThingGroup, error) {
	tg, err := s.GetThingGroup(name)
	if err != nil {
		return nil, errThingGroupNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM thing_groups WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return tg, nil
}

func (s *Store) AddThingToThingGroup(groupName, thingName string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO thing_group_members (group_name, thing_name) VALUES (?, ?)`,
		groupName, thingName,
	)
	return err
}

func (s *Store) RemoveThingFromThingGroup(groupName, thingName string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM thing_group_members WHERE group_name=? AND thing_name=?`,
		groupName, thingName,
	)
	return err
}

func (s *Store) ListThingsInThingGroup(groupName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT thing_name FROM thing_group_members WHERE group_name=? ORDER BY thing_name`, groupName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

func (s *Store) ListThingGroupsForThing(thingName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT group_name FROM thing_group_members WHERE thing_name=? ORDER BY group_name`, thingName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

// --- Policies ---

func (s *Store) CreatePolicy(p *Policy) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO policies (name, arn, document, version, created_at) VALUES (?, ?, ?, ?, ?)`,
		p.Name, p.ARN, p.Document, p.Version, p.CreatedAt,
	)
	return err
}

func (s *Store) GetPolicy(name string) (*Policy, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, document, version, created_at FROM policies WHERE name = ?`, name)
	return scanPolicy(row)
}

func (s *Store) ListPolicies() ([]Policy, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, document, version, created_at FROM policies ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *p)
	}
	return result, rows.Err()
}

func (s *Store) DeletePolicy(name string) (*Policy, error) {
	p, err := s.GetPolicy(name)
	if err != nil {
		return nil, errPolicyNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM policies WHERE name = ?`, name); err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM policy_versions WHERE policy_name = ?`, name); err != nil {
		return nil, err
	}
	if _, err := s.store.DB().Exec(`DELETE FROM policy_targets WHERE policy_name = ?`, name); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) CreatePolicyVersion(pv *PolicyVersion) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO policy_versions (policy_name, version_id, document, is_default, created_at) VALUES (?, ?, ?, ?, ?)`,
		pv.PolicyName, pv.VersionID, pv.Document, boolToInt(pv.IsDefault), pv.CreatedAt,
	)
	return err
}

func (s *Store) GetPolicyVersion(policyName, versionID string) (*PolicyVersion, error) {
	row := s.store.DB().QueryRow(
		`SELECT policy_name, version_id, document, is_default, created_at FROM policy_versions WHERE policy_name=? AND version_id=?`,
		policyName, versionID)
	return scanPolicyVersion(row)
}

func (s *Store) ListPolicyVersions(policyName string) ([]PolicyVersion, error) {
	rows, err := s.store.DB().Query(
		`SELECT policy_name, version_id, document, is_default, created_at FROM policy_versions WHERE policy_name=? ORDER BY version_id`,
		policyName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []PolicyVersion
	for rows.Next() {
		pv, err := scanPolicyVersion(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *pv)
	}
	return result, rows.Err()
}

func (s *Store) SetDefaultPolicyVersion(policyName, versionID string) error {
	_, err := s.store.DB().Exec(
		`UPDATE policy_versions SET is_default=0 WHERE policy_name=?`, policyName)
	if err != nil {
		return err
	}
	res, err := s.store.DB().Exec(
		`UPDATE policy_versions SET is_default=1 WHERE policy_name=? AND version_id=?`,
		policyName, versionID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

func (s *Store) DeletePolicyVersion(policyName, versionID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM policy_versions WHERE policy_name=? AND version_id=? AND is_default=0`,
		policyName, versionID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

func (s *Store) AttachPolicy(policyName, target string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO policy_targets (policy_name, target) VALUES (?, ?)`,
		policyName, target,
	)
	return err
}

func (s *Store) DetachPolicy(policyName, target string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM policy_targets WHERE policy_name=? AND target=?`,
		policyName, target,
	)
	return err
}

func (s *Store) ListAttachedPolicies(target string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT policy_name FROM policy_targets WHERE target=? ORDER BY policy_name`, target)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

func (s *Store) ListTargetsForPolicy(policyName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT target FROM policy_targets WHERE policy_name=? ORDER BY target`, policyName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var target string
		if err := rows.Scan(&target); err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	return result, rows.Err()
}

// --- Certificates ---

func (s *Store) CreateCertificate(c *Certificate) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO certificates (id, arn, status, pem, ca_pem, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		c.ID, c.ARN, c.Status, c.PEM, c.CAPEM, c.CreatedAt,
	)
	return err
}

func (s *Store) GetCertificate(id string) (*Certificate, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, pem, ca_pem, created_at FROM certificates WHERE id = ?`, id)
	return scanCertificate(row)
}

func (s *Store) ListCertificates() ([]Certificate, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, status, pem, ca_pem, created_at FROM certificates ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Certificate
	for rows.Next() {
		c, err := scanCertificate(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *c)
	}
	return result, rows.Err()
}

func (s *Store) UpdateCertificate(id, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE certificates SET status=? WHERE id=?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errCertNotFound
	}
	return nil
}

func (s *Store) DeleteCertificate(id string) (*Certificate, error) {
	c, err := s.GetCertificate(id)
	if err != nil {
		return nil, errCertNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM certificates WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return c, nil
}

func (s *Store) AttachThingPrincipal(thingName, principal string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO thing_principals (thing_name, principal) VALUES (?, ?)`,
		thingName, principal,
	)
	return err
}

func (s *Store) DetachThingPrincipal(thingName, principal string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM thing_principals WHERE thing_name=? AND principal=?`,
		thingName, principal,
	)
	return err
}

func (s *Store) ListThingPrincipals(thingName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT principal FROM thing_principals WHERE thing_name=? ORDER BY principal`, thingName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *Store) ListPrincipalThings(principal string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT thing_name FROM thing_principals WHERE principal=? ORDER BY thing_name`, principal)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

// --- Topic Rules ---

func (s *Store) CreateTopicRule(tr *TopicRule) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO topic_rules (name, arn, sql_query, actions, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		tr.Name, tr.ARN, tr.SQLQuery, tr.Actions, boolToInt(tr.Enabled), tr.CreatedAt,
	)
	return err
}

func (s *Store) GetTopicRule(name string) (*TopicRule, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, sql_query, actions, enabled, created_at FROM topic_rules WHERE name = ?`, name)
	return scanTopicRule(row)
}

func (s *Store) ListTopicRules() ([]TopicRule, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, sql_query, actions, enabled, created_at FROM topic_rules ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []TopicRule
	for rows.Next() {
		tr, err := scanTopicRule(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *tr)
	}
	return result, rows.Err()
}

func (s *Store) ReplaceTopicRule(name string, fields map[string]any) error {
	tr, err := s.GetTopicRule(name)
	if err != nil {
		return errTopicRuleNotFound
	}
	if v, ok := fields["sql"].(string); ok {
		tr.SQLQuery = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE topic_rules SET sql_query=? WHERE name=?`, tr.SQLQuery, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTopicRuleNotFound
	}
	return nil
}

func (s *Store) SetTopicRuleEnabled(name string, enabled bool) error {
	res, err := s.store.DB().Exec(
		`UPDATE topic_rules SET enabled=? WHERE name=?`, boolToInt(enabled), name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTopicRuleNotFound
	}
	return nil
}

func (s *Store) DeleteTopicRule(name string) (*TopicRule, error) {
	tr, err := s.GetTopicRule(name)
	if err != nil {
		return nil, errTopicRuleNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM topic_rules WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return tr, nil
}

// --- Jobs ---

func (s *Store) CreateJob(j *Job) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO jobs (id, arn, status, targets, document, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.ARN, j.Status, j.Targets, j.Document, j.Description, j.CreatedAt,
	)
	return err
}

func (s *Store) GetJob(id string) (*Job, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, targets, document, description, created_at FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

func (s *Store) ListJobs() ([]Job, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, status, targets, document, description, created_at FROM jobs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *j)
	}
	return result, rows.Err()
}

func (s *Store) CancelJob(id string) error {
	res, err := s.store.DB().Exec(
		`UPDATE jobs SET status='CANCELED' WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errJobNotFound
	}
	return nil
}

func (s *Store) DeleteJob(id string) (*Job, error) {
	j, err := s.GetJob(id)
	if err != nil {
		return nil, errJobNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM jobs WHERE id = ?`, id); err != nil {
		return nil, err
	}
	return j, nil
}

// --- Role Aliases ---

func (s *Store) CreateRoleAlias(ra *RoleAlias) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO role_aliases (name, arn, role_arn, credential_duration, created_at) VALUES (?, ?, ?, ?, ?)`,
		ra.Name, ra.ARN, ra.RoleARN, ra.CredentialDuration, ra.CreatedAt,
	)
	return err
}

func (s *Store) GetRoleAlias(name string) (*RoleAlias, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, role_arn, credential_duration, created_at FROM role_aliases WHERE name = ?`, name)
	return scanRoleAlias(row)
}

func (s *Store) ListRoleAliases() ([]RoleAlias, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, role_arn, credential_duration, created_at FROM role_aliases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RoleAlias
	for rows.Next() {
		ra, err := scanRoleAlias(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *ra)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRoleAlias(name string, fields map[string]any) error {
	ra, err := s.GetRoleAlias(name)
	if err != nil {
		return errRoleAliasNotFound
	}
	if v, ok := fields["roleArn"].(string); ok {
		ra.RoleARN = v
	}
	if v, ok := fields["credentialDurationSeconds"].(float64); ok {
		ra.CredentialDuration = int64(v)
	}
	res, err := s.store.DB().Exec(
		`UPDATE role_aliases SET role_arn=?, credential_duration=? WHERE name=?`,
		ra.RoleARN, ra.CredentialDuration, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRoleAliasNotFound
	}
	return nil
}

func (s *Store) DeleteRoleAlias(name string) (*RoleAlias, error) {
	ra, err := s.GetRoleAlias(name)
	if err != nil {
		return nil, errRoleAliasNotFound
	}
	if _, err := s.store.DB().Exec(`DELETE FROM role_aliases WHERE name = ?`, name); err != nil {
		return nil, err
	}
	return ra, nil
}

// --- scan helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func scanThing(s scanner) (*Thing, error) {
	var t Thing
	err := s.Scan(&t.Name, &t.ARN, &t.TypeName, &t.Attributes, &t.Version, &t.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errThingNotFound
	}
	return &t, err
}

func scanThingType(s scanner) (*ThingType, error) {
	var tt ThingType
	var deprecated int
	err := s.Scan(&tt.Name, &tt.ARN, &tt.Description, &tt.SearchableAttrs, &deprecated, &tt.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errThingTypeNotFound
	}
	tt.Deprecated = deprecated != 0
	return &tt, err
}

func scanThingGroup(s scanner) (*ThingGroup, error) {
	var tg ThingGroup
	err := s.Scan(&tg.Name, &tg.ARN, &tg.Parent, &tg.Description, &tg.Attributes, &tg.Version, &tg.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errThingGroupNotFound
	}
	return &tg, err
}

func scanPolicy(s scanner) (*Policy, error) {
	var p Policy
	err := s.Scan(&p.Name, &p.ARN, &p.Document, &p.Version, &p.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errPolicyNotFound
	}
	return &p, err
}

func scanPolicyVersion(s scanner) (*PolicyVersion, error) {
	var pv PolicyVersion
	var isDefault int
	err := s.Scan(&pv.PolicyName, &pv.VersionID, &pv.Document, &isDefault, &pv.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errPolicyNotFound
	}
	pv.IsDefault = isDefault != 0
	return &pv, err
}

func scanCertificate(s scanner) (*Certificate, error) {
	var c Certificate
	err := s.Scan(&c.ID, &c.ARN, &c.Status, &c.PEM, &c.CAPEM, &c.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errCertNotFound
	}
	return &c, err
}

func scanTopicRule(s scanner) (*TopicRule, error) {
	var tr TopicRule
	var enabled int
	err := s.Scan(&tr.Name, &tr.ARN, &tr.SQLQuery, &tr.Actions, &enabled, &tr.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errTopicRuleNotFound
	}
	tr.Enabled = enabled != 0
	return &tr, err
}

func scanJob(s scanner) (*Job, error) {
	var j Job
	err := s.Scan(&j.ID, &j.ARN, &j.Status, &j.Targets, &j.Document, &j.Description, &j.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errJobNotFound
	}
	return &j, err
}

func scanRoleAlias(s scanner) (*RoleAlias, error) {
	var ra RoleAlias
	err := s.Scan(&ra.Name, &ra.ARN, &ra.RoleARN, &ra.CredentialDuration, &ra.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errRoleAliasNotFound
	}
	return &ra, err
}

// --- utilities ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
