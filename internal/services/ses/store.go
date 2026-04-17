// SPDX-License-Identifier: Apache-2.0

package ses

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errNotFound = errors.New("not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS identities (
			identity        TEXT PRIMARY KEY,
			type            TEXT NOT NULL DEFAULT 'EmailAddress',
			verification    TEXT NOT NULL DEFAULT 'Success',
			dkim_enabled    INTEGER NOT NULL DEFAULT 0,
			dkim_tokens     TEXT NOT NULL DEFAULT '[]',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS templates (
			name            TEXT PRIMARY KEY,
			subject         TEXT NOT NULL DEFAULT '',
			html_body       TEXT NOT NULL DEFAULT '',
			text_body       TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS config_sets (
			name            TEXT PRIMARY KEY,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS receipt_rule_sets (
			name            TEXT PRIMARY KEY,
			active          INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS receipt_rules (
			rule_set_name   TEXT NOT NULL,
			name            TEXT NOT NULL,
			enabled         INTEGER NOT NULL DEFAULT 1,
			scan_enabled    INTEGER NOT NULL DEFAULT 0,
			recipients      TEXT NOT NULL DEFAULT '[]',
			actions         TEXT NOT NULL DEFAULT '[]',
			position        INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (rule_set_name, name)
		);
		CREATE TABLE IF NOT EXISTS receipt_filters (
			name            TEXT PRIMARY KEY,
			policy          TEXT NOT NULL DEFAULT 'Block',
			cidr            TEXT NOT NULL DEFAULT '0.0.0.0/0'
		);
		CREATE TABLE IF NOT EXISTS sent_emails (
			message_id      TEXT PRIMARY KEY,
			source          TEXT NOT NULL DEFAULT '',
			destination     TEXT NOT NULL DEFAULT '{}',
			subject         TEXT NOT NULL DEFAULT '',
			body            TEXT NOT NULL DEFAULT '',
			sent_at         INTEGER NOT NULL
		);
	`},
}

// Identity represents an SES verified identity.
type Identity struct {
	Identity     string
	Type         string
	Verification string
	DkimEnabled  bool
	DkimTokens   []string
	CreatedAt    time.Time
}

// Template represents an SES email template.
type Template struct {
	Name      string
	Subject   string
	HTMLBody  string
	TextBody  string
	CreatedAt time.Time
}

// ConfigSet represents an SES configuration set.
type ConfigSet struct {
	Name      string
	CreatedAt time.Time
}

// ReceiptRuleSet represents a receipt rule set.
type ReceiptRuleSet struct {
	Name      string
	Active    bool
	CreatedAt time.Time
}

// ReceiptRule represents a receipt rule.
type ReceiptRule struct {
	RuleSetName string
	Name        string
	Enabled     bool
	ScanEnabled bool
	Recipients  []string
	Actions     []map[string]any
	Position    int
}

// ReceiptFilter represents a receipt filter.
type ReceiptFilter struct {
	Name   string
	Policy string
	CIDR   string
}

// SentEmail represents a stored sent email.
type SentEmail struct {
	MessageID   string
	Source      string
	Destination map[string][]string
	Subject     string
	Body        string
	SentAt      time.Time
}

// Store provides SES data persistence.
type Store struct {
	db *sql.DB
}

// NewStore opens (or creates) the SQLite database for SES.
func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "ses.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: s.DB()}, nil
}

// Close is a no-op since we don't hold a direct reference to sqlite.Store.
func (s *Store) Close() error { return nil }

// --- Identity ---

func (s *Store) UpsertIdentity(identity, identityType string) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(
		`INSERT INTO identities (identity, type, verification, dkim_enabled, dkim_tokens, created_at)
		 VALUES (?, ?, 'Success', 0, '[]', ?)
		 ON CONFLICT(identity) DO UPDATE SET type=excluded.type`,
		identity, identityType, now,
	)
	return err
}

func (s *Store) GetIdentity(identity string) (*Identity, error) {
	row := s.db.QueryRow(
		`SELECT identity, type, verification, dkim_enabled, dkim_tokens, created_at
		 FROM identities WHERE identity = ?`, identity)
	return scanIdentity(row)
}

func (s *Store) ListIdentities(identityType string) ([]Identity, error) {
	query := `SELECT identity, type, verification, dkim_enabled, dkim_tokens, created_at FROM identities`
	var args []any
	if identityType != "" {
		query += ` WHERE type = ?`
		args = append(args, identityType)
	}
	query += ` ORDER BY identity`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Identity
	for rows.Next() {
		id, err := scanIdentity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *id)
	}
	return out, rows.Err()
}

func (s *Store) DeleteIdentity(identity string) error {
	res, err := s.db.Exec(`DELETE FROM identities WHERE identity = ?`, identity)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) SetDkimEnabled(identity string, enabled bool) error {
	val := 0
	if enabled {
		val = 1
	}
	tokens := `[]`
	if enabled {
		t1 := randomToken()
		t2 := randomToken()
		t3 := randomToken()
		b, _ := json.Marshal([]string{t1, t2, t3})
		tokens = string(b)
	}
	_, err := s.db.Exec(
		`UPDATE identities SET dkim_enabled=?, dkim_tokens=? WHERE identity=?`,
		val, tokens, identity,
	)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanIdentity(row scanner) (*Identity, error) {
	var id Identity
	var tokensJSON string
	var createdAt int64
	var dkimEnabled int
	err := row.Scan(&id.Identity, &id.Type, &id.Verification, &dkimEnabled, &tokensJSON, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	id.DkimEnabled = dkimEnabled == 1
	_ = json.Unmarshal([]byte(tokensJSON), &id.DkimTokens)
	id.CreatedAt = time.Unix(createdAt, 0)
	return &id, nil
}

// --- Template ---

func (s *Store) CreateTemplate(name, subject, htmlBody, textBody string) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(
		`INSERT INTO templates (name, subject, html_body, text_body, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, subject, htmlBody, textBody, now,
	)
	return err
}

func (s *Store) GetTemplate(name string) (*Template, error) {
	row := s.db.QueryRow(
		`SELECT name, subject, html_body, text_body, created_at FROM templates WHERE name=?`, name)
	var t Template
	var createdAt int64
	err := row.Scan(&t.Name, &t.Subject, &t.HTMLBody, &t.TextBody, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	t.CreatedAt = time.Unix(createdAt, 0)
	return &t, nil
}

func (s *Store) ListTemplates() ([]Template, error) {
	rows, err := s.db.Query(
		`SELECT name, subject, html_body, text_body, created_at FROM templates ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Template
	for rows.Next() {
		var t Template
		var createdAt int64
		if err := rows.Scan(&t.Name, &t.Subject, &t.HTMLBody, &t.TextBody, &createdAt); err != nil {
			return nil, err
		}
		t.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateTemplate(name, subject, htmlBody, textBody string) error {
	res, err := s.db.Exec(
		`UPDATE templates SET subject=?, html_body=?, text_body=? WHERE name=?`,
		subject, htmlBody, textBody, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteTemplate(name string) error {
	res, err := s.db.Exec(`DELETE FROM templates WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- ConfigSet ---

func (s *Store) CreateConfigSet(name string) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(
		`INSERT INTO config_sets (name, created_at) VALUES (?, ?)`, name, now)
	return err
}

func (s *Store) ListConfigSets() ([]ConfigSet, error) {
	rows, err := s.db.Query(`SELECT name, created_at FROM config_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigSet
	for rows.Next() {
		var cs ConfigSet
		var createdAt int64
		if err := rows.Scan(&cs.Name, &createdAt); err != nil {
			return nil, err
		}
		cs.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, cs)
	}
	return out, rows.Err()
}

func (s *Store) GetConfigSet(name string) (*ConfigSet, error) {
	row := s.db.QueryRow(`SELECT name, created_at FROM config_sets WHERE name=?`, name)
	var cs ConfigSet
	var createdAt int64
	err := row.Scan(&cs.Name, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	cs.CreatedAt = time.Unix(createdAt, 0)
	return &cs, nil
}

func (s *Store) DeleteConfigSet(name string) error {
	res, err := s.db.Exec(`DELETE FROM config_sets WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- ReceiptRuleSet ---

func (s *Store) CreateReceiptRuleSet(name string) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(
		`INSERT INTO receipt_rule_sets (name, active, created_at) VALUES (?, 0, ?)`, name, now)
	return err
}

func (s *Store) GetReceiptRuleSet(name string) (*ReceiptRuleSet, error) {
	row := s.db.QueryRow(`SELECT name, active, created_at FROM receipt_rule_sets WHERE name=?`, name)
	return scanRuleSet(row)
}

func (s *Store) ListReceiptRuleSets() ([]ReceiptRuleSet, error) {
	rows, err := s.db.Query(`SELECT name, active, created_at FROM receipt_rule_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiptRuleSet
	for rows.Next() {
		rs, err := scanRuleSet(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rs)
	}
	return out, rows.Err()
}

func (s *Store) DeleteReceiptRuleSet(name string) error {
	if _, err := s.db.Exec(`DELETE FROM receipt_rules WHERE rule_set_name=?`, name); err != nil {
		return err
	}
	res, err := s.db.Exec(`DELETE FROM receipt_rule_sets WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) SetActiveReceiptRuleSet(name string) error {
	if _, err := s.db.Exec(`UPDATE receipt_rule_sets SET active=0`); err != nil {
		return err
	}
	if name == "" {
		return nil
	}
	res, err := s.db.Exec(`UPDATE receipt_rule_sets SET active=1 WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) GetActiveReceiptRuleSet() (*ReceiptRuleSet, error) {
	row := s.db.QueryRow(`SELECT name, active, created_at FROM receipt_rule_sets WHERE active=1 LIMIT 1`)
	return scanRuleSet(row)
}

func (s *Store) CloneReceiptRuleSet(originalName, newName string) error {
	now := time.Now().Unix()
	if _, err := s.db.Exec(
		`INSERT INTO receipt_rule_sets (name, active, created_at) VALUES (?, 0, ?)`, newName, now); err != nil {
		return err
	}
	rows, err := s.db.Query(
		`SELECT name, enabled, scan_enabled, recipients, actions, position FROM receipt_rules
		 WHERE rule_set_name=? ORDER BY position`, originalName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var rr ReceiptRule
		var recJSON, actJSON string
		if err := rows.Scan(&rr.Name, &rr.Enabled, &rr.ScanEnabled, &recJSON, &actJSON, &rr.Position); err != nil {
			return err
		}
		if _, err := s.db.Exec(
			`INSERT INTO receipt_rules (rule_set_name, name, enabled, scan_enabled, recipients, actions, position)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			newName, rr.Name, rr.Enabled, rr.ScanEnabled, recJSON, actJSON, rr.Position,
		); err != nil {
			return err
		}
	}
	return rows.Err()
}

func scanRuleSet(row scanner) (*ReceiptRuleSet, error) {
	var rs ReceiptRuleSet
	var active int
	var createdAt int64
	err := row.Scan(&rs.Name, &active, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	rs.Active = active == 1
	rs.CreatedAt = time.Unix(createdAt, 0)
	return &rs, nil
}

// --- ReceiptRule ---

func (s *Store) CreateReceiptRule(ruleSetName, ruleName string, enabled, scanEnabled bool, recipients []string, actions []map[string]any, after string) error {
	recJSON, _ := json.Marshal(recipients)
	actJSON, _ := json.Marshal(actions)

	// determine position
	var maxPos int
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(position),0) FROM receipt_rules WHERE rule_set_name=?`, ruleSetName).Scan(&maxPos)
	position := maxPos + 1

	if after != "" {
		var afterPos int
		_ = s.db.QueryRow(`SELECT position FROM receipt_rules WHERE rule_set_name=? AND name=?`, ruleSetName, after).Scan(&afterPos)
		position = afterPos + 1
		// shift others
		if _, err := s.db.Exec(
			`UPDATE receipt_rules SET position=position+1 WHERE rule_set_name=? AND position>=?`,
			ruleSetName, position,
		); err != nil {
			return err
		}
	}

	_, err := s.db.Exec(
		`INSERT INTO receipt_rules (rule_set_name, name, enabled, scan_enabled, recipients, actions, position)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ruleSetName, ruleName,
		boolToInt(enabled), boolToInt(scanEnabled),
		string(recJSON), string(actJSON), position,
	)
	return err
}

func (s *Store) GetReceiptRule(ruleSetName, ruleName string) (*ReceiptRule, error) {
	row := s.db.QueryRow(
		`SELECT rule_set_name, name, enabled, scan_enabled, recipients, actions, position
		 FROM receipt_rules WHERE rule_set_name=? AND name=?`, ruleSetName, ruleName)
	return scanRule(row)
}

func (s *Store) ListReceiptRules(ruleSetName string) ([]ReceiptRule, error) {
	rows, err := s.db.Query(
		`SELECT rule_set_name, name, enabled, scan_enabled, recipients, actions, position
		 FROM receipt_rules WHERE rule_set_name=? ORDER BY position`, ruleSetName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiptRule
	for rows.Next() {
		rr, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rr)
	}
	return out, rows.Err()
}

func (s *Store) UpdateReceiptRule(ruleSetName, ruleName string, enabled, scanEnabled bool, recipients []string, actions []map[string]any) error {
	recJSON, _ := json.Marshal(recipients)
	actJSON, _ := json.Marshal(actions)
	res, err := s.db.Exec(
		`UPDATE receipt_rules SET enabled=?, scan_enabled=?, recipients=?, actions=?
		 WHERE rule_set_name=? AND name=?`,
		boolToInt(enabled), boolToInt(scanEnabled), string(recJSON), string(actJSON),
		ruleSetName, ruleName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteReceiptRule(ruleSetName, ruleName string) error {
	res, err := s.db.Exec(
		`DELETE FROM receipt_rules WHERE rule_set_name=? AND name=?`, ruleSetName, ruleName)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) SetReceiptRulePosition(ruleSetName, ruleName string, position int) error {
	res, err := s.db.Exec(
		`UPDATE receipt_rules SET position=? WHERE rule_set_name=? AND name=?`,
		position, ruleSetName, ruleName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ReorderReceiptRuleSet(ruleSetName string, ruleNames []string) error {
	for i, name := range ruleNames {
		if _, err := s.db.Exec(
			`UPDATE receipt_rules SET position=? WHERE rule_set_name=? AND name=?`,
			i+1, ruleSetName, name,
		); err != nil {
			return err
		}
	}
	return nil
}

func scanRule(row scanner) (*ReceiptRule, error) {
	var rr ReceiptRule
	var enabled, scanEnabled int
	var recJSON, actJSON string
	err := row.Scan(&rr.RuleSetName, &rr.Name, &enabled, &scanEnabled, &recJSON, &actJSON, &rr.Position)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, err
	}
	rr.Enabled = enabled == 1
	rr.ScanEnabled = scanEnabled == 1
	_ = json.Unmarshal([]byte(recJSON), &rr.Recipients)
	_ = json.Unmarshal([]byte(actJSON), &rr.Actions)
	return &rr, nil
}

// --- ReceiptFilter ---

func (s *Store) CreateReceiptFilter(name, policy, cidr string) error {
	_, err := s.db.Exec(
		`INSERT INTO receipt_filters (name, policy, cidr) VALUES (?, ?, ?)`, name, policy, cidr)
	return err
}

func (s *Store) ListReceiptFilters() ([]ReceiptFilter, error) {
	rows, err := s.db.Query(`SELECT name, policy, cidr FROM receipt_filters ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiptFilter
	for rows.Next() {
		var f ReceiptFilter
		if err := rows.Scan(&f.Name, &f.Policy, &f.CIDR); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Store) DeleteReceiptFilter(name string) error {
	res, err := s.db.Exec(`DELETE FROM receipt_filters WHERE name=?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- SentEmail ---

func (s *Store) StoreSentEmail(messageID, source string, destination map[string][]string, subject, body string) error {
	destJSON, _ := json.Marshal(destination)
	now := time.Now().Unix()
	_, err := s.db.Exec(
		`INSERT INTO sent_emails (message_id, source, destination, subject, body, sent_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		messageID, source, string(destJSON), subject, body, now,
	)
	return err
}

// --- Helpers ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func randomToken() string {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte('a' + (timeNow()>>uint(i*4))&0xf)
	}
	return string(b)
}

var timeNow = func() int64 { return time.Now().UnixNano() }
