// SPDX-License-Identifier: Apache-2.0

// internal/services/waf/store.go
package waf

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS web_acls (
			id             TEXT PRIMARY KEY,
			name           TEXT NOT NULL,
			arn            TEXT NOT NULL UNIQUE,
			default_action TEXT NOT NULL DEFAULT 'ALLOW',
			rules          TEXT NOT NULL DEFAULT '[]',
			change_token   TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS ip_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			descriptors  TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS rules (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			metric_name  TEXT NOT NULL DEFAULT '',
			predicates   TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS rule_groups (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			metric_name  TEXT NOT NULL DEFAULT '',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS rate_based_rules (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			arn          TEXT NOT NULL UNIQUE,
			metric_name  TEXT NOT NULL DEFAULT '',
			rate_limit   INTEGER NOT NULL DEFAULT 2000,
			predicates   TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS byte_match_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			change_token TEXT NOT NULL DEFAULT '',
			tuples       TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS regex_pattern_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			patterns     TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS size_constraint_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			constraints  TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS sql_injection_match_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			tuples       TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS xss_match_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			tuples       TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS geo_match_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			constraints  TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS regex_match_sets (
			id           TEXT PRIMARY KEY,
			name         TEXT NOT NULL,
			tuples       TEXT NOT NULL DEFAULT '[]',
			change_token TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS logging_configs (
			resource_arn TEXT PRIMARY KEY,
			config       TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS permission_policies (
			resource_arn TEXT PRIMARY KEY,
			policy       TEXT NOT NULL DEFAULT ''
		);
	`},
}

// --- model types ---

type WebACL struct {
	ID            string
	Name          string
	ARN           string
	DefaultAction string
	Rules         string
	ChangeToken   string
}

type IPSet struct {
	ID          string
	Name        string
	ARN         string
	Descriptors string
	ChangeToken string
}

type Rule struct {
	ID          string
	Name        string
	ARN         string
	MetricName  string
	Predicates  string
	ChangeToken string
}

type RuleGroup struct {
	ID          string
	Name        string
	ARN         string
	MetricName  string
	ChangeToken string
}

type RateBasedRule struct {
	ID          string
	Name        string
	ARN         string
	MetricName  string
	RateLimit   int64
	Predicates  string
	ChangeToken string
}

type ByteMatchSet struct {
	ID          string
	Name        string
	ChangeToken string
	Tuples      string
}

type RegexPatternSet struct {
	ID          string
	Name        string
	Patterns    string
	ChangeToken string
}

type SizeConstraintSet struct {
	ID          string
	Name        string
	Constraints string
	ChangeToken string
}

type SqlInjectionMatchSet struct {
	ID          string
	Name        string
	Tuples      string
	ChangeToken string
}

type XssMatchSet struct {
	ID          string
	Name        string
	Tuples      string
	ChangeToken string
}

type GeoMatchSet struct {
	ID          string
	Name        string
	Constraints string
	ChangeToken string
}

type RegexMatchSet struct {
	ID          string
	Name        string
	Tuples      string
	ChangeToken string
}

// --- store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "waf.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- WebACL ---

func (s *Store) CreateWebACL(w *WebACL) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO web_acls (id, name, arn, default_action, rules, change_token) VALUES (?, ?, ?, ?, ?, ?)`,
		w.ID, w.Name, w.ARN, w.DefaultAction, w.Rules, w.ChangeToken,
	)
	return err
}

func (s *Store) GetWebACL(id string) (*WebACL, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, default_action, rules, change_token FROM web_acls WHERE id = ?`, id)
	var w WebACL
	if err := row.Scan(&w.ID, &w.Name, &w.ARN, &w.DefaultAction, &w.Rules, &w.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &w, nil
}

func (s *Store) ListWebACLs() ([]WebACL, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, arn, default_action, rules, change_token FROM web_acls ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []WebACL
	for rows.Next() {
		var w WebACL
		if err := rows.Scan(&w.ID, &w.Name, &w.ARN, &w.DefaultAction, &w.Rules, &w.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, w)
	}
	return result, rows.Err()
}

func (s *Store) UpdateWebACL(w *WebACL) error {
	res, err := s.store.DB().Exec(
		`UPDATE web_acls SET default_action=?, rules=?, change_token=? WHERE id=?`,
		w.DefaultAction, w.Rules, w.ChangeToken, w.ID,
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

func (s *Store) DeleteWebACL(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM web_acls WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- IPSet ---

func (s *Store) CreateIPSet(ip *IPSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO ip_sets (id, name, arn, descriptors, change_token) VALUES (?, ?, ?, ?, ?)`,
		ip.ID, ip.Name, ip.ARN, ip.Descriptors, ip.ChangeToken,
	)
	return err
}

func (s *Store) GetIPSet(id string) (*IPSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, descriptors, change_token FROM ip_sets WHERE id = ?`, id)
	var ip IPSet
	if err := row.Scan(&ip.ID, &ip.Name, &ip.ARN, &ip.Descriptors, &ip.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &ip, nil
}

func (s *Store) ListIPSets() ([]IPSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, arn, descriptors, change_token FROM ip_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []IPSet
	for rows.Next() {
		var ip IPSet
		if err := rows.Scan(&ip.ID, &ip.Name, &ip.ARN, &ip.Descriptors, &ip.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, ip)
	}
	return result, rows.Err()
}

func (s *Store) UpdateIPSet(ip *IPSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE ip_sets SET descriptors=?, change_token=? WHERE id=?`,
		ip.Descriptors, ip.ChangeToken, ip.ID,
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

func (s *Store) DeleteIPSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM ip_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- Rule ---

func (s *Store) CreateRule(r *Rule) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO rules (id, name, arn, metric_name, predicates, change_token) VALUES (?, ?, ?, ?, ?, ?)`,
		r.ID, r.Name, r.ARN, r.MetricName, r.Predicates, r.ChangeToken,
	)
	return err
}

func (s *Store) GetRule(id string) (*Rule, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, metric_name, predicates, change_token FROM rules WHERE id = ?`, id)
	var r Rule
	if err := row.Scan(&r.ID, &r.Name, &r.ARN, &r.MetricName, &r.Predicates, &r.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRules() ([]Rule, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, arn, metric_name, predicates, change_token FROM rules ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Rule
	for rows.Next() {
		var r Rule
		if err := rows.Scan(&r.ID, &r.Name, &r.ARN, &r.MetricName, &r.Predicates, &r.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRule(r *Rule) error {
	res, err := s.store.DB().Exec(
		`UPDATE rules SET predicates=?, change_token=? WHERE id=?`,
		r.Predicates, r.ChangeToken, r.ID,
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

func (s *Store) DeleteRule(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM rules WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- RuleGroup ---

func (s *Store) CreateRuleGroup(rg *RuleGroup) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO rule_groups (id, name, arn, metric_name, change_token) VALUES (?, ?, ?, ?, ?)`,
		rg.ID, rg.Name, rg.ARN, rg.MetricName, rg.ChangeToken,
	)
	return err
}

func (s *Store) GetRuleGroup(id string) (*RuleGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, metric_name, change_token FROM rule_groups WHERE id = ?`, id)
	var rg RuleGroup
	if err := row.Scan(&rg.ID, &rg.Name, &rg.ARN, &rg.MetricName, &rg.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &rg, nil
}

func (s *Store) ListRuleGroups() ([]RuleGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, arn, metric_name, change_token FROM rule_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RuleGroup
	for rows.Next() {
		var rg RuleGroup
		if err := rows.Scan(&rg.ID, &rg.Name, &rg.ARN, &rg.MetricName, &rg.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, rg)
	}
	return result, rows.Err()
}

func (s *Store) DeleteRuleGroup(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM rule_groups WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- RateBasedRule ---

func (s *Store) CreateRateBasedRule(r *RateBasedRule) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO rate_based_rules (id, name, arn, metric_name, rate_limit, predicates, change_token) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.Name, r.ARN, r.MetricName, r.RateLimit, r.Predicates, r.ChangeToken,
	)
	return err
}

func (s *Store) GetRateBasedRule(id string) (*RateBasedRule, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, arn, metric_name, rate_limit, predicates, change_token FROM rate_based_rules WHERE id = ?`, id)
	var r RateBasedRule
	if err := row.Scan(&r.ID, &r.Name, &r.ARN, &r.MetricName, &r.RateLimit, &r.Predicates, &r.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRateBasedRules() ([]RateBasedRule, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, arn, metric_name, rate_limit, predicates, change_token FROM rate_based_rules ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RateBasedRule
	for rows.Next() {
		var r RateBasedRule
		if err := rows.Scan(&r.ID, &r.Name, &r.ARN, &r.MetricName, &r.RateLimit, &r.Predicates, &r.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRateBasedRule(r *RateBasedRule) error {
	res, err := s.store.DB().Exec(
		`UPDATE rate_based_rules SET rate_limit=?, predicates=?, change_token=? WHERE id=?`,
		r.RateLimit, r.Predicates, r.ChangeToken, r.ID,
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

func (s *Store) DeleteRateBasedRule(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM rate_based_rules WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- ByteMatchSet ---

func (s *Store) CreateByteMatchSet(b *ByteMatchSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO byte_match_sets (id, name, change_token, tuples) VALUES (?, ?, ?, ?)`,
		b.ID, b.Name, b.ChangeToken, b.Tuples,
	)
	return err
}

func (s *Store) GetByteMatchSet(id string) (*ByteMatchSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, change_token, tuples FROM byte_match_sets WHERE id = ?`, id)
	var b ByteMatchSet
	if err := row.Scan(&b.ID, &b.Name, &b.ChangeToken, &b.Tuples); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &b, nil
}

func (s *Store) ListByteMatchSets() ([]ByteMatchSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, change_token, tuples FROM byte_match_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []ByteMatchSet
	for rows.Next() {
		var b ByteMatchSet
		if err := rows.Scan(&b.ID, &b.Name, &b.ChangeToken, &b.Tuples); err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, rows.Err()
}

func (s *Store) UpdateByteMatchSet(b *ByteMatchSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE byte_match_sets SET tuples=?, change_token=? WHERE id=?`,
		b.Tuples, b.ChangeToken, b.ID,
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

func (s *Store) DeleteByteMatchSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM byte_match_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- RegexPatternSet ---

func (s *Store) CreateRegexPatternSet(r *RegexPatternSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO regex_pattern_sets (id, name, patterns, change_token) VALUES (?, ?, ?, ?)`,
		r.ID, r.Name, r.Patterns, r.ChangeToken,
	)
	return err
}

func (s *Store) GetRegexPatternSet(id string) (*RegexPatternSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, patterns, change_token FROM regex_pattern_sets WHERE id = ?`, id)
	var r RegexPatternSet
	if err := row.Scan(&r.ID, &r.Name, &r.Patterns, &r.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRegexPatternSets() ([]RegexPatternSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, patterns, change_token FROM regex_pattern_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RegexPatternSet
	for rows.Next() {
		var r RegexPatternSet
		if err := rows.Scan(&r.ID, &r.Name, &r.Patterns, &r.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRegexPatternSet(r *RegexPatternSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE regex_pattern_sets SET patterns=?, change_token=? WHERE id=?`,
		r.Patterns, r.ChangeToken, r.ID,
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

func (s *Store) DeleteRegexPatternSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM regex_pattern_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- SizeConstraintSet ---

func (s *Store) CreateSizeConstraintSet(sc *SizeConstraintSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO size_constraint_sets (id, name, constraints, change_token) VALUES (?, ?, ?, ?)`,
		sc.ID, sc.Name, sc.Constraints, sc.ChangeToken,
	)
	return err
}

func (s *Store) GetSizeConstraintSet(id string) (*SizeConstraintSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, constraints, change_token FROM size_constraint_sets WHERE id = ?`, id)
	var sc SizeConstraintSet
	if err := row.Scan(&sc.ID, &sc.Name, &sc.Constraints, &sc.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &sc, nil
}

func (s *Store) ListSizeConstraintSets() ([]SizeConstraintSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, constraints, change_token FROM size_constraint_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []SizeConstraintSet
	for rows.Next() {
		var sc SizeConstraintSet
		if err := rows.Scan(&sc.ID, &sc.Name, &sc.Constraints, &sc.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, sc)
	}
	return result, rows.Err()
}

func (s *Store) UpdateSizeConstraintSet(sc *SizeConstraintSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE size_constraint_sets SET constraints=?, change_token=? WHERE id=?`,
		sc.Constraints, sc.ChangeToken, sc.ID,
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

func (s *Store) DeleteSizeConstraintSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM size_constraint_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- SqlInjectionMatchSet ---

func (s *Store) CreateSqlInjectionMatchSet(si *SqlInjectionMatchSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO sql_injection_match_sets (id, name, tuples, change_token) VALUES (?, ?, ?, ?)`,
		si.ID, si.Name, si.Tuples, si.ChangeToken,
	)
	return err
}

func (s *Store) GetSqlInjectionMatchSet(id string) (*SqlInjectionMatchSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, tuples, change_token FROM sql_injection_match_sets WHERE id = ?`, id)
	var si SqlInjectionMatchSet
	if err := row.Scan(&si.ID, &si.Name, &si.Tuples, &si.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &si, nil
}

func (s *Store) ListSqlInjectionMatchSets() ([]SqlInjectionMatchSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, tuples, change_token FROM sql_injection_match_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []SqlInjectionMatchSet
	for rows.Next() {
		var si SqlInjectionMatchSet
		if err := rows.Scan(&si.ID, &si.Name, &si.Tuples, &si.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, si)
	}
	return result, rows.Err()
}

func (s *Store) UpdateSqlInjectionMatchSet(si *SqlInjectionMatchSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE sql_injection_match_sets SET tuples=?, change_token=? WHERE id=?`,
		si.Tuples, si.ChangeToken, si.ID,
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

func (s *Store) DeleteSqlInjectionMatchSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM sql_injection_match_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- XssMatchSet ---

func (s *Store) CreateXssMatchSet(x *XssMatchSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO xss_match_sets (id, name, tuples, change_token) VALUES (?, ?, ?, ?)`,
		x.ID, x.Name, x.Tuples, x.ChangeToken,
	)
	return err
}

func (s *Store) GetXssMatchSet(id string) (*XssMatchSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, tuples, change_token FROM xss_match_sets WHERE id = ?`, id)
	var x XssMatchSet
	if err := row.Scan(&x.ID, &x.Name, &x.Tuples, &x.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &x, nil
}

func (s *Store) ListXssMatchSets() ([]XssMatchSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, tuples, change_token FROM xss_match_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []XssMatchSet
	for rows.Next() {
		var x XssMatchSet
		if err := rows.Scan(&x.ID, &x.Name, &x.Tuples, &x.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, x)
	}
	return result, rows.Err()
}

func (s *Store) UpdateXssMatchSet(x *XssMatchSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE xss_match_sets SET tuples=?, change_token=? WHERE id=?`,
		x.Tuples, x.ChangeToken, x.ID,
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

func (s *Store) DeleteXssMatchSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM xss_match_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- GeoMatchSet ---

func (s *Store) CreateGeoMatchSet(g *GeoMatchSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO geo_match_sets (id, name, constraints, change_token) VALUES (?, ?, ?, ?)`,
		g.ID, g.Name, g.Constraints, g.ChangeToken,
	)
	return err
}

func (s *Store) GetGeoMatchSet(id string) (*GeoMatchSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, constraints, change_token FROM geo_match_sets WHERE id = ?`, id)
	var g GeoMatchSet
	if err := row.Scan(&g.ID, &g.Name, &g.Constraints, &g.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &g, nil
}

func (s *Store) ListGeoMatchSets() ([]GeoMatchSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, constraints, change_token FROM geo_match_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []GeoMatchSet
	for rows.Next() {
		var g GeoMatchSet
		if err := rows.Scan(&g.ID, &g.Name, &g.Constraints, &g.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, g)
	}
	return result, rows.Err()
}

func (s *Store) UpdateGeoMatchSet(g *GeoMatchSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE geo_match_sets SET constraints=?, change_token=? WHERE id=?`,
		g.Constraints, g.ChangeToken, g.ID,
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

func (s *Store) DeleteGeoMatchSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM geo_match_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- RegexMatchSet ---

func (s *Store) CreateRegexMatchSet(r *RegexMatchSet) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO regex_match_sets (id, name, tuples, change_token) VALUES (?, ?, ?, ?)`,
		r.ID, r.Name, r.Tuples, r.ChangeToken,
	)
	return err
}

func (s *Store) GetRegexMatchSet(id string) (*RegexMatchSet, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, name, tuples, change_token FROM regex_match_sets WHERE id = ?`, id)
	var r RegexMatchSet
	if err := row.Scan(&r.ID, &r.Name, &r.Tuples, &r.ChangeToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRegexMatchSets() ([]RegexMatchSet, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, name, tuples, change_token FROM regex_match_sets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []RegexMatchSet
	for rows.Next() {
		var r RegexMatchSet
		if err := rows.Scan(&r.ID, &r.Name, &r.Tuples, &r.ChangeToken); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) UpdateRegexMatchSet(r *RegexMatchSet) error {
	res, err := s.store.DB().Exec(
		`UPDATE regex_match_sets SET tuples=?, change_token=? WHERE id=?`,
		r.Tuples, r.ChangeToken, r.ID,
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

func (s *Store) DeleteRegexMatchSet(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM regex_match_sets WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- Logging ---

func (s *Store) PutLoggingConfig(arn, config string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO logging_configs (resource_arn, config) VALUES (?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET config=excluded.config`,
		arn, config,
	)
	return err
}

func (s *Store) GetLoggingConfig(arn string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT config FROM logging_configs WHERE resource_arn = ?`, arn)
	var config string
	if err := row.Scan(&config); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errNotFound
		}
		return "", err
	}
	return config, nil
}

func (s *Store) DeleteLoggingConfig(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM logging_configs WHERE resource_arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) ListLoggingConfigs() ([]map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT resource_arn, config FROM logging_configs ORDER BY resource_arn`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []map[string]string
	for rows.Next() {
		var arn, config string
		if err := rows.Scan(&arn, &config); err != nil {
			return nil, err
		}
		result = append(result, map[string]string{"resource_arn": arn, "config": config})
	}
	return result, rows.Err()
}

// --- Permission Policy ---

func (s *Store) PutPermissionPolicy(arn, policy string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO permission_policies (resource_arn, policy) VALUES (?, ?)
		 ON CONFLICT(resource_arn) DO UPDATE SET policy=excluded.policy`,
		arn, policy,
	)
	return err
}

func (s *Store) GetPermissionPolicy(arn string) (string, error) {
	row := s.store.DB().QueryRow(`SELECT policy FROM permission_policies WHERE resource_arn = ?`, arn)
	var policy string
	if err := row.Scan(&policy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", errNotFound
		}
		return "", err
	}
	return policy, nil
}

func (s *Store) DeletePermissionPolicy(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM permission_policies WHERE resource_arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

// --- Tags ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}
