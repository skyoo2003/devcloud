// SPDX-License-Identifier: Apache-2.0

// internal/services/organizations/store.go
package organizations

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errOrgNotFound     = errors.New("organization not found")
	errAccountNotFound = errors.New("account not found")
	errOUNotFound      = errors.New("organizational unit not found")
	errRootNotFound    = errors.New("root not found")
	errPolicyNotFound  = errors.New("policy not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS organization (
			id                TEXT PRIMARY KEY,
			arn               TEXT NOT NULL,
			master_account_id TEXT NOT NULL DEFAULT '000000000000',
			master_account_arn TEXT NOT NULL DEFAULT '',
			master_email      TEXT NOT NULL DEFAULT 'admin@example.com',
			feature_set       TEXT NOT NULL DEFAULT 'ALL'
		);
		CREATE TABLE IF NOT EXISTS accounts (
			id           TEXT PRIMARY KEY,
			arn          TEXT NOT NULL UNIQUE,
			name         TEXT NOT NULL,
			email        TEXT NOT NULL,
			status       TEXT NOT NULL DEFAULT 'ACTIVE',
			joined_method TEXT NOT NULL DEFAULT 'CREATED',
			parent_id    TEXT NOT NULL DEFAULT '',
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ous (
			id         TEXT PRIMARY KEY,
			arn        TEXT NOT NULL UNIQUE,
			name       TEXT NOT NULL,
			parent_id  TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS roots (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL DEFAULT 'Root',
			policy_types TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS policies (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			name        TEXT NOT NULL,
			type        TEXT NOT NULL DEFAULT 'SERVICE_CONTROL_POLICY',
			description TEXT NOT NULL DEFAULT '',
			content     TEXT NOT NULL DEFAULT '{}',
			created_at  INTEGER NOT NULL,
			updated_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS policy_attachments (
			policy_id TEXT NOT NULL,
			target_id TEXT NOT NULL,
			PRIMARY KEY (policy_id, target_id)
		);
	`},
}

// ---- model structs ----

type Organization struct {
	ID               string
	ARN              string
	MasterAccountID  string
	MasterAccountARN string
	MasterEmail      string
	FeatureSet       string
}

type Account struct {
	ID           string
	ARN          string
	Name         string
	Email        string
	Status       string
	JoinedMethod string
	ParentID     string
	CreatedAt    time.Time
}

type OU struct {
	ID        string
	ARN       string
	Name      string
	ParentID  string
	CreatedAt time.Time
}

type Root struct {
	ID          string
	ARN         string
	Name        string
	PolicyTypes string // JSON array
}

type Policy struct {
	ID          string
	ARN         string
	Name        string
	Type        string
	Description string
	Content     string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "organizations.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Organization ----

func (s *Store) CreateOrganization(id, arn, masterID, masterARN, masterEmail, featureSet string) (*Organization, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO organization (id, arn, master_account_id, master_account_arn, master_email, feature_set)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, arn, masterID, masterARN, masterEmail, featureSet,
	)
	if err != nil {
		return nil, err
	}
	return &Organization{
		ID: id, ARN: arn,
		MasterAccountID: masterID, MasterAccountARN: masterARN,
		MasterEmail: masterEmail, FeatureSet: featureSet,
	}, nil
}

func (s *Store) GetOrganization() (*Organization, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, master_account_id, master_account_arn, master_email, feature_set FROM organization LIMIT 1`)
	var o Organization
	err := row.Scan(&o.ID, &o.ARN, &o.MasterAccountID, &o.MasterAccountARN, &o.MasterEmail, &o.FeatureSet)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errOrgNotFound
		}
		return nil, err
	}
	return &o, nil
}

func (s *Store) DeleteOrganization() error {
	_, err := s.store.DB().Exec(`DELETE FROM organization`)
	return err
}

// ---- Root ----

func (s *Store) CreateRoot(id, arn, name string) (*Root, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO roots (id, arn, name) VALUES (?, ?, ?)`, id, arn, name)
	if err != nil {
		return nil, err
	}
	return &Root{ID: id, ARN: arn, Name: name, PolicyTypes: "[]"}, nil
}

func (s *Store) GetRoot() (*Root, error) {
	row := s.store.DB().QueryRow(`SELECT id, arn, name, policy_types FROM roots LIMIT 1`)
	return scanRoot(row)
}

func (s *Store) ListRoots() ([]Root, error) {
	rows, err := s.store.DB().Query(`SELECT id, arn, name, policy_types FROM roots`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Root
	for rows.Next() {
		r, err := scanRoot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *Store) UpdateRootPolicyTypes(id, policyTypesJSON string) error {
	_, err := s.store.DB().Exec(`UPDATE roots SET policy_types = ? WHERE id = ?`, policyTypesJSON, id)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanRoot(sc scanner) (*Root, error) {
	var r Root
	err := sc.Scan(&r.ID, &r.ARN, &r.Name, &r.PolicyTypes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRootNotFound
		}
		return nil, err
	}
	return &r, nil
}

// ---- Account ----

func (s *Store) CreateAccount(id, arn, name, email, joinedMethod, parentID string) (*Account, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO accounts (id, arn, name, email, joined_method, parent_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, arn, name, email, joinedMethod, parentID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Account{
		ID: id, ARN: arn, Name: name, Email: email,
		Status: "ACTIVE", JoinedMethod: joinedMethod,
		ParentID: parentID, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAccount(id string) (*Account, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, email, status, joined_method, parent_id, created_at FROM accounts WHERE id = ?`, id)
	return scanAccount(row)
}

func (s *Store) ListAccounts() ([]Account, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, email, status, joined_method, parent_id, created_at FROM accounts ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAccounts(rows)
}

func (s *Store) ListAccountsForParent(parentID string) ([]Account, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, email, status, joined_method, parent_id, created_at FROM accounts WHERE parent_id = ? ORDER BY created_at`,
		parentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAccounts(rows)
}

func (s *Store) UpdateAccountStatus(id, status string) error {
	_, err := s.store.DB().Exec(`UPDATE accounts SET status = ? WHERE id = ?`, status, id)
	return err
}

func (s *Store) UpdateAccountParent(id, parentID string) error {
	_, err := s.store.DB().Exec(`UPDATE accounts SET parent_id = ? WHERE id = ?`, parentID, id)
	return err
}

func (s *Store) DeleteAccount(id string) error {
	_, err := s.store.DB().Exec(`DELETE FROM accounts WHERE id = ?`, id)
	return err
}

func scanAccount(sc scanner) (*Account, error) {
	var a Account
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Name, &a.Email, &a.Status, &a.JoinedMethod, &a.ParentID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAccountNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanAccounts(rows *sql.Rows) ([]Account, error) {
	var out []Account
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// ---- Organizational Unit ----

func (s *Store) CreateOU(id, arn, name, parentID string) (*OU, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO ous (id, arn, name, parent_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		id, arn, name, parentID, now,
	)
	if err != nil {
		return nil, err
	}
	return &OU{ID: id, ARN: arn, Name: name, ParentID: parentID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetOU(id string) (*OU, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, parent_id, created_at FROM ous WHERE id = ?`, id)
	return scanOU(row)
}

func (s *Store) ListOUsForParent(parentID string) ([]OU, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, parent_id, created_at FROM ous WHERE parent_id = ? ORDER BY created_at`,
		parentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OU
	for rows.Next() {
		ou, err := scanOU(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ou)
	}
	return out, rows.Err()
}

func (s *Store) UpdateOU(id, name string) error {
	_, err := s.store.DB().Exec(`UPDATE ous SET name = ? WHERE id = ?`, name, id)
	return err
}

func (s *Store) DeleteOU(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM ous WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errOUNotFound
	}
	return nil
}

func (s *Store) OUHasChildren(id string) (bool, error) {
	var count int
	err := s.store.DB().QueryRow(
		`SELECT COUNT(*) FROM (
			SELECT id FROM accounts WHERE parent_id = ?
			UNION ALL
			SELECT id FROM ous WHERE parent_id = ?
		)`, id, id).Scan(&count)
	return count > 0, err
}

func scanOU(sc scanner) (*OU, error) {
	var ou OU
	var createdAt int64
	err := sc.Scan(&ou.ID, &ou.ARN, &ou.Name, &ou.ParentID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errOUNotFound
		}
		return nil, err
	}
	ou.CreatedAt = time.Unix(createdAt, 0)
	return &ou, nil
}

// ---- Policy ----

func (s *Store) CreatePolicy(id, arn, name, policyType, description, content string) (*Policy, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO policies (id, arn, name, type, description, content, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, name, policyType, description, content, now, now,
	)
	if err != nil {
		return nil, err
	}
	t := time.Unix(now, 0)
	return &Policy{
		ID: id, ARN: arn, Name: name, Type: policyType,
		Description: description, Content: content,
		CreatedAt: t, UpdatedAt: t,
	}, nil
}

func (s *Store) GetPolicy(id string) (*Policy, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, type, description, content, created_at, updated_at FROM policies WHERE id = ?`, id)
	return scanPolicy(row)
}

func (s *Store) ListPolicies(policyType string) ([]Policy, error) {
	var rows *sql.Rows
	var err error
	if policyType != "" {
		rows, err = s.store.DB().Query(
			`SELECT id, arn, name, type, description, content, created_at, updated_at FROM policies WHERE type = ? ORDER BY created_at`,
			policyType,
		)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT id, arn, name, type, description, content, created_at, updated_at FROM policies ORDER BY created_at`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *Store) UpdatePolicy(id, name, description, content string) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`UPDATE policies SET name = ?, description = ?, content = ?, updated_at = ? WHERE id = ?`,
		name, description, content, now, id,
	)
	return err
}

func (s *Store) DeletePolicy(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM policies WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPolicyNotFound
	}
	return nil
}

func scanPolicy(sc scanner) (*Policy, error) {
	var p Policy
	var createdAt, updatedAt int64
	err := sc.Scan(&p.ID, &p.ARN, &p.Name, &p.Type, &p.Description, &p.Content, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPolicyNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	p.UpdatedAt = time.Unix(updatedAt, 0)
	return &p, nil
}

// ---- Policy Attachments ----

func (s *Store) AttachPolicy(policyID, targetID string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR IGNORE INTO policy_attachments (policy_id, target_id) VALUES (?, ?)`,
		policyID, targetID,
	)
	return err
}

func (s *Store) DetachPolicy(policyID, targetID string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM policy_attachments WHERE policy_id = ? AND target_id = ?`,
		policyID, targetID,
	)
	return err
}

func (s *Store) ListPoliciesForTarget(targetID, policyType string) ([]Policy, error) {
	query := `SELECT p.id, p.arn, p.name, p.type, p.description, p.content, p.created_at, p.updated_at
		FROM policies p JOIN policy_attachments pa ON p.id = pa.policy_id
		WHERE pa.target_id = ?`
	args := []any{targetID}
	if policyType != "" {
		query += ` AND p.type = ?`
		args = append(args, policyType)
	}
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Policy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *Store) ListTargetsForPolicy(policyID string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT target_id FROM policy_attachments WHERE policy_id = ?`, policyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) PolicyAttachmentCount(policyID string) (int, error) {
	var count int
	err := s.store.DB().QueryRow(
		`SELECT COUNT(*) FROM policy_attachments WHERE policy_id = ?`, policyID).Scan(&count)
	return count, err
}
