// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

// Sentinel errors.
var (
	ErrUserAlreadyExists            = errors.New("user already exists")
	ErrUserNotFound                 = errors.New("user not found")
	ErrRoleAlreadyExists            = errors.New("role already exists")
	ErrRoleNotFound                 = errors.New("role not found")
	ErrPolicyAlreadyExists          = errors.New("policy already exists")
	ErrPolicyNotFound               = errors.New("policy not found")
	ErrGroupAlreadyExists           = errors.New("group already exists")
	ErrGroupNotFound                = errors.New("group not found")
	ErrInstanceProfileAlreadyExists = errors.New("instance profile already exists")
	ErrInstanceProfileNotFound      = errors.New("instance profile not found")
)

// User represents an IAM user.
type User struct {
	UserName  string
	UserID    string
	Arn       string
	AccountID string
	CreatedAt time.Time
}

// Role represents an IAM role.
type Role struct {
	RoleName                 string
	RoleID                   string
	Arn                      string
	AccountID                string
	AssumeRolePolicyDocument string
	CreatedAt                time.Time
}

// Policy represents an IAM managed policy.
type Policy struct {
	PolicyName     string
	PolicyID       string
	Arn            string
	AccountID      string
	PolicyDocument string
	CreatedAt      time.Time
}

// AccessKey represents an IAM access key.
type AccessKey struct {
	AccessKeyID     string
	SecretAccessKey string
	UserName        string
	AccountID       string
	Status          string
	CreatedAt       time.Time
}

// Group represents an IAM group.
type Group struct {
	GroupName string
	GroupID   string
	Arn       string
	AccountID string
	CreatedAt time.Time
}

// InstanceProfile represents an IAM instance profile.
type InstanceProfile struct {
	ProfileName string
	ProfileID   string
	Arn         string
	AccountID   string
	CreatedAt   time.Time
}

// iamMigrations defines the versioned schema migrations for the IAM database.
var iamMigrations = []sqlite.Migration{
	{
		Version: 1,
		SQL: `CREATE TABLE IF NOT EXISTS users (
			user_name  TEXT NOT NULL,
			user_id    TEXT NOT NULL,
			arn        TEXT NOT NULL,
			account_id TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			PRIMARY KEY (user_name, account_id)
		);`,
	},
	{
		Version: 2,
		SQL: `CREATE TABLE IF NOT EXISTS roles (
			role_name        TEXT NOT NULL,
			role_id          TEXT NOT NULL,
			arn              TEXT NOT NULL,
			account_id       TEXT NOT NULL,
			assume_role_policy TEXT NOT NULL,
			created_at       DATETIME NOT NULL,
			PRIMARY KEY (role_name, account_id)
		);`,
	},
	{
		Version: 3,
		SQL: `CREATE TABLE IF NOT EXISTS role_policies (
			role_name  TEXT NOT NULL,
			policy_arn TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (role_name, policy_arn, account_id)
		);`,
	},
	{
		Version: 4,
		SQL: `CREATE TABLE IF NOT EXISTS access_keys (
			access_key_id TEXT PRIMARY KEY,
			secret_key    TEXT NOT NULL,
			user_name     TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'Active',
			created_at    DATETIME NOT NULL
		);`,
	},
	{
		Version: 5,
		SQL: `CREATE TABLE IF NOT EXISTS policies (
			policy_name     TEXT NOT NULL,
			policy_id       TEXT NOT NULL,
			arn             TEXT NOT NULL,
			account_id      TEXT NOT NULL,
			policy_document TEXT NOT NULL,
			created_at      DATETIME NOT NULL,
			PRIMARY KEY (policy_name, account_id)
		);`,
	},
	{
		Version: 6,
		SQL: `CREATE TABLE IF NOT EXISTS policy_versions (
			policy_arn      TEXT NOT NULL,
			version_id      TEXT NOT NULL,
			document        TEXT NOT NULL,
			is_default      INTEGER NOT NULL DEFAULT 0,
			created_at      DATETIME NOT NULL,
			PRIMARY KEY (policy_arn, version_id)
		);`,
	},
	{
		Version: 7,
		SQL: `CREATE TABLE IF NOT EXISTS user_policies (
			user_name  TEXT NOT NULL,
			policy_arn TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (user_name, policy_arn, account_id)
		);`,
	},
	{
		Version: 8,
		SQL: `CREATE TABLE IF NOT EXISTS user_inline_policies (
			user_name   TEXT NOT NULL,
			policy_name TEXT NOT NULL,
			policy_doc  TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			PRIMARY KEY (user_name, policy_name, account_id)
		);`,
	},
	{
		Version: 9,
		SQL: `CREATE TABLE IF NOT EXISTS role_inline_policies (
			role_name   TEXT NOT NULL,
			policy_name TEXT NOT NULL,
			policy_doc  TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			PRIMARY KEY (role_name, policy_name, account_id)
		);`,
	},
	{
		Version: 10,
		SQL: `CREATE TABLE IF NOT EXISTS groups (
			group_name TEXT NOT NULL,
			group_id   TEXT NOT NULL,
			arn        TEXT NOT NULL,
			account_id TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			PRIMARY KEY (group_name, account_id)
		);`,
	},
	{
		Version: 11,
		SQL: `CREATE TABLE IF NOT EXISTS group_memberships (
			group_name TEXT NOT NULL,
			user_name  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (group_name, user_name, account_id)
		);`,
	},
	{
		Version: 12,
		SQL: `CREATE TABLE IF NOT EXISTS instance_profiles (
			profile_name TEXT NOT NULL,
			profile_id   TEXT NOT NULL,
			arn          TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			created_at   DATETIME NOT NULL,
			PRIMARY KEY (profile_name, account_id)
		);`,
	},
	{
		Version: 13,
		SQL: `CREATE TABLE IF NOT EXISTS instance_profile_roles (
			profile_name TEXT NOT NULL,
			role_name    TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			PRIMARY KEY (profile_name, role_name, account_id)
		);`,
	},
	{
		Version: 14,
		SQL: `CREATE TABLE IF NOT EXISTS user_tags (
			user_name  TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (user_name, tag_key, account_id)
		);`,
	},
	{
		Version: 15,
		SQL: `CREATE TABLE IF NOT EXISTS role_tags (
			role_name  TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (role_name, tag_key, account_id)
		);`,
	},
	{
		Version: 16,
		SQL: `CREATE TABLE IF NOT EXISTS group_policies (
			group_name TEXT NOT NULL,
			policy_arn TEXT NOT NULL,
			account_id TEXT NOT NULL,
			PRIMARY KEY (group_name, policy_arn, account_id)
		);
		CREATE TABLE IF NOT EXISTS group_inline_policies (
			group_name  TEXT NOT NULL,
			policy_name TEXT NOT NULL,
			policy_doc  TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			PRIMARY KEY (group_name, policy_name, account_id)
		);`,
	},
	{
		Version: 17,
		SQL: `CREATE TABLE IF NOT EXISTS mfa_devices (
			serial_number TEXT PRIMARY KEY,
			user_name     TEXT NOT NULL,
			account_id    TEXT NOT NULL,
			status        TEXT NOT NULL DEFAULT 'Active',
			enable_date   DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS oidc_providers (
			arn         TEXT PRIMARY KEY,
			url         TEXT NOT NULL,
			client_ids  TEXT NOT NULL DEFAULT '',
			thumbprints TEXT NOT NULL DEFAULT '',
			account_id  TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS signing_certificates (
			certificate_id TEXT PRIMARY KEY,
			user_name      TEXT NOT NULL,
			certificate_body TEXT NOT NULL,
			status         TEXT NOT NULL DEFAULT 'Active',
			account_id     TEXT NOT NULL,
			upload_date    DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS ssh_public_keys (
			key_id      TEXT PRIMARY KEY,
			user_name   TEXT NOT NULL,
			key_body    TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'Active',
			account_id  TEXT NOT NULL,
			upload_date DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS server_certificates (
			certificate_name TEXT NOT NULL,
			certificate_id   TEXT NOT NULL,
			arn              TEXT NOT NULL,
			path             TEXT NOT NULL DEFAULT '/',
			certificate_body TEXT NOT NULL,
			account_id       TEXT NOT NULL,
			upload_date      DATETIME NOT NULL,
			PRIMARY KEY (certificate_name, account_id)
		);
		CREATE TABLE IF NOT EXISTS delegation_requests (
			request_id     TEXT PRIMARY KEY,
			target_account TEXT NOT NULL,
			status         TEXT NOT NULL DEFAULT 'Pending',
			token          TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			created_at     DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS org_settings (
			account_id                    TEXT PRIMARY KEY,
			root_credentials_enabled      INTEGER NOT NULL DEFAULT 0,
			root_sessions_enabled         INTEGER NOT NULL DEFAULT 0,
			outbound_web_identity_enabled INTEGER NOT NULL DEFAULT 1
		);
		CREATE TABLE IF NOT EXISTS sts_preferences (
			account_id    TEXT PRIMARY KEY,
			token_version TEXT NOT NULL DEFAULT 'v1Token'
		);`,
	},
}

// IAMStore is a SQLite-backed store for IAM entities.
type IAMStore struct {
	store *sqlite.Store
}

// NewIAMStore opens (or creates) a SQLite database at dbPath and initializes
// the IAM schema tables.
func NewIAMStore(dbPath string) (*IAMStore, error) {
	store, err := sqlite.Open(dbPath, iamMigrations)
	if err != nil {
		return nil, err
	}
	return &IAMStore{store: store}, nil
}

// NewIAMStoreFromDB wraps an existing sqlite.Store (for sharing with STS).
func NewIAMStoreFromDB(store *sqlite.Store) *IAMStore {
	return &IAMStore{store: store}
}

// SQLiteStore returns the underlying sqlite.Store (for STS sharing).
func (s *IAMStore) SQLiteStore() *sqlite.Store {
	return s.store
}

// Close closes the underlying database connection.
func (s *IAMStore) Close() error {
	return s.store.Close()
}

// db returns the underlying *sql.DB for query methods.
func (s *IAMStore) db() *sql.DB {
	return s.store.DB()
}

// CreateUser creates a new IAM user for the given account.
func (s *IAMStore) CreateUser(accountID, userName string) (*User, error) {
	userID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("generate user id: %w", err)
	}
	arn := fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, userName)
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO users (user_name, user_id, arn, account_id, created_at) VALUES (?, ?, ?, ?, ?);`,
		userName, userID, arn, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrUserAlreadyExists
		}
		return nil, err
	}

	return &User{
		UserName:  userName,
		UserID:    userID,
		Arn:       arn,
		AccountID: accountID,
		CreatedAt: now,
	}, nil
}

// ListUsers returns all IAM users for the given account.
func (s *IAMStore) ListUsers(accountID string) ([]User, error) {
	rows, err := s.db().Query(
		`SELECT user_name, user_id, arn, account_id, created_at FROM users WHERE account_id = ? ORDER BY user_name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.UserName, &u.UserID, &u.Arn, &u.AccountID, &u.CreatedAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// DeleteUser deletes an IAM user. Returns ErrUserNotFound if the user does not exist.
func (s *IAMStore) DeleteUser(accountID, userName string) error {
	result, err := s.db().Exec(
		`DELETE FROM users WHERE user_name = ? AND account_id = ?;`,
		userName, accountID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrUserNotFound
	}
	return nil
}

// CreatePolicy creates a new IAM managed policy.
func (s *IAMStore) CreatePolicy(accountID, policyName, policyDocument string) (*Policy, error) {
	policyID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("generate policy id: %w", err)
	}
	arn := fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountID, policyName)
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO policies (policy_name, policy_id, arn, account_id, policy_document, created_at) VALUES (?, ?, ?, ?, ?, ?);`,
		policyName, policyID, arn, accountID, policyDocument, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrPolicyAlreadyExists
		}
		return nil, err
	}

	return &Policy{
		PolicyName:     policyName,
		PolicyID:       policyID,
		Arn:            arn,
		AccountID:      accountID,
		PolicyDocument: policyDocument,
		CreatedAt:      now,
	}, nil
}

// ListAttachedRolePolicies returns the policy ARNs attached to a role.
func (s *IAMStore) ListAttachedRolePolicies(accountID, roleName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_arn FROM role_policies WHERE role_name = ? AND account_id = ? ORDER BY policy_arn;`,
		roleName, accountID,
	)
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

// CreateRole creates a new IAM role for the given account.
func (s *IAMStore) CreateRole(accountID, roleName, assumeRolePolicy string) (*Role, error) {
	roleID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("generate role id: %w", err)
	}
	arn := fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, roleName)
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO roles (role_name, role_id, arn, account_id, assume_role_policy, created_at) VALUES (?, ?, ?, ?, ?, ?);`,
		roleName, roleID, arn, accountID, assumeRolePolicy, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrRoleAlreadyExists
		}
		return nil, err
	}

	return &Role{
		RoleName:                 roleName,
		RoleID:                   roleID,
		Arn:                      arn,
		AccountID:                accountID,
		AssumeRolePolicyDocument: assumeRolePolicy,
		CreatedAt:                now,
	}, nil
}

// ListRoles returns all IAM roles for the given account.
func (s *IAMStore) ListRoles(accountID string) ([]Role, error) {
	rows, err := s.db().Query(
		`SELECT role_name, role_id, arn, account_id, assume_role_policy, created_at FROM roles WHERE account_id = ? ORDER BY role_name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var roles []Role
	for rows.Next() {
		var r Role
		if err := rows.Scan(&r.RoleName, &r.RoleID, &r.Arn, &r.AccountID, &r.AssumeRolePolicyDocument, &r.CreatedAt); err != nil {
			return nil, err
		}
		roles = append(roles, r)
	}
	return roles, rows.Err()
}

// GetUser returns an IAM user by name. Returns ErrUserNotFound if not found.
func (s *IAMStore) GetUser(accountID, userName string) (*User, error) {
	var u User
	err := s.db().QueryRow(
		`SELECT user_name, user_id, arn, account_id, created_at FROM users WHERE user_name = ? AND account_id = ?;`,
		userName, accountID,
	).Scan(&u.UserName, &u.UserID, &u.Arn, &u.AccountID, &u.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &u, nil
}

// UpdateUser renames an IAM user. Returns ErrUserNotFound if not found.
func (s *IAMStore) UpdateUser(accountID, oldName, newName string) error {
	newArn := fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, newName)
	result, err := s.db().Exec(
		`UPDATE users SET user_name = ?, arn = ? WHERE user_name = ? AND account_id = ?;`,
		newName, newArn, oldName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrUserNotFound
	}
	return nil
}

// GetRole returns an IAM role by name. Returns ErrRoleNotFound if not found.
func (s *IAMStore) GetRole(accountID, roleName string) (*Role, error) {
	var r Role
	err := s.db().QueryRow(
		`SELECT role_name, role_id, arn, account_id, assume_role_policy, created_at FROM roles WHERE role_name = ? AND account_id = ?;`,
		roleName, accountID,
	).Scan(&r.RoleName, &r.RoleID, &r.Arn, &r.AccountID, &r.AssumeRolePolicyDocument, &r.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrRoleNotFound
		}
		return nil, err
	}
	return &r, nil
}

// DeleteRole deletes an IAM role. Returns ErrRoleNotFound if not found.
func (s *IAMStore) DeleteRole(accountID, roleName string) error {
	result, err := s.db().Exec(
		`DELETE FROM roles WHERE role_name = ? AND account_id = ?;`,
		roleName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrRoleNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM role_policies WHERE role_name = ? AND account_id = ?;`, roleName, accountID)
	return nil
}

// UpdateAssumeRolePolicy updates the assume role policy document for a role.
func (s *IAMStore) UpdateAssumeRolePolicy(accountID, roleName, policyDocument string) error {
	result, err := s.db().Exec(
		`UPDATE roles SET assume_role_policy = ? WHERE role_name = ? AND account_id = ?;`,
		policyDocument, roleName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrRoleNotFound
	}
	return nil
}

// AttachRolePolicy attaches a managed policy ARN to the given role.
func (s *IAMStore) AttachRolePolicy(accountID, roleName, policyArn string) error {
	_, err := s.db().Exec(
		`INSERT OR IGNORE INTO role_policies (role_name, policy_arn, account_id) VALUES (?, ?, ?);`,
		roleName, policyArn, accountID,
	)
	return err
}

// CreateAccessKey generates a new AKIA* access key for the given user.
func (s *IAMStore) CreateAccessKey(accountID, userName string) (*AccessKey, error) {
	keyID, err := generateAccessKeyID()
	if err != nil {
		return nil, fmt.Errorf("generate access key id: %w", err)
	}
	secret, err := generateSecret()
	if err != nil {
		return nil, fmt.Errorf("generate secret: %w", err)
	}
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO access_keys (access_key_id, secret_key, user_name, account_id, status, created_at) VALUES (?, ?, ?, ?, 'Active', ?);`,
		keyID, secret, userName, accountID, now,
	)
	if err != nil {
		return nil, err
	}

	return &AccessKey{
		AccessKeyID:     keyID,
		SecretAccessKey: secret,
		UserName:        userName,
		AccountID:       accountID,
		Status:          "Active",
		CreatedAt:       now,
	}, nil
}

// GetCallerIdentity returns a default identity for the given account.
func (s *IAMStore) GetCallerIdentity(accountID string) (string, string, string) {
	userID := "AIDA000000000000ROOT"
	arn := fmt.Sprintf("arn:aws:iam::%s:root", accountID)
	return accountID, arn, userID
}

// PolicyVersion represents a version of an IAM managed policy.
type PolicyVersion struct {
	VersionID string
	Document  string
	IsDefault bool
	CreatedAt time.Time
}

// GetPolicyByArn returns an IAM policy by ARN. Returns ErrPolicyNotFound if not found.
func (s *IAMStore) GetPolicyByArn(policyArn string) (*Policy, error) {
	var p Policy
	err := s.db().QueryRow(
		`SELECT policy_name, policy_id, arn, account_id, policy_document, created_at FROM policies WHERE arn = ?;`,
		policyArn,
	).Scan(&p.PolicyName, &p.PolicyID, &p.Arn, &p.AccountID, &p.PolicyDocument, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrPolicyNotFound
		}
		return nil, err
	}
	return &p, nil
}

// DeletePolicy deletes a managed policy and all its attachments.
func (s *IAMStore) DeletePolicy(accountID, policyArn string) error {
	result, err := s.db().Exec(`DELETE FROM policies WHERE arn = ? AND account_id = ?;`, policyArn, accountID)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM role_policies WHERE policy_arn = ? AND account_id = ?;`, policyArn, accountID)
	_, _ = s.db().Exec(`DELETE FROM user_policies WHERE policy_arn = ? AND account_id = ?;`, policyArn, accountID)
	_, _ = s.db().Exec(`DELETE FROM policy_versions WHERE policy_arn = ?;`, policyArn)
	return nil
}

// DetachRolePolicy removes a managed policy from a role.
func (s *IAMStore) DetachRolePolicy(accountID, roleName, policyArn string) error {
	result, err := s.db().Exec(
		`DELETE FROM role_policies WHERE role_name = ? AND policy_arn = ? AND account_id = ?;`,
		roleName, policyArn, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

// AttachUserPolicy attaches a managed policy ARN to the given user.
func (s *IAMStore) AttachUserPolicy(accountID, userName, policyArn string) error {
	_, err := s.db().Exec(
		`INSERT OR IGNORE INTO user_policies (user_name, policy_arn, account_id) VALUES (?, ?, ?);`,
		userName, policyArn, accountID,
	)
	return err
}

// DetachUserPolicy removes a managed policy from a user.
func (s *IAMStore) DetachUserPolicy(accountID, userName, policyArn string) error {
	result, err := s.db().Exec(
		`DELETE FROM user_policies WHERE user_name = ? AND policy_arn = ? AND account_id = ?;`,
		userName, policyArn, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

// ListAttachedUserPolicies returns the policy ARNs attached to a user.
func (s *IAMStore) ListAttachedUserPolicies(accountID, userName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_arn FROM user_policies WHERE user_name = ? AND account_id = ? ORDER BY policy_arn;`,
		userName, accountID,
	)
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

// CreatePolicyVersion creates a new version of a managed policy.
func (s *IAMStore) CreatePolicyVersion(policyArn, document string, setAsDefault bool) (*PolicyVersion, error) {
	var count int
	_ = s.db().QueryRow(`SELECT COUNT(*) FROM policy_versions WHERE policy_arn = ?;`, policyArn).Scan(&count)
	versionID := fmt.Sprintf("v%d", count+1)
	now := time.Now().UTC()
	if setAsDefault {
		_, _ = s.db().Exec(`UPDATE policy_versions SET is_default = 0 WHERE policy_arn = ?;`, policyArn)
	}
	isDefault := 0
	if setAsDefault {
		isDefault = 1
	}
	_, err := s.db().Exec(
		`INSERT INTO policy_versions (policy_arn, version_id, document, is_default, created_at) VALUES (?, ?, ?, ?, ?);`,
		policyArn, versionID, document, isDefault, now,
	)
	if err != nil {
		return nil, err
	}
	return &PolicyVersion{VersionID: versionID, Document: document, IsDefault: setAsDefault, CreatedAt: now}, nil
}

// GetPolicyVersion returns a specific version of a managed policy.
func (s *IAMStore) GetPolicyVersion(policyArn, versionID string) (*PolicyVersion, error) {
	var pv PolicyVersion
	var isDefault int
	err := s.db().QueryRow(
		`SELECT version_id, document, is_default, created_at FROM policy_versions WHERE policy_arn = ? AND version_id = ?;`,
		policyArn, versionID,
	).Scan(&pv.VersionID, &pv.Document, &isDefault, &pv.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrPolicyNotFound
		}
		return nil, err
	}
	pv.IsDefault = isDefault == 1
	return &pv, nil
}

// ListPolicyVersions returns all versions of a managed policy ordered by creation time.
func (s *IAMStore) ListPolicyVersions(policyArn string) ([]PolicyVersion, error) {
	rows, err := s.db().Query(
		`SELECT version_id, document, is_default, created_at FROM policy_versions WHERE policy_arn = ? ORDER BY created_at;`,
		policyArn,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var versions []PolicyVersion
	for rows.Next() {
		var pv PolicyVersion
		var isDefault int
		if err := rows.Scan(&pv.VersionID, &pv.Document, &isDefault, &pv.CreatedAt); err != nil {
			return nil, err
		}
		pv.IsDefault = isDefault == 1
		versions = append(versions, pv)
	}
	return versions, rows.Err()
}

// --- Group store methods ---

// CreateGroup creates a new IAM group.
func (s *IAMStore) CreateGroup(accountID, groupName string) (*Group, error) {
	groupID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("generate group id: %w", err)
	}
	arn := fmt.Sprintf("arn:aws:iam::%s:group/%s", accountID, groupName)
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO groups (group_name, group_id, arn, account_id, created_at) VALUES (?, ?, ?, ?, ?);`,
		groupName, groupID, arn, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrGroupAlreadyExists
		}
		return nil, err
	}

	return &Group{
		GroupName: groupName,
		GroupID:   groupID,
		Arn:       arn,
		AccountID: accountID,
		CreatedAt: now,
	}, nil
}

// GetGroup returns a group and its member usernames.
func (s *IAMStore) GetGroup(accountID, groupName string) (*Group, []string, error) {
	var g Group
	err := s.db().QueryRow(
		`SELECT group_name, group_id, arn, account_id, created_at FROM groups WHERE group_name = ? AND account_id = ?;`,
		groupName, accountID,
	).Scan(&g.GroupName, &g.GroupID, &g.Arn, &g.AccountID, &g.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, ErrGroupNotFound
		}
		return nil, nil, err
	}

	rows, err := s.db().Query(
		`SELECT user_name FROM group_memberships WHERE group_name = ? AND account_id = ? ORDER BY user_name;`,
		groupName, accountID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	var members []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, nil, err
		}
		members = append(members, u)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return &g, members, nil
}

// DeleteGroup deletes a group and its memberships.
func (s *IAMStore) DeleteGroup(accountID, groupName string) error {
	result, err := s.db().Exec(
		`DELETE FROM groups WHERE group_name = ? AND account_id = ?;`,
		groupName, accountID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrGroupNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM group_memberships WHERE group_name = ? AND account_id = ?;`, groupName, accountID)
	return nil
}

// ListGroups returns all groups in an account.
func (s *IAMStore) ListGroups(accountID string) ([]Group, error) {
	rows, err := s.db().Query(
		`SELECT group_name, group_id, arn, account_id, created_at FROM groups WHERE account_id = ? ORDER BY group_name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var groups []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.GroupName, &g.GroupID, &g.Arn, &g.AccountID, &g.CreatedAt); err != nil {
			return nil, err
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// AddUserToGroup adds a user to a group (idempotent).
func (s *IAMStore) AddUserToGroup(accountID, groupName, userName string) error {
	_, err := s.db().Exec(
		`INSERT OR IGNORE INTO group_memberships (group_name, user_name, account_id) VALUES (?, ?, ?);`,
		groupName, userName, accountID,
	)
	return err
}

// RemoveUserFromGroup removes a user from a group.
func (s *IAMStore) RemoveUserFromGroup(accountID, groupName, userName string) error {
	_, err := s.db().Exec(
		`DELETE FROM group_memberships WHERE group_name = ? AND user_name = ? AND account_id = ?;`,
		groupName, userName, accountID,
	)
	return err
}

// --- Access key management store methods ---

// ListAccessKeys returns all access keys for a user.
func (s *IAMStore) ListAccessKeys(accountID, userName string) ([]AccessKey, error) {
	rows, err := s.db().Query(
		`SELECT access_key_id, secret_key, user_name, account_id, status, created_at FROM access_keys WHERE user_name = ? AND account_id = ? ORDER BY created_at;`,
		userName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []AccessKey
	for rows.Next() {
		var k AccessKey
		if err := rows.Scan(&k.AccessKeyID, &k.SecretAccessKey, &k.UserName, &k.AccountID, &k.Status, &k.CreatedAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// UpdateAccessKey updates the status of an access key.
func (s *IAMStore) UpdateAccessKey(accountID, userName, accessKeyID, status string) error {
	_, err := s.db().Exec(
		`UPDATE access_keys SET status = ? WHERE access_key_id = ? AND user_name = ? AND account_id = ?;`,
		status, accessKeyID, userName, accountID,
	)
	return err
}

// DeleteAccessKey deletes an access key.
func (s *IAMStore) DeleteAccessKey(accountID, userName, accessKeyID string) error {
	_, err := s.db().Exec(
		`DELETE FROM access_keys WHERE access_key_id = ? AND user_name = ? AND account_id = ?;`,
		accessKeyID, userName, accountID,
	)
	return err
}

// --- Tagging store methods ---

// TagUser sets tags on a user (upsert per key).
func (s *IAMStore) TagUser(accountID, userName string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(
			`INSERT OR REPLACE INTO user_tags (user_name, tag_key, tag_value, account_id) VALUES (?, ?, ?, ?);`,
			userName, k, v, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UntagUser removes tags by key from a user.
func (s *IAMStore) UntagUser(accountID, userName string, tagKeys []string) error {
	for _, k := range tagKeys {
		_, err := s.db().Exec(
			`DELETE FROM user_tags WHERE user_name = ? AND tag_key = ? AND account_id = ?;`,
			userName, k, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// ListUserTags returns all tags for a user as key→value pairs.
func (s *IAMStore) ListUserTags(accountID, userName string) (map[string]string, error) {
	rows, err := s.db().Query(
		`SELECT tag_key, tag_value FROM user_tags WHERE user_name = ? AND account_id = ? ORDER BY tag_key;`,
		userName, accountID,
	)
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

// TagRole sets tags on a role (upsert per key).
func (s *IAMStore) TagRole(accountID, roleName string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(
			`INSERT OR REPLACE INTO role_tags (role_name, tag_key, tag_value, account_id) VALUES (?, ?, ?, ?);`,
			roleName, k, v, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// UntagRole removes tags by key from a role.
func (s *IAMStore) UntagRole(accountID, roleName string, tagKeys []string) error {
	for _, k := range tagKeys {
		_, err := s.db().Exec(
			`DELETE FROM role_tags WHERE role_name = ? AND tag_key = ? AND account_id = ?;`,
			roleName, k, accountID,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// ListRoleTags returns all tags for a role as key→value pairs.
func (s *IAMStore) ListRoleTags(accountID, roleName string) (map[string]string, error) {
	rows, err := s.db().Query(
		`SELECT tag_key, tag_value FROM role_tags WHERE role_name = ? AND account_id = ? ORDER BY tag_key;`,
		roleName, accountID,
	)
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

// --- Instance profile store methods ---

// CreateInstanceProfile creates a new IAM instance profile.
func (s *IAMStore) CreateInstanceProfile(accountID, profileName string) (*InstanceProfile, error) {
	profileID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("generate instance profile id: %w", err)
	}
	arn := fmt.Sprintf("arn:aws:iam::%s:instance-profile/%s", accountID, profileName)
	now := time.Now().UTC()

	_, err = s.db().Exec(
		`INSERT INTO instance_profiles (profile_name, profile_id, arn, account_id, created_at) VALUES (?, ?, ?, ?, ?);`,
		profileName, profileID, arn, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrInstanceProfileAlreadyExists
		}
		return nil, err
	}

	return &InstanceProfile{
		ProfileName: profileName,
		ProfileID:   profileID,
		Arn:         arn,
		AccountID:   accountID,
		CreatedAt:   now,
	}, nil
}

// GetInstanceProfile returns an instance profile and its associated role names.
func (s *IAMStore) GetInstanceProfile(accountID, profileName string) (*InstanceProfile, []string, error) {
	var ip InstanceProfile
	err := s.db().QueryRow(
		`SELECT profile_name, profile_id, arn, account_id, created_at FROM instance_profiles WHERE profile_name = ? AND account_id = ?;`,
		profileName, accountID,
	).Scan(&ip.ProfileName, &ip.ProfileID, &ip.Arn, &ip.AccountID, &ip.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, ErrInstanceProfileNotFound
		}
		return nil, nil, err
	}

	rows, err := s.db().Query(
		`SELECT role_name FROM instance_profile_roles WHERE profile_name = ? AND account_id = ? ORDER BY role_name;`,
		profileName, accountID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	var roles []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, nil, err
		}
		roles = append(roles, r)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return &ip, roles, nil
}

// DeleteInstanceProfile deletes an instance profile and its role associations.
func (s *IAMStore) DeleteInstanceProfile(accountID, profileName string) error {
	result, err := s.db().Exec(
		`DELETE FROM instance_profiles WHERE profile_name = ? AND account_id = ?;`,
		profileName, accountID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrInstanceProfileNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM instance_profile_roles WHERE profile_name = ? AND account_id = ?;`, profileName, accountID)
	return nil
}

// ListInstanceProfiles returns all instance profiles in an account.
func (s *IAMStore) ListInstanceProfiles(accountID string) ([]InstanceProfile, error) {
	rows, err := s.db().Query(
		`SELECT profile_name, profile_id, arn, account_id, created_at FROM instance_profiles WHERE account_id = ? ORDER BY profile_name;`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var profiles []InstanceProfile
	for rows.Next() {
		var ip InstanceProfile
		if err := rows.Scan(&ip.ProfileName, &ip.ProfileID, &ip.Arn, &ip.AccountID, &ip.CreatedAt); err != nil {
			return nil, err
		}
		profiles = append(profiles, ip)
	}
	return profiles, rows.Err()
}

// AddRoleToInstanceProfile associates a role with an instance profile (idempotent).
func (s *IAMStore) AddRoleToInstanceProfile(accountID, profileName, roleName string) error {
	_, err := s.db().Exec(
		`INSERT OR IGNORE INTO instance_profile_roles (profile_name, role_name, account_id) VALUES (?, ?, ?);`,
		profileName, roleName, accountID,
	)
	return err
}

// RemoveRoleFromInstanceProfile removes a role from an instance profile.
func (s *IAMStore) RemoveRoleFromInstanceProfile(accountID, profileName, roleName string) error {
	_, err := s.db().Exec(
		`DELETE FROM instance_profile_roles WHERE profile_name = ? AND role_name = ? AND account_id = ?;`,
		profileName, roleName, accountID,
	)
	return err
}

// --- Inline policy store methods ---

// PutUserInlinePolicy inserts or replaces an inline policy for a user.
func (s *IAMStore) PutUserInlinePolicy(accountID, userName, policyName, policyDoc string) error {
	_, err := s.db().Exec(
		`INSERT OR REPLACE INTO user_inline_policies (user_name, policy_name, policy_doc, account_id) VALUES (?, ?, ?, ?);`,
		userName, policyName, policyDoc, accountID,
	)
	return err
}

// GetUserInlinePolicy returns the document for a named inline user policy.
func (s *IAMStore) GetUserInlinePolicy(accountID, userName, policyName string) (string, error) {
	var doc string
	err := s.db().QueryRow(
		`SELECT policy_doc FROM user_inline_policies WHERE user_name = ? AND policy_name = ? AND account_id = ?;`,
		userName, policyName, accountID,
	).Scan(&doc)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", ErrPolicyNotFound
		}
		return "", err
	}
	return doc, nil
}

// DeleteUserInlinePolicy deletes a named inline policy from a user.
func (s *IAMStore) DeleteUserInlinePolicy(accountID, userName, policyName string) error {
	result, err := s.db().Exec(
		`DELETE FROM user_inline_policies WHERE user_name = ? AND policy_name = ? AND account_id = ?;`,
		userName, policyName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

// ListUserInlinePolicies returns the names of inline policies attached to a user.
func (s *IAMStore) ListUserInlinePolicies(accountID, userName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_name FROM user_inline_policies WHERE user_name = ? AND account_id = ? ORDER BY policy_name;`,
		userName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// --- Group managed & inline policies (mirror the user/role variants) ---

func (s *IAMStore) AttachGroupPolicy(accountID, groupName, policyArn string) error {
	_, err := s.db().Exec(
		`INSERT OR IGNORE INTO group_policies (group_name, policy_arn, account_id) VALUES (?, ?, ?);`,
		groupName, policyArn, accountID,
	)
	return err
}

func (s *IAMStore) DetachGroupPolicy(accountID, groupName, policyArn string) error {
	result, err := s.db().Exec(
		`DELETE FROM group_policies WHERE group_name = ? AND policy_arn = ? AND account_id = ?;`,
		groupName, policyArn, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

func (s *IAMStore) ListAttachedGroupPolicies(accountID, groupName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_arn FROM group_policies WHERE group_name = ? AND account_id = ? ORDER BY policy_arn;`,
		groupName, accountID,
	)
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

func (s *IAMStore) PutGroupInlinePolicy(accountID, groupName, policyName, policyDoc string) error {
	_, err := s.db().Exec(
		`INSERT OR REPLACE INTO group_inline_policies (group_name, policy_name, policy_doc, account_id) VALUES (?, ?, ?, ?);`,
		groupName, policyName, policyDoc, accountID,
	)
	return err
}

func (s *IAMStore) GetGroupInlinePolicy(accountID, groupName, policyName string) (string, error) {
	var doc string
	err := s.db().QueryRow(
		`SELECT policy_doc FROM group_inline_policies WHERE group_name = ? AND policy_name = ? AND account_id = ?;`,
		groupName, policyName, accountID,
	).Scan(&doc)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", ErrPolicyNotFound
		}
		return "", err
	}
	return doc, nil
}

func (s *IAMStore) DeleteGroupInlinePolicy(accountID, groupName, policyName string) error {
	result, err := s.db().Exec(
		`DELETE FROM group_inline_policies WHERE group_name = ? AND policy_name = ? AND account_id = ?;`,
		groupName, policyName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

func (s *IAMStore) ListGroupInlinePolicies(accountID, groupName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_name FROM group_inline_policies WHERE group_name = ? AND account_id = ? ORDER BY policy_name;`,
		groupName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// PutRoleInlinePolicy inserts or replaces an inline policy for a role.
func (s *IAMStore) PutRoleInlinePolicy(accountID, roleName, policyName, policyDoc string) error {
	_, err := s.db().Exec(
		`INSERT OR REPLACE INTO role_inline_policies (role_name, policy_name, policy_doc, account_id) VALUES (?, ?, ?, ?);`,
		roleName, policyName, policyDoc, accountID,
	)
	return err
}

// GetRoleInlinePolicy returns the document for a named inline role policy.
func (s *IAMStore) GetRoleInlinePolicy(accountID, roleName, policyName string) (string, error) {
	var doc string
	err := s.db().QueryRow(
		`SELECT policy_doc FROM role_inline_policies WHERE role_name = ? AND policy_name = ? AND account_id = ?;`,
		roleName, policyName, accountID,
	).Scan(&doc)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", ErrPolicyNotFound
		}
		return "", err
	}
	return doc, nil
}

// DeleteRoleInlinePolicy deletes a named inline policy from a role.
func (s *IAMStore) DeleteRoleInlinePolicy(accountID, roleName, policyName string) error {
	result, err := s.db().Exec(
		`DELETE FROM role_inline_policies WHERE role_name = ? AND policy_name = ? AND account_id = ?;`,
		roleName, policyName, accountID,
	)
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrPolicyNotFound
	}
	return nil
}

// ListRoleInlinePolicies returns the names of inline policies attached to a role.
func (s *IAMStore) ListRoleInlinePolicies(accountID, roleName string) ([]string, error) {
	rows, err := s.db().Query(
		`SELECT policy_name FROM role_inline_policies WHERE role_name = ? AND account_id = ? ORDER BY policy_name;`,
		roleName, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// generateID returns a random hex string suitable for use as a resource ID.
func generateID() (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.ToUpper(hex.EncodeToString(b)), nil
}

// generateAccessKeyID returns a random AKIA-prefixed access key ID (20 chars total).
func generateAccessKeyID() (string, error) {
	// AKIA + 16 uppercase alphanumeric characters
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "AKIA" + strings.ToUpper(hex.EncodeToString(b)), nil
}

// generateSecret returns a random 40-character secret access key.
func generateSecret() (string, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// EnableMFADevice records an MFA device as active for the specified user.
func (s *IAMStore) EnableMFADevice(accountID, userName, serialNumber string) error {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(`
		INSERT INTO mfa_devices (serial_number, user_name, account_id, status, enable_date)
		VALUES (?, ?, ?, 'Active', ?)
		ON CONFLICT(serial_number) DO UPDATE SET status = 'Active', user_name = excluded.user_name, enable_date = excluded.enable_date
	`, serialNumber, userName, accountID, now)
	return err
}

// DeactivateMFADevice deactivates an MFA device.
func (s *IAMStore) DeactivateMFADevice(accountID, userName, serialNumber string) error {
	_, err := s.store.DB().Exec(`
		UPDATE mfa_devices SET status = 'Inactive'
		WHERE serial_number = ? AND user_name = ? AND account_id = ?
	`, serialNumber, userName, accountID)
	return err
}

// ResyncMFADevice resyncs an MFA device.
func (s *IAMStore) ResyncMFADevice(accountID, userName, serialNumber string) error {
	return s.EnableMFADevice(accountID, userName, serialNumber)
}

// AddClientIDToOpenIDConnectProvider adds a client ID to an OIDC provider.
func (s *IAMStore) AddClientIDToOpenIDConnectProvider(accountID, arn, clientID string) error {
	var clientIDs string
	err := s.store.DB().QueryRow("SELECT client_ids FROM oidc_providers WHERE arn = ? AND account_id = ?", arn, accountID).Scan(&clientIDs)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = s.store.DB().Exec("INSERT INTO oidc_providers (arn, url, client_ids, account_id) VALUES (?, ?, ?, ?)", arn, "https://oidc.devcloud.local", clientID, accountID)
		return err
	}
	if err != nil {
		return err
	}
	list := strings.Split(clientIDs, ",")
	for _, id := range list {
		if id == clientID {
			return nil
		}
	}
	if clientIDs != "" {
		clientIDs += "," + clientID
	} else {
		clientIDs = clientID
	}
	_, err = s.store.DB().Exec("UPDATE oidc_providers SET client_ids = ? WHERE arn = ? AND account_id = ?", clientIDs, arn, accountID)
	return err
}

// RemoveClientIDFromOpenIDConnectProvider removes a client ID from an OIDC provider.
func (s *IAMStore) RemoveClientIDFromOpenIDConnectProvider(accountID, arn, clientID string) error {
	var clientIDs string
	err := s.store.DB().QueryRow("SELECT client_ids FROM oidc_providers WHERE arn = ? AND account_id = ?", arn, accountID).Scan(&clientIDs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	list := strings.Split(clientIDs, ",")
	var remaining []string
	for _, id := range list {
		if id != clientID && id != "" {
			remaining = append(remaining, id)
		}
	}
	_, err = s.store.DB().Exec("UPDATE oidc_providers SET client_ids = ? WHERE arn = ? AND account_id = ?", strings.Join(remaining, ","), arn, accountID)
	return err
}

// UploadSigningCertificate uploads a user signing certificate.
func (s *IAMStore) UploadSigningCertificate(accountID, userName, certBody string) (string, time.Time, error) {
	certID, err := generateID()
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now().UTC()
	_, err = s.store.DB().Exec(`
		INSERT INTO signing_certificates (certificate_id, user_name, certificate_body, status, account_id, upload_date)
		VALUES (?, ?, ?, 'Active', ?, ?)
	`, certID, userName, certBody, accountID, now)
	return certID, now, err
}

// UploadSSHPublicKey uploads a user SSH public key.
func (s *IAMStore) UploadSSHPublicKey(accountID, userName, keyBody string) (string, time.Time, error) {
	keyID, err := generateID()
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now().UTC()
	_, err = s.store.DB().Exec(`
		INSERT INTO ssh_public_keys (key_id, user_name, key_body, status, account_id, upload_date)
		VALUES (?, ?, ?, 'Active', ?, ?)
	`, keyID, userName, keyBody, accountID, now)
	return keyID, now, err
}

// UploadServerCertificate uploads an SSL server certificate.
func (s *IAMStore) UploadServerCertificate(accountID, name, path, certBody string) (string, string, time.Time, error) {
	if path == "" {
		path = "/"
	}
	certID, err := generateID()
	if err != nil {
		return "", "", time.Time{}, err
	}
	now := time.Now().UTC()
	arn := fmt.Sprintf("arn:aws:iam::%s:server-certificate%s%s", accountID, path, name)
	_, err = s.store.DB().Exec(`
		INSERT INTO server_certificates (certificate_name, certificate_id, arn, path, certificate_body, account_id, upload_date)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(certificate_name, account_id) DO UPDATE SET certificate_id = excluded.certificate_id, arn = excluded.arn, certificate_body = excluded.certificate_body, upload_date = excluded.upload_date
	`, name, certID, arn, path, certBody, accountID, now)
	return certID, arn, now, err
}

// SetDefaultPolicyVersion sets the default version for a managed policy.
func (s *IAMStore) SetDefaultPolicyVersion(accountID, policyArn, versionID string) error {
	_, err := s.store.DB().Exec("UPDATE policy_versions SET is_default = 0 WHERE policy_arn = ?", policyArn)
	if err != nil {
		return err
	}
	_, err = s.store.DB().Exec("UPDATE policy_versions SET is_default = 1 WHERE policy_arn = ? AND version_id = ?", policyArn, versionID)
	return err
}

// SetSTSPreferences records STS endpoint preferences.
func (s *IAMStore) SetSTSPreferences(accountID, tokenVersion string) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO sts_preferences (account_id, token_version)
		VALUES (?, ?)
		ON CONFLICT(account_id) DO UPDATE SET token_version = excluded.token_version
	`, accountID, tokenVersion)
	return err
}

// SetOrgSettings updates organizational root / session settings.
func (s *IAMStore) SetOrgSettings(accountID string, rootCreds, rootSessions, outboundWebIdentity *bool) error {
	_, _ = s.store.DB().Exec(`INSERT OR IGNORE INTO org_settings (account_id, root_credentials_enabled, root_sessions_enabled, outbound_web_identity_enabled) VALUES (?, 0, 0, 1)`, accountID)
	if rootCreds != nil {
		val := 0
		if *rootCreds {
			val = 1
		}
		if _, err := s.store.DB().Exec("UPDATE org_settings SET root_credentials_enabled = ? WHERE account_id = ?", val, accountID); err != nil {
			return err
		}
	}
	if rootSessions != nil {
		val := 0
		if *rootSessions {
			val = 1
		}
		if _, err := s.store.DB().Exec("UPDATE org_settings SET root_sessions_enabled = ? WHERE account_id = ?", val, accountID); err != nil {
			return err
		}
	}
	if outboundWebIdentity != nil {
		val := 0
		if *outboundWebIdentity {
			val = 1
		}
		if _, err := s.store.DB().Exec("UPDATE org_settings SET outbound_web_identity_enabled = ? WHERE account_id = ?", val, accountID); err != nil {
			return err
		}
	}
	return nil
}

// RecordDelegationRequest stores or updates a delegation request.
func (s *IAMStore) RecordDelegationRequest(accountID, requestID, targetAccount, token, status string) error {
	now := time.Now().UTC()
	_, err := s.store.DB().Exec(`
		INSERT INTO delegation_requests (request_id, target_account, status, token, account_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(request_id) DO UPDATE SET status = excluded.status, token = excluded.token
	`, requestID, targetAccount, status, token, accountID, now)
	return err
}
