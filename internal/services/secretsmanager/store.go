// SPDX-License-Identifier: Apache-2.0

// internal/services/secretsmanager/store.go
package secretsmanager

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrSecretNotFound  = errors.New("secret not found")
	ErrSecretDeleted   = errors.New("secret is scheduled for deletion")
	ErrVersionNotFound = errors.New("secret version not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS secrets (
			arn         TEXT PRIMARY KEY,
			name        TEXT NOT NULL UNIQUE,
			account_id  TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			kms_key_id  TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			deleted_at  INTEGER
		);
		CREATE TABLE IF NOT EXISTS secret_versions (
			secret_arn     TEXT NOT NULL,
			version_id     TEXT NOT NULL,
			version_stages TEXT NOT NULL DEFAULT 'AWSCURRENT',
			secret_binary  BLOB,
			secret_string  TEXT,
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (secret_arn, version_id)
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS secret_tags (
			secret_arn TEXT NOT NULL,
			tag_key    TEXT NOT NULL,
			tag_value  TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (secret_arn, tag_key)
		);
	`},
	{Version: 3, SQL: `
		ALTER TABLE secrets ADD COLUMN rotation_enabled INTEGER NOT NULL DEFAULT 0;
		ALTER TABLE secrets ADD COLUMN rotation_lambda_arn TEXT NOT NULL DEFAULT '';
		ALTER TABLE secrets ADD COLUMN rotation_rules TEXT NOT NULL DEFAULT '';
		ALTER TABLE secrets ADD COLUMN resource_policy TEXT NOT NULL DEFAULT '';
	`},
}

type Secret struct {
	ARN               string
	Name              string
	AccountID         string
	Description       string
	KMSKeyID          string
	CreatedAt         time.Time
	DeletedAt         *time.Time
	RotationEnabled   bool
	RotationLambdaARN string
	RotationRules     string // JSON
	ResourcePolicy    string // JSON
}

type SecretVersion struct {
	SecretARN     string
	SecretName    string
	VersionID     string
	VersionStages []string
	SecretBinary  []byte
	SecretString  string
	CreatedAt     time.Time
}

type SecretStore struct {
	store *sqlite.Store
}

func NewSecretStore(dataDir string) (*SecretStore, error) {
	dbPath := filepath.Join(dataDir, "secretsmanager.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &SecretStore{store: s}, nil
}

func (s *SecretStore) Close() error { return s.store.Close() }

func randomVersionID() string {
	b := make([]byte, 16)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func (s *SecretStore) CreateSecret(arn, name, accountID, description, kmsKeyID string, secretString string, secretBinary []byte) (*Secret, error) {
	now := time.Now().Unix()
	tx, err := s.store.DB().Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.Exec(
		`INSERT INTO secrets (arn, name, account_id, description, kms_key_id, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, accountID, description, kmsKeyID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, fmt.Errorf("secret already exists: %s", name)
		}
		return nil, err
	}
	versionID := randomVersionID()
	_, err = tx.Exec(
		`INSERT INTO secret_versions (secret_arn, version_id, version_stages, secret_string, secret_binary, created_at) VALUES (?, ?, 'AWSCURRENT', ?, ?, ?)`,
		arn, versionID, secretString, secretBinary, now,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &Secret{ARN: arn, Name: name, AccountID: accountID, Description: description, KMSKeyID: kmsKeyID, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *SecretStore) GetSecret(nameOrARN string) (*Secret, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, account_id, description, kms_key_id, created_at, deleted_at,
		        rotation_enabled, rotation_lambda_arn, rotation_rules, resource_policy
		 FROM secrets WHERE (arn = ? OR name = ?)`,
		nameOrARN, nameOrARN,
	)
	return scanSecret(row)
}

func scanSecret(row *sql.Row) (*Secret, error) {
	var sec Secret
	var createdAt int64
	var deletedAt sql.NullInt64
	var rotEnabled int
	if err := row.Scan(
		&sec.ARN, &sec.Name, &sec.AccountID, &sec.Description, &sec.KMSKeyID,
		&createdAt, &deletedAt,
		&rotEnabled, &sec.RotationLambdaARN, &sec.RotationRules, &sec.ResourcePolicy,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSecretNotFound
		}
		return nil, err
	}
	sec.CreatedAt = time.Unix(createdAt, 0)
	if deletedAt.Valid {
		t := time.Unix(deletedAt.Int64, 0)
		sec.DeletedAt = &t
	}
	sec.RotationEnabled = rotEnabled != 0
	return &sec, nil
}

func (s *SecretStore) ListSecrets(accountID string) ([]Secret, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, account_id, description, kms_key_id, created_at, deleted_at,
		        rotation_enabled, rotation_lambda_arn, rotation_rules, resource_policy
		 FROM secrets WHERE account_id = ? AND deleted_at IS NULL ORDER BY created_at`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var secrets []Secret
	for rows.Next() {
		var sec Secret
		var createdAt int64
		var deletedAt sql.NullInt64
		var rotEnabled int
		if err := rows.Scan(
			&sec.ARN, &sec.Name, &sec.AccountID, &sec.Description, &sec.KMSKeyID,
			&createdAt, &deletedAt,
			&rotEnabled, &sec.RotationLambdaARN, &sec.RotationRules, &sec.ResourcePolicy,
		); err != nil {
			return nil, err
		}
		sec.CreatedAt = time.Unix(createdAt, 0)
		sec.RotationEnabled = rotEnabled != 0
		secrets = append(secrets, sec)
	}
	return secrets, rows.Err()
}

func (s *SecretStore) DeleteSecret(nameOrARN string) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE secrets SET deleted_at = ? WHERE (arn = ? OR name = ?) AND deleted_at IS NULL`,
		now, nameOrARN, nameOrARN,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSecretNotFound
	}
	return nil
}

func (s *SecretStore) PutSecretValue(nameOrARN, secretString string, secretBinary []byte) (*SecretVersion, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	if sec.DeletedAt != nil {
		return nil, ErrSecretDeleted
	}
	now := time.Now().Unix()
	tx, err := s.store.DB().Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	// Rotate: AWSCURRENT → AWSPREVIOUS.
	_, err = tx.Exec(
		`UPDATE secret_versions SET version_stages = 'AWSPREVIOUS' WHERE secret_arn = ? AND version_stages LIKE '%AWSCURRENT%'`,
		sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	versionID := randomVersionID()
	_, err = tx.Exec(
		`INSERT INTO secret_versions (secret_arn, version_id, version_stages, secret_string, secret_binary, created_at) VALUES (?, ?, 'AWSCURRENT', ?, ?, ?)`,
		sec.ARN, versionID, secretString, secretBinary, now,
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &SecretVersion{SecretARN: sec.ARN, SecretName: sec.Name, VersionID: versionID, VersionStages: []string{"AWSCURRENT"}, SecretString: secretString, SecretBinary: secretBinary, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *SecretStore) GetSecretValue(nameOrARN, versionID, versionStage string) (*SecretVersion, error) {
	if versionStage == "" {
		versionStage = "AWSCURRENT"
	}
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	var query string
	var args []any
	if versionID != "" {
		query = `SELECT secret_arn, version_id, version_stages, secret_string, secret_binary, created_at FROM secret_versions WHERE secret_arn = ? AND version_id = ?`
		args = []any{sec.ARN, versionID}
	} else {
		query = `SELECT secret_arn, version_id, version_stages, secret_string, secret_binary, created_at FROM secret_versions WHERE secret_arn = ? AND version_stages LIKE ? ORDER BY created_at DESC LIMIT 1`
		args = []any{sec.ARN, "%" + versionStage + "%"}
	}
	row := s.store.DB().QueryRow(query, args...)
	var v SecretVersion
	var createdAt int64
	var stages string
	var secretString sql.NullString
	var secretBinary []byte
	if err := row.Scan(&v.SecretARN, &v.VersionID, &stages, &secretString, &secretBinary, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVersionNotFound
		}
		return nil, err
	}
	v.VersionStages = []string{stages}
	v.SecretString = secretString.String
	v.SecretBinary = secretBinary
	v.CreatedAt = time.Unix(createdAt, 0)
	v.SecretName = sec.Name
	return &v, nil
}

func (s *SecretStore) UpdateSecret(nameOrARN, description, kmsKeyID string) (*Secret, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	if sec.DeletedAt != nil {
		return nil, ErrSecretDeleted
	}
	if description != "" {
		sec.Description = description
	}
	if kmsKeyID != "" {
		sec.KMSKeyID = kmsKeyID
	}
	_, err = s.store.DB().Exec(
		`UPDATE secrets SET description = ?, kms_key_id = ? WHERE arn = ?`,
		sec.Description, sec.KMSKeyID, sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	return sec, nil
}

func (s *SecretStore) RestoreSecret(nameOrARN string) (*Secret, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	if sec.DeletedAt == nil {
		return nil, fmt.Errorf("secret is not scheduled for deletion")
	}
	_, err = s.store.DB().Exec(
		`UPDATE secrets SET deleted_at = NULL WHERE arn = ?`,
		sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	sec.DeletedAt = nil
	return sec, nil
}

type Tag struct {
	Key   string
	Value string
}

func (s *SecretStore) TagResource(nameOrARN string, tags []Tag) error {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return err
	}
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, t := range tags {
		_, err = tx.Exec(
			`INSERT OR REPLACE INTO secret_tags (secret_arn, tag_key, tag_value) VALUES (?, ?, ?)`,
			sec.ARN, t.Key, t.Value,
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SecretStore) GetTags(arn string) ([]Tag, error) {
	rows, err := s.store.DB().Query(
		`SELECT tag_key, tag_value FROM secret_tags WHERE secret_arn = ? ORDER BY tag_key`,
		arn,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tags []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.Key, &t.Value); err != nil {
			return nil, err
		}
		tags = append(tags, t)
	}
	return tags, rows.Err()
}

func (s *SecretStore) UntagResource(nameOrARN string, tagKeys []string) error {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return err
	}
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, k := range tagKeys {
		_, err = tx.Exec(`DELETE FROM secret_tags WHERE secret_arn = ? AND tag_key = ?`, sec.ARN, k)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RotateSecret enables rotation and records lambda ARN + rules.
func (s *SecretStore) RotateSecret(nameOrARN, lambdaARN, rulesJSON string) (*Secret, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	_, err = s.store.DB().Exec(
		`UPDATE secrets SET rotation_enabled = 1, rotation_lambda_arn = ?, rotation_rules = ? WHERE arn = ?`,
		lambdaARN, rulesJSON, sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	sec.RotationEnabled = true
	sec.RotationLambdaARN = lambdaARN
	sec.RotationRules = rulesJSON
	return sec, nil
}

// CancelRotateSecret disables rotation.
func (s *SecretStore) CancelRotateSecret(nameOrARN string) (*Secret, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	_, err = s.store.DB().Exec(
		`UPDATE secrets SET rotation_enabled = 0, rotation_lambda_arn = '', rotation_rules = '' WHERE arn = ?`,
		sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	sec.RotationEnabled = false
	sec.RotationLambdaARN = ""
	sec.RotationRules = ""
	return sec, nil
}

type SecretVersionInfo struct {
	VersionID      string
	VersionStages  []string
	CreatedAt      time.Time
	LastAccessedAt time.Time
}

// ListSecretVersionIds returns all versions for a secret.
func (s *SecretStore) ListSecretVersionIds(nameOrARN string, includeDeprecated bool) ([]SecretVersionInfo, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	query := `SELECT version_id, version_stages, created_at FROM secret_versions WHERE secret_arn = ?`
	if !includeDeprecated {
		query += ` AND version_stages != ''`
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, sec.ARN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []SecretVersionInfo
	for rows.Next() {
		var v SecretVersionInfo
		var stages string
		var createdAt int64
		if err := rows.Scan(&v.VersionID, &stages, &createdAt); err != nil {
			return nil, err
		}
		v.CreatedAt = time.Unix(createdAt, 0)
		v.LastAccessedAt = v.CreatedAt
		if stages != "" {
			v.VersionStages = strings.Split(stages, ",")
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

// UpdateSecretVersionStage moves a stage label between versions.
func (s *SecretStore) UpdateSecretVersionStage(nameOrARN, stage, moveToVersionID, removeFromVersionID string) error {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return err
	}
	tx, err := s.store.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	// Remove stage from removeFromVersionID if specified.
	if removeFromVersionID != "" {
		var stages string
		err = tx.QueryRow(
			`SELECT version_stages FROM secret_versions WHERE secret_arn = ? AND version_id = ?`,
			sec.ARN, removeFromVersionID,
		).Scan(&stages)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			parts := removeStage(strings.Split(stages, ","), stage)
			_, err = tx.Exec(
				`UPDATE secret_versions SET version_stages = ? WHERE secret_arn = ? AND version_id = ?`,
				strings.Join(parts, ","), sec.ARN, removeFromVersionID,
			)
			if err != nil {
				return err
			}
		}
	}

	// Add stage to moveToVersionID if specified.
	if moveToVersionID != "" {
		var stages string
		err = tx.QueryRow(
			`SELECT version_stages FROM secret_versions WHERE secret_arn = ? AND version_id = ?`,
			sec.ARN, moveToVersionID,
		).Scan(&stages)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrVersionNotFound
			}
			return err
		}
		parts := addStage(strings.Split(stages, ","), stage)
		_, err = tx.Exec(
			`UPDATE secret_versions SET version_stages = ? WHERE secret_arn = ? AND version_id = ?`,
			strings.Join(parts, ","), sec.ARN, moveToVersionID,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func removeStage(stages []string, stage string) []string {
	var out []string
	for _, s := range stages {
		if strings.TrimSpace(s) != stage {
			out = append(out, s)
		}
	}
	return out
}

func addStage(stages []string, stage string) []string {
	for _, s := range stages {
		if strings.TrimSpace(s) == stage {
			return stages
		}
	}
	return append(stages, stage)
}

// PutResourcePolicy stores a resource policy on the secret.
func (s *SecretStore) PutResourcePolicy(nameOrARN, policy string) (*Secret, error) {
	sec, err := s.GetSecret(nameOrARN)
	if err != nil {
		return nil, err
	}
	_, err = s.store.DB().Exec(
		`UPDATE secrets SET resource_policy = ? WHERE arn = ?`,
		policy, sec.ARN,
	)
	if err != nil {
		return nil, err
	}
	sec.ResourcePolicy = policy
	return sec, nil
}

// GetResourcePolicy returns the stored resource policy.
func (s *SecretStore) GetResourcePolicy(nameOrARN string) (*Secret, error) {
	return s.GetSecret(nameOrARN)
}

// DeleteResourcePolicy clears the resource policy.
func (s *SecretStore) DeleteResourcePolicy(nameOrARN string) (*Secret, error) {
	return s.PutResourcePolicy(nameOrARN, "")
}

// BatchGetSecretValues retrieves values for multiple secrets by name or ARN.
func (s *SecretStore) BatchGetSecretValues(ids []string) ([]*SecretVersion, []error) {
	results := make([]*SecretVersion, 0, len(ids))
	errs := make([]error, 0)
	for _, id := range ids {
		v, err := s.GetSecretValue(id, "", "AWSCURRENT")
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", id, err))
			continue
		}
		results = append(results, v)
	}
	return results, errs
}
