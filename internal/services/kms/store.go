// SPDX-License-Identifier: Apache-2.0

package kms

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrAliasNotFound = errors.New("alias not found")
	ErrKeyDisabled   = errors.New("key is disabled")
	ErrGrantNotFound = errors.New("grant not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS keys (
			key_id       TEXT PRIMARY KEY,
			arn          TEXT NOT NULL,
			account_id   TEXT NOT NULL,
			description  TEXT NOT NULL DEFAULT '',
			key_state    TEXT NOT NULL DEFAULT 'Enabled',
			key_spec     TEXT NOT NULL DEFAULT 'SYMMETRIC_DEFAULT',
			key_usage    TEXT NOT NULL DEFAULT 'ENCRYPT_DECRYPT',
			key_material BLOB NOT NULL,
			created_at   INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS aliases (
			alias_name TEXT PRIMARY KEY,
			key_id     TEXT NOT NULL,
			account_id TEXT NOT NULL
		);
	`},
	{Version: 2, SQL: `
		ALTER TABLE keys ADD COLUMN deletion_date INTEGER DEFAULT 0;
	`},
	{Version: 3, SQL: `
		CREATE TABLE IF NOT EXISTS grants (
			grant_id          TEXT PRIMARY KEY,
			grant_token       TEXT NOT NULL,
			key_id            TEXT NOT NULL,
			grantee_principal TEXT NOT NULL,
			retiring_principal TEXT,
			operations        TEXT NOT NULL,
			name              TEXT,
			account_id        TEXT NOT NULL,
			created_at        DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS key_policies (
			key_id      TEXT NOT NULL,
			policy_name TEXT NOT NULL,
			policy      TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			PRIMARY KEY (key_id, policy_name, account_id)
		);
	`},
}

type Key struct {
	KeyID       string
	ARN         string
	AccountID   string
	Description string
	KeyState    string
	KeySpec     string
	KeyUsage    string
	KeyMaterial []byte
	CreatedAt   time.Time
}

type Alias struct {
	AliasName string
	KeyID     string
	AccountID string
}

type KMSStore struct {
	store *sqlite.Store
}

func NewKMSStore(dataDir string) (*KMSStore, error) {
	dbPath := filepath.Join(dataDir, "kms.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &KMSStore{store: s}, nil
}

func (s *KMSStore) Close() error { return s.store.Close() }

func generateKeyMaterial() ([]byte, error) {
	key := make([]byte, 32) // AES-256
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

func (s *KMSStore) CreateKey(keyID, arn, accountID, description, keySpec, keyUsage string) (*Key, error) {
	material, err := generateKeyMaterial()
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	_, err = s.store.DB().Exec(
		`INSERT INTO keys (key_id, arn, account_id, description, key_state, key_spec, key_usage, key_material, created_at)
		 VALUES (?, ?, ?, ?, 'Enabled', ?, ?, ?, ?)`,
		keyID, arn, accountID, description, keySpec, keyUsage, material, now,
	)
	if err != nil {
		return nil, err
	}
	return &Key{
		KeyID: keyID, ARN: arn, AccountID: accountID, Description: description,
		KeyState: "Enabled", KeySpec: keySpec, KeyUsage: keyUsage,
		KeyMaterial: material, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *KMSStore) GetKey(keyID string) (*Key, error) {
	row := s.store.DB().QueryRow(
		`SELECT key_id, arn, account_id, description, key_state, key_spec, key_usage, key_material, created_at FROM keys WHERE key_id = ?`,
		keyID,
	)
	return scanKey(row)
}

func (s *KMSStore) GetKeyByAlias(aliasName string) (*Key, error) {
	row := s.store.DB().QueryRow(
		`SELECT k.key_id, k.arn, k.account_id, k.description, k.key_state, k.key_spec, k.key_usage, k.key_material, k.created_at
		 FROM keys k JOIN aliases a ON k.key_id = a.key_id WHERE a.alias_name = ?`,
		aliasName,
	)
	return scanKey(row)
}

func scanKey(row *sql.Row) (*Key, error) {
	var k Key
	var createdAt int64
	if err := row.Scan(&k.KeyID, &k.ARN, &k.AccountID, &k.Description, &k.KeyState, &k.KeySpec, &k.KeyUsage, &k.KeyMaterial, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	k.CreatedAt = time.Unix(createdAt, 0)
	return &k, nil
}

func (s *KMSStore) ListKeys(accountID string) ([]Key, error) {
	rows, err := s.store.DB().Query(
		`SELECT key_id, arn, account_id, description, key_state, key_spec, key_usage, key_material, created_at FROM keys WHERE account_id = ? ORDER BY created_at`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []Key
	for rows.Next() {
		var k Key
		var createdAt int64
		if err := rows.Scan(&k.KeyID, &k.ARN, &k.AccountID, &k.Description, &k.KeyState, &k.KeySpec, &k.KeyUsage, &k.KeyMaterial, &createdAt); err != nil {
			return nil, err
		}
		k.CreatedAt = time.Unix(createdAt, 0)
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

func (s *KMSStore) CreateAlias(aliasName, keyID, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO aliases (alias_name, key_id, account_id) VALUES (?, ?, ?)
		 ON CONFLICT(alias_name) DO UPDATE SET key_id = excluded.key_id`,
		aliasName, keyID, accountID,
	)
	return err
}

func (s *KMSStore) ListAliases(accountID string) ([]Alias, error) {
	rows, err := s.store.DB().Query(`SELECT alias_name, key_id, account_id FROM aliases WHERE account_id = ?`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var aliases []Alias
	for rows.Next() {
		var a Alias
		if err := rows.Scan(&a.AliasName, &a.KeyID, &a.AccountID); err != nil {
			return nil, err
		}
		aliases = append(aliases, a)
	}
	return aliases, rows.Err()
}

func (s *KMSStore) UpdateKeyState(keyID, state string) error {
	_, err := s.store.DB().Exec(`UPDATE keys SET key_state = ? WHERE key_id = ?`, state, keyID)
	return err
}

// Encrypt encrypts plaintext using AES-256-GCM with the key's material.
// Returns hex-encoded ciphertext with a 12-byte nonce prefix.
func Encrypt(key *Key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key.KeyMaterial)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	dst := make([]byte, hex.EncodedLen(len(ciphertext)))
	hex.Encode(dst, ciphertext)
	return dst, nil
}

// Decrypt decrypts hex-encoded ciphertext produced by Encrypt.
func Decrypt(key *Key, ciphertextHex []byte) ([]byte, error) {
	ciphertext, err := hex.DecodeString(string(ciphertextHex))
	if err != nil {
		return nil, fmt.Errorf("invalid ciphertext encoding: %w", err)
	}
	block, err := aes.NewCipher(key.KeyMaterial)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("ciphertext too short")
	}
	nonce, ct := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ct, nil)
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

// HMACSign computes HMAC-SHA256 of message using the key's material.
func HMACSign(key *Key, message []byte) []byte {
	mac := hmac.New(sha256.New, key.KeyMaterial)
	mac.Write(message)
	return mac.Sum(nil)
}

// HMACVerify checks whether signature matches HMAC-SHA256 of message.
func HMACVerify(key *Key, message, signature []byte) bool {
	expected := HMACSign(key, message)
	return hmac.Equal(expected, signature)
}

// --- Key extensions ---

func (s *KMSStore) UpdateKeyDescription(keyID, description string) error {
	_, err := s.store.DB().Exec(`UPDATE keys SET description = ? WHERE key_id = ?`, description, keyID)
	return err
}

func (s *KMSStore) SetDeletionDate(keyID string, deletionDate int64) error {
	_, err := s.store.DB().Exec(`UPDATE keys SET key_state = 'PendingDeletion', deletion_date = ? WHERE key_id = ?`, deletionDate, keyID)
	return err
}

func (s *KMSStore) CancelDeletion(keyID string) error {
	_, err := s.store.DB().Exec(`UPDATE keys SET key_state = 'Enabled', deletion_date = 0 WHERE key_id = ?`, keyID)
	return err
}

// --- Grants ---

type Grant struct {
	GrantID           string
	GrantToken        string
	KeyID             string
	GranteePrincipal  string
	RetiringPrincipal string
	Operations        string
	Name              string
	AccountID         string
	CreatedAt         time.Time
}

func (s *KMSStore) CreateGrant(grantID, grantToken, keyID, granteePrincipal, retiringPrincipal, operations, name, accountID string) (*Grant, error) {
	now := time.Now()
	_, err := s.store.DB().Exec(
		`INSERT INTO grants (grant_id, grant_token, key_id, grantee_principal, retiring_principal, operations, name, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		grantID, grantToken, keyID, granteePrincipal, retiringPrincipal, operations, name, accountID, now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, err
	}
	return &Grant{
		GrantID: grantID, GrantToken: grantToken, KeyID: keyID,
		GranteePrincipal: granteePrincipal, RetiringPrincipal: retiringPrincipal,
		Operations: operations, Name: name, AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *KMSStore) ListGrants(keyID, accountID string) ([]Grant, error) {
	rows, err := s.store.DB().Query(
		`SELECT grant_id, grant_token, key_id, grantee_principal, COALESCE(retiring_principal,''), operations, COALESCE(name,''), account_id, created_at
		 FROM grants WHERE key_id = ? AND account_id = ? ORDER BY created_at`,
		keyID, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGrants(rows)
}

func (s *KMSStore) ListRetirableGrants(retiringPrincipal, accountID string) ([]Grant, error) {
	rows, err := s.store.DB().Query(
		`SELECT grant_id, grant_token, key_id, grantee_principal, COALESCE(retiring_principal,''), operations, COALESCE(name,''), account_id, created_at
		 FROM grants WHERE retiring_principal = ? AND account_id = ? ORDER BY created_at`,
		retiringPrincipal, accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGrants(rows)
}

func scanGrants(rows *sql.Rows) ([]Grant, error) {
	var grants []Grant
	for rows.Next() {
		var g Grant
		var createdStr string
		if err := rows.Scan(&g.GrantID, &g.GrantToken, &g.KeyID, &g.GranteePrincipal, &g.RetiringPrincipal, &g.Operations, &g.Name, &g.AccountID, &createdStr); err != nil {
			return nil, err
		}
		g.CreatedAt, _ = time.Parse(time.RFC3339, createdStr)
		grants = append(grants, g)
	}
	return grants, rows.Err()
}

func (s *KMSStore) DeleteGrantByID(keyID, grantID, accountID string) error {
	result, err := s.store.DB().Exec(`DELETE FROM grants WHERE key_id = ? AND grant_id = ? AND account_id = ?`, keyID, grantID, accountID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrGrantNotFound
	}
	return nil
}

func (s *KMSStore) DeleteGrantByToken(grantToken string) error {
	result, err := s.store.DB().Exec(`DELETE FROM grants WHERE grant_token = ?`, grantToken)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrGrantNotFound
	}
	return nil
}

// --- Key Policies ---

func (s *KMSStore) PutKeyPolicy(keyID, policyName, policy, accountID string) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO key_policies (key_id, policy_name, policy, account_id) VALUES (?, ?, ?, ?)
		 ON CONFLICT(key_id, policy_name, account_id) DO UPDATE SET policy = excluded.policy`,
		keyID, policyName, policy, accountID,
	)
	return err
}

func (s *KMSStore) GetKeyPolicy(keyID, policyName, accountID string) (string, error) {
	var policy string
	err := s.store.DB().QueryRow(
		`SELECT policy FROM key_policies WHERE key_id = ? AND policy_name = ? AND account_id = ?`,
		keyID, policyName, accountID,
	).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return policy, err
}
