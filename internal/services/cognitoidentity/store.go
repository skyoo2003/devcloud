// SPDX-License-Identifier: Apache-2.0

package cognitoidentity

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrPoolNotFound     = errors.New("identity pool not found")
	ErrIdentityNotFound = errors.New("identity not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS identity_pools (
			id                  TEXT PRIMARY KEY,
			arn                 TEXT NOT NULL UNIQUE,
			name                TEXT NOT NULL,
			allow_unauthenticated INTEGER NOT NULL DEFAULT 0,
			developer_provider  TEXT NOT NULL DEFAULT '',
			supported_logins    TEXT NOT NULL DEFAULT '{}',
			cognito_idps        TEXT NOT NULL DEFAULT '[]',
			roles               TEXT NOT NULL DEFAULT '{}',
			created_at          INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS identities (
			id         TEXT PRIMARY KEY,
			pool_id    TEXT NOT NULL,
			logins     TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS identity_pool_tags (
			pool_arn TEXT NOT NULL,
			key      TEXT NOT NULL,
			value    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (pool_arn, key)
		);
	`},
}

type IdentityPool struct {
	ID                   string
	ARN                  string
	Name                 string
	AllowUnauthenticated bool
	DeveloperProvider    string
	SupportedLogins      map[string]string
	CognitoIDPs          []map[string]any
	Roles                map[string]string
	CreatedAt            time.Time
}

type Identity struct {
	ID        string
	PoolID    string
	Logins    []string
	CreatedAt time.Time
}

type CognitoIdentityStore struct {
	store *sqlite.Store
}

func NewCognitoIdentityStore(dataDir string) (*CognitoIdentityStore, error) {
	dbPath := filepath.Join(dataDir, "cognitoidentity.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &CognitoIdentityStore{store: s}, nil
}

func (s *CognitoIdentityStore) Close() error { return s.store.Close() }

func (s *CognitoIdentityStore) PutPool(pool *IdentityPool) error {
	supportedLogins, _ := json.Marshal(pool.SupportedLogins)
	cognitoIDPs, _ := json.Marshal(pool.CognitoIDPs)
	roles, _ := json.Marshal(pool.Roles)
	_, err := s.store.DB().Exec(`
		INSERT INTO identity_pools
		  (id, arn, name, allow_unauthenticated, developer_provider, supported_logins, cognito_idps, roles, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
		  name=excluded.name,
		  allow_unauthenticated=excluded.allow_unauthenticated,
		  developer_provider=excluded.developer_provider,
		  supported_logins=excluded.supported_logins,
		  cognito_idps=excluded.cognito_idps,
		  roles=excluded.roles`,
		pool.ID, pool.ARN, pool.Name,
		boolToInt(pool.AllowUnauthenticated),
		pool.DeveloperProvider,
		string(supportedLogins), string(cognitoIDPs), string(roles),
		pool.CreatedAt.Unix(),
	)
	return err
}

func (s *CognitoIdentityStore) GetPool(id string) (*IdentityPool, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, arn, name, allow_unauthenticated, developer_provider,
		       supported_logins, cognito_idps, roles, created_at
		FROM identity_pools WHERE id = ?`, id)
	return scanPool(row)
}

func (s *CognitoIdentityStore) GetPoolByARN(arn string) (*IdentityPool, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, arn, name, allow_unauthenticated, developer_provider,
		       supported_logins, cognito_idps, roles, created_at
		FROM identity_pools WHERE arn = ?`, arn)
	return scanPool(row)
}

func (s *CognitoIdentityStore) ListPools() ([]IdentityPool, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, arn, name, allow_unauthenticated, developer_provider,
		       supported_logins, cognito_idps, roles, created_at
		FROM identity_pools ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var pools []IdentityPool
	for rows.Next() {
		p, err := scanPool(rows)
		if err != nil {
			return nil, err
		}
		pools = append(pools, *p)
	}
	return pools, rows.Err()
}

func (s *CognitoIdentityStore) DeletePool(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM identity_pools WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPoolNotFound
	}
	return nil
}

func (s *CognitoIdentityStore) PutIdentity(identity *Identity) error {
	logins, _ := json.Marshal(identity.Logins)
	_, err := s.store.DB().Exec(`
		INSERT INTO identities (id, pool_id, logins, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET logins=excluded.logins`,
		identity.ID, identity.PoolID, string(logins), identity.CreatedAt.Unix(),
	)
	return err
}

func (s *CognitoIdentityStore) GetIdentity(id string) (*Identity, error) {
	row := s.store.DB().QueryRow(`
		SELECT id, pool_id, logins, created_at FROM identities WHERE id = ?`, id)
	return scanIdentity(row)
}

func (s *CognitoIdentityStore) ListIdentities(poolID string) ([]Identity, error) {
	rows, err := s.store.DB().Query(`
		SELECT id, pool_id, logins, created_at
		FROM identities WHERE pool_id = ? ORDER BY created_at`, poolID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var identities []Identity
	for rows.Next() {
		i, err := scanIdentity(rows)
		if err != nil {
			return nil, err
		}
		identities = append(identities, *i)
	}
	return identities, rows.Err()
}

func (s *CognitoIdentityStore) DeleteIdentities(ids []string) error {
	for _, id := range ids {
		_, err := s.store.DB().Exec(`DELETE FROM identities WHERE id = ?`, id)
		if err != nil {
			return fmt.Errorf("delete identity %q: %w", id, err)
		}
	}
	return nil
}

func (s *CognitoIdentityStore) AddTags(poolARN string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(`
			INSERT INTO identity_pool_tags (pool_arn, key, value) VALUES (?, ?, ?)
			ON CONFLICT(pool_arn, key) DO UPDATE SET value=excluded.value`,
			poolARN, k, v)
		if err != nil {
			return fmt.Errorf("add tag %q: %w", k, err)
		}
	}
	return nil
}

func (s *CognitoIdentityStore) ListTags(poolARN string) (map[string]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT key, value FROM identity_pool_tags WHERE pool_arn = ? ORDER BY key`, poolARN)
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

func (s *CognitoIdentityStore) RemoveTags(poolARN string, keys []string) error {
	for _, k := range keys {
		_, err := s.store.DB().Exec(
			`DELETE FROM identity_pool_tags WHERE pool_arn = ? AND key = ?`, poolARN, k)
		if err != nil {
			return err
		}
	}
	return nil
}

type poolScanner interface {
	Scan(dest ...any) error
}

func scanPool(s poolScanner) (*IdentityPool, error) {
	var p IdentityPool
	var allowUnauth int
	var supportedLogins, cognitoIDPs, roles string
	var createdAt int64
	err := s.Scan(
		&p.ID, &p.ARN, &p.Name, &allowUnauth, &p.DeveloperProvider,
		&supportedLogins, &cognitoIDPs, &roles, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrPoolNotFound
		}
		return nil, err
	}
	p.AllowUnauthenticated = allowUnauth != 0
	p.CreatedAt = time.Unix(createdAt, 0)
	_ = json.Unmarshal([]byte(supportedLogins), &p.SupportedLogins)
	_ = json.Unmarshal([]byte(cognitoIDPs), &p.CognitoIDPs)
	_ = json.Unmarshal([]byte(roles), &p.Roles)
	if p.SupportedLogins == nil {
		p.SupportedLogins = map[string]string{}
	}
	if p.CognitoIDPs == nil {
		p.CognitoIDPs = []map[string]any{}
	}
	if p.Roles == nil {
		p.Roles = map[string]string{}
	}
	return &p, nil
}

type identityScanner interface {
	Scan(dest ...any) error
}

func scanIdentity(s identityScanner) (*Identity, error) {
	var i Identity
	var logins string
	var createdAt int64
	err := s.Scan(&i.ID, &i.PoolID, &logins, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrIdentityNotFound
		}
		return nil, err
	}
	i.CreatedAt = time.Unix(createdAt, 0)
	_ = json.Unmarshal([]byte(logins), &i.Logins)
	if i.Logins == nil {
		i.Logins = []string{}
	}
	return &i, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
