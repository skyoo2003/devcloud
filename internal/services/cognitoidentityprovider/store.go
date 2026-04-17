// SPDX-License-Identifier: Apache-2.0

// internal/services/cognitoidentityprovider/store.go
package cognitoidentityprovider

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrPoolNotFound           = errors.New("user pool not found")
	ErrClientNotFound         = errors.New("user pool client not found")
	ErrUserNotFound           = errors.New("user not found")
	ErrGroupNotFound          = errors.New("group not found")
	ErrIDPNotFound            = errors.New("identity provider not found")
	ErrResourceServerNotFound = errors.New("resource server not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS user_pools (
			id TEXT PRIMARY KEY,
			arn TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'ACTIVE',
			policies TEXT NOT NULL DEFAULT '{}',
			mfa_config TEXT NOT NULL DEFAULT 'OFF',
			schema_attrs TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS user_pool_clients (
			id TEXT PRIMARY KEY,
			pool_id TEXT NOT NULL,
			name TEXT NOT NULL,
			secret TEXT NOT NULL DEFAULT '',
			auth_flows TEXT NOT NULL DEFAULT '[]',
			scopes TEXT NOT NULL DEFAULT '[]',
			callback_urls TEXT NOT NULL DEFAULT '[]',
			logout_urls TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS users (
			pool_id TEXT NOT NULL,
			username TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'CONFIRMED',
			enabled INTEGER NOT NULL DEFAULT 1,
			attributes TEXT NOT NULL DEFAULT '[]',
			password TEXT NOT NULL DEFAULT '',
			mfa_settings TEXT NOT NULL DEFAULT '{}',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (pool_id, username)
		);
		CREATE TABLE IF NOT EXISTS groups (
			pool_id TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			role_arn TEXT NOT NULL DEFAULT '',
			precedence INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (pool_id, name)
		);
		CREATE TABLE IF NOT EXISTS user_group_membership (
			pool_id TEXT NOT NULL,
			username TEXT NOT NULL,
			group_name TEXT NOT NULL,
			PRIMARY KEY (pool_id, username, group_name)
		);
		CREATE TABLE IF NOT EXISTS identity_providers (
			pool_id TEXT NOT NULL,
			name TEXT NOT NULL,
			type TEXT NOT NULL DEFAULT 'SAML',
			details TEXT NOT NULL DEFAULT '{}',
			attribute_mapping TEXT NOT NULL DEFAULT '{}',
			idp_identifiers TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL,
			PRIMARY KEY (pool_id, name)
		);
		CREATE TABLE IF NOT EXISTS resource_servers (
			pool_id TEXT NOT NULL,
			identifier TEXT NOT NULL,
			name TEXT NOT NULL,
			scopes TEXT NOT NULL DEFAULT '[]',
			PRIMARY KEY (pool_id, identifier)
		);
		CREATE TABLE IF NOT EXISTS pool_tags (
			pool_arn TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (pool_arn, key)
		);
	`},
}

// --- models ---

type UserPool struct {
	ID          string
	ARN         string
	Name        string
	Status      string
	Policies    string
	MFAConfig   string
	SchemaAttrs string
	CreatedAt   int64
	UpdatedAt   int64
}

type UserPoolClient struct {
	ID           string
	PoolID       string
	Name         string
	Secret       string
	AuthFlows    string
	Scopes       string
	CallbackURLs string
	LogoutURLs   string
	CreatedAt    int64
}

type User struct {
	PoolID      string
	Username    string
	Status      string
	Enabled     int
	Attributes  string
	Password    string
	MFASettings string
	CreatedAt   int64
	UpdatedAt   int64
}

type Group struct {
	PoolID      string
	Name        string
	Description string
	RoleARN     string
	Precedence  int
	CreatedAt   int64
}

type IdentityProvider struct {
	PoolID           string
	Name             string
	Type             string
	Details          string
	AttributeMapping string
	IDPIdentifiers   string
	CreatedAt        int64
}

type ResourceServer struct {
	PoolID     string
	Identifier string
	Name       string
	Scopes     string
}

// --- store ---

type CognitoStore struct {
	store *sqlite.Store
}

func NewCognitoStore(dataDir string) (*CognitoStore, error) {
	dbPath := filepath.Join(dataDir, "cognitoidentityprovider.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &CognitoStore{store: s}, nil
}

func (s *CognitoStore) Close() error { return s.store.Close() }

func (s *CognitoStore) db() *sql.DB { return s.store.DB() }

// --- UserPool ---

func (s *CognitoStore) CreateUserPool(p *UserPool) error {
	_, err := s.db().Exec(`
		INSERT INTO user_pools (id, arn, name, status, policies, mfa_config, schema_attrs, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.ARN, p.Name, p.Status, p.Policies, p.MFAConfig, p.SchemaAttrs, p.CreatedAt, p.UpdatedAt)
	return err
}

func (s *CognitoStore) GetUserPool(id string) (*UserPool, error) {
	row := s.db().QueryRow(`SELECT id, arn, name, status, policies, mfa_config, schema_attrs, created_at, updated_at FROM user_pools WHERE id = ?`, id)
	return scanUserPool(row)
}

func (s *CognitoStore) ListUserPools() ([]UserPool, error) {
	rows, err := s.db().Query(`SELECT id, arn, name, status, policies, mfa_config, schema_attrs, created_at, updated_at FROM user_pools ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pools []UserPool
	for rows.Next() {
		p, err := scanUserPool(rows)
		if err != nil {
			return nil, err
		}
		pools = append(pools, *p)
	}
	return pools, rows.Err()
}

func (s *CognitoStore) UpdateUserPool(p *UserPool) error {
	res, err := s.db().Exec(`UPDATE user_pools SET name=?, policies=?, mfa_config=?, schema_attrs=?, updated_at=? WHERE id=?`,
		p.Name, p.Policies, p.MFAConfig, p.SchemaAttrs, p.UpdatedAt, p.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPoolNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteUserPool(id string) error {
	res, err := s.db().Exec(`DELETE FROM user_pools WHERE id=?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrPoolNotFound
	}
	return nil
}

type poolScanner interface{ Scan(dest ...any) error }

func scanUserPool(r poolScanner) (*UserPool, error) {
	var p UserPool
	err := r.Scan(&p.ID, &p.ARN, &p.Name, &p.Status, &p.Policies, &p.MFAConfig, &p.SchemaAttrs, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrPoolNotFound
		}
		return nil, err
	}
	return &p, nil
}

// --- UserPoolClient ---

func (s *CognitoStore) CreateUserPoolClient(c *UserPoolClient) error {
	_, err := s.db().Exec(`
		INSERT INTO user_pool_clients (id, pool_id, name, secret, auth_flows, scopes, callback_urls, logout_urls, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.PoolID, c.Name, c.Secret, c.AuthFlows, c.Scopes, c.CallbackURLs, c.LogoutURLs, c.CreatedAt)
	return err
}

func (s *CognitoStore) GetUserPoolClient(poolID, clientID string) (*UserPoolClient, error) {
	row := s.db().QueryRow(`SELECT id, pool_id, name, secret, auth_flows, scopes, callback_urls, logout_urls, created_at FROM user_pool_clients WHERE pool_id=? AND id=?`, poolID, clientID)
	return scanClient(row)
}

func (s *CognitoStore) ListUserPoolClients(poolID string) ([]UserPoolClient, error) {
	rows, err := s.db().Query(`SELECT id, pool_id, name, secret, auth_flows, scopes, callback_urls, logout_urls, created_at FROM user_pool_clients WHERE pool_id=? ORDER BY created_at`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clients []UserPoolClient
	for rows.Next() {
		c, err := scanClient(rows)
		if err != nil {
			return nil, err
		}
		clients = append(clients, *c)
	}
	return clients, rows.Err()
}

func (s *CognitoStore) UpdateUserPoolClient(c *UserPoolClient) error {
	res, err := s.db().Exec(`UPDATE user_pool_clients SET name=?, auth_flows=?, scopes=?, callback_urls=?, logout_urls=? WHERE pool_id=? AND id=?`,
		c.Name, c.AuthFlows, c.Scopes, c.CallbackURLs, c.LogoutURLs, c.PoolID, c.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrClientNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteUserPoolClient(poolID, clientID string) error {
	res, err := s.db().Exec(`DELETE FROM user_pool_clients WHERE pool_id=? AND id=?`, poolID, clientID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrClientNotFound
	}
	return nil
}

type clientScanner interface{ Scan(dest ...any) error }

func scanClient(r clientScanner) (*UserPoolClient, error) {
	var c UserPoolClient
	err := r.Scan(&c.ID, &c.PoolID, &c.Name, &c.Secret, &c.AuthFlows, &c.Scopes, &c.CallbackURLs, &c.LogoutURLs, &c.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrClientNotFound
		}
		return nil, err
	}
	return &c, nil
}

// --- User ---

func (s *CognitoStore) CreateUser(u *User) error {
	_, err := s.db().Exec(`
		INSERT INTO users (pool_id, username, status, enabled, attributes, password, mfa_settings, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.PoolID, u.Username, u.Status, u.Enabled, u.Attributes, u.Password, u.MFASettings, u.CreatedAt, u.UpdatedAt)
	return err
}

func (s *CognitoStore) GetUser(poolID, username string) (*User, error) {
	row := s.db().QueryRow(`SELECT pool_id, username, status, enabled, attributes, password, mfa_settings, created_at, updated_at FROM users WHERE pool_id=? AND username=?`, poolID, username)
	return scanUser(row)
}

func (s *CognitoStore) ListUsers(poolID string) ([]User, error) {
	rows, err := s.db().Query(`SELECT pool_id, username, status, enabled, attributes, password, mfa_settings, created_at, updated_at FROM users WHERE pool_id=? ORDER BY username`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *u)
	}
	return users, rows.Err()
}

func (s *CognitoStore) UpdateUser(u *User) error {
	res, err := s.db().Exec(`UPDATE users SET status=?, enabled=?, attributes=?, password=?, mfa_settings=?, updated_at=? WHERE pool_id=? AND username=?`,
		u.Status, u.Enabled, u.Attributes, u.Password, u.MFASettings, u.UpdatedAt, u.PoolID, u.Username)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrUserNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteUser(poolID, username string) error {
	res, err := s.db().Exec(`DELETE FROM users WHERE pool_id=? AND username=?`, poolID, username)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrUserNotFound
	}
	return nil
}

type userScanner interface{ Scan(dest ...any) error }

func scanUser(r userScanner) (*User, error) {
	var u User
	err := r.Scan(&u.PoolID, &u.Username, &u.Status, &u.Enabled, &u.Attributes, &u.Password, &u.MFASettings, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &u, nil
}

// --- Group ---

func (s *CognitoStore) CreateGroup(g *Group) error {
	_, err := s.db().Exec(`
		INSERT INTO groups (pool_id, name, description, role_arn, precedence, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		g.PoolID, g.Name, g.Description, g.RoleARN, g.Precedence, g.CreatedAt)
	return err
}

func (s *CognitoStore) GetGroup(poolID, name string) (*Group, error) {
	row := s.db().QueryRow(`SELECT pool_id, name, description, role_arn, precedence, created_at FROM groups WHERE pool_id=? AND name=?`, poolID, name)
	return scanGroup(row)
}

func (s *CognitoStore) ListGroups(poolID string) ([]Group, error) {
	rows, err := s.db().Query(`SELECT pool_id, name, description, role_arn, precedence, created_at FROM groups WHERE pool_id=? ORDER BY name`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *CognitoStore) UpdateGroup(g *Group) error {
	res, err := s.db().Exec(`UPDATE groups SET description=?, role_arn=?, precedence=? WHERE pool_id=? AND name=?`,
		g.Description, g.RoleARN, g.Precedence, g.PoolID, g.Name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrGroupNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteGroup(poolID, name string) error {
	res, err := s.db().Exec(`DELETE FROM groups WHERE pool_id=? AND name=?`, poolID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrGroupNotFound
	}
	return nil
}

type groupScanner interface{ Scan(dest ...any) error }

func scanGroup(r groupScanner) (*Group, error) {
	var g Group
	err := r.Scan(&g.PoolID, &g.Name, &g.Description, &g.RoleARN, &g.Precedence, &g.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrGroupNotFound
		}
		return nil, err
	}
	return &g, nil
}

// --- UserGroupMembership ---

func (s *CognitoStore) AddUserToGroup(poolID, username, groupName string) error {
	_, err := s.db().Exec(`INSERT OR IGNORE INTO user_group_membership (pool_id, username, group_name) VALUES (?, ?, ?)`,
		poolID, username, groupName)
	return err
}

func (s *CognitoStore) RemoveUserFromGroup(poolID, username, groupName string) error {
	_, err := s.db().Exec(`DELETE FROM user_group_membership WHERE pool_id=? AND username=? AND group_name=?`,
		poolID, username, groupName)
	return err
}

func (s *CognitoStore) ListGroupsForUser(poolID, username string) ([]Group, error) {
	rows, err := s.db().Query(`
		SELECT g.pool_id, g.name, g.description, g.role_arn, g.precedence, g.created_at
		FROM groups g
		JOIN user_group_membership m ON m.pool_id=g.pool_id AND m.group_name=g.name
		WHERE m.pool_id=? AND m.username=? ORDER BY g.name`, poolID, username)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []Group
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *g)
	}
	return groups, rows.Err()
}

func (s *CognitoStore) ListUsersInGroup(poolID, groupName string) ([]User, error) {
	rows, err := s.db().Query(`
		SELECT u.pool_id, u.username, u.status, u.enabled, u.attributes, u.password, u.mfa_settings, u.created_at, u.updated_at
		FROM users u
		JOIN user_group_membership m ON m.pool_id=u.pool_id AND m.username=u.username
		WHERE m.pool_id=? AND m.group_name=? ORDER BY u.username`, poolID, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *u)
	}
	return users, rows.Err()
}

// --- IdentityProvider ---

func (s *CognitoStore) CreateIdentityProvider(idp *IdentityProvider) error {
	_, err := s.db().Exec(`
		INSERT INTO identity_providers (pool_id, name, type, details, attribute_mapping, idp_identifiers, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		idp.PoolID, idp.Name, idp.Type, idp.Details, idp.AttributeMapping, idp.IDPIdentifiers, idp.CreatedAt)
	return err
}

func (s *CognitoStore) GetIdentityProvider(poolID, name string) (*IdentityProvider, error) {
	row := s.db().QueryRow(`SELECT pool_id, name, type, details, attribute_mapping, idp_identifiers, created_at FROM identity_providers WHERE pool_id=? AND name=?`, poolID, name)
	return scanIDP(row)
}

func (s *CognitoStore) GetIdentityProviderByIdentifier(poolID, identifier string) (*IdentityProvider, error) {
	rows, err := s.db().Query(`SELECT pool_id, name, type, details, attribute_mapping, idp_identifiers, created_at FROM identity_providers WHERE pool_id=?`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		idp, err := scanIDP(rows)
		if err != nil {
			return nil, err
		}
		// check if identifier appears in idp_identifiers JSON array
		if containsString(idp.IDPIdentifiers, identifier) {
			return idp, nil
		}
	}
	return nil, ErrIDPNotFound
}

func (s *CognitoStore) ListIdentityProviders(poolID string) ([]IdentityProvider, error) {
	rows, err := s.db().Query(`SELECT pool_id, name, type, details, attribute_mapping, idp_identifiers, created_at FROM identity_providers WHERE pool_id=? ORDER BY name`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var idps []IdentityProvider
	for rows.Next() {
		idp, err := scanIDP(rows)
		if err != nil {
			return nil, err
		}
		idps = append(idps, *idp)
	}
	return idps, rows.Err()
}

func (s *CognitoStore) UpdateIdentityProvider(idp *IdentityProvider) error {
	res, err := s.db().Exec(`UPDATE identity_providers SET type=?, details=?, attribute_mapping=?, idp_identifiers=? WHERE pool_id=? AND name=?`,
		idp.Type, idp.Details, idp.AttributeMapping, idp.IDPIdentifiers, idp.PoolID, idp.Name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrIDPNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteIdentityProvider(poolID, name string) error {
	res, err := s.db().Exec(`DELETE FROM identity_providers WHERE pool_id=? AND name=?`, poolID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrIDPNotFound
	}
	return nil
}

type idpScanner interface{ Scan(dest ...any) error }

func scanIDP(r idpScanner) (*IdentityProvider, error) {
	var idp IdentityProvider
	err := r.Scan(&idp.PoolID, &idp.Name, &idp.Type, &idp.Details, &idp.AttributeMapping, &idp.IDPIdentifiers, &idp.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrIDPNotFound
		}
		return nil, err
	}
	return &idp, nil
}

// --- ResourceServer ---

func (s *CognitoStore) CreateResourceServer(rs *ResourceServer) error {
	_, err := s.db().Exec(`
		INSERT INTO resource_servers (pool_id, identifier, name, scopes)
		VALUES (?, ?, ?, ?)`,
		rs.PoolID, rs.Identifier, rs.Name, rs.Scopes)
	return err
}

func (s *CognitoStore) GetResourceServer(poolID, identifier string) (*ResourceServer, error) {
	row := s.db().QueryRow(`SELECT pool_id, identifier, name, scopes FROM resource_servers WHERE pool_id=? AND identifier=?`, poolID, identifier)
	return scanRS(row)
}

func (s *CognitoStore) ListResourceServers(poolID string) ([]ResourceServer, error) {
	rows, err := s.db().Query(`SELECT pool_id, identifier, name, scopes FROM resource_servers WHERE pool_id=? ORDER BY identifier`, poolID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var servers []ResourceServer
	for rows.Next() {
		rs, err := scanRS(rows)
		if err != nil {
			return nil, err
		}
		servers = append(servers, *rs)
	}
	return servers, rows.Err()
}

func (s *CognitoStore) UpdateResourceServer(rs *ResourceServer) error {
	res, err := s.db().Exec(`UPDATE resource_servers SET name=?, scopes=? WHERE pool_id=? AND identifier=?`,
		rs.Name, rs.Scopes, rs.PoolID, rs.Identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrResourceServerNotFound
	}
	return nil
}

func (s *CognitoStore) DeleteResourceServer(poolID, identifier string) error {
	res, err := s.db().Exec(`DELETE FROM resource_servers WHERE pool_id=? AND identifier=?`, poolID, identifier)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrResourceServerNotFound
	}
	return nil
}

type rsScanner interface{ Scan(dest ...any) error }

func scanRS(r rsScanner) (*ResourceServer, error) {
	var rs ResourceServer
	err := r.Scan(&rs.PoolID, &rs.Identifier, &rs.Name, &rs.Scopes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrResourceServerNotFound
		}
		return nil, err
	}
	return &rs, nil
}

// --- Tags ---

func (s *CognitoStore) TagResource(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.db().Exec(`INSERT INTO pool_tags (pool_arn, key, value) VALUES (?, ?, ?) ON CONFLICT(pool_arn, key) DO UPDATE SET value=excluded.value`,
			arn, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CognitoStore) UntagResource(arn string, keys []string) error {
	for _, k := range keys {
		_, err := s.db().Exec(`DELETE FROM pool_tags WHERE pool_arn=? AND key=?`, arn, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CognitoStore) ListTagsForResource(arn string) (map[string]string, error) {
	rows, err := s.db().Query(`SELECT key, value FROM pool_tags WHERE pool_arn=? ORDER BY key`, arn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

// --- helpers ---

// containsString checks if a JSON array string contains the given value.
func containsString(jsonArr, val string) bool {
	// Simple string search: val surrounded by quotes inside JSON array.
	needle := `"` + val + `"`
	for i := 0; i <= len(jsonArr)-len(needle); i++ {
		if jsonArr[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
