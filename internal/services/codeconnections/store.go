// SPDX-License-Identifier: Apache-2.0

// internal/services/codeconnections/store.go
package codeconnections

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errConnectionNotFound = errors.New("connection not found")
	errHostNotFound       = errors.New("host not found")
	errRepoLinkNotFound   = errors.New("repository link not found")
	errSyncConfigNotFound = errors.New("sync configuration not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS connections (
			arn             TEXT PRIMARY KEY,
			connection_name TEXT NOT NULL,
			provider_type   TEXT NOT NULL DEFAULT 'GitHub',
			owner_account   TEXT NOT NULL,
			host_arn        TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS hosts (
			arn            TEXT PRIMARY KEY,
			name           TEXT NOT NULL,
			provider_type  TEXT NOT NULL DEFAULT 'GitHubEnterpriseServer',
			provider_endpoint TEXT NOT NULL DEFAULT '',
			status         TEXT NOT NULL DEFAULT 'AVAILABLE',
			vpc_config     TEXT NOT NULL DEFAULT '{}',
			account_id     TEXT NOT NULL,
			created_at     INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS repository_links (
			arn                 TEXT PRIMARY KEY,
			repo_link_id        TEXT NOT NULL,
			connection_arn      TEXT NOT NULL,
			owner_id            TEXT NOT NULL,
			repository_name     TEXT NOT NULL,
			encryption_key_arn  TEXT NOT NULL DEFAULT '',
			account_id          TEXT NOT NULL,
			created_at          INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS sync_configurations (
			resource_name       TEXT NOT NULL,
			sync_type           TEXT NOT NULL,
			branch              TEXT NOT NULL DEFAULT 'main',
			config_file         TEXT NOT NULL DEFAULT '',
			repo_link_id        TEXT NOT NULL,
			role_arn            TEXT NOT NULL DEFAULT '',
			publish_deployment_status TEXT NOT NULL DEFAULT 'DISABLED',
			trigger_resource_update_on TEXT NOT NULL DEFAULT 'ANY_CHANGE',
			account_id          TEXT NOT NULL,
			created_at          INTEGER NOT NULL,
			PRIMARY KEY (resource_name, sync_type)
		);
		CREATE TABLE IF NOT EXISTS cc_tags (
			resource_arn TEXT NOT NULL,
			tag_key      TEXT NOT NULL,
			tag_value    TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (resource_arn, tag_key)
		);
	`},
}

type Connection struct {
	ARN          string
	Name         string
	ProviderType string
	OwnerAccount string
	HostARN      string
	Status       string
	CreatedAt    time.Time
}

type Host struct {
	ARN              string
	Name             string
	ProviderType     string
	ProviderEndpoint string
	Status           string
	VpcConfig        string
	AccountID        string
	CreatedAt        time.Time
}

type RepositoryLink struct {
	ARN              string
	RepoLinkID       string
	ConnectionARN    string
	OwnerID          string
	RepositoryName   string
	EncryptionKeyARN string
	AccountID        string
	CreatedAt        time.Time
}

type SyncConfiguration struct {
	ResourceName            string
	SyncType                string
	Branch                  string
	ConfigFile              string
	RepoLinkID              string
	RoleARN                 string
	PublishDeploymentStatus string
	TriggerResourceUpdateOn string
	AccountID               string
	CreatedAt               time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "codeconnections.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Connections ---

func (s *Store) CreateConnection(c *Connection) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO connections (arn, connection_name, provider_type, owner_account, host_arn, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.ARN, c.Name, c.ProviderType, c.OwnerAccount, c.HostARN, c.Status, c.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetConnection(arn string) (*Connection, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, connection_name, provider_type, owner_account, host_arn, status, created_at
		 FROM connections WHERE arn = ?`, arn)
	var c Connection
	var createdAt int64
	err := row.Scan(&c.ARN, &c.Name, &c.ProviderType, &c.OwnerAccount, &c.HostARN, &c.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConnectionNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func (s *Store) ListConnections() ([]Connection, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, connection_name, provider_type, owner_account, host_arn, status, created_at
		 FROM connections ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Connection
	for rows.Next() {
		var c Connection
		var createdAt int64
		if err := rows.Scan(&c.ARN, &c.Name, &c.ProviderType, &c.OwnerAccount, &c.HostARN, &c.Status, &createdAt); err != nil {
			return nil, err
		}
		c.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) DeleteConnection(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM connections WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConnectionNotFound
	}
	return nil
}

// --- Hosts ---

func (s *Store) CreateHost(h *Host) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO hosts (arn, name, provider_type, provider_endpoint, status, vpc_config, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		h.ARN, h.Name, h.ProviderType, h.ProviderEndpoint, h.Status, h.VpcConfig, h.AccountID, h.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetHost(arn string) (*Host, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, provider_type, provider_endpoint, status, vpc_config, account_id, created_at
		 FROM hosts WHERE arn = ?`, arn)
	var h Host
	var createdAt int64
	err := row.Scan(&h.ARN, &h.Name, &h.ProviderType, &h.ProviderEndpoint, &h.Status, &h.VpcConfig, &h.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errHostNotFound
		}
		return nil, err
	}
	h.CreatedAt = time.Unix(createdAt, 0)
	return &h, nil
}

func (s *Store) ListHosts() ([]Host, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, provider_type, provider_endpoint, status, vpc_config, account_id, created_at
		 FROM hosts ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Host
	for rows.Next() {
		var h Host
		var createdAt int64
		if err := rows.Scan(&h.ARN, &h.Name, &h.ProviderType, &h.ProviderEndpoint, &h.Status, &h.VpcConfig, &h.AccountID, &createdAt); err != nil {
			return nil, err
		}
		h.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *Store) DeleteHost(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM hosts WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHostNotFound
	}
	return nil
}

func (s *Store) UpdateHost(arn, providerEndpoint, vpcConfig string) error {
	res, err := s.store.DB().Exec(
		`UPDATE hosts SET provider_endpoint=?, vpc_config=? WHERE arn=?`,
		providerEndpoint, vpcConfig, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHostNotFound
	}
	return nil
}

// --- Repository Links ---

func (s *Store) CreateRepoLink(r *RepositoryLink) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO repository_links (arn, repo_link_id, connection_arn, owner_id, repository_name, encryption_key_arn, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ARN, r.RepoLinkID, r.ConnectionARN, r.OwnerID, r.RepositoryName, r.EncryptionKeyARN, r.AccountID, r.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetRepoLink(id string) (*RepositoryLink, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, repo_link_id, connection_arn, owner_id, repository_name, encryption_key_arn, account_id, created_at
		 FROM repository_links WHERE repo_link_id = ? OR arn = ?`, id, id)
	var r RepositoryLink
	var createdAt int64
	err := row.Scan(&r.ARN, &r.RepoLinkID, &r.ConnectionARN, &r.OwnerID, &r.RepositoryName, &r.EncryptionKeyARN, &r.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRepoLinkNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func (s *Store) ListRepoLinks() ([]RepositoryLink, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, repo_link_id, connection_arn, owner_id, repository_name, encryption_key_arn, account_id, created_at
		 FROM repository_links ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RepositoryLink
	for rows.Next() {
		var r RepositoryLink
		var createdAt int64
		if err := rows.Scan(&r.ARN, &r.RepoLinkID, &r.ConnectionARN, &r.OwnerID, &r.RepositoryName, &r.EncryptionKeyARN, &r.AccountID, &createdAt); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteRepoLink(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM repository_links WHERE repo_link_id = ? OR arn = ?`, id, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRepoLinkNotFound
	}
	return nil
}

// --- Sync Configurations ---

func (s *Store) CreateSyncConfig(c *SyncConfiguration) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO sync_configurations (resource_name, sync_type, branch, config_file, repo_link_id, role_arn, publish_deployment_status, trigger_resource_update_on, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ResourceName, c.SyncType, c.Branch, c.ConfigFile, c.RepoLinkID, c.RoleARN,
		c.PublishDeploymentStatus, c.TriggerResourceUpdateOn, c.AccountID, c.CreatedAt.Unix(),
	)
	return err
}

func (s *Store) GetSyncConfig(resourceName, syncType string) (*SyncConfiguration, error) {
	row := s.store.DB().QueryRow(
		`SELECT resource_name, sync_type, branch, config_file, repo_link_id, role_arn, publish_deployment_status, trigger_resource_update_on, account_id, created_at
		 FROM sync_configurations WHERE resource_name = ? AND sync_type = ?`, resourceName, syncType)
	var c SyncConfiguration
	var createdAt int64
	err := row.Scan(&c.ResourceName, &c.SyncType, &c.Branch, &c.ConfigFile, &c.RepoLinkID, &c.RoleARN,
		&c.PublishDeploymentStatus, &c.TriggerResourceUpdateOn, &c.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSyncConfigNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func (s *Store) ListSyncConfigs(syncType string) ([]SyncConfiguration, error) {
	var rows *sql.Rows
	var err error
	if syncType == "" {
		rows, err = s.store.DB().Query(
			`SELECT resource_name, sync_type, branch, config_file, repo_link_id, role_arn, publish_deployment_status, trigger_resource_update_on, account_id, created_at
			 FROM sync_configurations ORDER BY created_at`)
	} else {
		rows, err = s.store.DB().Query(
			`SELECT resource_name, sync_type, branch, config_file, repo_link_id, role_arn, publish_deployment_status, trigger_resource_update_on, account_id, created_at
			 FROM sync_configurations WHERE sync_type = ? ORDER BY created_at`, syncType)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SyncConfiguration
	for rows.Next() {
		var c SyncConfiguration
		var createdAt int64
		if err := rows.Scan(&c.ResourceName, &c.SyncType, &c.Branch, &c.ConfigFile, &c.RepoLinkID, &c.RoleARN,
			&c.PublishDeploymentStatus, &c.TriggerResourceUpdateOn, &c.AccountID, &createdAt); err != nil {
			return nil, err
		}
		c.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSyncConfig(resourceName, syncType string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM sync_configurations WHERE resource_name = ? AND sync_type = ?`, resourceName, syncType)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSyncConfigNotFound
	}
	return nil
}

// --- Tags ---

func (s *Store) PutTags(arn string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO cc_tags (resource_arn, tag_key, tag_value) VALUES (?, ?, ?)
			 ON CONFLICT(resource_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			arn, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetTags(arn string) (map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT tag_key, tag_value FROM cc_tags WHERE resource_arn = ?`, arn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
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

func (s *Store) DeleteTags(arn string, keys []string) error {
	for _, k := range keys {
		_, err := s.store.DB().Exec(`DELETE FROM cc_tags WHERE resource_arn = ? AND tag_key = ?`, arn, k)
		if err != nil {
			return err
		}
	}
	return nil
}
