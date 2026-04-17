// SPDX-License-Identifier: Apache-2.0

// internal/services/transfer/store.go
package transfer

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errServerNotFound  = errors.New("server not found")
	errHostKeyNotFound = errors.New("host key not found")
	errAccessNotFound  = errors.New("access not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS servers (
			id                TEXT PRIMARY KEY,
			arn               TEXT NOT NULL UNIQUE,
			state             TEXT NOT NULL DEFAULT 'ONLINE',
			protocols         TEXT NOT NULL DEFAULT '["SFTP"]',
			endpoint_type     TEXT NOT NULL DEFAULT 'PUBLIC',
			identity_provider TEXT NOT NULL DEFAULT '{}',
			logging_role      TEXT NOT NULL DEFAULT '',
			domain            TEXT NOT NULL DEFAULT 'S3',
			created_at        INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS host_keys (
			id          TEXT PRIMARY KEY,
			arn         TEXT NOT NULL UNIQUE,
			server_id   TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			fingerprint TEXT NOT NULL DEFAULT '',
			type        TEXT NOT NULL DEFAULT 'ssh-rsa',
			created_at  INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS accesses (
			server_id   TEXT NOT NULL,
			external_id TEXT NOT NULL,
			role        TEXT NOT NULL DEFAULT '',
			home_directory TEXT NOT NULL DEFAULT '/',
			policy      TEXT NOT NULL DEFAULT '',
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (server_id, external_id)
		);
	`},
}

type Server struct {
	ID               string
	ARN              string
	State            string
	Protocols        string
	EndpointType     string
	IdentityProvider string
	LoggingRole      string
	Domain           string
	CreatedAt        time.Time
}

type HostKey struct {
	ID          string
	ARN         string
	ServerID    string
	Description string
	Fingerprint string
	Type        string
	CreatedAt   time.Time
}

type Access struct {
	ServerID      string
	ExternalID    string
	Role          string
	HomeDirectory string
	Policy        string
	CreatedAt     time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "transfer.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// Server CRUD

func (s *Store) CreateServer(id, arn, protocols, endpointType, identityProvider, loggingRole, domain string) (*Server, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO servers (id, arn, state, protocols, endpoint_type, identity_provider, logging_role, domain, created_at)
		 VALUES (?, ?, 'ONLINE', ?, ?, ?, ?, ?, ?)`,
		id, arn, protocols, endpointType, identityProvider, loggingRole, domain, now,
	)
	if err != nil {
		return nil, err
	}
	return &Server{
		ID: id, ARN: arn, State: "ONLINE", Protocols: protocols,
		EndpointType: endpointType, IdentityProvider: identityProvider,
		LoggingRole: loggingRole, Domain: domain, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetServer(id string) (*Server, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, state, protocols, endpoint_type, identity_provider, logging_role, domain, created_at
		 FROM servers WHERE id = ?`, id)
	return scanServer(row)
}

func (s *Store) ListServers() ([]Server, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, state, protocols, endpoint_type, identity_provider, logging_role, domain, created_at
		 FROM servers ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Server
	for rows.Next() {
		srv, err := scanServer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *srv)
	}
	return out, rows.Err()
}

func (s *Store) UpdateServerState(id, state string) error {
	res, err := s.store.DB().Exec(`UPDATE servers SET state = ? WHERE id = ?`, state, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errServerNotFound
	}
	return nil
}

func (s *Store) DeleteServer(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM servers WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errServerNotFound
	}
	return nil
}

// HostKey CRUD

func (s *Store) CreateHostKey(id, arn, serverID, description, fingerprint, keyType string) (*HostKey, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO host_keys (id, arn, server_id, description, fingerprint, type, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, arn, serverID, description, fingerprint, keyType, now,
	)
	if err != nil {
		return nil, err
	}
	return &HostKey{
		ID: id, ARN: arn, ServerID: serverID, Description: description,
		Fingerprint: fingerprint, Type: keyType, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetHostKey(id, serverID string) (*HostKey, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, server_id, description, fingerprint, type, created_at
		 FROM host_keys WHERE id = ? AND server_id = ?`, id, serverID)
	return scanHostKey(row)
}

func (s *Store) ListHostKeys(serverID string) ([]HostKey, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, server_id, description, fingerprint, type, created_at
		 FROM host_keys WHERE server_id = ? ORDER BY created_at`, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HostKey
	for rows.Next() {
		hk, err := scanHostKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *hk)
	}
	return out, rows.Err()
}

func (s *Store) UpdateHostKey(id, serverID, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE host_keys SET description = ? WHERE id = ? AND server_id = ?`, description, id, serverID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHostKeyNotFound
	}
	return nil
}

func (s *Store) DeleteHostKey(id, serverID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM host_keys WHERE id = ? AND server_id = ?`, id, serverID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errHostKeyNotFound
	}
	return nil
}

// Access CRUD

func (s *Store) CreateAccess(serverID, externalID, role, homeDirectory, policy string) (*Access, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO accesses (server_id, external_id, role, home_directory, policy, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		serverID, externalID, role, homeDirectory, policy, now,
	)
	if err != nil {
		return nil, err
	}
	return &Access{
		ServerID: serverID, ExternalID: externalID, Role: role,
		HomeDirectory: homeDirectory, Policy: policy, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAccess(serverID, externalID string) (*Access, error) {
	row := s.store.DB().QueryRow(
		`SELECT server_id, external_id, role, home_directory, policy, created_at
		 FROM accesses WHERE server_id = ? AND external_id = ?`, serverID, externalID)
	return scanAccess(row)
}

func (s *Store) ListAccesses(serverID string) ([]Access, error) {
	rows, err := s.store.DB().Query(
		`SELECT server_id, external_id, role, home_directory, policy, created_at
		 FROM accesses WHERE server_id = ? ORDER BY created_at`, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Access
	for rows.Next() {
		a, err := scanAccess(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) UpdateAccess(serverID, externalID, role, homeDirectory, policy string) error {
	res, err := s.store.DB().Exec(
		`UPDATE accesses SET role = ?, home_directory = ?, policy = ? WHERE server_id = ? AND external_id = ?`,
		role, homeDirectory, policy, serverID, externalID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAccessNotFound
	}
	return nil
}

func (s *Store) DeleteAccess(serverID, externalID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM accesses WHERE server_id = ? AND external_id = ?`, serverID, externalID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAccessNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanServer(s scanner) (*Server, error) {
	var srv Server
	var createdAt int64
	err := s.Scan(&srv.ID, &srv.ARN, &srv.State, &srv.Protocols, &srv.EndpointType,
		&srv.IdentityProvider, &srv.LoggingRole, &srv.Domain, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errServerNotFound
		}
		return nil, err
	}
	srv.CreatedAt = time.Unix(createdAt, 0)
	return &srv, nil
}

func scanHostKey(s scanner) (*HostKey, error) {
	var hk HostKey
	var createdAt int64
	err := s.Scan(&hk.ID, &hk.ARN, &hk.ServerID, &hk.Description, &hk.Fingerprint, &hk.Type, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errHostKeyNotFound
		}
		return nil, err
	}
	hk.CreatedAt = time.Unix(createdAt, 0)
	return &hk, nil
}

func scanAccess(s scanner) (*Access, error) {
	var a Access
	var createdAt int64
	err := s.Scan(&a.ServerID, &a.ExternalID, &a.Role, &a.HomeDirectory, &a.Policy, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAccessNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}
