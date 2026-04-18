// SPDX-License-Identifier: Apache-2.0

// internal/services/ram/store.go
package ram

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errShareNotFound      = errors.New("resource share not found")
	errPermissionNotFound = errors.New("permission not found")
	errInvitationNotFound = errors.New("invitation not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS resource_shares (
			arn             TEXT PRIMARY KEY,
			name            TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'ACTIVE',
			owner           TEXT NOT NULL DEFAULT '000000000000',
			allow_external  INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL,
			updated_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS share_associations (
			arn              TEXT PRIMARY KEY,
			share_arn        TEXT NOT NULL,
			associated_entity TEXT NOT NULL,
			type             TEXT NOT NULL DEFAULT 'RESOURCE',
			status           TEXT NOT NULL DEFAULT 'ASSOCIATED',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS share_invitations (
			arn       TEXT PRIMARY KEY,
			share_arn TEXT NOT NULL,
			sender    TEXT NOT NULL DEFAULT '000000000000',
			receiver  TEXT NOT NULL DEFAULT '',
			status    TEXT NOT NULL DEFAULT 'PENDING',
			created_at INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS permissions (
			arn           TEXT PRIMARY KEY,
			name          TEXT NOT NULL,
			resource_type TEXT NOT NULL DEFAULT '',
			version       TEXT NOT NULL DEFAULT '1',
			is_default    INTEGER NOT NULL DEFAULT 0,
			status        TEXT NOT NULL DEFAULT 'ATTACHABLE',
			created_at    INTEGER NOT NULL
		);
	`},
}

type ResourceShare struct {
	ARN           string
	Name          string
	Status        string
	Owner         string
	AllowExternal bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ShareAssociation struct {
	ARN              string
	ShareARN         string
	AssociatedEntity string
	Type             string
	Status           string
	CreatedAt        time.Time
}

type ShareInvitation struct {
	ARN       string
	ShareARN  string
	Sender    string
	Receiver  string
	Status    string
	CreatedAt time.Time
}

type Permission struct {
	ARN          string
	Name         string
	ResourceType string
	Version      string
	IsDefault    bool
	Status       string
	CreatedAt    time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "ram.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- ResourceShare ---

func (s *Store) CreateShare(rs *ResourceShare) error {
	now := time.Now().Unix()
	ext := 0
	if rs.AllowExternal {
		ext = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO resource_shares (arn, name, status, owner, allow_external, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		rs.ARN, rs.Name, rs.Status, rs.Owner, ext, now, now,
	)
	return err
}

func (s *Store) GetShare(arn string) (*ResourceShare, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, status, owner, allow_external, created_at, updated_at
		 FROM resource_shares WHERE arn = ?`, arn)
	return scanShare(row)
}

func (s *Store) GetShareByName(name string) (*ResourceShare, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, status, owner, allow_external, created_at, updated_at
		 FROM resource_shares WHERE name = ?`, name)
	return scanShare(row)
}

func (s *Store) ListShares(status string) ([]ResourceShare, error) {
	query := `SELECT arn, name, status, owner, allow_external, created_at, updated_at FROM resource_shares`
	var args []any
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var shares []ResourceShare
	for rows.Next() {
		rs, err := scanShare(rows)
		if err != nil {
			return nil, err
		}
		shares = append(shares, *rs)
	}
	return shares, rows.Err()
}

func (s *Store) UpdateShare(arn string, fields map[string]any) error {
	rs, err := s.GetShare(arn)
	if err != nil {
		return errShareNotFound
	}
	if v, ok := fields["name"].(string); ok && v != "" {
		rs.Name = v
	}
	if v, ok := fields["allowExternalPrincipals"].(bool); ok {
		rs.AllowExternal = v
	}
	now := time.Now().Unix()
	ext := 0
	if rs.AllowExternal {
		ext = 1
	}
	res, err := s.store.DB().Exec(
		`UPDATE resource_shares SET name=?, allow_external=?, updated_at=? WHERE arn=?`,
		rs.Name, ext, now, arn,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errShareNotFound
	}
	return nil
}

func (s *Store) DeleteShare(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM resource_shares WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errShareNotFound
	}
	return nil
}

// --- Associations ---

func (s *Store) AddAssociation(a *ShareAssociation) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO share_associations (arn, share_arn, associated_entity, type, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(arn) DO UPDATE SET status=excluded.status`,
		a.ARN, a.ShareARN, a.AssociatedEntity, a.Type, a.Status, now,
	)
	return err
}

func (s *Store) ListAssociations(shareARN, assocType string) ([]ShareAssociation, error) {
	query := `SELECT arn, share_arn, associated_entity, type, status, created_at FROM share_associations WHERE 1=1`
	var args []any
	if shareARN != "" {
		query += ` AND share_arn = ?`
		args = append(args, shareARN)
	}
	if assocType != "" {
		query += ` AND type = ?`
		args = append(args, assocType)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var assocs []ShareAssociation
	for rows.Next() {
		var a ShareAssociation
		var createdAt int64
		if err := rows.Scan(&a.ARN, &a.ShareARN, &a.AssociatedEntity, &a.Type, &a.Status, &createdAt); err != nil {
			return nil, err
		}
		a.CreatedAt = time.Unix(createdAt, 0)
		assocs = append(assocs, a)
	}
	return assocs, rows.Err()
}

func (s *Store) DeleteAssociation(shareARN, entity string) error {
	_, err := s.store.DB().Exec(
		`DELETE FROM share_associations WHERE share_arn = ? AND associated_entity = ?`,
		shareARN, entity,
	)
	return err
}

// --- Invitations ---

func (s *Store) CreateInvitation(inv *ShareInvitation) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO share_invitations (arn, share_arn, sender, receiver, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		inv.ARN, inv.ShareARN, inv.Sender, inv.Receiver, inv.Status, now,
	)
	return err
}

func (s *Store) GetInvitation(arn string) (*ShareInvitation, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, share_arn, sender, receiver, status, created_at
		 FROM share_invitations WHERE arn = ?`, arn)
	return scanInvitation(row)
}

func (s *Store) ListInvitations(shareARN string) ([]ShareInvitation, error) {
	query := `SELECT arn, share_arn, sender, receiver, status, created_at FROM share_invitations`
	var args []any
	if shareARN != "" {
		query += ` WHERE share_arn = ?`
		args = append(args, shareARN)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var invs []ShareInvitation
	for rows.Next() {
		inv, err := scanInvitation(rows)
		if err != nil {
			return nil, err
		}
		invs = append(invs, *inv)
	}
	return invs, rows.Err()
}

func (s *Store) UpdateInvitationStatus(arn, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE share_invitations SET status = ? WHERE arn = ?`, status, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errInvitationNotFound
	}
	return nil
}

// --- Permissions ---

func (s *Store) CreatePermission(p *Permission) error {
	now := time.Now().Unix()
	isDef := 0
	if p.IsDefault {
		isDef = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO permissions (arn, name, resource_type, version, is_default, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.ARN, p.Name, p.ResourceType, p.Version, isDef, p.Status, now,
	)
	return err
}

func (s *Store) GetPermission(arn string) (*Permission, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, resource_type, version, is_default, status, created_at
		 FROM permissions WHERE arn = ?`, arn)
	return scanPermission(row)
}

func (s *Store) ListPermissions(resourceType string) ([]Permission, error) {
	query := `SELECT arn, name, resource_type, version, is_default, status, created_at FROM permissions`
	var args []any
	if resourceType != "" {
		query += ` WHERE resource_type = ?`
		args = append(args, resourceType)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var perms []Permission
	for rows.Next() {
		perm, err := scanPermission(rows)
		if err != nil {
			return nil, err
		}
		perms = append(perms, *perm)
	}
	return perms, rows.Err()
}

func (s *Store) DeletePermission(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM permissions WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPermissionNotFound
	}
	return nil
}

// --- scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanShare(sc scanner) (*ResourceShare, error) {
	var rs ResourceShare
	var ext int
	var createdAt, updatedAt int64
	err := sc.Scan(&rs.ARN, &rs.Name, &rs.Status, &rs.Owner, &ext, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errShareNotFound
		}
		return nil, err
	}
	rs.AllowExternal = ext != 0
	rs.CreatedAt = time.Unix(createdAt, 0)
	rs.UpdatedAt = time.Unix(updatedAt, 0)
	return &rs, nil
}

func scanInvitation(sc scanner) (*ShareInvitation, error) {
	var inv ShareInvitation
	var createdAt int64
	err := sc.Scan(&inv.ARN, &inv.ShareARN, &inv.Sender, &inv.Receiver, &inv.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInvitationNotFound
		}
		return nil, err
	}
	inv.CreatedAt = time.Unix(createdAt, 0)
	return &inv, nil
}

func scanPermission(sc scanner) (*Permission, error) {
	var p Permission
	var isDef int
	var createdAt int64
	err := sc.Scan(&p.ARN, &p.Name, &p.ResourceType, &p.Version, &isDef, &p.Status, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPermissionNotFound
		}
		return nil, err
	}
	p.IsDefault = isDef != 0
	p.CreatedAt = time.Unix(createdAt, 0)
	return &p, nil
}
