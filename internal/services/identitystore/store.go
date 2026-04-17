// SPDX-License-Identifier: Apache-2.0

// internal/services/identitystore/store.go
package identitystore

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errUserNotFound            = errors.New("user not found")
	errGroupNotFound           = errors.New("group not found")
	errGroupMembershipNotFound = errors.New("group membership not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS identity_store_users (
			user_id          TEXT PRIMARY KEY,
			identity_store_id TEXT NOT NULL,
			user_name        TEXT NOT NULL,
			display_name     TEXT NOT NULL DEFAULT '',
			email            TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			UNIQUE(identity_store_id, user_name)
		);
		CREATE TABLE IF NOT EXISTS identity_store_groups (
			group_id         TEXT PRIMARY KEY,
			identity_store_id TEXT NOT NULL,
			display_name     TEXT NOT NULL,
			description      TEXT NOT NULL DEFAULT '',
			created_at       INTEGER NOT NULL,
			UNIQUE(identity_store_id, display_name)
		);
		CREATE TABLE IF NOT EXISTS identity_store_memberships (
			membership_id    TEXT PRIMARY KEY,
			identity_store_id TEXT NOT NULL,
			group_id         TEXT NOT NULL,
			user_id          TEXT NOT NULL,
			created_at       INTEGER NOT NULL,
			UNIQUE(identity_store_id, group_id, user_id)
		);
	`},
}

// User represents an Identity Store user.
type User struct {
	UserID          string
	IdentityStoreID string
	UserName        string
	DisplayName     string
	Email           string
	CreatedAt       time.Time
}

// Group represents an Identity Store group.
type Group struct {
	GroupID         string
	IdentityStoreID string
	DisplayName     string
	Description     string
	CreatedAt       time.Time
}

// GroupMembership represents a group membership.
type GroupMembership struct {
	MembershipID    string
	IdentityStoreID string
	GroupID         string
	UserID          string
	CreatedAt       time.Time
}

// Store manages Identity Store data.
type Store struct {
	store *sqlite.Store
}

// NewStore opens (or creates) the SQLite database for Identity Store.
func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "identitystore.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error { return s.store.Close() }

// --- User operations ---

func (s *Store) CreateUser(identityStoreID, userName, displayName, email string) (*User, error) {
	userID := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO identity_store_users (user_id, identity_store_id, user_name, display_name, email, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		userID, identityStoreID, userName, displayName, email, now,
	)
	if err != nil {
		return nil, err
	}
	return &User{
		UserID: userID, IdentityStoreID: identityStoreID,
		UserName: userName, DisplayName: displayName,
		Email: email, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetUser(identityStoreID, userID string) (*User, error) {
	row := s.store.DB().QueryRow(
		`SELECT user_id, identity_store_id, user_name, display_name, email, created_at
		 FROM identity_store_users WHERE identity_store_id=? AND user_id=?`,
		identityStoreID, userID,
	)
	return scanUser(row)
}

func (s *Store) ListUsers(identityStoreID string) ([]User, error) {
	rows, err := s.store.DB().Query(
		`SELECT user_id, identity_store_id, user_name, display_name, email, created_at
		 FROM identity_store_users WHERE identity_store_id=? ORDER BY created_at`,
		identityStoreID,
	)
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

func (s *Store) GetUserByName(identityStoreID, userName string) (*User, error) {
	row := s.store.DB().QueryRow(
		`SELECT user_id, identity_store_id, user_name, display_name, email, created_at
		 FROM identity_store_users WHERE identity_store_id=? AND user_name=?`,
		identityStoreID, userName,
	)
	return scanUser(row)
}

func (s *Store) UpdateUser(identityStoreID, userID, displayName, email string) error {
	res, err := s.store.DB().Exec(
		`UPDATE identity_store_users SET display_name=?, email=? WHERE identity_store_id=? AND user_id=?`,
		displayName, email, identityStoreID, userID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserNotFound
	}
	return nil
}

func (s *Store) DeleteUser(identityStoreID, userID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM identity_store_users WHERE identity_store_id=? AND user_id=?`,
		identityStoreID, userID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserNotFound
	}
	return nil
}

// --- Group operations ---

func (s *Store) CreateGroup(identityStoreID, displayName, description string) (*Group, error) {
	groupID := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO identity_store_groups (group_id, identity_store_id, display_name, description, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		groupID, identityStoreID, displayName, description, now,
	)
	if err != nil {
		return nil, err
	}
	return &Group{
		GroupID: groupID, IdentityStoreID: identityStoreID,
		DisplayName: displayName, Description: description,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetGroup(identityStoreID, groupID string) (*Group, error) {
	row := s.store.DB().QueryRow(
		`SELECT group_id, identity_store_id, display_name, description, created_at
		 FROM identity_store_groups WHERE identity_store_id=? AND group_id=?`,
		identityStoreID, groupID,
	)
	return scanGroup(row)
}

func (s *Store) ListGroups(identityStoreID string) ([]Group, error) {
	rows, err := s.store.DB().Query(
		`SELECT group_id, identity_store_id, display_name, description, created_at
		 FROM identity_store_groups WHERE identity_store_id=? ORDER BY created_at`,
		identityStoreID,
	)
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

func (s *Store) GetGroupByName(identityStoreID, displayName string) (*Group, error) {
	row := s.store.DB().QueryRow(
		`SELECT group_id, identity_store_id, display_name, description, created_at
		 FROM identity_store_groups WHERE identity_store_id=? AND display_name=?`,
		identityStoreID, displayName,
	)
	return scanGroup(row)
}

func (s *Store) UpdateGroup(identityStoreID, groupID, displayName, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE identity_store_groups SET display_name=?, description=? WHERE identity_store_id=? AND group_id=?`,
		displayName, description, identityStoreID, groupID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

func (s *Store) DeleteGroup(identityStoreID, groupID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM identity_store_groups WHERE identity_store_id=? AND group_id=?`,
		identityStoreID, groupID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupNotFound
	}
	return nil
}

// --- GroupMembership operations ---

func (s *Store) CreateGroupMembership(identityStoreID, groupID, userID string) (*GroupMembership, error) {
	membershipID := shared.GenerateUUID()
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO identity_store_memberships (membership_id, identity_store_id, group_id, user_id, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		membershipID, identityStoreID, groupID, userID, now,
	)
	if err != nil {
		return nil, err
	}
	return &GroupMembership{
		MembershipID: membershipID, IdentityStoreID: identityStoreID,
		GroupID: groupID, UserID: userID,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) DeleteGroupMembership(identityStoreID, membershipID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM identity_store_memberships WHERE identity_store_id=? AND membership_id=?`,
		identityStoreID, membershipID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errGroupMembershipNotFound
	}
	return nil
}

func (s *Store) GetGroupMembership(identityStoreID, membershipID string) (*GroupMembership, error) {
	row := s.store.DB().QueryRow(
		`SELECT membership_id, identity_store_id, group_id, user_id, created_at
		 FROM identity_store_memberships WHERE identity_store_id=? AND membership_id=?`,
		identityStoreID, membershipID,
	)
	var m GroupMembership
	var createdAt int64
	if err := row.Scan(&m.MembershipID, &m.IdentityStoreID, &m.GroupID, &m.UserID, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupMembershipNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

func (s *Store) GetGroupMembershipByGroupUser(identityStoreID, groupID, userID string) (*GroupMembership, error) {
	row := s.store.DB().QueryRow(
		`SELECT membership_id, identity_store_id, group_id, user_id, created_at
		 FROM identity_store_memberships WHERE identity_store_id=? AND group_id=? AND user_id=?`,
		identityStoreID, groupID, userID,
	)
	var m GroupMembership
	var createdAt int64
	if err := row.Scan(&m.MembershipID, &m.IdentityStoreID, &m.GroupID, &m.UserID, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupMembershipNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

func (s *Store) ListMembershipsForMember(identityStoreID, userID string) ([]GroupMembership, error) {
	rows, err := s.store.DB().Query(
		`SELECT membership_id, identity_store_id, group_id, user_id, created_at
		 FROM identity_store_memberships WHERE identity_store_id=? AND user_id=? ORDER BY created_at`,
		identityStoreID, userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var memberships []GroupMembership
	for rows.Next() {
		var m GroupMembership
		var createdAt int64
		if err := rows.Scan(&m.MembershipID, &m.IdentityStoreID, &m.GroupID, &m.UserID, &createdAt); err != nil {
			return nil, err
		}
		m.CreatedAt = time.Unix(createdAt, 0)
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

func (s *Store) ListGroupMemberships(identityStoreID, groupID string) ([]GroupMembership, error) {
	rows, err := s.store.DB().Query(
		`SELECT membership_id, identity_store_id, group_id, user_id, created_at
		 FROM identity_store_memberships WHERE identity_store_id=? AND group_id=? ORDER BY created_at`,
		identityStoreID, groupID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var memberships []GroupMembership
	for rows.Next() {
		var m GroupMembership
		var createdAt int64
		if err := rows.Scan(&m.MembershipID, &m.IdentityStoreID, &m.GroupID, &m.UserID, &createdAt); err != nil {
			return nil, err
		}
		m.CreatedAt = time.Unix(createdAt, 0)
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

type storeScanner interface{ Scan(dest ...any) error }

func scanUser(s storeScanner) (*User, error) {
	var u User
	var createdAt int64
	err := s.Scan(&u.UserID, &u.IdentityStoreID, &u.UserName, &u.DisplayName, &u.Email, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUserNotFound
		}
		return nil, err
	}
	u.CreatedAt = time.Unix(createdAt, 0)
	return &u, nil
}

func scanGroup(s storeScanner) (*Group, error) {
	var g Group
	var createdAt int64
	err := s.Scan(&g.GroupID, &g.IdentityStoreID, &g.DisplayName, &g.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errGroupNotFound
		}
		return nil, err
	}
	g.CreatedAt = time.Unix(createdAt, 0)
	return &g, nil
}
