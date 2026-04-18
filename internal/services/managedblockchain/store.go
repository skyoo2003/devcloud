// SPDX-License-Identifier: Apache-2.0

// internal/services/managedblockchain/store.go
package managedblockchain

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNetworkNotFound  = errors.New("network not found")
	errMemberNotFound   = errors.New("member not found")
	errNodeNotFound     = errors.New("node not found")
	errProposalNotFound = errors.New("proposal not found")
	errAccessorNotFound = errors.New("accessor not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS networks (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			name            TEXT NOT NULL,
			framework       TEXT NOT NULL DEFAULT 'HYPERLEDGER_FABRIC',
			framework_ver   TEXT NOT NULL DEFAULT '2.2',
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			description     TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS members (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			network_id      TEXT NOT NULL,
			name            TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			description     TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS nodes (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			network_id      TEXT NOT NULL,
			member_id       TEXT NOT NULL,
			instance_type   TEXT NOT NULL DEFAULT 'bc.t3.small',
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			availability_zone TEXT NOT NULL DEFAULT 'us-east-1a',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS proposals (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			network_id      TEXT NOT NULL,
			member_id       TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'IN_PROGRESS',
			description     TEXT NOT NULL DEFAULT '',
			actions         TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL,
			expires_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS accessors (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			type            TEXT NOT NULL DEFAULT 'BILLING_TOKEN',
			status          TEXT NOT NULL DEFAULT 'AVAILABLE',
			billing_token   TEXT NOT NULL DEFAULT '',
			network_type    TEXT NOT NULL DEFAULT 'ETHEREUM_MAINNET',
			created_at      INTEGER NOT NULL
		);
	`},
}

// --- Model structs ---

type Network struct {
	ID           string
	ARN          string
	Name         string
	Framework    string
	FrameworkVer string
	Status       string
	Description  string
	CreatedAt    time.Time
}

type Member struct {
	ID          string
	ARN         string
	NetworkID   string
	Name        string
	Status      string
	Description string
	CreatedAt   time.Time
}

type Node struct {
	ID               string
	ARN              string
	NetworkID        string
	MemberID         string
	InstanceType     string
	Status           string
	AvailabilityZone string
	CreatedAt        time.Time
}

type Proposal struct {
	ID          string
	ARN         string
	NetworkID   string
	MemberID    string
	Status      string
	Description string
	Actions     string
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

type Accessor struct {
	ID           string
	ARN          string
	Type         string
	Status       string
	BillingToken string
	NetworkType  string
	CreatedAt    time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "managedblockchain.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Networks ---

func (s *Store) CreateNetwork(n *Network) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO networks (id, arn, name, framework, framework_ver, status, description, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		n.ID, n.ARN, n.Name, n.Framework, n.FrameworkVer, n.Status, n.Description, now,
	)
	return err
}

func (s *Store) GetNetwork(id string) (*Network, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, framework, framework_ver, status, description, created_at
		 FROM networks WHERE id = ?`, id)
	return scanNetwork(row)
}

func (s *Store) ListNetworks() ([]Network, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, framework, framework_ver, status, description, created_at
		 FROM networks ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Network
	for rows.Next() {
		n, err := scanNetwork(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *n)
	}
	return result, rows.Err()
}

func (s *Store) DeleteNetwork(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM networks WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNetworkNotFound
	}
	return nil
}

// --- Members ---

func (s *Store) CreateMember(m *Member) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO members (id, arn, network_id, name, status, description, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ARN, m.NetworkID, m.Name, m.Status, m.Description, now,
	)
	return err
}

func (s *Store) GetMember(networkID, memberID string) (*Member, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, network_id, name, status, description, created_at
		 FROM members WHERE network_id = ? AND id = ?`, networkID, memberID)
	return scanMember(row)
}

func (s *Store) ListMembers(networkID string) ([]Member, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, network_id, name, status, description, created_at
		 FROM members WHERE network_id = ? ORDER BY created_at`, networkID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Member
	for rows.Next() {
		m, err := scanMember(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *m)
	}
	return result, rows.Err()
}

func (s *Store) UpdateMember(networkID, memberID string, fields map[string]any) error {
	m, err := s.GetMember(networkID, memberID)
	if err != nil {
		return errMemberNotFound
	}
	if v, ok := fields["Description"].(string); ok {
		m.Description = v
	}
	if v, ok := fields["Status"].(string); ok && v != "" {
		m.Status = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE members SET description=?, status=? WHERE network_id=? AND id=?`,
		m.Description, m.Status, networkID, memberID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errMemberNotFound
	}
	return nil
}

func (s *Store) DeleteMember(networkID, memberID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM members WHERE network_id = ? AND id = ?`, networkID, memberID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errMemberNotFound
	}
	return nil
}

// --- Nodes ---

func (s *Store) CreateNode(n *Node) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO nodes (id, arn, network_id, member_id, instance_type, status, availability_zone, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		n.ID, n.ARN, n.NetworkID, n.MemberID, n.InstanceType, n.Status, n.AvailabilityZone, now,
	)
	return err
}

func (s *Store) GetNode(networkID, memberID, nodeID string) (*Node, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, network_id, member_id, instance_type, status, availability_zone, created_at
		 FROM nodes WHERE network_id = ? AND member_id = ? AND id = ?`, networkID, memberID, nodeID)
	return scanNode(row)
}

func (s *Store) ListNodes(networkID, memberID string) ([]Node, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, network_id, member_id, instance_type, status, availability_zone, created_at
		 FROM nodes WHERE network_id = ? AND member_id = ? ORDER BY created_at`, networkID, memberID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *n)
	}
	return result, rows.Err()
}

func (s *Store) UpdateNode(networkID, memberID, nodeID string, fields map[string]any) error {
	n, err := s.GetNode(networkID, memberID, nodeID)
	if err != nil {
		return errNodeNotFound
	}
	if v, ok := fields["InstanceType"].(string); ok && v != "" {
		n.InstanceType = v
	}
	if v, ok := fields["Status"].(string); ok && v != "" {
		n.Status = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE nodes SET instance_type=?, status=? WHERE network_id=? AND member_id=? AND id=?`,
		n.InstanceType, n.Status, networkID, memberID, nodeID,
	)
	if err != nil {
		return err
	}
	cnt, _ := res.RowsAffected()
	if cnt == 0 {
		return errNodeNotFound
	}
	return nil
}

func (s *Store) DeleteNode(networkID, memberID, nodeID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM nodes WHERE network_id = ? AND member_id = ? AND id = ?`, networkID, memberID, nodeID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNodeNotFound
	}
	return nil
}

// --- Proposals ---

func (s *Store) CreateProposal(p *Proposal) error {
	now := time.Now().Unix()
	expiresAt := now + 86400 // 24h default
	_, err := s.store.DB().Exec(
		`INSERT INTO proposals (id, arn, network_id, member_id, status, description, actions, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.ARN, p.NetworkID, p.MemberID, p.Status, p.Description, p.Actions, now, expiresAt,
	)
	return err
}

func (s *Store) GetProposal(networkID, proposalID string) (*Proposal, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, network_id, member_id, status, description, actions, created_at, expires_at
		 FROM proposals WHERE network_id = ? AND id = ?`, networkID, proposalID)
	return scanProposal(row)
}

func (s *Store) ListProposals(networkID string) ([]Proposal, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, network_id, member_id, status, description, actions, created_at, expires_at
		 FROM proposals WHERE network_id = ? ORDER BY created_at`, networkID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Proposal
	for rows.Next() {
		p, err := scanProposal(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *p)
	}
	return result, rows.Err()
}

func (s *Store) UpdateProposalStatus(networkID, proposalID, status string) error {
	res, err := s.store.DB().Exec(
		`UPDATE proposals SET status=? WHERE network_id=? AND id=?`, status, networkID, proposalID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errProposalNotFound
	}
	return nil
}

// --- Accessors ---

func (s *Store) CreateAccessor(a *Accessor) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO accessors (id, arn, type, status, billing_token, network_type, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.ID, a.ARN, a.Type, a.Status, a.BillingToken, a.NetworkType, now,
	)
	return err
}

func (s *Store) GetAccessor(id string) (*Accessor, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, type, status, billing_token, network_type, created_at
		 FROM accessors WHERE id = ?`, id)
	return scanAccessor(row)
}

func (s *Store) ListAccessors() ([]Accessor, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, type, status, billing_token, network_type, created_at
		 FROM accessors ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []Accessor
	for rows.Next() {
		a, err := scanAccessor(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *a)
	}
	return result, rows.Err()
}

func (s *Store) DeleteAccessor(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM accessors WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAccessorNotFound
	}
	return nil
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanNetwork(sc scanner) (*Network, error) {
	var n Network
	var createdAt int64
	err := sc.Scan(&n.ID, &n.ARN, &n.Name, &n.Framework, &n.FrameworkVer, &n.Status, &n.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNetworkNotFound
		}
		return nil, err
	}
	n.CreatedAt = time.Unix(createdAt, 0)
	return &n, nil
}

func scanMember(sc scanner) (*Member, error) {
	var m Member
	var createdAt int64
	err := sc.Scan(&m.ID, &m.ARN, &m.NetworkID, &m.Name, &m.Status, &m.Description, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errMemberNotFound
		}
		return nil, err
	}
	m.CreatedAt = time.Unix(createdAt, 0)
	return &m, nil
}

func scanNode(sc scanner) (*Node, error) {
	var n Node
	var createdAt int64
	err := sc.Scan(&n.ID, &n.ARN, &n.NetworkID, &n.MemberID, &n.InstanceType, &n.Status, &n.AvailabilityZone, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNodeNotFound
		}
		return nil, err
	}
	n.CreatedAt = time.Unix(createdAt, 0)
	return &n, nil
}

func scanProposal(sc scanner) (*Proposal, error) {
	var p Proposal
	var createdAt, expiresAt int64
	err := sc.Scan(&p.ID, &p.ARN, &p.NetworkID, &p.MemberID, &p.Status, &p.Description, &p.Actions, &createdAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errProposalNotFound
		}
		return nil, err
	}
	p.CreatedAt = time.Unix(createdAt, 0)
	p.ExpiresAt = time.Unix(expiresAt, 0)
	return &p, nil
}

func scanAccessor(sc scanner) (*Accessor, error) {
	var a Accessor
	var createdAt int64
	err := sc.Scan(&a.ID, &a.ARN, &a.Type, &a.Status, &a.BillingToken, &a.NetworkType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAccessorNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}
