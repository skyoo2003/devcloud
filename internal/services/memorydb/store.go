// SPDX-License-Identifier: Apache-2.0

// internal/services/memorydb/store.go
package memorydb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errNotFound = errors.New("not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			node_type       TEXT NOT NULL DEFAULT 'db.r6g.large',
			num_shards      INTEGER NOT NULL DEFAULT 1,
			num_replicas    INTEGER NOT NULL DEFAULT 1,
			engine_version  TEXT NOT NULL DEFAULT '7.1',
			subnet_group    TEXT NOT NULL DEFAULT '',
			acl_name        TEXT NOT NULL DEFAULT 'open-access',
			endpoint        TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 6379,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS parameter_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'memorydb_redis7',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS subnet_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			vpc_id          TEXT NOT NULL DEFAULT '',
			subnets         TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS acls (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'active',
			user_names      TEXT NOT NULL DEFAULT '[]',
			min_engine      TEXT NOT NULL DEFAULT '6.2'
		);
		CREATE TABLE IF NOT EXISTS users (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'active',
			access_string   TEXT NOT NULL DEFAULT 'on ~* +@all',
			auth            TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS snapshots (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_name    TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'available',
			source          TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
	`},
}

// ---- model types ----

type Cluster struct {
	Name          string
	ARN           string
	Status        string
	NodeType      string
	NumShards     int
	NumReplicas   int
	EngineVersion string
	SubnetGroup   string
	ACLName       string
	Endpoint      string
	Port          int
	CreatedAt     int64
}

type ParameterGroup struct {
	Name        string
	ARN         string
	Family      string
	Description string
}

type SubnetGroup struct {
	Name        string
	ARN         string
	Description string
	VPCID       string
	Subnets     string // JSON array
}

type ACL struct {
	Name      string
	ARN       string
	Status    string
	UserNames string // JSON array
	MinEngine string
}

type User struct {
	Name         string
	ARN          string
	Status       string
	AccessString string
	Auth         string // JSON
}

type Snapshot struct {
	Name        string
	ARN         string
	ClusterName string
	Status      string
	Source      string
	CreatedAt   int64
}

// ---- Store ----

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "memorydb.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func isUnique(err error) bool {
	return err != nil && !errors.Is(err, sql.ErrNoRows) &&
		containsStr(err.Error(), "UNIQUE constraint failed")
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

// ---- Cluster ----

func (s *Store) CreateCluster(c *Cluster) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO clusters (name, arn, status, node_type, num_shards, num_replicas,
			engine_version, subnet_group, acl_name, endpoint, port, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.Name, c.ARN, c.Status, c.NodeType, c.NumShards, c.NumReplicas,
		c.EngineVersion, c.SubnetGroup, c.ACLName, c.Endpoint, c.Port, c.CreatedAt,
	)
	return err
}

func (s *Store) GetCluster(name string) (*Cluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, node_type, num_shards, num_replicas,
			engine_version, subnet_group, acl_name, endpoint, port, created_at
		 FROM clusters WHERE name = ?`, name)
	return scanCluster(row)
}

func (s *Store) ListClusters() ([]Cluster, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, node_type, num_shards, num_replicas,
			engine_version, subnet_group, acl_name, endpoint, port, created_at
		 FROM clusters ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateCluster(name string, fields map[string]any) error {
	c, err := s.GetCluster(name)
	if err != nil {
		return err
	}
	if v, ok := fields["status"].(string); ok {
		c.Status = v
	}
	if v, ok := fields["node_type"].(string); ok {
		c.NodeType = v
	}
	if v, ok := fields["num_shards"].(int); ok {
		c.NumShards = v
	}
	if v, ok := fields["num_replicas"].(int); ok {
		c.NumReplicas = v
	}
	if v, ok := fields["engine_version"].(string); ok {
		c.EngineVersion = v
	}
	if v, ok := fields["acl_name"].(string); ok {
		c.ACLName = v
	}
	_, err = s.store.DB().Exec(
		`UPDATE clusters SET status=?, node_type=?, num_shards=?, num_replicas=?,
			engine_version=?, acl_name=? WHERE name=?`,
		c.Status, c.NodeType, c.NumShards, c.NumReplicas,
		c.EngineVersion, c.ACLName, name,
	)
	return err
}

func (s *Store) DeleteCluster(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM clusters WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanCluster(sc scanner) (*Cluster, error) {
	var c Cluster
	err := sc.Scan(&c.Name, &c.ARN, &c.Status, &c.NodeType, &c.NumShards, &c.NumReplicas,
		&c.EngineVersion, &c.SubnetGroup, &c.ACLName, &c.Endpoint, &c.Port, &c.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &c, nil
}

// ---- ParameterGroup ----

func (s *Store) CreateParameterGroup(pg *ParameterGroup) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO parameter_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		pg.Name, pg.ARN, pg.Family, pg.Description,
	)
	return err
}

func (s *Store) GetParameterGroup(name string) (*ParameterGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, family, description FROM parameter_groups WHERE name = ?`, name)
	return scanPG(row)
}

func (s *Store) ListParameterGroups() ([]ParameterGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, family, description FROM parameter_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ParameterGroup
	for rows.Next() {
		pg, err := scanPG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *pg)
	}
	return out, rows.Err()
}

func (s *Store) UpdateParameterGroup(name, description string) error {
	res, err := s.store.DB().Exec(
		`UPDATE parameter_groups SET description=? WHERE name=?`, description, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func (s *Store) DeleteParameterGroup(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM parameter_groups WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanPG(sc scanner) (*ParameterGroup, error) {
	var pg ParameterGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &pg, nil
}

// ---- SubnetGroup ----

func (s *Store) CreateSubnetGroup(sg *SubnetGroup) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO subnet_groups (name, arn, description, vpc_id, subnets) VALUES (?, ?, ?, ?, ?)`,
		sg.Name, sg.ARN, sg.Description, sg.VPCID, sg.Subnets,
	)
	return err
}

func (s *Store) GetSubnetGroup(name string) (*SubnetGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, vpc_id, subnets FROM subnet_groups WHERE name = ?`, name)
	return scanSG(row)
}

func (s *Store) ListSubnetGroups() ([]SubnetGroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, description, vpc_id, subnets FROM subnet_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []SubnetGroup
	for rows.Next() {
		sg, err := scanSG(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sg)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSubnetGroup(name, description, vpcID, subnets string) error {
	sg, err := s.GetSubnetGroup(name)
	if err != nil {
		return err
	}
	if description != "" {
		sg.Description = description
	}
	if vpcID != "" {
		sg.VPCID = vpcID
	}
	if subnets != "" {
		sg.Subnets = subnets
	}
	_, err = s.store.DB().Exec(
		`UPDATE subnet_groups SET description=?, vpc_id=?, subnets=? WHERE name=?`,
		sg.Description, sg.VPCID, sg.Subnets, name,
	)
	return err
}

func (s *Store) DeleteSubnetGroup(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM subnet_groups WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanSG(sc scanner) (*SubnetGroup, error) {
	var sg SubnetGroup
	err := sc.Scan(&sg.Name, &sg.ARN, &sg.Description, &sg.VPCID, &sg.Subnets)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &sg, nil
}

// ---- ACL ----

func (s *Store) CreateACL(a *ACL) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO acls (name, arn, status, user_names, min_engine) VALUES (?, ?, ?, ?, ?)`,
		a.Name, a.ARN, a.Status, a.UserNames, a.MinEngine,
	)
	return err
}

func (s *Store) GetACL(name string) (*ACL, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, user_names, min_engine FROM acls WHERE name = ?`, name)
	return scanACL(row)
}

func (s *Store) ListACLs() ([]ACL, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, user_names, min_engine FROM acls ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ACL
	for rows.Next() {
		a, err := scanACL(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) UpdateACL(name, userNames string) error {
	a, err := s.GetACL(name)
	if err != nil {
		return err
	}
	if userNames != "" {
		a.UserNames = userNames
	}
	_, err = s.store.DB().Exec(
		`UPDATE acls SET user_names=? WHERE name=?`, a.UserNames, name)
	return err
}

func (s *Store) DeleteACL(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM acls WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanACL(sc scanner) (*ACL, error) {
	var a ACL
	err := sc.Scan(&a.Name, &a.ARN, &a.Status, &a.UserNames, &a.MinEngine)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &a, nil
}

// ---- User ----

func (s *Store) CreateUser(u *User) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO users (name, arn, status, access_string, auth) VALUES (?, ?, ?, ?, ?)`,
		u.Name, u.ARN, u.Status, u.AccessString, u.Auth,
	)
	return err
}

func (s *Store) GetUser(name string) (*User, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, access_string, auth FROM users WHERE name = ?`, name)
	return scanUser(row)
}

func (s *Store) ListUsers() ([]User, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, access_string, auth FROM users ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *u)
	}
	return out, rows.Err()
}

func (s *Store) UpdateUser(name, accessString, auth string) error {
	u, err := s.GetUser(name)
	if err != nil {
		return err
	}
	if accessString != "" {
		u.AccessString = accessString
	}
	if auth != "" {
		u.Auth = auth
	}
	_, err = s.store.DB().Exec(
		`UPDATE users SET access_string=?, auth=? WHERE name=?`,
		u.AccessString, u.Auth, name)
	return err
}

func (s *Store) DeleteUser(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM users WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanUser(sc scanner) (*User, error) {
	var u User
	err := sc.Scan(&u.Name, &u.ARN, &u.Status, &u.AccessString, &u.Auth)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &u, nil
}

// ---- Snapshot ----

func (s *Store) CreateSnapshot(snap *Snapshot) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO snapshots (name, arn, cluster_name, status, source, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		snap.Name, snap.ARN, snap.ClusterName, snap.Status, snap.Source, snap.CreatedAt,
	)
	return err
}

func (s *Store) GetSnapshot(name string) (*Snapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, cluster_name, status, source, created_at FROM snapshots WHERE name = ?`, name)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(clusterName string) ([]Snapshot, error) {
	query := `SELECT name, arn, cluster_name, status, source, created_at FROM snapshots`
	var args []any
	if clusterName != "" {
		query += ` WHERE cluster_name = ?`
		args = append(args, clusterName)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Snapshot
	for rows.Next() {
		snap, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *snap)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSnapshot(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM snapshots WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNotFound
	}
	return nil
}

func scanSnapshot(sc scanner) (*Snapshot, error) {
	var snap Snapshot
	err := sc.Scan(&snap.Name, &snap.ARN, &snap.ClusterName, &snap.Status, &snap.Source, &snap.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNotFound
		}
		return nil, err
	}
	return &snap, nil
}

// ---- helpers ----

func toStringSlice(raw []any) []string {
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func marshalStringSlice(ss []string) string {
	if ss == nil {
		return "[]"
	}
	b, _ := json.Marshal(ss)
	return string(b)
}

func unmarshalStringSlice(s string) []string {
	var out []string
	_ = json.Unmarshal([]byte(s), &out)
	return out
}

func parseTags(rawTags []any) map[string]string {
	tags := make(map[string]string)
	for _, t := range rawTags {
		tag, _ := t.(map[string]any)
		k, _ := tag["Key"].(string)
		v, _ := tag["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	return tags
}

func nowUnix() int64 {
	return time.Now().Unix()
}
