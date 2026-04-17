// SPDX-License-Identifier: Apache-2.0

package elasticache

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errClusterNotFound = errors.New("cache cluster not found")
var errReplGroupNotFound = errors.New("replication group not found")
var errParamGroupNotFound = errors.New("cache parameter group not found")
var errSubnetGroupNotFound = errors.New("cache subnet group not found")
var errUserNotFound = errors.New("user not found")
var errUserGroupNotFound = errors.New("user group not found")
var errSnapshotNotFound = errors.New("snapshot not found")
var errServerlessNotFound = errors.New("serverless cache not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS cache_clusters (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'redis',
			engine_version  TEXT NOT NULL DEFAULT '7.1',
			node_type       TEXT NOT NULL DEFAULT 'cache.r6g.large',
			num_nodes       INTEGER NOT NULL DEFAULT 1,
			port            INTEGER NOT NULL DEFAULT 6379,
			endpoint        TEXT NOT NULL DEFAULT '',
			az              TEXT NOT NULL DEFAULT 'us-east-1a',
			subnet_group    TEXT NOT NULL DEFAULT '',
			param_group     TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS replication_groups (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'redis',
			engine_version  TEXT NOT NULL DEFAULT '7.1',
			node_type       TEXT NOT NULL DEFAULT 'cache.r6g.large',
			num_node_groups INTEGER NOT NULL DEFAULT 1,
			replicas_per_group INTEGER NOT NULL DEFAULT 1,
			endpoint        TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 6379,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS param_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'redis7',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS subnet_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			vpc_id          TEXT NOT NULL DEFAULT '',
			subnets         TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS users (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			user_name       TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'active',
			engine          TEXT NOT NULL DEFAULT 'redis',
			access_string   TEXT NOT NULL DEFAULT 'on ~* +@all',
			auth            TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS user_groups (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'active',
			engine          TEXT NOT NULL DEFAULT 'redis',
			user_ids        TEXT NOT NULL DEFAULT '[]'
		);
		CREATE TABLE IF NOT EXISTS snapshots (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL DEFAULT '',
			repl_group_id   TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'available',
			source          TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS serverless_caches (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'redis',
			endpoint        TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL
		);
	`},
}

// --- Model types ---

type CacheCluster struct {
	ID            string
	ARN           string
	Status        string
	Engine        string
	EngineVersion string
	NodeType      string
	NumNodes      int
	Port          int
	Endpoint      string
	AZ            string
	SubnetGroup   string
	ParamGroup    string
	CreatedAt     time.Time
}

type ReplicationGroup struct {
	ID               string
	ARN              string
	Description      string
	Status           string
	Engine           string
	EngineVersion    string
	NodeType         string
	NumNodeGroups    int
	ReplicasPerGroup int
	Endpoint         string
	Port             int
	CreatedAt        time.Time
}

type ParamGroup struct {
	Name        string
	ARN         string
	Family      string
	Description string
}

type SubnetGroup struct {
	Name        string
	ARN         string
	Description string
	VpcID       string
	Subnets     string
}

type User struct {
	ID           string
	ARN          string
	UserName     string
	Status       string
	Engine       string
	AccessString string
	Auth         string
}

type UserGroup struct {
	ID      string
	ARN     string
	Status  string
	Engine  string
	UserIDs string
}

type Snapshot struct {
	Name        string
	ARN         string
	ClusterID   string
	ReplGroupID string
	Status      string
	Source      string
	CreatedAt   time.Time
}

type ServerlessCache struct {
	Name      string
	ARN       string
	Status    string
	Engine    string
	Endpoint  string
	CreatedAt time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "elasticache.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- CacheCluster CRUD ---

func (s *Store) CreateCacheCluster(id, arn, engine, engineVersion, nodeType, az, subnetGroup, paramGroup string, numNodes, port int) (*CacheCluster, error) {
	now := time.Now().Unix()
	endpoint := id + ".cache.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO cache_clusters (id, arn, status, engine, engine_version, node_type, num_nodes, port, endpoint, az, subnet_group, param_group, created_at)
		 VALUES (?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, engine, engineVersion, nodeType, numNodes, port, endpoint, az, subnetGroup, paramGroup, now,
	)
	if err != nil {
		return nil, err
	}
	return &CacheCluster{
		ID: id, ARN: arn, Status: "available",
		Engine: engine, EngineVersion: engineVersion,
		NodeType: nodeType, NumNodes: numNodes, Port: port,
		Endpoint: endpoint, AZ: az,
		SubnetGroup: subnetGroup, ParamGroup: paramGroup,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCacheCluster(id string) (*CacheCluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, engine, engine_version, node_type, num_nodes, port, endpoint, az, subnet_group, param_group, created_at
		 FROM cache_clusters WHERE id = ?`, id)
	return scanCacheCluster(row)
}

func (s *Store) ListCacheClusters(ids []string) ([]CacheCluster, error) {
	query := `SELECT id, arn, status, engine, engine_version, node_type, num_nodes, port, endpoint, az, subnet_group, param_group, created_at FROM cache_clusters`
	var args []any
	if len(ids) > 0 {
		ph := buildPlaceholders(len(ids))
		query += " WHERE id IN (" + ph + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CacheCluster
	for rows.Next() {
		c, err := scanCacheCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateCacheClusterStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE cache_clusters SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) DeleteCacheCluster(id string) (*CacheCluster, error) {
	c, err := s.GetCacheCluster(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM cache_clusters WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errClusterNotFound
	}
	return c, nil
}

// --- ReplicationGroup CRUD ---

func (s *Store) CreateReplicationGroup(id, arn, description, engine, engineVersion, nodeType string, numNodeGroups, replicasPerGroup, port int) (*ReplicationGroup, error) {
	now := time.Now().Unix()
	endpoint := id + ".repl.cache.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO replication_groups (id, arn, description, status, engine, engine_version, node_type, num_node_groups, replicas_per_group, endpoint, port, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, description, engine, engineVersion, nodeType, numNodeGroups, replicasPerGroup, endpoint, port, now,
	)
	if err != nil {
		return nil, err
	}
	return &ReplicationGroup{
		ID: id, ARN: arn, Description: description, Status: "available",
		Engine: engine, EngineVersion: engineVersion, NodeType: nodeType,
		NumNodeGroups: numNodeGroups, ReplicasPerGroup: replicasPerGroup,
		Endpoint: endpoint, Port: port, CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetReplicationGroup(id string) (*ReplicationGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, description, status, engine, engine_version, node_type, num_node_groups, replicas_per_group, endpoint, port, created_at
		 FROM replication_groups WHERE id = ?`, id)
	return scanReplicationGroup(row)
}

func (s *Store) ListReplicationGroups(ids []string) ([]ReplicationGroup, error) {
	query := `SELECT id, arn, description, status, engine, engine_version, node_type, num_node_groups, replicas_per_group, endpoint, port, created_at FROM replication_groups`
	var args []any
	if len(ids) > 0 {
		ph := buildPlaceholders(len(ids))
		query += " WHERE id IN (" + ph + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicationGroup
	for rows.Next() {
		rg, err := scanReplicationGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *rg)
	}
	return out, rows.Err()
}

func (s *Store) UpdateReplicationGroupStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE replication_groups SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errReplGroupNotFound
	}
	return nil
}

func (s *Store) DeleteReplicationGroup(id string) (*ReplicationGroup, error) {
	rg, err := s.GetReplicationGroup(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM replication_groups WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errReplGroupNotFound
	}
	return rg, nil
}

// --- ParamGroup CRUD ---

func (s *Store) CreateParamGroup(name, arn, family, description string) (*ParamGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO param_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		name, arn, family, description,
	)
	if err != nil {
		return nil, err
	}
	return &ParamGroup{Name: name, ARN: arn, Family: family, Description: description}, nil
}

func (s *Store) GetParamGroup(name string) (*ParamGroup, error) {
	row := s.store.DB().QueryRow(`SELECT name, arn, family, description FROM param_groups WHERE name = ?`, name)
	return scanParamGroup(row)
}

func (s *Store) ListParamGroups(names []string) ([]ParamGroup, error) {
	query := `SELECT name, arn, family, description FROM param_groups`
	var args []any
	if len(names) > 0 {
		ph := buildPlaceholders(len(names))
		query += " WHERE name IN (" + ph + ")"
		for _, n := range names {
			args = append(args, n)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ParamGroup
	for rows.Next() {
		pg, err := scanParamGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *pg)
	}
	return out, rows.Err()
}

func (s *Store) DeleteParamGroup(name string) (*ParamGroup, error) {
	pg, err := s.GetParamGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM param_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errParamGroupNotFound
	}
	return pg, nil
}

// --- SubnetGroup CRUD ---

func (s *Store) CreateSubnetGroup(name, arn, description, vpcID, subnets string) (*SubnetGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO subnet_groups (name, arn, description, vpc_id, subnets) VALUES (?, ?, ?, ?, ?)`,
		name, arn, description, vpcID, subnets,
	)
	if err != nil {
		return nil, err
	}
	return &SubnetGroup{Name: name, ARN: arn, Description: description, VpcID: vpcID, Subnets: subnets}, nil
}

func (s *Store) GetSubnetGroup(name string) (*SubnetGroup, error) {
	row := s.store.DB().QueryRow(`SELECT name, arn, description, vpc_id, subnets FROM subnet_groups WHERE name = ?`, name)
	return scanSubnetGroup(row)
}

func (s *Store) ListSubnetGroups(names []string) ([]SubnetGroup, error) {
	query := `SELECT name, arn, description, vpc_id, subnets FROM subnet_groups`
	var args []any
	if len(names) > 0 {
		ph := buildPlaceholders(len(names))
		query += " WHERE name IN (" + ph + ")"
		for _, n := range names {
			args = append(args, n)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SubnetGroup
	for rows.Next() {
		sg, err := scanSubnetGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sg)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSubnetGroup(name, description, vpcID, subnets string) error {
	res, err := s.store.DB().Exec(
		`UPDATE subnet_groups SET description = ?, vpc_id = ?, subnets = ? WHERE name = ?`,
		description, vpcID, subnets, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errSubnetGroupNotFound
	}
	return nil
}

func (s *Store) DeleteSubnetGroup(name string) (*SubnetGroup, error) {
	sg, err := s.GetSubnetGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM subnet_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errSubnetGroupNotFound
	}
	return sg, nil
}

// --- User CRUD ---

func (s *Store) CreateUser(id, arn, userName, engine, accessString string) (*User, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO users (id, arn, user_name, status, engine, access_string, auth) VALUES (?, ?, ?, 'active', ?, ?, '{}')`,
		id, arn, userName, engine, accessString,
	)
	if err != nil {
		return nil, err
	}
	return &User{ID: id, ARN: arn, UserName: userName, Status: "active", Engine: engine, AccessString: accessString, Auth: "{}"}, nil
}

func (s *Store) GetUser(id string) (*User, error) {
	row := s.store.DB().QueryRow(`SELECT id, arn, user_name, status, engine, access_string, auth FROM users WHERE id = ?`, id)
	return scanUser(row)
}

func (s *Store) ListUsers(ids []string) ([]User, error) {
	query := `SELECT id, arn, user_name, status, engine, access_string, auth FROM users`
	var args []any
	if len(ids) > 0 {
		ph := buildPlaceholders(len(ids))
		query += " WHERE id IN (" + ph + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *Store) UpdateUserAccessString(id, accessString string) error {
	res, err := s.store.DB().Exec(`UPDATE users SET access_string = ? WHERE id = ?`, accessString, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserNotFound
	}
	return nil
}

func (s *Store) DeleteUser(id string) (*User, error) {
	u, err := s.GetUser(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM users WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errUserNotFound
	}
	return u, nil
}

// --- UserGroup CRUD ---

func (s *Store) CreateUserGroup(id, arn, engine, userIDs string) (*UserGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO user_groups (id, arn, status, engine, user_ids) VALUES (?, ?, 'active', ?, ?)`,
		id, arn, engine, userIDs,
	)
	if err != nil {
		return nil, err
	}
	return &UserGroup{ID: id, ARN: arn, Status: "active", Engine: engine, UserIDs: userIDs}, nil
}

func (s *Store) GetUserGroup(id string) (*UserGroup, error) {
	row := s.store.DB().QueryRow(`SELECT id, arn, status, engine, user_ids FROM user_groups WHERE id = ?`, id)
	return scanUserGroup(row)
}

func (s *Store) ListUserGroups(ids []string) ([]UserGroup, error) {
	query := `SELECT id, arn, status, engine, user_ids FROM user_groups`
	var args []any
	if len(ids) > 0 {
		ph := buildPlaceholders(len(ids))
		query += " WHERE id IN (" + ph + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserGroup
	for rows.Next() {
		ug, err := scanUserGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ug)
	}
	return out, rows.Err()
}

func (s *Store) UpdateUserGroup(id, userIDs string) error {
	res, err := s.store.DB().Exec(`UPDATE user_groups SET user_ids = ? WHERE id = ?`, userIDs, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserGroupNotFound
	}
	return nil
}

func (s *Store) DeleteUserGroup(id string) (*UserGroup, error) {
	ug, err := s.GetUserGroup(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM user_groups WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errUserGroupNotFound
	}
	return ug, nil
}

// --- Snapshot CRUD ---

func (s *Store) CreateSnapshot(name, arn, clusterID, replGroupID, source string) (*Snapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO snapshots (name, arn, cluster_id, repl_group_id, status, source, created_at) VALUES (?, ?, ?, ?, 'available', ?, ?)`,
		name, arn, clusterID, replGroupID, source, now,
	)
	if err != nil {
		return nil, err
	}
	return &Snapshot{Name: name, ARN: arn, ClusterID: clusterID, ReplGroupID: replGroupID, Status: "available", Source: source, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetSnapshot(name string) (*Snapshot, error) {
	row := s.store.DB().QueryRow(`SELECT name, arn, cluster_id, repl_group_id, status, source, created_at FROM snapshots WHERE name = ?`, name)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(clusterID, replGroupID string) ([]Snapshot, error) {
	query := `SELECT name, arn, cluster_id, repl_group_id, status, source, created_at FROM snapshots`
	var args []any
	var conditions []string
	if clusterID != "" {
		conditions = append(conditions, "cluster_id = ?")
		args = append(args, clusterID)
	}
	if replGroupID != "" {
		conditions = append(conditions, "repl_group_id = ?")
		args = append(args, replGroupID)
	}
	if len(conditions) > 0 {
		query += " WHERE " + conditions[0]
		for _, c := range conditions[1:] {
			query += " AND " + c
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		sn, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sn)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSnapshot(name string) (*Snapshot, error) {
	sn, err := s.GetSnapshot(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM snapshots WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errSnapshotNotFound
	}
	return sn, nil
}

// --- ServerlessCache CRUD ---

func (s *Store) CreateServerlessCache(name, arn, engine string) (*ServerlessCache, error) {
	now := time.Now().Unix()
	endpoint := name + ".serverless.cache.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO serverless_caches (name, arn, status, engine, endpoint, created_at) VALUES (?, ?, 'available', ?, ?, ?)`,
		name, arn, engine, endpoint, now,
	)
	if err != nil {
		return nil, err
	}
	return &ServerlessCache{Name: name, ARN: arn, Status: "available", Engine: engine, Endpoint: endpoint, CreatedAt: time.Unix(now, 0)}, nil
}

func (s *Store) GetServerlessCache(name string) (*ServerlessCache, error) {
	row := s.store.DB().QueryRow(`SELECT name, arn, status, engine, endpoint, created_at FROM serverless_caches WHERE name = ?`, name)
	return scanServerlessCache(row)
}

func (s *Store) ListServerlessCaches(names []string) ([]ServerlessCache, error) {
	query := `SELECT name, arn, status, engine, endpoint, created_at FROM serverless_caches`
	var args []any
	if len(names) > 0 {
		ph := buildPlaceholders(len(names))
		query += " WHERE name IN (" + ph + ")"
		for _, n := range names {
			args = append(args, n)
		}
	}
	query += " ORDER BY name"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServerlessCache
	for rows.Next() {
		sc, err := scanServerlessCache(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sc)
	}
	return out, rows.Err()
}

func (s *Store) UpdateServerlessCacheStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE serverless_caches SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errServerlessNotFound
	}
	return nil
}

func (s *Store) DeleteServerlessCache(name string) (*ServerlessCache, error) {
	sc, err := s.GetServerlessCache(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM serverless_caches WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errServerlessNotFound
	}
	return sc, nil
}

// --- Tag helpers ---

func (s *Store) AddTags(arn string, tags map[string]string) error {
	return s.tags.AddTags(arn, tags)
}

func (s *Store) RemoveTags(arn string, keys []string) error {
	return s.tags.RemoveTags(arn, keys)
}

func (s *Store) ListTags(arn string) (map[string]string, error) {
	return s.tags.ListTags(arn)
}

// --- scan helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanCacheCluster(sc scanner) (*CacheCluster, error) {
	var c CacheCluster
	var createdAt int64
	err := sc.Scan(&c.ID, &c.ARN, &c.Status, &c.Engine, &c.EngineVersion,
		&c.NodeType, &c.NumNodes, &c.Port, &c.Endpoint, &c.AZ, &c.SubnetGroup, &c.ParamGroup, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanReplicationGroup(sc scanner) (*ReplicationGroup, error) {
	var rg ReplicationGroup
	var createdAt int64
	err := sc.Scan(&rg.ID, &rg.ARN, &rg.Description, &rg.Status, &rg.Engine, &rg.EngineVersion,
		&rg.NodeType, &rg.NumNodeGroups, &rg.ReplicasPerGroup, &rg.Endpoint, &rg.Port, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errReplGroupNotFound
		}
		return nil, err
	}
	rg.CreatedAt = time.Unix(createdAt, 0)
	return &rg, nil
}

func scanParamGroup(sc scanner) (*ParamGroup, error) {
	var pg ParamGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errParamGroupNotFound
		}
		return nil, err
	}
	return &pg, nil
}

func scanSubnetGroup(sc scanner) (*SubnetGroup, error) {
	var sg SubnetGroup
	err := sc.Scan(&sg.Name, &sg.ARN, &sg.Description, &sg.VpcID, &sg.Subnets)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSubnetGroupNotFound
		}
		return nil, err
	}
	return &sg, nil
}

func scanUser(sc scanner) (*User, error) {
	var u User
	err := sc.Scan(&u.ID, &u.ARN, &u.UserName, &u.Status, &u.Engine, &u.AccessString, &u.Auth)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUserNotFound
		}
		return nil, err
	}
	return &u, nil
}

func scanUserGroup(sc scanner) (*UserGroup, error) {
	var ug UserGroup
	err := sc.Scan(&ug.ID, &ug.ARN, &ug.Status, &ug.Engine, &ug.UserIDs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUserGroupNotFound
		}
		return nil, err
	}
	return &ug, nil
}

func scanSnapshot(sc scanner) (*Snapshot, error) {
	var sn Snapshot
	var createdAt int64
	err := sc.Scan(&sn.Name, &sn.ARN, &sn.ClusterID, &sn.ReplGroupID, &sn.Status, &sn.Source, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	sn.CreatedAt = time.Unix(createdAt, 0)
	return &sn, nil
}

func scanServerlessCache(sc scanner) (*ServerlessCache, error) {
	var sc2 ServerlessCache
	var createdAt int64
	err := sc.Scan(&sc2.Name, &sc2.ARN, &sc2.Status, &sc2.Engine, &sc2.Endpoint, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errServerlessNotFound
		}
		return nil, err
	}
	sc2.CreatedAt = time.Unix(createdAt, 0)
	return &sc2, nil
}

func buildPlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, 0, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}
