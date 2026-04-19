// SPDX-License-Identifier: Apache-2.0

package redshift

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errClusterNotFound = errors.New("cluster not found")
var errSnapshotNotFound = errors.New("snapshot not found")
var errParamGroupNotFound = errors.New("parameter group not found")
var errSubnetGroupNotFound = errors.New("subnet group not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			node_type       TEXT NOT NULL DEFAULT 'dc2.large',
			num_nodes       INTEGER NOT NULL DEFAULT 1,
			db_name         TEXT NOT NULL DEFAULT 'dev',
			master_user     TEXT NOT NULL DEFAULT 'admin',
			endpoint_addr   TEXT NOT NULL DEFAULT '',
			endpoint_port   INTEGER NOT NULL DEFAULT 5439,
			vpc_id          TEXT NOT NULL DEFAULT '',
			encrypted       INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cluster_snapshots (
			id              TEXT PRIMARY KEY,
			cluster_id      TEXT NOT NULL,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			snapshot_type   TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS parameter_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'redshift-1.0',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS subnet_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			vpc_id          TEXT NOT NULL DEFAULT '',
			subnets         TEXT NOT NULL DEFAULT '[]'
		);
	`},
}

// --- Model types ---

type Cluster struct {
	ID           string
	ARN          string
	Status       string
	NodeType     string
	NumNodes     int
	DBName       string
	MasterUser   string
	EndpointAddr string
	EndpointPort int
	VpcID        string
	Encrypted    bool
	CreatedAt    time.Time
}

type ClusterSnapshot struct {
	ID           string
	ClusterID    string
	ARN          string
	Status       string
	SnapshotType string
	CreatedAt    time.Time
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
	VpcID       string
	Subnets     string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "redshift.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Cluster CRUD ---

func (s *Store) CreateCluster(id, arn, nodeType, dbName, masterUser string, numNodes int, encrypted bool) (*Cluster, error) {
	now := time.Now().Unix()
	enc := 0
	if encrypted {
		enc = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO clusters (id, arn, status, node_type, num_nodes, db_name, master_user,
		 endpoint_addr, endpoint_port, vpc_id, encrypted, created_at)
		 VALUES (?, ?, 'available', ?, ?, ?, ?, ?, 5439, '', ?, ?)`,
		id, arn, nodeType, numNodes, dbName, masterUser, id+".redshift.localhost", enc, now,
	)
	if err != nil {
		return nil, err
	}
	return &Cluster{
		ID:           id,
		ARN:          arn,
		Status:       "available",
		NodeType:     nodeType,
		NumNodes:     numNodes,
		DBName:       dbName,
		MasterUser:   masterUser,
		EndpointAddr: id + ".redshift.localhost",
		EndpointPort: 5439,
		Encrypted:    encrypted,
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCluster(id string) (*Cluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, node_type, num_nodes, db_name, master_user,
		 endpoint_addr, endpoint_port, vpc_id, encrypted, created_at
		 FROM clusters WHERE id = ?`, id)
	return scanCluster(row)
}

func (s *Store) ListClusters(ids []string) ([]Cluster, error) {
	query := `SELECT id, arn, status, node_type, num_nodes, db_name, master_user,
	          endpoint_addr, endpoint_port, vpc_id, encrypted, created_at FROM clusters`
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
	defer func() { _ = rows.Close() }()
	var clusters []Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, *c)
	}
	return clusters, rows.Err()
}

func (s *Store) UpdateClusterStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE clusters SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) DeleteCluster(id string) (*Cluster, error) {
	c, err := s.GetCluster(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM clusters WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errClusterNotFound
	}
	return c, nil
}

// --- ClusterSnapshot CRUD ---

func (s *Store) CreateSnapshot(id, clusterID, arn, snapshotType string) (*ClusterSnapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_snapshots (id, cluster_id, arn, status, snapshot_type, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?)`,
		id, clusterID, arn, snapshotType, now,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterSnapshot{
		ID:           id,
		ClusterID:    clusterID,
		ARN:          arn,
		Status:       "available",
		SnapshotType: snapshotType,
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSnapshot(id string) (*ClusterSnapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, cluster_id, arn, status, snapshot_type, created_at
		 FROM cluster_snapshots WHERE id = ?`, id)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(clusterID string) ([]ClusterSnapshot, error) {
	query := `SELECT id, cluster_id, arn, status, snapshot_type, created_at FROM cluster_snapshots`
	var args []any
	if clusterID != "" {
		query += " WHERE cluster_id = ?"
		args = append(args, clusterID)
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var snaps []ClusterSnapshot
	for rows.Next() {
		sn, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *sn)
	}
	return snaps, rows.Err()
}

func (s *Store) DeleteSnapshot(id string) (*ClusterSnapshot, error) {
	sn, err := s.GetSnapshot(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM cluster_snapshots WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errSnapshotNotFound
	}
	return sn, nil
}

// --- ParameterGroup CRUD ---

func (s *Store) CreateParameterGroup(name, arn, family, description string) (*ParameterGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO parameter_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		name, arn, family, description,
	)
	if err != nil {
		return nil, err
	}
	return &ParameterGroup{Name: name, ARN: arn, Family: family, Description: description}, nil
}

func (s *Store) GetParameterGroup(name string) (*ParameterGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, family, description FROM parameter_groups WHERE name = ?`, name)
	return scanParameterGroup(row)
}

func (s *Store) ListParameterGroups(names []string) ([]ParameterGroup, error) {
	query := `SELECT name, arn, family, description FROM parameter_groups`
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
	defer func() { _ = rows.Close() }()
	var groups []ParameterGroup
	for rows.Next() {
		pg, err := scanParameterGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *pg)
	}
	return groups, rows.Err()
}

func (s *Store) DeleteParameterGroup(name string) (*ParameterGroup, error) {
	pg, err := s.GetParameterGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM parameter_groups WHERE name = ?`, name)
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
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, vpc_id, subnets FROM subnet_groups WHERE name = ?`, name)
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
	defer func() { _ = rows.Close() }()
	var groups []SubnetGroup
	for rows.Next() {
		sg, err := scanSubnetGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *sg)
	}
	return groups, rows.Err()
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

// --- helpers ---

type scanner interface{ Scan(dest ...any) error }

func scanCluster(sc scanner) (*Cluster, error) {
	var c Cluster
	var createdAt int64
	var enc int
	err := sc.Scan(&c.ID, &c.ARN, &c.Status, &c.NodeType, &c.NumNodes, &c.DBName,
		&c.MasterUser, &c.EndpointAddr, &c.EndpointPort, &c.VpcID, &enc, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.Encrypted = enc != 0
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanSnapshot(sc scanner) (*ClusterSnapshot, error) {
	var sn ClusterSnapshot
	var createdAt int64
	err := sc.Scan(&sn.ID, &sn.ClusterID, &sn.ARN, &sn.Status, &sn.SnapshotType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	sn.CreatedAt = time.Unix(createdAt, 0)
	return &sn, nil
}

func scanParameterGroup(sc scanner) (*ParameterGroup, error) {
	var pg ParameterGroup
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
