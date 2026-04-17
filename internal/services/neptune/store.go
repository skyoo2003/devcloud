// SPDX-License-Identifier: Apache-2.0

package neptune

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errClusterNotFound = errors.New("cluster not found")
var errInstanceNotFound = errors.New("instance not found")
var errSnapshotNotFound = errors.New("snapshot not found")
var errSubnetGroupNotFound = errors.New("subnet group not found")
var errParamGroupNotFound = errors.New("parameter group not found")
var errDBParamGroupNotFound = errors.New("db parameter group not found")
var errEndpointNotFound = errors.New("cluster endpoint not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS db_clusters (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'neptune',
			engine_version  TEXT NOT NULL DEFAULT '1.3.0.0',
			master_user     TEXT NOT NULL DEFAULT '',
			endpoint        TEXT NOT NULL DEFAULT '',
			reader_endpoint TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 8182,
			storage_encrypted INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS db_instances (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'available',
			instance_class  TEXT NOT NULL DEFAULT 'db.r6g.large',
			engine          TEXT NOT NULL DEFAULT 'neptune',
			engine_version  TEXT NOT NULL DEFAULT '1.3.0.0',
			endpoint        TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 8182,
			az              TEXT NOT NULL DEFAULT 'us-east-1a',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cluster_snapshots (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'available',
			snapshot_type   TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS subnet_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			vpc_id          TEXT NOT NULL DEFAULT '',
			subnets         TEXT NOT NULL DEFAULT '[]',
			status          TEXT NOT NULL DEFAULT 'Complete'
		);
		CREATE TABLE IF NOT EXISTS cluster_param_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'neptune1.3',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS db_param_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'neptune1.3',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS cluster_endpoints (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL,
			endpoint_type   TEXT NOT NULL DEFAULT 'CUSTOM',
			static_members  TEXT NOT NULL DEFAULT '[]',
			excluded_members TEXT NOT NULL DEFAULT '[]',
			status          TEXT NOT NULL DEFAULT 'available',
			endpoint        TEXT NOT NULL DEFAULT ''
		);
	`},
}

// --- Model types ---

type DBCluster struct {
	ID               string
	ARN              string
	Status           string
	Engine           string
	EngineVersion    string
	MasterUser       string
	Endpoint         string
	ReaderEndpoint   string
	Port             int
	StorageEncrypted bool
	CreatedAt        time.Time
}

type DBInstance struct {
	ID            string
	ARN           string
	ClusterID     string
	Status        string
	InstanceClass string
	Engine        string
	EngineVersion string
	Endpoint      string
	Port          int
	AZ            string
	CreatedAt     time.Time
}

type ClusterSnapshot struct {
	ID           string
	ARN          string
	ClusterID    string
	Status       string
	SnapshotType string
	CreatedAt    time.Time
}

type SubnetGroup struct {
	Name        string
	ARN         string
	Description string
	VpcID       string
	Subnets     string
	Status      string
}

type ClusterParamGroup struct {
	Name        string
	ARN         string
	Family      string
	Description string
}

type DBParamGroup struct {
	Name        string
	ARN         string
	Family      string
	Description string
}

type ClusterEndpoint struct {
	ID              string
	ARN             string
	ClusterID       string
	EndpointType    string
	StaticMembers   string
	ExcludedMembers string
	Status          string
	Endpoint        string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "neptune.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- DBCluster CRUD ---

func (s *Store) CreateCluster(id, arn, engine, engineVersion, masterUser string, port int, storageEncrypted bool) (*DBCluster, error) {
	now := time.Now().Unix()
	enc := 0
	if storageEncrypted {
		enc = 1
	}
	endpoint := id + ".neptune.localhost"
	readerEndpoint := id + ".cluster-ro.neptune.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO db_clusters (id, arn, status, engine, engine_version, master_user,
		 endpoint, reader_endpoint, port, storage_encrypted, created_at)
		 VALUES (?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, engine, engineVersion, masterUser, endpoint, readerEndpoint, port, enc, now,
	)
	if err != nil {
		return nil, err
	}
	return &DBCluster{
		ID:               id,
		ARN:              arn,
		Status:           "available",
		Engine:           engine,
		EngineVersion:    engineVersion,
		MasterUser:       masterUser,
		Endpoint:         endpoint,
		ReaderEndpoint:   readerEndpoint,
		Port:             port,
		StorageEncrypted: storageEncrypted,
		CreatedAt:        time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCluster(id string) (*DBCluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, engine, engine_version, master_user,
		 endpoint, reader_endpoint, port, storage_encrypted, created_at
		 FROM db_clusters WHERE id = ?`, id)
	return scanCluster(row)
}

func (s *Store) ListClusters(ids []string) ([]DBCluster, error) {
	query := `SELECT id, arn, status, engine, engine_version, master_user,
	          endpoint, reader_endpoint, port, storage_encrypted, created_at FROM db_clusters`
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
	var clusters []DBCluster
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
	res, err := s.store.DB().Exec(`UPDATE db_clusters SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) DeleteCluster(id string) (*DBCluster, error) {
	c, err := s.GetCluster(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM db_clusters WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errClusterNotFound
	}
	return c, nil
}

// --- DBInstance CRUD ---

func (s *Store) CreateInstance(id, arn, clusterID, instanceClass, engine, engineVersion, az string, port int) (*DBInstance, error) {
	now := time.Now().Unix()
	endpoint := id + ".neptune.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO db_instances (id, arn, cluster_id, status, instance_class, engine, engine_version,
		 endpoint, port, az, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, clusterID, instanceClass, engine, engineVersion, endpoint, port, az, now,
	)
	if err != nil {
		return nil, err
	}
	return &DBInstance{
		ID:            id,
		ARN:           arn,
		ClusterID:     clusterID,
		Status:        "available",
		InstanceClass: instanceClass,
		Engine:        engine,
		EngineVersion: engineVersion,
		Endpoint:      endpoint,
		Port:          port,
		AZ:            az,
		CreatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetInstance(id string) (*DBInstance, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, cluster_id, status, instance_class, engine, engine_version,
		 endpoint, port, az, created_at FROM db_instances WHERE id = ?`, id)
	return scanInstance(row)
}

func (s *Store) ListInstances(ids []string) ([]DBInstance, error) {
	query := `SELECT id, arn, cluster_id, status, instance_class, engine, engine_version,
	          endpoint, port, az, created_at FROM db_instances`
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
	var instances []DBInstance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, *inst)
	}
	return instances, rows.Err()
}

func (s *Store) UpdateInstanceStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE db_instances SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errInstanceNotFound
	}
	return nil
}

func (s *Store) DeleteInstance(id string) (*DBInstance, error) {
	inst, err := s.GetInstance(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM db_instances WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errInstanceNotFound
	}
	return inst, nil
}

// --- ClusterSnapshot CRUD ---

func (s *Store) CreateSnapshot(id, arn, clusterID, snapshotType string) (*ClusterSnapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_snapshots (id, arn, cluster_id, status, snapshot_type, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?)`,
		id, arn, clusterID, snapshotType, now,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterSnapshot{
		ID:           id,
		ARN:          arn,
		ClusterID:    clusterID,
		Status:       "available",
		SnapshotType: snapshotType,
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSnapshot(id string) (*ClusterSnapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, cluster_id, status, snapshot_type, created_at
		 FROM cluster_snapshots WHERE id = ?`, id)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(clusterID string) ([]ClusterSnapshot, error) {
	query := `SELECT id, arn, cluster_id, status, snapshot_type, created_at FROM cluster_snapshots`
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
	defer rows.Close()
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

// --- SubnetGroup CRUD ---

func (s *Store) CreateSubnetGroup(name, arn, description, vpcID, subnets string) (*SubnetGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO subnet_groups (name, arn, description, vpc_id, subnets, status) VALUES (?, ?, ?, ?, ?, 'Complete')`,
		name, arn, description, vpcID, subnets,
	)
	if err != nil {
		return nil, err
	}
	return &SubnetGroup{Name: name, ARN: arn, Description: description, VpcID: vpcID, Subnets: subnets, Status: "Complete"}, nil
}

func (s *Store) GetSubnetGroup(name string) (*SubnetGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, description, vpc_id, subnets, status FROM subnet_groups WHERE name = ?`, name)
	return scanSubnetGroup(row)
}

func (s *Store) ListSubnetGroups(names []string) ([]SubnetGroup, error) {
	query := `SELECT name, arn, description, vpc_id, subnets, status FROM subnet_groups`
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

// --- ClusterParamGroup CRUD ---

func (s *Store) CreateClusterParamGroup(name, arn, family, description string) (*ClusterParamGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_param_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		name, arn, family, description,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterParamGroup{Name: name, ARN: arn, Family: family, Description: description}, nil
}

func (s *Store) GetClusterParamGroup(name string) (*ClusterParamGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, family, description FROM cluster_param_groups WHERE name = ?`, name)
	return scanClusterParamGroup(row)
}

func (s *Store) ListClusterParamGroups(names []string) ([]ClusterParamGroup, error) {
	query := `SELECT name, arn, family, description FROM cluster_param_groups`
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
	var groups []ClusterParamGroup
	for rows.Next() {
		pg, err := scanClusterParamGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *pg)
	}
	return groups, rows.Err()
}

func (s *Store) DeleteClusterParamGroup(name string) (*ClusterParamGroup, error) {
	pg, err := s.GetClusterParamGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM cluster_param_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errParamGroupNotFound
	}
	return pg, nil
}

// --- DBParamGroup CRUD ---

func (s *Store) CreateDBParamGroup(name, arn, family, description string) (*DBParamGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO db_param_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		name, arn, family, description,
	)
	if err != nil {
		return nil, err
	}
	return &DBParamGroup{Name: name, ARN: arn, Family: family, Description: description}, nil
}

func (s *Store) GetDBParamGroup(name string) (*DBParamGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, family, description FROM db_param_groups WHERE name = ?`, name)
	return scanDBParamGroup(row)
}

func (s *Store) ListDBParamGroups(names []string) ([]DBParamGroup, error) {
	query := `SELECT name, arn, family, description FROM db_param_groups`
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
	var groups []DBParamGroup
	for rows.Next() {
		pg, err := scanDBParamGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *pg)
	}
	return groups, rows.Err()
}

func (s *Store) DeleteDBParamGroup(name string) (*DBParamGroup, error) {
	pg, err := s.GetDBParamGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM db_param_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errDBParamGroupNotFound
	}
	return pg, nil
}

// --- ClusterEndpoint CRUD ---

func (s *Store) CreateClusterEndpoint(id, arn, clusterID, endpointType string) (*ClusterEndpoint, error) {
	endpoint := id + ".neptune.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_endpoints (id, arn, cluster_id, endpoint_type, static_members, excluded_members, status, endpoint)
		 VALUES (?, ?, ?, ?, '[]', '[]', 'available', ?)`,
		id, arn, clusterID, endpointType, endpoint,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterEndpoint{
		ID:              id,
		ARN:             arn,
		ClusterID:       clusterID,
		EndpointType:    endpointType,
		StaticMembers:   "[]",
		ExcludedMembers: "[]",
		Status:          "available",
		Endpoint:        endpoint,
	}, nil
}

func (s *Store) GetClusterEndpoint(id string) (*ClusterEndpoint, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, cluster_id, endpoint_type, static_members, excluded_members, status, endpoint
		 FROM cluster_endpoints WHERE id = ?`, id)
	return scanClusterEndpoint(row)
}

func (s *Store) ListClusterEndpoints(clusterID string) ([]ClusterEndpoint, error) {
	query := `SELECT id, arn, cluster_id, endpoint_type, static_members, excluded_members, status, endpoint FROM cluster_endpoints`
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
	defer rows.Close()
	var endpoints []ClusterEndpoint
	for rows.Next() {
		ep, err := scanClusterEndpoint(rows)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, *ep)
	}
	return endpoints, rows.Err()
}

func (s *Store) UpdateClusterEndpointStatus(id, status string) error {
	res, err := s.store.DB().Exec(`UPDATE cluster_endpoints SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errEndpointNotFound
	}
	return nil
}

func (s *Store) DeleteClusterEndpoint(id string) (*ClusterEndpoint, error) {
	ep, err := s.GetClusterEndpoint(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM cluster_endpoints WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errEndpointNotFound
	}
	return ep, nil
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

func scanCluster(sc scanner) (*DBCluster, error) {
	var c DBCluster
	var createdAt int64
	var enc int
	err := sc.Scan(&c.ID, &c.ARN, &c.Status, &c.Engine, &c.EngineVersion,
		&c.MasterUser, &c.Endpoint, &c.ReaderEndpoint, &c.Port, &enc, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.StorageEncrypted = enc != 0
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanInstance(sc scanner) (*DBInstance, error) {
	var inst DBInstance
	var createdAt int64
	err := sc.Scan(&inst.ID, &inst.ARN, &inst.ClusterID, &inst.Status, &inst.InstanceClass,
		&inst.Engine, &inst.EngineVersion, &inst.Endpoint, &inst.Port, &inst.AZ, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInstanceNotFound
		}
		return nil, err
	}
	inst.CreatedAt = time.Unix(createdAt, 0)
	return &inst, nil
}

func scanSnapshot(sc scanner) (*ClusterSnapshot, error) {
	var sn ClusterSnapshot
	var createdAt int64
	err := sc.Scan(&sn.ID, &sn.ARN, &sn.ClusterID, &sn.Status, &sn.SnapshotType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	sn.CreatedAt = time.Unix(createdAt, 0)
	return &sn, nil
}

func scanSubnetGroup(sc scanner) (*SubnetGroup, error) {
	var sg SubnetGroup
	err := sc.Scan(&sg.Name, &sg.ARN, &sg.Description, &sg.VpcID, &sg.Subnets, &sg.Status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSubnetGroupNotFound
		}
		return nil, err
	}
	return &sg, nil
}

func scanClusterParamGroup(sc scanner) (*ClusterParamGroup, error) {
	var pg ClusterParamGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errParamGroupNotFound
		}
		return nil, err
	}
	return &pg, nil
}

func scanDBParamGroup(sc scanner) (*DBParamGroup, error) {
	var pg DBParamGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errDBParamGroupNotFound
		}
		return nil, err
	}
	return &pg, nil
}

func scanClusterEndpoint(sc scanner) (*ClusterEndpoint, error) {
	var ep ClusterEndpoint
	err := sc.Scan(&ep.ID, &ep.ARN, &ep.ClusterID, &ep.EndpointType,
		&ep.StaticMembers, &ep.ExcludedMembers, &ep.Status, &ep.Endpoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errEndpointNotFound
		}
		return nil, err
	}
	return &ep, nil
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
