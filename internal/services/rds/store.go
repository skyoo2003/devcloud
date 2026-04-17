// SPDX-License-Identifier: Apache-2.0

package rds

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errInstanceNotFound = errors.New("db instance not found")
var errClusterNotFound = errors.New("db cluster not found")
var errSnapshotNotFound = errors.New("db snapshot not found")
var errClusterSnapNotFound = errors.New("db cluster snapshot not found")
var errParamGroupNotFound = errors.New("db parameter group not found")
var errClusterPGNotFound = errors.New("db cluster parameter group not found")
var errSubnetGroupNotFound = errors.New("db subnet group not found")
var errOptionGroupNotFound = errors.New("option group not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS db_instances (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL DEFAULT '',
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'mysql',
			engine_version  TEXT NOT NULL DEFAULT '8.0.35',
			instance_class  TEXT NOT NULL DEFAULT 'db.t3.medium',
			master_user     TEXT NOT NULL DEFAULT 'admin',
			db_name         TEXT NOT NULL DEFAULT '',
			endpoint        TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 3306,
			az              TEXT NOT NULL DEFAULT 'us-east-1a',
			multi_az        INTEGER NOT NULL DEFAULT 0,
			storage_type    TEXT NOT NULL DEFAULT 'gp3',
			allocated_storage INTEGER NOT NULL DEFAULT 20,
			encrypted       INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS db_clusters (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'aurora-mysql',
			engine_version  TEXT NOT NULL DEFAULT '8.0.mysql_aurora.3.07.0',
			master_user     TEXT NOT NULL DEFAULT 'admin',
			db_name         TEXT NOT NULL DEFAULT '',
			endpoint        TEXT NOT NULL DEFAULT '',
			reader_endpoint TEXT NOT NULL DEFAULT '',
			port            INTEGER NOT NULL DEFAULT 3306,
			storage_encrypted INTEGER NOT NULL DEFAULT 0,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS db_snapshots (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			instance_id     TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'mysql',
			snapshot_type   TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS cluster_snapshots (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			cluster_id      TEXT NOT NULL,
			status          TEXT NOT NULL DEFAULT 'available',
			engine          TEXT NOT NULL DEFAULT 'aurora-mysql',
			snapshot_type   TEXT NOT NULL DEFAULT 'manual',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS db_param_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'mysql8.0',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS cluster_param_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			family          TEXT NOT NULL DEFAULT 'aurora-mysql8.0',
			description     TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS subnet_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			description     TEXT NOT NULL DEFAULT '',
			vpc_id          TEXT NOT NULL DEFAULT '',
			subnets         TEXT NOT NULL DEFAULT '[]',
			status          TEXT NOT NULL DEFAULT 'Complete'
		);
		CREATE TABLE IF NOT EXISTS option_groups (
			name            TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			engine          TEXT NOT NULL DEFAULT 'mysql',
			major_version   TEXT NOT NULL DEFAULT '8.0',
			description     TEXT NOT NULL DEFAULT ''
		);
	`},
}

// --- Model types ---

type DBInstance struct {
	ID               string
	ARN              string
	ClusterID        string
	Status           string
	Engine           string
	EngineVersion    string
	InstanceClass    string
	MasterUser       string
	DBName           string
	Endpoint         string
	Port             int
	AZ               string
	MultiAZ          bool
	StorageType      string
	AllocatedStorage int
	Encrypted        bool
	CreatedAt        time.Time
}

type DBCluster struct {
	ID               string
	ARN              string
	Status           string
	Engine           string
	EngineVersion    string
	MasterUser       string
	DBName           string
	Endpoint         string
	ReaderEndpoint   string
	Port             int
	StorageEncrypted bool
	CreatedAt        time.Time
}

type DBSnapshot struct {
	ID           string
	ARN          string
	InstanceID   string
	Status       string
	Engine       string
	SnapshotType string
	CreatedAt    time.Time
}

type ClusterSnapshot struct {
	ID           string
	ARN          string
	ClusterID    string
	Status       string
	Engine       string
	SnapshotType string
	CreatedAt    time.Time
}

type DBParamGroup struct {
	Name        string
	ARN         string
	Family      string
	Description string
}

type ClusterParamGroup struct {
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
	Status      string
}

type OptionGroup struct {
	Name         string
	ARN          string
	Engine       string
	MajorVersion string
	Description  string
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "rds.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- DBInstance CRUD ---

func (s *Store) CreateInstance(id, arn, clusterID, engine, engineVersion, instanceClass, masterUser, dbName, az, storageType string, port, allocatedStorage int, multiAZ, encrypted bool) (*DBInstance, error) {
	now := time.Now().Unix()
	endpoint := id + ".rds.localhost"
	maz := 0
	if multiAZ {
		maz = 1
	}
	enc := 0
	if encrypted {
		enc = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT INTO db_instances (id, arn, cluster_id, status, engine, engine_version, instance_class,
		 master_user, db_name, endpoint, port, az, multi_az, storage_type, allocated_storage, encrypted, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, clusterID, engine, engineVersion, instanceClass, masterUser, dbName, endpoint, port, az, maz, storageType, allocatedStorage, enc, now,
	)
	if err != nil {
		return nil, err
	}
	return &DBInstance{
		ID: id, ARN: arn, ClusterID: clusterID, Status: "available",
		Engine: engine, EngineVersion: engineVersion, InstanceClass: instanceClass,
		MasterUser: masterUser, DBName: dbName, Endpoint: endpoint, Port: port,
		AZ: az, MultiAZ: multiAZ, StorageType: storageType,
		AllocatedStorage: allocatedStorage, Encrypted: encrypted,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetInstance(id string) (*DBInstance, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, cluster_id, status, engine, engine_version, instance_class,
		 master_user, db_name, endpoint, port, az, multi_az, storage_type, allocated_storage, encrypted, created_at
		 FROM db_instances WHERE id = ?`, id)
	return scanInstance(row)
}

func (s *Store) ListInstances(ids []string) ([]DBInstance, error) {
	query := `SELECT id, arn, cluster_id, status, engine, engine_version, instance_class,
	          master_user, db_name, endpoint, port, az, multi_az, storage_type, allocated_storage, encrypted, created_at FROM db_instances`
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

func (s *Store) UpdateInstanceClass(id, instanceClass string) error {
	res, err := s.store.DB().Exec(`UPDATE db_instances SET instance_class = ? WHERE id = ?`, instanceClass, id)
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

// --- DBCluster CRUD ---

func (s *Store) CreateCluster(id, arn, engine, engineVersion, masterUser, dbName string, port int, storageEncrypted bool) (*DBCluster, error) {
	now := time.Now().Unix()
	enc := 0
	if storageEncrypted {
		enc = 1
	}
	endpoint := id + ".cluster.rds.localhost"
	readerEndpoint := id + ".cluster-ro.rds.localhost"
	_, err := s.store.DB().Exec(
		`INSERT INTO db_clusters (id, arn, status, engine, engine_version, master_user, db_name,
		 endpoint, reader_endpoint, port, storage_encrypted, created_at)
		 VALUES (?, ?, 'available', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, arn, engine, engineVersion, masterUser, dbName, endpoint, readerEndpoint, port, enc, now,
	)
	if err != nil {
		return nil, err
	}
	return &DBCluster{
		ID: id, ARN: arn, Status: "available",
		Engine: engine, EngineVersion: engineVersion, MasterUser: masterUser,
		DBName: dbName, Endpoint: endpoint, ReaderEndpoint: readerEndpoint,
		Port: port, StorageEncrypted: storageEncrypted,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCluster(id string) (*DBCluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, status, engine, engine_version, master_user, db_name,
		 endpoint, reader_endpoint, port, storage_encrypted, created_at
		 FROM db_clusters WHERE id = ?`, id)
	return scanCluster(row)
}

func (s *Store) ListClusters(ids []string) ([]DBCluster, error) {
	query := `SELECT id, arn, status, engine, engine_version, master_user, db_name,
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

// --- DBSnapshot CRUD ---

func (s *Store) CreateSnapshot(id, arn, instanceID, engine, snapshotType string) (*DBSnapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO db_snapshots (id, arn, instance_id, status, engine, snapshot_type, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?, ?)`,
		id, arn, instanceID, engine, snapshotType, now,
	)
	if err != nil {
		return nil, err
	}
	return &DBSnapshot{
		ID: id, ARN: arn, InstanceID: instanceID,
		Status: "available", Engine: engine, SnapshotType: snapshotType,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetSnapshot(id string) (*DBSnapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, instance_id, status, engine, snapshot_type, created_at
		 FROM db_snapshots WHERE id = ?`, id)
	return scanSnapshot(row)
}

func (s *Store) ListSnapshots(instanceID string) ([]DBSnapshot, error) {
	query := `SELECT id, arn, instance_id, status, engine, snapshot_type, created_at FROM db_snapshots`
	var args []any
	if instanceID != "" {
		query += " WHERE instance_id = ?"
		args = append(args, instanceID)
	}
	query += " ORDER BY id"
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snaps []DBSnapshot
	for rows.Next() {
		sn, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *sn)
	}
	return snaps, rows.Err()
}

func (s *Store) DeleteSnapshot(id string) (*DBSnapshot, error) {
	sn, err := s.GetSnapshot(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM db_snapshots WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errSnapshotNotFound
	}
	return sn, nil
}

// --- ClusterSnapshot CRUD ---

func (s *Store) CreateClusterSnapshot(id, arn, clusterID, engine, snapshotType string) (*ClusterSnapshot, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_snapshots (id, arn, cluster_id, status, engine, snapshot_type, created_at)
		 VALUES (?, ?, ?, 'available', ?, ?, ?)`,
		id, arn, clusterID, engine, snapshotType, now,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterSnapshot{
		ID: id, ARN: arn, ClusterID: clusterID,
		Status: "available", Engine: engine, SnapshotType: snapshotType,
		CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetClusterSnapshot(id string) (*ClusterSnapshot, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, cluster_id, status, engine, snapshot_type, created_at
		 FROM cluster_snapshots WHERE id = ?`, id)
	return scanClusterSnapshot(row)
}

func (s *Store) ListClusterSnapshots(clusterID string) ([]ClusterSnapshot, error) {
	query := `SELECT id, arn, cluster_id, status, engine, snapshot_type, created_at FROM cluster_snapshots`
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
		sn, err := scanClusterSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, *sn)
	}
	return snaps, rows.Err()
}

func (s *Store) DeleteClusterSnapshot(id string) (*ClusterSnapshot, error) {
	sn, err := s.GetClusterSnapshot(id)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM cluster_snapshots WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errClusterSnapNotFound
	}
	return sn, nil
}

// --- DBParamGroup CRUD ---

func (s *Store) CreateParamGroup(name, arn, family, description string) (*DBParamGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO db_param_groups (name, arn, family, description) VALUES (?, ?, ?, ?)`,
		name, arn, family, description,
	)
	if err != nil {
		return nil, err
	}
	return &DBParamGroup{Name: name, ARN: arn, Family: family, Description: description}, nil
}

func (s *Store) GetParamGroup(name string) (*DBParamGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, family, description FROM db_param_groups WHERE name = ?`, name)
	return scanParamGroup(row)
}

func (s *Store) ListParamGroups(names []string) ([]DBParamGroup, error) {
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
		pg, err := scanParamGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *pg)
	}
	return groups, rows.Err()
}

func (s *Store) DeleteParamGroup(name string) (*DBParamGroup, error) {
	pg, err := s.GetParamGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM db_param_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errParamGroupNotFound
	}
	return pg, nil
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
		return nil, errClusterPGNotFound
	}
	return pg, nil
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

// --- OptionGroup CRUD ---

func (s *Store) CreateOptionGroup(name, arn, engine, majorVersion, description string) (*OptionGroup, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO option_groups (name, arn, engine, major_version, description) VALUES (?, ?, ?, ?, ?)`,
		name, arn, engine, majorVersion, description,
	)
	if err != nil {
		return nil, err
	}
	return &OptionGroup{Name: name, ARN: arn, Engine: engine, MajorVersion: majorVersion, Description: description}, nil
}

func (s *Store) GetOptionGroup(name string) (*OptionGroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, engine, major_version, description FROM option_groups WHERE name = ?`, name)
	return scanOptionGroup(row)
}

func (s *Store) ListOptionGroups(names []string) ([]OptionGroup, error) {
	query := `SELECT name, arn, engine, major_version, description FROM option_groups`
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
	var groups []OptionGroup
	for rows.Next() {
		og, err := scanOptionGroup(rows)
		if err != nil {
			return nil, err
		}
		groups = append(groups, *og)
	}
	return groups, rows.Err()
}

func (s *Store) DeleteOptionGroup(name string) (*OptionGroup, error) {
	og, err := s.GetOptionGroup(name)
	if err != nil {
		return nil, err
	}
	res, err := s.store.DB().Exec(`DELETE FROM option_groups WHERE name = ?`, name)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errOptionGroupNotFound
	}
	return og, nil
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

func scanInstance(sc scanner) (*DBInstance, error) {
	var inst DBInstance
	var createdAt int64
	var maz, enc int
	err := sc.Scan(&inst.ID, &inst.ARN, &inst.ClusterID, &inst.Status,
		&inst.Engine, &inst.EngineVersion, &inst.InstanceClass,
		&inst.MasterUser, &inst.DBName, &inst.Endpoint, &inst.Port,
		&inst.AZ, &maz, &inst.StorageType, &inst.AllocatedStorage, &enc, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errInstanceNotFound
		}
		return nil, err
	}
	inst.MultiAZ = maz != 0
	inst.Encrypted = enc != 0
	inst.CreatedAt = time.Unix(createdAt, 0)
	return &inst, nil
}

func scanCluster(sc scanner) (*DBCluster, error) {
	var c DBCluster
	var createdAt int64
	var enc int
	err := sc.Scan(&c.ID, &c.ARN, &c.Status, &c.Engine, &c.EngineVersion,
		&c.MasterUser, &c.DBName, &c.Endpoint, &c.ReaderEndpoint, &c.Port, &enc, &createdAt)
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

func scanSnapshot(sc scanner) (*DBSnapshot, error) {
	var sn DBSnapshot
	var createdAt int64
	err := sc.Scan(&sn.ID, &sn.ARN, &sn.InstanceID, &sn.Status, &sn.Engine, &sn.SnapshotType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errSnapshotNotFound
		}
		return nil, err
	}
	sn.CreatedAt = time.Unix(createdAt, 0)
	return &sn, nil
}

func scanClusterSnapshot(sc scanner) (*ClusterSnapshot, error) {
	var sn ClusterSnapshot
	var createdAt int64
	err := sc.Scan(&sn.ID, &sn.ARN, &sn.ClusterID, &sn.Status, &sn.Engine, &sn.SnapshotType, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterSnapNotFound
		}
		return nil, err
	}
	sn.CreatedAt = time.Unix(createdAt, 0)
	return &sn, nil
}

func scanParamGroup(sc scanner) (*DBParamGroup, error) {
	var pg DBParamGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errParamGroupNotFound
		}
		return nil, err
	}
	return &pg, nil
}

func scanClusterParamGroup(sc scanner) (*ClusterParamGroup, error) {
	var pg ClusterParamGroup
	err := sc.Scan(&pg.Name, &pg.ARN, &pg.Family, &pg.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterPGNotFound
		}
		return nil, err
	}
	return &pg, nil
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

func scanOptionGroup(sc scanner) (*OptionGroup, error) {
	var og OptionGroup
	err := sc.Scan(&og.Name, &og.ARN, &og.Engine, &og.MajorVersion, &og.Description)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errOptionGroupNotFound
		}
		return nil, err
	}
	return &og, nil
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
