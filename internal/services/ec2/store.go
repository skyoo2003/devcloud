// SPDX-License-Identifier: Apache-2.0

package ec2

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	ErrInstanceNotFound        = errors.New("instance not found")
	ErrVpcNotFound             = errors.New("vpc not found")
	ErrSubnetNotFound          = errors.New("subnet not found")
	ErrSecurityGroupNotFound   = errors.New("security group not found")
	ErrVolumeNotFound          = errors.New("volume not found")
	ErrSnapshotNotFound        = errors.New("snapshot not found")
	ErrKeyPairNotFound         = errors.New("key pair not found")
	ErrKeyPairAlreadyExists    = errors.New("key pair already exists")
	ErrImageNotFound           = errors.New("image not found")
	ErrRouteTableNotFound      = errors.New("route table not found")
	ErrInternetGatewayNotFound = errors.New("internet gateway not found")
	ErrNetworkACLNotFound      = errors.New("network acl not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS instances (
			instance_id   TEXT PRIMARY KEY,
			image_id      TEXT NOT NULL,
			instance_type TEXT NOT NULL DEFAULT 't2.micro',
			state         TEXT NOT NULL DEFAULT 'running',
			vpc_id        TEXT NOT NULL DEFAULT '',
			subnet_id     TEXT NOT NULL DEFAULT '',
			private_ip    TEXT NOT NULL DEFAULT '',
			account_id    TEXT NOT NULL,
			launched_at   DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS vpcs (
			vpc_id     TEXT PRIMARY KEY,
			cidr_block TEXT NOT NULL,
			state      TEXT NOT NULL DEFAULT 'available',
			account_id TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS subnets (
			subnet_id  TEXT PRIMARY KEY,
			vpc_id     TEXT NOT NULL,
			cidr_block TEXT NOT NULL,
			az         TEXT NOT NULL DEFAULT 'us-east-1a',
			account_id TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS security_groups (
			group_id    TEXT PRIMARY KEY,
			group_name  TEXT NOT NULL,
			vpc_id      TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT '',
			account_id  TEXT NOT NULL
		);
	`},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS tags (
			resource_id TEXT NOT NULL,
			tag_key     TEXT NOT NULL,
			tag_value   TEXT NOT NULL DEFAULT '',
			account_id  TEXT NOT NULL,
			PRIMARY KEY (resource_id, tag_key)
		);
		CREATE TABLE IF NOT EXISTS addresses (
			allocation_id TEXT PRIMARY KEY,
			public_ip     TEXT NOT NULL,
			domain        TEXT NOT NULL DEFAULT 'vpc',
			instance_id   TEXT NOT NULL DEFAULT '',
			account_id    TEXT NOT NULL
		);
	`},
	{Version: 3, SQL: `
		CREATE TABLE IF NOT EXISTS volumes (
			volume_id         TEXT PRIMARY KEY,
			size              INTEGER NOT NULL,
			volume_type       TEXT DEFAULT 'gp3',
			availability_zone TEXT,
			state             TEXT DEFAULT 'available',
			iops              INTEGER,
			encrypted         INTEGER DEFAULT 0,
			kms_key_id        TEXT,
			instance_id       TEXT,
			device            TEXT,
			account_id        TEXT NOT NULL,
			created_at        DATETIME NOT NULL
		);
	`},
	{Version: 4, SQL: `
		CREATE TABLE IF NOT EXISTS ec2_snapshots (
			snapshot_id  TEXT PRIMARY KEY,
			volume_id    TEXT,
			volume_size  INTEGER,
			state        TEXT DEFAULT 'completed',
			description  TEXT,
			account_id   TEXT NOT NULL,
			created_at   DATETIME NOT NULL
		);
	`},
	{Version: 5, SQL: `
		CREATE TABLE IF NOT EXISTS ec2_keypairs (
			key_name        TEXT NOT NULL,
			key_fingerprint TEXT,
			key_material    TEXT,
			key_pair_id     TEXT,
			account_id      TEXT NOT NULL,
			created_at      DATETIME NOT NULL,
			PRIMARY KEY (key_name, account_id)
		);
	`},
	{Version: 6, SQL: `
		CREATE TABLE IF NOT EXISTS ec2_images (
			image_id            TEXT PRIMARY KEY,
			name                TEXT,
			description         TEXT,
			image_type          TEXT DEFAULT 'machine',
			state               TEXT DEFAULT 'available',
			architecture        TEXT DEFAULT 'x86_64',
			root_device_name    TEXT,
			root_device_type    TEXT DEFAULT 'ebs',
			virtualization_type TEXT DEFAULT 'hvm',
			account_id          TEXT NOT NULL,
			created_at          DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS route_tables (
			route_table_id TEXT PRIMARY KEY,
			vpc_id         TEXT NOT NULL,
			account_id     TEXT NOT NULL,
			created_at     DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS routes (
			route_table_id   TEXT NOT NULL,
			destination_cidr TEXT NOT NULL,
			gateway_id       TEXT,
			instance_id      TEXT,
			nat_gateway_id   TEXT,
			state            TEXT DEFAULT 'active',
			PRIMARY KEY (route_table_id, destination_cidr)
		);
		CREATE TABLE IF NOT EXISTS internet_gateways (
			internet_gateway_id TEXT PRIMARY KEY,
			vpc_id              TEXT,
			state               TEXT DEFAULT 'available',
			account_id          TEXT NOT NULL,
			created_at          DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS network_acls (
			network_acl_id TEXT PRIMARY KEY,
			vpc_id         TEXT NOT NULL,
			is_default     INTEGER DEFAULT 0,
			account_id     TEXT NOT NULL,
			created_at     DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS acl_entries (
			network_acl_id TEXT NOT NULL,
			rule_number    INTEGER NOT NULL,
			egress         INTEGER NOT NULL,
			protocol       TEXT,
			rule_action    TEXT,
			cidr_block     TEXT,
			port_from      INTEGER,
			port_to        INTEGER,
			PRIMARY KEY (network_acl_id, rule_number, egress)
		);
	`},
	{Version: 7, SQL: `
		ALTER TABLE vpcs ADD COLUMN enable_dns_support    INTEGER NOT NULL DEFAULT 1;
		ALTER TABLE vpcs ADD COLUMN enable_dns_hostnames  INTEGER NOT NULL DEFAULT 1;
	`},
}

type Instance struct {
	InstanceID   string
	ImageID      string
	InstanceType string
	State        string
	VpcID        string
	SubnetID     string
	PrivateIP    string
	AccountID    string
	LaunchedAt   time.Time
}

type Vpc struct {
	VpcID     string
	CidrBlock string
	State     string
	AccountID string
}

type Subnet struct {
	SubnetID  string
	VpcID     string
	CidrBlock string
	AZ        string
	AccountID string
}

type SecurityGroup struct {
	GroupID     string
	GroupName   string
	VpcID       string
	Description string
	AccountID   string
}

type Tag struct {
	ResourceID string
	Key        string
	Value      string
}

type Address struct {
	AllocationID string
	PublicIP     string
	Domain       string
	InstanceID   string
	AccountID    string
}

type EC2Store struct {
	store *sqlite.Store
}

func NewEC2Store(dataDir string) (*EC2Store, error) {
	dbPath := filepath.Join(dataDir, "ec2.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &EC2Store{store: s}, nil
}

func (s *EC2Store) Close() error { return s.store.Close() }
func (s *EC2Store) db() *sql.DB  { return s.store.DB() }

// RunInstances creates count instances and returns them.
func (s *EC2Store) RunInstances(accountID, imageID, instanceType string, count int) ([]Instance, error) {
	if instanceType == "" {
		instanceType = "t2.micro"
	}
	now := time.Now().UTC()
	instances := make([]Instance, 0, count)
	for i := 0; i < count; i++ {
		id, err := randHex(12)
		if err != nil {
			return nil, err
		}
		instanceID := "i-" + id
		privateIP, err := randPrivateIP()
		if err != nil {
			return nil, err
		}
		_, err = s.db().Exec(
			`INSERT INTO instances (instance_id, image_id, instance_type, state, vpc_id, subnet_id, private_ip, account_id, launched_at)
			 VALUES (?, ?, ?, 'running', '', '', ?, ?, ?)`,
			instanceID, imageID, instanceType, privateIP, accountID, now,
		)
		if err != nil {
			return nil, err
		}
		instances = append(instances, Instance{
			InstanceID:   instanceID,
			ImageID:      imageID,
			InstanceType: instanceType,
			State:        "running",
			PrivateIP:    privateIP,
			AccountID:    accountID,
			LaunchedAt:   now,
		})
	}
	return instances, nil
}

// DescribeInstances returns all instances for the account (optionally filtered by IDs).
func (s *EC2Store) DescribeInstances(accountID string, ids []string) ([]Instance, error) {
	query := `SELECT instance_id, image_id, instance_type, state, vpc_id, subnet_id, private_ip, account_id, launched_at
	          FROM instances WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := ""
		for i, id := range ids {
			if i > 0 {
				placeholders += ","
			}
			placeholders += "?"
			args = append(args, id)
		}
		query += " AND instance_id IN (" + placeholders + ")"
	}
	query += " ORDER BY launched_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var inst Instance
		if err := rows.Scan(&inst.InstanceID, &inst.ImageID, &inst.InstanceType, &inst.State,
			&inst.VpcID, &inst.SubnetID, &inst.PrivateIP, &inst.AccountID, &inst.LaunchedAt); err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

// TerminateInstances sets state=terminated for the given instance IDs.
// Returns ErrInstanceNotFound if any instance ID does not exist.
func (s *EC2Store) TerminateInstances(accountID string, ids []string) error {
	for _, id := range ids {
		res, err := s.db().Exec(
			`UPDATE instances SET state='terminated' WHERE instance_id=? AND account_id=?`,
			id, accountID,
		)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
	}
	return nil
}

// CreateVpc creates a new VPC.
func (s *EC2Store) CreateVpc(accountID, cidrBlock string) (*Vpc, error) {
	id, err := randHex(8)
	if err != nil {
		return nil, err
	}
	vpcID := "vpc-" + id
	_, err = s.db().Exec(
		`INSERT INTO vpcs (vpc_id, cidr_block, state, account_id) VALUES (?, ?, 'available', ?)`,
		vpcID, cidrBlock, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &Vpc{VpcID: vpcID, CidrBlock: cidrBlock, State: "available", AccountID: accountID}, nil
}

// DescribeVpcs returns all VPCs for the account.
func (s *EC2Store) DescribeVpcs(accountID string) ([]Vpc, error) {
	rows, err := s.db().Query(
		`SELECT vpc_id, cidr_block, state, account_id FROM vpcs WHERE account_id=? ORDER BY vpc_id`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Vpc
	for rows.Next() {
		var v Vpc
		if err := rows.Scan(&v.VpcID, &v.CidrBlock, &v.State, &v.AccountID); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// DeleteVpc deletes a VPC by ID.
func (s *EC2Store) DeleteVpc(accountID, vpcID string) error {
	res, err := s.db().Exec(`DELETE FROM vpcs WHERE vpc_id=? AND account_id=?`, vpcID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrVpcNotFound
	}
	return nil
}

// CreateSubnet creates a new subnet inside a VPC.
func (s *EC2Store) CreateSubnet(accountID, vpcID, cidrBlock, az string) (*Subnet, error) {
	if az == "" {
		az = "us-east-1a"
	}
	id, err := randHex(8)
	if err != nil {
		return nil, err
	}
	subnetID := "subnet-" + id
	_, err = s.db().Exec(
		`INSERT INTO subnets (subnet_id, vpc_id, cidr_block, az, account_id) VALUES (?, ?, ?, ?, ?)`,
		subnetID, vpcID, cidrBlock, az, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &Subnet{SubnetID: subnetID, VpcID: vpcID, CidrBlock: cidrBlock, AZ: az, AccountID: accountID}, nil
}

// DescribeSubnets returns all subnets for the account.
func (s *EC2Store) DescribeSubnets(accountID string) ([]Subnet, error) {
	rows, err := s.db().Query(
		`SELECT subnet_id, vpc_id, cidr_block, az, account_id FROM subnets WHERE account_id=? ORDER BY subnet_id`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Subnet
	for rows.Next() {
		var sub Subnet
		if err := rows.Scan(&sub.SubnetID, &sub.VpcID, &sub.CidrBlock, &sub.AZ, &sub.AccountID); err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// CreateSecurityGroup creates a new security group.
func (s *EC2Store) CreateSecurityGroup(accountID, groupName, vpcID, description string) (*SecurityGroup, error) {
	id, err := randHex(8)
	if err != nil {
		return nil, err
	}
	groupID := "sg-" + id
	_, err = s.db().Exec(
		`INSERT INTO security_groups (group_id, group_name, vpc_id, description, account_id) VALUES (?, ?, ?, ?, ?)`,
		groupID, groupName, vpcID, description, accountID,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, fmt.Errorf("security group %q already exists", groupName)
		}
		return nil, err
	}
	return &SecurityGroup{GroupID: groupID, GroupName: groupName, VpcID: vpcID, Description: description, AccountID: accountID}, nil
}

// DescribeSecurityGroups returns all security groups for the account.
func (s *EC2Store) DescribeSecurityGroups(accountID string) ([]SecurityGroup, error) {
	rows, err := s.db().Query(
		`SELECT group_id, group_name, vpc_id, description, account_id FROM security_groups WHERE account_id=? ORDER BY group_id`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SecurityGroup
	for rows.Next() {
		var sg SecurityGroup
		if err := rows.Scan(&sg.GroupID, &sg.GroupName, &sg.VpcID, &sg.Description, &sg.AccountID); err != nil {
			return nil, err
		}
		out = append(out, sg)
	}
	return out, rows.Err()
}

// CreateTags stores tags on the given resource IDs.
func (s *EC2Store) CreateTags(accountID string, resourceIDs []string, tags []Tag) error {
	for _, resID := range resourceIDs {
		for _, t := range tags {
			_, err := s.db().Exec(
				`INSERT OR REPLACE INTO tags (resource_id, tag_key, tag_value, account_id) VALUES (?, ?, ?, ?)`,
				resID, t.Key, t.Value, accountID,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// GetTags returns all tags for a given resource.
func (s *EC2Store) GetTags(resourceID string) ([]Tag, error) {
	rows, err := s.db().Query(
		`SELECT resource_id, tag_key, tag_value FROM tags WHERE resource_id=?`, resourceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tag
	for rows.Next() {
		var t Tag
		if err := rows.Scan(&t.ResourceID, &t.Key, &t.Value); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AllocateAddress creates a new Elastic IP address.
func (s *EC2Store) AllocateAddress(accountID, domain string) (*Address, error) {
	if domain == "" {
		domain = "vpc"
	}
	id, err := randHex(8)
	if err != nil {
		return nil, err
	}
	allocID := "eipalloc-" + id
	ip, err := randPublicIP()
	if err != nil {
		return nil, err
	}
	_, err = s.db().Exec(
		`INSERT INTO addresses (allocation_id, public_ip, domain, instance_id, account_id) VALUES (?, ?, ?, '', ?)`,
		allocID, ip, domain, accountID,
	)
	if err != nil {
		return nil, err
	}
	return &Address{AllocationID: allocID, PublicIP: ip, Domain: domain, AccountID: accountID}, nil
}

// ---- Volume operations ----

type Volume struct {
	VolumeID         string
	Size             int
	VolumeType       string
	AvailabilityZone string
	State            string
	Iops             int
	Encrypted        bool
	KmsKeyID         string
	InstanceID       string
	Device           string
	AccountID        string
	CreatedAt        time.Time
}

func (s *EC2Store) CreateVolume(accountID string, size int, az, volumeType string, iops int, encrypted bool, kmsKeyID string) (*Volume, error) {
	if volumeType == "" {
		volumeType = "gp3"
	}
	if az == "" {
		az = "us-east-1a"
	}
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	volumeID := "vol-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO volumes (volume_id, size, volume_type, availability_zone, state, iops, encrypted, kms_key_id, instance_id, device, account_id, created_at)
		 VALUES (?, ?, ?, ?, 'available', ?, ?, ?, '', '', ?, ?)`,
		volumeID, size, volumeType, az, iops, boolToInt(encrypted), kmsKeyID, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Volume{
		VolumeID: volumeID, Size: size, VolumeType: volumeType, AvailabilityZone: az,
		State: "available", Iops: iops, Encrypted: encrypted, KmsKeyID: kmsKeyID,
		AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *EC2Store) DescribeVolumes(accountID string, ids []string) ([]Volume, error) {
	query := `SELECT volume_id, size, volume_type, availability_zone, state, iops, encrypted, kms_key_id, instance_id, device, account_id, created_at
	          FROM volumes WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND volume_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Volume
	for rows.Next() {
		var v Volume
		var enc int
		if err := rows.Scan(&v.VolumeID, &v.Size, &v.VolumeType, &v.AvailabilityZone, &v.State,
			&v.Iops, &enc, &v.KmsKeyID, &v.InstanceID, &v.Device, &v.AccountID, &v.CreatedAt); err != nil {
			return nil, err
		}
		v.Encrypted = enc != 0
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeleteVolume(accountID, volumeID string) error {
	res, err := s.db().Exec(`DELETE FROM volumes WHERE volume_id=? AND account_id=?`, volumeID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrVolumeNotFound
	}
	return nil
}

func (s *EC2Store) AttachVolume(accountID, volumeID, instanceID, device string) (*Volume, error) {
	res, err := s.db().Exec(
		`UPDATE volumes SET instance_id=?, device=?, state='in-use' WHERE volume_id=? AND account_id=?`,
		instanceID, device, volumeID, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrVolumeNotFound
	}
	vols, err := s.DescribeVolumes(accountID, []string{volumeID})
	if err != nil || len(vols) == 0 {
		return nil, ErrVolumeNotFound
	}
	return &vols[0], nil
}

func (s *EC2Store) DetachVolume(accountID, volumeID string) (*Volume, error) {
	res, err := s.db().Exec(
		`UPDATE volumes SET instance_id='', device='', state='available' WHERE volume_id=? AND account_id=?`,
		volumeID, accountID,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrVolumeNotFound
	}
	vols, err := s.DescribeVolumes(accountID, []string{volumeID})
	if err != nil || len(vols) == 0 {
		return nil, ErrVolumeNotFound
	}
	return &vols[0], nil
}

func (s *EC2Store) ModifyVolume(accountID, volumeID string, size int, volumeType string, iops int) (*Volume, error) {
	setParts := []string{}
	args := []any{}
	if size > 0 {
		setParts = append(setParts, "size=?")
		args = append(args, size)
	}
	if volumeType != "" {
		setParts = append(setParts, "volume_type=?")
		args = append(args, volumeType)
	}
	if iops > 0 {
		setParts = append(setParts, "iops=?")
		args = append(args, iops)
	}
	if len(setParts) == 0 {
		vols, err := s.DescribeVolumes(accountID, []string{volumeID})
		if err != nil || len(vols) == 0 {
			return nil, ErrVolumeNotFound
		}
		return &vols[0], nil
	}
	args = append(args, volumeID, accountID)
	res, err := s.db().Exec(
		`UPDATE volumes SET `+strings.Join(setParts, ",")+` WHERE volume_id=? AND account_id=?`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrVolumeNotFound
	}
	vols, err := s.DescribeVolumes(accountID, []string{volumeID})
	if err != nil || len(vols) == 0 {
		return nil, ErrVolumeNotFound
	}
	return &vols[0], nil
}

// ---- Snapshot operations ----

type Snapshot struct {
	SnapshotID  string
	VolumeID    string
	VolumeSize  int
	State       string
	Description string
	AccountID   string
	CreatedAt   time.Time
}

func (s *EC2Store) CreateSnapshot(accountID, volumeID, description string) (*Snapshot, error) {
	var volumeSize int
	row := s.db().QueryRow(`SELECT size FROM volumes WHERE volume_id=? AND account_id=?`, volumeID, accountID)
	_ = row.Scan(&volumeSize)

	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	snapshotID := "snap-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_snapshots (snapshot_id, volume_id, volume_size, state, description, account_id, created_at)
		 VALUES (?, ?, ?, 'completed', ?, ?, ?)`,
		snapshotID, volumeID, volumeSize, description, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		SnapshotID: snapshotID, VolumeID: volumeID, VolumeSize: volumeSize,
		State: "completed", Description: description, AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *EC2Store) DescribeSnapshots(accountID string, ids []string) ([]Snapshot, error) {
	query := `SELECT snapshot_id, volume_id, volume_size, state, description, account_id, created_at
	          FROM ec2_snapshots WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND snapshot_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		var snap Snapshot
		if err := rows.Scan(&snap.SnapshotID, &snap.VolumeID, &snap.VolumeSize, &snap.State,
			&snap.Description, &snap.AccountID, &snap.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, snap)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeleteSnapshot(accountID, snapshotID string) error {
	res, err := s.db().Exec(`DELETE FROM ec2_snapshots WHERE snapshot_id=? AND account_id=?`, snapshotID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrSnapshotNotFound
	}
	return nil
}

func (s *EC2Store) CopySnapshot(accountID, sourceSnapshotID, description string) (*Snapshot, error) {
	var srcVolumeID string
	var srcVolumeSize int
	row := s.db().QueryRow(`SELECT volume_id, volume_size FROM ec2_snapshots WHERE snapshot_id=?`, sourceSnapshotID)
	_ = row.Scan(&srcVolumeID, &srcVolumeSize)

	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	snapshotID := "snap-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_snapshots (snapshot_id, volume_id, volume_size, state, description, account_id, created_at)
		 VALUES (?, ?, ?, 'completed', ?, ?, ?)`,
		snapshotID, srcVolumeID, srcVolumeSize, description, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		SnapshotID: snapshotID, VolumeID: srcVolumeID, VolumeSize: srcVolumeSize,
		State: "completed", Description: description, AccountID: accountID, CreatedAt: now,
	}, nil
}

// ---- KeyPair operations ----

type KeyPair struct {
	KeyName        string
	KeyFingerprint string
	KeyMaterial    string
	KeyPairID      string
	AccountID      string
	CreatedAt      time.Time
}

func (s *EC2Store) CreateKeyPair(accountID, keyName string) (*KeyPair, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	keyPairID := "key-" + id
	material := mockPrivateKey()
	fingerprint, ferr := randHex(20)
	if ferr != nil {
		return nil, ferr
	}
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_keypairs (key_name, key_fingerprint, key_material, key_pair_id, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		keyName, fingerprint, material, keyPairID, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrKeyPairAlreadyExists
		}
		return nil, err
	}
	return &KeyPair{
		KeyName: keyName, KeyFingerprint: fingerprint, KeyMaterial: material,
		KeyPairID: keyPairID, AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *EC2Store) DescribeKeyPairs(accountID string, names []string) ([]KeyPair, error) {
	query := `SELECT key_name, key_fingerprint, key_material, key_pair_id, account_id, created_at
	          FROM ec2_keypairs WHERE account_id = ?`
	args := []any{accountID}
	if len(names) > 0 {
		placeholders := make([]string, len(names))
		for i, n := range names {
			placeholders[i] = "?"
			args = append(args, n)
		}
		query += " AND key_name IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY key_name"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyPair
	for rows.Next() {
		var kp KeyPair
		if err := rows.Scan(&kp.KeyName, &kp.KeyFingerprint, &kp.KeyMaterial, &kp.KeyPairID, &kp.AccountID, &kp.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, kp)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeleteKeyPair(accountID, keyName string) error {
	res, err := s.db().Exec(`DELETE FROM ec2_keypairs WHERE key_name=? AND account_id=?`, keyName, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrKeyPairNotFound
	}
	return nil
}

func (s *EC2Store) ImportKeyPair(accountID, keyName, publicKeyMaterial string) (*KeyPair, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	keyPairID := "key-" + id
	fingerprint, ferr := randHex(20)
	if ferr != nil {
		return nil, ferr
	}
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_keypairs (key_name, key_fingerprint, key_material, key_pair_id, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		keyName, fingerprint, publicKeyMaterial, keyPairID, accountID, now,
	)
	if err != nil {
		if sqlite.IsUniqueConstraintError(err) {
			return nil, ErrKeyPairAlreadyExists
		}
		return nil, err
	}
	return &KeyPair{
		KeyName: keyName, KeyFingerprint: fingerprint, KeyMaterial: publicKeyMaterial,
		KeyPairID: keyPairID, AccountID: accountID, CreatedAt: now,
	}, nil
}

// ---- Image/AMI operations ----

type Image struct {
	ImageID            string
	Name               string
	Description        string
	ImageType          string
	State              string
	Architecture       string
	RootDeviceName     string
	RootDeviceType     string
	VirtualizationType string
	AccountID          string
	CreatedAt          time.Time
}

func (s *EC2Store) CreateImage(accountID, instanceID, name, description string) (*Image, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	imageID := "ami-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_images (image_id, name, description, image_type, state, architecture, root_device_name, root_device_type, virtualization_type, account_id, created_at)
		 VALUES (?, ?, ?, 'machine', 'available', 'x86_64', '/dev/xvda', 'ebs', 'hvm', ?, ?)`,
		imageID, name, description, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Image{
		ImageID: imageID, Name: name, Description: description, ImageType: "machine",
		State: "available", Architecture: "x86_64", RootDeviceName: "/dev/xvda",
		RootDeviceType: "ebs", VirtualizationType: "hvm", AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *EC2Store) RegisterImage(accountID, name, architecture, rootDeviceName string) (*Image, error) {
	if architecture == "" {
		architecture = "x86_64"
	}
	if rootDeviceName == "" {
		rootDeviceName = "/dev/xvda"
	}
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	imageID := "ami-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_images (image_id, name, description, image_type, state, architecture, root_device_name, root_device_type, virtualization_type, account_id, created_at)
		 VALUES (?, ?, '', 'machine', 'available', ?, ?, 'ebs', 'hvm', ?, ?)`,
		imageID, name, architecture, rootDeviceName, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Image{
		ImageID: imageID, Name: name, ImageType: "machine", State: "available",
		Architecture: architecture, RootDeviceName: rootDeviceName, RootDeviceType: "ebs",
		VirtualizationType: "hvm", AccountID: accountID, CreatedAt: now,
	}, nil
}

func (s *EC2Store) DescribeImages(accountID string, ids []string) ([]Image, error) {
	query := `SELECT image_id, name, description, image_type, state, architecture, root_device_name, root_device_type, virtualization_type, account_id, created_at
	          FROM ec2_images WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND image_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Image
	for rows.Next() {
		var img Image
		if err := rows.Scan(&img.ImageID, &img.Name, &img.Description, &img.ImageType, &img.State,
			&img.Architecture, &img.RootDeviceName, &img.RootDeviceType, &img.VirtualizationType,
			&img.AccountID, &img.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, img)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeregisterImage(accountID, imageID string) error {
	res, err := s.db().Exec(`DELETE FROM ec2_images WHERE image_id=? AND account_id=?`, imageID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrImageNotFound
	}
	return nil
}

func (s *EC2Store) CopyImage(accountID, sourceImageID, name, description string) (*Image, error) {
	var srcArch, srcRootDevice string
	row := s.db().QueryRow(`SELECT architecture, root_device_name FROM ec2_images WHERE image_id=?`, sourceImageID)
	if err := row.Scan(&srcArch, &srcRootDevice); err != nil {
		srcArch = "x86_64"
		srcRootDevice = "/dev/xvda"
	}
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	imageID := "ami-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO ec2_images (image_id, name, description, image_type, state, architecture, root_device_name, root_device_type, virtualization_type, account_id, created_at)
		 VALUES (?, ?, ?, 'machine', 'available', ?, ?, 'ebs', 'hvm', ?, ?)`,
		imageID, name, description, srcArch, srcRootDevice, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &Image{
		ImageID: imageID, Name: name, Description: description, ImageType: "machine",
		State: "available", Architecture: srcArch, RootDeviceName: srcRootDevice,
		RootDeviceType: "ebs", VirtualizationType: "hvm", AccountID: accountID, CreatedAt: now,
	}, nil
}

// ---- RouteTable operations ----

type RouteTable struct {
	RouteTableID string
	VpcID        string
	AccountID    string
	CreatedAt    time.Time
	Routes       []Route
}

type Route struct {
	RouteTableID    string
	DestinationCIDR string
	GatewayID       string
	InstanceID      string
	NatGatewayID    string
	State           string
}

func (s *EC2Store) CreateRouteTable(accountID, vpcID string) (*RouteTable, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	rtID := "rtb-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO route_tables (route_table_id, vpc_id, account_id, created_at) VALUES (?, ?, ?, ?)`,
		rtID, vpcID, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &RouteTable{RouteTableID: rtID, VpcID: vpcID, AccountID: accountID, CreatedAt: now}, nil
}

func (s *EC2Store) DescribeRouteTables(accountID string, ids []string) ([]RouteTable, error) {
	query := `SELECT route_table_id, vpc_id, account_id, created_at FROM route_tables WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND route_table_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RouteTable
	for rows.Next() {
		var rt RouteTable
		if err := rows.Scan(&rt.RouteTableID, &rt.VpcID, &rt.AccountID, &rt.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, rt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		routes, err := s.listRoutes(out[i].RouteTableID)
		if err != nil {
			return nil, err
		}
		out[i].Routes = routes
	}
	return out, nil
}

func (s *EC2Store) listRoutes(routeTableID string) ([]Route, error) {
	rows, err := s.db().Query(
		`SELECT route_table_id, destination_cidr, COALESCE(gateway_id,''), COALESCE(instance_id,''), COALESCE(nat_gateway_id,''), state
		 FROM routes WHERE route_table_id=?`, routeTableID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Route
	for rows.Next() {
		var r Route
		if err := rows.Scan(&r.RouteTableID, &r.DestinationCIDR, &r.GatewayID, &r.InstanceID, &r.NatGatewayID, &r.State); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeleteRouteTable(accountID, routeTableID string) error {
	res, err := s.db().Exec(`DELETE FROM route_tables WHERE route_table_id=? AND account_id=?`, routeTableID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrRouteTableNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM routes WHERE route_table_id=?`, routeTableID)
	return nil
}

func (s *EC2Store) CreateRoute(accountID, routeTableID, destinationCIDR, gatewayID, instanceID string) error {
	_, err := s.db().Exec(
		`INSERT OR REPLACE INTO routes (route_table_id, destination_cidr, gateway_id, instance_id, state) VALUES (?, ?, ?, ?, 'active')`,
		routeTableID, destinationCIDR, gatewayID, instanceID,
	)
	return err
}

func (s *EC2Store) DeleteRoute(accountID, routeTableID, destinationCIDR string) error {
	_, err := s.db().Exec(
		`DELETE FROM routes WHERE route_table_id=? AND destination_cidr=?`,
		routeTableID, destinationCIDR,
	)
	return err
}

func (s *EC2Store) AssociateRouteTable(accountID, routeTableID, subnetID string) (string, error) {
	id, err := randHex(17)
	if err != nil {
		return "", err
	}
	assocID := "rtbassoc-" + id
	return assocID, nil
}

// ---- InternetGateway operations ----

type InternetGateway struct {
	InternetGatewayID string
	VpcID             string
	State             string
	AccountID         string
	CreatedAt         time.Time
}

func (s *EC2Store) CreateInternetGateway(accountID string) (*InternetGateway, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	igwID := "igw-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO internet_gateways (internet_gateway_id, vpc_id, state, account_id, created_at) VALUES (?, '', 'available', ?, ?)`,
		igwID, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &InternetGateway{InternetGatewayID: igwID, State: "available", AccountID: accountID, CreatedAt: now}, nil
}

func (s *EC2Store) DescribeInternetGateways(accountID string, ids []string) ([]InternetGateway, error) {
	query := `SELECT internet_gateway_id, COALESCE(vpc_id,''), state, account_id, created_at FROM internet_gateways WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND internet_gateway_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InternetGateway
	for rows.Next() {
		var igw InternetGateway
		if err := rows.Scan(&igw.InternetGatewayID, &igw.VpcID, &igw.State, &igw.AccountID, &igw.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, igw)
	}
	return out, rows.Err()
}

func (s *EC2Store) AttachInternetGateway(accountID, igwID, vpcID string) error {
	res, err := s.db().Exec(
		`UPDATE internet_gateways SET vpc_id=?, state='available' WHERE internet_gateway_id=? AND account_id=?`,
		vpcID, igwID, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrInternetGatewayNotFound
	}
	return nil
}

func (s *EC2Store) DetachInternetGateway(accountID, igwID, vpcID string) error {
	res, err := s.db().Exec(
		`UPDATE internet_gateways SET vpc_id='', state='available' WHERE internet_gateway_id=? AND account_id=?`,
		igwID, accountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrInternetGatewayNotFound
	}
	return nil
}

// ---- NetworkACL operations ----

type NetworkACL struct {
	NetworkACLID string
	VpcID        string
	IsDefault    bool
	AccountID    string
	CreatedAt    time.Time
	Entries      []ACLEntry
}

type ACLEntry struct {
	NetworkACLID string
	RuleNumber   int
	Egress       bool
	Protocol     string
	RuleAction   string
	CidrBlock    string
	PortFrom     int
	PortTo       int
}

func (s *EC2Store) CreateNetworkACL(accountID, vpcID string) (*NetworkACL, error) {
	id, err := randHex(17)
	if err != nil {
		return nil, err
	}
	aclID := "acl-" + id
	now := time.Now().UTC()
	_, err = s.db().Exec(
		`INSERT INTO network_acls (network_acl_id, vpc_id, is_default, account_id, created_at) VALUES (?, ?, 0, ?, ?)`,
		aclID, vpcID, accountID, now,
	)
	if err != nil {
		return nil, err
	}
	return &NetworkACL{NetworkACLID: aclID, VpcID: vpcID, IsDefault: false, AccountID: accountID, CreatedAt: now}, nil
}

func (s *EC2Store) DescribeNetworkACLs(accountID string, ids []string) ([]NetworkACL, error) {
	query := `SELECT network_acl_id, vpc_id, is_default, account_id, created_at FROM network_acls WHERE account_id = ?`
	args := []any{accountID}
	if len(ids) > 0 {
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query += " AND network_acl_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY created_at"
	rows, err := s.db().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NetworkACL
	for rows.Next() {
		var acl NetworkACL
		var isDefault int
		if err := rows.Scan(&acl.NetworkACLID, &acl.VpcID, &isDefault, &acl.AccountID, &acl.CreatedAt); err != nil {
			return nil, err
		}
		acl.IsDefault = isDefault != 0
		out = append(out, acl)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		entries, err := s.listACLEntries(out[i].NetworkACLID)
		if err != nil {
			return nil, err
		}
		out[i].Entries = entries
	}
	return out, nil
}

func (s *EC2Store) listACLEntries(networkACLID string) ([]ACLEntry, error) {
	rows, err := s.db().Query(
		`SELECT network_acl_id, rule_number, egress, COALESCE(protocol,''), COALESCE(rule_action,''), COALESCE(cidr_block,''), COALESCE(port_from,0), COALESCE(port_to,0)
		 FROM acl_entries WHERE network_acl_id=? ORDER BY rule_number`, networkACLID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ACLEntry
	for rows.Next() {
		var e ACLEntry
		var egress int
		if err := rows.Scan(&e.NetworkACLID, &e.RuleNumber, &egress, &e.Protocol, &e.RuleAction, &e.CidrBlock, &e.PortFrom, &e.PortTo); err != nil {
			return nil, err
		}
		e.Egress = egress != 0
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *EC2Store) DeleteNetworkACL(accountID, networkACLID string) error {
	res, err := s.db().Exec(`DELETE FROM network_acls WHERE network_acl_id=? AND account_id=?`, networkACLID, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNetworkACLNotFound
	}
	_, _ = s.db().Exec(`DELETE FROM acl_entries WHERE network_acl_id=?`, networkACLID)
	return nil
}

func (s *EC2Store) CreateNetworkACLEntry(accountID, networkACLID string, ruleNumber int, protocol, ruleAction, cidrBlock string, egress bool, portFrom, portTo int) error {
	_, err := s.db().Exec(
		`INSERT OR REPLACE INTO acl_entries (network_acl_id, rule_number, egress, protocol, rule_action, cidr_block, port_from, port_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		networkACLID, ruleNumber, boolToInt(egress), protocol, ruleAction, cidrBlock, portFrom, portTo,
	)
	return err
}

func (s *EC2Store) ReplaceNetworkACLEntry(accountID, networkACLID string, ruleNumber int, protocol, ruleAction, cidrBlock string, egress bool, portFrom, portTo int) error {
	return s.CreateNetworkACLEntry(accountID, networkACLID, ruleNumber, protocol, ruleAction, cidrBlock, egress, portFrom, portTo)
}

// ---- VPC Attribute operations ----

func (s *EC2Store) ModifyVpcAttribute(accountID, vpcID string, enableDnsSupport, enableDnsHostnames *bool) error {
	if enableDnsSupport == nil && enableDnsHostnames == nil {
		return nil
	}
	setParts := []string{}
	args := []any{}
	if enableDnsSupport != nil {
		setParts = append(setParts, "enable_dns_support=?")
		args = append(args, boolToInt(*enableDnsSupport))
	}
	if enableDnsHostnames != nil {
		setParts = append(setParts, "enable_dns_hostnames=?")
		args = append(args, boolToInt(*enableDnsHostnames))
	}
	args = append(args, vpcID, accountID)
	res, err := s.db().Exec(
		`UPDATE vpcs SET `+strings.Join(setParts, ",")+` WHERE vpc_id=? AND account_id=?`,
		args...,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrVpcNotFound
	}
	return nil
}

func (s *EC2Store) DescribeVpcAttribute(accountID, vpcID, attribute string) (bool, error) {
	var enableDnsSupport, enableDnsHostnames int
	row := s.db().QueryRow(
		`SELECT enable_dns_support, enable_dns_hostnames FROM vpcs WHERE vpc_id=? AND account_id=?`,
		vpcID, accountID,
	)
	if err := row.Scan(&enableDnsSupport, &enableDnsHostnames); err != nil {
		return false, ErrVpcNotFound
	}
	switch attribute {
	case "enableDnsSupport":
		return enableDnsSupport != 0, nil
	case "enableDnsHostnames":
		return enableDnsHostnames != 0, nil
	default:
		return false, fmt.Errorf("unknown attribute: %s", attribute)
	}
}

// --- helpers ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mockPrivateKey() string {
	s, _ := randHex(64)
	return "-----BEGIN RSA PRIVATE KEY-----\n" + s + "\n-----END RSA PRIVATE KEY-----\n"
}

func randHex(nBytes int) (string, error) {
	b := make([]byte, nBytes/2+nBytes%2)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b)[:nBytes], nil
}

func randPublicIP() (string, error) {
	a, err := rand.Int(rand.Reader, big.NewInt(223))
	if err != nil {
		return "", err
	}
	b, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		return "", err
	}
	c, err := rand.Int(rand.Reader, big.NewInt(256))
	if err != nil {
		return "", err
	}
	d, err := rand.Int(rand.Reader, big.NewInt(254))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d.%d.%d.%d", a.Int64()+1, b.Int64(), c.Int64(), d.Int64()+1), nil
}

func randPrivateIP() (string, error) {
	b, err := rand.Int(rand.Reader, big.NewInt(254))
	if err != nil {
		return "", err
	}
	c, err := rand.Int(rand.Reader, big.NewInt(254))
	if err != nil {
		return "", err
	}
	d, err := rand.Int(rand.Reader, big.NewInt(254))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("10.%d.%d.%d", b.Int64()+1, c.Int64()+1, d.Int64()+1), nil
}
