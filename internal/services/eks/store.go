// SPDX-License-Identifier: Apache-2.0

// internal/services/eks/store.go
package eks

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errClusterNotFound        = errors.New("cluster not found")
	errNodegroupNotFound      = errors.New("nodegroup not found")
	errFargateProfileNotFound = errors.New("fargate profile not found")
	errAddonNotFound          = errors.New("addon not found")
	errAccessEntryNotFound    = errors.New("access entry not found")
	errPodIdentityNotFound    = errors.New("pod identity association not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			name             TEXT PRIMARY KEY,
			arn              TEXT NOT NULL UNIQUE,
			status           TEXT NOT NULL DEFAULT 'ACTIVE',
			version          TEXT NOT NULL DEFAULT '1.29',
			role_arn         TEXT NOT NULL DEFAULT '',
			endpoint         TEXT NOT NULL DEFAULT '',
			certificate      TEXT NOT NULL DEFAULT '',
			platform_version TEXT NOT NULL DEFAULT 'eks.1',
			config           TEXT NOT NULL DEFAULT '{}',
			created_at       INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS nodegroups (
			name           TEXT NOT NULL,
			cluster_name   TEXT NOT NULL,
			arn            TEXT NOT NULL UNIQUE,
			status         TEXT NOT NULL DEFAULT 'ACTIVE',
			instance_types TEXT NOT NULL DEFAULT '["t3.medium"]',
			desired_size   INTEGER NOT NULL DEFAULT 2,
			min_size       INTEGER NOT NULL DEFAULT 1,
			max_size       INTEGER NOT NULL DEFAULT 3,
			ami_type       TEXT NOT NULL DEFAULT 'AL2_x86_64',
			node_role      TEXT NOT NULL DEFAULT '',
			subnets        TEXT NOT NULL DEFAULT '[]',
			created_at     INTEGER NOT NULL,
			PRIMARY KEY (name, cluster_name)
		);
		CREATE TABLE IF NOT EXISTS fargate_profiles (
			name                TEXT NOT NULL,
			cluster_name        TEXT NOT NULL,
			arn                 TEXT NOT NULL UNIQUE,
			status              TEXT NOT NULL DEFAULT 'ACTIVE',
			pod_execution_role  TEXT NOT NULL DEFAULT '',
			selectors           TEXT NOT NULL DEFAULT '[]',
			subnets             TEXT NOT NULL DEFAULT '[]',
			created_at          INTEGER NOT NULL,
			PRIMARY KEY (name, cluster_name)
		);
		CREATE TABLE IF NOT EXISTS addons (
			name          TEXT NOT NULL,
			cluster_name  TEXT NOT NULL,
			arn           TEXT NOT NULL UNIQUE,
			status        TEXT NOT NULL DEFAULT 'ACTIVE',
			addon_version TEXT NOT NULL DEFAULT '',
			service_role  TEXT NOT NULL DEFAULT '',
			config        TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL,
			PRIMARY KEY (name, cluster_name)
		);
		CREATE TABLE IF NOT EXISTS access_entries (
			principal_arn TEXT NOT NULL,
			cluster_name  TEXT NOT NULL,
			arn           TEXT NOT NULL UNIQUE,
			entry_type    TEXT NOT NULL DEFAULT 'STANDARD',
			kubernetes_groups TEXT NOT NULL DEFAULT '[]',
			username      TEXT NOT NULL DEFAULT '',
			created_at    INTEGER NOT NULL,
			PRIMARY KEY (principal_arn, cluster_name)
		);
		CREATE TABLE IF NOT EXISTS pod_identity_associations (
			association_id TEXT PRIMARY KEY,
			cluster_name   TEXT NOT NULL,
			arn            TEXT NOT NULL UNIQUE,
			namespace      TEXT NOT NULL DEFAULT '',
			service_account TEXT NOT NULL DEFAULT '',
			role_arn       TEXT NOT NULL DEFAULT '',
			created_at     INTEGER NOT NULL
		);
	`},
}

// --- Model types ---

type Cluster struct {
	Name            string
	ARN             string
	Status          string
	Version         string
	RoleARN         string
	Endpoint        string
	Certificate     string
	PlatformVersion string
	Config          string
	CreatedAt       time.Time
}

type Nodegroup struct {
	Name          string
	ClusterName   string
	ARN           string
	Status        string
	InstanceTypes string
	DesiredSize   int
	MinSize       int
	MaxSize       int
	AMIType       string
	NodeRole      string
	Subnets       string
	CreatedAt     time.Time
}

type FargateProfile struct {
	Name             string
	ClusterName      string
	ARN              string
	Status           string
	PodExecutionRole string
	Selectors        string
	Subnets          string
	CreatedAt        time.Time
}

type Addon struct {
	Name         string
	ClusterName  string
	ARN          string
	Status       string
	AddonVersion string
	ServiceRole  string
	Config       string
	CreatedAt    time.Time
}

type AccessEntry struct {
	PrincipalARN     string
	ClusterName      string
	ARN              string
	EntryType        string
	KubernetesGroups string
	Username         string
	CreatedAt        time.Time
}

type PodIdentityAssociation struct {
	AssociationID  string
	ClusterName    string
	ARN            string
	Namespace      string
	ServiceAccount string
	RoleARN        string
	CreatedAt      time.Time
}

// --- Store ---

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "eks.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Cluster ---

func (s *Store) CreateCluster(name, arn, version, roleARN string) (*Cluster, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO clusters (name, arn, version, role_arn, created_at) VALUES (?, ?, ?, ?, ?)`,
		name, arn, version, roleARN, now,
	)
	if err != nil {
		return nil, err
	}
	return &Cluster{
		Name:            name,
		ARN:             arn,
		Status:          "ACTIVE",
		Version:         version,
		RoleARN:         roleARN,
		PlatformVersion: "eks.1",
		Config:          "{}",
		CreatedAt:       time.Unix(now, 0),
	}, nil
}

func (s *Store) GetCluster(name string) (*Cluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, version, role_arn, endpoint, certificate, platform_version, config, created_at
		 FROM clusters WHERE name = ?`, name)
	return scanCluster(row)
}

func (s *Store) ListClusters() ([]Cluster, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, arn, status, version, role_arn, endpoint, certificate, platform_version, config, created_at
		 FROM clusters ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (s *Store) DeleteCluster(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM clusters WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) UpdateClusterConfig(name, config string) error {
	res, err := s.store.DB().Exec(`UPDATE clusters SET config=? WHERE name=?`, config, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) UpdateClusterVersion(name, version string) error {
	res, err := s.store.DB().Exec(`UPDATE clusters SET version=? WHERE name=?`, version, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

// --- Nodegroup ---

func (s *Store) CreateNodegroup(name, clusterName, arn, nodeRole, amiType string, desiredSize, minSize, maxSize int, instanceTypes, subnets string) (*Nodegroup, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO nodegroups (name, cluster_name, arn, node_role, ami_type, desired_size, min_size, max_size, instance_types, subnets, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, clusterName, arn, nodeRole, amiType, desiredSize, minSize, maxSize, instanceTypes, subnets, now,
	)
	if err != nil {
		return nil, err
	}
	return &Nodegroup{
		Name:          name,
		ClusterName:   clusterName,
		ARN:           arn,
		Status:        "ACTIVE",
		NodeRole:      nodeRole,
		AMIType:       amiType,
		DesiredSize:   desiredSize,
		MinSize:       minSize,
		MaxSize:       maxSize,
		InstanceTypes: instanceTypes,
		Subnets:       subnets,
		CreatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetNodegroup(clusterName, name string) (*Nodegroup, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, cluster_name, arn, status, instance_types, desired_size, min_size, max_size, ami_type, node_role, subnets, created_at
		 FROM nodegroups WHERE cluster_name = ? AND name = ?`, clusterName, name)
	return scanNodegroup(row)
}

func (s *Store) ListNodegroups(clusterName string) ([]Nodegroup, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, cluster_name, arn, status, instance_types, desired_size, min_size, max_size, ami_type, node_role, subnets, created_at
		 FROM nodegroups WHERE cluster_name = ? ORDER BY created_at`, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Nodegroup
	for rows.Next() {
		ng, err := scanNodegroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ng)
	}
	return out, rows.Err()
}

func (s *Store) DeleteNodegroup(clusterName, name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM nodegroups WHERE cluster_name = ? AND name = ?`, clusterName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNodegroupNotFound
	}
	return nil
}

func (s *Store) UpdateNodegroupConfig(clusterName, name string, desiredSize, minSize, maxSize int) error {
	res, err := s.store.DB().Exec(
		`UPDATE nodegroups SET desired_size=?, min_size=?, max_size=? WHERE cluster_name=? AND name=?`,
		desiredSize, minSize, maxSize, clusterName, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errNodegroupNotFound
	}
	return nil
}

// --- FargateProfile ---

func (s *Store) CreateFargateProfile(name, clusterName, arn, podExecRole, selectors, subnets string) (*FargateProfile, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO fargate_profiles (name, cluster_name, arn, pod_execution_role, selectors, subnets, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, clusterName, arn, podExecRole, selectors, subnets, now,
	)
	if err != nil {
		return nil, err
	}
	return &FargateProfile{
		Name:             name,
		ClusterName:      clusterName,
		ARN:              arn,
		Status:           "ACTIVE",
		PodExecutionRole: podExecRole,
		Selectors:        selectors,
		Subnets:          subnets,
		CreatedAt:        time.Unix(now, 0),
	}, nil
}

func (s *Store) GetFargateProfile(clusterName, name string) (*FargateProfile, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, cluster_name, arn, status, pod_execution_role, selectors, subnets, created_at
		 FROM fargate_profiles WHERE cluster_name = ? AND name = ?`, clusterName, name)
	return scanFargateProfile(row)
}

func (s *Store) ListFargateProfiles(clusterName string) ([]FargateProfile, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, cluster_name, arn, status, pod_execution_role, selectors, subnets, created_at
		 FROM fargate_profiles WHERE cluster_name = ? ORDER BY created_at`, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FargateProfile
	for rows.Next() {
		fp, err := scanFargateProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *fp)
	}
	return out, rows.Err()
}

func (s *Store) DeleteFargateProfile(clusterName, name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM fargate_profiles WHERE cluster_name = ? AND name = ?`, clusterName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errFargateProfileNotFound
	}
	return nil
}

// --- Addon ---

func (s *Store) CreateAddon(name, clusterName, arn, addonVersion, serviceRole, config string) (*Addon, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO addons (name, cluster_name, arn, addon_version, service_role, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		name, clusterName, arn, addonVersion, serviceRole, config, now,
	)
	if err != nil {
		return nil, err
	}
	return &Addon{
		Name:         name,
		ClusterName:  clusterName,
		ARN:          arn,
		Status:       "ACTIVE",
		AddonVersion: addonVersion,
		ServiceRole:  serviceRole,
		Config:       config,
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAddon(clusterName, name string) (*Addon, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, cluster_name, arn, status, addon_version, service_role, config, created_at
		 FROM addons WHERE cluster_name = ? AND name = ?`, clusterName, name)
	return scanAddon(row)
}

func (s *Store) ListAddons(clusterName string) ([]Addon, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, cluster_name, arn, status, addon_version, service_role, config, created_at
		 FROM addons WHERE cluster_name = ? ORDER BY created_at`, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Addon
	for rows.Next() {
		a, err := scanAddon(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAddon(clusterName, name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM addons WHERE cluster_name = ? AND name = ?`, clusterName, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAddonNotFound
	}
	return nil
}

func (s *Store) UpdateAddon(clusterName, name, addonVersion, serviceRole, config string) error {
	res, err := s.store.DB().Exec(
		`UPDATE addons SET addon_version=?, service_role=?, config=? WHERE cluster_name=? AND name=?`,
		addonVersion, serviceRole, config, clusterName, name,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAddonNotFound
	}
	return nil
}

// --- AccessEntry ---

func (s *Store) CreateAccessEntry(principalARN, clusterName, arn, entryType, kubernetesGroups, username string) (*AccessEntry, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO access_entries (principal_arn, cluster_name, arn, entry_type, kubernetes_groups, username, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		principalARN, clusterName, arn, entryType, kubernetesGroups, username, now,
	)
	if err != nil {
		return nil, err
	}
	return &AccessEntry{
		PrincipalARN:     principalARN,
		ClusterName:      clusterName,
		ARN:              arn,
		EntryType:        entryType,
		KubernetesGroups: kubernetesGroups,
		Username:         username,
		CreatedAt:        time.Unix(now, 0),
	}, nil
}

func (s *Store) GetAccessEntry(clusterName, principalARN string) (*AccessEntry, error) {
	row := s.store.DB().QueryRow(
		`SELECT principal_arn, cluster_name, arn, entry_type, kubernetes_groups, username, created_at
		 FROM access_entries WHERE cluster_name = ? AND principal_arn = ?`, clusterName, principalARN)
	return scanAccessEntry(row)
}

func (s *Store) ListAccessEntries(clusterName string) ([]AccessEntry, error) {
	rows, err := s.store.DB().Query(
		`SELECT principal_arn, cluster_name, arn, entry_type, kubernetes_groups, username, created_at
		 FROM access_entries WHERE cluster_name = ? ORDER BY created_at`, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccessEntry
	for rows.Next() {
		ae, err := scanAccessEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ae)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAccessEntry(clusterName, principalARN string) error {
	res, err := s.store.DB().Exec(`DELETE FROM access_entries WHERE cluster_name = ? AND principal_arn = ?`, clusterName, principalARN)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAccessEntryNotFound
	}
	return nil
}

func (s *Store) UpdateAccessEntry(clusterName, principalARN, kubernetesGroups, username string) error {
	res, err := s.store.DB().Exec(
		`UPDATE access_entries SET kubernetes_groups=?, username=? WHERE cluster_name=? AND principal_arn=?`,
		kubernetesGroups, username, clusterName, principalARN,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errAccessEntryNotFound
	}
	return nil
}

// --- PodIdentityAssociation ---

func (s *Store) CreatePodIdentityAssociation(associationID, clusterName, arn, namespace, serviceAccount, roleARN string) (*PodIdentityAssociation, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pod_identity_associations (association_id, cluster_name, arn, namespace, service_account, role_arn, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		associationID, clusterName, arn, namespace, serviceAccount, roleARN, now,
	)
	if err != nil {
		return nil, err
	}
	return &PodIdentityAssociation{
		AssociationID:  associationID,
		ClusterName:    clusterName,
		ARN:            arn,
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
		RoleARN:        roleARN,
		CreatedAt:      time.Unix(now, 0),
	}, nil
}

func (s *Store) GetPodIdentityAssociation(clusterName, associationID string) (*PodIdentityAssociation, error) {
	row := s.store.DB().QueryRow(
		`SELECT association_id, cluster_name, arn, namespace, service_account, role_arn, created_at
		 FROM pod_identity_associations WHERE cluster_name = ? AND association_id = ?`, clusterName, associationID)
	return scanPodIdentity(row)
}

func (s *Store) ListPodIdentityAssociations(clusterName string) ([]PodIdentityAssociation, error) {
	rows, err := s.store.DB().Query(
		`SELECT association_id, cluster_name, arn, namespace, service_account, role_arn, created_at
		 FROM pod_identity_associations WHERE cluster_name = ? ORDER BY created_at`, clusterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PodIdentityAssociation
	for rows.Next() {
		pa, err := scanPodIdentity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *pa)
	}
	return out, rows.Err()
}

func (s *Store) DeletePodIdentityAssociation(clusterName, associationID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM pod_identity_associations WHERE cluster_name = ? AND association_id = ?`, clusterName, associationID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPodIdentityNotFound
	}
	return nil
}

func (s *Store) UpdatePodIdentityAssociation(clusterName, associationID, roleARN string) error {
	res, err := s.store.DB().Exec(
		`UPDATE pod_identity_associations SET role_arn=? WHERE cluster_name=? AND association_id=?`,
		roleARN, clusterName, associationID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPodIdentityNotFound
	}
	return nil
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanCluster(s scanner) (*Cluster, error) {
	var c Cluster
	var createdAt int64
	err := s.Scan(&c.Name, &c.ARN, &c.Status, &c.Version, &c.RoleARN, &c.Endpoint, &c.Certificate, &c.PlatformVersion, &c.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanNodegroup(s scanner) (*Nodegroup, error) {
	var ng Nodegroup
	var createdAt int64
	err := s.Scan(&ng.Name, &ng.ClusterName, &ng.ARN, &ng.Status, &ng.InstanceTypes, &ng.DesiredSize, &ng.MinSize, &ng.MaxSize, &ng.AMIType, &ng.NodeRole, &ng.Subnets, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNodegroupNotFound
		}
		return nil, err
	}
	ng.CreatedAt = time.Unix(createdAt, 0)
	return &ng, nil
}

func scanFargateProfile(s scanner) (*FargateProfile, error) {
	var fp FargateProfile
	var createdAt int64
	err := s.Scan(&fp.Name, &fp.ClusterName, &fp.ARN, &fp.Status, &fp.PodExecutionRole, &fp.Selectors, &fp.Subnets, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errFargateProfileNotFound
		}
		return nil, err
	}
	fp.CreatedAt = time.Unix(createdAt, 0)
	return &fp, nil
}

func scanAddon(s scanner) (*Addon, error) {
	var a Addon
	var createdAt int64
	err := s.Scan(&a.Name, &a.ClusterName, &a.ARN, &a.Status, &a.AddonVersion, &a.ServiceRole, &a.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAddonNotFound
		}
		return nil, err
	}
	a.CreatedAt = time.Unix(createdAt, 0)
	return &a, nil
}

func scanAccessEntry(s scanner) (*AccessEntry, error) {
	var ae AccessEntry
	var createdAt int64
	err := s.Scan(&ae.PrincipalARN, &ae.ClusterName, &ae.ARN, &ae.EntryType, &ae.KubernetesGroups, &ae.Username, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errAccessEntryNotFound
		}
		return nil, err
	}
	ae.CreatedAt = time.Unix(createdAt, 0)
	return &ae, nil
}

func scanPodIdentity(s scanner) (*PodIdentityAssociation, error) {
	var pa PodIdentityAssociation
	var createdAt int64
	err := s.Scan(&pa.AssociationID, &pa.ClusterName, &pa.ARN, &pa.Namespace, &pa.ServiceAccount, &pa.RoleARN, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPodIdentityNotFound
		}
		return nil, err
	}
	pa.CreatedAt = time.Unix(createdAt, 0)
	return &pa, nil
}
