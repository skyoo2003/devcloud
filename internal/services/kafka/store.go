// SPDX-License-Identifier: Apache-2.0

// internal/services/kafka/store.go
package kafka

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var (
	errClusterNotFound       = errors.New("cluster not found")
	errConfigurationNotFound = errors.New("configuration not found")
	errTopicNotFound         = errors.New("topic not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS clusters (
			arn           TEXT PRIMARY KEY,
			name          TEXT NOT NULL UNIQUE,
			status        TEXT NOT NULL DEFAULT 'ACTIVE',
			kafka_version TEXT NOT NULL DEFAULT '3.5.1',
			broker_count  INTEGER NOT NULL DEFAULT 3,
			broker_type   TEXT NOT NULL DEFAULT 'kafka.m5.large',
			config        TEXT NOT NULL DEFAULT '{}',
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS configurations (
			arn           TEXT PRIMARY KEY,
			name          TEXT NOT NULL UNIQUE,
			kafka_versions TEXT NOT NULL DEFAULT '[]',
			server_props  TEXT NOT NULL DEFAULT '',
			revision      INTEGER NOT NULL DEFAULT 1,
			created_at    INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS topics (
			arn         TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			cluster_arn TEXT NOT NULL,
			partitions  INTEGER NOT NULL DEFAULT 1,
			replication INTEGER NOT NULL DEFAULT 3,
			config      TEXT NOT NULL DEFAULT '{}',
			UNIQUE(name, cluster_arn)
		);
		CREATE TABLE IF NOT EXISTS cluster_policies (
			cluster_arn TEXT PRIMARY KEY,
			policy      TEXT NOT NULL DEFAULT '',
			current_version TEXT NOT NULL DEFAULT '1'
		);
	`},
}

// Cluster represents an MSK cluster record.
type Cluster struct {
	ARN          string
	Name         string
	Status       string
	KafkaVersion string
	BrokerCount  int
	BrokerType   string
	Config       string
	CreatedAt    time.Time
}

// Configuration represents an MSK configuration record.
type Configuration struct {
	ARN           string
	Name          string
	KafkaVersions string
	ServerProps   string
	Revision      int
	CreatedAt     time.Time
}

// Topic represents an MSK topic record.
type Topic struct {
	ARN         string
	Name        string
	ClusterARN  string
	Partitions  int
	Replication int
	Config      string
}

// Store wraps SQLite for MSK data.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore opens or creates the MSK SQLite database.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "kafka.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- Cluster ---

func (s *Store) CreateCluster(arn, name, kafkaVersion, brokerType string, brokerCount int) (*Cluster, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO clusters (arn, name, kafka_version, broker_type, broker_count, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, kafkaVersion, brokerType, brokerCount, now,
	)
	if err != nil {
		return nil, err
	}
	return &Cluster{
		ARN:          arn,
		Name:         name,
		Status:       "ACTIVE",
		KafkaVersion: kafkaVersion,
		BrokerCount:  brokerCount,
		BrokerType:   brokerType,
		Config:       "{}",
		CreatedAt:    time.Unix(now, 0),
	}, nil
}

func (s *Store) GetClusterByARN(arn string) (*Cluster, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, status, kafka_version, broker_count, broker_type, config, created_at
		 FROM clusters WHERE arn = ?`, arn)
	return scanCluster(row)
}

func (s *Store) ListClusters() ([]Cluster, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, status, kafka_version, broker_count, broker_type, config, created_at
		 FROM clusters ORDER BY created_at`)
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

func (s *Store) DeleteCluster(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM clusters WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errClusterNotFound
	}
	return nil
}

func (s *Store) UpdateCluster(arn string, updates map[string]any) error {
	c, err := s.GetClusterByARN(arn)
	if err != nil {
		return err
	}
	if v, ok := updates["broker_count"].(int); ok {
		c.BrokerCount = v
	}
	if v, ok := updates["broker_type"].(string); ok {
		c.BrokerType = v
	}
	if v, ok := updates["kafka_version"].(string); ok {
		c.KafkaVersion = v
	}
	if v, ok := updates["config"].(string); ok {
		c.Config = v
	}
	_, err = s.store.DB().Exec(
		`UPDATE clusters SET broker_count=?, broker_type=?, kafka_version=?, config=? WHERE arn=?`,
		c.BrokerCount, c.BrokerType, c.KafkaVersion, c.Config, arn,
	)
	return err
}

// --- Configuration ---

func (s *Store) CreateConfiguration(arn, name, kafkaVersions, serverProps string) (*Configuration, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO configurations (arn, name, kafka_versions, server_props, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		arn, name, kafkaVersions, serverProps, now,
	)
	if err != nil {
		return nil, err
	}
	return &Configuration{
		ARN:           arn,
		Name:          name,
		KafkaVersions: kafkaVersions,
		ServerProps:   serverProps,
		Revision:      1,
		CreatedAt:     time.Unix(now, 0),
	}, nil
}

func (s *Store) GetConfigurationByARN(arn string) (*Configuration, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, kafka_versions, server_props, revision, created_at
		 FROM configurations WHERE arn = ?`, arn)
	return scanConfiguration(row)
}

func (s *Store) ListConfigurations() ([]Configuration, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, kafka_versions, server_props, revision, created_at
		 FROM configurations ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cfgs []Configuration
	for rows.Next() {
		c, err := scanConfiguration(rows)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, *c)
	}
	return cfgs, rows.Err()
}

func (s *Store) UpdateConfiguration(arn, serverProps string) (*Configuration, error) {
	res, err := s.store.DB().Exec(
		`UPDATE configurations SET server_props=?, revision=revision+1 WHERE arn=?`,
		serverProps, arn,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errConfigurationNotFound
	}
	return s.GetConfigurationByARN(arn)
}

func (s *Store) DeleteConfiguration(arn string) error {
	res, err := s.store.DB().Exec(`DELETE FROM configurations WHERE arn = ?`, arn)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigurationNotFound
	}
	return nil
}

// --- Topic ---

func (s *Store) CreateTopic(arn, name, clusterARN string, partitions, replication int, config string) (*Topic, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO topics (arn, name, cluster_arn, partitions, replication, config)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		arn, name, clusterARN, partitions, replication, config,
	)
	if err != nil {
		return nil, err
	}
	return &Topic{
		ARN:         arn,
		Name:        name,
		ClusterARN:  clusterARN,
		Partitions:  partitions,
		Replication: replication,
		Config:      config,
	}, nil
}

func (s *Store) GetTopic(clusterARN, topicName string) (*Topic, error) {
	row := s.store.DB().QueryRow(
		`SELECT arn, name, cluster_arn, partitions, replication, config
		 FROM topics WHERE cluster_arn = ? AND name = ?`,
		clusterARN, topicName,
	)
	return scanTopic(row)
}

func (s *Store) ListTopics(clusterARN string) ([]Topic, error) {
	rows, err := s.store.DB().Query(
		`SELECT arn, name, cluster_arn, partitions, replication, config
		 FROM topics WHERE cluster_arn = ? ORDER BY name`,
		clusterARN,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var topics []Topic
	for rows.Next() {
		t, err := scanTopic(rows)
		if err != nil {
			return nil, err
		}
		topics = append(topics, *t)
	}
	return topics, rows.Err()
}

func (s *Store) DeleteTopic(clusterARN, topicName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM topics WHERE cluster_arn = ? AND name = ?`,
		clusterARN, topicName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errTopicNotFound
	}
	return nil
}

func (s *Store) UpdateTopic(clusterARN, topicName string, partitions int, config string) (*Topic, error) {
	res, err := s.store.DB().Exec(
		`UPDATE topics SET partitions=?, config=? WHERE cluster_arn=? AND name=?`,
		partitions, config, clusterARN, topicName,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errTopicNotFound
	}
	return s.GetTopic(clusterARN, topicName)
}

// --- Cluster Policy ---

func (s *Store) GetClusterPolicy(clusterARN string) (string, string, error) {
	var policy, version string
	err := s.store.DB().QueryRow(
		`SELECT policy, current_version FROM cluster_policies WHERE cluster_arn = ?`, clusterARN,
	).Scan(&policy, &version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", errClusterNotFound
		}
		return "", "", err
	}
	return policy, version, nil
}

func (s *Store) PutClusterPolicy(clusterARN, policy string) (string, error) {
	_, err := s.store.DB().Exec(
		`INSERT INTO cluster_policies (cluster_arn, policy, current_version) VALUES (?, ?, '1')
		 ON CONFLICT(cluster_arn) DO UPDATE SET policy=excluded.policy, current_version=CAST(CAST(current_version AS INTEGER)+1 AS TEXT)`,
		clusterARN, policy,
	)
	if err != nil {
		return "", err
	}
	var version string
	_ = s.store.DB().QueryRow(`SELECT current_version FROM cluster_policies WHERE cluster_arn = ?`, clusterARN).Scan(&version)
	return version, nil
}

func (s *Store) DeleteClusterPolicy(clusterARN string) error {
	_, err := s.store.DB().Exec(`DELETE FROM cluster_policies WHERE cluster_arn = ?`, clusterARN)
	return err
}

// --- Scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanCluster(s scanner) (*Cluster, error) {
	var c Cluster
	var createdAt int64
	err := s.Scan(&c.ARN, &c.Name, &c.Status, &c.KafkaVersion, &c.BrokerCount, &c.BrokerType, &c.Config, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errClusterNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanConfiguration(s scanner) (*Configuration, error) {
	var c Configuration
	var createdAt int64
	err := s.Scan(&c.ARN, &c.Name, &c.KafkaVersions, &c.ServerProps, &c.Revision, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConfigurationNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanTopic(s scanner) (*Topic, error) {
	var t Topic
	err := s.Scan(&t.ARN, &t.Name, &t.ClusterARN, &t.Partitions, &t.Replication, &t.Config)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errTopicNotFound
		}
		return nil, err
	}
	return &t, nil
}
