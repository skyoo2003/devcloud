// SPDX-License-Identifier: Apache-2.0

// internal/services/firehose/store.go
package firehose

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errStreamNotFound = errors.New("stream not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
        CREATE TABLE IF NOT EXISTS delivery_streams (
            name            TEXT PRIMARY KEY,
            arn             TEXT NOT NULL UNIQUE,
            status          TEXT NOT NULL DEFAULT 'ACTIVE',
            stream_type     TEXT NOT NULL DEFAULT 'DirectPut',
            destination     TEXT NOT NULL DEFAULT '{}',
            encryption      TEXT NOT NULL DEFAULT 'DISABLED',
            created_at      INTEGER NOT NULL
        );
    `},
	{Version: 2, SQL: `
		CREATE TABLE IF NOT EXISTS kinesis_sources (
			delivery_stream_name TEXT NOT NULL,
			kinesis_stream_arn   TEXT NOT NULL,
			role_arn             TEXT NOT NULL DEFAULT '',
			created_at           INTEGER NOT NULL,
			PRIMARY KEY (delivery_stream_name, kinesis_stream_arn)
		);
		CREATE TABLE IF NOT EXISTS redshift_targets (
			delivery_stream_name TEXT PRIMARY KEY,
			role_arn             TEXT NOT NULL,
			cluster_jdbc_url     TEXT NOT NULL DEFAULT '',
			database_name        TEXT NOT NULL DEFAULT '',
			username             TEXT NOT NULL DEFAULT '',
			s3_bucket            TEXT NOT NULL DEFAULT '',
			copy_options         TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS elasticsearch_targets (
			delivery_stream_name TEXT PRIMARY KEY,
			domain_arn           TEXT NOT NULL DEFAULT '',
			index_name           TEXT NOT NULL DEFAULT '',
			type_name            TEXT NOT NULL DEFAULT '',
			role_arn             TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS http_endpoint_targets (
			delivery_stream_name TEXT PRIMARY KEY,
			url                  TEXT NOT NULL DEFAULT '',
			name                 TEXT NOT NULL DEFAULT '',
			access_key           TEXT NOT NULL DEFAULT '',
			role_arn             TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS splunk_targets (
			delivery_stream_name TEXT PRIMARY KEY,
			hec_endpoint         TEXT NOT NULL DEFAULT '',
			hec_token            TEXT NOT NULL DEFAULT '',
			hec_acknowledgement  INTEGER NOT NULL DEFAULT 0,
			role_arn             TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE IF NOT EXISTS firehose_records (
			id                   TEXT PRIMARY KEY,
			delivery_stream_name TEXT NOT NULL,
			data                 TEXT NOT NULL DEFAULT '',
			created_at           INTEGER NOT NULL
		);
	`},
}

type DeliveryStream struct {
	Name       string
	ARN        string
	Status     string
	Type       string
	Dest       string
	Encryption string
	CreatedAt  time.Time
}

type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "firehose.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateStream(name, arn, streamType, dest string) (*DeliveryStream, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO delivery_streams (name, arn, stream_type, destination, created_at)
         VALUES (?, ?, ?, ?, ?)`,
		name, arn, streamType, dest, now,
	)
	if err != nil {
		return nil, err
	}
	return &DeliveryStream{
		Name: name, ARN: arn, Status: "ACTIVE", Type: streamType,
		Dest: dest, Encryption: "DISABLED", CreatedAt: time.Unix(now, 0),
	}, nil
}

func (s *Store) GetStream(name string) (*DeliveryStream, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, arn, status, stream_type, destination, encryption, created_at
         FROM delivery_streams WHERE name = ?`, name)
	return scanStream(row)
}

func (s *Store) ListStreams(streamType string) ([]DeliveryStream, error) {
	query := `SELECT name, arn, status, stream_type, destination, encryption, created_at FROM delivery_streams`
	var args []any
	if streamType != "" {
		query += ` WHERE stream_type = ?`
		args = append(args, streamType)
	}
	query += ` ORDER BY created_at`
	rows, err := s.store.DB().Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var streams []DeliveryStream
	for rows.Next() {
		st, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		streams = append(streams, *st)
	}
	return streams, rows.Err()
}

func (s *Store) DeleteStream(name string) error {
	res, err := s.store.DB().Exec(`DELETE FROM delivery_streams WHERE name = ?`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

func (s *Store) UpdateDestination(name, dest string) error {
	res, err := s.store.DB().Exec(
		`UPDATE delivery_streams SET destination = ? WHERE name = ?`, dest, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

func (s *Store) UpdateEncryption(name, encryption string) error {
	res, err := s.store.DB().Exec(
		`UPDATE delivery_streams SET encryption = ? WHERE name = ?`, encryption, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanStream(s scanner) (*DeliveryStream, error) {
	var st DeliveryStream
	var createdAt int64
	err := s.Scan(&st.Name, &st.ARN, &st.Status, &st.Type, &st.Dest, &st.Encryption, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errStreamNotFound
		}
		return nil, err
	}
	st.CreatedAt = time.Unix(createdAt, 0)
	return &st, nil
}

// --- Kinesis Sources ---

type KinesisSource struct {
	DeliveryStreamName string
	KinesisStreamARN   string
	RoleARN            string
	CreatedAt          time.Time
}

func (s *Store) AddKinesisSource(streamName, kinesisARN, roleARN string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO kinesis_sources (delivery_stream_name, kinesis_stream_arn, role_arn, created_at) VALUES (?, ?, ?, ?)`,
		streamName, kinesisARN, roleARN, time.Now().Unix())
	return err
}

func (s *Store) GetKinesisSources(streamName string) ([]KinesisSource, error) {
	rows, err := s.store.DB().Query(
		`SELECT delivery_stream_name, kinesis_stream_arn, role_arn, created_at FROM kinesis_sources WHERE delivery_stream_name=?`, streamName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []KinesisSource
	for rows.Next() {
		var ks KinesisSource
		var createdAt int64
		if err := rows.Scan(&ks.DeliveryStreamName, &ks.KinesisStreamARN, &ks.RoleARN, &createdAt); err != nil {
			return nil, err
		}
		ks.CreatedAt = time.Unix(createdAt, 0)
		out = append(out, ks)
	}
	return out, rows.Err()
}

// --- Redshift targets ---

type RedshiftTarget struct {
	DeliveryStreamName string
	RoleARN            string
	ClusterJDBCURL     string
	DatabaseName       string
	Username           string
	S3Bucket           string
	CopyOptions        string
}

func (s *Store) PutRedshiftTarget(streamName, roleARN, jdbcURL, db, username, s3Bucket, copyOpts string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO redshift_targets (delivery_stream_name, role_arn, cluster_jdbc_url, database_name, username, s3_bucket, copy_options)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		streamName, roleARN, jdbcURL, db, username, s3Bucket, copyOpts)
	return err
}

func (s *Store) GetRedshiftTarget(streamName string) (*RedshiftTarget, error) {
	var rt RedshiftTarget
	err := s.store.DB().QueryRow(
		`SELECT delivery_stream_name, role_arn, cluster_jdbc_url, database_name, username, s3_bucket, copy_options FROM redshift_targets WHERE delivery_stream_name=?`,
		streamName,
	).Scan(&rt.DeliveryStreamName, &rt.RoleARN, &rt.ClusterJDBCURL, &rt.DatabaseName, &rt.Username, &rt.S3Bucket, &rt.CopyOptions)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errStreamNotFound
	}
	return &rt, err
}

// --- Elasticsearch targets ---

func (s *Store) PutElasticsearchTarget(streamName, domainARN, indexName, typeName, roleARN string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO elasticsearch_targets (delivery_stream_name, domain_arn, index_name, type_name, role_arn)
		 VALUES (?, ?, ?, ?, ?)`,
		streamName, domainARN, indexName, typeName, roleARN)
	return err
}

// --- HTTP Endpoint targets ---

func (s *Store) PutHTTPEndpointTarget(streamName, url, name, accessKey, roleARN string) error {
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO http_endpoint_targets (delivery_stream_name, url, name, access_key, role_arn)
		 VALUES (?, ?, ?, ?, ?)`,
		streamName, url, name, accessKey, roleARN)
	return err
}

// --- Splunk targets ---

func (s *Store) PutSplunkTarget(streamName, endpoint, token string, ack bool, roleARN string) error {
	ackInt := 0
	if ack {
		ackInt = 1
	}
	_, err := s.store.DB().Exec(
		`INSERT OR REPLACE INTO splunk_targets (delivery_stream_name, hec_endpoint, hec_token, hec_acknowledgement, role_arn)
		 VALUES (?, ?, ?, ?, ?)`,
		streamName, endpoint, token, ackInt, roleARN)
	return err
}

// --- Records ---

func (s *Store) SaveRecord(streamName, data string) (string, error) {
	id := shared.GenerateUUID()
	_, err := s.store.DB().Exec(
		`INSERT INTO firehose_records (id, delivery_stream_name, data, created_at) VALUES (?, ?, ?, ?)`,
		id, streamName, data, time.Now().Unix())
	return id, err
}

// --- Stream status updates ---

func (s *Store) UpdateStreamStatus(name, status string) error {
	res, err := s.store.DB().Exec(`UPDATE delivery_streams SET status = ? WHERE name = ?`, status, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errStreamNotFound
	}
	return nil
}

// Tags exposes the tag store.
func (s *Store) Tags() *shared.TagStore {
	return s.tags
}
