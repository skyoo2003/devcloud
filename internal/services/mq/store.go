// SPDX-License-Identifier: Apache-2.0

// internal/services/mq/store.go
package mq

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
	errBrokerNotFound        = errors.New("broker not found")
	errConfigurationNotFound = errors.New("configuration not found")
	errUserNotFound          = errors.New("user not found")
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS brokers (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			name            TEXT NOT NULL,
			engine_type     TEXT NOT NULL DEFAULT 'ACTIVEMQ',
			engine_version  TEXT NOT NULL DEFAULT '5.17.6',
			host_instance   TEXT NOT NULL DEFAULT 'mq.m5.large',
			deployment_mode TEXT NOT NULL DEFAULT 'SINGLE_INSTANCE',
			status          TEXT NOT NULL DEFAULT 'RUNNING',
			publicly_accessible INTEGER NOT NULL DEFAULT 0,
			auto_minor_upgrade INTEGER NOT NULL DEFAULT 1,
			console_url     TEXT NOT NULL DEFAULT '',
			endpoints       TEXT NOT NULL DEFAULT '[]',
			config          TEXT NOT NULL DEFAULT '{}',
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS configurations (
			id              TEXT PRIMARY KEY,
			arn             TEXT NOT NULL UNIQUE,
			name            TEXT NOT NULL,
			engine_type     TEXT NOT NULL DEFAULT 'ACTIVEMQ',
			engine_version  TEXT NOT NULL DEFAULT '5.17.6',
			data            TEXT NOT NULL DEFAULT '',
			revision        INTEGER NOT NULL DEFAULT 1,
			created_at      INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS broker_users (
			broker_id       TEXT NOT NULL,
			username        TEXT NOT NULL,
			password        TEXT NOT NULL DEFAULT '',
			console_access  INTEGER NOT NULL DEFAULT 0,
			groups          TEXT NOT NULL DEFAULT '[]',
			PRIMARY KEY (broker_id, username)
		);
	`},
}

// Broker represents an Amazon MQ broker.
type Broker struct {
	ID                 string
	ARN                string
	Name               string
	EngineType         string
	EngineVersion      string
	HostInstance       string
	DeploymentMode     string
	Status             string
	PubliclyAccessible bool
	AutoMinorUpgrade   bool
	ConsoleURL         string
	Endpoints          []string
	Config             string
	CreatedAt          time.Time
}

// MQConfiguration represents an Amazon MQ configuration.
type MQConfiguration struct {
	ID            string
	ARN           string
	Name          string
	EngineType    string
	EngineVersion string
	Data          string
	Revision      int
	CreatedAt     time.Time
}

// BrokerUser represents a user associated with a broker.
type BrokerUser struct {
	BrokerID      string
	Username      string
	Password      string
	ConsoleAccess bool
	Groups        []string
}

// Store handles persistence for the MQ service.
type Store struct {
	store *sqlite.Store
	tags  *shared.TagStore
}

// NewStore creates and initialises the MQ store.
func NewStore(dataDir string) (*Store, error) {
	allMigrations := append(migrations, shared.TagMigrations...)
	dbPath := filepath.Join(dataDir, "mq.db")
	s, err := sqlite.Open(dbPath, allMigrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s, tags: shared.NewTagStore(s)}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// ---- Broker ----

func (s *Store) CreateBroker(b *Broker) error {
	endpoints, _ := json.Marshal(b.Endpoints)
	publicly := boolToInt(b.PubliclyAccessible)
	autoMinor := boolToInt(b.AutoMinorUpgrade)
	_, err := s.store.DB().Exec(
		`INSERT INTO brokers
			(id, arn, name, engine_type, engine_version, host_instance, deployment_mode,
			 status, publicly_accessible, auto_minor_upgrade, console_url, endpoints, config, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.ID, b.ARN, b.Name, b.EngineType, b.EngineVersion, b.HostInstance, b.DeploymentMode,
		b.Status, publicly, autoMinor, b.ConsoleURL, string(endpoints), b.Config,
		time.Now().Unix(),
	)
	return err
}

func (s *Store) GetBroker(id string) (*Broker, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, engine_type, engine_version, host_instance, deployment_mode,
		        status, publicly_accessible, auto_minor_upgrade, console_url, endpoints, config, created_at
		 FROM brokers WHERE id = ?`, id)
	return scanBroker(row)
}

func (s *Store) ListBrokers() ([]Broker, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, engine_type, engine_version, host_instance, deployment_mode,
		        status, publicly_accessible, auto_minor_upgrade, console_url, endpoints, config, created_at
		 FROM brokers ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var brokers []Broker
	for rows.Next() {
		b, err := scanBroker(rows)
		if err != nil {
			return nil, err
		}
		brokers = append(brokers, *b)
	}
	return brokers, rows.Err()
}

func (s *Store) UpdateBroker(id string, fields map[string]any) error {
	b, err := s.GetBroker(id)
	if err != nil {
		return errBrokerNotFound
	}
	if v, ok := fields["engineVersion"].(string); ok && v != "" {
		b.EngineVersion = v
	}
	if v, ok := fields["hostInstanceType"].(string); ok && v != "" {
		b.HostInstance = v
	}
	if v, ok := fields["autoMinorVersionUpgrade"].(bool); ok {
		b.AutoMinorUpgrade = v
	}
	res, err := s.store.DB().Exec(
		`UPDATE brokers SET engine_version=?, host_instance=?, auto_minor_upgrade=? WHERE id=?`,
		b.EngineVersion, b.HostInstance, boolToInt(b.AutoMinorUpgrade), id,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBrokerNotFound
	}
	return nil
}

func (s *Store) DeleteBroker(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM brokers WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errBrokerNotFound
	}
	// cascade delete users
	_, _ = s.store.DB().Exec(`DELETE FROM broker_users WHERE broker_id = ?`, id)
	return nil
}

// ---- Configuration ----

func (s *Store) CreateConfiguration(c *MQConfiguration) error {
	_, err := s.store.DB().Exec(
		`INSERT INTO configurations (id, arn, name, engine_type, engine_version, data, revision, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.ARN, c.Name, c.EngineType, c.EngineVersion, c.Data, c.Revision,
		time.Now().Unix(),
	)
	return err
}

func (s *Store) GetConfiguration(id string) (*MQConfiguration, error) {
	row := s.store.DB().QueryRow(
		`SELECT id, arn, name, engine_type, engine_version, data, revision, created_at
		 FROM configurations WHERE id = ?`, id)
	return scanConfiguration(row)
}

func (s *Store) ListConfigurations() ([]MQConfiguration, error) {
	rows, err := s.store.DB().Query(
		`SELECT id, arn, name, engine_type, engine_version, data, revision, created_at
		 FROM configurations ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var cfgs []MQConfiguration
	for rows.Next() {
		c, err := scanConfiguration(rows)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, *c)
	}
	return cfgs, rows.Err()
}

func (s *Store) UpdateConfiguration(id string, data string) (*MQConfiguration, error) {
	c, err := s.GetConfiguration(id)
	if err != nil {
		return nil, errConfigurationNotFound
	}
	c.Data = data
	c.Revision++
	res, err := s.store.DB().Exec(
		`UPDATE configurations SET data=?, revision=? WHERE id=?`,
		c.Data, c.Revision, id,
	)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, errConfigurationNotFound
	}
	return c, nil
}

func (s *Store) DeleteConfiguration(id string) error {
	res, err := s.store.DB().Exec(`DELETE FROM configurations WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errConfigurationNotFound
	}
	return nil
}

// ---- User ----

func (s *Store) CreateUser(u *BrokerUser) error {
	groups, _ := json.Marshal(u.Groups)
	_, err := s.store.DB().Exec(
		`INSERT INTO broker_users (broker_id, username, password, console_access, groups)
		 VALUES (?, ?, ?, ?, ?)`,
		u.BrokerID, u.Username, u.Password, boolToInt(u.ConsoleAccess), string(groups),
	)
	return err
}

func (s *Store) GetUser(brokerID, username string) (*BrokerUser, error) {
	row := s.store.DB().QueryRow(
		`SELECT broker_id, username, password, console_access, groups
		 FROM broker_users WHERE broker_id = ? AND username = ?`, brokerID, username)
	return scanUser(row)
}

func (s *Store) ListUsers(brokerID string) ([]BrokerUser, error) {
	rows, err := s.store.DB().Query(
		`SELECT broker_id, username, password, console_access, groups
		 FROM broker_users WHERE broker_id = ? ORDER BY username`, brokerID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var users []BrokerUser
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, *u)
	}
	return users, rows.Err()
}

func (s *Store) UpdateUser(brokerID, username string, fields map[string]any) error {
	u, err := s.GetUser(brokerID, username)
	if err != nil {
		return errUserNotFound
	}
	if v, ok := fields["password"].(string); ok && v != "" {
		u.Password = v
	}
	if v, ok := fields["consoleAccess"].(bool); ok {
		u.ConsoleAccess = v
	}
	if v, ok := fields["groups"].([]any); ok {
		groups := make([]string, 0, len(v))
		for _, g := range v {
			if s, ok := g.(string); ok {
				groups = append(groups, s)
			}
		}
		u.Groups = groups
	}
	groups, _ := json.Marshal(u.Groups)
	res, err := s.store.DB().Exec(
		`UPDATE broker_users SET password=?, console_access=?, groups=? WHERE broker_id=? AND username=?`,
		u.Password, boolToInt(u.ConsoleAccess), string(groups), brokerID, username,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserNotFound
	}
	return nil
}

func (s *Store) DeleteUser(brokerID, username string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM broker_users WHERE broker_id = ? AND username = ?`, brokerID, username)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errUserNotFound
	}
	return nil
}

// ---- Scanners ----

type scanner interface{ Scan(dest ...any) error }

func scanBroker(sc scanner) (*Broker, error) {
	var b Broker
	var createdAt int64
	var endpointsJSON string
	var publicly, autoMinor int
	err := sc.Scan(
		&b.ID, &b.ARN, &b.Name, &b.EngineType, &b.EngineVersion, &b.HostInstance,
		&b.DeploymentMode, &b.Status, &publicly, &autoMinor,
		&b.ConsoleURL, &endpointsJSON, &b.Config, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errBrokerNotFound
		}
		return nil, err
	}
	b.PubliclyAccessible = publicly != 0
	b.AutoMinorUpgrade = autoMinor != 0
	b.CreatedAt = time.Unix(createdAt, 0)
	_ = json.Unmarshal([]byte(endpointsJSON), &b.Endpoints)
	if b.Endpoints == nil {
		b.Endpoints = []string{}
	}
	return &b, nil
}

func scanConfiguration(sc scanner) (*MQConfiguration, error) {
	var c MQConfiguration
	var createdAt int64
	err := sc.Scan(
		&c.ID, &c.ARN, &c.Name, &c.EngineType, &c.EngineVersion,
		&c.Data, &c.Revision, &createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errConfigurationNotFound
		}
		return nil, err
	}
	c.CreatedAt = time.Unix(createdAt, 0)
	return &c, nil
}

func scanUser(sc scanner) (*BrokerUser, error) {
	var u BrokerUser
	var consoleAccess int
	var groupsJSON string
	err := sc.Scan(&u.BrokerID, &u.Username, &u.Password, &consoleAccess, &groupsJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUserNotFound
		}
		return nil, err
	}
	u.ConsoleAccess = consoleAccess != 0
	_ = json.Unmarshal([]byte(groupsJSON), &u.Groups)
	if u.Groups == nil {
		u.Groups = []string{}
	}
	return &u, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
