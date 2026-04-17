// SPDX-License-Identifier: Apache-2.0

// internal/services/iotdataplane/store.go
package iotdataplane

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errShadowNotFound = errors.New("thing shadow not found")
var errRetainedMessageNotFound = errors.New("retained message not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS thing_shadows (
			thing_name TEXT NOT NULL,
			shadow_name TEXT NOT NULL DEFAULT 'classic',
			payload TEXT NOT NULL DEFAULT '{}',
			version INTEGER NOT NULL DEFAULT 1,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (thing_name, shadow_name)
		);
		CREATE TABLE IF NOT EXISTS retained_messages (
			topic TEXT PRIMARY KEY,
			payload TEXT NOT NULL DEFAULT '',
			qos INTEGER NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL
		);
	`},
}

type ThingShadow struct {
	ThingName  string
	ShadowName string
	Payload    string
	Version    int64
	UpdatedAt  time.Time
}

type RetainedMessage struct {
	Topic     string
	Payload   string
	Qos       int32
	UpdatedAt time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "iotdataplane.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

// --- ThingShadow ---

func (s *Store) GetThingShadow(thingName, shadowName string) (*ThingShadow, error) {
	row := s.store.DB().QueryRow(
		`SELECT thing_name, shadow_name, payload, version, updated_at
		 FROM thing_shadows WHERE thing_name = ? AND shadow_name = ?`,
		thingName, shadowName,
	)
	return scanThingShadow(row)
}

func (s *Store) UpsertThingShadow(thingName, shadowName, payload string) (*ThingShadow, error) {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO thing_shadows (thing_name, shadow_name, payload, version, updated_at)
		VALUES (?, ?, ?, 1, ?)
		ON CONFLICT(thing_name, shadow_name) DO UPDATE SET
			payload = excluded.payload,
			version = version + 1,
			updated_at = excluded.updated_at`,
		thingName, shadowName, payload, now,
	)
	if err != nil {
		return nil, err
	}
	return s.GetThingShadow(thingName, shadowName)
}

func (s *Store) DeleteThingShadow(thingName, shadowName string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM thing_shadows WHERE thing_name = ? AND shadow_name = ?`,
		thingName, shadowName,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errShadowNotFound
	}
	return nil
}

func (s *Store) ListShadowNames(thingName string) ([]string, error) {
	rows, err := s.store.DB().Query(
		`SELECT shadow_name FROM thing_shadows WHERE thing_name = ? ORDER BY shadow_name`,
		thingName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// --- RetainedMessage ---

func (s *Store) UpsertRetainedMessage(topic, payload string, qos int32) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(`
		INSERT INTO retained_messages (topic, payload, qos, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(topic) DO UPDATE SET
			payload = excluded.payload,
			qos = excluded.qos,
			updated_at = excluded.updated_at`,
		topic, payload, qos, now,
	)
	return err
}

func (s *Store) GetRetainedMessage(topic string) (*RetainedMessage, error) {
	row := s.store.DB().QueryRow(
		`SELECT topic, payload, qos, updated_at FROM retained_messages WHERE topic = ?`,
		topic,
	)
	return scanRetainedMessage(row)
}

func (s *Store) ListRetainedMessages() ([]*RetainedMessage, error) {
	rows, err := s.store.DB().Query(
		`SELECT topic, payload, qos, updated_at FROM retained_messages ORDER BY topic`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var msgs []*RetainedMessage
	for rows.Next() {
		m, err := scanRetainedMessage(rows)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// --- scanners ---

type scanner interface{ Scan(dest ...any) error }

func scanThingShadow(sc scanner) (*ThingShadow, error) {
	var ts ThingShadow
	var updatedAt int64
	err := sc.Scan(&ts.ThingName, &ts.ShadowName, &ts.Payload, &ts.Version, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errShadowNotFound
		}
		return nil, err
	}
	ts.UpdatedAt = time.Unix(updatedAt, 0)
	return &ts, nil
}

func scanRetainedMessage(sc scanner) (*RetainedMessage, error) {
	var m RetainedMessage
	var updatedAt int64
	err := sc.Scan(&m.Topic, &m.Payload, &m.Qos, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRetainedMessageNotFound
		}
		return nil, err
	}
	m.UpdatedAt = time.Unix(updatedAt, 0)
	return &m, nil
}
