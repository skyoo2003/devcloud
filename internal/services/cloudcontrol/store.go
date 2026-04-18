// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudcontrol/store.go
package cloudcontrol

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errResourceNotFound = errors.New("resource not found")
var errRequestNotFound = errors.New("request not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS cc_resources (
			type_name   TEXT NOT NULL,
			identifier  TEXT NOT NULL,
			state       TEXT NOT NULL,
			account_id  TEXT NOT NULL,
			created_at  INTEGER NOT NULL,
			PRIMARY KEY (type_name, identifier, account_id)
		);
		CREATE TABLE IF NOT EXISTS cc_requests (
			request_token TEXT PRIMARY KEY,
			operation     TEXT NOT NULL,
			type_name     TEXT NOT NULL,
			identifier    TEXT NOT NULL DEFAULT '',
			status        TEXT NOT NULL DEFAULT 'SUCCESS',
			status_message TEXT NOT NULL DEFAULT '',
			account_id    TEXT NOT NULL,
			created_at    INTEGER NOT NULL,
			completed_at  INTEGER NOT NULL
		);
	`},
}

type CCResource struct {
	TypeName   string
	Identifier string
	State      string
	AccountID  string
	CreatedAt  time.Time
}

type CCRequest struct {
	RequestToken  string
	Operation     string
	TypeName      string
	Identifier    string
	Status        string
	StatusMessage string
	AccountID     string
	CreatedAt     time.Time
	CompletedAt   time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "cloudcontrol.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreateResource(res *CCResource) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cc_resources (type_name, identifier, state, account_id, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(type_name, identifier, account_id) DO UPDATE SET state=excluded.state`,
		res.TypeName, res.Identifier, res.State, res.AccountID, now,
	)
	return err
}

func (s *Store) GetResource(typeName, identifier, accountID string) (*CCResource, error) {
	row := s.store.DB().QueryRow(
		`SELECT type_name, identifier, state, account_id, created_at
		 FROM cc_resources WHERE type_name=? AND identifier=? AND account_id=?`,
		typeName, identifier, accountID)
	var r CCResource
	var createdAt int64
	err := row.Scan(&r.TypeName, &r.Identifier, &r.State, &r.AccountID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errResourceNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	return &r, nil
}

func (s *Store) ListResources(typeName, accountID string) ([]CCResource, error) {
	rows, err := s.store.DB().Query(
		`SELECT type_name, identifier, state, account_id, created_at
		 FROM cc_resources WHERE type_name=? AND account_id=? ORDER BY created_at`,
		typeName, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var resources []CCResource
	for rows.Next() {
		var r CCResource
		var createdAt int64
		if err := rows.Scan(&r.TypeName, &r.Identifier, &r.State, &r.AccountID, &createdAt); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(createdAt, 0)
		resources = append(resources, r)
	}
	return resources, rows.Err()
}

func (s *Store) UpdateResource(typeName, identifier, accountID, state string) error {
	res, err := s.store.DB().Exec(
		`UPDATE cc_resources SET state=? WHERE type_name=? AND identifier=? AND account_id=?`,
		state, typeName, identifier, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errResourceNotFound
	}
	return nil
}

func (s *Store) DeleteResource(typeName, identifier, accountID string) error {
	res, err := s.store.DB().Exec(
		`DELETE FROM cc_resources WHERE type_name=? AND identifier=? AND account_id=?`,
		typeName, identifier, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errResourceNotFound
	}
	return nil
}

func (s *Store) CreateRequest(req *CCRequest) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO cc_requests (request_token, operation, type_name, identifier, status, status_message, account_id, created_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.RequestToken, req.Operation, req.TypeName, req.Identifier, req.Status, req.StatusMessage,
		req.AccountID, now, now,
	)
	return err
}

func (s *Store) GetRequest(requestToken string) (*CCRequest, error) {
	row := s.store.DB().QueryRow(
		`SELECT request_token, operation, type_name, identifier, status, status_message, account_id, created_at, completed_at
		 FROM cc_requests WHERE request_token=?`, requestToken)
	var r CCRequest
	var createdAt, completedAt int64
	err := row.Scan(&r.RequestToken, &r.Operation, &r.TypeName, &r.Identifier, &r.Status, &r.StatusMessage, &r.AccountID, &createdAt, &completedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errRequestNotFound
		}
		return nil, err
	}
	r.CreatedAt = time.Unix(createdAt, 0)
	r.CompletedAt = time.Unix(completedAt, 0)
	return &r, nil
}

func (s *Store) ListRequests(accountID string) ([]CCRequest, error) {
	rows, err := s.store.DB().Query(
		`SELECT request_token, operation, type_name, identifier, status, status_message, account_id, created_at, completed_at
		 FROM cc_requests WHERE account_id=? ORDER BY created_at DESC`,
		accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var requests []CCRequest
	for rows.Next() {
		var r CCRequest
		var createdAt, completedAt int64
		if err := rows.Scan(&r.RequestToken, &r.Operation, &r.TypeName, &r.Identifier, &r.Status, &r.StatusMessage, &r.AccountID, &createdAt, &completedAt); err != nil {
			return nil, err
		}
		r.CreatedAt = time.Unix(createdAt, 0)
		r.CompletedAt = time.Unix(completedAt, 0)
		requests = append(requests, r)
	}
	return requests, rows.Err()
}

func (s *Store) CancelRequest(requestToken, accountID string) error {
	res, err := s.store.DB().Exec(
		`UPDATE cc_requests SET status='CANCEL_COMPLETE' WHERE request_token=? AND account_id=?`,
		requestToken, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errRequestNotFound
	}
	return nil
}
