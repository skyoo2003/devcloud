// SPDX-License-Identifier: Apache-2.0

// internal/services/pipes/store.go
package pipes

import (
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var errPipeNotFound = errors.New("pipe not found")

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS pipes (
			name              TEXT NOT NULL,
			account_id        TEXT NOT NULL,
			arn               TEXT NOT NULL,
			source            TEXT NOT NULL DEFAULT '',
			target            TEXT NOT NULL DEFAULT '',
			role_arn          TEXT NOT NULL DEFAULT '',
			source_parameters TEXT NOT NULL DEFAULT '{}',
			target_parameters TEXT NOT NULL DEFAULT '{}',
			enrichment        TEXT NOT NULL DEFAULT '',
			state             TEXT NOT NULL DEFAULT 'RUNNING',
			description       TEXT NOT NULL DEFAULT '',
			created_at        INTEGER NOT NULL,
			updated_at        INTEGER NOT NULL,
			PRIMARY KEY (name, account_id)
		);
		CREATE TABLE IF NOT EXISTS pipe_tags (
			pipe_arn  TEXT NOT NULL,
			tag_key   TEXT NOT NULL,
			tag_value TEXT NOT NULL,
			PRIMARY KEY (pipe_arn, tag_key)
		);
	`},
}

type Pipe struct {
	Name             string
	AccountID        string
	ARN              string
	Source           string
	Target           string
	RoleARN          string
	SourceParameters string
	TargetParameters string
	Enrichment       string
	State            string
	Description      string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "pipes.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) CreatePipe(pipe *Pipe) error {
	now := time.Now().Unix()
	_, err := s.store.DB().Exec(
		`INSERT INTO pipes (name, account_id, arn, source, target, role_arn, source_parameters, target_parameters, enrichment, state, description, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		pipe.Name, pipe.AccountID, pipe.ARN, pipe.Source, pipe.Target, pipe.RoleARN,
		pipe.SourceParameters, pipe.TargetParameters, pipe.Enrichment, pipe.State, pipe.Description, now, now,
	)
	return err
}

func (s *Store) GetPipe(name, accountID string) (*Pipe, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, account_id, arn, source, target, role_arn, source_parameters, target_parameters, enrichment, state, description, created_at, updated_at
		 FROM pipes WHERE name = ? AND account_id = ?`, name, accountID)
	return scanPipe(row)
}

func (s *Store) ListPipes(accountID string) ([]Pipe, error) {
	rows, err := s.store.DB().Query(
		`SELECT name, account_id, arn, source, target, role_arn, source_parameters, target_parameters, enrichment, state, description, created_at, updated_at
		 FROM pipes WHERE account_id = ? ORDER BY name`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var pipes []Pipe
	for rows.Next() {
		pipe, err := scanPipe(rows)
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, *pipe)
	}
	return pipes, rows.Err()
}

func (s *Store) UpdatePipe(pipe *Pipe) error {
	now := time.Now().Unix()
	res, err := s.store.DB().Exec(
		`UPDATE pipes SET source=?, target=?, role_arn=?, source_parameters=?, target_parameters=?, enrichment=?, state=?, description=?, updated_at=?
		 WHERE name=? AND account_id=?`,
		pipe.Source, pipe.Target, pipe.RoleARN, pipe.SourceParameters, pipe.TargetParameters,
		pipe.Enrichment, pipe.State, pipe.Description, now, pipe.Name, pipe.AccountID,
	)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipeNotFound
	}
	return nil
}

func (s *Store) UpdatePipeState(name, accountID, state string) error {
	res, err := s.store.DB().Exec(
		`UPDATE pipes SET state=? WHERE name=? AND account_id=?`, state, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipeNotFound
	}
	return nil
}

func (s *Store) DeletePipe(name, accountID string) error {
	res, err := s.store.DB().Exec(`DELETE FROM pipes WHERE name=? AND account_id=?`, name, accountID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errPipeNotFound
	}
	return nil
}

func (s *Store) PutTags(pipeARN string, tags map[string]string) error {
	for k, v := range tags {
		_, err := s.store.DB().Exec(
			`INSERT INTO pipe_tags (pipe_arn, tag_key, tag_value) VALUES (?, ?, ?)
			 ON CONFLICT(pipe_arn, tag_key) DO UPDATE SET tag_value=excluded.tag_value`,
			pipeARN, k, v,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetTags(pipeARN string) (map[string]string, error) {
	rows, err := s.store.DB().Query(`SELECT tag_key, tag_value FROM pipe_tags WHERE pipe_arn=?`, pipeARN)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	return tags, rows.Err()
}

func (s *Store) DeleteTags(pipeARN string, keys []string) error {
	for _, k := range keys {
		_, err := s.store.DB().Exec(`DELETE FROM pipe_tags WHERE pipe_arn=? AND tag_key=?`, pipeARN, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetPipeByARN(arn string) (*Pipe, error) {
	row := s.store.DB().QueryRow(
		`SELECT name, account_id, arn, source, target, role_arn, source_parameters, target_parameters, enrichment, state, description, created_at, updated_at
		 FROM pipes WHERE arn = ?`, arn)
	return scanPipe(row)
}

type pipeScanner interface{ Scan(dest ...any) error }

func scanPipe(sc pipeScanner) (*Pipe, error) {
	var pipe Pipe
	var createdAt, updatedAt int64
	err := sc.Scan(&pipe.Name, &pipe.AccountID, &pipe.ARN, &pipe.Source, &pipe.Target,
		&pipe.RoleARN, &pipe.SourceParameters, &pipe.TargetParameters, &pipe.Enrichment,
		&pipe.State, &pipe.Description, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errPipeNotFound
		}
		return nil, err
	}
	pipe.CreatedAt = time.Unix(createdAt, 0)
	pipe.UpdatedAt = time.Unix(updatedAt, 0)
	return &pipe, nil
}
