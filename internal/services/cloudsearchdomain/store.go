// SPDX-License-Identifier: Apache-2.0

// internal/services/cloudsearchdomain/store.go
package cloudsearchdomain

import (
	"path/filepath"
	"time"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
)

var migrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE IF NOT EXISTS documents (
			id         TEXT PRIMARY KEY,
			fields     TEXT NOT NULL DEFAULT '{}',
			updated_at INTEGER NOT NULL
		);
	`},
}

// Document is one indexed document. Fields is kept as the raw JSON the caller
// uploaded, because the search here is a substring match over exactly that text.
type Document struct {
	ID     string
	Fields string
}

type Store struct {
	store *sqlite.Store
}

func NewStore(dataDir string) (*Store, error) {
	dbPath := filepath.Join(dataDir, "cloudsearchdomain.db")
	s, err := sqlite.Open(dbPath, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func (s *Store) Close() error { return s.store.Close() }

func (s *Store) Upsert(id, fields string) error {
	_, err := s.store.DB().Exec(`
		INSERT INTO documents (id, fields, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			fields = excluded.fields,
			updated_at = excluded.updated_at`,
		id, fields, time.Now().Unix(),
	)
	return err
}

// Delete removes a document. A missing id is not an error: the batch format
// counts deletes, it does not report which ones matched.
func (s *Store) Delete(id string) error {
	_, err := s.store.DB().Exec(`DELETE FROM documents WHERE id = ?`, id)
	return err
}

func (s *Store) All() ([]Document, error) {
	rows, err := s.store.DB().Query(`SELECT id, fields FROM documents ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	docs := []Document{}
	for rows.Next() {
		var d Document
		if err := rows.Scan(&d.ID, &d.Fields); err != nil {
			return nil, err
		}
		docs = append(docs, d)
	}
	return docs, rows.Err()
}
