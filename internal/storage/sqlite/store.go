// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	sqlite "modernc.org/sqlite"
)

// driverName is the driver modernc.org/sqlite registers on import. It is a pure
// Go translation of SQLite, chosen because DevCloud's released binaries are
// built with CGO_ENABLED=0: a cgo-only driver still compiles under that flag
// and then exits at startup, which is how every tagged binary through v0.2.0
// shipped unable to open its own database.
const driverName = "sqlite"

// NUMTEXT orders TEXT columns holding numeric strings by true numeric value
// (exact across DynamoDB's full 38-digit precision), which a float CAST cannot
// do past 2^53. Registration is process-wide here, so every connection carries
// it without a per-connection hook.
func init() {
	sqlite.MustRegisterCollationUtf8("NUMTEXT", compareNumericText)
}

func compareNumericText(a, b string) int {
	ra, oka := new(big.Rat).SetString(a)
	rb, okb := new(big.Rat).SetString(b)
	if oka && okb {
		return ra.Cmp(rb)
	}
	return strings.Compare(a, b) // non-numeric values fall back to byte order
}

type Migration struct {
	Version int
	SQL     string
}

// Scanner is implemented by *sql.Row and *sql.Rows; it scans one row into dest.
type Scanner interface {
	Scan(dest ...any) error
}

type Store struct {
	db   *sql.DB
	path string
}

func Open(dbPath string, migrations []Migration) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// busy_timeout has to travel in the DSN so every pooled connection gets it.
	// It is per-connection state, unlike journal_mode below, which the database
	// file remembers — a single Exec here would configure whichever connection
	// happened to answer and leave the rest at SQLite's default of no waiting.
	// mattn/go-sqlite3 applied 5s unconditionally; modernc.org/sqlite installs
	// no busy handler unless asked, so without this a second concurrent writer
	// fails instantly with SQLITE_BUSY instead of waiting its turn.
	db, err := sql.Open(driverName, dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}

	s := &Store{db: db, path: dbPath}
	if err := s.migrate(migrations); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return s, nil
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(migrations []Migration) error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)`)
	if err != nil {
		return fmt.Errorf("create schema_version: %w", err)
	}

	var current int
	row := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`)
	if err := row.Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		if _, err := s.db.Exec(m.SQL); err != nil {
			return fmt.Errorf("migration v%d: %w", m.Version, err)
		}
		if _, err := s.db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, m.Version); err != nil {
			return fmt.Errorf("record migration v%d: %w", m.Version, err)
		}
	}

	return nil
}

func IsUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}
