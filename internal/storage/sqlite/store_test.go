// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_CreatesDatabaseAndRunsMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	migrations := []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
	}

	store, err := Open(dbPath, migrations)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.DB().Exec(`INSERT INTO items (id, name) VALUES ('1', 'test')`)
	assert.NoError(t, err)
}

func TestOpen_SkipsAlreadyAppliedMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	migrations := []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
	}

	store, err := Open(dbPath, migrations)
	require.NoError(t, err)
	store.Close()

	store2, err := Open(dbPath, migrations)
	require.NoError(t, err)
	defer store2.Close()
}

func TestOpen_AppliesNewMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := Open(dbPath, []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
	})
	require.NoError(t, err)
	store.Close()

	store2, err := Open(dbPath, []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
		{Version: 2, SQL: `ALTER TABLE items ADD COLUMN description TEXT NOT NULL DEFAULT ''`},
	})
	require.NoError(t, err)
	defer store2.Close()

	_, err = store2.DB().Exec(`INSERT INTO items (id, name, description) VALUES ('1', 'test', 'desc')`)
	assert.NoError(t, err)
}
