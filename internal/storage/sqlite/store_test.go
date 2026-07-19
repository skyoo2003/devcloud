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
	defer func() { _ = store.Close() }()

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
	_ = store.Close()

	store2, err := Open(dbPath, migrations)
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()
}

func TestOpen_AppliesNewMigrations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	store, err := Open(dbPath, []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
	})
	require.NoError(t, err)
	_ = store.Close()

	store2, err := Open(dbPath, []Migration{
		{Version: 1, SQL: `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT NOT NULL)`},
		{Version: 2, SQL: `ALTER TABLE items ADD COLUMN description TEXT NOT NULL DEFAULT ''`},
	})
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()

	_, err = store2.DB().Exec(`INSERT INTO items (id, name, description) VALUES ('1', 'test', 'desc')`)
	assert.NoError(t, err)
}

// TestCompareNumericText guards the NUMTEXT comparator on the cases a float CAST
// gets wrong: consecutive integers past 2^53 (which collapse to one double) and
// near-equal high-precision decimals. It also covers numeric-vs-text ordering,
// signs, and the non-numeric byte-order fallback.
func TestCompareNumericText(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"9007199254740992", "9007199254740993", -1}, // 2^53 vs 2^53+1 (float: tie)
		{"9007199254740994", "9007199254740993", 1},  // 2^53+2 vs 2^53+1 (float: tie)
		{"1.0000000000000001", "1.0000000000000002", -1},
		{"2", "10", -1}, // numeric, not lexicographic
		{"10", "2", 1},
		{"-5", "3", -1},
		{"100", "100", 0},
		{"abc", "abd", -1}, // non-numeric → byte order
		{"5", "x", -1},     // one side non-numeric → byte order
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, compareNumericText(c.a, c.b), "compareNumericText(%q, %q)", c.a, c.b)
	}
}

// TestNumtextCollationOrdering proves the collation is registered on the
// connection and changes ORDER BY: plain TEXT order sorts "10","100","2" but
// COLLATE NUMTEXT sorts them numerically.
func TestNumtextCollationOrdering(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(dbPath, []Migration{
		{Version: 1, SQL: `CREATE TABLE nums (v TEXT NOT NULL)`},
	})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	for _, v := range []string{"100", "2", "10"} {
		_, err := store.DB().Exec(`INSERT INTO nums (v) VALUES (?)`, v)
		require.NoError(t, err)
	}

	read := func(orderBy string) []string {
		rows, err := store.DB().Query(`SELECT v FROM nums ORDER BY ` + orderBy)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var v string
			require.NoError(t, rows.Scan(&v))
			out = append(out, v)
		}
		return out
	}

	assert.Equal(t, []string{"10", "100", "2"}, read("v"))                 // byte order
	assert.Equal(t, []string{"2", "10", "100"}, read("v COLLATE NUMTEXT")) // numeric order
}
