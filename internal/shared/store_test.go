// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testItem struct {
	ID   string
	Name string
}

var testMigrations = []sqlite.Migration{
	{Version: 1, SQL: `
		CREATE TABLE items (
			id   TEXT PRIMARY KEY,
			name TEXT NOT NULL
		);
	`},
}

func testScanner(s Scanner) (testItem, error) {
	var item testItem
	err := s.Scan(&item.ID, &item.Name)
	return item, err
}

func newTestResourceStore(t *testing.T) *ResourceStore[testItem] {
	t.Helper()
	dbPath := t.TempDir() + "/test.db"
	db, err := sqlite.Open(dbPath, testMigrations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rs, err := NewResourceStore[testItem](db, "items", "id", "id, name", testScanner)
	require.NoError(t, err)
	return rs
}

func TestResourceStore_GetNotFound(t *testing.T) {
	rs := newTestResourceStore(t)
	_, err := rs.Get("nonexistent")
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestResourceStore_InsertAndGet(t *testing.T) {
	rs := newTestResourceStore(t)
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "1", "alpha"))

	item, err := rs.Get("1")
	require.NoError(t, err)
	assert.Equal(t, "1", item.ID)
	assert.Equal(t, "alpha", item.Name)
}

func TestResourceStore_List(t *testing.T) {
	rs := newTestResourceStore(t)
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "1", "alpha"))
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "2", "beta"))

	items, err := rs.List("", nil)
	require.NoError(t, err)
	assert.Len(t, items, 2)
}

func TestResourceStore_ListWithFilter(t *testing.T) {
	rs := newTestResourceStore(t)
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "1", "alpha"))
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "2", "beta"))

	items, err := rs.List("name = ?", []any{"beta"})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "beta", items[0].Name)
}

func TestResourceStore_Delete(t *testing.T) {
	rs := newTestResourceStore(t)
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "1", "alpha"))

	deleted, err := rs.Delete("1")
	require.NoError(t, err)
	assert.True(t, deleted)

	_, err = rs.Get("1")
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestResourceStore_DeleteNotFound(t *testing.T) {
	rs := newTestResourceStore(t)
	deleted, err := rs.Delete("nonexistent")
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestResourceStore_Count(t *testing.T) {
	rs := newTestResourceStore(t)
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "1", "alpha"))
	require.NoError(t, rs.Exec("INSERT INTO items (id, name) VALUES (?, ?)", "2", "beta"))

	n, err := rs.Count("", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	n, err = rs.Count("name = ?", []any{"alpha"})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func newTestDB(t *testing.T) *sqlite.Store {
	t.Helper()
	dbPath := t.TempDir() + "/test.db"
	db, err := sqlite.Open(dbPath, testMigrations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNewResourceStore_InvalidTable(t *testing.T) {
	db := newTestDB(t)
	_, err := NewResourceStore[testItem](db, "DROP TABLE items; --", "id", "id", testScanner)
	assert.ErrorContains(t, err, "invalid table identifier")
}

func TestNewResourceStore_InvalidIdCol(t *testing.T) {
	db := newTestDB(t)
	_, err := NewResourceStore[testItem](db, "items", "id; --", "id", testScanner)
	assert.ErrorContains(t, err, "invalid idCol identifier")
}

func TestNewResourceStore_InvalidCol(t *testing.T) {
	db := newTestDB(t)
	_, err := NewResourceStore[testItem](db, "items", "id", "id, name; --", testScanner)
	assert.ErrorContains(t, err, "invalid col identifier")
}

func TestNewResourceStore_EmptyTable(t *testing.T) {
	db := newTestDB(t)
	_, err := NewResourceStore[testItem](db, "", "id", "id", testScanner)
	assert.ErrorContains(t, err, "empty table identifier")
}

func TestNewResourceStore_TrailingComma(t *testing.T) {
	db := newTestDB(t)
	rs, err := NewResourceStore[testItem](db, "items", "id", "id, name,", testScanner)
	require.NoError(t, err)
	require.NotNil(t, rs)
}

func TestNewResourceStore_ValidIdentifiers(t *testing.T) {
	db := newTestDB(t)

	tests := []struct {
		name      string
		tableName string
		primary   string
		cols      string
	}{
		{
			name:      "underscores",
			tableName: "items",
			primary:   "id",
			cols:      "id, item_name, created_at",
		},
		{
			name:      "digits_in_identifiers",
			tableName: "items",
			primary:   "id",
			cols:      "id, name2, col3_v1",
		},
		{
			name:      "trailing_comma",
			tableName: "items",
			primary:   "id",
			cols:      "id, name,",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs, err := NewResourceStore[testItem](db, tc.tableName, tc.primary, tc.cols, testScanner)
			require.NoError(t, err)
			require.NotNil(t, rs)
		})
	}
}
