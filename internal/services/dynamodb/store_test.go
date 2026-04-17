// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string { return &s }

// newTestStore creates a DynamoStore backed by a temporary BadgerDB directory.
func newTestStore(t *testing.T) *DynamoStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewDynamoStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// TestDynamoStore_CreateAndListTables verifies that a created table appears in ListTables.
func TestDynamoStore_CreateAndListTables(t *testing.T) {
	store := newTestStore(t)

	err := store.CreateTable(TableInfo{
		Name:         "Users",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		Status:       "ACTIVE",
	})
	require.NoError(t, err)

	tables := store.ListTables()
	assert.Contains(t, tables, "Users")
}

// TestDynamoStore_CreateDuplicateTable verifies that creating an existing table returns ErrTableAlreadyExists.
func TestDynamoStore_CreateDuplicateTable(t *testing.T) {
	store := newTestStore(t)

	info := TableInfo{
		Name:         "Orders",
		PartitionKey: KeyDef{Name: "OrderID", Type: "S"},
	}
	require.NoError(t, store.CreateTable(info))

	err := store.CreateTable(info)
	assert.ErrorIs(t, err, ErrTableAlreadyExists)
}

// TestDynamoStore_PutAndGetItem verifies that a stored item can be retrieved by key.
func TestDynamoStore_PutAndGetItem(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Products",
		PartitionKey: KeyDef{Name: "ProductID", Type: "S"},
	}))

	item := Item{
		"ProductID": {S: strPtr("prod-1")},
		"Name":      {S: strPtr("Widget")},
		"Price":     {N: strPtr("9.99")},
	}
	require.NoError(t, store.PutItem("Products", item))

	got, err := store.GetItem("Products", Item{
		"ProductID": {S: strPtr("prod-1")},
	})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, strPtr("Widget"), (*got)["Name"].S)
	assert.Equal(t, strPtr("9.99"), (*got)["Price"].N)
}

// TestDynamoStore_DeleteItem verifies that a deleted item is no longer retrievable.
func TestDynamoStore_DeleteItem(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Sessions",
		PartitionKey: KeyDef{Name: "SessionID", Type: "S"},
	}))

	item := Item{"SessionID": {S: strPtr("sess-42")}}
	require.NoError(t, store.PutItem("Sessions", item))

	key := Item{"SessionID": {S: strPtr("sess-42")}}
	require.NoError(t, store.DeleteItem("Sessions", key))

	_, err := store.GetItem("Sessions", key)
	assert.ErrorIs(t, err, ErrItemNotFound)
}

// TestDynamoStore_Query verifies that querying by partition key returns all matching items.
func TestDynamoStore_Query(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Events",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		SortKey:      &KeyDef{Name: "EventTime", Type: "S"},
	}))

	items := []Item{
		{"UserID": {S: strPtr("user-1")}, "EventTime": {S: strPtr("2024-01-01")}, "Type": {S: strPtr("login")}},
		{"UserID": {S: strPtr("user-1")}, "EventTime": {S: strPtr("2024-01-02")}, "Type": {S: strPtr("purchase")}},
		{"UserID": {S: strPtr("user-1")}, "EventTime": {S: strPtr("2024-01-03")}, "Type": {S: strPtr("logout")}},
	}
	for _, it := range items {
		require.NoError(t, store.PutItem("Events", it))
	}

	results, err := store.Query("Events", "user-1", "")
	require.NoError(t, err)
	assert.Len(t, results, 3)
}

// TestDynamoStore_Scan verifies that Scan returns all items in a table.
func TestDynamoStore_Scan(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Inventory",
		PartitionKey: KeyDef{Name: "SKU", Type: "S"},
	}))

	for i, name := range []string{"alpha", "beta", "gamma"} {
		item := Item{
			"SKU": {S: strPtr(name)},
			"Qty": {N: strPtr(string(rune('0' + i + 1)))},
		}
		require.NoError(t, store.PutItem("Inventory", item))
	}

	results, err := store.Scan("Inventory")
	require.NoError(t, err)
	assert.Len(t, results, 3)
}

// TestDynamoStore_DeleteTable verifies that a deleted table no longer appears in ListTables.
func TestDynamoStore_DeleteTable(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Temp",
		PartitionKey: KeyDef{Name: "ID", Type: "S"},
	}))

	require.NoError(t, store.DeleteTable("Temp"))

	tables := store.ListTables()
	assert.NotContains(t, tables, "Temp")
	assert.Empty(t, tables)
}
