// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"fmt"
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

// TestDynamoStore_QueryGSI verifies indexed queries against a secondary index,
// including that updates and deletes keep the projection in sync.
func TestDynamoStore_QueryGSI(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Users",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		GlobalSecondaryIndexes: []IndexDef{{
			IndexName: "ByEmail",
			KeySchema: []KeyDef{{Name: "Email", Type: "S", KeyType: "HASH"}},
		}},
	}))

	require.NoError(t, store.PutItem("Users", Item{"UserID": {S: strPtr("u1")}, "Email": {S: strPtr("a@x.com")}}))
	require.NoError(t, store.PutItem("Users", Item{"UserID": {S: strPtr("u2")}, "Email": {S: strPtr("a@x.com")}}))
	require.NoError(t, store.PutItem("Users", Item{"UserID": {S: strPtr("u3")}, "Email": {S: strPtr("b@x.com")}}))

	got, err := store.QueryGSI("Users", "ByEmail", "a@x.com")
	require.NoError(t, err)
	assert.Len(t, got, 2)

	// Unknown index → empty result, not an error.
	none, err := store.QueryGSI("Users", "Nope", "a@x.com")
	require.NoError(t, err)
	assert.Empty(t, none)

	// Updating an item's index key re-syncs its projection (no stale rows).
	require.NoError(t, store.PutItem("Users", Item{"UserID": {S: strPtr("u1")}, "Email": {S: strPtr("b@x.com")}}))
	got, err = store.QueryGSI("Users", "ByEmail", "a@x.com")
	require.NoError(t, err)
	assert.Len(t, got, 1) // only u2 remains under a@x.com

	// Deleting an item removes it from the index too.
	require.NoError(t, store.DeleteItem("Users", Item{"UserID": {S: strPtr("u2")}}))
	got, err = store.QueryGSI("Users", "ByEmail", "a@x.com")
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestDynamoStore_QueryGSIVariants covers QueryGSI edge cases: an unknown table,
// a local secondary index, an index with HASH+RANGE keys, and a binary key.
func TestDynamoStore_QueryGSIVariants(t *testing.T) {
	t.Run("missing table returns ErrTableNotFound", func(t *testing.T) {
		store := newTestStore(t)
		_, err := store.QueryGSI("Nope", "ByEmail", "x")
		assert.ErrorIs(t, err, ErrTableNotFound)
	})

	t.Run("local secondary index is queryable", func(t *testing.T) {
		store := newTestStore(t)
		require.NoError(t, store.CreateTable(TableInfo{
			Name:         "Events",
			PartitionKey: KeyDef{Name: "UserID", Type: "S"},
			SortKey:      &KeyDef{Name: "Ts", Type: "S"},
			LocalSecondaryIndexes: []IndexDef{{
				IndexName: "ByScore",
				KeySchema: []KeyDef{
					{Name: "UserID", Type: "S", KeyType: "HASH"},
					{Name: "Score", Type: "N", KeyType: "RANGE"},
				},
			}},
		}))
		require.NoError(t, store.PutItem("Events", Item{"UserID": {S: strPtr("u1")}, "Ts": {S: strPtr("t1")}, "Score": {N: strPtr("10")}}))
		require.NoError(t, store.PutItem("Events", Item{"UserID": {S: strPtr("u1")}, "Ts": {S: strPtr("t2")}, "Score": {N: strPtr("20")}}))
		require.NoError(t, store.PutItem("Events", Item{"UserID": {S: strPtr("u2")}, "Ts": {S: strPtr("t3")}, "Score": {N: strPtr("30")}}))

		got, err := store.QueryGSI("Events", "ByScore", "u1")
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})

	t.Run("index with HASH and RANGE keys, items vary by base sort key", func(t *testing.T) {
		store := newTestStore(t)
		require.NoError(t, store.CreateTable(TableInfo{
			Name:         "Messages",
			PartitionKey: KeyDef{Name: "MsgID", Type: "S"},
			SortKey:      &KeyDef{Name: "Seq", Type: "S"},
			GlobalSecondaryIndexes: []IndexDef{{
				IndexName: "ByConversation",
				KeySchema: []KeyDef{
					{Name: "ConvID", Type: "S", KeyType: "HASH"},
					{Name: "CreatedAt", Type: "N", KeyType: "RANGE"},
				},
			}},
		}))
		// Two distinct items sharing the GSI hash key but differing in base key.
		require.NoError(t, store.PutItem("Messages", Item{"MsgID": {S: strPtr("m1")}, "Seq": {S: strPtr("1")}, "ConvID": {S: strPtr("c1")}, "CreatedAt": {N: strPtr("100")}}))
		require.NoError(t, store.PutItem("Messages", Item{"MsgID": {S: strPtr("m1")}, "Seq": {S: strPtr("2")}, "ConvID": {S: strPtr("c1")}, "CreatedAt": {N: strPtr("200")}}))
		require.NoError(t, store.PutItem("Messages", Item{"MsgID": {S: strPtr("m2")}, "Seq": {S: strPtr("1")}, "ConvID": {S: strPtr("c2")}, "CreatedAt": {N: strPtr("300")}}))

		got, err := store.QueryGSI("Messages", "ByConversation", "c1")
		require.NoError(t, err)
		assert.Len(t, got, 2) // both c1 items projected, no collision on same gsi_pk
	})

	t.Run("binary hash key", func(t *testing.T) {
		store := newTestStore(t)
		require.NoError(t, store.CreateTable(TableInfo{
			Name:         "Blobs",
			PartitionKey: KeyDef{Name: "BlobID", Type: "S"},
			GlobalSecondaryIndexes: []IndexDef{{
				IndexName: "ByDigest",
				KeySchema: []KeyDef{{Name: "Digest", Type: "B", KeyType: "HASH"}},
			}},
		}))
		d1 := []byte{0x01, 0x02, 0x03}
		d2 := []byte{0x04, 0x05, 0x06}
		require.NoError(t, store.PutItem("Blobs", Item{"BlobID": {S: strPtr("b1")}, "Digest": {B: d1}}))
		require.NoError(t, store.PutItem("Blobs", Item{"BlobID": {S: strPtr("b2")}, "Digest": {B: d1}}))
		require.NoError(t, store.PutItem("Blobs", Item{"BlobID": {S: strPtr("b3")}, "Digest": {B: d2}}))

		got, err := store.QueryGSI("Blobs", "ByDigest", string(d1))
		require.NoError(t, err)
		assert.Len(t, got, 2)
	})
}

// TestDynamoStore_KeyValidationErrors verifies keyValues surfaces missing-key
// errors on the item write/read/delete paths.
func TestDynamoStore_KeyValidationErrors(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Orders",
		PartitionKey: KeyDef{Name: "OrderID", Type: "S"},
		SortKey:      &KeyDef{Name: "LineID", Type: "S"},
	}))

	t.Run("PutItem missing partition key", func(t *testing.T) {
		err := store.PutItem("Orders", Item{"LineID": {S: strPtr("L1")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing partition key attribute")
	})
	t.Run("PutItem missing sort key", func(t *testing.T) {
		err := store.PutItem("Orders", Item{"OrderID": {S: strPtr("O1")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing sort key attribute")
	})
	t.Run("GetItem missing partition key", func(t *testing.T) {
		_, err := store.GetItem("Orders", Item{"LineID": {S: strPtr("L1")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing partition key attribute")
	})
	t.Run("DeleteItem missing sort key", func(t *testing.T) {
		err := store.DeleteItem("Orders", Item{"OrderID": {S: strPtr("O1")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing sort key attribute")
	})
}

// TestDynamoStore_NumericKey verifies non-string (N) key attributes round-trip.
func TestDynamoStore_NumericKey(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Accounts",
		PartitionKey: KeyDef{Name: "AccountID", Type: "N"},
	}))
	require.NoError(t, store.PutItem("Accounts", Item{"AccountID": {N: strPtr("1001")}, "Name": {S: strPtr("Primary")}}))

	got, err := store.GetItem("Accounts", Item{"AccountID": {N: strPtr("1001")}})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, strPtr("Primary"), (*got)["Name"].S)
}

// TestDynamoStore_QuerySortKeyPrefixEscaping verifies a sort-key prefix matches
// LIKE metacharacters (%, _, \) literally (the range scan has no wildcards).
func TestDynamoStore_QuerySortKeyPrefixEscaping(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Files",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		SortKey:      &KeyDef{Name: "Path", Type: "S"},
	}))
	for _, p := range []string{"a%b", "a_b", "axb", `a\z`} {
		require.NoError(t, store.PutItem("Files", Item{"UserID": {S: strPtr("u1")}, "Path": {S: strPtr(p)}}))
	}

	tests := []struct {
		name   string
		prefix string
		want   int
	}{
		{"no special chars matches all", "a", 4},
		{"literal percent", "a%", 1},
		{"literal underscore", "a_", 1},
		{"literal backslash", `a\`, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Query("Files", "u1", tt.prefix)
			require.NoError(t, err)
			assert.Len(t, got, tt.want)
		})
	}
}

// TestDynamoStore_QueryNumericSortOrder verifies a numeric ('N') sort key sorts
// numerically, not lexicographically (2, 10, 100 — not 10, 100, 2).
func TestDynamoStore_QueryNumericSortOrder(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Readings",
		PartitionKey: KeyDef{Name: "Sensor", Type: "S"},
		SortKey:      &KeyDef{Name: "Seq", Type: "N"},
	}))
	for _, seq := range []string{"100", "2", "10", "9"} {
		require.NoError(t, store.PutItem("Readings", Item{"Sensor": {S: strPtr("s1")}, "Seq": {N: strPtr(seq)}}))
	}

	got, err := store.Query("Readings", "s1", "")
	require.NoError(t, err)
	var order []string
	for _, it := range got {
		order = append(order, *it["Seq"].N)
	}
	assert.Equal(t, []string{"2", "9", "10", "100"}, order)
}

// TestDynamoStore_QueryPrefixCaseSensitive verifies a begins_with prefix matches
// case-sensitively (the old LIKE path over-matched across letter case).
func TestDynamoStore_QueryPrefixCaseSensitive(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Files",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		SortKey:      &KeyDef{Name: "Path", Type: "S"},
	}))
	for _, p := range []string{"ABC", "aBc", "abc"} {
		require.NoError(t, store.PutItem("Files", Item{"UserID": {S: strPtr("u1")}, "Path": {S: strPtr(p)}}))
	}
	got, err := store.Query("Files", "u1", "AB")
	require.NoError(t, err)
	assert.Len(t, got, 1) // only "ABC", not "aBc"/"abc"
}

// TestDynamoStore_QueryGSISortOrder verifies QueryGSI returns items ordered by
// the index RANGE key, numerically for an 'N' key.
func TestDynamoStore_QueryGSISortOrder(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Messages",
		PartitionKey: KeyDef{Name: "MsgID", Type: "S"},
		GlobalSecondaryIndexes: []IndexDef{{
			IndexName: "ByConversation",
			KeySchema: []KeyDef{
				{Name: "ConvID", Type: "S", KeyType: "HASH"},
				{Name: "CreatedAt", Type: "N", KeyType: "RANGE"},
			},
		}},
	}))
	for i, ts := range []string{"300", "100", "200"} {
		require.NoError(t, store.PutItem("Messages", Item{
			"MsgID": {S: strPtr(fmt.Sprintf("m%d", i))}, "ConvID": {S: strPtr("c1")}, "CreatedAt": {N: strPtr(ts)},
		}))
	}
	got, err := store.QueryGSI("Messages", "ByConversation", "c1")
	require.NoError(t, err)
	var order []string
	for _, it := range got {
		order = append(order, *it["CreatedAt"].N)
	}
	assert.Equal(t, []string{"100", "200", "300"}, order)
}

// TestDynamoStore_GSISparseComposite verifies an item missing a composite index's
// RANGE attribute is not projected (no phantom item in the index).
func TestDynamoStore_GSISparseComposite(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Messages",
		PartitionKey: KeyDef{Name: "MsgID", Type: "S"},
		GlobalSecondaryIndexes: []IndexDef{{
			IndexName: "ByConversation",
			KeySchema: []KeyDef{
				{Name: "ConvID", Type: "S", KeyType: "HASH"},
				{Name: "CreatedAt", Type: "N", KeyType: "RANGE"},
			},
		}},
	}))
	// Has HASH but no RANGE attribute → excluded from the index.
	require.NoError(t, store.PutItem("Messages", Item{"MsgID": {S: strPtr("m1")}, "ConvID": {S: strPtr("c1")}}))
	// Full key → included.
	require.NoError(t, store.PutItem("Messages", Item{"MsgID": {S: strPtr("m2")}, "ConvID": {S: strPtr("c1")}, "CreatedAt": {N: strPtr("10")}}))

	got, err := store.QueryGSI("Messages", "ByConversation", "c1")
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

// TestDynamoStore_GSIBadKeyType verifies a present-but-non-scalar index key value
// fails the put rather than silently dropping the item from the index.
func TestDynamoStore_GSIBadKeyType(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.CreateTable(TableInfo{
		Name:         "Users",
		PartitionKey: KeyDef{Name: "UserID", Type: "S"},
		GlobalSecondaryIndexes: []IndexDef{{
			IndexName: "ByFlag",
			KeySchema: []KeyDef{{Name: "Flag", Type: "S", KeyType: "HASH"}},
		}},
	}))
	tru := true
	err := store.PutItem("Users", Item{"UserID": {S: strPtr("u1")}, "Flag": {BOOL: &tru}})
	require.Error(t, err)
}

// TestDynamoStore_DeleteTableClearsTTLAndTags verifies DeleteTable removes the
// table's TTL config and tags, so a recreated same-name table doesn't inherit them.
func TestDynamoStore_DeleteTableClearsTTLAndTags(t *testing.T) {
	store := newTestStore(t)
	info := TableInfo{Name: "T", PartitionKey: KeyDef{Name: "ID", Type: "S"}}
	require.NoError(t, store.CreateTable(info))
	require.NoError(t, store.PutTTLConfig("T", TTLConfig{Enabled: true, AttributeName: "exp"}))

	arn, err := store.GetTable("T")
	require.NoError(t, err)
	require.NoError(t, store.PutTags(arn.TableArn, map[string]string{"env": "test"}))

	require.NoError(t, store.DeleteTable("T"))
	require.NoError(t, store.CreateTable(info))

	ttl, err := store.GetTTLConfig("T")
	require.NoError(t, err)
	assert.False(t, ttl.Enabled)
	assert.Empty(t, ttl.AttributeName)

	recreated, err := store.GetTable("T")
	require.NoError(t, err)
	tags, err := store.GetTags(recreated.TableArn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}
