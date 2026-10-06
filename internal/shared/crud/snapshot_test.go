// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func snapshotIDs(rows []ResourceSnapshot) []string {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	return ids
}

func TestSnapshotMemoryIsDetached(t *testing.T) {
	require.NoError(t, Close())
	require.NoError(t, Reset())
	t.Cleanup(func() { require.NoError(t, Reset()) })
	require.NoError(t, put("snapshot", "Connection", "key", map[string]any{"AuthParameters": map[string]any{"Value": "original"}}))
	rows, err := Snapshot("snapshot", []string{"Connection"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.JSONEq(t, `{"AuthParameters":{"Value":"original"}}`, string(rows[0].JSON))
	rows[0].JSON[0] = 'x'
	again, err := Snapshot("snapshot", []string{"Connection"})
	require.NoError(t, err)
	require.JSONEq(t, `{"AuthParameters":{"Value":"original"}}`, string(again[0].JSON))
	empty, err := Snapshot("snapshot", nil)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)
}

func TestSnapshotSQLiteStableOrder(t *testing.T) {
	require.NoError(t, Open(filepath.Join(t.TempDir(), "crud.db")))
	t.Cleanup(func() { require.NoError(t, Close()) })
	for _, id := range []string{"b", "a"} {
		require.NoError(t, put("snapshot", "Connection", id, map[string]any{"Name": id}))
	}
	require.NoError(t, put("snapshot", "Source", "z", map[string]any{"Name": "z"}))
	rows, err := Snapshot("snapshot", []string{"Source", "Connection", "Connection"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "z"}, snapshotIDs(rows))
	require.Equal(t, "Connection", rows[0].Resource)
	require.JSONEq(t, `{"Name":"a"}`, string(rows[0].JSON))
}

func TestSnapshotServiceIsolation(t *testing.T) {
	for _, durable := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "sqlite"}[durable], func(t *testing.T) {
			require.NoError(t, Close())
			require.NoError(t, Reset())
			if durable {
				require.NoError(t, Open(filepath.Join(t.TempDir(), "crud.db")))
			}
			t.Cleanup(func() { require.NoError(t, Close()); require.NoError(t, Reset()) })
			require.NoError(t, put("wanted", "Connection", "a", map[string]any{"Name": "a"}))
			require.NoError(t, put("other", "Connection", "b", map[string]any{"Name": "b"}))
			require.NoError(t, put("wanted", "Other", "c", map[string]any{"Name": "c"}))
			rows, err := Snapshot("wanted", []string{"Connection"})
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, snapshotIDs(rows))
		})
	}
}

func TestSnapshotFailureDoesNotMutate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crud.db")
	require.NoError(t, Open(path))
	t.Cleanup(func() { require.NoError(t, Close()) })
	require.NoError(t, put("snapshot", "Connection", "a", map[string]any{"Name": "a"}))
	require.NoError(t, db.DB().Close())
	_, err := Snapshot("snapshot", []string{"Connection"})
	require.Error(t, err)
	require.NoError(t, Open(path))
	rows, err := Snapshot("snapshot", []string{"Connection"})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(rows[0].JSON, &doc))
	require.Equal(t, "a", doc["Name"])
}
