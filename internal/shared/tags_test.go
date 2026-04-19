// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"testing"

	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTagStore(t *testing.T) *TagStore {
	t.Helper()
	dbPath := t.TempDir() + "/tags.db"
	db, err := sqlite.Open(dbPath, TagMigrations)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewTagStore(db)
}

func TestTagStore_AddAndList(t *testing.T) {
	ts := newTestTagStore(t)
	err := ts.AddTags("arn:aws:s3:::bucket1", map[string]string{"env": "prod", "team": "infra"})
	require.NoError(t, err)

	tags, err := ts.ListTags("arn:aws:s3:::bucket1")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, tags)
}

func TestTagStore_UpsertTag(t *testing.T) {
	ts := newTestTagStore(t)
	require.NoError(t, ts.AddTags("arn:1", map[string]string{"k": "v1"}))
	require.NoError(t, ts.AddTags("arn:1", map[string]string{"k": "v2"}))

	tags, err := ts.ListTags("arn:1")
	require.NoError(t, err)
	assert.Equal(t, "v2", tags["k"])
}

func TestTagStore_RemoveTags(t *testing.T) {
	ts := newTestTagStore(t)
	require.NoError(t, ts.AddTags("arn:1", map[string]string{"a": "1", "b": "2", "c": "3"}))
	require.NoError(t, ts.RemoveTags("arn:1", []string{"b"}))

	tags, err := ts.ListTags("arn:1")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "c": "3"}, tags)
}

func TestTagStore_DeleteAllTags(t *testing.T) {
	ts := newTestTagStore(t)
	require.NoError(t, ts.AddTags("arn:1", map[string]string{"a": "1", "b": "2"}))
	require.NoError(t, ts.DeleteAllTags("arn:1"))

	tags, err := ts.ListTags("arn:1")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestTagStore_ListTags_Empty(t *testing.T) {
	ts := newTestTagStore(t)
	tags, err := ts.ListTags("arn:nonexistent")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestTagStore_IsolationBetweenARNs(t *testing.T) {
	ts := newTestTagStore(t)
	require.NoError(t, ts.AddTags("arn:1", map[string]string{"env": "prod"}))
	require.NoError(t, ts.AddTags("arn:2", map[string]string{"env": "dev"}))

	tags1, _ := ts.ListTags("arn:1")
	tags2, _ := ts.ListTags("arn:2")
	assert.Equal(t, "prod", tags1["env"])
	assert.Equal(t, "dev", tags2["env"])
}
