// SPDX-License-Identifier: Apache-2.0
package eventbridge

import (
	"path/filepath"
	"testing"

	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
)

func TestMigrationCanonicalStateUpgrade(t *testing.T) {
	path := t.TempDir()
	legacy, err := sqlite.Open(filepath.Join(path, "eventbridge.db"), append(migrations[:2:2], shared.TagMigrations...))
	require.NoError(t, err)
	_, err = legacy.DB().Exec(`INSERT INTO event_buses VALUES ('legacy','legacy-arn','000000000000'); INSERT INTO resource_tags VALUES ('legacy-arn','keep','value')`)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())
	s, err := NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	for _, table := range []string{"partner_sources", "connections", "bus_policies", "legacy_imports"} {
		var count int
		require.NoError(t, s.store.DB().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count))
		require.Equal(t, 1, count, table)
	}
	bus, err := s.GetEventBus("legacy", "000000000000")
	require.NoError(t, err)
	require.Equal(t, "legacy-arn", bus.ARN)
	tags, err := s.tags.ListTags("legacy-arn")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"keep": "value"}, tags)
}

func TestMigrationPartialCanonicalDDLRetry(t *testing.T) {
	path := t.TempDir()
	legacy, err := sqlite.Open(filepath.Join(path, "eventbridge.db"), append(migrations[:2:2], shared.TagMigrations...))
	require.NoError(t, err)
	_, err = legacy.DB().Exec(`CREATE TABLE partner_sources (name TEXT NOT NULL, account_id TEXT NOT NULL, document_json BLOB NOT NULL, PRIMARY KEY(name,account_id))`)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())
	s, err := NewEBStore(path)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	var count int
	require.NoError(t, s.store.DB().QueryRow(`SELECT count(*) FROM schema_version WHERE version=1001`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestMigrationInterruptedBeforeTagsRetry(t *testing.T) {
	path := t.TempDir()
	original := shared.TagMigrations
	defer func() { shared.TagMigrations = original }()
	shared.TagMigrations = []sqlite.Migration{{Version: 1000, SQL: "INVALID SQL TO INTERRUPT MIGRATION"}}
	_, err := NewEBStore(path)
	require.Error(t, err)
	shared.TagMigrations = original
	s, err := NewEBStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.tags.AddTags(busARN("default", defaultAccountID), map[string]string{"retry": "preserved"}))
	tags, err := s.tags.ListTags(busARN("default", defaultAccountID))
	require.NoError(t, err)
	require.Equal(t, "preserved", tags["retry"])
	require.NoError(t, s.DeleteEventBus("default", defaultAccountID))
}
