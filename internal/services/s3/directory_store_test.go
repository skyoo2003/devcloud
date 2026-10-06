// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"database/sql"
	"github.com/skyoo2003/devcloud/internal/storage/sqlite"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func snapshotDirectoryLegacy(t *testing.T, db *sql.DB) map[string][][]any {
	t.Helper()
	result := map[string][][]any{}
	for _, table := range []string{"buckets", "objects", "multipart_uploads", "upload_parts", "bucket_policies", "bucket_versioning", "bucket_cors", "bucket_tags", "object_tags", "bucket_acls", "bucket_notifications"} {
		rows, err := db.Query("SELECT * FROM " + table + " ORDER BY 1,2")
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(cols))
			refs := make([]any, len(cols))
			for i := range refs {
				refs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(refs...))
			result[table] = append(result[table], values)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return result
}

func TestDirectoryMigrationPreservesV11AndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	old, err := sqlite.Open(path, metadataMigrations[:11])
	require.NoError(t, err)
	for _, stmt := range []string{
		`INSERT INTO buckets VALUES ('old','us-east-1','000000000000',123)`,
		`INSERT INTO objects VALUES ('old','key',4,'text/plain','etag','000000000000',123)`,
		`INSERT INTO multipart_uploads VALUES ('old-upload','old','result','000000000000',123)`,
		`INSERT INTO upload_parts VALUES ('old-upload',1,'part-etag',4)`,
		`INSERT INTO bucket_policies VALUES ('old','000000000000','policy')`,
		`INSERT INTO bucket_versioning VALUES ('old','000000000000','Enabled')`,
		`INSERT INTO bucket_cors VALUES ('old','000000000000','cors')`,
		`INSERT INTO bucket_tags VALUES ('old','tag','value','000000000000')`,
		`INSERT INTO object_tags VALUES ('old','key','tag','value','000000000000')`,
		`INSERT INTO bucket_acls VALUES ('old','000000000000','acl')`,
		`INSERT INTO bucket_notifications VALUES ('old','000000000000','notification')`,
	} {
		_, err = old.DB().Exec(stmt)
		require.NoError(t, err)
	}
	before := snapshotDirectoryLegacy(t, old.DB())
	require.Len(t, before, 11)
	require.NoError(t, old.Close())
	for i := 0; i < 2; i++ {
		current, err := NewMetadataStore(path)
		require.NoError(t, err)
		require.Equal(t, before, snapshotDirectoryLegacy(t, current.store.DB()))
		var n int
		require.NoError(t, current.store.DB().QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('directory_buckets','directory_sessions','rename_receipts')`).Scan(&n))
		require.Equal(t, 3, n)
		require.NoError(t, current.Close())
	}
}
