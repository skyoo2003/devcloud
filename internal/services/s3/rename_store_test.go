// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRenameTokenReplayMismatchAndReopen(t *testing.T) {
	p, c := renameSetup(t)
	request := renameHTTP(directoryTestBucket, "source", "destination", "token", c)
	require.Equal(t, 200, renameRun(t, p, request).StatusCode)
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/source", []byte("new source"), c).StatusCode)
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/destination", []byte("new destination"), c).StatusCode)
	require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, directoryTestBucket+"/source", "destination", "token", c)).StatusCode)
	require.Equal(t, []byte("new source"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).Body)
	require.Equal(t, []byte("new destination"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/destination", nil, c).Body)
	mismatch := renameHTTP(directoryTestBucket, "source", "other", "token", c)
	response := renameRun(t, p, mismatch)
	require.Equal(t, 400, response.StatusCode)
	require.Contains(t, string(response.Body), "IdempotencyParameterMismatch")
	changed := renameHTTP(directoryTestBucket, "source", "destination", "token", c)
	changed.Header.Set("If-None-Match", "*")
	require.Equal(t, 400, renameRun(t, p, changed).StatusCode)
	require.Equal(t, 204, directoryCall(t, p, "DELETE", "/"+directoryTestBucket+"/destination", nil, c).StatusCode)
	root := p.fileStore.baseDir
	require.NoError(t, p.Shutdown(context.Background()))
	reopen := &S3Provider{}
	require.NoError(t, reopen.Init(plugin.PluginConfig{DataDir: root}))
	defer func() { _ = reopen.Shutdown(context.Background()) }()
	fresh := directoryIssue(t, reopen, directoryTestBucket, "")
	require.Equal(t, 200, renameRun(t, reopen, renameHTTP(directoryTestBucket, "/source", "destination", "token", fresh)).StatusCode)
	require.Equal(t, 404, directoryCall(t, reopen, "GET", "/"+directoryTestBucket+"/destination", nil, fresh).StatusCode)
	require.Equal(t, []byte("new source"), directoryCall(t, reopen, "GET", "/"+directoryTestBucket+"/source", nil, fresh).Body)
}

func TestRenameFailureDoesNotConsumeToken(t *testing.T) {
	for _, table := range []string{"objects", "object_tags", "rename_receipts"} {
		t.Run(table, func(t *testing.T) {
			p, c := renameSetup(t)
			before := snapshotDirectoryLegacy(t, p.metaStore.store.DB())
			_, err := p.metaStore.store.DB().Exec(fmt.Sprintf(`CREATE TRIGGER reject_rename BEFORE INSERT ON %s BEGIN SELECT RAISE(ABORT,'test failure'); END`, table))
			require.NoError(t, err)
			response, err := p.HandleRequest(context.Background(), "", renameHTTP(directoryTestBucket, "source", "destination", "retry", c))
			require.Error(t, err)
			require.Nil(t, response)
			require.Equal(t, before, snapshotDirectoryLegacy(t, p.metaStore.store.DB()))
			require.Zero(t, renameReceiptCount(t, p))
			require.Equal(t, []byte("original bytes"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/source", nil, c).Body)
			require.Equal(t, []byte("previous destination"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/destination", nil, c).Body)
			_, err = p.metaStore.store.DB().Exec(`DROP TRIGGER reject_rename`)
			require.NoError(t, err)
			require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, "source", "destination", "retry", c)).StatusCode)
		})
	}
}

func TestRenameReplacesOrphanDestinationTags(t *testing.T) {
	p, c := renameSetup(t)
	require.Equal(t, 204, directoryCall(t, p, "DELETE", "/"+directoryTestBucket+"/destination", nil, c).StatusCode)
	require.NoError(t, p.metaStore.PutObjectTags(directoryTestBucket, "destination", defaultAccountID, map[string]string{"orphan": "remove"}))
	require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, "source", "destination", "", c)).StatusCode)
	tags, err := p.metaStore.GetObjectTags(directoryTestBucket, "destination", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"source": "keep"}, tags)
}
