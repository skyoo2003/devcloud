// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"encoding/xml"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameParentChildPathsPreserveObjects(t *testing.T) {
	p, c := renameSetup(t)
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/a", []byte("keep"), c).StatusCode)
	_, err := p.HandleRequest(context.Background(), "", renameHTTP(directoryTestBucket, "a", "a/child", "", c))
	require.Error(t, err)
	require.Equal(t, []byte("keep"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/a", nil, c).Body)
	require.Equal(t, 200, directoryCall(t, p, "PUT", "/"+directoryTestBucket+"/folder/child", []byte("nested"), c).StatusCode)
	r := renameRun(t, p, renameHTTP(directoryTestBucket, "folder/child", "folder", "", c))
	require.Equal(t, 400, r.StatusCode)
	require.Equal(t, []byte("nested"), directoryCall(t, p, "GET", "/"+directoryTestBucket+"/folder/child", nil, c).Body)
}

func TestRenamePathsAndSparseSource(t *testing.T) {
	p, c := renameSetup(t)
	dir, err := p.fileStore.bucketDir(defaultAccountID, directoryTestBucket)
	require.NoError(t, err)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "victim"), []byte("preserve"), 0600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "link")))
	r := renameRun(t, p, renameHTTP(directoryTestBucket, "source", "link/victim", "", c))
	require.Equal(t, 400, r.StatusCode)
	victim, err := os.ReadFile(filepath.Join(outside, "victim"))
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), victim)
	path, err := p.fileStore.objectPath(defaultAccountID, directoryTestBucket, "source")
	require.NoError(t, err)
	size := int64(5*1024*1024*1024 + 1)
	require.NoError(t, os.Truncate(path, size))
	meta, err := p.metaStore.GetObjectMeta(directoryTestBucket, "source", defaultAccountID)
	require.NoError(t, err)
	meta.Size = size
	require.NoError(t, p.metaStore.PutObjectMeta(*meta))
	require.Equal(t, 200, renameRun(t, p, renameHTTP(directoryTestBucket, "source", "sparse", "sparse", c)).StatusCode)
	stat, err := os.Stat(filepath.Join(dir, "sparse"))
	require.NoError(t, err)
	require.Equal(t, size, stat.Size())
	after, err := p.metaStore.GetObjectMeta(directoryTestBucket, "sparse", defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, meta.ETag, after.ETag)
	require.Equal(t, meta.LastModified, after.LastModified)
	var list struct {
		Contents []struct{ Key string } `xml:"Contents"`
	}
	require.NoError(t, xml.Unmarshal(directoryCall(t, p, "GET", "/"+directoryTestBucket+"?list-type=2", nil, c).Body, &list))
	require.NotEmpty(t, list.Contents)
}
