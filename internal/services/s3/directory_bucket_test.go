// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"context"
	"fmt"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const directoryTestBucket = "demo--use1-az1--x-s3"

func directoryConfig(zone, locationType string) []byte {
	return []byte(fmt.Sprintf(`<CreateBucketConfiguration><Location><Type>%s</Type><Name>%s</Name></Location><Bucket><Type>Directory</Type><DataRedundancy>SingleAvailabilityZone</DataRedundancy></Bucket></CreateBucketConfiguration>`, locationType, zone))
}
func directorySetup(t *testing.T) *S3Provider {
	t.Helper()
	p := newTestProvider(t)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+directoryTestBucket, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	return p
}
func directoryIdentity(t *testing.T, p *S3Provider, bucket string) (string, string) {
	t.Helper()
	var zone, id string
	require.NoError(t, p.metaStore.store.DB().QueryRow(`SELECT zone,incarnation FROM directory_buckets WHERE bucket=? AND account_id=?`, bucket, defaultAccountID).Scan(&zone, &id))
	return zone, id
}

func TestDirectoryBucketCreateAndReopen(t *testing.T) {
	p := directorySetup(t)
	zone, id := directoryIdentity(t, p, directoryTestBucket)
	require.Equal(t, "use1-az1", zone)
	require.NotEmpty(t, id)
	root := p.fileStore.baseDir
	require.NoError(t, p.Shutdown(context.Background()))
	reopened := &S3Provider{}
	require.NoError(t, reopened.Init(plugin.PluginConfig{DataDir: root}))
	t.Cleanup(func() { _ = reopened.Shutdown(context.Background()) })
	_, after := directoryIdentity(t, reopened, directoryTestBucket)
	require.Equal(t, id, after)
	legacy := "legacy--use1-az1--x-s3"
	require.Equal(t, 200, partCopyCall(t, reopened, "PUT", "/"+legacy, nil, nil).StatusCode)
	var n int
	require.NoError(t, reopened.metaStore.store.DB().QueryRow(`SELECT count(*) FROM directory_buckets WHERE bucket=?`, legacy).Scan(&n))
	require.Zero(t, n)
	require.Equal(t, 409, partCopyCall(t, reopened, "PUT", "/"+legacy, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	require.Equal(t, 204, partCopyCall(t, reopened, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.Equal(t, 200, partCopyCall(t, reopened, "PUT", "/"+directoryTestBucket, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	_, fresh := directoryIdentity(t, reopened, directoryTestBucket)
	require.NotEqual(t, id, fresh)
}

func TestDirectoryBucketRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name, bucket string
		body         []byte
		status       int
		code         string
	}{
		{"xml", directoryTestBucket, []byte("<bad"), 400, "MalformedXML"},
		{"zone", directoryTestBucket, directoryConfig("use1-az2", "AvailabilityZone"), 400, "InvalidRequest"},
		{"name", "INVALID--use1-az1--x-s3", directoryConfig("use1-az1", "AvailabilityZone"), 400, "InvalidRequest"},
		{"missingzone", directoryTestBucket, directoryConfig("", "AvailabilityZone"), 400, "InvalidRequest"},
		{"type", directoryTestBucket, []byte(strings.ReplaceAll(string(directoryConfig("use1-az1", "AvailabilityZone")), ">Directory<", ">Other<")), 400, "InvalidRequest"},
		{"localzone", directoryTestBucket, directoryConfig("use1-az1", "LocalZone"), 501, "NotImplemented"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestProvider(t)
			defer func() { _ = p.Shutdown(context.Background()) }()
			r := partCopyCall(t, p, "PUT", "/"+tc.bucket, tc.body, nil)
			require.Equal(t, tc.status, r.StatusCode)
			require.Contains(t, string(r.Body), tc.code)
			bs, err := p.metaStore.ListBuckets(defaultAccountID)
			require.NoError(t, err)
			require.Empty(t, bs)
		})
	}
	p := directorySetup(t)
	require.Equal(t, 409, partCopyCall(t, p, "PUT", "/"+directoryTestBucket, directoryConfig("use1-az1", "AvailabilityZone"), nil).StatusCode)
	require.Equal(t, 409, partCopyCall(t, p, "PUT", "/"+directoryTestBucket, nil, nil).StatusCode)
}

func TestDirectoryBucketDeletePreservesNonEmptyAndRollsBack(t *testing.T) {
	p := directorySetup(t)
	credentials := directoryIssue(t, p, directoryTestBucket, "ReadWrite")
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/"+directoryTestBucket+"/key", []byte("keep"), directoryAuth(credentials)).StatusCode)
	require.Equal(t, 409, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.Equal(t, []byte("keep"), partCopyCall(t, p, "GET", "/"+directoryTestBucket+"/key", nil, directoryAuth(credentials)).Body)
	require.Equal(t, 204, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket+"/key", nil, directoryAuth(credentials)).StatusCode)
	require.NoError(t, p.metaStore.CreateMultipartUpload("upload", directoryTestBucket, "key", defaultAccountID))
	require.Equal(t, 409, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	require.NoError(t, p.metaStore.DeleteMultipartUpload("upload"))
	_, id := directoryIdentity(t, p, directoryTestBucket)
	_, err := p.metaStore.store.DB().Exec(`CREATE TRIGGER reject_directory_delete BEFORE DELETE ON buckets BEGIN SELECT RAISE(ABORT,'reject'); END`)
	require.NoError(t, err)
	response, err := p.HandleRequest(context.Background(), "", httptest.NewRequest("DELETE", "/"+directoryTestBucket, nil))
	require.Error(t, err)
	require.Nil(t, response)
	_, after := directoryIdentity(t, p, directoryTestBucket)
	require.Equal(t, id, after)
	dir, err := p.fileStore.bucketDir(defaultAccountID, directoryTestBucket)
	require.NoError(t, err)
	stat, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	_, err = p.metaStore.store.DB().Exec(`DROP TRIGGER reject_directory_delete`)
	require.NoError(t, err)
	// A file without a metadata row still makes the filesystem nonempty.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "orphan"), []byte("preserve"), 0600))
	require.Equal(t, 409, partCopyCall(t, p, "DELETE", "/"+directoryTestBucket, nil, nil).StatusCode)
	actual, err := os.ReadFile(filepath.Join(dir, "orphan"))
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), actual)
}
