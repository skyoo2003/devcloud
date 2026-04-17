// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStore_PutAndGetObject(t *testing.T) {
	store := NewFileStore(t.TempDir())
	err := store.PutObject("000000000000", "test-bucket", "hello.txt", []byte("hello world"))
	require.NoError(t, err)
	data, err := store.GetObject("000000000000", "test-bucket", "hello.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello world"), data)
}

func TestFileStore_DeleteObject(t *testing.T) {
	store := NewFileStore(t.TempDir())
	store.PutObject("000000000000", "test-bucket", "hello.txt", []byte("hello"))
	err := store.DeleteObject("000000000000", "test-bucket", "hello.txt")
	require.NoError(t, err)
	_, err = store.GetObject("000000000000", "test-bucket", "hello.txt")
	assert.Error(t, err)
}

func TestFileStore_NestedKeys(t *testing.T) {
	store := NewFileStore(t.TempDir())
	err := store.PutObject("000000000000", "bucket", "a/b/c/file.txt", []byte("nested"))
	require.NoError(t, err)
	data, err := store.GetObject("000000000000", "bucket", "a/b/c/file.txt")
	require.NoError(t, err)
	assert.Equal(t, []byte("nested"), data)
}

func TestFileStore_CreateAndDeleteBucketDir(t *testing.T) {
	store := NewFileStore(t.TempDir())
	err := store.CreateBucketDir("000000000000", "test-bucket")
	require.NoError(t, err)
	err = store.DeleteBucketDir("000000000000", "test-bucket")
	require.NoError(t, err)
}

func TestFileStore_PathTraversal(t *testing.T) {
	store := NewFileStore(t.TempDir())

	// Key with enough ../ to escape baseDir (key is 3 levels deep: account/bucket/key)
	err := store.PutObject("000000000000", "bucket", "../../../../etc/passwd", []byte("evil"))
	assert.Error(t, err, "path traversal via key should be rejected")

	// Bucket name with traversal escaping baseDir
	err = store.PutObject("000000000000", "../../escape", "file.txt", []byte("evil"))
	assert.Error(t, err, "path traversal via bucket should be rejected")

	// Account ID with traversal escaping baseDir
	err = store.PutObject("../../root", "bucket", "file.txt", []byte("evil"))
	assert.Error(t, err, "path traversal via accountID should be rejected")

	// Ensure legitimate nested keys still work
	err = store.PutObject("000000000000", "bucket", "a/b/../../c/file.txt", []byte("ok"))
	require.NoError(t, err, "relative path within bounds should be allowed")
}
