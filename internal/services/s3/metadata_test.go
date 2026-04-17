// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataStore_CreateAndListBuckets(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewMetadataStore(dbPath)
	require.NoError(t, err)
	defer store.Close()

	err = store.CreateBucket("test-bucket", "us-east-1", "000000000000")
	require.NoError(t, err)

	buckets, err := store.ListBuckets("000000000000")
	require.NoError(t, err)
	require.Len(t, buckets, 1)
	assert.Equal(t, "test-bucket", buckets[0].Name)
	assert.WithinDuration(t, time.Now(), buckets[0].CreatedAt, 2*time.Second)
}

func TestMetadataStore_CreateDuplicateBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewMetadataStore(dbPath)
	require.NoError(t, err)
	defer store.Close()

	err = store.CreateBucket("test-bucket", "us-east-1", "000000000000")
	require.NoError(t, err)

	err = store.CreateBucket("test-bucket", "us-east-1", "000000000000")
	assert.ErrorIs(t, err, ErrBucketAlreadyExists)
}

func TestMetadataStore_DeleteBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewMetadataStore(dbPath)
	require.NoError(t, err)
	defer store.Close()

	store.CreateBucket("test-bucket", "us-east-1", "000000000000")
	err = store.DeleteBucket("test-bucket", "000000000000")
	require.NoError(t, err)

	buckets, _ := store.ListBuckets("000000000000")
	assert.Len(t, buckets, 0)
}

func TestMetadataStore_PutAndGetObjectMeta(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	store, err := NewMetadataStore(dbPath)
	require.NoError(t, err)
	defer store.Close()

	store.CreateBucket("test-bucket", "us-east-1", "000000000000")

	meta := ObjectMeta{
		Bucket:      "test-bucket",
		Key:         "hello.txt",
		Size:        5,
		ContentType: "text/plain",
		ETag:        `"5d41402abc4b2a76b9719d911017c592"`,
		AccountID:   "000000000000",
	}
	err = store.PutObjectMeta(meta)
	require.NoError(t, err)

	got, err := store.GetObjectMeta("test-bucket", "hello.txt", "000000000000")
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", got.Key)
	assert.Equal(t, int64(5), got.Size)
	assert.Equal(t, "text/plain", got.ContentType)
}
