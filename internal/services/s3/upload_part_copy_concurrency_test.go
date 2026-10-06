// SPDX-License-Identifier: Apache-2.0
package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
)

// Hold a real SQLite writer while copy replaces its file, then fails insertion.
// Completion must consume the previous committed part, never the rejected copy.
func TestUploadPartCopyFailureCannotLeakIntoConcurrentCompletion(t *testing.T) {
	p, id, _ := partCopySetup(t)
	target := "/destination/result?uploadId=" + id + "&partNumber=1"
	old := partCopyCall(t, p, "PUT", target, []byte("keep"), nil)
	require.Equal(t, 200, old.StatusCode)
	_, err := p.metaStore.store.DB().Exec(`CREATE TRIGGER reject_copy BEFORE INSERT ON upload_parts BEGIN SELECT RAISE(ABORT,'test write failure'); END;`)
	require.NoError(t, err)
	tx, err := p.metaStore.store.DB().Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`UPDATE upload_parts SET size=size WHERE upload_id=?`, id)
	require.NoError(t, err)
	copyDone := make(chan error, 1)
	go func() {
		r := httptest.NewRequest("PUT", target, nil)
		r.Header.Set("X-Amz-Copy-Source", "/source/original")
		r.Header.Set("X-Amz-Copy-Source-Range", "bytes=2-5")
		_, err := p.HandleRequest(context.Background(), "", r)
		copyDone <- err
	}()
	path, err := p.partPath(id, 1)
	require.NoError(t, err)
	require.Eventually(t, func() bool { data, _ := os.ReadFile(path); return bytes.Equal(data, []byte("2345")) }, time.Second, 5*time.Millisecond)
	type outcome struct {
		response *plugin.Response
		err      error
	}
	completeDone := make(chan outcome, 1)
	go func() {
		body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, old.Headers["ETag"])
		r := httptest.NewRequest("POST", "/destination/result?uploadId="+id, bytes.NewBufferString(body))
		response, err := p.HandleRequest(context.Background(), "", r)
		completeDone <- outcome{response, err}
	}()
	// Allow the pre-fix completion to reach its filesystem write while SQLite
	// remains locked; after the fix it waits for the part transaction instead.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if data, _ := p.fileStore.GetObject(defaultAccountID, "destination", "result"); len(data) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, tx.Rollback())
	select {
	case err := <-copyDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("copy did not finish")
	}
	select {
	case result := <-completeDone:
		require.NoError(t, result.err)
		require.Equal(t, 200, result.response.StatusCode, string(result.response.Body))
	case <-time.After(5 * time.Second):
		t.Fatal("completion did not finish")
	}
	require.Equal(t, []byte("keep"), partCopyCall(t, p, "GET", "/destination/result", nil, nil).Body)
}

func TestUploadPartCopyConditionUsesCommittedSourceSnapshot(t *testing.T) {
	p, id, original := partCopySetup(t)
	meta, err := p.metaStore.GetObjectMeta("source", "original", defaultAccountID)
	require.NoError(t, err)
	target := "/destination/result?uploadId=" + id + "&partNumber=1"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", target, []byte("keep"), nil).StatusCode)
	tx, err := p.metaStore.store.DB().Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`UPDATE objects SET size=size WHERE bucket='source' AND key='original'`)
	require.NoError(t, err)
	replacement := append([]byte("abcdefghij"), original[10:]...)
	type outcome struct {
		response *plugin.Response
		err      error
	}
	putDone := make(chan outcome, 1)
	go func() {
		r := httptest.NewRequest("PUT", "/source/original", bytes.NewReader(replacement))
		response, err := p.HandleRequest(context.Background(), "", r)
		putDone <- outcome{response, err}
	}()
	require.Eventually(t, func() bool {
		data, _ := p.fileStore.GetObject(defaultAccountID, "source", "original")
		return bytes.Equal(data, replacement)
	}, time.Second, 5*time.Millisecond)
	copyDone := make(chan outcome, 1)
	go func() {
		r := httptest.NewRequest("PUT", target, nil)
		r.Header.Set("X-Amz-Copy-Source", "/source/original")
		r.Header.Set("X-Amz-Copy-Source-Range", "bytes=2-5")
		r.Header.Set("X-Amz-Copy-Source-If-Match", meta.ETag)
		response, err := p.HandleRequest(context.Background(), "", r)
		copyDone <- outcome{response, err}
	}()
	path, err := p.partPath(id, 1)
	require.NoError(t, err)
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if data, _ := os.ReadFile(path); bytes.Equal(data, []byte("cdef")) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, tx.Rollback())
	select {
	case result := <-putDone:
		require.NoError(t, result.err)
		require.Equal(t, 200, result.response.StatusCode)
	case <-time.After(5 * time.Second):
		t.Fatal("source put did not finish")
	}
	select {
	case result := <-copyDone:
		require.NoError(t, result.err)
		require.Equal(t, 412, result.response.StatusCode, string(result.response.Body))
	case <-time.After(5 * time.Second):
		t.Fatal("copy did not finish")
	}
	partData, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), partData)
}

func TestUploadPartCopyRejectsExplicitMalformedRequestsWithoutMutation(t *testing.T) {
	p, id, _ := partCopySetup(t)
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/destination/result", []byte("existing object"), nil).StatusCode)
	target := "/destination/result?uploadId=" + id + "&partNumber=1"
	require.Equal(t, 200, partCopyCall(t, p, "PUT", target, []byte("keep"), nil).StatusCode)
	for _, tc := range []struct {
		target  string
		headers map[string]string
	}{
		{"/destination?x-id=UploadPartCopy", map[string]string{"X-Amz-Copy-Source": "/source/original"}},
		{"/destination/result?x-id=UploadPartCopy", map[string]string{"X-Amz-Copy-Source": "/source/original"}},
		{target + "&x-id=UploadPartCopy", nil},
	} {
		r := partCopyCall(t, p, "PUT", tc.target, []byte("reject body"), tc.headers)
		require.Equal(t, 400, r.StatusCode, string(r.Body))
		require.Equal(t, []byte("existing object"), partCopyCall(t, p, "GET", "/destination/result", nil, nil).Body)
		path, err := p.partPath(id, 1)
		require.NoError(t, err)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, []byte("keep"), data)
	}
}

func TestUploadPartCopyCannotCreateOrphanPartAfterAbort(t *testing.T) {
	p, id, _ := partCopySetup(t)
	require.Equal(t, 204, partCopyCall(t, p, "DELETE", "/destination/result?uploadId="+id, nil, nil).StatusCode)
	dir, err := p.multipartDir(id)
	require.NoError(t, err)
	// A stale part writer must still check the upload, even if a directory is
	// present (for example from a concurrent write begun before deletion).
	require.NoError(t, os.MkdirAll(dir, 0o755))
	_, err = p.storeMultipartPart(id, 1, []byte("orphan"))
	require.ErrorIs(t, err, ErrUploadNotFound)
	parts, err := p.metaStore.ListUploadParts(id)
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestUploadPartCopyConditionAfterRejectedSourceOverwrite(t *testing.T) {
	p, id, original := partCopySetup(t)
	meta, err := p.metaStore.GetObjectMeta("source", "original", defaultAccountID)
	require.NoError(t, err)
	_, err = p.metaStore.store.DB().Exec(`CREATE TRIGGER reject_object BEFORE INSERT ON objects BEGIN SELECT RAISE(ABORT,'test object failure'); END;`)
	require.NoError(t, err)
	r := httptest.NewRequest("PUT", "/source/original", bytes.NewReader(append([]byte("abcdefghij"), original[10:]...)))
	_, err = p.HandleRequest(context.Background(), "", r)
	require.Error(t, err)
	headers := map[string]string{"X-Amz-Copy-Source": "/source/original", "X-Amz-Copy-Source-Range": "bytes=2-5", "X-Amz-Copy-Source-If-Match": meta.ETag}
	require.Equal(t, 200, partCopyCall(t, p, "PUT", "/destination/result?uploadId="+id+"&partNumber=1", nil, headers).StatusCode)
	path, err := p.partPath(id, 1)
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("2345"), data)
}
