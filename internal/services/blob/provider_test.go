// SPDX-License-Identifier: Apache-2.0

package blob

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir, err := os.MkdirTemp("", "devcloud-blob-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	err = p.Init(plugin.PluginConfig{DataDir: filepath.Join(dir, "blob")})
	require.NoError(t, err)
	return p
}

func TestBlobProvider_Metadata(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "blob", p.ServiceID())
	assert.Equal(t, "Azure Blob Storage", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTXML, p.Protocol())
	assert.Equal(t, "azure", plugin.ProviderOf(p))
}

func TestBlobProvider_Lifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// 1. Create Container
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/mycontainer?restype=container", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.NotEmpty(t, resp.Headers["ETag"])
	assert.Equal(t, "2023-11-03", resp.Headers["x-ms-version"])

	// Duplicate create should return 409
	respDup, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respDup.StatusCode)
	assert.Equal(t, "ContainerAlreadyExists", respDup.Headers["x-ms-error-code"])

	// 2. Get Container Properties
	reqGetProps := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/mycontainer?restype=container", nil)
	respProps, err := p.HandleRequest(ctx, "", reqGetProps)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respProps.StatusCode)
	assert.Equal(t, "available", respProps.Headers["x-ms-lease-state"])

	// 3. List Containers
	reqListCont := httptest.NewRequest(http.MethodGet, "/devstoreaccount1?comp=list", nil)
	respListCont, err := p.HandleRequest(ctx, "", reqListCont)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respListCont.StatusCode)
	assert.Contains(t, string(respListCont.Body), "<Name>mycontainer</Name>")

	// 4. Put Blob
	payload := []byte("Hello, Azure Blob Storage from DevCloud!")
	reqPutBlob := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/mycontainer/hello.txt", bytes.NewReader(payload))
	reqPutBlob.Header.Set("Content-Type", "text/plain")
	respPutBlob, err := p.HandleRequest(ctx, "", reqPutBlob)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respPutBlob.StatusCode)
	assert.NotEmpty(t, respPutBlob.Headers["ETag"])
	assert.NotEmpty(t, respPutBlob.Headers["Content-MD5"])

	// 5. Get Blob
	reqGetBlob := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/mycontainer/hello.txt", nil)
	respGetBlob, err := p.HandleRequest(ctx, "", reqGetBlob)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGetBlob.StatusCode)
	assert.Equal(t, "text/plain", respGetBlob.Headers["Content-Type"])
	assert.Equal(t, payload, respGetBlob.Body)

	// 6. Get Blob Properties (HEAD)
	reqHeadBlob := httptest.NewRequest(http.MethodHead, "/devstoreaccount1/mycontainer/hello.txt", nil)
	respHeadBlob, err := p.HandleRequest(ctx, "", reqHeadBlob)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respHeadBlob.StatusCode)
	assert.Equal(t, "BlockBlob", respHeadBlob.Headers["x-ms-blob-type"])
	assert.Empty(t, respHeadBlob.Body)

	// 7. List Blobs
	reqListBlobs := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/mycontainer?restype=container&comp=list", nil)
	respListBlobs, err := p.HandleRequest(ctx, "", reqListBlobs)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respListBlobs.StatusCode)
	assert.Contains(t, string(respListBlobs.Body), "<Name>hello.txt</Name>")

	// 8. Delete Blob
	reqDelBlob := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/mycontainer/hello.txt", nil)
	respDelBlob, err := p.HandleRequest(ctx, "", reqDelBlob)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, respDelBlob.StatusCode)

	// Get deleted blob should return 404
	respGetDel, err := p.HandleRequest(ctx, "", reqGetBlob)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGetDel.StatusCode)
	assert.Equal(t, "BlobNotFound", respGetDel.Headers["x-ms-error-code"])

	// 9. Delete Container
	reqDelCont := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/mycontainer?restype=container", nil)
	respDelCont, err := p.HandleRequest(ctx, "", reqDelCont)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, respDelCont.StatusCode)

	// Get deleted container should return 404
	respGetDelCont, err := p.HandleRequest(ctx, "", reqGetProps)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGetDelCont.StatusCode)
	assert.Equal(t, "ContainerNotFound", respGetDelCont.Headers["x-ms-error-code"])
}

func TestBlobProvider_ListResources(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	_, err := p.store.CreateContainer("devstoreaccount1", "res-container-1")
	require.NoError(t, err)
	_, err = p.store.CreateContainer("devstoreaccount1", "res-container-2")
	require.NoError(t, err)

	resources, err := p.ListResources(ctx)
	require.NoError(t, err)
	assert.Len(t, resources, 2)
}

func TestFileStore_PathTraversal(t *testing.T) {
	store := NewFileStore(t.TempDir())

	// Account traversal rejected
	_, err := store.CreateContainer("../escape", "cont")
	assert.ErrorIs(t, err, ErrInvalidName)

	_, err = store.CreateContainer("account/sub", "cont")
	assert.ErrorIs(t, err, ErrInvalidName)

	_, err = store.ListContainers("../escape")
	assert.ErrorIs(t, err, ErrInvalidName)

	// Container traversal rejected
	for _, badContainer := range []string{"..", "../escape", "a/b", "a\\b", ".", "/abs"} {
		_, err := store.CreateContainer("devstoreaccount1", badContainer)
		assert.ErrorIs(t, err, ErrInvalidName, "container %q should be rejected", badContainer)

		err = store.DeleteContainer("devstoreaccount1", badContainer)
		assert.ErrorIs(t, err, ErrInvalidName, "container %q should be rejected", badContainer)

		_, err = store.GetContainer("devstoreaccount1", badContainer)
		assert.ErrorIs(t, err, ErrInvalidName, "container %q should be rejected", badContainer)
	}

	// Create valid container
	_, err = store.CreateContainer("devstoreaccount1", "validcont")
	require.NoError(t, err)

	// Blob traversal rejected
	for _, badBlob := range []string{"..", "../escape", "../../secret", "/etc/passwd", "a/../../escape", "a\\b"} {
		_, err := store.PutBlob("devstoreaccount1", "validcont", badBlob, "text/plain", bytes.NewReader([]byte("data")))
		assert.ErrorIs(t, err, ErrInvalidName, "blob %q should be rejected", badBlob)

		_, _, err = store.GetBlob("devstoreaccount1", "validcont", badBlob)
		assert.ErrorIs(t, err, ErrInvalidName, "blob %q should be rejected", badBlob)

		_, err = store.GetBlobProperties("devstoreaccount1", "validcont", badBlob)
		assert.ErrorIs(t, err, ErrInvalidName, "blob %q should be rejected", badBlob)

		err = store.DeleteBlob("devstoreaccount1", "validcont", badBlob)
		assert.ErrorIs(t, err, ErrInvalidName, "blob %q should be rejected", badBlob)
	}

	// Valid nested blob accepted
	bi, err := store.PutBlob("devstoreaccount1", "validcont", "a/b/c.txt", "text/plain", bytes.NewReader([]byte("ok")))
	require.NoError(t, err)
	assert.Equal(t, "a/b/c.txt", bi.Name)
}

func TestBlobProvider_VirtualDirectory(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Create container
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/vdir-container?restype=container", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Put blob with virtual directory
	payload := []byte("nested content")
	reqPut := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/vdir-container/folder/subfolder/file.txt", bytes.NewReader(payload))
	resp, err = p.HandleRequest(ctx, "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Get blob with virtual directory
	reqGet := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/vdir-container/folder/subfolder/file.txt", nil)
	resp, err = p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, payload, resp.Body)
}

func TestBlobProvider_ConditionalPutBlob(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Create container
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/cond-container?restype=container", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// 1. Put blob with If-None-Match: * (should succeed since blob does not exist)
	reqPut1 := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/cond-container/item.txt", bytes.NewReader([]byte("first")))
	reqPut1.Header.Set("If-None-Match", "*")
	respPut1, err := p.HandleRequest(ctx, "", reqPut1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respPut1.StatusCode)
	etag := respPut1.Headers["ETag"]
	assert.NotEmpty(t, etag)

	// 2. Put blob again with If-None-Match: * (should fail with 409 BlobAlreadyExists)
	reqPutDup := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/cond-container/item.txt", bytes.NewReader([]byte("duplicate")))
	reqPutDup.Header.Set("If-None-Match", "*")
	respPutDup, err := p.HandleRequest(ctx, "", reqPutDup)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respPutDup.StatusCode)
	assert.Equal(t, "BlobAlreadyExists", respPutDup.Headers["x-ms-error-code"])

	// 3. Put blob with valid If-Match (should succeed)
	reqPutMatch := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/cond-container/item.txt", bytes.NewReader([]byte("updated")))
	reqPutMatch.Header.Set("If-Match", etag)
	respPutMatch, err := p.HandleRequest(ctx, "", reqPutMatch)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respPutMatch.StatusCode)

	// 4. Put blob with invalid If-Match (should fail with 412 ConditionNotMet)
	reqPutBadMatch := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/cond-container/item.txt", bytes.NewReader([]byte("bad")))
	reqPutBadMatch.Header.Set("If-Match", "\"0xINVALID\"")
	respPutBadMatch, err := p.HandleRequest(ctx, "", reqPutBadMatch)
	require.NoError(t, err)
	assert.Equal(t, http.StatusPreconditionFailed, respPutBadMatch.StatusCode)
	assert.Equal(t, "ConditionNotMet", respPutBadMatch.Headers["x-ms-error-code"])
}

func TestBlobProvider_BlockStagingAndCommit(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Create container
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/block-container?restype=container", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Stage block 1
	reqStage1 := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/block-container/chunked.txt?comp=block&blockid=YmxvY2sx", bytes.NewReader([]byte("Hello ")))
	resp1, err := p.HandleRequest(ctx, "", reqStage1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp1.StatusCode)

	// Stage block 2
	reqStage2 := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/block-container/chunked.txt?comp=block&blockid=YmxvY2sy", bytes.NewReader([]byte("World!")))
	resp2, err := p.HandleRequest(ctx, "", reqStage2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp2.StatusCode)

	// Commit block list
	blockListXML := `<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>YmxvY2sx</Latest><Latest>YmxvY2sy</Latest></BlockList>`
	reqCommit := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/block-container/chunked.txt?comp=blocklist", bytes.NewReader([]byte(blockListXML)))
	respCommit, err := p.HandleRequest(ctx, "", reqCommit)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respCommit.StatusCode)

	// Download blob and verify concatenated content
	reqGet := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/block-container/chunked.txt", nil)
	respGet, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGet.StatusCode)
	assert.Equal(t, []byte("Hello World!"), respGet.Body)
}

func TestBlobProvider_CustomMetadata(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Create container
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/meta-container?restype=container", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Put blob with metadata
	reqPut := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/meta-container/doc.txt", bytes.NewReader([]byte("meta payload")))
	reqPut.Header.Set("x-ms-meta-author", "antigravity")
	reqPut.Header.Set("x-ms-meta-environment", "staging")
	respPut, err := p.HandleRequest(ctx, "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respPut.StatusCode)

	// Get blob properties and verify metadata
	reqHead := httptest.NewRequest(http.MethodHead, "/devstoreaccount1/meta-container/doc.txt", nil)
	respHead, err := p.HandleRequest(ctx, "", reqHead)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respHead.StatusCode)
	assert.Equal(t, "antigravity", respHead.Headers["x-ms-meta-author"])
	assert.Equal(t, "staging", respHead.Headers["x-ms-meta-environment"])

	// Update metadata via SetBlobMetadata
	reqSetMeta := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/meta-container/doc.txt?comp=metadata", nil)
	reqSetMeta.Header.Set("x-ms-meta-author", "updated-user")
	respSetMeta, err := p.HandleRequest(ctx, "", reqSetMeta)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respSetMeta.StatusCode)

	// Re-check properties
	respHead2, err := p.HandleRequest(ctx, "", reqHead)
	require.NoError(t, err)
	assert.Equal(t, "updated-user", respHead2.Headers["x-ms-meta-author"])
}
