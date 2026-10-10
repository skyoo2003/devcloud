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
