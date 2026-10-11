// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir, err := os.MkdirTemp("", "devcloud-gcs-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	err = p.Init(plugin.PluginConfig{DataDir: filepath.Join(dir, "gcs")})
	require.NoError(t, err)
	return p
}

func TestGCSProvider_Metadata(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "gcs", p.ServiceID())
	assert.Equal(t, "Google Cloud Storage", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())
	assert.Equal(t, "gcp", plugin.ProviderOf(p))
}

func TestGCSProvider_BucketLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// 1. Create Bucket
	createBody := `{"name": "my-bucket", "location": "US", "storageClass": "STANDARD"}`
	reqCreate := httptest.NewRequest(http.MethodPost, "/storage/v1/b?project=test-proj", stringsReader(createBody))
	reqCreate.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var b BucketInfo
	err = json.Unmarshal(resp.Body, &b)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", b.Name)
	assert.Equal(t, "US", b.Location)

	// 2. Duplicate Create returns 409 Conflict
	reqDup := httptest.NewRequest(http.MethodPost, "/storage/v1/b?project=test-proj", stringsReader(createBody))
	reqDup.Header.Set("Content-Type", "application/json")
	respDup, err := p.HandleRequest(ctx, "", reqDup)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respDup.StatusCode)

	// 3. Get Bucket
	reqGet := httptest.NewRequest(http.MethodGet, "/storage/v1/b/my-bucket?project=test-proj", nil)
	respGet, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGet.StatusCode)

	// 4. List Buckets
	reqList := httptest.NewRequest(http.MethodGet, "/storage/v1/b?project=test-proj", nil)
	respList, err := p.HandleRequest(ctx, "", reqList)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respList.StatusCode)
	assert.Contains(t, string(respList.Body), "my-bucket")

	// 5. Delete Bucket
	reqDel := httptest.NewRequest(http.MethodDelete, "/storage/v1/b/my-bucket?project=test-proj", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 6. Get Deleted Bucket returns 404
	respGet404, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGet404.StatusCode)
}

func TestGCSProvider_ObjectLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Setup bucket
	createBody := `{"name": "test-bucket"}`
	reqCreateB := httptest.NewRequest(http.MethodPost, "/storage/v1/b?project=test-proj", stringsReader(createBody))
	_, err := p.HandleRequest(ctx, "", reqCreateB)
	require.NoError(t, err)

	// 1. Upload Object (Media upload)
	payload := []byte("Hello Google Cloud Storage from DevCloud!")
	reqUpload := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/test-bucket/o?uploadType=media&name=notes/sample.txt&project=test-proj", bytes.NewReader(payload))
	reqUpload.Header.Set("Content-Type", "text/plain")
	respUpload, err := p.HandleRequest(ctx, "", reqUpload)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respUpload.StatusCode)

	var obj ObjectInfo
	err = json.Unmarshal(respUpload.Body, &obj)
	require.NoError(t, err)
	assert.Equal(t, "notes/sample.txt", obj.Name)
	assert.NotEmpty(t, obj.MD5Hash)
	assert.NotEmpty(t, obj.CRC32c)

	// 2. Get Object Metadata
	reqMeta := httptest.NewRequest(http.MethodGet, "/storage/v1/b/test-bucket/o/notes/sample.txt?project=test-proj", nil)
	respMeta, err := p.HandleRequest(ctx, "", reqMeta)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respMeta.StatusCode)
	assert.Contains(t, string(respMeta.Body), "text/plain")

	// 3. Download Object Media (alt=media)
	reqDown := httptest.NewRequest(http.MethodGet, "/storage/v1/b/test-bucket/o/notes/sample.txt?alt=media&project=test-proj", nil)
	respDown, err := p.HandleRequest(ctx, "", reqDown)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respDown.StatusCode)
	assert.Equal(t, payload, respDown.Body)
	assert.NotEmpty(t, respDown.Headers["x-goog-hash"])

	// 4. List Objects with prefix
	reqList := httptest.NewRequest(http.MethodGet, "/storage/v1/b/test-bucket/o?prefix=notes/&project=test-proj", nil)
	respList, err := p.HandleRequest(ctx, "", reqList)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respList.StatusCode)
	assert.Contains(t, string(respList.Body), "notes/sample.txt")

	// 5. Delete Object
	reqDel := httptest.NewRequest(http.MethodDelete, "/storage/v1/b/test-bucket/o/notes/sample.txt?project=test-proj", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 6. Download Deleted Object returns 404
	respDown404, err := p.HandleRequest(ctx, "", reqDown)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respDown404.StatusCode)
}

func TestGCSProvider_MultipartUpload(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Setup bucket
	createBody := `{"name": "mp-bucket"}`
	reqCreateB := httptest.NewRequest(http.MethodPost, "/storage/v1/b?project=test-proj", stringsReader(createBody))
	_, err := p.HandleRequest(ctx, "", reqCreateB)
	require.NoError(t, err)

	// Build multipart body
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Part 1: JSON metadata
	hJSON := make(textproto.MIMEHeader)
	hJSON.Set("Content-Type", "application/json; charset=UTF-8")
	part1, err := writer.CreatePart(hJSON)
	require.NoError(t, err)
	_, _ = part1.Write([]byte(`{"name": "data/large.bin", "contentType": "application/octet-stream"}`))

	// Part 2: Payload
	hMedia := make(textproto.MIMEHeader)
	hMedia.Set("Content-Type", "application/octet-stream")
	part2, err := writer.CreatePart(hMedia)
	require.NoError(t, err)
	payload := []byte("Multipart upload bytes in DevCloud GCS")
	_, _ = part2.Write(payload)
	_ = writer.Close()

	reqUpload := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/mp-bucket/o?uploadType=multipart&project=test-proj", &body)
	reqUpload.Header.Set("Content-Type", fmt.Sprintf("multipart/related; boundary=%s", writer.Boundary()))

	respUpload, err := p.HandleRequest(ctx, "", reqUpload)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respUpload.StatusCode)

	var obj ObjectInfo
	err = json.Unmarshal(respUpload.Body, &obj)
	require.NoError(t, err)
	assert.Equal(t, "data/large.bin", obj.Name)
	assert.Equal(t, int64(len(payload)), obj.Size)

	// Verify download
	reqDown := httptest.NewRequest(http.MethodGet, "/storage/v1/b/mp-bucket/o/data/large.bin?alt=media&project=test-proj", nil)
	respDown, err := p.HandleRequest(ctx, "", reqDown)
	require.NoError(t, err)
	assert.Equal(t, payload, respDown.Body)
}

func stringsReader(s string) *bytes.Reader {
	return bytes.NewReader([]byte(s))
}
