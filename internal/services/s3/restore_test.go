// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestRestoreObject(t *testing.T) {
	tmpDir := t.TempDir()
	p := &S3Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. Create a bucket
	reqCreate := httptest.NewRequest(http.MethodPut, "/my-bucket", nil)
	resp, err := p.HandleRequest(ctx, "CreateBucket", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Put an object
	reqPut := httptest.NewRequest(http.MethodPut, "/my-bucket/archive.txt", bytes.NewReader([]byte("archived content")))
	resp, err = p.HandleRequest(ctx, "PutObject", reqPut)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 3. RestoreObject
	restoreXML := `<?xml version="1.0" encoding="UTF-8"?><RestoreRequest><Days>5</Days><Tier>Standard</Tier></RestoreRequest>`
	reqRestore := httptest.NewRequest(http.MethodPost, "/my-bucket/archive.txt?restore", bytes.NewReader([]byte(restoreXML)))
	resp, err = p.HandleRequest(ctx, "RestoreObject", reqRestore)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. HeadObject should have x-amz-restore header
	reqHead := httptest.NewRequest(http.MethodHead, "/my-bucket/archive.txt", nil)
	resp, err = p.HandleRequest(ctx, "HeadObject", reqHead)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Headers["x-amz-restore"], `ongoing-request="false"`)
	assert.Contains(t, resp.Headers["x-amz-restore"], `expiry-date=`)

	// 5. GetObject should also have x-amz-restore header
	reqGet := httptest.NewRequest(http.MethodGet, "/my-bucket/archive.txt", nil)
	resp, err = p.HandleRequest(ctx, "GetObject", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Headers["x-amz-restore"], `ongoing-request="false"`)
	assert.Equal(t, []byte("archived content"), resp.Body)

	// 6. Restore on nonexistent object returns 404
	reqRestoreMissing := httptest.NewRequest(http.MethodPost, "/my-bucket/missing.txt?restore", bytes.NewReader([]byte(restoreXML)))
	resp, err = p.HandleRequest(ctx, "RestoreObject", reqRestoreMissing)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
