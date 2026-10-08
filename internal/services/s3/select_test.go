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

func TestSelectObjectContentCSV(t *testing.T) {
	tmpDir := t.TempDir()
	p := &S3Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. Create bucket & put CSV object
	reqCreate := httptest.NewRequest(http.MethodPut, "/csv-bucket", nil)
	resp, err := p.HandleRequest(ctx, "CreateBucket", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	csvData := "id,name,role\n1,alice,admin\n2,bob,user\n3,charlie,user\n"
	reqPut := httptest.NewRequest(http.MethodPut, "/csv-bucket/users.csv", bytes.NewReader([]byte(csvData)))
	resp, err = p.HandleRequest(ctx, "PutObject", reqPut)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Query CSV with filter WHERE role = 'admin'
	selectXML := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM S3Object s WHERE s.role = 'admin'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
</SelectObjectContentRequest>`

	reqSelect := httptest.NewRequest(http.MethodPost, "/csv-bucket/users.csv?select&select-type=2", bytes.NewReader([]byte(selectXML)))
	resp, err = p.HandleRequest(ctx, "SelectObjectContent", reqSelect)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.ContentType)

	// Verify EventStream framing contains Alice
	assert.Contains(t, string(resp.Body), "alice")
	assert.NotContains(t, string(resp.Body), "bob")
}

func TestWriteGetObjectResponse(t *testing.T) {
	tmpDir := t.TempDir()
	p := &S3Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	req := httptest.NewRequest(http.MethodPost, "/WriteGetObjectResponse", bytes.NewReader([]byte("transformed content")))
	req.Header.Set("x-amz-request-route", "test-route")
	req.Header.Set("x-amz-request-token", "test-token")

	resp, err := p.HandleRequest(ctx, "WriteGetObjectResponse", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
