// SPDX-License-Identifier: Apache-2.0

// internal/services/s3tables/provider_test.go
package s3tables

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *S3TablesProvider {
	t.Helper()
	p := &S3TablesProvider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *S3TablesProvider, method, path, body string) *plugin.Response {
	t.Helper()
	var bodyStr string
	if body != "" {
		bodyStr = body
	} else {
		bodyStr = "{}"
	}
	req := httptest.NewRequest(method, path, strings.NewReader(bodyStr))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func parseJSON(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestTableBucketCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create table bucket: POST /buckets/{name} with name in body
	resp := callREST(t, p, "POST", "/buckets/my-bucket", `{"name": "my-bucket"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["arn"])

	// Get table bucket
	resp2 := callREST(t, p, "GET", "/buckets/my-bucket", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-bucket", m2["name"])

	// List table buckets
	resp3 := callREST(t, p, "GET", "/buckets", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	buckets := m3["tableBuckets"].([]any)
	assert.Len(t, buckets, 1)

	// Delete table bucket
	resp4 := callREST(t, p, "DELETE", "/buckets/my-bucket", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get should fail
	resp5 := callREST(t, p, "GET", "/buckets/my-bucket", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestNamespaceCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create bucket first
	callREST(t, p, "POST", "/buckets/test-bucket", `{"name": "test-bucket"}`)

	// Create namespace: PUT /buckets/{bucket}/namespaces/{ns}
	resp := callREST(t, p, "PUT", "/buckets/test-bucket/namespaces/my-ns", `{"namespace": ["my-ns"]}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get namespace
	resp2 := callREST(t, p, "GET", "/buckets/test-bucket/namespaces/my-ns", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	// namespace is returned as an array
	nsArr := m2["namespace"].([]any)
	assert.Equal(t, "my-ns", nsArr[0])

	// List namespaces
	resp3 := callREST(t, p, "GET", "/buckets/test-bucket/namespaces", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	nsList := m3["namespaces"].([]any)
	assert.Len(t, nsList, 1)

	// Delete namespace
	resp4 := callREST(t, p, "DELETE", "/buckets/test-bucket/namespaces/my-ns", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get should fail after deletion
	resp5 := callREST(t, p, "GET", "/buckets/test-bucket/namespaces/my-ns", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestTableCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup bucket and namespace
	callREST(t, p, "POST", "/buckets/tbl-bucket", `{"name": "tbl-bucket"}`)
	callREST(t, p, "PUT", "/buckets/tbl-bucket/namespaces/ns1", `{"namespace": ["ns1"]}`)

	// Create table: POST /buckets/{bucket}/namespaces/{ns}/tables
	resp := callREST(t, p, "POST", "/buckets/tbl-bucket/namespaces/ns1/tables", `{"name": "my-table", "format": "ICEBERG"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["tableARN"])

	// Get table: GET /buckets/{bucket}/namespaces/{ns}/tables/{name}
	resp2 := callREST(t, p, "GET", "/buckets/tbl-bucket/namespaces/ns1/tables/my-table", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-table", m2["name"])

	// List tables
	resp3 := callREST(t, p, "GET", "/buckets/tbl-bucket/namespaces/ns1/tables", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	tables := m3["tables"].([]any)
	assert.Len(t, tables, 1)

	// Delete table: DELETE /buckets/{bucket}/namespaces/{ns}/tables/{name}
	resp4 := callREST(t, p, "DELETE", "/buckets/tbl-bucket/namespaces/ns1/tables/my-table", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get should fail
	resp5 := callREST(t, p, "GET", "/buckets/tbl-bucket/namespaces/ns1/tables/my-table", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestTablePolicy(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets/pol-bucket", `{"name": "pol-bucket"}`)
	callREST(t, p, "PUT", "/buckets/pol-bucket/namespaces/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "POST", "/buckets/pol-bucket/namespaces/ns/tables", `{"name": "t1"}`)

	// Put table policy
	resp := callREST(t, p, "PUT", "/buckets/pol-bucket/namespaces/ns/tables/t1/policy", `{"resourcePolicy": "{\"Version\":\"2012-10-17\"}"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get table policy
	resp2 := callREST(t, p, "GET", "/buckets/pol-bucket/namespaces/ns/tables/t1/policy", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.NotEmpty(t, m["resourcePolicy"])

	// Delete table policy
	resp3 := callREST(t, p, "DELETE", "/buckets/pol-bucket/namespaces/ns/tables/t1/policy", "")
	assert.Equal(t, 204, resp3.StatusCode)
}

func TestTableBucketPolicy(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets/bp-bucket", `{"name": "bp-bucket"}`)

	// Put
	resp := callREST(t, p, "PUT", "/buckets/bp-bucket/policy", `{"resourcePolicy": "{}"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get
	resp2 := callREST(t, p, "GET", "/buckets/bp-bucket/policy", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Delete
	resp3 := callREST(t, p, "DELETE", "/buckets/bp-bucket/policy", "")
	assert.Equal(t, 204, resp3.StatusCode)
}

func TestTableEncryption(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets/enc-bucket", `{"name": "enc-bucket"}`)
	callREST(t, p, "PUT", "/buckets/enc-bucket/namespaces/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "POST", "/buckets/enc-bucket/namespaces/ns/tables", `{"name": "t1"}`)

	// Put table encryption
	resp := callREST(t, p, "PUT", "/buckets/enc-bucket/namespaces/ns/tables/t1/encryption",
		`{"encryptionConfiguration": {"sseAlgorithm": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:000000000000:key/test"}}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get
	resp2 := callREST(t, p, "GET", "/buckets/enc-bucket/namespaces/ns/tables/t1/encryption", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.NotEmpty(t, m["encryptionConfiguration"])

	// Bucket-level encryption
	resp3 := callREST(t, p, "PUT", "/buckets/enc-bucket/encryption",
		`{"encryptionConfiguration": {"sseAlgorithm": "AES256"}}`)
	assert.Equal(t, 204, resp3.StatusCode)

	resp4 := callREST(t, p, "GET", "/buckets/enc-bucket/encryption", "")
	assert.Equal(t, 200, resp4.StatusCode)
}

func TestTableMaintenance(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets/m-bucket", `{"name": "m-bucket"}`)
	callREST(t, p, "PUT", "/buckets/m-bucket/namespaces/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "POST", "/buckets/m-bucket/namespaces/ns/tables", `{"name": "t1"}`)

	// Put maintenance config
	resp := callREST(t, p, "PUT", "/buckets/m-bucket/namespaces/ns/tables/t1/maintenance",
		`{"type": "icebergCompaction", "value": {"status": "enabled"}}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get maintenance
	resp2 := callREST(t, p, "GET", "/buckets/m-bucket/namespaces/ns/tables/t1/maintenance", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Get maintenance status
	resp3 := callREST(t, p, "GET", "/buckets/m-bucket/namespaces/ns/tables/t1/maintenance-status", "")
	assert.Equal(t, 200, resp3.StatusCode)
}

func TestTableRenameAndMetadata(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets/ren-bucket", `{"name": "ren-bucket"}`)
	callREST(t, p, "PUT", "/buckets/ren-bucket/namespaces/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "POST", "/buckets/ren-bucket/namespaces/ns/tables", `{"name": "old-name"}`)

	// Rename
	resp := callREST(t, p, "POST", "/buckets/ren-bucket/namespaces/ns/tables/old-name/rename",
		`{"newName": "new-name"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Update metadata location
	resp2 := callREST(t, p, "PUT", "/buckets/ren-bucket/namespaces/ns/tables/new-name/metadata-location",
		`{"metadataLocation": "s3://bucket/metadata.json"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.Equal(t, "s3://bucket/metadata.json", m["metadataLocation"])

	// Get metadata location
	resp3 := callREST(t, p, "GET", "/buckets/ren-bucket/namespaces/ns/tables/new-name/metadata-location", "")
	assert.Equal(t, 200, resp3.StatusCode)
}
