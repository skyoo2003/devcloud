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

	// Create table bucket: POST /buckets with name in body
	resp := callREST(t, p, "POST", "/buckets", `{"name": "my-bucket"}`)
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
	callREST(t, p, "POST", "/buckets", `{"name": "test-bucket"}`)

	// Create namespace: PUT /namespaces/{bucketARN}/{ns}
	resp := callREST(t, p, "PUT", "/namespaces/test-bucket/my-ns", `{"namespace": ["my-ns"]}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get namespace
	resp2 := callREST(t, p, "GET", "/namespaces/test-bucket/my-ns", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	// namespace is returned as an array
	nsArr := m2["namespace"].([]any)
	assert.Equal(t, "my-ns", nsArr[0])

	// List namespaces
	resp3 := callREST(t, p, "GET", "/namespaces/test-bucket", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	nsList := m3["namespaces"].([]any)
	assert.Len(t, nsList, 1)

	// Delete namespace
	resp4 := callREST(t, p, "DELETE", "/namespaces/test-bucket/my-ns", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get should fail after deletion
	resp5 := callREST(t, p, "GET", "/namespaces/test-bucket/my-ns", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestTableCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup bucket and namespace
	callREST(t, p, "POST", "/buckets", `{"name": "tbl-bucket"}`)
	callREST(t, p, "PUT", "/namespaces/tbl-bucket/ns1", `{"namespace": ["ns1"]}`)

	// Create table: PUT /tables/{bucketARN}/{ns}
	resp := callREST(t, p, "PUT", "/tables/tbl-bucket/ns1", `{"name": "my-table", "format": "ICEBERG"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["tableARN"])

	// Get table: GET /get-table?tableBucketARN=...&namespace=...&name=...
	resp2 := callREST(t, p, "GET", "/get-table?tableBucketARN=tbl-bucket&namespace=ns1&name=my-table", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	assert.Equal(t, "my-table", m2["name"])

	// List tables: GET /tables/{bucketARN}
	resp3 := callREST(t, p, "GET", "/tables/tbl-bucket", "")
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	tables := m3["tables"].([]any)
	assert.Len(t, tables, 1)

	// Delete table: DELETE /tables/{bucketARN}/{ns}/{name}
	resp4 := callREST(t, p, "DELETE", "/tables/tbl-bucket/ns1/my-table", "")
	assert.Equal(t, 204, resp4.StatusCode)

	// Get should fail
	resp5 := callREST(t, p, "GET", "/get-table?tableBucketARN=tbl-bucket&namespace=ns1&name=my-table", "")
	assert.Equal(t, 404, resp5.StatusCode)
}

func TestTablePolicy(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets", `{"name": "pol-bucket"}`)
	callREST(t, p, "PUT", "/namespaces/pol-bucket/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "PUT", "/tables/pol-bucket/ns", `{"name": "t1", "format": "ICEBERG"}`)

	// Put table policy
	resp := callREST(t, p, "PUT", "/tables/pol-bucket/ns/t1/policy", `{"resourcePolicy": "{\"Version\":\"2012-10-17\"}"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get table policy
	resp2 := callREST(t, p, "GET", "/tables/pol-bucket/ns/t1/policy", "")
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.NotEmpty(t, m["resourcePolicy"])

	// Delete table policy
	resp3 := callREST(t, p, "DELETE", "/tables/pol-bucket/ns/t1/policy", "")
	assert.Equal(t, 204, resp3.StatusCode)
}

func TestTableBucketPolicy(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets", `{"name": "bp-bucket"}`)

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

	callREST(t, p, "POST", "/buckets", `{"name": "enc-bucket"}`)
	callREST(t, p, "PUT", "/namespaces/enc-bucket/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "PUT", "/tables/enc-bucket/ns", `{"name": "t1", "format": "ICEBERG"}`)

	// Put table encryption
	resp := callREST(t, p, "PUT", "/tables/enc-bucket/ns/t1/encryption",
		`{"encryptionConfiguration": {"sseAlgorithm": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:000000000000:key/test"}}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get
	resp2 := callREST(t, p, "GET", "/tables/enc-bucket/ns/t1/encryption", "")
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

	callREST(t, p, "POST", "/buckets", `{"name": "m-bucket"}`)
	callREST(t, p, "PUT", "/namespaces/m-bucket/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "PUT", "/tables/m-bucket/ns", `{"name": "t1", "format": "ICEBERG"}`)

	// Put maintenance config
	resp := callREST(t, p, "PUT", "/tables/m-bucket/ns/t1/maintenance",
		`{"type": "icebergCompaction", "value": {"status": "enabled"}}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Get maintenance
	resp2 := callREST(t, p, "GET", "/tables/m-bucket/ns/t1/maintenance", "")
	assert.Equal(t, 200, resp2.StatusCode)

	// Get maintenance status
	resp3 := callREST(t, p, "GET", "/tables/m-bucket/ns/t1/maintenance-job-status", "")
	assert.Equal(t, 200, resp3.StatusCode)
}

func TestTableRenameAndMetadata(t *testing.T) {
	p := newTestProvider(t)

	callREST(t, p, "POST", "/buckets", `{"name": "ren-bucket"}`)
	callREST(t, p, "PUT", "/namespaces/ren-bucket/ns", `{"namespace": ["ns"]}`)
	callREST(t, p, "PUT", "/tables/ren-bucket/ns", `{"name": "old-name", "format": "ICEBERG"}`)

	// Rename
	resp := callREST(t, p, "POST", "/tables/ren-bucket/ns/old-name/rename",
		`{"newName": "new-name"}`)
	assert.Equal(t, 204, resp.StatusCode)

	// Update metadata location
	resp2 := callREST(t, p, "PUT", "/tables/ren-bucket/ns/new-name/metadata-location",
		`{"metadataLocation": "s3://bucket/metadata.json"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	assert.Equal(t, "s3://bucket/metadata.json", m["metadataLocation"])

	// Get metadata location
	resp3 := callREST(t, p, "GET", "/tables/ren-bucket/ns/new-name/metadata-location", "")
	assert.Equal(t, 200, resp3.StatusCode)
}
