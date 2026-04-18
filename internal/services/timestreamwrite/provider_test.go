// SPDX-License-Identifier: Apache-2.0

// internal/services/timestreamwrite/provider_test.go
package timestreamwrite

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

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callJSON(t *testing.T, p *Provider, target, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
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

const svcTarget = "Timestream_20181101"

func TestDatabaseCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateDatabase
	resp := callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)
	m := parseJSON(t, resp)
	db, ok := m["Database"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "mydb", db["DatabaseName"])
	assert.NotEmpty(t, db["Arn"])

	// Duplicate CreateDatabase → ConflictException
	resp2 := callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// DescribeDatabase
	resp3 := callJSON(t, p, svcTarget+".DescribeDatabase", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	db3, ok := m3["Database"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "mydb", db3["DatabaseName"])

	// ListDatabases
	callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"other"}`)
	resp4 := callJSON(t, p, svcTarget+".ListDatabases", `{}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	dbs, ok := m4["Databases"].([]any)
	require.True(t, ok)
	assert.Len(t, dbs, 2)

	// UpdateDatabase
	resp5 := callJSON(t, p, svcTarget+".UpdateDatabase", `{"DatabaseName":"mydb","KmsKeyId":"key-123"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	db5, ok := m5["Database"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "key-123", db5["KmsKeyId"])

	// DeleteDatabase
	resp6 := callJSON(t, p, svcTarget+".DeleteDatabase", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	// DescribeDatabase after delete → 404
	resp7 := callJSON(t, p, svcTarget+".DescribeDatabase", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 404, resp7.StatusCode)

	// DescribeEndpoints
	resp8 := callJSON(t, p, svcTarget+".DescribeEndpoints", `{}`)
	assert.Equal(t, 200, resp8.StatusCode)
	m8 := parseJSON(t, resp8)
	endpoints, ok := m8["Endpoints"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, endpoints)
}

func TestTableCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Setup database.
	callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"testdb"}`)

	// CreateTable
	resp := callJSON(t, p, svcTarget+".CreateTable", `{"DatabaseName":"testdb","TableName":"events"}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)
	m := parseJSON(t, resp)
	tbl, ok := m["Table"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "events", tbl["TableName"])
	assert.Equal(t, "testdb", tbl["DatabaseName"])
	assert.NotEmpty(t, tbl["Arn"])

	// Duplicate CreateTable → ConflictException
	resp2 := callJSON(t, p, svcTarget+".CreateTable", `{"DatabaseName":"testdb","TableName":"events"}`)
	assert.Equal(t, 409, resp2.StatusCode)

	// CreateTable in non-existent database → 404
	resp3 := callJSON(t, p, svcTarget+".CreateTable", `{"DatabaseName":"nope","TableName":"events"}`)
	assert.Equal(t, 404, resp3.StatusCode)

	// DescribeTable
	resp4 := callJSON(t, p, svcTarget+".DescribeTable", `{"DatabaseName":"testdb","TableName":"events"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	tbl4, ok := m4["Table"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", tbl4["TableStatus"])

	// ListTables
	callJSON(t, p, svcTarget+".CreateTable", `{"DatabaseName":"testdb","TableName":"metrics"}`)
	resp5 := callJSON(t, p, svcTarget+".ListTables", `{"DatabaseName":"testdb"}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	tables, ok := m5["Tables"].([]any)
	require.True(t, ok)
	assert.Len(t, tables, 2)

	// UpdateTable
	resp6 := callJSON(t, p, svcTarget+".UpdateTable",
		`{"DatabaseName":"testdb","TableName":"events","RetentionProperties":{"MemoryStoreRetentionPeriodInHours":12,"MagneticStoreRetentionPeriodInDays":365}}`)
	assert.Equal(t, 200, resp6.StatusCode)
	m6 := parseJSON(t, resp6)
	tbl6, ok := m6["Table"].(map[string]any)
	require.True(t, ok)
	rp, ok := tbl6["RetentionProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(12), rp["MemoryStoreRetentionPeriodInHours"])

	// DeleteTable
	resp7 := callJSON(t, p, svcTarget+".DeleteTable", `{"DatabaseName":"testdb","TableName":"events"}`)
	assert.Equal(t, 200, resp7.StatusCode)

	// DescribeTable after delete → 404
	resp8 := callJSON(t, p, svcTarget+".DescribeTable", `{"DatabaseName":"testdb","TableName":"events"}`)
	assert.Equal(t, 404, resp8.StatusCode)
}

func TestWriteRecords(t *testing.T) {
	p := newTestProvider(t)

	// Setup database + table.
	callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"mydb"}`)
	callJSON(t, p, svcTarget+".CreateTable", `{"DatabaseName":"mydb","TableName":"metrics"}`)

	// WriteRecords — success (no-op).
	body := `{
		"DatabaseName":"mydb",
		"TableName":"metrics",
		"Records":[
			{"MeasureName":"cpu","MeasureValue":"75.0","MeasureValueType":"DOUBLE","Time":"1234567890"},
			{"MeasureName":"mem","MeasureValue":"50.0","MeasureValueType":"DOUBLE","Time":"1234567891"}
		]
	}`
	resp := callJSON(t, p, svcTarget+".WriteRecords", body)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)
	m := parseJSON(t, resp)
	ingested, ok := m["RecordsIngested"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(2), ingested["Total"])

	// Missing fields → 400.
	resp2 := callJSON(t, p, svcTarget+".WriteRecords", `{"DatabaseName":"mydb"}`)
	assert.Equal(t, 400, resp2.StatusCode)

	// BatchLoadTask round-trip.
	resp3 := callJSON(t, p, svcTarget+".CreateBatchLoadTask",
		`{"TargetDatabaseName":"mydb","TargetTableName":"metrics"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	taskID, ok := m3["TaskId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, taskID)

	resp4 := callJSON(t, p, svcTarget+".DescribeBatchLoadTask", `{"TaskId":"`+taskID+`"}`)
	assert.Equal(t, 200, resp4.StatusCode)
	m4 := parseJSON(t, resp4)
	desc, ok := m4["BatchLoadTaskDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, taskID, desc["TaskId"])
	assert.Equal(t, "CREATED", desc["TaskStatus"])

	resp5 := callJSON(t, p, svcTarget+".ListBatchLoadTasks", `{}`)
	assert.Equal(t, 200, resp5.StatusCode)
	m5 := parseJSON(t, resp5)
	tasks, ok := m5["BatchLoadTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, tasks, 1)

	resp6 := callJSON(t, p, svcTarget+".ResumeBatchLoadTask", `{"TaskId":"`+taskID+`"}`)
	assert.Equal(t, 200, resp6.StatusCode)

	resp7 := callJSON(t, p, svcTarget+".DescribeBatchLoadTask", `{"TaskId":"`+taskID+`"}`)
	m7 := parseJSON(t, resp7)
	desc7, ok := m7["BatchLoadTaskDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "IN_PROGRESS", desc7["TaskStatus"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create database to get its ARN.
	resp := callJSON(t, p, svcTarget+".CreateDatabase", `{"DatabaseName":"tagdb"}`)
	m := parseJSON(t, resp)
	db, _ := m["Database"].(map[string]any)
	arn, _ := db["Arn"].(string)
	require.NotEmpty(t, arn)

	// TagResource
	tagBody := `{"ResourceARN":"` + arn + `","Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"data"}]}`
	resp2 := callJSON(t, p, svcTarget+".TagResource", tagBody)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp2.ContentType)

	// ListTagsForResource
	resp3 := callJSON(t, p, svcTarget+".ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
	assert.Equal(t, 200, resp3.StatusCode)
	m3 := parseJSON(t, resp3)
	tagsList, ok := m3["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList, 2)

	// UntagResource
	resp4 := callJSON(t, p, svcTarget+".UntagResource", `{"ResourceARN":"`+arn+`","TagKeys":["env"]}`)
	assert.Equal(t, 200, resp4.StatusCode)

	// ListTagsForResource after untag
	resp5 := callJSON(t, p, svcTarget+".ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
	m5 := parseJSON(t, resp5)
	tagsList5, ok := m5["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tagsList5, 1)
	tag5 := tagsList5[0].(map[string]any)
	assert.Equal(t, "team", tag5["Key"])
	assert.Equal(t, "data", tag5["Value"])
}
