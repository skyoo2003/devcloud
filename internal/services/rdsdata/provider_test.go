// SPDX-License-Identifier: Apache-2.0

package rdsdata

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestRDSDataExecuteStatementLifecycle(t *testing.T) {
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. Create table
	createReq, err := http.NewRequestWithContext(ctx, "POST", "/Execute", strings.NewReader(`{
		"resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret",
		"database": "testdb",
		"sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN);"
	}`))
	require.NoError(t, err)

	resp, err := p.HandleRequest(ctx, "ExecuteStatement", createReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Insert records
	insertReq, err := http.NewRequestWithContext(ctx, "POST", "/Execute", strings.NewReader(`{
		"resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"database": "testdb",
		"sql": "INSERT INTO users (id, name, active) VALUES (:id, :name, :active);",
		"parameters": [
			{"name": "id", "value": {"longValue": 1}},
			{"name": "name", "value": {"stringValue": "Alice"}},
			{"name": "active", "value": {"booleanValue": true}}
		]
	}`))
	require.NoError(t, err)

	resp, err = p.HandleRequest(ctx, "ExecuteStatement", insertReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var insertOut map[string]any
	err = json.Unmarshal(resp.Body, &insertOut)
	require.NoError(t, err)
	assert.Equal(t, float64(1), insertOut["numberOfRecordsUpdated"])

	// 3. Select records (standard format)
	selectReq, err := http.NewRequestWithContext(ctx, "POST", "/Execute", strings.NewReader(`{
		"resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"database": "testdb",
		"sql": "SELECT id, name, active FROM users WHERE id = :id;",
		"includeResultMetadata": true,
		"parameters": [
			{"name": "id", "value": {"longValue": 1}}
		]
	}`))
	require.NoError(t, err)

	resp, err = p.HandleRequest(ctx, "ExecuteStatement", selectReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var selectOut map[string]any
	err = json.Unmarshal(resp.Body, &selectOut)
	require.NoError(t, err)

	records, ok := selectOut["records"].([]any)
	require.True(t, ok)
	require.Len(t, records, 1)

	row := records[0].([]any)
	assert.Equal(t, float64(1), row[0].(map[string]any)["longValue"])
	assert.Equal(t, "Alice", row[1].(map[string]any)["stringValue"])

	// 4. Select records (JSON format)
	selectJsonReq, err := http.NewRequestWithContext(ctx, "POST", "/Execute", strings.NewReader(`{
		"resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"database": "testdb",
		"sql": "SELECT id, name, active FROM users;",
		"formatRecordsAs": "JSON"
	}`))
	require.NoError(t, err)

	resp, err = p.HandleRequest(ctx, "ExecuteStatement", selectJsonReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var selectJsonOut map[string]any
	err = json.Unmarshal(resp.Body, &selectJsonOut)
	require.NoError(t, err)
	assert.Contains(t, selectJsonOut["formattedRecords"], "Alice")

	// 5. BatchExecuteStatement
	batchReq, err := http.NewRequestWithContext(ctx, "POST", "/BatchExecute", strings.NewReader(`{
		"resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"database": "testdb",
		"sql": "INSERT INTO users (id, name, active) VALUES (:id, :name, :active);",
		"parameterSets": [
			[
				{"name": "id", "value": {"longValue": 2}},
				{"name": "name", "value": {"stringValue": "Bob"}},
				{"name": "active", "value": {"booleanValue": false}}
			],
			[
				{"name": "id", "value": {"longValue": 3}},
				{"name": "name", "value": {"stringValue": "Charlie"}},
				{"name": "active", "value": {"booleanValue": true}}
			]
		]
	}`))
	require.NoError(t, err)

	resp, err = p.HandleRequest(ctx, "BatchExecuteStatement", batchReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 6. Transactions
	beginReq, _ := http.NewRequestWithContext(ctx, "POST", "/BeginTransaction", strings.NewReader(`{}`))
	resp, err = p.HandleRequest(ctx, "BeginTransaction", beginReq)
	require.NoError(t, err)
	var txOut map[string]any
	_ = json.Unmarshal(resp.Body, &txOut)
	assert.NotEmpty(t, txOut["transactionId"])

	commitReq, _ := http.NewRequestWithContext(ctx, "POST", "/CommitTransaction", strings.NewReader(`{}`))
	resp, err = p.HandleRequest(ctx, "CommitTransaction", commitReq)
	require.NoError(t, err)
	var commitOut map[string]any
	_ = json.Unmarshal(resp.Body, &commitOut)
	assert.Equal(t, "Transaction Committed", commitOut["transactionStatus"])

	rollbackReq, _ := http.NewRequestWithContext(ctx, "POST", "/RollbackTransaction", strings.NewReader(`{}`))
	resp, err = p.HandleRequest(ctx, "RollbackTransaction", rollbackReq)
	require.NoError(t, err)
	var rollbackOut map[string]any
	_ = json.Unmarshal(resp.Body, &rollbackOut)
	assert.Equal(t, "Rollback Complete", rollbackOut["transactionStatus"])

	// 7. ExecuteSql
	sqlReq, _ := http.NewRequestWithContext(ctx, "POST", "/ExecuteSql", strings.NewReader(`{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		"database": "testdb",
		"sqlStatements": "DELETE FROM users WHERE id = 1; DELETE FROM users WHERE id = 2;"
	}`))
	resp, err = p.HandleRequest(ctx, "ExecuteSql", sqlReq)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
