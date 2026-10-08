// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestDynamoDBPartiQLAndAdvanced(t *testing.T) {
	tmpDir := t.TempDir()
	p := &DynamoDBProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: tmpDir})
	require.NoError(t, err)
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx := context.Background()

	// 1. Create a table
	createReq := map[string]any{
		"TableName": "Users",
		"KeySchema": []map[string]string{
			{"AttributeName": "id", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "id", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	}
	body, _ := json.Marshal(createReq)
	resp, err := p.HandleRequest(ctx, "CreateTable", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Insert with PartiQL ExecuteStatement
	insertStmt := `INSERT INTO "Users" VALUE {'id': 'u1', 'name': 'Alice'}`
	stmtReq := map[string]any{
		"Statement": insertStmt,
	}
	body, _ = json.Marshal(stmtReq)
	resp, err = p.HandleRequest(ctx, "ExecuteStatement", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 3. Select with PartiQL ExecuteStatement
	selectStmt := `SELECT * FROM "Users" WHERE id = 'u1'`
	stmtReq = map[string]any{
		"Statement": selectStmt,
	}
	body, _ = json.Marshal(stmtReq)
	resp, err = p.HandleRequest(ctx, "ExecuteStatement", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var selectOut map[string]any
	err = json.Unmarshal(resp.Body, &selectOut)
	require.NoError(t, err)
	items := selectOut["Items"].([]any)
	assert.Len(t, items, 1)

	// 4. BatchExecuteStatement
	batchReq := map[string]any{
		"Statements": []map[string]any{
			{"Statement": selectStmt},
		},
	}
	body, _ = json.Marshal(batchReq)
	resp, err = p.HandleRequest(ctx, "BatchExecuteStatement", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 5. ExecuteTransaction
	txReq := map[string]any{
		"TransactStatements": []map[string]any{
			{"Statement": selectStmt},
		},
	}
	body, _ = json.Marshal(txReq)
	resp, err = p.HandleRequest(ctx, "ExecuteTransaction", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 6. EnableKinesisStreamingDestination & Disable
	kinesisReq := map[string]any{
		"TableName": "Users",
		"StreamArn": "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream",
	}
	body, _ = json.Marshal(kinesisReq)
	resp, err = p.HandleRequest(ctx, "EnableKinesisStreamingDestination", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = p.HandleRequest(ctx, "DisableKinesisStreamingDestination", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 7. ExportTableToPointInTime
	exportReq := map[string]any{
		"TableArn": "arn:aws:dynamodb:us-east-1:000000000000:table/Users",
		"S3Bucket": "backup-bucket",
	}
	body, _ = json.Marshal(exportReq)
	resp, err = p.HandleRequest(ctx, "ExportTableToPointInTime", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 8. RestoreTableToPointInTime
	pitrReq := map[string]any{
		"SourceTableName": "Users",
		"TargetTableName": "Users-Restored",
	}
	body, _ = json.Marshal(pitrReq)
	resp, err = p.HandleRequest(ctx, "RestoreTableToPointInTime", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 9. RestoreTableFromBackup
	backupReq := map[string]any{
		"TargetTableName": "Users-FromBackup",
		"BackupArn":       "arn:aws:dynamodb:us-east-1:000000000000:table/Users/backup/b1",
	}
	body, _ = json.Marshal(backupReq)
	resp, err = p.HandleRequest(ctx, "RestoreTableFromBackup", reqWithBody(body))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func reqWithBody(b []byte) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
}
