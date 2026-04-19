// SPDX-License-Identifier: Apache-2.0

// internal/services/lakeformation/provider_test.go
package lakeformation

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
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callOp(t *testing.T, p *Provider, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/"+op, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

func TestLFTagCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callOp(t, p, "CreateLFTag", `{"TagKey":"env","TagValues":["dev","prod"]}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Get
	resp = callOp(t, p, "GetLFTag", `{"TagKey":"env"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	assert.Equal(t, "env", body["TagKey"])
	vals, _ := body["TagValues"].([]any)
	assert.Len(t, vals, 2)

	// Duplicate create
	resp = callOp(t, p, "CreateLFTag", `{"TagKey":"env","TagValues":["dev"]}`)
	assert.Equal(t, 409, resp.StatusCode)

	// Update: add "staging", delete "dev"
	resp = callOp(t, p, "UpdateLFTag", `{"TagKey":"env","TagValuesToAdd":["staging"],"TagValuesToDelete":["dev"]}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "GetLFTag", `{"TagKey":"env"}`)
	body = parseBody(t, resp)
	vals, _ = body["TagValues"].([]any)
	assert.Len(t, vals, 2)
	var valStrs []string
	for _, v := range vals {
		valStrs = append(valStrs, v.(string))
	}
	assert.Contains(t, valStrs, "prod")
	assert.Contains(t, valStrs, "staging")
	assert.NotContains(t, valStrs, "dev")

	// List
	callOp(t, p, "CreateLFTag", `{"TagKey":"team","TagValues":["data"]}`)
	resp = callOp(t, p, "ListLFTags", `{}`)
	body = parseBody(t, resp)
	list, _ := body["LFTags"].([]any)
	assert.Len(t, list, 2)

	// Delete
	resp = callOp(t, p, "DeleteLFTag", `{"TagKey":"env"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "GetLFTag", `{"TagKey":"env"}`)
	assert.Equal(t, 404, resp.StatusCode)

	// Delete non-existent
	resp = callOp(t, p, "DeleteLFTag", `{"TagKey":"env"}`)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestPermissionGrantAndRevoke(t *testing.T) {
	p := newTestProvider(t)

	// Grant permissions
	body := `{
		"Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/TestRole"},
		"Resource": {"Database": {"Name": "mydb"}},
		"Permissions": ["SELECT", "DESCRIBE"],
		"PermissionsWithGrantOption": []
	}`
	resp := callOp(t, p, "GrantPermissions", body)
	assert.Equal(t, 200, resp.StatusCode)

	// List permissions
	resp = callOp(t, p, "ListPermissions", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseBody(t, resp)
	list, _ := m["PrincipalResourcePermissions"].([]any)
	assert.Len(t, list, 1)

	// Grant another
	body2 := `{
		"Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/AnotherRole"},
		"Resource": {"Database": {"Name": "mydb"}},
		"Permissions": ["CREATE_TABLE"]
	}`
	callOp(t, p, "GrantPermissions", body2)

	resp = callOp(t, p, "ListPermissions", `{}`)
	m = parseBody(t, resp)
	list, _ = m["PrincipalResourcePermissions"].([]any)
	assert.Len(t, list, 2)

	// Revoke permissions for first principal
	revokeBody := `{
		"Principal": {"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/TestRole"},
		"Resource": {"Database": {"Name": "mydb"}},
		"Permissions": ["SELECT"]
	}`
	resp = callOp(t, p, "RevokePermissions", revokeBody)
	assert.Equal(t, 200, resp.StatusCode)

	// Only second principal remains
	resp = callOp(t, p, "ListPermissions", `{}`)
	m = parseBody(t, resp)
	list, _ = m["PrincipalResourcePermissions"].([]any)
	assert.Len(t, list, 1)

	// Batch grant
	batchBody := `{
		"Entries": [
			{
				"Id": "e1",
				"Principal": {"DataLakePrincipalIdentifier": "role1"},
				"Resource": {"Table": {"DatabaseName": "mydb", "Name": "t1"}},
				"Permissions": ["SELECT"]
			},
			{
				"Id": "e2",
				"Principal": {"DataLakePrincipalIdentifier": "role2"},
				"Resource": {"Table": {"DatabaseName": "mydb", "Name": "t2"}},
				"Permissions": ["INSERT"]
			}
		]
	}`
	resp = callOp(t, p, "BatchGrantPermissions", batchBody)
	assert.Equal(t, 200, resp.StatusCode)
	bm := parseBody(t, resp)
	failures, _ := bm["Failures"].([]any)
	assert.Empty(t, failures)

	resp = callOp(t, p, "ListPermissions", `{}`)
	m = parseBody(t, resp)
	list, _ = m["PrincipalResourcePermissions"].([]any)
	assert.Len(t, list, 3) // 1 remaining + 2 batch granted
}

func TestResourceRegisterAndDeregister(t *testing.T) {
	p := newTestProvider(t)

	arn := "arn:aws:s3:::my-data-lake-bucket"

	// Register
	resp := callOp(t, p, "RegisterResource", `{"ResourceArn":"`+arn+`","RoleArn":"arn:aws:iam::123456789012:role/LakeFormationRole"}`)
	assert.Equal(t, 200, resp.StatusCode)

	// Duplicate register
	resp = callOp(t, p, "RegisterResource", `{"ResourceArn":"`+arn+`","RoleArn":"arn:aws:iam::123456789012:role/LakeFormationRole"}`)
	assert.Equal(t, 409, resp.StatusCode)

	// Describe
	resp = callOp(t, p, "DescribeResource", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	info, _ := body["ResourceInfo"].(map[string]any)
	assert.Equal(t, arn, info["ResourceArn"])

	// Update
	resp = callOp(t, p, "UpdateResource", `{"ResourceArn":"`+arn+`","RoleArn":"arn:aws:iam::123456789012:role/NewRole"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "DescribeResource", `{"ResourceArn":"`+arn+`"}`)
	body = parseBody(t, resp)
	info, _ = body["ResourceInfo"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::123456789012:role/NewRole", info["RoleArn"])

	// List
	resp = callOp(t, p, "ListResources", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	body = parseBody(t, resp)
	list, _ := body["ResourceInfoList"].([]any)
	assert.Len(t, list, 1)

	// Deregister
	resp = callOp(t, p, "DeregisterResource", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "DescribeResource", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, 404, resp.StatusCode)

	// Deregister non-existent
	resp = callOp(t, p, "DeregisterResource", `{"ResourceArn":"arn:aws:s3:::nonexistent"}`)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestDataLakeSettings(t *testing.T) {
	p := newTestProvider(t)

	// Get default (empty) settings
	resp := callOp(t, p, "GetDataLakeSettings", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	settings, _ := body["DataLakeSettings"].(map[string]any)
	assert.NotNil(t, settings)
	admins, _ := settings["DataLakeAdmins"].([]any)
	assert.Empty(t, admins)

	// Put settings
	putBody := `{
		"DataLakeSettings": {
			"DataLakeAdmins": [
				{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/Admin"}
			],
			"CreateDatabaseDefaultPermissions": [],
			"CreateTableDefaultPermissions": []
		}
	}`
	resp = callOp(t, p, "PutDataLakeSettings", putBody)
	assert.Equal(t, 200, resp.StatusCode)

	// Get updated settings
	resp = callOp(t, p, "GetDataLakeSettings", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	body = parseBody(t, resp)
	settings, _ = body["DataLakeSettings"].(map[string]any)
	admins, _ = settings["DataLakeAdmins"].([]any)
	assert.Len(t, admins, 1)

	// Put with catalog ID
	resp = callOp(t, p, "PutDataLakeSettings", `{"CatalogId":"111122223333","DataLakeSettings":{"DataLakeAdmins":[]}}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "GetDataLakeSettings", `{"CatalogId":"111122223333"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body = parseBody(t, resp)
	settings, _ = body["DataLakeSettings"].(map[string]any)
	admins, _ = settings["DataLakeAdmins"].([]any)
	assert.Empty(t, admins)
}

func TestDataCellsFilterCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `{
		"TableData": {
			"Name": "myfilter",
			"DatabaseName": "mydb",
			"TableName": "mytable",
			"ColumnNames": ["col1", "col2"],
			"RowFilter": {"FilterExpression": "col1 = 'val'"}
		}
	}`

	// Create
	resp := callOp(t, p, "CreateDataCellsFilter", createBody)
	assert.Equal(t, 200, resp.StatusCode)

	// Duplicate
	resp = callOp(t, p, "CreateDataCellsFilter", createBody)
	assert.Equal(t, 409, resp.StatusCode)

	// Get
	resp = callOp(t, p, "GetDataCellsFilter", `{"Name":"myfilter","DatabaseName":"mydb","TableName":"mytable"}`)
	assert.Equal(t, 200, resp.StatusCode)
	body := parseBody(t, resp)
	f, _ := body["DataCellsFilter"].(map[string]any)
	assert.Equal(t, "myfilter", f["Name"])
	colNames, _ := f["ColumnNames"].([]any)
	assert.Len(t, colNames, 2)

	// Update
	updateBody := `{
		"TableData": {
			"Name": "myfilter",
			"DatabaseName": "mydb",
			"TableName": "mytable",
			"ColumnNames": ["col1"],
			"RowFilter": {"FilterExpression": "col1 = 'newval'"}
		}
	}`
	resp = callOp(t, p, "UpdateDataCellsFilter", updateBody)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "GetDataCellsFilter", `{"Name":"myfilter","DatabaseName":"mydb","TableName":"mytable"}`)
	body = parseBody(t, resp)
	f, _ = body["DataCellsFilter"].(map[string]any)
	colNames, _ = f["ColumnNames"].([]any)
	assert.Len(t, colNames, 1)

	// List
	resp = callOp(t, p, "ListDataCellsFilter", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	body = parseBody(t, resp)
	list, _ := body["DataCellsFilters"].([]any)
	assert.Len(t, list, 1)

	// Delete
	resp = callOp(t, p, "DeleteDataCellsFilter", `{"Name":"myfilter","DatabaseName":"mydb","TableName":"mytable"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp = callOp(t, p, "GetDataCellsFilter", `{"Name":"myfilter","DatabaseName":"mydb","TableName":"mytable"}`)
	assert.Equal(t, 404, resp.StatusCode)

	// Delete non-existent
	resp = callOp(t, p, "DeleteDataCellsFilter", `{"Name":"myfilter","DatabaseName":"mydb","TableName":"mytable"}`)
	assert.Equal(t, 404, resp.StatusCode)
}

func TestStubOperations(t *testing.T) {
	p := newTestProvider(t)

	stubs := []struct {
		op   string
		body string
	}{
		{"StartTransaction", `{}`},
		{"CommitTransaction", `{"TransactionId":"abc"}`},
		{"CancelTransaction", `{"TransactionId":"abc"}`},
		{"ListTransactions", `{}`},
		{"CreateLakeFormationOptIn", `{"Principal":{},"Resource":{}}`},
		{"ListLakeFormationOptIns", `{}`},
		{"DeleteLakeFormationOptIn", `{"Principal":{},"Resource":{}}`},
		{"GetDataLakePrincipal", `{}`},
		{"SearchDatabasesByLFTags", `{"Expression":[]}`},
		{"SearchTablesByLFTags", `{"Expression":[]}`},
		{"GetResourceLFTags", `{"Resource":{}}`},
		{"AddLFTagsToResource", `{"Resource":{},"LFTags":[]}`},
		{"RemoveLFTagsFromResource", `{"Resource":{},"LFTags":[]}`},
		{"StartQueryPlanning", `{"QueryPlanningContext":{},"QueryString":"SELECT 1"}`},
		{"ListTableStorageOptimizers", `{"DatabaseName":"db","TableName":"t"}`},
		{"GetEffectivePermissionsForPath", `{"ResourceArn":"arn:aws:s3:::bucket"}`},
	}

	for _, tc := range stubs {
		t.Run(tc.op, func(t *testing.T) {
			resp := callOp(t, p, tc.op, tc.body)
			assert.Equal(t, 200, resp.StatusCode, "op=%s body=%s", tc.op, string(resp.Body))
		})
	}
}
