// SPDX-License-Identifier: Apache-2.0

package dynamodb

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

func newTestDynamoDBProvider(t *testing.T) *DynamoDBProvider {
	t.Helper()
	p := &DynamoDBProvider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	return p
}

// doRequest is a helper that sends a JSON body to the provider with the given operation.
func doRequest(t *testing.T, p *DynamoDBProvider, op string, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func TestDynamoDBProvider_CreateTable(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer p.Shutdown(context.Background())

	resp := doRequest(t, p, "CreateTable", `{
		"TableName": "Users",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	assert.Equal(t, 200, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	desc, ok := result["TableDescription"].(map[string]any)
	require.True(t, ok, "expected TableDescription in response")
	assert.Equal(t, "Users", desc["TableName"])
	assert.Equal(t, "ACTIVE", desc["TableStatus"])
}

func TestDynamoDBProvider_PutAndGetItem(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer p.Shutdown(context.Background())

	// Create table.
	doRequest(t, p, "CreateTable", `{
		"TableName": "Items",
		"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]
	}`)

	// Put item.
	putResp := doRequest(t, p, "PutItem", `{
		"TableName": "Items",
		"Item": {"id": {"S": "item-1"}, "name": {"S": "Widget"}}
	}`)
	assert.Equal(t, 200, putResp.StatusCode)

	// Get item.
	getResp := doRequest(t, p, "GetItem", `{
		"TableName": "Items",
		"Key": {"id": {"S": "item-1"}}
	}`)
	assert.Equal(t, 200, getResp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body, &result))
	item, ok := result["Item"].(map[string]any)
	require.True(t, ok, "expected Item in response")
	idAttr, ok := item["id"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "item-1", idAttr["S"])
	nameAttr, ok := item["name"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Widget", nameAttr["S"])
}

func TestDynamoDBProvider_DeleteItem(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer p.Shutdown(context.Background())

	// Create table and put item.
	doRequest(t, p, "CreateTable", `{
		"TableName": "Orders",
		"KeySchema": [{"AttributeName": "orderId", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "orderId", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{
		"TableName": "Orders",
		"Item": {"orderId": {"S": "order-99"}, "status": {"S": "pending"}}
	}`)

	// Delete item.
	delResp := doRequest(t, p, "DeleteItem", `{
		"TableName": "Orders",
		"Key": {"orderId": {"S": "order-99"}}
	}`)
	assert.Equal(t, 200, delResp.StatusCode)

	// Get item — should return empty response (no Item key).
	getResp := doRequest(t, p, "GetItem", `{
		"TableName": "Orders",
		"Key": {"orderId": {"S": "order-99"}}
	}`)
	assert.Equal(t, 200, getResp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body, &result))
	_, hasItem := result["Item"]
	assert.False(t, hasItem, "expected no Item in response after deletion")
}

func TestDynamoDBProvider_ListTables(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer p.Shutdown(context.Background())

	doRequest(t, p, "CreateTable", `{
		"TableName": "Alpha",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "CreateTable", `{
		"TableName": "Beta",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	listResp := doRequest(t, p, "ListTables", `{}`)
	assert.Equal(t, 200, listResp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &result))
	rawNames, ok := result["TableNames"].([]any)
	require.True(t, ok, "expected TableNames in response")

	names := make([]string, 0, len(rawNames))
	for _, n := range rawNames {
		names = append(names, n.(string))
	}
	assert.Contains(t, names, "Alpha")
	assert.Contains(t, names, "Beta")
}

func TestDynamoDBProvider_Scan(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer p.Shutdown(context.Background())

	doRequest(t, p, "CreateTable", `{
		"TableName": "Products",
		"KeySchema": [{"AttributeName": "sku", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "sku", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{
		"TableName": "Products",
		"Item": {"sku": {"S": "sku-a"}, "price": {"N": "10"}}
	}`)
	doRequest(t, p, "PutItem", `{
		"TableName": "Products",
		"Item": {"sku": {"S": "sku-b"}, "price": {"N": "20"}}
	}`)

	scanResp := doRequest(t, p, "Scan", `{"TableName": "Products"}`)
	assert.Equal(t, 200, scanResp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(scanResp.Body, &result))
	items, ok := result["Items"].([]any)
	require.True(t, ok, "expected Items in response")
	assert.Equal(t, 2, len(items))

	count, ok := result["Count"].(float64)
	require.True(t, ok)
	assert.Equal(t, float64(2), count)
}
