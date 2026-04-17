// SPDX-License-Identifier: Apache-2.0

// internal/services/dynamodbstreams/integration_test.go
//
// End-to-end test that verifies DynamoDB writes flow into the streams buffer.
// Lives in the dynamodbstreams package (not dynamodb) so the test can import
// dynamodb without creating an import cycle — the dynamodb package depends on
// dynamodbstreams, never the other way.

package dynamodbstreams_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/services/dynamodb"
	"github.com/skyoo2003/devcloud/internal/services/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doJSON(t *testing.T, handler plugin.ServicePlugin, target, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", target)
	resp, err := handler.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

// TestStreamsReceivesDynamoDBWrites is the headline integration test: create a
// stream-enabled table, write several items via the DynamoDB handler, and
// confirm the records surface through GetRecords.
func TestStreamsReceivesDynamoDBWrites(t *testing.T) {
	dir := t.TempDir()

	// Boot the streams service first so dynamodb can publish into its
	// package-level global store. The order matters only for initialization;
	// afterward, either provider can be exercised in any order.
	streams := &dynamodbstreams.Provider{}
	require.NoError(t, streams.Init(plugin.PluginConfig{DataDir: dir}))
	t.Cleanup(func() { _ = streams.Shutdown(context.Background()) })

	ddb := &dynamodb.DynamoDBProvider{}
	require.NoError(t, ddb.Init(plugin.PluginConfig{DataDir: dir}))
	t.Cleanup(func() { _ = ddb.Shutdown(context.Background()) })

	// 1) Create the table with a stream enabled.
	createResp := doJSON(t, ddb, "DynamoDB_20120810.CreateTable", "CreateTable", `{
		"TableName": "Events",
		"KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
		"StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
	}`)
	require.Equal(t, 200, createResp.StatusCode, string(createResp.Body))

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))
	desc := createOut["TableDescription"].(map[string]any)
	streamARN, _ := desc["LatestStreamArn"].(string)
	require.NotEmpty(t, streamARN, "CreateTable must populate LatestStreamArn when stream is enabled")

	// 2) Write two items, modify one, then delete one.
	doJSON(t, ddb, "DynamoDB_20120810.PutItem", "PutItem", `{
		"TableName": "Events",
		"Item": {"id": {"S": "e1"}, "name": {"S": "first"}}
	}`)
	doJSON(t, ddb, "DynamoDB_20120810.PutItem", "PutItem", `{
		"TableName": "Events",
		"Item": {"id": {"S": "e2"}, "name": {"S": "second"}}
	}`)
	// Second PutItem for e1 should produce a MODIFY event with both images.
	doJSON(t, ddb, "DynamoDB_20120810.PutItem", "PutItem", `{
		"TableName": "Events",
		"Item": {"id": {"S": "e1"}, "name": {"S": "first-v2"}}
	}`)
	doJSON(t, ddb, "DynamoDB_20120810.DeleteItem", "DeleteItem", `{
		"TableName": "Events",
		"Key": {"id": {"S": "e2"}}
	}`)

	// 3) Read the stream through the public API.
	iterResp := doJSON(t, streams, "DynamoDBStreams_20120810.GetShardIterator", "GetShardIterator", `{
		"StreamArn": "`+streamARN+`",
		"ShardId": "`+dynamodbstreams.DefaultShardID()+`",
		"ShardIteratorType": "TRIM_HORIZON"
	}`)
	require.Equal(t, 200, iterResp.StatusCode, string(iterResp.Body))
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body, &iterOut))
	iter := iterOut["ShardIterator"].(string)

	recResp := doJSON(t, streams, "DynamoDBStreams_20120810.GetRecords", "GetRecords",
		`{"ShardIterator":"`+iter+`"}`)
	require.Equal(t, 200, recResp.StatusCode, string(recResp.Body))
	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body, &recOut))
	records, _ := recOut["Records"].([]any)
	require.Len(t, records, 4, "expected 4 stream records (2 INSERT, 1 MODIFY, 1 REMOVE)")

	// Aggregate event names to assert the sequence regardless of per-run ordering
	// of metadata. The records should arrive in write order.
	events := make([]string, 0, len(records))
	for _, r := range records {
		rec := r.(map[string]any)
		events = append(events, rec["eventName"].(string))
	}
	assert.Equal(t, []string{"INSERT", "INSERT", "MODIFY", "REMOVE"}, events)

	// Verify the MODIFY record carries both images for NEW_AND_OLD_IMAGES.
	modify := records[2].(map[string]any)
	dyn := modify["dynamodb"].(map[string]any)
	assert.Contains(t, dyn, "NewImage")
	assert.Contains(t, dyn, "OldImage")
	assert.Contains(t, dyn, "Keys")

	// The REMOVE record should carry OldImage but no NewImage.
	remove := records[3].(map[string]any)
	rdyn := remove["dynamodb"].(map[string]any)
	assert.Contains(t, rdyn, "OldImage")
	_, hasNew := rdyn["NewImage"]
	assert.False(t, hasNew, "REMOVE event must not include NewImage")
}

// TestStreamViewTypeKeysOnly verifies the KEYS_ONLY view type suppresses images.
func TestStreamViewTypeKeysOnly(t *testing.T) {
	dir := t.TempDir()

	streams := &dynamodbstreams.Provider{}
	require.NoError(t, streams.Init(plugin.PluginConfig{DataDir: dir}))
	t.Cleanup(func() { _ = streams.Shutdown(context.Background()) })

	ddb := &dynamodb.DynamoDBProvider{}
	require.NoError(t, ddb.Init(plugin.PluginConfig{DataDir: dir}))
	t.Cleanup(func() { _ = ddb.Shutdown(context.Background()) })

	createResp := doJSON(t, ddb, "DynamoDB_20120810.CreateTable", "CreateTable", `{
		"TableName": "KOnly",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}
	}`)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))
	arn := createOut["TableDescription"].(map[string]any)["LatestStreamArn"].(string)

	doJSON(t, ddb, "DynamoDB_20120810.PutItem", "PutItem", `{
		"TableName": "KOnly",
		"Item": {"pk": {"S": "x1"}, "name": {"S": "visible?"}}
	}`)

	iterResp := doJSON(t, streams, "DynamoDBStreams_20120810.GetShardIterator", "GetShardIterator", `{
		"StreamArn": "`+arn+`",
		"ShardId": "`+dynamodbstreams.DefaultShardID()+`",
		"ShardIteratorType": "TRIM_HORIZON"
	}`)
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body, &iterOut))
	iter := iterOut["ShardIterator"].(string)

	recResp := doJSON(t, streams, "DynamoDBStreams_20120810.GetRecords", "GetRecords",
		`{"ShardIterator":"`+iter+`"}`)
	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body, &recOut))
	records := recOut["Records"].([]any)
	require.Len(t, records, 1)
	dyn := records[0].(map[string]any)["dynamodb"].(map[string]any)
	_, hasNew := dyn["NewImage"]
	_, hasOld := dyn["OldImage"]
	assert.False(t, hasNew, "KEYS_ONLY must not include NewImage")
	assert.False(t, hasOld, "KEYS_ONLY must not include OldImage")
	assert.Contains(t, dyn, "Keys")
}

// TestStreamsTagsLifecycle covers the new tag operations end-to-end.
func TestStreamsTagsLifecycle(t *testing.T) {
	p := &dynamodbstreams.Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	// Seed a stream via the DynamoDB handler so the tag APIs have a valid ARN.
	ddb := &dynamodb.DynamoDBProvider{}
	require.NoError(t, ddb.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = ddb.Shutdown(context.Background()) })

	createResp := doJSON(t, ddb, "DynamoDB_20120810.CreateTable", "CreateTable", `{
		"TableName": "Tagged",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}
	}`)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))
	arn := createOut["TableDescription"].(map[string]any)["LatestStreamArn"].(string)

	addResp := doJSON(t, p, "DynamoDBStreams_20120810.AddTagsToStream", "AddTagsToStream", `{
		"StreamArn": "`+arn+`",
		"Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "infra"}]
	}`)
	assert.Equal(t, 200, addResp.StatusCode)

	listResp := doJSON(t, p, "DynamoDBStreams_20120810.ListTagsOfStream", "ListTagsOfStream",
		`{"StreamArn":"`+arn+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listOut))
	tags, _ := listOut["Tags"].([]any)
	assert.Len(t, tags, 2)

	rmResp := doJSON(t, p, "DynamoDBStreams_20120810.RemoveTagsFromStream", "RemoveTagsFromStream", `{
		"StreamArn": "`+arn+`",
		"TagKeys": ["env"]
	}`)
	assert.Equal(t, 200, rmResp.StatusCode)

	listResp2 := doJSON(t, p, "DynamoDBStreams_20120810.ListTagsOfStream", "ListTagsOfStream",
		`{"StreamArn":"`+arn+`"}`)
	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listResp2.Body, &listOut2))
	tags2, _ := listOut2["Tags"].([]any)
	assert.Len(t, tags2, 1)
}
