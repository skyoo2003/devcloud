// SPDX-License-Identifier: Apache-2.0

// internal/services/dynamodbstreams/provider_test.go
package dynamodbstreams

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

func TestListStreams(t *testing.T) {
	p := newTestProvider(t)

	// Create two streams directly via store.
	_, err := p.createStreamForTable("orders")
	require.NoError(t, err)
	_, err = p.createStreamForTable("products")
	require.NoError(t, err)

	resp := callJSON(t, p, "DynamoDBStreams_20120810.ListStreams", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)

	m := parseJSON(t, resp)
	streams, ok := m["Streams"].([]any)
	require.True(t, ok)
	assert.Len(t, streams, 2)

	// Filter by table name.
	resp2 := callJSON(t, p, "DynamoDBStreams_20120810.ListStreams", `{"TableName":"orders"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m2 := parseJSON(t, resp2)
	filtered, ok := m2["Streams"].([]any)
	require.True(t, ok)
	assert.Len(t, filtered, 1)
	entry := filtered[0].(map[string]any)
	assert.Equal(t, "orders", entry["TableName"])
}

func TestDescribeStream(t *testing.T) {
	p := newTestProvider(t)

	st, err := p.createStreamForTable("users")
	require.NoError(t, err)

	resp := callJSON(t, p, "DynamoDBStreams_20120810.DescribeStream",
		`{"StreamArn":"`+st.ARN+`"}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)

	m := parseJSON(t, resp)
	desc, ok := m["StreamDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, st.ARN, desc["StreamArn"])
	assert.Equal(t, "users", desc["TableName"])
	assert.Equal(t, "ENABLED", desc["StreamStatus"])
	assert.Equal(t, "NEW_AND_OLD_IMAGES", desc["StreamViewType"])

	shards, ok := desc["Shards"].([]any)
	require.True(t, ok)
	assert.Len(t, shards, 1)
	shard := shards[0].(map[string]any)
	assert.Equal(t, DefaultShardID(), shard["ShardId"])

	// Non-existent stream.
	resp2 := callJSON(t, p, "DynamoDBStreams_20120810.DescribeStream",
		`{"StreamArn":"arn:aws:dynamodb:us-east-1:000000000000:table/nope/stream/2024"}`)
	assert.Equal(t, 400, resp2.StatusCode)
}

func TestGetShardIteratorAndRecords(t *testing.T) {
	p := newTestProvider(t)

	st, err := p.createStreamForTable("events")
	require.NoError(t, err)

	shardID := DefaultShardID()

	// GetShardIterator — TRIM_HORIZON
	resp := callJSON(t, p, "DynamoDBStreams_20120810.GetShardIterator",
		`{"StreamArn":"`+st.ARN+`","ShardId":"`+shardID+`","ShardIteratorType":"TRIM_HORIZON"}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.ContentType)

	m := parseJSON(t, resp)
	iterID, ok := m["ShardIterator"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, iterID)

	// GetRecords — should return empty records (no real DynamoDB feed).
	resp2 := callJSON(t, p, "DynamoDBStreams_20120810.GetRecords",
		`{"ShardIterator":"`+iterID+`"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp2.ContentType)

	m2 := parseJSON(t, resp2)
	records, ok := m2["Records"].([]any)
	require.True(t, ok)
	assert.Empty(t, records)

	nextIter, ok := m2["NextShardIterator"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, nextIter)
	assert.NotEqual(t, iterID, nextIter)

	// Missing ShardIterator.
	resp3 := callJSON(t, p, "DynamoDBStreams_20120810.GetRecords", `{}`)
	assert.Equal(t, 400, resp3.StatusCode)
}
