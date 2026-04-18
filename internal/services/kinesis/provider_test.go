// SPDX-License-Identifier: Apache-2.0

package kinesis

import (
	"context"
	"encoding/base64"
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
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
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

func TestCreateAndDescribeStream(t *testing.T) {
	p := newTestProvider(t)

	resp := callJSON(t, p, "Kinesis_20131202.CreateStream",
		`{"StreamName":"test-stream","ShardCount":2}`)
	assert.Equal(t, 200, resp.StatusCode)

	desc := callJSON(t, p, "Kinesis_20131202.DescribeStream",
		`{"StreamName":"test-stream"}`)
	assert.Equal(t, 200, desc.StatusCode)
	m := parseJSON(t, desc)
	sd := m["StreamDescription"].(map[string]any)
	assert.Equal(t, "test-stream", sd["StreamName"])
	assert.Equal(t, "ACTIVE", sd["StreamStatus"])
	assert.Contains(t, sd["StreamARN"], "arn:aws:kinesis:")

	shards := sd["Shards"].([]any)
	assert.Len(t, shards, 2)
}

func TestListStreams(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"s1"}`)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"s2"}`)

	resp := callJSON(t, p, "Kinesis_20131202.ListStreams", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	names := m["StreamNames"].([]any)
	assert.Len(t, names, 2)
}

func TestDeleteStream(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"del-me"}`)

	resp := callJSON(t, p, "Kinesis_20131202.DeleteStream", `{"StreamName":"del-me"}`)
	assert.Equal(t, 200, resp.StatusCode)

	desc := callJSON(t, p, "Kinesis_20131202.DescribeStream", `{"StreamName":"del-me"}`)
	assert.Equal(t, 400, desc.StatusCode)
}

func TestPutAndGetRecords(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"data-stream","ShardCount":1}`)

	// Put a record
	data := base64.StdEncoding.EncodeToString([]byte("hello-kinesis"))
	putResp := callJSON(t, p, "Kinesis_20131202.PutRecord",
		`{"StreamName":"data-stream","PartitionKey":"pk1","Data":"`+data+`"}`)
	assert.Equal(t, 200, putResp.StatusCode)
	putM := parseJSON(t, putResp)
	assert.NotEmpty(t, putM["ShardId"])
	assert.NotEmpty(t, putM["SequenceNumber"])
	shardID := putM["ShardId"].(string)

	// Get shard iterator (TRIM_HORIZON)
	iterResp := callJSON(t, p, "Kinesis_20131202.GetShardIterator",
		`{"StreamName":"data-stream","ShardId":"`+shardID+`","ShardIteratorType":"TRIM_HORIZON"}`)
	assert.Equal(t, 200, iterResp.StatusCode)
	iterM := parseJSON(t, iterResp)
	iterID := iterM["ShardIterator"].(string)

	// Get records
	getResp := callJSON(t, p, "Kinesis_20131202.GetRecords",
		`{"ShardIterator":"`+iterID+`"}`)
	assert.Equal(t, 200, getResp.StatusCode)
	getM := parseJSON(t, getResp)
	records := getM["Records"].([]any)
	require.Len(t, records, 1)

	rec := records[0].(map[string]any)
	assert.Equal(t, "pk1", rec["PartitionKey"])
	decoded, _ := base64.StdEncoding.DecodeString(rec["Data"].(string))
	assert.Equal(t, "hello-kinesis", string(decoded))
	assert.NotEmpty(t, rec["SequenceNumber"])
	assert.NotEmpty(t, getM["NextShardIterator"])
}

func TestShardIteratorTypes(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"iter-stream","ShardCount":1}`)

	data := base64.StdEncoding.EncodeToString([]byte("rec1"))
	putM := parseJSON(t, callJSON(t, p, "Kinesis_20131202.PutRecord",
		`{"StreamName":"iter-stream","PartitionKey":"pk1","Data":"`+data+`"}`))
	shardID := putM["ShardId"].(string)

	// LATEST should return no records (we're at the end)
	latestIter := parseJSON(t, callJSON(t, p, "Kinesis_20131202.GetShardIterator",
		`{"StreamName":"iter-stream","ShardId":"`+shardID+`","ShardIteratorType":"LATEST"}`))
	latestRecords := parseJSON(t, callJSON(t, p, "Kinesis_20131202.GetRecords",
		`{"ShardIterator":"`+latestIter["ShardIterator"].(string)+`"}`))
	assert.Empty(t, latestRecords["Records"])

	// TRIM_HORIZON should return the record
	horizonIter := parseJSON(t, callJSON(t, p, "Kinesis_20131202.GetShardIterator",
		`{"StreamName":"iter-stream","ShardId":"`+shardID+`","ShardIteratorType":"TRIM_HORIZON"}`))
	horizonRecords := parseJSON(t, callJSON(t, p, "Kinesis_20131202.GetRecords",
		`{"ShardIterator":"`+horizonIter["ShardIterator"].(string)+`"}`))
	recs := horizonRecords["Records"].([]any)
	assert.Len(t, recs, 1)
}

func TestPutRecordsBatch(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"batch-stream","ShardCount":1}`)

	d1 := base64.StdEncoding.EncodeToString([]byte("r1"))
	d2 := base64.StdEncoding.EncodeToString([]byte("r2"))
	resp := callJSON(t, p, "Kinesis_20131202.PutRecords",
		`{"StreamName":"batch-stream","Records":[`+
			`{"PartitionKey":"pk1","Data":"`+d1+`"},`+
			`{"PartitionKey":"pk2","Data":"`+d2+`"}`+
			`]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, float64(0), m["FailedRecordCount"])
	results := m["Records"].([]any)
	assert.Len(t, results, 2)
	for _, r := range results {
		rec := r.(map[string]any)
		assert.NotEmpty(t, rec["ShardId"])
		assert.NotEmpty(t, rec["SequenceNumber"])
	}
}

func TestStreamConsumerCRUD(t *testing.T) {
	p := newTestProvider(t)
	crResp := callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"consumer-stream"}`)
	assert.Equal(t, 200, crResp.StatusCode)

	// Get stream ARN
	desc := parseJSON(t, callJSON(t, p, "Kinesis_20131202.DescribeStreamSummary",
		`{"StreamName":"consumer-stream"}`))
	streamDesc := desc["StreamDescriptionSummary"].(map[string]any)
	streamARN := streamDesc["StreamARN"].(string)

	// Register consumer
	regResp := callJSON(t, p, "Kinesis_20131202.RegisterStreamConsumer",
		`{"StreamARN":"`+streamARN+`","ConsumerName":"my-consumer"}`)
	assert.Equal(t, 200, regResp.StatusCode)
	regM := parseJSON(t, regResp)
	consumer := regM["Consumer"].(map[string]any)
	assert.Equal(t, "my-consumer", consumer["ConsumerName"])
	consumerARN := consumer["ConsumerARN"].(string)

	// List consumers
	listResp := callJSON(t, p, "Kinesis_20131202.ListStreamConsumers",
		`{"StreamARN":"`+streamARN+`"}`)
	assert.Equal(t, 200, listResp.StatusCode)
	listM := parseJSON(t, listResp)
	consumers := listM["Consumers"].([]any)
	assert.Len(t, consumers, 1)

	// Deregister
	deregResp := callJSON(t, p, "Kinesis_20131202.DeregisterStreamConsumer",
		`{"ConsumerARN":"`+consumerARN+`"}`)
	assert.Equal(t, 200, deregResp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Kinesis_20131202.CreateStream", `{"StreamName":"tag-stream"}`)

	callJSON(t, p, "Kinesis_20131202.AddTagsToStream",
		`{"StreamName":"tag-stream","Tags":{"env":"prod","team":"data"}}`)

	resp := callJSON(t, p, "Kinesis_20131202.ListTagsForStream",
		`{"StreamName":"tag-stream"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	tags := m["Tags"].([]any)
	assert.Len(t, tags, 2)

	callJSON(t, p, "Kinesis_20131202.RemoveTagsFromStream",
		`{"StreamName":"tag-stream","TagKeys":["team"]}`)

	resp2 := callJSON(t, p, "Kinesis_20131202.ListTagsForStream",
		`{"StreamName":"tag-stream"}`)
	m2 := parseJSON(t, resp2)
	tags2 := m2["Tags"].([]any)
	assert.Len(t, tags2, 1)
}
