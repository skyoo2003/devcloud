// SPDX-License-Identifier: Apache-2.0

// internal/services/firehose/provider_test.go
package firehose

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
	t.Cleanup(func() { p.Shutdown(context.Background()) })
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

func TestFirehose_CreateAndDescribe(t *testing.T) {
	p := newTestProvider(t)
	resp := callJSON(t, p, "Firehose_20150804.CreateDeliveryStream",
		`{"DeliveryStreamName":"test-stream"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Contains(t, m["DeliveryStreamARN"], "arn:aws:firehose:")

	desc := callJSON(t, p, "Firehose_20150804.DescribeDeliveryStream",
		`{"DeliveryStreamName":"test-stream"}`)
	assert.Equal(t, 200, desc.StatusCode)
	dm := parseJSON(t, desc)
	info := dm["DeliveryStreamDescription"].(map[string]any)
	assert.Equal(t, "test-stream", info["DeliveryStreamName"])
	assert.Equal(t, "ACTIVE", info["DeliveryStreamStatus"])
}

func TestFirehose_ListStreams(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"s1"}`)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"s2"}`)

	resp := callJSON(t, p, "Firehose_20150804.ListDeliveryStreams", `{}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	names := m["DeliveryStreamNames"].([]any)
	assert.Len(t, names, 2)
}

func TestFirehose_DeleteStream(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"del-me"}`)

	resp := callJSON(t, p, "Firehose_20150804.DeleteDeliveryStream", `{"DeliveryStreamName":"del-me"}`)
	assert.Equal(t, 200, resp.StatusCode)

	desc := callJSON(t, p, "Firehose_20150804.DescribeDeliveryStream", `{"DeliveryStreamName":"del-me"}`)
	assert.Equal(t, 400, desc.StatusCode)
}

func TestFirehose_PutRecord(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"s1"}`)

	resp := callJSON(t, p, "Firehose_20150804.PutRecord",
		`{"DeliveryStreamName":"s1","Record":{"Data":"dGVzdA=="}}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.NotEmpty(t, m["RecordId"])
}

func TestFirehose_PutRecordBatch(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"s1"}`)

	resp := callJSON(t, p, "Firehose_20150804.PutRecordBatch",
		`{"DeliveryStreamName":"s1","Records":[{"Data":"YQ=="},{"Data":"Yg=="}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, float64(0), m["FailedPutCount"])
	results := m["RequestResponses"].([]any)
	assert.Len(t, results, 2)
}

func TestFirehose_Tags(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream",
		`{"DeliveryStreamName":"s1","Tags":[{"Key":"env","Value":"prod"}]}`)

	resp := callJSON(t, p, "Firehose_20150804.ListTagsForDeliveryStream",
		`{"DeliveryStreamName":"s1"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	tags := m["Tags"].([]any)
	assert.Len(t, tags, 1)

	callJSON(t, p, "Firehose_20150804.UntagDeliveryStream",
		`{"DeliveryStreamName":"s1","TagKeys":["env"]}`)
	resp2 := callJSON(t, p, "Firehose_20150804.ListTagsForDeliveryStream",
		`{"DeliveryStreamName":"s1"}`)
	m2 := parseJSON(t, resp2)
	assert.Empty(t, m2["Tags"])
}

func TestFirehose_Encryption(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"s1"}`)

	callJSON(t, p, "Firehose_20150804.StartDeliveryStreamEncryption", `{"DeliveryStreamName":"s1"}`)
	desc := parseJSON(t, callJSON(t, p, "Firehose_20150804.DescribeDeliveryStream", `{"DeliveryStreamName":"s1"}`))
	info := desc["DeliveryStreamDescription"].(map[string]any)
	enc := info["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	assert.Equal(t, "ENABLED", enc["Status"])

	callJSON(t, p, "Firehose_20150804.StopDeliveryStreamEncryption", `{"DeliveryStreamName":"s1"}`)
	desc2 := parseJSON(t, callJSON(t, p, "Firehose_20150804.DescribeDeliveryStream", `{"DeliveryStreamName":"s1"}`))
	info2 := desc2["DeliveryStreamDescription"].(map[string]any)
	enc2 := info2["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	assert.Equal(t, "DISABLED", enc2["Status"])
}

func TestFirehose_KinesisSource(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"ks-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.AddKinesisSource",
		`{"DeliveryStreamName":"ks-stream","KinesisStreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/source","RoleARN":"arn:aws:iam::000000000000:role/firehose"}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp2 := callJSON(t, p, "Firehose_20150804.GetKinesisSources", `{"DeliveryStreamName":"ks-stream"}`)
	m := parseJSON(t, resp2)
	sources := m["KinesisStreamSources"].([]any)
	assert.Len(t, sources, 1)
}

func TestFirehose_RedshiftDestination(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"rs-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.PutRedshiftDestination",
		`{"DeliveryStreamName":"rs-stream","RedshiftDestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/firehose","ClusterJDBCURL":"jdbc:redshift://my-cluster:5439/dev","DatabaseName":"dev","Username":"admin"}}`)
	assert.Equal(t, 200, resp.StatusCode)

	resp2 := callJSON(t, p, "Firehose_20150804.GetRedshiftDestination", `{"DeliveryStreamName":"rs-stream"}`)
	assert.Equal(t, 200, resp2.StatusCode)
	m := parseJSON(t, resp2)
	desc := m["RedshiftDestinationDescription"].(map[string]any)
	assert.Equal(t, "dev", desc["DatabaseName"])
}

func TestFirehose_ElasticsearchDestination(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"es-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.PutElasticsearchDestination",
		`{"DeliveryStreamName":"es-stream","ElasticsearchDestinationConfiguration":{"DomainARN":"arn:aws:es:us-east-1:000000000000:domain/test","IndexName":"logs","TypeName":"_doc","RoleARN":"arn:aws:iam::000000000000:role/firehose"}}`)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestFirehose_HTTPEndpoint(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"http-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.PutHttpEndpointDestination",
		`{"DeliveryStreamName":"http-stream","HttpEndpointDestinationConfiguration":{"EndpointConfiguration":{"Url":"https://example.com","Name":"ep1","AccessKey":"abc"},"RoleARN":"arn:aws:iam::000000000000:role/firehose"}}`)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestFirehose_Lifecycle(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"lc-stream"}`)

	callJSON(t, p, "Firehose_20150804.StopDeliveryStream", `{"DeliveryStreamName":"lc-stream"}`)
	callJSON(t, p, "Firehose_20150804.StartDeliveryStream", `{"DeliveryStreamName":"lc-stream"}`)
}

func TestFirehose_Metrics(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"m-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.GetDeliveryStreamMetrics", `{"DeliveryStreamName":"m-stream"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "m-stream", m["DeliveryStreamName"])
}

func TestFirehose_Health(t *testing.T) {
	p := newTestProvider(t)
	callJSON(t, p, "Firehose_20150804.CreateDeliveryStream", `{"DeliveryStreamName":"h-stream"}`)

	resp := callJSON(t, p, "Firehose_20150804.DescribeDeliveryStreamHealth", `{"DeliveryStreamName":"h-stream"}`)
	assert.Equal(t, 200, resp.StatusCode)
	m := parseJSON(t, resp)
	assert.Equal(t, "HEALTHY", m["Health"])
}
