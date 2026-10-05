// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"encoding/json"
	"github.com/skyoo2003/devcloud/internal/gateway"
	"github.com/skyoo2003/devcloud/internal/plugin"
	streams "github.com/skyoo2003/devcloud/internal/services/dynamodbstreams"
	"github.com/skyoo2003/devcloud/internal/services/sqs"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func serverPort(t *testing.T, s *httptest.Server) int {
	u, err := url.Parse(s.URL)
	require.NoError(t, err)
	n, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	return n
}
func TestMappingEnabledState(t *testing.T) {
	s := newTestLambdaStore(t)
	m := &EventSourceMapping{UUID: "disabled", AccountID: defaultAccountID, Enabled: false}
	require.NoError(t, s.CreateEventSourceMapping(m))
	got, err := s.GetEventSourceMapping(m.UUID)
	require.NoError(t, err)
	require.Equal(t, "Disabled", got.State)
	enabled := true
	got, err = s.UpdateEventSourceMapping(m.UUID, 0, &enabled)
	require.NoError(t, err)
	require.Equal(t, "Enabled", got.State)
}
func TestSQSFunctionErrorPreservesMessages(t *testing.T) {
	deleted := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			w.Header().Set("X-Amz-Function-Error", "Unhandled")
			_, _ = w.Write([]byte(`{"errorType":"ValueError"}`))
			return
		}
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.GetQueueUrl":
			_, _ = w.Write([]byte(`{"QueueUrl":"http://localhost/000000000000/source"}`))
		case "AmazonSQS.ReceiveMessage":
			_, _ = w.Write([]byte(`{"Messages":[{"MessageId":"1","Body":"payload","ReceiptHandle":"receipt"}]}`))
		case "AmazonSQS.DeleteMessage":
			deleted++
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()
	p := NewEventSourcePoller(newTestLambdaStore(t), serverPort(t, srv))
	p.pollSQS(context.Background(), EventSourceMapping{FunctionName: "function", EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:source"})
	require.Zero(t, deleted)
}
func TestStreamCheckpointPreventsReplay(t *testing.T) {
	reads := 0
	invokes := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			invokes++
			_, _ = w.Write([]byte(`{}`))
			return
		}
		switch r.Header.Get("X-Amz-Target") {
		case "DynamoDBStreams_20120810.DescribeStream":
			_, _ = w.Write([]byte(`{"StreamDescription":{"Shards":[{"ShardId":"one"}]}}`))
		case "DynamoDBStreams_20120810.GetShardIterator":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			if reads > 0 {
				require.Equal(t, "AFTER_SEQUENCE_NUMBER", body["ShardIteratorType"])
				require.Equal(t, "1", body["SequenceNumber"])
			}
			_, _ = w.Write([]byte(`{"ShardIterator":"iterator"}`))
		case "DynamoDBStreams_20120810.GetRecords":
			reads++
			if reads == 1 {
				_, _ = w.Write([]byte(`{"Records":[{"eventID":"1","dynamodb":{"SequenceNumber":"1"}}],"NextShardIterator":"next"}`))
			} else {
				_, _ = w.Write([]byte(`{"Records":[],"NextShardIterator":"next2"}`))
			}
		}
	}))
	defer srv.Close()
	s := newTestLambdaStore(t)
	m := EventSourceMapping{UUID: "stream", FunctionName: "function", AccountID: defaultAccountID, EventSourceARN: "arn:aws:dynamodb:us-east-1:000000000000:table/table/stream/1", Enabled: true}
	require.NoError(t, s.CreateEventSourceMapping(&m))
	p := NewEventSourcePoller(s, serverPort(t, srv))
	p.pollDynamoDBStream(context.Background(), m)
	p = NewEventSourcePoller(s, serverPort(t, srv))
	p.pollDynamoDBStream(context.Background(), m)
	require.Equal(t, 1, invokes)
}

func TestDeleteMappingClearsCheckpoints(t *testing.T) {
	s := newTestLambdaStore(t)
	m := EventSourceMapping{UUID: "checkpoint", AccountID: defaultAccountID, Enabled: true}
	require.NoError(t, s.CreateEventSourceMapping(&m))
	require.NoError(t, s.SaveStreamCheckpoint(m.UUID, "one", StreamCheckpoint{Generation: "g1", SequenceNumber: "12"}))
	saved, err := s.GetStreamCheckpoint(m.UUID, "one")
	require.NoError(t, err)
	require.Equal(t, "12", saved.SequenceNumber)
	require.NoError(t, s.DeleteEventSourceMapping(m.UUID))
	saved, err = s.GetStreamCheckpoint(m.UUID, "one")
	require.NoError(t, err)
	require.Nil(t, saved)
	require.Error(t, s.SaveStreamCheckpoint(m.UUID, "one", StreamCheckpoint{SequenceNumber: "13"}))
}

func TestSQSPollerRoutesToLambdaGateway(t *testing.T) {
	registry := plugin.NewRegistry()
	var invoked int
	registry.Register("sqs", func() plugin.ServicePlugin { return &sqs.SQSProvider{} })
	queueSvc, err := registry.Init("sqs", plugin.PluginConfig{})
	require.NoError(t, err)
	registry.Register("lambda", func() plugin.ServicePlugin { return &LambdaProvider{} })
	lambdaSvc, err := registry.Init("lambda", plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, registry.ShutdownAll(context.Background())) }()
	lp := lambdaSvc.(*LambdaProvider)
	_, err = lp.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	lp.runtime = testInvoker{func(context.Context, *FunctionInfo, []byte) (*InvokeResult, error) {
		invoked++
		return &InvokeResult{StatusCode: 200, Payload: []byte(`{}`)}, nil
	}}
	for _, body := range []string{`{"QueueName":"source"}`, `{"QueueUrl":"http://localhost/000000000000/source","MessageBody":"deliver"}`} {
		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		op := "CreateQueue"
		if strings.Contains(body, "MessageBody") {
			op = "SendMessage"
		}
		resp, err := queueSvc.HandleRequest(context.Background(), op, req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
	}
	server := httptest.NewServer(gateway.NewServiceRouter(registry, nil))
	defer server.Close()
	poller := NewEventSourcePoller(lp.store, serverPort(t, server))
	poller.pollSQS(context.Background(), EventSourceMapping{FunctionName: "immutable", EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:source"})
	require.Equal(t, 1, invoked)
}

func TestSQSPollerHonorsQueueVisibility(t *testing.T) {
	var visibility any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("X-Amz-Target") {
		case "AmazonSQS.GetQueueUrl":
			_, _ = w.Write([]byte(`{"QueueUrl":"http://localhost/000000000000/source"}`))
		case "AmazonSQS.GetQueueAttributes":
			_, _ = w.Write([]byte(`{"Attributes":{"VisibilityTimeout":"1"}}`))
		case "AmazonSQS.ReceiveMessage":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			visibility = body["VisibilityTimeout"]
			_, _ = w.Write([]byte(`{"Messages":[]}`))
		}
	}))
	defer srv.Close()
	p := NewEventSourcePoller(newTestLambdaStore(t), serverPort(t, srv))
	p.pollSQS(context.Background(), EventSourceMapping{FunctionName: "function", EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:source"})
	require.Equal(t, float64(1), visibility)
}

func TestLatestUnavailableDoesNotLeaveMapping(t *testing.T) {
	p := newTestLambdaProvider(t)
	resp, err := p.createEventSourceMapping(httptest.NewRequest("POST", "/", strings.NewReader(`{"FunctionName":"immutable","EventSourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/1","StartingPosition":"LATEST"}`)))
	require.NoError(t, err)
	require.Equal(t, 503, resp.StatusCode)
	mappings, err := p.store.ListEventSourceMappings(defaultAccountID, "")
	require.NoError(t, err)
	require.Empty(t, mappings)
}
func TestDeleteMappingClearsPollerBatches(t *testing.T) {
	p := NewEventSourcePoller(newTestLambdaStore(t), 1)
	p.states["deleted/one"] = &streamPollState{pending: []byte(`{}`)}
	p.poll(context.Background())
	require.Empty(t, p.states)
}

type testBoundaryProvider struct {
	*streams.Provider
	generation *string
}

func (p *testBoundaryProvider) ShardBoundary(string, string) (string, string, error) {
	return *p.generation, "10", nil
}
func TestStreamsRetryConsumedBatchAndAllShards(t *testing.T) {
	old := plugin.DefaultRegistry
	reg := plugin.NewRegistry()
	generation := "g1"
	reg.Register("dynamodbstreams", func() plugin.ServicePlugin {
		return &testBoundaryProvider{Provider: &streams.Provider{}, generation: &generation}
	})
	_, err := reg.Init("dynamodbstreams", plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	plugin.DefaultRegistry = reg
	defer func() { require.NoError(t, reg.ShutdownAll(context.Background())); plugin.DefaultRegistry = old }()
	attempts := map[string]int{}
	reads := map[string]int{}
	failed := true
	positions := map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		if strings.HasSuffix(r.URL.Path, "/invocations") {
			records := body["Records"].([]any)
			id := records[0].(map[string]any)["eventID"].(string)
			attempts[id]++
			if id == "one" && failed {
				failed = false
				w.Header().Set("X-Amz-Function-Error", "Unhandled")
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		switch r.Header.Get("X-Amz-Target") {
		case "DynamoDBStreams_20120810.DescribeStream":
			_, _ = w.Write([]byte(`{"StreamDescription":{"Shards":[{"ShardId":"one"},{"ShardId":"two"}]}}`))
		case "DynamoDBStreams_20120810.GetShardIterator":
			shard := body["ShardId"].(string)
			positions[shard] = body
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ShardIterator": shard}))
		case "DynamoDBStreams_20120810.GetRecords":
			iter := body["ShardIterator"].(string)
			reads[iter]++
			if iter == "one" || iter == "two" {
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"Records": []any{map[string]any{"eventID": iter, "dynamodb": map[string]any{"SequenceNumber": "12"}}}, "NextShardIterator": "next-" + iter}))
			} else {
				_, _ = w.Write([]byte(`{"Records":[],"NextShardIterator":"expired"}`))
			}
		}
	}))
	defer server.Close()
	store := newTestLambdaStore(t)
	m := EventSourceMapping{UUID: "retry", FunctionName: "immutable", EventSourceARN: "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/1", AccountID: defaultAccountID, Enabled: true}
	require.NoError(t, store.CreateEventSourceMapping(&m))
	poller := NewEventSourcePoller(store, serverPort(t, server))
	poller.pollDynamoDBStream(context.Background(), m)
	checkpoint, err := store.GetStreamCheckpoint(m.UUID, "one")
	require.NoError(t, err)
	require.Nil(t, checkpoint)
	checkpoint, err = store.GetStreamCheckpoint(m.UUID, "two")
	require.NoError(t, err)
	require.Equal(t, "12", checkpoint.SequenceNumber)
	poller.pollDynamoDBStream(context.Background(), m)
	require.Equal(t, 2, attempts["one"])
	require.Equal(t, 1, attempts["two"])
	require.Equal(t, 1, reads["one"])
	checkpoint, err = store.GetStreamCheckpoint(m.UUID, "one")
	require.NoError(t, err)
	require.Equal(t, "g1", checkpoint.Generation)
	poller = NewEventSourcePoller(store, serverPort(t, server))
	poller.pollDynamoDBStream(context.Background(), m)
	require.Equal(t, "AFTER_SEQUENCE_NUMBER", positions["one"].(map[string]any)["ShardIteratorType"])
	generation = "g2"
	poller.pollDynamoDBStream(context.Background(), m)
	require.Equal(t, "TRIM_HORIZON", positions["one"].(map[string]any)["ShardIteratorType"])
	latest := m
	latest.UUID = "latest"
	latest.StartingPosition = "LATEST"
	require.NoError(t, store.CreateEventSourceMapping(&latest))
	require.NoError(t, poller.initializeLatestCheckpoints(context.Background(), latest))
	checkpoint, err = store.GetStreamCheckpoint(latest.UUID, "one")
	require.NoError(t, err)
	require.Equal(t, "10", checkpoint.SequenceNumber)
	poller.pollDynamoDBStream(context.Background(), latest)
	require.Equal(t, "10", positions["one"].(map[string]any)["SequenceNumber"])
}

func TestLatestEmptyBoundaryKeepsPostCreationRecord(t *testing.T) {
	old := plugin.DefaultRegistry
	reg := plugin.NewRegistry()
	plugin.DefaultRegistry = reg
	defer func() { require.NoError(t, reg.ShutdownAll(context.Background())); plugin.DefaultRegistry = old }()
	reg.Register("dynamodbstreams", func() plugin.ServicePlugin { return &streams.Provider{} })
	_, err := reg.Init("dynamodbstreams", plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	source := streams.GetGlobalStore()
	arn := "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/1"
	_, err = source.CreateStream(arn, "t", "1", "NEW_IMAGE")
	require.NoError(t, err)
	reg.Register("lambda", func() plugin.ServicePlugin { return &LambdaProvider{} })
	svc, err := reg.Init("lambda", plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	lp := svc.(*LambdaProvider)
	_, err = lp.store.CreateFunction(regressionFunction(), handlerZIP(t, "pass"))
	require.NoError(t, err)
	invoked := 0
	lp.runtime = testInvoker{func(context.Context, *FunctionInfo, []byte) (*InvokeResult, error) {
		invoked++
		return &InvokeResult{StatusCode: 200, Payload: []byte(`{}`)}, nil
	}}
	server := httptest.NewServer(gateway.NewServiceRouter(reg, nil))
	defer server.Close()
	poller := NewEventSourcePoller(lp.store, serverPort(t, server))
	m := EventSourceMapping{UUID: "empty-latest", FunctionName: "immutable", EventSourceARN: arn, StartingPosition: "LATEST", Enabled: true, AccountID: defaultAccountID}
	require.NoError(t, lp.store.CreateEventSourceMapping(&m))
	require.NoError(t, poller.initializeLatestCheckpoints(context.Background(), m))
	require.NoError(t, source.PublishRecord("t", "INSERT", map[string]any{"id": "after-mapping"}, nil, nil))
	poller.pollDynamoDBStream(context.Background(), m)
	require.Equal(t, 1, invoked)
}
