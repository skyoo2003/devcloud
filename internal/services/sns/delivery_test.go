// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/services/sqs"
	"github.com/stretchr/testify/require"
)

const deliveryMessage = "한글 + payload & key=value %20\nsecond line"

func deliverySQS(t *testing.T) plugin.ServicePlugin {
	t.Helper()
	old := plugin.DefaultRegistry
	plugin.DefaultRegistry = plugin.NewRegistry()
	plugin.DefaultRegistry.Register("sqs", func() plugin.ServicePlugin { return &sqs.SQSProvider{} })
	svc, err := plugin.DefaultRegistry.Init("sqs", plugin.PluginConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { plugin.DefaultRegistry = old })
	return svc
}

func sqsQuery(t *testing.T, svc plugin.ServicePlugin, values url.Values) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := svc.HandleRequest(context.Background(), values.Get("Action"), req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, string(resp.Body))
	return resp
}

func deliveryQueue(t *testing.T, svc plugin.ServicePlugin) string {
	var result struct {
		URL string `xml:"CreateQueueResult>QueueUrl"`
	}
	resp := sqsQuery(t, svc, url.Values{"Action": {"CreateQueue"}, "QueueName": {"phase1-sns-arn"}})
	require.NoError(t, xml.Unmarshal(resp.Body, &result))
	return result.URL
}

func receivedBodies(t *testing.T, svc plugin.ServicePlugin, queueURL string) []string {
	var result struct {
		Messages []struct {
			Body string `xml:"Body"`
		} `xml:"ReceiveMessageResult>Message"`
	}
	resp := sqsQuery(t, svc, url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {queueURL}})
	require.NoError(t, xml.Unmarshal(resp.Body, &result))
	bodies := make([]string, 0, len(result.Messages))
	for _, m := range result.Messages {
		bodies = append(bodies, m.Body)
	}
	return bodies
}

func TestSNSFanoutARNResolvesQueueURL(t *testing.T) {
	svc := deliverySQS(t)
	queue := deliveryQueue(t, svc)
	p := newTestProvider(t)
	require.NoError(t, p.fanoutToSQS(context.Background(), "arn:aws:sqs:us-east-1:000000000000:phase1-sns-arn", deliveryMessage))
	require.Equal(t, []string{deliveryMessage}, receivedBodies(t, svc, queue))
}

func TestSNSFanoutPreservesMessage(t *testing.T) {
	svc := deliverySQS(t)
	queue := deliveryQueue(t, svc)
	p := newTestProvider(t)
	require.NoError(t, p.fanoutToSQS(context.Background(), queue, deliveryMessage))
	require.Equal(t, []string{deliveryMessage}, receivedBodies(t, svc, queue))
}

func TestSNSFanoutURLSubscription(t *testing.T) {
	svc := deliverySQS(t)
	queue := deliveryQueue(t, svc)
	p := newTestProvider(t)
	require.NoError(t, p.fanoutToSQS(context.Background(), queue, "hello"))
	require.Equal(t, []string{"hello"}, receivedBodies(t, svc, queue))
}

func TestSNSFanoutContinuesAfterFailure(t *testing.T) {
	svc := deliverySQS(t)
	queue := deliveryQueue(t, svc)
	p := newTestProvider(t)
	topic := "arn:aws:sns:us-east-1:000000000000:fanout"
	require.Equal(t, 200, handle(t, p, "Action=CreateTopic&Name=fanout").StatusCode)
	_, err := p.store.Subscribe(topic+":a", topic, "sqs", "arn:aws:sqs:us-east-1:000000000000:missing", defaultAccountID)
	require.NoError(t, err)
	_, err = p.store.Subscribe(topic+":b", topic, "sqs", queue, defaultAccountID)
	require.NoError(t, err)
	require.Equal(t, 200, handle(t, p, url.Values{"Action": {"Publish"}, "TopicArn": {topic}, "Message": {deliveryMessage}}.Encode()).StatusCode)
	require.Equal(t, []string{deliveryMessage}, receivedBodies(t, svc, queue))
}

func TestSNSFanoutInvalidEndpoint(t *testing.T) {
	svc := deliverySQS(t)
	queue := deliveryQueue(t, svc)
	p := newTestProvider(t)
	for _, endpoint := range []string{"", "not-a-url", "arn:aws:sqs:us-east-1:111111111111:phase1-sns-arn", "arn:aws:sns:us-east-1:000000000000:phase1-sns-arn", "arn:aws:sqs::000000000000:phase1-sns-arn"} {
		require.Error(t, p.fanoutToSQS(context.Background(), endpoint, "forbidden"))
	}
	require.Empty(t, receivedBodies(t, svc, queue))
}
func TestSNSFanoutMissingQueue(t *testing.T) {
	deliverySQS(t)
	p := newTestProvider(t)
	require.Error(t, p.fanoutToSQS(context.Background(), "arn:aws:sqs:us-east-1:000000000000:missing", "hello"))
	plugin.DefaultRegistry = plugin.NewRegistry()
	require.Error(t, p.fanoutToSQS(context.Background(), "http://localhost:4747/000000000000/missing", "hello"))
}
