// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"context"
	"encoding/xml"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/require"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func batchFields(topic string, entries []map[string]string) map[string]string {
	f := map[string]string{"TopicArn": topic}
	for i, e := range entries {
		for k, v := range e {
			f["PublishBatchRequestEntries.member."+strconv.Itoa(i+1)+"."+k] = v
		}
	}
	return f
}
func batchSetup(t *testing.T) (*Provider, plugin.ServicePlugin, string, string) {
	t.Helper()
	svc := deliverySQS(t)
	q := deliveryQueue(t, svc)
	p := newTestProvider(t)
	r := snsCall(t, p, "CreateTopic", map[string]string{"Name": "batch"})
	require.Equal(t, 200, r.StatusCode)
	topic := snsText(t, r, "TopicArn")
	_, e := p.store.Subscribe(topic+":sub", topic, "sqs", q, defaultAccountID)
	require.NoError(t, e)
	return p, svc, topic, q
}

func TestPublishBatchMixedEntriesAndActualSQS(t *testing.T) {
	p, svc, topic, q := batchSetup(t)
	r := snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "good", "Message": deliveryMessage}, {"Id": "bad", "Message": "invalid", "MessageStructure": "json"}, {"Id": "empty", "Message": ""}}))
	require.Equal(t, 200, r.StatusCode)
	var v struct {
		Result struct {
			Success []struct {
				ID        string `xml:"Id"`
				MessageID string `xml:"MessageId"`
			} `xml:"Successful>member"`
			Failed []struct {
				ID          string `xml:"Id"`
				SenderFault bool   `xml:"SenderFault"`
			} `xml:"Failed>member"`
		} `xml:"PublishBatchResult"`
	}
	require.NoError(t, xml.Unmarshal(r.Body, &v))
	require.Len(t, v.Result.Success, 1)
	require.Equal(t, "good", v.Result.Success[0].ID)
	require.NotEmpty(t, v.Result.Success[0].MessageID)
	require.Len(t, v.Result.Failed, 2)
	require.Equal(t, "bad", v.Result.Failed[0].ID)
	require.Equal(t, "empty", v.Result.Failed[1].ID)
	require.True(t, v.Result.Failed[0].SenderFault)
	require.Equal(t, []string{deliveryMessage}, receivedBodies(t, svc, q))
}

func TestPublishBatchStructuralErrorsHaveNoDelivery(t *testing.T) {
	p, svc, topic, q := batchSetup(t)
	cases := [][]map[string]string{nil, {{"Id": "same", "Message": "a"}, {"Id": "same", "Message": "b"}}, {{"Id": "bad space", "Message": "a"}}, {{"Message": "a"}}, {{"Id": "a", "Message": strings.Repeat("x", 262145)}}}
	many := []map[string]string{}
	for i := 0; i < 11; i++ {
		many = append(many, map[string]string{"Id": strconv.Itoa(i), "Message": "x"})
	}
	cases = append(cases, many, []map[string]string{{"Id": "a", "Message": strings.Repeat("x", 131073)}, {"Id": "b", "Message": strings.Repeat("x", 131072)}})
	for _, entries := range cases {
		require.Equal(t, 400, snsCall(t, p, "PublishBatch", batchFields(topic, entries)).StatusCode)
	}
	require.Equal(t, 400, snsCall(t, p, "PublishBatch", map[string]string{"TopicArn": topic, "PublishBatchRequestEntries.member.2.Id": "gap", "PublishBatchRequestEntries.member.2.Message": "x"}).StatusCode)
	require.Empty(t, receivedBodies(t, svc, q))
	require.Equal(t, 400, snsCall(t, p, "PublishBatch", batchFields("missing", []map[string]string{{"Id": "x", "Message": "m"}})).StatusCode)
	require.NoError(t, p.store.Close())
	require.Equal(t, 500, snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "x", "Message": "m"}})).StatusCode)
}

func TestPublishBatchCountsInvalidAttributePayloadBeforeDelivery(t *testing.T) {
	p, svc, topic, q := batchSetup(t)
	entry := map[string]string{"Id": "bad", "Message": "m", "MessageAttributes.entry.1.Name": "tag", "MessageAttributes.entry.1.Value.DataType": "String", "MessageAttributes.entry.1.Value.StringValue": "first", "MessageAttributes.entry.2.Name": "tag", "MessageAttributes.entry.2.Value.DataType": "String", "MessageAttributes.entry.2.Value.StringValue": strings.Repeat("x", 262144)}
	r := snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{entry, {"Id": "good", "Message": "must not arrive"}}))
	require.Equal(t, 400, r.StatusCode)
	require.Contains(t, string(r.Body), "BatchRequestTooLong")
	require.Empty(t, receivedBodies(t, svc, q))
}

func TestPublishPayloadUTF8AndAttributeSize(t *testing.T) {
	_, pinnedErr := parsePublishEntry(url.Values{"Message": {"pinned"}, "MessageAttributes.entry.1.Name": {"tag"}, "MessageAttributes.entry.1.Value.DataType": {"Binary"}, "MessageAttributes.entry.1.Value.BinaryValue": {"AAEC"}})
	require.NoError(t, pinnedErr)
	e, err := parsePublishEntry(url.Values{"Message": {"한"}, "MessageAttributes.entry.1.Name": {"tag"}, "MessageAttributes.entry.1.Value.DataType": {"Binary"}, "MessageAttributes.entry.1.Value.BinaryValue": {"AAEC"}})
	require.NoError(t, err)
	require.Equal(t, 15, publishPayloadBytes(e))
	require.Equal(t, []byte{0, 1, 2}, e.Attributes["tag"].BinaryValue)
	for _, v := range []string{strings.Repeat("한", 99), strings.Repeat("x", 99)} {
		_, e := parsePublishEntry(url.Values{"Message": {"m"}, "Subject": {v}})
		require.NoError(t, e)
	}
	for _, v := range []string{strings.Repeat("한", 100), "line\nbreak"} {
		_, e := parsePublishEntry(url.Values{"Message": {"m"}, "Subject": {v}})
		require.Error(t, e)
	}
}

func TestPublishJSONStructureSelection(t *testing.T) {
	p, svc, topic, q := batchSetup(t)
	r := snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "a", "Message": `{"default":"fallback","sqs":"selected + & 한"}`, "MessageStructure": "json"}}))
	require.Equal(t, 200, r.StatusCode)
	require.Equal(t, []string{"selected + & 한"}, receivedBodies(t, svc, q))
	for _, body := range []string{`{"sqs":"only"}`, `{"default":"ok","http":123}`} {
		r := snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "bad", "Message": body, "MessageStructure": "json"}}))
		require.Equal(t, 200, r.StatusCode)
		require.Contains(t, string(r.Body), "<Failed>")
	}
}

func TestFIFOCreateAttributesAndSharedDedup(t *testing.T) {
	svc := deliverySQS(t)
	resp := sqsQuery(t, svc, url.Values{"Action": {"CreateQueue"}, "QueueName": {"batch.fifo"}, "Attribute.1.Name": {"FifoQueue"}, "Attribute.1.Value": {"true"}})
	q := snsText(t, resp, "QueueUrl")
	p := newTestProvider(t)
	f := attributeFields(map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true"})
	f["Name"] = "batch.fifo"
	r := snsCall(t, p, "CreateTopic", f)
	require.Equal(t, 200, r.StatusCode)
	topic := snsText(t, r, "TopicArn")
	v, e := p.store.GetTopic(topic)
	require.NoError(t, e)
	require.Equal(t, "true", v.Attributes["FifoTopic"])
	_, e = p.store.Subscribe(topic+":sub", topic, "sqs", q, defaultAccountID)
	require.NoError(t, e)
	entry := map[string]string{"TopicArn": topic, "Message": "once", "MessageGroupId": "group-a", "MessageDeduplicationId": "dedup"}
	r = snsCall(t, p, "Publish", entry)
	require.Equal(t, 200, r.StatusCode)
	seq := snsText(t, r, "SequenceNumber")
	r = snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "duplicate", "Message": "once", "MessageGroupId": "group-a", "MessageDeduplicationId": "dedup"}}))
	require.Equal(t, 200, r.StatusCode)
	require.Equal(t, seq, snsText(t, r, "SequenceNumber"))
	require.Equal(t, []string{"once"}, receivedBodies(t, svc, q))
	require.NoError(t, p.store.SetTopicAttribute(topic, "DisplayName", "retained"))
	require.Error(t, p.store.SetTopicAttribute(topic, "FifoTopic", "false"))
}

func TestPublishFailureReportsActualDeliveryFailure(t *testing.T) {
	p, svc, topic, q := batchSetup(t)
	plugin.DefaultRegistry = plugin.NewRegistry()
	r := snsCall(t, p, "PublishBatch", batchFields(topic, []map[string]string{{"Id": "fail", "Message": "m"}}))
	require.Equal(t, 200, r.StatusCode)
	require.Contains(t, string(r.Body), "<SenderFault>false</SenderFault>")
	require.Contains(t, string(r.Body), "InternalError")
	require.Empty(t, receivedBodies(t, svc, q))
	require.Equal(t, 500, snsCall(t, p, "Publish", map[string]string{"TopicArn": topic, "Message": "m"}).StatusCode)
	_ = context.Background()
	_ = time.Second
}
