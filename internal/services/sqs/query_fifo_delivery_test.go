// SPDX-License-Identifier: Apache-2.0
package sqs

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestSQSQueryRetainsFIFOIdentifiers(t *testing.T) {
	p := newTestSQSProvider(t)
	require.NoError(t, p.store.CreateQueueWithAttributes("q.fifo", defaultAccountID, map[string]string{"FifoQueue": "true"}))
	f := url.Values{"Action": {"SendMessage"}, "QueueUrl": {p.store.QueueURL(defaultAccountID, "q.fifo")}, "MessageBody": {"payload"}, "MessageGroupId": {"group-a"}, "MessageDeduplicationId": {"dedup"}, "MessageAttribute.1.Name": {"binary"}, "MessageAttribute.1.Value.DataType": {"Binary"}, "MessageAttribute.1.Value.BinaryValue": {"AAEC"}}
	require.Equal(t, 200, handleForm(t, p, f.Encode()).StatusCode)
	items, e := p.store.ReceiveMessage("q.fifo", defaultAccountID, 10, 0)
	require.NoError(t, e)
	require.Len(t, items, 1)
	require.Equal(t, "group-a", items[0].MessageGroupID)
	require.Equal(t, "dedup", items[0].MessageDeduplicationID)
	require.Equal(t, []byte{0, 1, 2}, items[0].MessageAttributes["binary"].BinaryValue)
	r := handleJSON(t, p, "ReceiveMessage", map[string]any{"QueueUrl": p.store.QueueURL(defaultAccountID, "q.fifo"), "MessageSystemAttributeNames": []string{"All"}, "MessageAttributeNames": []string{"All"}})
	var received struct {
		Messages []struct {
			Attributes        map[string]string
			MessageAttributes map[string]struct {
				DataType    string
				BinaryValue string
			}
		}
	}
	require.NoError(t, json.Unmarshal(r.Body, &received))
	require.Len(t, received.Messages, 1)
	require.Equal(t, "group-a", received.Messages[0].Attributes["MessageGroupId"])
	require.Equal(t, "dedup", received.Messages[0].Attributes["MessageDeduplicationId"])
	require.Equal(t, "00000000000000000001", received.Messages[0].Attributes["SequenceNumber"])
	require.Equal(t, "AAEC", received.Messages[0].MessageAttributes["binary"].BinaryValue)
}
func TestFIFOQueryDedupScope(t *testing.T) {
	for _, scope := range []string{"queue", "messageGroup"} {
		t.Run(scope, func(t *testing.T) {
			p := newTestSQSProvider(t)
			require.NoError(t, p.store.CreateQueueWithAttributes("q.fifo", defaultAccountID, map[string]string{"FifoQueue": "true", "DeduplicationScope": scope}))
			for _, g := range []string{"a", "b", "a"} {
				f := url.Values{"Action": {"SendMessage"}, "QueueUrl": {p.store.QueueURL(defaultAccountID, "q.fifo")}, "MessageBody": {g}, "MessageGroupId": {g}, "MessageDeduplicationId": {"shared"}}
				require.Equal(t, 200, handleForm(t, p, f.Encode()).StatusCode)
			}
			items, e := p.store.ReceiveMessage("q.fifo", defaultAccountID, 10, 30)
			require.NoError(t, e)
			want := 1
			if scope == "messageGroup" {
				want = 2
			}
			require.Len(t, items, want)
		})
	}
}
