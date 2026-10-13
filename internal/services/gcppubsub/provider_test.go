// SPDX-License-Identifier: Apache-2.0

package gcppubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCPPubSub_Lifecycle(t *testing.T) {
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{})
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Create Topic
	req := httptest.NewRequest("PUT", "/v1/projects/my-proj/topics/order-events", bytes.NewReader([]byte(`{"labels":{"env":"test"}}`)))
	resp, err := p.HandleRequest(ctx, "CreateTopic", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var topic Topic
	err = json.Unmarshal(resp.Body, &topic)
	require.NoError(t, err)
	assert.Equal(t, "projects/my-proj/topics/order-events", topic.Name)

	// 2. Duplicate Create Topic -> 409
	req = httptest.NewRequest("PUT", "/v1/projects/my-proj/topics/order-events", nil)
	resp, err = p.HandleRequest(ctx, "CreateTopic", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	// 3. Get Topic
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/topics/order-events", nil)
	resp, err = p.HandleRequest(ctx, "GetTopic", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. Create Subscription
	subBody := `{"topic":"projects/my-proj/topics/order-events","ackDeadlineSeconds":30}`
	req = httptest.NewRequest("PUT", "/v1/projects/my-proj/subscriptions/order-worker", bytes.NewReader([]byte(subBody)))
	resp, err = p.HandleRequest(ctx, "CreateSubscription", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var sub Subscription
	err = json.Unmarshal(resp.Body, &sub)
	require.NoError(t, err)
	assert.Equal(t, "projects/my-proj/subscriptions/order-worker", sub.Name)
	assert.Equal(t, "projects/my-proj/topics/order-events", sub.Topic)

	// 5. Publish Message
	payloadData := base64.StdEncoding.EncodeToString([]byte("order #1001 created"))
	publishBody := `{"messages":[{"data":"` + payloadData + `","attributes":{"type":"checkout"}}]}`
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/topics/order-events:publish", bytes.NewReader([]byte(publishBody)))
	resp, err = p.HandleRequest(ctx, "Publish", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var publishResp struct {
		MessageIDs []string `json:"messageIds"`
	}
	err = json.Unmarshal(resp.Body, &publishResp)
	require.NoError(t, err)
	assert.Len(t, publishResp.MessageIDs, 1)

	// 6. Pull Message
	pullBody := `{"maxMessages":10}`
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/subscriptions/order-worker:pull", bytes.NewReader([]byte(pullBody)))
	resp, err = p.HandleRequest(ctx, "Pull", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var pullResp struct {
		ReceivedMessages []ReceivedMessage `json:"receivedMessages"`
	}
	err = json.Unmarshal(resp.Body, &pullResp)
	require.NoError(t, err)
	require.Len(t, pullResp.ReceivedMessages, 1)

	recMsg := pullResp.ReceivedMessages[0]
	assert.NotEmpty(t, recMsg.AckID)
	assert.Equal(t, payloadData, recMsg.Message.Data)
	assert.Equal(t, "checkout", recMsg.Message.Attributes["type"])

	// 7. Acknowledge Message
	ackBody := `{"ackIds":["` + recMsg.AckID + `"]}`
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/subscriptions/order-worker:acknowledge", bytes.NewReader([]byte(ackBody)))
	resp, err = p.HandleRequest(ctx, "Acknowledge", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 8. Pull Again -> No messages
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/subscriptions/order-worker:pull", bytes.NewReader([]byte(pullBody)))
	resp, err = p.HandleRequest(ctx, "Pull", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	err = json.Unmarshal(resp.Body, &pullResp)
	require.NoError(t, err)
	assert.Len(t, pullResp.ReceivedMessages, 0)

	// 9. Delete Subscription
	req = httptest.NewRequest("DELETE", "/v1/projects/my-proj/subscriptions/order-worker", nil)
	resp, err = p.HandleRequest(ctx, "DeleteSubscription", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 10. Delete Topic
	req = httptest.NewRequest("DELETE", "/v1/projects/my-proj/topics/order-events", nil)
	resp, err = p.HandleRequest(ctx, "DeleteTopic", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGCPPubSub_RedeliversAfterAckDeadline(t *testing.T) {
	s := NewStore()
	_, err := s.CreateTopic("p", "t", nil)
	require.NoError(t, err)
	_, err = s.CreateSubscription("p", "orphan", "projects/p/topics/missing", 0, nil)
	require.ErrorIs(t, err, ErrTopicNotFound)
	_, err = s.CreateSubscription("p", "s", "projects/p/topics/t", 10, nil)
	require.NoError(t, err)
	_, err = s.Publish("p", "t", []PubsubMessage{{Data: "aGk="}})
	require.NoError(t, err)

	first, err := s.Pull("p", "s", 10)
	require.NoError(t, err)
	require.Len(t, first, 1)

	none, err := s.Pull("p", "s", 10)
	require.NoError(t, err)
	assert.Empty(t, none, "still within the ack deadline")

	first[0].DeliveryTime = first[0].DeliveryTime.Add(-11 * time.Second)
	again, err := s.Pull("p", "s", 10)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, first[0].Message.MessageID, again[0].Message.MessageID)
	assert.NotEqual(t, first[0].AckID, again[0].AckID)
}
