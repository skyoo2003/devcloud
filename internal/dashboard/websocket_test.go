// SPDX-License-Identifier: Apache-2.0

package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/skyoo2003/devcloud/internal/eventbus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsURL converts an http:// URL to ws://.
func wsURL(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}

// dialWS connects a gorilla WebSocket client to the given URL.
func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(url, nil)
	require.NoError(t, err)
	return conn
}

// TestHub_ClientConnects verifies that a client can successfully upgrade to
// WebSocket and that the hub registers it.
func TestHub_ClientConnects(t *testing.T) {
	bus := eventbus.New()
	hub := NewHub(bus)
	hub.Start()

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	conn := dialWS(t, wsURL(server.URL))
	defer func() { _ = conn.Close() }()

	// Give the hub a moment to register the client.
	time.Sleep(50 * time.Millisecond)

	hub.mu.RLock()
	clientCount := len(hub.clients)
	hub.mu.RUnlock()

	assert.Equal(t, 1, clientCount, "expected 1 registered client after connection")
}

// TestHub_BroadcastEvent verifies that an event published to the EventBus is
// delivered to a connected WebSocket client as the expected JSON payload.
func TestHub_BroadcastEvent(t *testing.T) {
	bus := eventbus.New()
	hub := NewHub(bus)
	hub.Start()

	server := httptest.NewServer(http.HandlerFunc(hub.ServeWS))
	defer server.Close()

	conn := dialWS(t, wsURL(server.URL))
	defer func() { _ = conn.Close() }()

	// Allow the hub to register the client before publishing.
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UTC().Truncate(time.Second)
	evt := eventbus.Event{
		Source:    "s3",
		Type:      "s3:ObjectCreated",
		Detail:    map[string]any{"key": "my-file.txt", "size": float64(1024)},
		Timestamp: ts,
	}
	err := bus.Publish(context.Background(), evt)
	require.NoError(t, err)

	// Read message with a deadline to avoid hanging on failure.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := conn.ReadMessage()
	require.NoError(t, err)

	var got eventMessage
	require.NoError(t, json.Unmarshal(raw, &got))

	assert.Equal(t, "s3", got.Source)
	assert.Equal(t, "s3:ObjectCreated", got.Type)
	assert.Equal(t, "my-file.txt", got.Detail["key"])
	assert.Equal(t, float64(1024), got.Detail["size"])
	assert.WithinDuration(t, ts, got.Timestamp, time.Second)
}
