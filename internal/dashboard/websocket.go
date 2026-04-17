// SPDX-License-Identifier: Apache-2.0

package dashboard

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/skyoo2003/devcloud/internal/eventbus"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// eventMessage is the JSON format sent to dashboard clients.
type eventMessage struct {
	Source    string         `json:"source"`
	Type      string         `json:"type"`
	Detail    map[string]any `json:"detail"`
	Timestamp time.Time      `json:"timestamp"`
}

// Client represents a single connected WebSocket client.
type Client struct {
	conn *websocket.Conn
	send chan []byte
}

// Hub maintains connected clients and broadcasts EventBus events to them.
type Hub struct {
	mu      sync.RWMutex
	clients map[*Client]bool
	bus     eventbus.EventBus
}

// NewHub creates a new Hub with a reference to the provided EventBus.
func NewHub(bus eventbus.EventBus) *Hub {
	return &Hub{
		clients: make(map[*Client]bool),
		bus:     bus,
	}
}

// Start subscribes to all events on the EventBus and begins broadcasting them
// to connected WebSocket clients.
func (h *Hub) Start() {
	h.bus.Subscribe("*", func(e eventbus.Event) {
		msg := eventMessage{
			Source:    e.Source,
			Type:      e.Type,
			Detail:    e.Detail,
			Timestamp: e.Timestamp,
		}
		data, err := json.Marshal(msg)
		if err != nil {
			log.Printf("dashboard: failed to marshal event: %v", err)
			return
		}
		h.broadcast(data)
	})
}

// ServeWS upgrades an HTTP connection to WebSocket, registers the client with
// the hub, and starts its write pump.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("dashboard: websocket upgrade failed: %v", err)
		return
	}

	client := &Client{
		conn: conn,
		send: make(chan []byte, 256),
	}

	h.mu.Lock()
	h.clients[client] = true
	h.mu.Unlock()

	// cleanup ensures the client is unregistered and the connection is closed
	// exactly once, regardless of which pump exits first.
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.clients, client)
			h.mu.Unlock()
			conn.Close()
		})
	}

	// Write pump: forwards messages from the send channel to the WebSocket connection.
	go func() {
		defer cleanup()
		for data := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("dashboard: write error: %v", err)
				return
			}
		}
	}()

	// Read pump: keeps the connection alive and detects disconnects.
	go func() {
		defer func() {
			close(client.send)
			cleanup()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

// broadcast sends data to every connected client.
func (h *Hub) broadcast(data []byte) {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.send <- data:
		default:
			// Drop message if the client's send buffer is full.
			log.Printf("dashboard: client send buffer full, dropping message")
		}
	}
}
