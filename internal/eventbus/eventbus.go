// SPDX-License-Identifier: Apache-2.0

package eventbus

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Event struct {
	Source    string
	Type      string
	Detail    map[string]any
	Timestamp time.Time
}

type EventHandler func(Event)

type EventBus interface {
	Publish(ctx context.Context, event Event) error
	Subscribe(eventType string, handler EventHandler) (unsubscribe func())
}

type subscriber struct {
	eventType string
	handler   EventHandler
}

type InMemoryEventBus struct {
	mu          sync.RWMutex
	subscribers map[uint64]*subscriber
	nextID      uint64
}

func New() *InMemoryEventBus {
	return &InMemoryEventBus{
		subscribers: make(map[uint64]*subscriber),
	}
}

func (b *InMemoryEventBus) Publish(_ context.Context, event Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	b.mu.RLock()
	subs := make([]*subscriber, 0, len(b.subscribers))
	for _, s := range b.subscribers {
		subs = append(subs, s)
	}
	b.mu.RUnlock()

	for _, s := range subs {
		if s.eventType == "*" || s.eventType == event.Type {
			go func(h EventHandler, e Event) {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("event handler panic", "event", e.Type, "panic", r)
					}
				}()
				h(e)
			}(s.handler, event)
		}
	}
	return nil
}

func (b *InMemoryEventBus) Subscribe(eventType string, handler EventHandler) func() {
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subscribers[id] = &subscriber{eventType: eventType, handler: handler}
	b.mu.Unlock()

	return func() {
		b.mu.Lock()
		delete(b.subscribers, id)
		b.mu.Unlock()
	}
}
