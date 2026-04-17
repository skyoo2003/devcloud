// SPDX-License-Identifier: Apache-2.0

package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishAndSubscribe(t *testing.T) {
	bus := New()
	var received []Event
	var mu sync.Mutex

	unsub := bus.Subscribe("s3:ObjectCreated", func(e Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})
	defer unsub()

	err := bus.Publish(context.Background(), Event{
		Source: "s3",
		Type:   "s3:ObjectCreated",
		Detail: map[string]any{"bucket": "test", "key": "hello.txt"},
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	assert.Equal(t, "s3", received[0].Source)
	assert.Equal(t, "test", received[0].Detail["bucket"])
}

func TestSubscribeWildcard(t *testing.T) {
	bus := New()
	var received []Event
	var mu sync.Mutex

	unsub := bus.Subscribe("*", func(e Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})
	defer unsub()

	bus.Publish(context.Background(), Event{Source: "s3", Type: "s3:ObjectCreated"})
	bus.Publish(context.Background(), Event{Source: "sqs", Type: "sqs:MessageSent"})

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 2)
}

func TestUnsubscribe(t *testing.T) {
	bus := New()
	callCount := 0
	var mu sync.Mutex

	unsub := bus.Subscribe("test", func(e Event) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})

	bus.Publish(context.Background(), Event{Type: "test"})
	time.Sleep(50 * time.Millisecond)

	unsub()

	bus.Publish(context.Background(), Event{Type: "test"})
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, callCount)
}
