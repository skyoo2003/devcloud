// SPDX-License-Identifier: Apache-2.0

package lambda

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *LambdaStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewLambdaStore(dir+"/lambda.db", dir+"/code")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestNewEventSourcePoller(t *testing.T) {
	store := newTestStore(t)
	p := NewEventSourcePoller(store, 4747)
	require.NotNil(t, p)
	require.Equal(t, 4747, p.port)
}

func TestEventSourcePollerStartStop(t *testing.T) {
	store := newTestStore(t)
	p := NewEventSourcePoller(store, 4747)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Start(ctx)
		close(done)
	}()

	// Let it run briefly.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop after context cancel")
	}
}

func TestEventSourcePollerStop(t *testing.T) {
	store := newTestStore(t)
	p := NewEventSourcePoller(store, 4747)

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		p.Start(ctx)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	p.Stop()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop after Stop()")
	}

	// Calling Stop again should not panic.
	p.Stop()
}

func TestIsSQSArn(t *testing.T) {
	require.True(t, isSQSArn("arn:aws:sqs:us-east-1:000000000000:my-queue"))
	require.False(t, isSQSArn("arn:aws:dynamodb:us-east-1:000000000000:table/foo/stream/2021-01-01"))
	require.False(t, isSQSArn("arn:aws:lambda:us-east-1:000000000000:function:foo"))
}

func TestIsDynamoDBStreamArn(t *testing.T) {
	require.True(t, isDynamoDBStreamArn("arn:aws:dynamodb:us-east-1:000000000000:table/Foo/stream/2021-01-01T00:00:00.000"))
	require.False(t, isDynamoDBStreamArn("arn:aws:sqs:us-east-1:000000000000:my-queue"))
}

func TestExtractQueueNameFromArn(t *testing.T) {
	require.Equal(t, "my-queue", extractQueueNameFromArn("arn:aws:sqs:us-east-1:000000000000:my-queue"))
	require.Equal(t, "raw", extractQueueNameFromArn("raw"))
}
