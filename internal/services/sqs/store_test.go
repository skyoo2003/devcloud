// SPDX-License-Identifier: Apache-2.0

package sqs

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAccount = "000000000000"

func TestQueueStore_CreateAndList(t *testing.T) {
	store := NewQueueStore(0)
	err := store.CreateQueue("my-queue", testAccount)
	require.NoError(t, err)

	queues := store.ListQueues(testAccount, "")
	require.Len(t, queues, 1)
	assert.Equal(t, "my-queue", queues[0].Name)
	assert.Equal(t, testAccount, queues[0].AccountID)
}

func TestQueueStore_CreateDuplicate(t *testing.T) {
	store := NewQueueStore(0)
	err := store.CreateQueue("dup-queue", testAccount)
	require.NoError(t, err)

	err = store.CreateQueue("dup-queue", testAccount)
	assert.ErrorIs(t, err, ErrQueueAlreadyExists)
}

func TestQueueStore_SendAndReceive(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("send-queue", testAccount))

	msgID, err := store.SendMessage("send-queue", testAccount, "hello world")
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)

	msgs, err := store.ReceiveMessage("send-queue", testAccount, 1, 30)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "hello world", msgs[0].Body)
	assert.NotEmpty(t, msgs[0].ReceiptHandle)
	assert.NotEmpty(t, msgs[0].MD5OfBody)
}

func TestQueueStore_DeleteMessage(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("del-queue", testAccount))

	_, err := store.SendMessage("del-queue", testAccount, "to be deleted")
	require.NoError(t, err)

	msgs, err := store.ReceiveMessage("del-queue", testAccount, 1, 30)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	err = store.DeleteMessage("del-queue", testAccount, msgs[0].ReceiptHandle)
	require.NoError(t, err)

	// Subsequent receive should return nothing (message is deleted, not just invisible)
	// Use 0 visibility timeout so we're not waiting
	msgs2, err := store.ReceiveMessage("del-queue", testAccount, 1, 0)
	require.NoError(t, err)
	assert.Len(t, msgs2, 0)
}

func TestQueueStore_VisibilityTimeout(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("vis-queue", testAccount))

	_, err := store.SendMessage("vis-queue", testAccount, "invisible message")
	require.NoError(t, err)

	// Receive with 1-second visibility timeout
	msgs, err := store.ReceiveMessage("vis-queue", testAccount, 1, 1)
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// Immediate re-receive should get nothing (message is invisible)
	msgs2, err := store.ReceiveMessage("vis-queue", testAccount, 1, 1)
	require.NoError(t, err)
	assert.Len(t, msgs2, 0)

	// After 1.1 seconds the visibility timeout expires, message should reappear
	time.Sleep(1100 * time.Millisecond)

	msgs3, err := store.ReceiveMessage("vis-queue", testAccount, 1, 30)
	require.NoError(t, err)
	assert.Len(t, msgs3, 1)
}

func TestQueueStore_GetQueueUrl(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("url-queue", testAccount))

	url, err := store.GetQueueUrl("url-queue", testAccount)
	require.NoError(t, err)
	assert.Contains(t, url, "url-queue")
}

func TestQueueStore_DeleteQueue(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("gone-queue", testAccount))

	err := store.DeleteQueue("gone-queue", testAccount)
	require.NoError(t, err)

	queues := store.ListQueues(testAccount, "")
	assert.Len(t, queues, 0)
}

func TestQueueStore_ChangeMessageVisibility(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("vis-chg-queue", testAccount))

	_, err := store.SendMessage("vis-chg-queue", testAccount, "test message")
	require.NoError(t, err)

	msgs, err := store.ReceiveMessage("vis-chg-queue", testAccount, 1, 30)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	rh := msgs[0].ReceiptHandle

	err = store.ChangeMessageVisibility(testAccount, "vis-chg-queue", rh, 0)
	require.NoError(t, err)

	msgs2, err := store.ReceiveMessage("vis-chg-queue", testAccount, 1, 30)
	require.NoError(t, err)
	assert.Len(t, msgs2, 1)
}

func TestQueueStore_DLQ(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("source-queue", testAccount))
	require.NoError(t, store.CreateQueue("dead-letter-queue", testAccount))

	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:dead-letter-queue", testAccount)
	err := store.SetQueueAttributes("source-queue", testAccount, map[string]string{
		"RedrivePolicy": `{"maxReceiveCount":2,"deadLetterTargetArn":"` + dlqArn + `"}`,
	})
	require.NoError(t, err)

	_, err = store.SendMessage("source-queue", testAccount, "dlq-test")
	require.NoError(t, err)

	// Receive 2 times (exactly at threshold)
	for i := 0; i < 2; i++ {
		msgs, err := store.ReceiveMessage("source-queue", testAccount, 1, 0)
		require.NoError(t, err)
		require.Len(t, msgs, 1)
	}

	// Third receive should move message to DLQ
	msgs3, err := store.ReceiveMessage("source-queue", testAccount, 1, 0)
	require.NoError(t, err)
	assert.Len(t, msgs3, 0)

	// Message should be in DLQ
	dlqMsgs, err := store.ReceiveMessage("dead-letter-queue", testAccount, 1, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMsgs, 1)
	assert.Equal(t, "dlq-test", dlqMsgs[0].Body)
}

func TestQueueStore_ListDeadLetterSourceQueues(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("mmt-source", testAccount))
	require.NoError(t, store.CreateQueue("mmt-dlq", testAccount))

	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:mmt-dlq", testAccount)
	require.NoError(t, store.SetQueueAttributes("mmt-source", testAccount, map[string]string{
		"RedrivePolicy": `{"maxReceiveCount":1,"deadLetterTargetArn":"` + dlqArn + `"}`,
	}))

	// The DLQ lists its source queue.
	sources, err := store.ListDeadLetterSourceQueues("mmt-dlq", testAccount)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Contains(t, sources[0], "mmt-source")

	// A queue that is not used as a DLQ has no source queues.
	empty, err := store.ListDeadLetterSourceQueues("mmt-source", testAccount)
	require.NoError(t, err)
	assert.Len(t, empty, 0)

	// An unknown queue errors.
	_, err = store.ListDeadLetterSourceQueues("no-such-queue", testAccount)
	assert.ErrorIs(t, err, ErrQueueNotFound)
}

func TestQueueStore_MessageMoveTaskLifecycle(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("redrive-source", testAccount))
	require.NoError(t, store.CreateQueue("redrive-dlq", testAccount))

	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:redrive-dlq", testAccount)
	require.NoError(t, store.SetQueueAttributes("redrive-source", testAccount, map[string]string{
		"RedrivePolicy": `{"maxReceiveCount":1,"deadLetterTargetArn":"` + dlqArn + `"}`,
	}))

	_, err := store.SendMessage("redrive-source", testAccount, "redrive-me")
	require.NoError(t, err)

	// Exceed maxReceiveCount so the message lands in the DLQ.
	for i := 0; i < 2; i++ {
		_, rerr := store.ReceiveMessage("redrive-source", testAccount, 1, 0)
		require.NoError(t, rerr)
	}
	dlqMsgs, err := store.ReceiveMessage("redrive-dlq", testAccount, 1, 0)
	require.NoError(t, err)
	require.Len(t, dlqMsgs, 1)

	// Redrive back to the source (no explicit destination).
	handle, err := store.StartMessageMoveTask(dlqArn, "", 0, testAccount)
	require.NoError(t, err)
	require.NotEmpty(t, handle)

	tasks := store.ListMessageMoveTasks(dlqArn, 0)
	require.Len(t, tasks, 1)
	assert.Equal(t, "COMPLETED", tasks[0].Status)
	assert.Equal(t, 1, tasks[0].Moved)

	// The message is back on the source queue.
	back, err := store.ReceiveMessage("redrive-source", testAccount, 1, 0)
	require.NoError(t, err)
	require.Len(t, back, 1)
	assert.Equal(t, "redrive-me", back[0].Body)

	// Cancel is rejected: the synchronous redrive already COMPLETED, so there is no RUNNING
	// task to cancel (matching AWS, which only cancels running tasks). It still reports the count.
	moved, err := store.CancelMessageMoveTask(handle)
	assert.ErrorIs(t, err, ErrTaskNotCancellable)
	assert.Equal(t, 1, moved)

	// An unknown handle errors.
	_, err = store.CancelMessageMoveTask("bogus-handle")
	assert.ErrorIs(t, err, ErrTaskNotFound)

	// A queue that is not a DLQ cannot start a move task.
	srcArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:redrive-source", testAccount)
	_, err = store.StartMessageMoveTask(srcArn, "", 0, testAccount)
	assert.ErrorIs(t, err, ErrSourceNotDLQ)
}

func TestQueueStore_MessageMoveTaskMultiSource(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("multi-src1", testAccount))
	require.NoError(t, store.CreateQueue("multi-src2", testAccount))
	require.NoError(t, store.CreateQueue("multi-dlq", testAccount))

	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:multi-dlq", testAccount)
	redrive := `{"maxReceiveCount":1,"deadLetterTargetArn":"` + dlqArn + `"}`
	require.NoError(t, store.SetQueueAttributes("multi-src1", testAccount, map[string]string{"RedrivePolicy": redrive}))
	require.NoError(t, store.SetQueueAttributes("multi-src2", testAccount, map[string]string{"RedrivePolicy": redrive}))

	_, err := store.SendMessage("multi-dlq", testAccount, "orphan")
	require.NoError(t, err)

	// Two sources share the DLQ, so redrive without a destination is ambiguous.
	_, err = store.StartMessageMoveTask(dlqArn, "", 0, testAccount)
	assert.ErrorIs(t, err, ErrAmbiguousDest)

	// An explicit destination routes deterministically.
	dstArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:multi-src2", testAccount)
	handle, err := store.StartMessageMoveTask(dlqArn, dstArn, 0, testAccount)
	require.NoError(t, err)
	require.NotEmpty(t, handle)

	msgs, err := store.ReceiveMessage("multi-src2", testAccount, 1, 0)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "orphan", msgs[0].Body)
}

func TestQueueStore_MessageMoveTaskRegistryBounded(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("bound-src", testAccount))
	require.NoError(t, store.CreateQueue("bound-dlq", testAccount))

	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:%s:bound-dlq", testAccount)
	require.NoError(t, store.SetQueueAttributes("bound-src", testAccount, map[string]string{
		"RedrivePolicy": `{"maxReceiveCount":1,"deadLetterTargetArn":"` + dlqArn + `"}`,
	}))

	// Starting many tasks for the same source retains only the most recent maxTasksPerSource.
	for i := 0; i < maxTasksPerSource+5; i++ {
		_, err := store.StartMessageMoveTask(dlqArn, "", 0, testAccount)
		require.NoError(t, err)
	}
	assert.Len(t, store.moveTasks, maxTasksPerSource)
	// A non-positive MaxResults defaults to 1 (AWS: the most recent task).
	assert.Len(t, store.ListMessageMoveTasks(dlqArn, 0), 1)
	// A positive MaxResults is honored below the cap, and clamped to it above.
	assert.Len(t, store.ListMessageMoveTasks(dlqArn, 3), 3)
	assert.Len(t, store.ListMessageMoveTasks(dlqArn, 25), maxTasksPerSource)
}

func TestQueueStore_Tags(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueue("tag-queue", testAccount))

	err := store.TagQueue("tag-queue", testAccount, map[string]string{"env": "test", "owner": "alice"})
	require.NoError(t, err)

	tags, err := store.ListQueueTags("tag-queue", testAccount)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "alice", tags["owner"])

	err = store.UntagQueue("tag-queue", testAccount, []string{"env"})
	require.NoError(t, err)

	tags2, err := store.ListQueueTags("tag-queue", testAccount)
	require.NoError(t, err)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "alice", tags2["owner"])
}

func TestQueueStore_FIFO_Dedup(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueueWithAttributes("dedup.fifo", testAccount, map[string]string{
		"FifoQueue": "true",
	}))

	fifoOpts := SendMessageFIFOOptions{
		MessageGroupID:         "g1",
		MessageDeduplicationID: "dup1",
	}

	_, err := store.SendMessageFull("dedup.fifo", testAccount, "msg1", nil, fifoOpts)
	require.NoError(t, err)

	_, err = store.SendMessageFull("dedup.fifo", testAccount, "msg1-dup", nil, fifoOpts)
	require.NoError(t, err)

	msgs, err := store.ReceiveMessage("dedup.fifo", testAccount, 10, 30)
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, "msg1", msgs[0].Body)
}

func TestQueueStore_FIFO_ContentBasedDedup(t *testing.T) {
	store := NewQueueStore(0)
	require.NoError(t, store.CreateQueueWithAttributes("cbd.fifo", testAccount, map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "true",
	}))

	fifoOpts := SendMessageFIFOOptions{MessageGroupID: "g1"}

	_, err := store.SendMessageFull("cbd.fifo", testAccount, "same-body", nil, fifoOpts)
	require.NoError(t, err)

	_, err = store.SendMessageFull("cbd.fifo", testAccount, "same-body", nil, fifoOpts)
	require.NoError(t, err)

	msgs, err := store.ReceiveMessage("cbd.fifo", testAccount, 10, 30)
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
}

// TestQueueStore_PortHandling verifies that NewQueueStore honors the
// configured HTTP port when emitting queue URLs, and that a zero value
// falls back to the canonical default (4747). This guards the opts-based
// port propagation refactor against regressions — a future change that
// drops the port argument or forgets the default would fail here.
func TestQueueStore_PortHandling(t *testing.T) {
	tests := []struct {
		name      string
		inputPort int
		wantPort  int
	}{
		{"zero falls back to default 4747", 0, 4747},
		{"negative port falls back to 4747", -1, 4747},
		{"negative large port falls back to 4747", -9999, 4747},
		{"custom port 5858", 5858, 5858},
		{"custom port 8080", 8080, 8080},
		{"custom port 12345", 12345, 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewQueueStore(tt.inputPort)
			got := s.QueueURL(testAccount, "my-queue")
			want := fmt.Sprintf("http://localhost:%d/%s/my-queue", tt.wantPort, testAccount)
			assert.Equal(t, want, got)
		})
	}
}
