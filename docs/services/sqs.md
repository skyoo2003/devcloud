# SQS (Simple Queue Service)

## Overview

DevCloud SQS provides in-memory message queues. Queues are stored in memory with thread-safe access (sync.RWMutex). Data is **not persisted** — all queues and messages are lost on server restart.

SQS supports dual protocol: legacy Query protocol (form-encoded with XML responses) and modern JSON 1.0 protocol (used by boto3 1.42+). The protocol is auto-detected based on the Content-Type header.

## Supported APIs

All 23 SQS operations are handled over the JSON 1.0 protocol.

| Operation | Description |
|-----------|-------------|
| CreateQueue | Create a new queue (returns existing if name matches); supports FIFO + attributes |
| ListQueues | List queues with optional name prefix filter |
| GetQueueUrl | Resolve queue name to its URL |
| DeleteQueue | Delete a queue and all its messages |
| PurgeQueue | Remove all messages from a queue |
| SendMessage / SendMessageBatch | Send one or many messages (computes MD5 hash) |
| ReceiveMessage | Receive messages with visibility timeout and MaxNumberOfMessages |
| DeleteMessage / DeleteMessageBatch | Delete one or many messages by receipt handle |
| ChangeMessageVisibility / ChangeMessageVisibilityBatch | Adjust message visibility timeout |
| GetQueueAttributes / SetQueueAttributes | Read/write queue attributes (incl. RedrivePolicy) |
| TagQueue / UntagQueue / ListQueueTags | Manage queue tags |
| ListDeadLetterSourceQueues | List queues that use a queue as their dead-letter target |
| StartMessageMoveTask / ListMessageMoveTasks / CancelMessageMoveTask | Redrive messages out of a dead-letter queue |
| AddPermission / RemovePermission | Accepted (no-op — permissions are not enforced) |

## boto3 Examples

### Send and receive messages

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Create queue
response = sqs.create_queue(QueueName="my-queue")
queue_url = response["QueueUrl"]

# Send message
sqs.send_message(QueueUrl=queue_url, MessageBody="Hello from DevCloud!")

# Receive messages
messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
for msg in messages.get("Messages", []):
    print(msg["Body"])
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
```

### List queues

```python
response = sqs.list_queues()
for url in response.get("QueueUrls", []):
    print(url)
```

## AWS CLI Examples

```bash
# Create queue
aws --endpoint-url http://localhost:4747 sqs create-queue --queue-name my-queue

# Send message
aws --endpoint-url http://localhost:4747 sqs send-message \
  --queue-url http://localhost:4747/000000000000/my-queue \
  --message-body "hello"

# Receive messages
aws --endpoint-url http://localhost:4747 sqs receive-message \
  --queue-url http://localhost:4747/000000000000/my-queue
```

## Known Limitations

- In-memory only — no persistence across restarts
- Dead-letter redrive (message move tasks) runs synchronously: tasks complete immediately,
  so there is no `RUNNING` status and `MaxNumberOfMessagesPerSecond` is not throttled
- `AddPermission`/`RemovePermission` are accepted but not enforced (no access-policy checks)
- Queue-level `DelaySeconds` and `VisibilityTimeout` attributes are stored and echoed back by
  `SetQueueAttributes`/`GetQueueAttributes` but not enforced: `SendMessage` ignores `DelaySeconds`
  and `ReceiveMessage` honors only its per-request `VisibilityTimeout` (default 30s)
- No message retention/expiry enforcement
- No long polling (`WaitTimeSeconds` is not honored — receives return immediately)
