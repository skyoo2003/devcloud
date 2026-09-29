"""Cross-service integration tests: S3 → SQS/Lambda, SQS → Lambda."""

import json
import time


def test_s3_to_sqs_notification(s3_client, sqs_client):
    """Test that S3 events reach SQS when notification is configured."""
    q = sqs_client.create_queue(QueueName="s3-events")
    queue_url = q["QueueUrl"]
    queue_arn = "arn:aws:sqs:us-east-1:000000000000:s3-events"

    s3_client.create_bucket(Bucket="notify-bucket")
    s3_client.put_bucket_notification_configuration(
        Bucket="notify-bucket",
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
        },
    )

    # PutObject should trigger a notification.
    s3_client.put_object(Bucket="notify-bucket", Key="test.txt", Body=b"hello")

    # Poll SQS for the notification message (allow up to 3s for delivery).
    msgs = None
    for _ in range(6):
        time.sleep(0.5)
        result = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0
        )
        if result.get("Messages"):
            msgs = result["Messages"]
            break

    assert msgs is not None, "No notification message received in SQS"
    body = msgs[0]["Body"]
    payload = json.loads(body)
    records = payload.get("Records", [])
    assert len(records) >= 1
    record = records[0]
    assert record["eventSource"] == "aws:s3"
    assert "ObjectCreated" in record["eventName"]
    assert record["s3"]["bucket"]["name"] == "notify-bucket"
    assert record["s3"]["object"]["key"] == "test.txt"


def test_s3_delete_notification(s3_client, sqs_client):
    """Test that S3 DeleteObject emits an ObjectRemoved notification."""
    q = sqs_client.create_queue(QueueName="s3-delete-events")
    queue_url = q["QueueUrl"]
    queue_arn = "arn:aws:sqs:us-east-1:000000000000:s3-delete-events"

    s3_client.create_bucket(Bucket="notify-delete-bucket")
    s3_client.put_bucket_notification_configuration(
        Bucket="notify-delete-bucket",
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectRemoved:*"],
                }
            ],
        },
    )

    # Create then delete an object.
    s3_client.put_object(Bucket="notify-delete-bucket", Key="del.txt", Body=b"bye")
    s3_client.delete_object(Bucket="notify-delete-bucket", Key="del.txt")

    msgs = None
    for _ in range(6):
        time.sleep(0.5)
        result = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=5, WaitTimeSeconds=0
        )
        # Filter for delete events specifically.
        for msg in result.get("Messages", []):
            payload = json.loads(msg["Body"])
            for rec in payload.get("Records", []):
                if "ObjectRemoved" in rec.get("eventName", ""):
                    msgs = [msg]
                    break
            if msgs:
                break

    assert msgs is not None, "No ObjectRemoved notification received in SQS"
