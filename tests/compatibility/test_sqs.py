import pytest
from botocore.exceptions import ClientError


def test_create_queue(sqs_client):
    response = sqs_client.create_queue(QueueName="test-create-queue")
    assert "QueueUrl" in response
    assert "test-create-queue" in response["QueueUrl"]


def test_send_and_receive_message(sqs_client):
    response = sqs_client.create_queue(QueueName="test-msg-queue")
    queue_url = response["QueueUrl"]

    sqs_client.send_message(QueueUrl=queue_url, MessageBody="hello world")

    msgs = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    assert len(msgs["Messages"]) == 1
    assert msgs["Messages"][0]["Body"] == "hello world"


def test_delete_message(sqs_client):
    response = sqs_client.create_queue(QueueName="test-del-msg-queue")
    queue_url = response["QueueUrl"]

    sqs_client.send_message(QueueUrl=queue_url, MessageBody="to delete")
    msgs = sqs_client.receive_message(QueueUrl=queue_url)
    receipt = msgs["Messages"][0]["ReceiptHandle"]

    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)


def test_list_queues(sqs_client):
    sqs_client.create_queue(QueueName="test-list-q1")
    sqs_client.create_queue(QueueName="test-list-q2")
    response = sqs_client.list_queues()
    urls = response.get("QueueUrls", [])
    assert any("test-list-q1" in u for u in urls)
    assert any("test-list-q2" in u for u in urls)


def test_delete_queue(sqs_client):
    response = sqs_client.create_queue(QueueName="test-del-queue")
    queue_url = response["QueueUrl"]
    sqs_client.delete_queue(QueueUrl=queue_url)


def test_send_to_nonexistent_queue(sqs_client):
    with pytest.raises(ClientError) as exc:
        sqs_client.send_message(
            QueueUrl="http://localhost:4747/000000000000/no-such-queue",
            MessageBody="test",
        )
    assert exc.value.response["Error"]["Code"] in (
        "AWS.SimpleQueueService.NonExistentQueue",
        "QueueDoesNotExist",
    )


def test_get_queue_attributes(sqs_client):
    resp = sqs_client.create_queue(QueueName="attr-queue")
    url = resp["QueueUrl"]
    attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])
    assert "Attributes" in attrs
    assert "QueueArn" in attrs["Attributes"]


def test_set_queue_attributes(sqs_client):
    resp = sqs_client.create_queue(QueueName="setattr-queue")
    url = resp["QueueUrl"]
    sqs_client.set_queue_attributes(
        QueueUrl=url, Attributes={"VisibilityTimeout": "60"}
    )
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=url, AttributeNames=["VisibilityTimeout"]
    )
    assert attrs["Attributes"]["VisibilityTimeout"] == "60"


def test_send_message_with_attributes(sqs_client):
    resp = sqs_client.create_queue(QueueName="msgattr-queue")
    url = resp["QueueUrl"]
    sqs_client.send_message(
        QueueUrl=url,
        MessageBody="with attrs",
        MessageAttributes={"color": {"StringValue": "blue", "DataType": "String"}},
    )
    msgs = sqs_client.receive_message(QueueUrl=url, MessageAttributeNames=["All"])
    assert msgs["Messages"][0]["MessageAttributes"]["color"]["StringValue"] == "blue"


def test_purge_queue(sqs_client):
    resp = sqs_client.create_queue(QueueName="purge-queue")
    url = resp["QueueUrl"]
    sqs_client.send_message(QueueUrl=url, MessageBody="m1")
    sqs_client.send_message(QueueUrl=url, MessageBody="m2")
    sqs_client.purge_queue(QueueUrl=url)
    msgs = sqs_client.receive_message(QueueUrl=url)
    assert msgs.get("Messages", []) == []


def test_create_duplicate_queue(sqs_client):
    sqs_client.create_queue(QueueName="dup-queue")
    resp = sqs_client.create_queue(QueueName="dup-queue")
    assert "QueueUrl" in resp


def test_send_message_batch(sqs_client):
    q = sqs_client.create_queue(QueueName="batch-q")
    url = q["QueueUrl"]
    resp = sqs_client.send_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "1", "MessageBody": "msg1"},
            {"Id": "2", "MessageBody": "msg2"},
        ],
    )
    assert len(resp["Successful"]) == 2


def test_delete_message_batch(sqs_client):
    q = sqs_client.create_queue(QueueName="delbatch-q")
    url = q["QueueUrl"]
    sqs_client.send_message(QueueUrl=url, MessageBody="a")
    sqs_client.send_message(QueueUrl=url, MessageBody="b")
    msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=2)["Messages"]
    resp = sqs_client.delete_message_batch(
        QueueUrl=url,
        Entries=[
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
            for i, m in enumerate(msgs)
        ],
    )
    assert len(resp["Successful"]) == 2


def test_change_message_visibility(sqs_client):
    q = sqs_client.create_queue(QueueName="vis-q")
    url = q["QueueUrl"]
    sqs_client.send_message(QueueUrl=url, MessageBody="test")
    msgs = sqs_client.receive_message(QueueUrl=url)["Messages"]
    sqs_client.change_message_visibility(
        QueueUrl=url,
        ReceiptHandle=msgs[0]["ReceiptHandle"],
        VisibilityTimeout=0,
    )
    msgs2 = sqs_client.receive_message(QueueUrl=url)
    assert len(msgs2.get("Messages", [])) >= 1


def test_queue_tags(sqs_client):
    q = sqs_client.create_queue(QueueName="tag-q")
    url = q["QueueUrl"]
    sqs_client.tag_queue(QueueUrl=url, Tags={"env": "test"})
    resp = sqs_client.list_queue_tags(QueueUrl=url)
    assert resp["Tags"]["env"] == "test"
    sqs_client.untag_queue(QueueUrl=url, TagKeys=["env"])


def test_fifo_queue(sqs_client):
    q = sqs_client.create_queue(
        QueueName="test.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )
    url = q["QueueUrl"]
    sqs_client.send_message(
        QueueUrl=url,
        MessageBody="fifo-msg",
        MessageGroupId="group1",
    )
    msgs = sqs_client.receive_message(QueueUrl=url)
    assert len(msgs["Messages"]) == 1
    assert msgs["Messages"][0]["Body"] == "fifo-msg"


def test_fifo_dedup(sqs_client):
    q = sqs_client.create_queue(
        QueueName="dedup.fifo",
        Attributes={"FifoQueue": "true"},
    )
    url = q["QueueUrl"]
    sqs_client.send_message(
        QueueUrl=url,
        MessageBody="msg1",
        MessageGroupId="g1",
        MessageDeduplicationId="dup1",
    )
    sqs_client.send_message(
        QueueUrl=url,
        MessageBody="msg1-dup",
        MessageGroupId="g1",
        MessageDeduplicationId="dup1",
    )
    msgs = sqs_client.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
    assert len(msgs["Messages"]) == 1
