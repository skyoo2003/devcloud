import json

import pytest
from botocore.exceptions import ClientError, ParamValidationError


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


def _queue_arn(sqs_client, url):
    return sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def _wire_to_dlq(sqs_client, src_url, dlq_arn):
    # RedrivePolicy is only parsed via set_queue_attributes, so wire the source here.
    sqs_client.set_queue_attributes(
        QueueUrl=src_url,
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1}
            )
        },
    )


def _redrive_pair(sqs_client, src_name, dlq_name):
    """Create a source queue wired to a DLQ (maxReceiveCount=1); return (src_url, dlq_url, dlq_arn)."""
    src = sqs_client.create_queue(QueueName=src_name)["QueueUrl"]
    dlq = sqs_client.create_queue(QueueName=dlq_name)["QueueUrl"]
    dlq_arn = _queue_arn(sqs_client, dlq)
    _wire_to_dlq(sqs_client, src, dlq_arn)
    return src, dlq, dlq_arn


def test_list_dead_letter_source_queues(sqs_client):
    src, dlq, _ = _redrive_pair(sqs_client, "dlq-src", "dlq-target")
    resp = sqs_client.list_dead_letter_source_queues(QueueUrl=dlq)
    urls = resp["queueUrls"]
    assert any("dlq-src" in u for u in urls)


def test_list_dead_letter_source_queues_no_sources(sqs_client):
    # A queue that exists but is not used as a DLQ returns an empty list.
    url = sqs_client.create_queue(QueueName="dlsq-no-sources")["QueueUrl"]
    resp = sqs_client.list_dead_letter_source_queues(QueueUrl=url)
    assert resp["queueUrls"] == []


def test_list_dead_letter_source_queues_nonexistent(sqs_client):
    with pytest.raises(ClientError) as exc:
        sqs_client.list_dead_letter_source_queues(
            QueueUrl="http://localhost:4747/000000000000/no-such-dlq"
        )
    assert (
        exc.value.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"
    )


def test_message_move_task_flow(sqs_client):
    src, dlq, dlq_arn = _redrive_pair(sqs_client, "mmt-src", "mmt-dlq")

    sqs_client.send_message(QueueUrl=src, MessageBody="redrive-me")
    # Exceed maxReceiveCount so the message is moved to the DLQ.
    sqs_client.receive_message(QueueUrl=src, VisibilityTimeout=0)
    sqs_client.receive_message(QueueUrl=src, VisibilityTimeout=0)

    handle = sqs_client.start_message_move_task(SourceArn=dlq_arn)["TaskHandle"]
    assert handle

    tasks = sqs_client.list_message_move_tasks(SourceArn=dlq_arn)["Results"]
    assert len(tasks) >= 1
    assert tasks[0]["SourceArn"] == dlq_arn

    cancel = sqs_client.cancel_message_move_task(TaskHandle=handle)
    assert "ApproximateNumberOfMessagesMoved" in cancel


def test_message_move_task_destination_routing(sqs_client):
    # Two sources share a DLQ; an explicit DestinationArn routes deterministically.
    src1, dlq, dlq_arn = _redrive_pair(sqs_client, "mmt-route-src1", "mmt-route-dlq")
    src2 = sqs_client.create_queue(QueueName="mmt-route-src2")["QueueUrl"]
    _wire_to_dlq(sqs_client, src2, dlq_arn)
    src2_arn = _queue_arn(sqs_client, src2)

    sqs_client.send_message(QueueUrl=dlq, MessageBody="to-src2")

    resp = sqs_client.start_message_move_task(
        SourceArn=dlq_arn, DestinationArn=src2_arn
    )
    assert "TaskHandle" in resp

    msgs = sqs_client.receive_message(QueueUrl=src2, MaxNumberOfMessages=10)
    assert len(msgs.get("Messages", [])) == 1
    assert msgs["Messages"][0]["Body"] == "to-src2"


def test_message_move_task_ambiguous_destination(sqs_client):
    # Two sources share a DLQ and no DestinationArn is given -> ambiguous.
    src1, dlq, dlq_arn = _redrive_pair(sqs_client, "mmt-amb-src1", "mmt-amb-dlq")
    src2 = sqs_client.create_queue(QueueName="mmt-amb-src2")["QueueUrl"]
    _wire_to_dlq(sqs_client, src2, dlq_arn)

    with pytest.raises(ClientError) as exc:
        sqs_client.start_message_move_task(SourceArn=dlq_arn)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"


def test_start_message_move_task_unsupported_source(sqs_client):
    # SourceArn must be a DLQ; a plain queue is rejected.
    url = sqs_client.create_queue(QueueName="mmt-not-a-dlq")["QueueUrl"]
    arn = _queue_arn(sqs_client, url)
    with pytest.raises(ClientError) as exc:
        sqs_client.start_message_move_task(SourceArn=arn)
    assert (
        exc.value.response["Error"]["Code"]
        == "AWS.SimpleQueueService.UnsupportedOperation"
    )


def test_start_message_move_task_requires_source_arn(sqs_client):
    # SourceArn is a required member, so botocore rejects the call client-side.
    with pytest.raises(ParamValidationError):
        sqs_client.start_message_move_task(
            DestinationArn="arn:aws:sqs:us-east-1:000000000000:x"
        )


def test_cancel_message_move_task_missing_handle(sqs_client):
    # An empty TaskHandle passes client validation and reaches the server handler.
    with pytest.raises(ClientError) as exc:
        sqs_client.cancel_message_move_task(TaskHandle="")
    assert exc.value.response["Error"]["Code"] == "MissingParameter"


def test_cancel_message_move_task_unknown_handle(sqs_client):
    with pytest.raises(ClientError) as exc:
        sqs_client.cancel_message_move_task(TaskHandle="does-not-exist")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
