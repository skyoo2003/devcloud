import pytest
import json
import sqlite3
import time
import uuid
from botocore.exceptions import ClientError


def _sns_unit(response):
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert set(response) == {"ResponseMetadata"}


def _sns_messages(sqs, queue, count, timeout=5):
    messages = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(messages) < count:
        response = sqs.receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            MessageSystemAttributeNames=["All"],
        )
        for message in response.get("Messages", []):
            messages.append(message)
            sqs.delete_message(QueueUrl=queue, ReceiptHandle=message["ReceiptHandle"])
        if len(messages) < count:
            time.sleep(0.05)
    return messages


def test_sns_publish_batch_partial_delivery(sns_client, sqs_client):
    name = "sns-batch-" + uuid.uuid4().hex[:12]
    queue = sqs_client.create_queue(QueueName=name)["QueueUrl"]
    topic = sns_client.create_topic(Name=name)["TopicArn"]
    try:
        sns_client.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue)
        message = "한글 + payload & key=value %20\nsecond line"
        response = sns_client.publish_batch(
            TopicArn=topic,
            PublishBatchRequestEntries=[
                {
                    "Id": "good",
                    "Message": message,
                    "MessageAttributes": {
                        "text": {"DataType": "String", "StringValue": "한글 + &"},
                        "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01\x02"},
                    },
                },
                {"Id": "bad", "Message": "invalid", "MessageStructure": "json"},
                {
                    "Id": "json",
                    "Message": json.dumps({"default": "fallback", "sqs": "selected"}),
                    "MessageStructure": "json",
                },
            ],
        )
        assert [r["Id"] for r in response["Successful"]] == ["good", "json"]
        assert [r["Id"] for r in response["Failed"]] == ["bad"]
        assert response["Failed"][0]["SenderFault"] is True
        messages = _sns_messages(sqs_client, queue, 2)
        assert [m["Body"] for m in messages] == [message, "selected"]
        assert (
            messages[0]["MessageAttributes"]["blob"]["BinaryValue"] == b"\x00\x01\x02"
        )
        assert messages[0]["MessageAttributes"]["text"]["StringValue"] == "한글 + &"
        with pytest.raises(ClientError) as exc:
            sns_client.publish_batch(
                TopicArn=topic,
                PublishBatchRequestEntries=[
                    {"Id": "same", "Message": "a"},
                    {"Id": "same", "Message": "b"},
                ],
            )
        assert exc.value.response["Error"]["Code"] == "BatchEntryIdsNotDistinct"
        assert _sns_messages(sqs_client, queue, 1, 1) == []
    finally:
        sns_client.delete_topic(TopicArn=topic)
        sqs_client.delete_queue(QueueUrl=queue)


def test_sns_publish_batch_fifo_order_and_dedup(sns_client, sqs_client):
    name = "sns-fifo-" + uuid.uuid4().hex[:12] + ".fifo"
    queue = sqs_client.create_queue(
        QueueName=name,
        Attributes={
            "FifoQueue": "true",
            "DeduplicationScope": "messageGroup",
            "FifoThroughputLimit": "perMessageGroupId",
        },
    )["QueueUrl"]
    topic = sns_client.create_topic(
        Name=name,
        Attributes={
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true",
            "FifoThroughputScope": "MessageGroup",
        },
    )["TopicArn"]
    try:
        sns_client.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue)
        first = sns_client.publish(
            TopicArn=topic,
            Message="first",
            MessageGroupId="a",
            MessageDeduplicationId="shared",
        )
        response = sns_client.publish_batch(
            TopicArn=topic,
            PublishBatchRequestEntries=[
                {
                    "Id": "duplicate",
                    "Message": "first",
                    "MessageGroupId": "a",
                    "MessageDeduplicationId": "shared",
                },
                {"Id": "second", "Message": "second", "MessageGroupId": "a"},
                {
                    "Id": "other",
                    "Message": "other",
                    "MessageGroupId": "b",
                    "MessageDeduplicationId": "shared",
                },
            ],
        )
        assert response["Failed"] == []
        assert [r["Id"] for r in response["Successful"]] == [
            "duplicate",
            "second",
            "other",
        ]
        assert response["Successful"][0]["MessageId"] == first["MessageId"]
        assert response["Successful"][0]["SequenceNumber"] == first["SequenceNumber"]
        assert int(first["SequenceNumber"]) < int(
            response["Successful"][1]["SequenceNumber"]
        )
        messages = _sns_messages(sqs_client, queue, 3)
        assert [m["Body"] for m in messages] == ["first", "second", "other"]
        assert [m["Attributes"]["MessageGroupId"] for m in messages] == ["a", "a", "b"]
        assert messages[0]["Attributes"]["MessageDeduplicationId"] == "shared"
        assert all(m["Attributes"]["SequenceNumber"].isdigit() for m in messages)
        sns_client.publish(TopicArn=topic, Message="second", MessageGroupId="a")
        assert _sns_messages(sqs_client, queue, 1, 1) == []
    finally:
        sns_client.delete_topic(TopicArn=topic)
        sqs_client.delete_queue(QueueUrl=queue)


def test_sns_platform_attribute_lifecycle(sns_client):
    app = sns_client.create_platform_application(
        Name="mobile." + uuid.uuid4().hex[:12],
        Platform="GCM",
        Attributes={"PlatformCredential": "local"},
    )["PlatformApplicationArn"]
    endpoint = None
    try:
        endpoint = sns_client.create_platform_endpoint(
            PlatformApplicationArn=app, Token="initial", CustomUserData="before"
        )["EndpointArn"]
        _sns_unit(
            sns_client.set_platform_application_attributes(
                PlatformApplicationArn=app,
                Attributes={"SuccessFeedbackSampleRate": "25"},
            )
        )
        attrs = sns_client.get_platform_application_attributes(
            PlatformApplicationArn=app
        )["Attributes"]
        assert (
            attrs["PlatformCredential"] == "local"
            and attrs["SuccessFeedbackSampleRate"] == "25"
        )
        _sns_unit(
            sns_client.set_endpoint_attributes(
                EndpointArn=endpoint,
                Attributes={
                    "Enabled": "false",
                    "Token": "updated",
                    "CustomUserData": "한글 + data",
                },
            )
        )
        attrs = sns_client.get_endpoint_attributes(EndpointArn=endpoint)["Attributes"]
        assert attrs == {
            "Enabled": "false",
            "Token": "updated",
            "CustomUserData": "한글 + data",
        }
        assert (
            sns_client.create_platform_endpoint(
                PlatformApplicationArn=app,
                Token="updated",
                CustomUserData="한글 + data",
                Attributes={"Enabled": "false"},
            )["EndpointArn"]
            == endpoint
        )
        assert endpoint in [
            e["EndpointArn"]
            for e in sns_client.list_endpoints_by_platform_application(
                PlatformApplicationArn=app
            )["Endpoints"]
        ]
        with pytest.raises(ClientError):
            sns_client.set_endpoint_attributes(
                EndpointArn=endpoint, Attributes={"Enabled": "bad"}
            )
        assert (
            sns_client.get_endpoint_attributes(EndpointArn=endpoint)["Attributes"]
            == attrs
        )
        sns_client.delete_platform_application(PlatformApplicationArn=app)
        with pytest.raises(ClientError) as exc:
            sns_client.get_endpoint_attributes(EndpointArn=endpoint)
        assert exc.value.response["Error"]["Code"] == "NotFound"
    finally:
        sns_client.delete_platform_application(PlatformApplicationArn=app)


def test_sns_sms_attributes_and_opt_in(sns_client, devcloud_data_dir):
    before = sns_client.get_sms_attributes()["attributes"]
    phone = "+1555" + str(int(uuid.uuid4().hex[:10], 16) % 10**7).zfill(7)
    database = devcloud_data_dir / "sns" / "sns" / "sns.db"
    try:
        _sns_unit(
            sns_client.set_sms_attributes(
                attributes={
                    "DefaultSMSType": "Transactional",
                    "DefaultSenderID": "DevCloud",
                    "MonthlySpendLimit": "1.50",
                }
            )
        )
        assert sns_client.get_sms_attributes(attributes=["DefaultSenderID"])[
            "attributes"
        ] == {"DefaultSenderID": "DevCloud"}
        with pytest.raises(ClientError):
            sns_client.set_sms_attributes(attributes={"DefaultSMSType": "invalid"})
        assert sns_client.get_sms_attributes(attributes=["DefaultSMSType"])[
            "attributes"
        ] == {"DefaultSMSType": "Transactional"}
        with sqlite3.connect(database) as db:
            db.execute("INSERT INTO sms_opt_outs VALUES (?,?)", (phone, "000000000000"))
        assert (
            sns_client.check_if_phone_number_is_opted_out(phoneNumber=phone)[
                "isOptedOut"
            ]
            is True
        )
        _sns_unit(sns_client.opt_in_phone_number(phoneNumber=phone))
        assert (
            sns_client.check_if_phone_number_is_opted_out(phoneNumber=phone)[
                "isOptedOut"
            ]
            is False
        )
        assert phone not in sns_client.list_phone_numbers_opted_out()["phoneNumbers"]
        _sns_unit(sns_client.opt_in_phone_number(phoneNumber=phone))
    finally:
        sns_client.set_sms_attributes(attributes=before)
        with sqlite3.connect(database) as db:
            db.execute("DELETE FROM sms_opt_outs WHERE phone_number=?", (phone,))


def test_sns_sandbox_otp_from_actual_outbox(sns_client, devcloud_data_dir):
    phone = "+1555" + str(int(uuid.uuid4().hex[:10], 16) % 10**7).zfill(7)
    database = devcloud_data_dir / "sns" / "sns" / "sns.db"
    try:
        assert sns_client.get_sms_sandbox_account_status()["IsInSandbox"] is True
        _sns_unit(
            sns_client.create_sms_sandbox_phone_number(
                PhoneNumber=phone, LanguageCode="kr-KR"
            )
        )
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
            raw = db.execute(
                "SELECT body_json FROM sns_sms_outbox WHERE account_id=? AND phone_number=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                ("000000000000", phone),
            ).fetchone()[0]
        otp = json.loads(raw)["OneTimePassword"]
        assert otp.isdigit() and len(otp) == 6
        wrong = str((int(otp) + 1) % 1000000).zfill(6)
        with pytest.raises(ClientError) as exc:
            sns_client.verify_sms_sandbox_phone_number(
                PhoneNumber=phone, OneTimePassword=wrong
            )
        assert exc.value.response["Error"]["Code"] == "Verification"
        _sns_unit(
            sns_client.verify_sms_sandbox_phone_number(
                PhoneNumber=phone, OneTimePassword=otp
            )
        )
        phones = sns_client.list_sms_sandbox_phone_numbers()["PhoneNumbers"]
        assert (
            next(p for p in phones if p["PhoneNumber"] == phone)["Status"] == "VERIFIED"
        )
        with pytest.raises(ClientError) as exc:
            sns_client.verify_sms_sandbox_phone_number(
                PhoneNumber=phone, OneTimePassword=otp
            )
        assert exc.value.response["Error"]["Code"] == "Verification"
    finally:
        sns_client.delete_sms_sandbox_phone_number(PhoneNumber=phone)


def test_create_topic(sns_client):
    resp = sns_client.create_topic(Name="compat-topic")
    assert "TopicArn" in resp
    assert "compat-topic" in resp["TopicArn"]


def test_list_topics(sns_client):
    sns_client.create_topic(Name="list-topic-1")
    sns_client.create_topic(Name="list-topic-2")
    resp = sns_client.list_topics()
    arns = [t["TopicArn"] for t in resp["Topics"]]
    assert any("list-topic-1" in a for a in arns)
    assert any("list-topic-2" in a for a in arns)


def test_subscribe_and_list(sns_client):
    topic = sns_client.create_topic(Name="sub-compat")
    arn = topic["TopicArn"]
    sub = sns_client.subscribe(
        TopicArn=arn, Protocol="email", Endpoint="test@example.com"
    )
    assert "SubscriptionArn" in sub
    resp = sns_client.list_subscriptions()
    assert len(resp["Subscriptions"]) >= 1


def test_publish(sns_client):
    topic = sns_client.create_topic(Name="pub-topic")
    resp = sns_client.publish(TopicArn=topic["TopicArn"], Message="hello world")
    assert "MessageId" in resp


def test_delete_topic(sns_client):
    topic = sns_client.create_topic(Name="del-compat-topic")
    sns_client.delete_topic(TopicArn=topic["TopicArn"])
    resp = sns_client.list_topics()
    arns = [t["TopicArn"] for t in resp["Topics"]]
    assert not any("del-compat-topic" in a for a in arns)


def test_publish_nonexistent_topic(sns_client):
    with pytest.raises(ClientError) as exc:
        sns_client.publish(
            TopicArn="arn:aws:sns:us-east-1:000000000000:no-such-topic",
            Message="test",
        )
    assert exc.value.response["Error"]["Code"] == "NotFound"


def test_get_topic_attributes(sns_client):
    topic = sns_client.create_topic(Name="attr-topic")
    resp = sns_client.get_topic_attributes(TopicArn=topic["TopicArn"])
    assert "Attributes" in resp
    assert "TopicArn" in resp["Attributes"]


def test_unsubscribe(sns_client):
    topic = sns_client.create_topic(Name="unsub-topic")
    sub = sns_client.subscribe(
        TopicArn=topic["TopicArn"], Protocol="email", Endpoint="unsub@test.com"
    )
    sub_arn = sub["SubscriptionArn"]
    sns_client.unsubscribe(SubscriptionArn=sub_arn)


def test_set_subscription_attributes(sns_client):
    topic = sns_client.create_topic(Name="subattr-topic")
    sub = sns_client.subscribe(
        TopicArn=topic["TopicArn"], Protocol="email", Endpoint="attr@test.com"
    )
    sub_arn = sub["SubscriptionArn"]
    sns_client.set_subscription_attributes(
        SubscriptionArn=sub_arn,
        AttributeName="RawMessageDelivery",
        AttributeValue="true",
    )


def test_topic_tags(sns_client):
    t = sns_client.create_topic(Name="tagged-topic")
    arn = t["TopicArn"]
    sns_client.tag_resource(
        ResourceArn=arn,
        Tags=[{"Key": "env", "Value": "test"}],
    )
    resp = sns_client.list_tags_for_resource(ResourceArn=arn)
    assert any(tag["Key"] == "env" for tag in resp["Tags"])
    sns_client.untag_resource(ResourceArn=arn, TagKeys=["env"])


def test_add_remove_permission(sns_client):
    t = sns_client.create_topic(Name="perm-topic")
    arn = t["TopicArn"]
    sns_client.add_permission(
        TopicArn=arn,
        Label="allow-account",
        AWSAccountId=["000000000000"],
        ActionName=["Publish"],
    )
    sns_client.remove_permission(TopicArn=arn, Label="allow-account")


def test_set_topic_attributes(sns_client):
    t = sns_client.create_topic(Name="attr-topic")
    arn = t["TopicArn"]
    sns_client.set_topic_attributes(
        TopicArn=arn,
        AttributeName="DisplayName",
        AttributeValue="My Topic",
    )
    resp = sns_client.get_topic_attributes(TopicArn=arn)
    assert resp["Attributes"].get("DisplayName") == "My Topic"


def test_check_phone_opted_out(sns_client):
    resp = sns_client.check_if_phone_number_is_opted_out(phoneNumber="+15555551234")
    assert resp["isOptedOut"] is False


def test_list_phone_numbers_opted_out(sns_client):
    resp = sns_client.list_phone_numbers_opted_out()
    assert "phoneNumbers" in resp


def test_data_protection_policy(sns_client):
    t = sns_client.create_topic(Name="dp-topic")
    arn = t["TopicArn"]
    policy = '{"Name":"test","Version":"2021-06-01","Statement":[]}'
    sns_client.put_data_protection_policy(ResourceArn=arn, DataProtectionPolicy=policy)
    resp = sns_client.get_data_protection_policy(ResourceArn=arn)
    assert "Statement" in resp["DataProtectionPolicy"]


def test_get_subscription_attributes(sns_client, sqs_client):
    t = sns_client.create_topic(Name="sub-attr-topic")
    sqs_client.create_queue(QueueName="sub-attr-q")
    queue_arn = "arn:aws:sqs:us-east-1:000000000000:sub-attr-q"
    sub = sns_client.subscribe(
        TopicArn=t["TopicArn"],
        Protocol="sqs",
        Endpoint=queue_arn,
    )
    resp = sns_client.get_subscription_attributes(
        SubscriptionArn=sub["SubscriptionArn"]
    )
    assert "Protocol" in resp["Attributes"]


@pytest.mark.parametrize("use_arn", [True, False])
def test_sns_sqs_arn_delivery_preserves_message(sns_client, sqs_client, use_arn):
    queue = sqs_client.create_queue(QueueName="phase1-sns-delivery")["QueueUrl"]
    topic = sns_client.create_topic(Name="phase1-sns-delivery")["TopicArn"]
    try:
        endpoint = queue
        if use_arn:
            endpoint = sqs_client.get_queue_attributes(
                QueueUrl=queue, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
        sns_client.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=endpoint)
        message = "한글 + payload & key=value %20\nsecond line"
        sns_client.publish(TopicArn=topic, Message=message)
        received = sqs_client.receive_message(QueueUrl=queue)
        assert [m["Body"] for m in received.get("Messages", [])] == [message]
    finally:
        sns_client.delete_topic(TopicArn=topic)
        sqs_client.delete_queue(QueueUrl=queue)
