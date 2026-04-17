import pytest
from botocore.exceptions import ClientError


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
