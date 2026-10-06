import json
import time
import uuid
from contextlib import contextmanager
from datetime import datetime

import pytest
from botocore.exceptions import ClientError


def _assert_unit(response):
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert set(response) == {"ResponseMetadata"}


@contextmanager
def _partner_source(events_client):
    name = f"aws.partner/devcloud/{uuid.uuid4().hex}"
    events_client.create_partner_event_source(Name=name, Account="000000000000")
    try:
        yield name
    finally:
        for method, arguments in (
            (
                events_client.remove_targets,
                {"Rule": "delivery", "EventBusName": name, "Ids": ["queue"]},
            ),
            (events_client.delete_rule, {"Name": "delivery", "EventBusName": name}),
            (events_client.delete_event_bus, {"Name": name}),
            (
                events_client.delete_partner_event_source,
                {"Name": name, "Account": "000000000000"},
            ),
        ):
            try:
                method(**arguments)
            except ClientError as error:
                if error.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise


def _receive_partner_message(sqs_client, queue_url, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = sqs_client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0)
        messages = response.get("Messages", [])
        if messages:
            message = messages[0]
            sqs_client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
            return json.loads(message["Body"])
        time.sleep(0.05)
    return None


def _bind_partner_target(events_client, source, queue_arn):
    events_client.create_event_bus(Name=source, EventSourceName=source)
    events_client.put_rule(
        Name="delivery",
        EventBusName=source,
        EventPattern=json.dumps({"source": [source]}),
        State="ENABLED",
    )
    events_client.put_targets(
        Rule="delivery",
        EventBusName=source,
        Targets=[{"Id": "queue", "Arn": queue_arn}],
    )


def test_partner_source_activation_controls_delivery(events_client, sqs_client):
    queue_name = f"partner-{uuid.uuid4().hex}"
    queue_url = sqs_client.create_queue(QueueName=queue_name)["QueueUrl"]
    try:
        with _partner_source(events_client) as source:
            before = events_client.describe_event_source(Name=source)
            assert before["State"] == "PENDING"
            assert isinstance(before["CreationTime"], datetime)
            with pytest.raises(ClientError) as error:
                events_client.activate_event_source(Name=source)
            assert error.value.response["Error"]["Code"] == "InvalidStateException"
            _bind_partner_target(
                events_client,
                source,
                f"arn:aws:sqs:us-east-1:000000000000:{queue_name}",
            )
            assert events_client.describe_event_source(Name=source)["State"] == "ACTIVE"
            detail = {
                "text": "한글 + payload & key=value %20\nsecond line",
                "marker": uuid.uuid4().hex,
            }
            entry = {
                "Source": source,
                "DetailType": "compat",
                "Detail": json.dumps(detail, ensure_ascii=False),
            }
            response = events_client.put_partner_events(Entries=[entry])
            assert response["FailedEntryCount"] == 0
            delivered = _receive_partner_message(sqs_client, queue_url)
            assert delivered is not None
            assert delivered["Source"] == source
            assert json.loads(delivered["Detail"]) == detail
            _assert_unit(events_client.deactivate_event_source(Name=source))
            assert (
                events_client.describe_event_source(Name=source)["State"] == "PENDING"
            )
            response = events_client.put_partner_events(Entries=[entry])
            assert response["FailedEntryCount"] == 1
            assert response["Entries"][0]["ErrorCode"] == "InvalidStateException"
            assert "EventId" not in response["Entries"][0]
            assert _receive_partner_message(sqs_client, queue_url, timeout=1) is None
            assert events_client.list_targets_by_rule(
                Rule="delivery", EventBusName=source
            )["Targets"]
            _assert_unit(events_client.activate_event_source(Name=source))
            _assert_unit(events_client.activate_event_source(Name=source))
            detail["marker"] = uuid.uuid4().hex
            entry["Detail"] = json.dumps(detail, ensure_ascii=False)
            assert (
                events_client.put_partner_events(Entries=[entry])["FailedEntryCount"]
                == 0
            )
            assert (
                json.loads(_receive_partner_message(sqs_client, queue_url)["Detail"])
                == detail
            )
    finally:
        sqs_client.delete_queue(QueueUrl=queue_url)


def test_partner_events_mixed_batch(events_client, sqs_client):
    queue_name = f"partner-{uuid.uuid4().hex}"
    queue_url = sqs_client.create_queue(QueueName=queue_name)["QueueUrl"]
    try:
        with _partner_source(events_client) as source:
            _bind_partner_target(
                events_client,
                source,
                f"arn:aws:sqs:us-east-1:000000000000:{queue_name}",
            )
            detail = {
                "text": "한글 + payload & key=value %20\nsecond line",
                "marker": uuid.uuid4().hex,
            }
            response = events_client.put_partner_events(
                Entries=[
                    {
                        "Source": source,
                        "DetailType": "compat",
                        "Detail": json.dumps(detail),
                    },
                    {
                        "Source": f"aws.partner/devcloud/missing-{uuid.uuid4().hex}",
                        "DetailType": "compat",
                        "Detail": "{}",
                    },
                    {
                        "Source": source,
                        "DetailType": "compat",
                        "Detail": "invalid JSON",
                    },
                ]
            )
            assert response["FailedEntryCount"] == 2
            assert len(response["Entries"]) == 3
            assert response["Entries"][0]["EventId"]
            assert response["Entries"][1]["ErrorCode"] == "ResourceNotFoundException"
            assert response["Entries"][2]["ErrorCode"] == "MalformedDetail"
            assert all("EventId" not in entry for entry in response["Entries"][1:])
            delivered = _receive_partner_message(sqs_client, queue_url)
            assert delivered is not None
            assert json.loads(delivered["Detail"]) == detail
            assert _receive_partner_message(sqs_client, queue_url, timeout=1) is None
    finally:
        sqs_client.delete_queue(QueueUrl=queue_url)


@pytest.mark.parametrize(
    "authorization_type,auth",
    [
        (
            "API_KEY",
            {
                "ApiKeyAuthParameters": {
                    "ApiKeyName": "key",
                    "ApiKeyValue": "secret-api-key",
                }
            },
        ),
        (
            "BASIC",
            {
                "BasicAuthParameters": {
                    "Username": "user",
                    "Password": "secret-password",
                }
            },
        ),
    ],
)
def test_connection_deauthorize_and_reuse(events_client, authorization_type, auth):
    name = f"connection-{uuid.uuid4().hex}"
    events_client.create_connection(
        Name=name, AuthorizationType=authorization_type, AuthParameters=auth
    )
    try:
        before = events_client.describe_connection(Name=name)
        assert before["ConnectionState"] == "AUTHORIZED"
        assert isinstance(before["CreationTime"], datetime)
        assert isinstance(before["LastModifiedTime"], datetime)
        assert "secret-" not in json.dumps(before, default=str)
        response = events_client.deauthorize_connection(Name=name)
        assert response["ConnectionState"] == "DEAUTHORIZED"
        assert isinstance(response["LastModifiedTime"], datetime)
        after = events_client.describe_connection(Name=name)
        assert after["ConnectionArn"] == before["ConnectionArn"]
        assert after["CreationTime"] == before["CreationTime"]
        assert after["LastAuthorizedTime"] == before["LastAuthorizedTime"]
        assert "AuthParameters" not in after
        assert (
            events_client.deauthorize_connection(Name=name)["ConnectionState"]
            == "DEAUTHORIZED"
        )
        response = events_client.update_connection(Name=name, AuthParameters=auth)
        assert response["ConnectionState"] == "AUTHORIZED"
        assert response["ConnectionArn"] == before["ConnectionArn"]
        response = events_client.delete_connection(Name=name)
        assert response["ConnectionState"] == "DELETING"
        with pytest.raises(ClientError) as error:
            events_client.describe_connection(Name=name)
        assert error.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            events_client.delete_connection(Name=name)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                raise


def test_remove_bus_permissions(events_client):
    name = f"policy-{uuid.uuid4().hex}"
    events_client.create_event_bus(Name=name)
    try:
        for sid in ("first", "second"):
            _assert_unit(
                events_client.put_permission(
                    EventBusName=name,
                    StatementId=sid,
                    Action="events:PutEvents",
                    Principal="*",
                )
            )
        policy = json.loads(events_client.describe_event_bus(Name=name)["Policy"])
        assert {statement["Sid"] for statement in policy["Statement"]} == {
            "first",
            "second",
        }
        _assert_unit(
            events_client.remove_permission(EventBusName=name, StatementId="first")
        )
        policy = json.loads(events_client.describe_event_bus(Name=name)["Policy"])
        assert [statement["Sid"] for statement in policy["Statement"]] == ["second"]
        with pytest.raises(ClientError) as error:
            events_client.remove_permission(EventBusName=name, StatementId="first")
        assert error.value.response["Error"]["Code"] == "ResourceNotFoundException"
        _assert_unit(
            events_client.remove_permission(
                EventBusName=name, RemoveAllPermissions=True
            )
        )
        assert "Policy" not in events_client.describe_event_bus(Name=name)
    finally:
        events_client.delete_event_bus(Name=name)


def test_eventbridge_lists_use_canonical_state(events_client):
    with _partner_source(events_client) as source:
        partners = events_client.list_partner_event_sources(NamePrefix=source)[
            "PartnerEventSources"
        ]
        assert [item["Name"] for item in partners] == [source]
        accounts = events_client.list_partner_event_source_accounts(
            EventSourceName=source
        )["PartnerEventSourceAccounts"]
        assert accounts[0]["Account"] == "000000000000"
        assert isinstance(accounts[0]["CreationTime"], datetime)
        assert accounts[0]["State"] == "PENDING"
        events_client.create_event_bus(Name=source, EventSourceName=source)
        _assert_unit(events_client.deactivate_event_source(Name=source))
        views = events_client.list_event_sources(NamePrefix=source)["EventSources"]
        assert views[0]["State"] == "PENDING"
        _assert_unit(events_client.activate_event_source(Name=source))
        assert (
            events_client.list_event_sources(NamePrefix=source)["EventSources"][0][
                "State"
            ]
            == "ACTIVE"
        )
        _assert_unit(
            events_client.delete_partner_event_source(
                Name=source, Account="000000000000"
            )
        )
        assert (
            events_client.list_partner_event_sources(NamePrefix=source)[
                "PartnerEventSources"
            ]
            == []
        )
        assert events_client.describe_event_bus(Name=source)["Name"] == source


def test_connection_partial_auth_update(events_client):
    name = f"partial-{uuid.uuid4().hex}"
    events_client.create_connection(
        Name=name,
        AuthorizationType="API_KEY",
        AuthParameters={
            "ApiKeyAuthParameters": {
                "ApiKeyName": "keep-name",
                "ApiKeyValue": "old-secret",
            },
        },
    )
    try:
        before = events_client.describe_connection(Name=name)
        assert (
            events_client.update_connection(
                Name=name,
                AuthParameters={
                    "ApiKeyAuthParameters": {"ApiKeyValue": "rotated-secret"},
                },
            )["ConnectionState"]
            == "AUTHORIZED"
        )
        assert (
            events_client.update_connection(
                Name=name,
                AuthParameters={
                    "InvocationHttpParameters": {
                        "HeaderParameters": [
                            {
                                "Key": "visible",
                                "Value": "public",
                                "IsValueSecret": False,
                            },
                            {
                                "Key": "private",
                                "Value": "header-secret",
                                "IsValueSecret": True,
                            },
                        ]
                    },
                },
            )["ConnectionState"]
            == "AUTHORIZED"
        )
        after = events_client.describe_connection(Name=name)
        assert after["ConnectionArn"] == before["ConnectionArn"]
        assert after["CreationTime"] == before["CreationTime"]
        assert (
            after["AuthParameters"]["ApiKeyAuthParameters"]["ApiKeyName"] == "keep-name"
        )
        assert "rotated-secret" not in json.dumps(after, default=str)
        assert "header-secret" not in json.dumps(after, default=str)
        headers = after["AuthParameters"]["InvocationHttpParameters"][
            "HeaderParameters"
        ]
        assert headers[0]["Value"] == "public"
        assert "Value" not in headers[1]
    finally:
        events_client.delete_connection(Name=name)


def test_create_event_bus(events_client):
    resp = events_client.create_event_bus(Name="compat-bus")
    assert "EventBusArn" in resp
    assert "compat-bus" in resp["EventBusArn"]


def test_list_event_buses(events_client):
    events_client.create_event_bus(Name="list-bus")
    resp = events_client.list_event_buses()
    names = [b["Name"] for b in resp["EventBuses"]]
    assert "default" in names
    assert "list-bus" in names


def test_put_and_list_rule(events_client):
    events_client.put_rule(
        Name="compat-rule",
        EventBusName="default",
        EventPattern=json.dumps({"source": ["com.example"]}),
        State="ENABLED",
    )
    resp = events_client.list_rules(EventBusName="default")
    names = [r["Name"] for r in resp["Rules"]]
    assert "compat-rule" in names


def test_put_targets(events_client):
    events_client.put_rule(
        Name="target-compat-rule", EventBusName="default", State="ENABLED"
    )
    resp = events_client.put_targets(
        Rule="target-compat-rule",
        Targets=[{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:my-queue"}],
    )
    assert resp["FailedEntryCount"] == 0


def test_put_events(events_client):
    resp = events_client.put_events(
        Entries=[
            {
                "EventBusName": "default",
                "Source": "com.example",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"key": "value"}),
            }
        ]
    )
    assert resp["FailedEntryCount"] == 0
    assert len(resp["Entries"]) == 1


def test_describe_nonexistent_event_bus(events_client):
    with pytest.raises(ClientError) as exc:
        events_client.describe_event_bus(Name="no-such-bus-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_describe_rule(events_client):
    events_client.put_rule(
        Name="describe-rule", EventBusName="default", State="ENABLED"
    )
    resp = events_client.describe_rule(Name="describe-rule", EventBusName="default")
    assert resp["Name"] == "describe-rule"
    assert resp["State"] == "ENABLED"


def test_remove_targets_and_delete_rule(events_client):
    events_client.put_rule(Name="rm-rule", EventBusName="default", State="ENABLED")
    events_client.put_targets(
        Rule="rm-rule",
        Targets=[{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:000000000000:q"}],
    )
    events_client.remove_targets(Rule="rm-rule", Ids=["t1"])
    events_client.delete_rule(Name="rm-rule", EventBusName="default")
    resp = events_client.list_rules(EventBusName="default")
    names = [r["Name"] for r in resp["Rules"]]
    assert "rm-rule" not in names


def test_delete_event_bus(events_client):
    events_client.create_event_bus(Name="del-bus")
    events_client.delete_event_bus(Name="del-bus")
    resp = events_client.list_event_buses()
    names = [b["Name"] for b in resp["EventBuses"]]
    assert "del-bus" not in names


def test_put_events_dispatches_to_sqs(events_client, sqs_client):
    """PutEvents should route matching events to SQS targets."""
    q = sqs_client.create_queue(QueueName="eb-target-q")
    queue_url = q["QueueUrl"]
    queue_arn = "arn:aws:sqs:us-east-1:000000000000:eb-target-q"

    events_client.put_rule(
        Name="test-rule",
        EventPattern='{"source": ["test.app"]}',
        State="ENABLED",
    )
    events_client.put_targets(
        Rule="test-rule",
        Targets=[{"Id": "1", "Arn": queue_arn}],
    )
    events_client.put_events(
        Entries=[
            {
                "Source": "test.app",
                "DetailType": "test",
                "Detail": json.dumps({"key": "value"}),
            }
        ],
    )
    time.sleep(0.5)
    msgs = sqs_client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)
    assert "Messages" in msgs
    assert len(msgs["Messages"]) >= 1


def test_archive_lifecycle(events_client):
    events_client.create_archive(
        ArchiveName="my-archive",
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
        Description="test archive",
        RetentionDays=7,
    )
    desc = events_client.describe_archive(ArchiveName="my-archive")
    assert desc["ArchiveName"] == "my-archive"
    assert desc["RetentionDays"] == 7

    archives = events_client.list_archives()
    assert any(a["ArchiveName"] == "my-archive" for a in archives["Archives"])

    events_client.update_archive(ArchiveName="my-archive", Description="updated")
    events_client.delete_archive(ArchiveName="my-archive")


def test_replay_lifecycle(events_client):
    events_client.create_archive(
        ArchiveName="replay-src",
        EventSourceArn="arn:aws:events:us-east-1:000000000000:event-bus/default",
    )
    events_client.start_replay(
        ReplayName="my-replay",
        EventSourceArn="arn:aws:events:us-east-1:000000000000:archive/replay-src",
        EventStartTime=time.time() - 3600,
        EventEndTime=time.time(),
        Destination={"Arn": "arn:aws:events:us-east-1:000000000000:event-bus/default"},
    )
    desc = events_client.describe_replay(ReplayName="my-replay")
    assert desc["ReplayName"] == "my-replay"


def test_event_pattern(events_client):
    resp = events_client.test_event_pattern(
        EventPattern='{"source": ["myapp"]}',
        Event=json.dumps(
            {
                "source": "myapp",
                "detail-type": "test",
                "detail": {},
                "time": "2026-04-13T00:00:00Z",
                "region": "us-east-1",
                "account": "000000000000",
                "id": "1",
            }
        ),
    )
    assert resp["Result"] is True

    resp2 = events_client.test_event_pattern(
        EventPattern='{"source": ["other"]}',
        Event=json.dumps(
            {
                "source": "myapp",
                "detail-type": "test",
                "detail": {},
                "time": "2026-04-13T00:00:00Z",
                "region": "us-east-1",
                "account": "000000000000",
                "id": "1",
            }
        ),
    )
    assert resp2["Result"] is False


def test_tag_round_trip(events_client):
    """Tags must persist: the round-trip the generic CRUD engine cannot do."""
    bus = events_client.create_event_bus(Name="tag-bus")
    arn = bus["EventBusArn"]

    events_client.tag_resource(
        ResourceARN=arn,
        Tags=[{"Key": "env", "Value": "dev"}, {"Key": "team", "Value": "platform"}],
    )
    tags = {
        t["Key"]: t["Value"]
        for t in events_client.list_tags_for_resource(ResourceARN=arn)["Tags"]
    }
    assert tags == {"env": "dev", "team": "platform"}

    events_client.untag_resource(ResourceARN=arn, TagKeys=["env"])
    tags = {
        t["Key"]: t["Value"]
        for t in events_client.list_tags_for_resource(ResourceARN=arn)["Tags"]
    }
    assert tags == {"team": "platform"}


def test_tags_do_not_survive_delete(events_client):
    """A recreated bus reuses its ARN, so a delete must drop its tags with it."""
    arn = events_client.create_event_bus(Name="tag-ghost-bus")["EventBusArn"]
    events_client.tag_resource(ResourceARN=arn, Tags=[{"Key": "env", "Value": "dev"}])
    events_client.delete_event_bus(Name="tag-ghost-bus")

    recreated = events_client.create_event_bus(Name="tag-ghost-bus")["EventBusArn"]
    assert recreated == arn
    assert events_client.list_tags_for_resource(ResourceARN=arn)["Tags"] == []


def test_rule_tags_do_not_survive_delete(events_client):
    events_client.put_rule(Name="tag-ghost-rule", ScheduleExpression="rate(5 minutes)")
    arn = events_client.describe_rule(Name="tag-ghost-rule")["Arn"]
    events_client.tag_resource(ResourceARN=arn, Tags=[{"Key": "env", "Value": "dev"}])
    events_client.delete_rule(Name="tag-ghost-rule")

    events_client.put_rule(Name="tag-ghost-rule", ScheduleExpression="rate(5 minutes)")
    assert events_client.list_tags_for_resource(ResourceARN=arn)["Tags"] == []


def test_same_named_rules_on_different_buses_do_not_share_tags(events_client):
    """A rule name is unique per bus, so the bus must be part of the rule ARN."""
    events_client.create_event_bus(Name="bus-a")
    events_client.create_event_bus(Name="bus-b")
    for bus in ("bus-a", "bus-b"):
        events_client.put_rule(
            Name="shared-name", EventBusName=bus, ScheduleExpression="rate(5 minutes)"
        )

    arn_a = events_client.describe_rule(Name="shared-name", EventBusName="bus-a")["Arn"]
    arn_b = events_client.describe_rule(Name="shared-name", EventBusName="bus-b")["Arn"]
    assert arn_a != arn_b

    events_client.tag_resource(ResourceARN=arn_a, Tags=[{"Key": "bus", "Value": "a"}])
    assert events_client.list_tags_for_resource(ResourceARN=arn_b)["Tags"] == []

    # Deleting one rule must not strip the other's tags.
    events_client.delete_rule(Name="shared-name", EventBusName="bus-b")
    tags = events_client.list_tags_for_resource(ResourceARN=arn_a)["Tags"]
    assert tags == [{"Key": "bus", "Value": "a"}]


def test_tags_are_per_resource(events_client):
    """Two resources do not share a tag set."""
    a = events_client.create_event_bus(Name="tag-bus-a")["EventBusArn"]
    b = events_client.create_event_bus(Name="tag-bus-b")["EventBusArn"]

    events_client.tag_resource(ResourceARN=a, Tags=[{"Key": "only", "Value": "a"}])
    assert events_client.list_tags_for_resource(ResourceARN=b)["Tags"] == []
