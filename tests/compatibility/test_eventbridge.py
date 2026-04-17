import json
import time

import pytest
from botocore.exceptions import ClientError


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
