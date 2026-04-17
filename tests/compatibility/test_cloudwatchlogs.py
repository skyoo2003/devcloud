import time

import pytest
from botocore.exceptions import ClientError


def test_create_log_group(logs_client):
    logs_client.create_log_group(logGroupName="/compat/group")
    resp = logs_client.describe_log_groups(logGroupNamePrefix="/compat/group")
    assert any(g["logGroupName"] == "/compat/group" for g in resp["logGroups"])


def test_create_and_describe_log_stream(logs_client):
    logs_client.create_log_group(logGroupName="/compat/stream-group")
    logs_client.create_log_stream(
        logGroupName="/compat/stream-group", logStreamName="my-stream"
    )
    resp = logs_client.describe_log_streams(logGroupName="/compat/stream-group")
    assert any(s["logStreamName"] == "my-stream" for s in resp["logStreams"])


def test_put_and_get_log_events(logs_client):
    logs_client.create_log_group(logGroupName="/compat/events-group")
    logs_client.create_log_stream(
        logGroupName="/compat/events-group", logStreamName="s1"
    )
    now_ms = int(time.time() * 1000)
    logs_client.put_log_events(
        logGroupName="/compat/events-group",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now_ms, "message": "hello"},
            {"timestamp": now_ms + 1000, "message": "world"},
        ],
    )
    resp = logs_client.get_log_events(
        logGroupName="/compat/events-group", logStreamName="s1"
    )
    messages = [e["message"] for e in resp["events"]]
    assert "hello" in messages
    assert "world" in messages


def test_filter_log_events(logs_client):
    logs_client.create_log_group(logGroupName="/compat/filter-group")
    logs_client.create_log_stream(
        logGroupName="/compat/filter-group", logStreamName="s1"
    )
    now_ms = int(time.time() * 1000)
    logs_client.put_log_events(
        logGroupName="/compat/filter-group",
        logStreamName="s1",
        logEvents=[
            {"timestamp": now_ms, "message": "ERROR: crashed"},
            {"timestamp": now_ms + 1000, "message": "INFO: running"},
        ],
    )
    resp = logs_client.filter_log_events(
        logGroupName="/compat/filter-group", filterPattern="ERROR"
    )
    assert len(resp["events"]) == 1
    assert "ERROR" in resp["events"][0]["message"]


def test_delete_log_group(logs_client):
    logs_client.create_log_group(logGroupName="/compat/del-group")
    logs_client.delete_log_group(logGroupName="/compat/del-group")
    resp = logs_client.describe_log_groups(logGroupNamePrefix="/compat/del-group")
    assert not any(g["logGroupName"] == "/compat/del-group" for g in resp["logGroups"])


def test_put_to_nonexistent_group(logs_client):
    with pytest.raises(ClientError) as exc:
        logs_client.put_log_events(
            logGroupName="/no/such/group",
            logStreamName="stream",
            logEvents=[{"timestamp": 1000, "message": "test"}],
        )
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_describe_log_groups(logs_client):
    logs_client.create_log_group(logGroupName="/compat/desc-groups")
    resp = logs_client.describe_log_groups(logGroupNamePrefix="/compat/desc-groups")
    names = [g["logGroupName"] for g in resp["logGroups"]]
    assert "/compat/desc-groups" in names


def test_delete_log_stream(logs_client):
    logs_client.create_log_group(logGroupName="/compat/del-stream-grp")
    logs_client.create_log_stream(
        logGroupName="/compat/del-stream-grp", logStreamName="to-delete"
    )
    logs_client.delete_log_stream(
        logGroupName="/compat/del-stream-grp", logStreamName="to-delete"
    )
    resp = logs_client.describe_log_streams(logGroupName="/compat/del-stream-grp")
    names = [s["logStreamName"] for s in resp["logStreams"]]
    assert "to-delete" not in names


def test_retention_policy(logs_client):
    logs_client.create_log_group(logGroupName="retain-group")
    logs_client.put_retention_policy(logGroupName="retain-group", retentionInDays=7)
    groups = logs_client.describe_log_groups(logGroupNamePrefix="retain-group")
    assert groups["logGroups"][0].get("retentionInDays") == 7
    logs_client.delete_retention_policy(logGroupName="retain-group")


def test_metric_filter(logs_client):
    logs_client.create_log_group(logGroupName="mf-group")
    logs_client.put_metric_filter(
        logGroupName="mf-group",
        filterName="error-filter",
        filterPattern="ERROR",
        metricTransformations=[
            {
                "metricName": "ErrorCount",
                "metricNamespace": "MyApp",
                "metricValue": "1",
            }
        ],
    )
    resp = logs_client.describe_metric_filters(logGroupName="mf-group")
    assert any(f["filterName"] == "error-filter" for f in resp["metricFilters"])
    logs_client.delete_metric_filter(logGroupName="mf-group", filterName="error-filter")


def test_subscription_filter(logs_client):
    logs_client.create_log_group(logGroupName="sf-group")
    logs_client.put_subscription_filter(
        logGroupName="sf-group",
        filterName="to-lambda",
        filterPattern="",
        destinationArn="arn:aws:lambda:us-east-1:000000000000:function:processor",
    )
    resp = logs_client.describe_subscription_filters(logGroupName="sf-group")
    assert any(f["filterName"] == "to-lambda" for f in resp["subscriptionFilters"])
    logs_client.delete_subscription_filter(
        logGroupName="sf-group", filterName="to-lambda"
    )


def test_log_group_tags(logs_client):
    logs_client.create_log_group(logGroupName="tagged-group")
    arn = "arn:aws:logs:us-east-1:000000000000:log-group:tagged-group:*"
    logs_client.tag_resource(resourceArn=arn, tags={"env": "test"})
    resp = logs_client.list_tags_for_resource(resourceArn=arn)
    assert resp["tags"].get("env") == "test"
    logs_client.untag_resource(resourceArn=arn, tagKeys=["env"])
