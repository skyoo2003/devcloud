import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_stream(kinesis_client):
    kinesis_client.create_stream(StreamName="compat-stream", ShardCount=1)

    resp = kinesis_client.describe_stream(StreamName="compat-stream")
    desc = resp["StreamDescription"]
    assert desc["StreamName"] == "compat-stream"
    assert desc["StreamStatus"] in ("CREATING", "ACTIVE")


def test_list_streams(kinesis_client):
    kinesis_client.create_stream(StreamName="list-stream-1", ShardCount=1)
    resp = kinesis_client.list_streams()
    assert "StreamNames" in resp
    assert "list-stream-1" in resp["StreamNames"]


def test_describe_stream_summary(kinesis_client):
    kinesis_client.create_stream(StreamName="summary-stream", ShardCount=1)
    resp = kinesis_client.describe_stream_summary(StreamName="summary-stream")
    assert "StreamDescriptionSummary" in resp
    assert resp["StreamDescriptionSummary"]["StreamName"] == "summary-stream"


def test_delete_stream(kinesis_client):
    kinesis_client.create_stream(StreamName="del-stream", ShardCount=1)
    kinesis_client.delete_stream(StreamName="del-stream")

    with pytest.raises(ClientError):
        kinesis_client.describe_stream(StreamName="del-stream")
