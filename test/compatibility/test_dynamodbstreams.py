import time

import pytest


def test_list_streams(dynamodbstreams_client):
    resp = dynamodbstreams_client.list_streams()
    assert "Streams" in resp
    assert isinstance(resp["Streams"], list)


def test_list_streams_with_table_filter(dynamodbstreams_client):
    resp = dynamodbstreams_client.list_streams(TableName="nonexistent-table")
    assert "Streams" in resp
    assert isinstance(resp["Streams"], list)


@pytest.fixture
def streams_table(dynamodb_client):
    """Create a DynamoDB table with streams enabled for the integration tests."""
    name = f"stream-test-{int(time.time() * 1000)}"
    dynamodb_client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
    except Exception:
        pass


def test_create_table_populates_latest_stream_arn(dynamodb_client, streams_table):
    desc = dynamodb_client.describe_table(TableName=streams_table)["Table"]
    assert desc.get("LatestStreamArn", "").startswith("arn:aws:dynamodb:"), desc
    assert desc.get("StreamSpecification", {}).get("StreamEnabled") is True


def test_streams_receives_putitem_events(
    dynamodb_client, dynamodbstreams_client, streams_table
):
    arn = dynamodb_client.describe_table(TableName=streams_table)["Table"][
        "LatestStreamArn"
    ]

    # Generate three events: INSERT, MODIFY (same key), REMOVE.
    dynamodb_client.put_item(
        TableName=streams_table,
        Item={"id": {"S": "a"}, "v": {"S": "one"}},
    )
    dynamodb_client.put_item(
        TableName=streams_table,
        Item={"id": {"S": "a"}, "v": {"S": "two"}},
    )
    dynamodb_client.delete_item(TableName=streams_table, Key={"id": {"S": "a"}})

    # Describe + shard iterator + GetRecords.
    shards = dynamodbstreams_client.describe_stream(StreamArn=arn)["StreamDescription"][
        "Shards"
    ]
    assert shards, "expected at least one shard"

    iter_resp = dynamodbstreams_client.get_shard_iterator(
        StreamArn=arn,
        ShardId=shards[0]["ShardId"],
        ShardIteratorType="TRIM_HORIZON",
    )
    records = dynamodbstreams_client.get_records(
        ShardIterator=iter_resp["ShardIterator"]
    )["Records"]

    event_names = [r["eventName"] for r in records]
    assert event_names == ["INSERT", "MODIFY", "REMOVE"], event_names

    # MODIFY must have both images, REMOVE only OldImage.
    modify = records[1]["dynamodb"]
    assert "NewImage" in modify and "OldImage" in modify
    remove = records[2]["dynamodb"]
    assert "OldImage" in remove and "NewImage" not in remove


def test_stream_tags_lifecycle_raw(
    dynamodb_client, dynamodbstreams_client, streams_table
):
    """boto3's dynamodbstreams client only exposes 4 operations, so the
    extended devcloud ops (AddTagsToStream etc.) must be invoked through the
    raw JSON 1.0 protocol."""
    import json
    import urllib.request

    arn = dynamodb_client.describe_table(TableName=streams_table)["Table"][
        "LatestStreamArn"
    ]
    endpoint = dynamodbstreams_client.meta.endpoint_url

    def _call(target, payload):
        req = urllib.request.Request(
            endpoint,
            method="POST",
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Target": f"DynamoDBStreams_20120810.{target}",
            },
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read() or b"{}")

    _call(
        "AddTagsToStream", {"StreamArn": arn, "Tags": [{"Key": "env", "Value": "prod"}]}
    )
    tags = _call("ListTagsOfStream", {"StreamArn": arn}).get("Tags", [])
    assert any(t["Key"] == "env" and t["Value"] == "prod" for t in tags)

    _call("RemoveTagsFromStream", {"StreamArn": arn, "TagKeys": ["env"]})
    tags_after = _call("ListTagsOfStream", {"StreamArn": arn}).get("Tags", [])
    assert all(t["Key"] != "env" for t in tags_after)
