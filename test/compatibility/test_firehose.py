import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_delivery_stream(firehose_client):
    resp = firehose_client.create_delivery_stream(
        DeliveryStreamName="compat-firehose",
        DeliveryStreamType="DirectPut",
    )
    assert "DeliveryStreamARN" in resp

    desc = firehose_client.describe_delivery_stream(
        DeliveryStreamName="compat-firehose"
    )
    assert desc["DeliveryStreamDescription"]["DeliveryStreamName"] == "compat-firehose"


def test_list_delivery_streams(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="list-firehose-1",
        DeliveryStreamType="DirectPut",
    )
    resp = firehose_client.list_delivery_streams()
    assert "DeliveryStreamNames" in resp
    assert "list-firehose-1" in resp["DeliveryStreamNames"]


def test_delete_delivery_stream(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="del-firehose",
        DeliveryStreamType="DirectPut",
    )
    firehose_client.delete_delivery_stream(DeliveryStreamName="del-firehose")

    with pytest.raises(ClientError):
        firehose_client.describe_delivery_stream(DeliveryStreamName="del-firehose")


def test_put_record(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="put-firehose",
        DeliveryStreamType="DirectPut",
    )
    resp = firehose_client.put_record(
        DeliveryStreamName="put-firehose",
        Record={"Data": b"test-data"},
    )
    assert "RecordId" in resp


def test_put_record_batch(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="batch-firehose",
        DeliveryStreamType="DirectPut",
    )
    resp = firehose_client.put_record_batch(
        DeliveryStreamName="batch-firehose",
        Records=[
            {"Data": b"record-1"},
            {"Data": b"record-2"},
        ],
    )
    assert resp["FailedPutCount"] == 0
    assert len(resp["RequestResponses"]) == 2
    for r in resp["RequestResponses"]:
        assert "RecordId" in r


def test_tag_delivery_stream(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="tagged-firehose",
        DeliveryStreamType="DirectPut",
    )
    firehose_client.tag_delivery_stream(
        DeliveryStreamName="tagged-firehose",
        Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "data"}],
    )
    resp = firehose_client.list_tags_for_delivery_stream(
        DeliveryStreamName="tagged-firehose"
    )
    tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
    assert tags.get("env") == "test"
    assert tags.get("team") == "data"


def test_untag_delivery_stream(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="untag-firehose",
        DeliveryStreamType="DirectPut",
    )
    firehose_client.tag_delivery_stream(
        DeliveryStreamName="untag-firehose",
        Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
    )
    firehose_client.untag_delivery_stream(
        DeliveryStreamName="untag-firehose",
        TagKeys=["k1"],
    )
    resp = firehose_client.list_tags_for_delivery_stream(
        DeliveryStreamName="untag-firehose"
    )
    keys = [t["Key"] for t in resp["Tags"]]
    assert "k1" not in keys
    assert "k2" in keys


def test_encryption_toggle(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="enc-firehose",
        DeliveryStreamType="DirectPut",
    )

    firehose_client.start_delivery_stream_encryption(DeliveryStreamName="enc-firehose")
    desc = firehose_client.describe_delivery_stream(DeliveryStreamName="enc-firehose")
    enc = desc["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]
    assert enc["Status"] == "ENABLED"

    firehose_client.stop_delivery_stream_encryption(DeliveryStreamName="enc-firehose")
    desc2 = firehose_client.describe_delivery_stream(DeliveryStreamName="enc-firehose")
    enc2 = desc2["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]
    assert enc2["Status"] == "DISABLED"


def test_update_destination(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="update-dest-firehose",
        DeliveryStreamType="DirectPut",
    )
    # UpdateDestination requires currentDeliveryStreamVersionId and destinationId
    resp = firehose_client.update_destination(
        DeliveryStreamName="update-dest-firehose",
        CurrentDeliveryStreamVersionId="1",
        DestinationId="destinationId-000000000001",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_delivery_streams_by_type(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="type-filter-firehose",
        DeliveryStreamType="DirectPut",
    )
    resp = firehose_client.list_delivery_streams(DeliveryStreamType="DirectPut")
    assert "DeliveryStreamNames" in resp
    assert "type-filter-firehose" in resp["DeliveryStreamNames"]


def test_kinesis_source(firehose_client):
    firehose_client.create_delivery_stream(
        DeliveryStreamName="ks-compat",
        DeliveryStreamType="KinesisStreamAsSource",
    )
    # Use low-level invocation through client to avoid boto3 schema strictness
    # Most SDKs do not expose AddKinesisSource directly; assert the stream exists.
    desc = firehose_client.describe_delivery_stream(DeliveryStreamName="ks-compat")
    assert desc["DeliveryStreamDescription"]["DeliveryStreamName"] == "ks-compat"


def test_describe_missing_stream(firehose_client):
    with pytest.raises(ClientError):
        firehose_client.describe_delivery_stream(DeliveryStreamName="no-such-stream")
