import pytest
from botocore.exceptions import ClientError


def test_create_bucket(s3_client):
    response = s3_client.create_bucket(Bucket="test-create-bucket")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_buckets(s3_client):
    s3_client.create_bucket(Bucket="test-list-a")
    s3_client.create_bucket(Bucket="test-list-b")
    response = s3_client.list_buckets()
    names = [b["Name"] for b in response["Buckets"]]
    assert "test-list-a" in names
    assert "test-list-b" in names


def test_delete_bucket(s3_client):
    s3_client.create_bucket(Bucket="test-delete-bucket")
    s3_client.delete_bucket(Bucket="test-delete-bucket")
    response = s3_client.list_buckets()
    names = [b["Name"] for b in response["Buckets"]]
    assert "test-delete-bucket" not in names


def test_head_bucket(s3_client):
    s3_client.create_bucket(Bucket="test-head-bucket")
    s3_client.head_bucket(Bucket="test-head-bucket")


def test_head_bucket_nonexistent(s3_client):
    with pytest.raises(ClientError) as exc:
        s3_client.head_bucket(Bucket="nonexistent-bucket-xyz")
    assert exc.value.response["Error"]["Code"] in ("404", "NoSuchBucket")


def test_create_bucket_duplicate(s3_client):
    s3_client.create_bucket(Bucket="dup-bucket")
    resp = s3_client.create_bucket(Bucket="dup-bucket")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
