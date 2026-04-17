import pytest
from botocore.exceptions import ClientError


def test_put_and_get_object(s3_client):
    s3_client.create_bucket(Bucket="test-obj-bucket")
    s3_client.put_object(
        Bucket="test-obj-bucket",
        Key="hello.txt",
        Body=b"hello world",
        ContentType="text/plain",
    )
    response = s3_client.get_object(Bucket="test-obj-bucket", Key="hello.txt")
    body = response["Body"].read()
    assert body == b"hello world"
    assert response["ContentType"] == "text/plain"


def test_delete_object(s3_client):
    s3_client.create_bucket(Bucket="test-del-obj-bucket")
    s3_client.put_object(Bucket="test-del-obj-bucket", Key="file.txt", Body=b"data")
    s3_client.delete_object(Bucket="test-del-obj-bucket", Key="file.txt")
    try:
        s3_client.get_object(Bucket="test-del-obj-bucket", Key="file.txt")
        assert False, "Expected NoSuchKey error"
    except s3_client.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] == "NoSuchKey"


def test_head_object(s3_client):
    s3_client.create_bucket(Bucket="test-head-obj-bucket")
    s3_client.put_object(Bucket="test-head-obj-bucket", Key="file.txt", Body=b"12345")
    response = s3_client.head_object(Bucket="test-head-obj-bucket", Key="file.txt")
    assert response["ContentLength"] == 5


def test_nested_key(s3_client):
    s3_client.create_bucket(Bucket="test-nested-bucket")
    s3_client.put_object(
        Bucket="test-nested-bucket", Key="a/b/c/deep.txt", Body=b"deep"
    )
    response = s3_client.get_object(Bucket="test-nested-bucket", Key="a/b/c/deep.txt")
    assert response["Body"].read() == b"deep"


def test_get_object_nonexistent_bucket(s3_client):
    with pytest.raises(ClientError) as exc:
        s3_client.get_object(Bucket="no-such-bucket-xyz", Key="k")
    assert exc.value.response["Error"]["Code"] in ("NoSuchBucket", "NoSuchKey")


def test_copy_object(s3_client):
    s3_client.create_bucket(Bucket="copy-src-bucket")
    s3_client.put_object(Bucket="copy-src-bucket", Key="src.txt", Body=b"original")
    s3_client.copy_object(
        Bucket="copy-src-bucket",
        Key="dst.txt",
        CopySource={"Bucket": "copy-src-bucket", "Key": "src.txt"},
    )
    resp = s3_client.get_object(Bucket="copy-src-bucket", Key="dst.txt")
    assert resp["Body"].read() == b"original"


def test_list_objects_v2_prefix(s3_client):
    s3_client.create_bucket(Bucket="listv2-bucket")
    s3_client.put_object(Bucket="listv2-bucket", Key="photos/a.jpg", Body=b"a")
    s3_client.put_object(Bucket="listv2-bucket", Key="photos/b.jpg", Body=b"b")
    s3_client.put_object(Bucket="listv2-bucket", Key="docs/c.txt", Body=b"c")
    resp = s3_client.list_objects_v2(Bucket="listv2-bucket", Prefix="photos/")
    keys = [o["Key"] for o in resp.get("Contents", [])]
    assert "photos/a.jpg" in keys
    assert "photos/b.jpg" in keys
    assert "docs/c.txt" not in keys


def test_list_objects_v2_delimiter(s3_client):
    s3_client.create_bucket(Bucket="listv2-delim-bucket")
    s3_client.put_object(Bucket="listv2-delim-bucket", Key="a/1.txt", Body=b"1")
    s3_client.put_object(Bucket="listv2-delim-bucket", Key="b/2.txt", Body=b"2")
    resp = s3_client.list_objects_v2(Bucket="listv2-delim-bucket", Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    assert "a/" in prefixes
    assert "b/" in prefixes
