"""Verify copied multipart data through the real boto3/gateway boundary."""

import datetime
import hashlib
import uuid

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def copy_upload(s3_client):
    source = "copy-source-" + uuid.uuid4().hex[:12]
    target = "copy-target-" + uuid.uuid4().hex[:12]
    key = "한글 + %2F ? #/source"
    data = b"0123456789" + b"x" * (6 * 1024 * 1024)
    for bucket in [source, target]:
        s3_client.create_bucket(Bucket=bucket)
    put = s3_client.put_object(Bucket=source, Key=key, Body=data)
    upload = s3_client.create_multipart_upload(Bucket=target, Key="result")["UploadId"]
    yield source, target, key, data, put["ETag"], upload
    for bucket in [source, target]:
        for item in s3_client.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            s3_client.abort_multipart_upload(
                Bucket=bucket, Key=item["Key"], UploadId=item["UploadId"]
            )
        for item in s3_client.list_objects_v2(Bucket=bucket).get("Contents", []):
            s3_client.delete_object(Bucket=bucket, Key=item["Key"])
        s3_client.delete_bucket(Bucket=bucket)


def test_upload_part_copy_full_range_and_complete(s3_client, copy_upload):
    source, target, key, data, etag, upload = copy_upload
    full = s3_client.upload_part_copy(
        Bucket=target,
        Key="result",
        UploadId=upload,
        PartNumber=1,
        CopySource={"Bucket": source, "Key": key},
        CopySourceIfMatch=etag,
    )["CopyPartResult"]
    assert full["ETag"] == '"' + hashlib.md5(data).hexdigest() + '"'
    assert isinstance(full["LastModified"], datetime.datetime)
    ranged = s3_client.upload_part_copy(
        Bucket=target,
        Key="result",
        UploadId=upload,
        PartNumber=2,
        CopySource={"Bucket": source, "Key": key},
        CopySourceRange="bytes=2-5",
    )["CopyPartResult"]
    assert ranged["ETag"] == '"' + hashlib.md5(b"2345").hexdigest() + '"'
    parts = s3_client.list_parts(Bucket=target, Key="result", UploadId=upload)["Parts"]
    assert [(p["PartNumber"], p["Size"], p["ETag"]) for p in parts] == [
        (1, len(data), full["ETag"]),
        (2, 4, ranged["ETag"]),
    ]
    s3_client.complete_multipart_upload(
        Bucket=target,
        Key="result",
        UploadId=upload,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": full["ETag"]},
                {"PartNumber": 2, "ETag": ranged["ETag"]},
            ]
        },
    )
    assert (
        s3_client.get_object(Bucket=target, Key="result")["Body"].read()
        == data + b"2345"
    )
    assert s3_client.get_object(Bucket=source, Key=key)["Body"].read() == data


def test_upload_part_copy_errors_preserve_existing_part(s3_client, copy_upload):
    source, target, key, data, etag, upload = copy_upload
    old = s3_client.upload_part(
        Bucket=target, Key="result", UploadId=upload, PartNumber=1, Body=b"keep"
    )["ETag"]
    args = dict(
        Bucket=target,
        Key="result",
        UploadId=upload,
        PartNumber=1,
        CopySource={"Bucket": source, "Key": key},
    )
    for changes, code, status in [
        ({"CopySourceIfMatch": '"wrong"'}, "PreconditionFailed", 412),
        ({"CopySourceIfNoneMatch": etag}, "PreconditionFailed", 412),
        ({"CopySourceRange": "bytes=5-2"}, "InvalidArgument", 400),
        ({"PartNumber": 10001}, "InvalidArgument", 400),
        ({"CopySource": {"Bucket": source, "Key": "missing"}}, "NoSuchKey", 404),
        ({"Key": "wrong-target"}, "NoSuchUpload", 404),
        (
            {"CopySource": {"Bucket": source, "Key": key, "VersionId": "old"}},
            "NotImplemented",
            501,
        ),
        ({"ExpectedBucketOwner": "111111111111"}, "AccessDenied", 403),
    ]:
        with pytest.raises(ClientError) as exc:
            s3_client.upload_part_copy(**(args | changes))
        assert exc.value.response["Error"]["Code"] == code
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == status
        parts = s3_client.list_parts(Bucket=target, Key="result", UploadId=upload)[
            "Parts"
        ]
        assert [(p["PartNumber"], p["Size"], p["ETag"]) for p in parts] == [(1, 4, old)]
    s3_client.complete_multipart_upload(
        Bucket=target,
        Key="result",
        UploadId=upload,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": old}]},
    )
    assert s3_client.get_object(Bucket=target, Key="result")["Body"].read() == b"keep"
    assert s3_client.get_object(Bucket=source, Key=key)["Body"].read() == data


def test_upload_part_copy_condition_precedence(s3_client, copy_upload):
    source, target, key, _, etag, upload = copy_upload
    modified = s3_client.head_object(Bucket=source, Key=key)["LastModified"]
    args = dict(
        Bucket=target,
        Key="result",
        UploadId=upload,
        PartNumber=1,
        CopySource={"Bucket": source, "Key": key},
    )
    result = s3_client.upload_part_copy(
        **args,
        CopySourceIfMatch=etag,
        CopySourceIfUnmodifiedSince=modified - datetime.timedelta(hours=1),
    )
    assert result["CopyPartResult"]["ETag"] == '"' + etag.strip('"') + '"'
    with pytest.raises(ClientError) as exc:
        s3_client.upload_part_copy(
            **args,
            CopySourceIfNoneMatch=etag,
            CopySourceIfModifiedSince=modified - datetime.timedelta(hours=1),
        )
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
