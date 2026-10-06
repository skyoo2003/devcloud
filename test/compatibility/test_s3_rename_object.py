"""Directory rename contracts through real boto3 automatic session authentication."""

import datetime
import uuid
from urllib.parse import quote, urlsplit, urlunsplit

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def rename_client(endpoint, *, control=False, credentials=None):
    settings = {"addressing_style": "path"}
    if control:
        settings["disable_s3_express_session_auth"] = True
    auth = dict(aws_access_key_id="test", aws_secret_access_key="test")
    if credentials:
        auth = dict(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        **auth,
        config=Config(
            s3=settings, connect_timeout=5, read_timeout=10, retries={"max_attempts": 0}
        ),
    )
    if credentials:

        def attach_directory_token(request, **kwargs):
            request.headers["x-amz-s3session-token"] = credentials["SessionToken"]

        client.meta.events.register("before-sign.s3", attach_directory_token)
    return client


@pytest.fixture
def rename_clients(s3_client):
    endpoint = urlsplit(s3_client.meta.endpoint_url)
    if endpoint.hostname in {"localhost", "127.0.0.1", "::1"}:
        endpoint = endpoint._replace(
            netloc="127.0.0.1" + (f":{endpoint.port}" if endpoint.port else "")
        )
    url = urlunsplit(endpoint)
    control, data = rename_client(url, control=True), rename_client(url)
    yield control, data
    control.close()
    data.close()


@pytest.fixture
def directory_rename_bucket(rename_clients):
    control, data = rename_clients
    bucket = "rename-" + uuid.uuid4().hex[:12] + "--use1-az1--x-s3"
    control.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={
            "Bucket": {"Type": "Directory", "DataRedundancy": "SingleAvailabilityZone"},
            "Location": {"Type": "AvailabilityZone", "Name": "use1-az1"},
        },
    )
    try:
        yield bucket
    finally:
        for upload in data.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
            data.abort_multipart_upload(
                Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )
        for obj in data.list_objects_v2(Bucket=bucket).get("Contents", []):
            data.delete_object(Bucket=bucket, Key=obj["Key"])
        control.delete_bucket(Bucket=bucket)


def expect_error(code, status, call, **kwargs):
    with pytest.raises(ClientError) as caught:
        call(**kwargs)
    assert caught.value.response["Error"]["Code"] == code
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == status
    return caught.value


def test_rename_object_moves_and_preserves_metadata(
    rename_clients, directory_rename_bucket
):
    _, data = rename_clients
    bucket = directory_rename_bucket
    source, destination = "한글 + %2F ? #/source", "nested/한글 + %2F ? #"
    original = b"unchanged object bytes"
    data.put_object(Bucket=bucket, Key=source, Body=original, ContentType="text/custom")
    data.put_object_tagging(
        Bucket=bucket,
        Key=source,
        Tagging={"TagSet": [{"Key": "source", "Value": "keep"}]},
    )
    before = data.head_object(Bucket=bucket, Key=source)
    result = data.rename_object(
        Bucket=bucket, Key=destination, RenameSource=quote(source, safe="/")
    )
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert data.get_object(Bucket=bucket, Key=destination)["Body"].read() == original
    after = data.head_object(Bucket=bucket, Key=destination)
    for name in [
        "ETag",
        "LastModified",
        "ContentType",
        "ContentLength",
        "StorageClass",
    ]:
        assert after[name] == before[name]
    assert after["StorageClass"] == "EXPRESS_ONEZONE"
    expect_error("NoSuchKey", 404, data.get_object, Bucket=bucket, Key=source)
    assert [o["Key"] for o in data.list_objects_v2(Bucket=bucket)["Contents"]] == [
        destination
    ]
    assert data.get_object_tagging(Bucket=bucket, Key=destination)["TagSet"] == [
        {"Key": "source", "Value": "keep"}
    ]


def test_rename_object_source_forms_and_conditions(
    rename_clients, directory_rename_bucket
):
    _, data = rename_clients
    bucket = directory_rename_bucket
    source, destination = "source", "destination"
    data.put_object(Bucket=bucket, Key=source, Body=b"source")
    data.put_object(Bucket=bucket, Key=destination, Body=b"destination")
    src = data.head_object(Bucket=bucket, Key=source)
    dst = data.head_object(Bucket=bucket, Key=destination)
    hour = datetime.timedelta(hours=1)
    args = dict(Bucket=bucket, Key=destination, RenameSource=source)
    for condition in [
        {"SourceIfMatch": '"wrong"'},
        {"SourceIfNoneMatch": src["ETag"]},
        {"SourceIfModifiedSince": src["LastModified"] + hour},
        {"SourceIfUnmodifiedSince": src["LastModified"] - hour},
        {"DestinationIfMatch": '"wrong"'},
        {"DestinationIfNoneMatch": "*"},
        {"DestinationIfModifiedSince": dst["LastModified"] + hour},
        {"DestinationIfUnmodifiedSince": dst["LastModified"] - hour},
    ]:
        expect_error(
            "PreconditionFailed", 412, data.rename_object, **(args | condition)
        )
        assert data.get_object(Bucket=bucket, Key=source)["Body"].read() == b"source"
        assert (
            data.get_object(Bucket=bucket, Key=destination)["Body"].read()
            == b"destination"
        )
    result = data.rename_object(
        **args,
        SourceIfMatch=src["ETag"],
        SourceIfUnmodifiedSince=src["LastModified"] - hour,
        DestinationIfMatch=dst["ETag"],
        DestinationIfUnmodifiedSince=dst["LastModified"] - hour,
    )
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
    # Key-only, leading slash, and explicit same-bucket forms all address the same source.
    for encoded in ["/destination", bucket + "/destination"]:
        result = data.rename_object(
            Bucket=bucket,
            Key="destination",
            RenameSource=encoded,
            SourceIfNoneMatch='"different"',
            DestinationIfNoneMatch='"different"',
            SourceIfModifiedSince=src["LastModified"] + hour,
            DestinationIfModifiedSince=src["LastModified"] + hour,
        )
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
    data.rename_object(
        Bucket=bucket,
        Key="missing-destination",
        RenameSource="destination",
        DestinationIfNoneMatch="*",
    )
    assert (
        data.get_object(Bucket=bucket, Key="missing-destination")["Body"].read()
        == b"source"
    )


def test_rename_object_token_replay_and_mismatch(
    rename_clients, directory_rename_bucket
):
    _, data = rename_clients
    bucket = directory_rename_bucket
    data.put_object(Bucket=bucket, Key="source", Body=b"initial")
    args = dict(
        Bucket=bucket,
        Key="destination",
        RenameSource="source",
        ClientToken="sdk-fixed-token",
    )
    data.rename_object(**args)
    data.put_object(Bucket=bucket, Key="source", Body=b"recreated")
    data.put_object(Bucket=bucket, Key="destination", Body=b"edited")
    assert data.rename_object(**args)["ResponseMetadata"]["HTTPStatusCode"] == 200
    fresh = rename_client(data.meta.endpoint_url)
    try:
        assert (
            fresh.rename_object(**(args | {"RenameSource": bucket + "/source"}))[
                "ResponseMetadata"
            ]["HTTPStatusCode"]
            == 200
        )
    finally:
        fresh.close()
    assert data.get_object(Bucket=bucket, Key="source")["Body"].read() == b"recreated"
    assert data.get_object(Bucket=bucket, Key="destination")["Body"].read() == b"edited"
    error = expect_error(
        "IdempotencyParameterMismatch",
        400,
        data.rename_object,
        **(args | {"Key": "other"}),
    )
    assert isinstance(error, data.exceptions.IdempotencyParameterMismatch)
    expect_error(
        "IdempotencyParameterMismatch",
        400,
        data.rename_object,
        **args,
        DestinationIfNoneMatch="*",
    )


def test_rename_object_sessions_and_errors(rename_clients, directory_rename_bucket):
    control, data = rename_clients
    bucket = directory_rename_bucket
    data.put_object(Bucket=bucket, Key="source", Body=b"source")
    for mode in ["ReadOnly", "ReadWrite"]:
        start = datetime.datetime.now(datetime.timezone.utc)
        credentials = control.create_session(Bucket=bucket, SessionMode=mode)[
            "Credentials"
        ]
        assert isinstance(credentials["Expiration"], datetime.datetime)
        assert 295 <= (credentials["Expiration"] - start).total_seconds() <= 300
        assert all(
            credentials[name]
            for name in ["AccessKeyId", "SecretAccessKey", "SessionToken"]
        )
        scoped = rename_client(
            control.meta.endpoint_url, control=True, credentials=credentials
        )
        try:
            assert (
                scoped.get_object(Bucket=bucket, Key="source")["Body"].read()
                == b"source"
            )
            kwargs = dict(
                Bucket=bucket,
                Key="destination",
                RenameSource="source",
                ClientToken=mode,
            )
            if mode == "ReadOnly":
                expect_error("AccessDenied", 403, scoped.rename_object, **kwargs)
            else:
                assert (
                    scoped.rename_object(**kwargs)["ResponseMetadata"]["HTTPStatusCode"]
                    == 200
                )
                scoped.rename_object(
                    Bucket=bucket, Key="source", RenameSource="destination"
                )
        finally:
            scoped.close()
    expect_error(
        "NotImplemented",
        501,
        control.create_session,
        Bucket=bucket,
        ServerSideEncryption="AES256",
    )
    ordinary = "rename-general-" + uuid.uuid4().hex[:12]
    control.create_bucket(Bucket=ordinary)
    try:
        expect_error("InvalidRequest", 400, control.create_session, Bucket=ordinary)
        expect_error(
            "InvalidRequest",
            400,
            control.rename_object,
            Bucket=ordinary,
            Key="dest",
            RenameSource="source",
        )
    finally:
        control.delete_bucket(Bucket=ordinary)
