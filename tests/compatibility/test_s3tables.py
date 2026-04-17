import json

import pytest
from botocore.exceptions import ClientError


def test_create_and_get_table_bucket(s3tables_client):
    resp = s3tables_client.create_table_bucket(name="compat-bucket")
    assert "arn" in resp
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = s3tables_client.get_table_bucket(tableBucketARN=resp["arn"])
    assert get_resp["name"] == "compat-bucket"


def test_list_table_buckets(s3tables_client):
    s3tables_client.create_table_bucket(name="list-bucket-1")
    resp = s3tables_client.list_table_buckets()
    assert "tableBuckets" in resp
    names = [b["name"] for b in resp["tableBuckets"]]
    assert "list-bucket-1" in names


def test_delete_table_bucket(s3tables_client):
    s3tables_client.create_table_bucket(name="del-bucket")
    s3tables_client.delete_table_bucket(tableBucketARN="del-bucket")

    with pytest.raises(ClientError):
        s3tables_client.get_table_bucket(tableBucketARN="del-bucket")


def test_create_and_get_namespace(s3tables_client):
    s3tables_client.create_table_bucket(name="ns-bucket")
    resp = s3tables_client.create_namespace(
        tableBucketARN="ns-bucket",
        namespace=["my-namespace"],
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = s3tables_client.get_namespace(
        tableBucketARN="ns-bucket",
        namespace="my-namespace",
    )
    assert get_resp["namespace"] == "my-namespace"


def test_list_namespaces(s3tables_client):
    s3tables_client.create_table_bucket(name="list-ns-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="list-ns-bucket",
        namespace=["ns-a"],
    )
    resp = s3tables_client.list_namespaces(tableBucketARN="list-ns-bucket")
    assert "namespaces" in resp
    ns_names = [n["namespace"] for n in resp["namespaces"]]
    assert "ns-a" in ns_names


def test_create_and_get_table(s3tables_client):
    s3tables_client.create_table_bucket(name="tbl-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="tbl-bucket",
        namespace=["tbl-ns"],
    )
    resp = s3tables_client.create_table(
        tableBucketARN="tbl-bucket",
        namespace="tbl-ns",
        name="my-table",
        format="ICEBERG",
    )
    assert "tableARN" in resp

    get_resp = s3tables_client.get_table(
        tableBucketARN="tbl-bucket",
        namespace="tbl-ns",
        name="my-table",
    )
    assert get_resp["name"] == "my-table"


def test_delete_table(s3tables_client):
    s3tables_client.create_table_bucket(name="del-tbl-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="del-tbl-bucket",
        namespace=["ns"],
    )
    s3tables_client.create_table(
        tableBucketARN="del-tbl-bucket",
        namespace="ns",
        name="drop-me",
    )

    s3tables_client.delete_table(
        tableBucketARN="del-tbl-bucket",
        namespace="ns",
        name="drop-me",
    )

    with pytest.raises(ClientError):
        s3tables_client.get_table(
            tableBucketARN="del-tbl-bucket",
            namespace="ns",
            name="drop-me",
        )


def test_list_tables(s3tables_client):
    s3tables_client.create_table_bucket(name="multi-tbl")
    s3tables_client.create_namespace(
        tableBucketARN="multi-tbl",
        namespace=["ns"],
    )
    for i in range(3):
        s3tables_client.create_table(
            tableBucketARN="multi-tbl",
            namespace="ns",
            name=f"t{i}",
        )

    resp = s3tables_client.list_tables(tableBucketARN="multi-tbl", namespace="ns")
    assert len(resp["tables"]) == 3


def test_table_policy_crud(s3tables_client):
    s3tables_client.create_table_bucket(name="pol-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="pol-bucket",
        namespace=["ns"],
    )
    s3tables_client.create_table(
        tableBucketARN="pol-bucket",
        namespace="ns",
        name="t1",
    )

    policy = json.dumps({"Version": "2012-10-17", "Statement": []})
    s3tables_client.put_table_policy(
        tableBucketARN="pol-bucket",
        namespace="ns",
        name="t1",
        resourcePolicy=policy,
    )

    resp = s3tables_client.get_table_policy(
        tableBucketARN="pol-bucket",
        namespace="ns",
        name="t1",
    )
    assert "resourcePolicy" in resp

    s3tables_client.delete_table_policy(
        tableBucketARN="pol-bucket",
        namespace="ns",
        name="t1",
    )


def test_table_bucket_policy(s3tables_client):
    s3tables_client.create_table_bucket(name="bp-bucket")
    s3tables_client.put_table_bucket_policy(
        tableBucketARN="bp-bucket",
        resourcePolicy=json.dumps({"Version": "2012-10-17", "Statement": []}),
    )
    resp = s3tables_client.get_table_bucket_policy(tableBucketARN="bp-bucket")
    assert "resourcePolicy" in resp
    s3tables_client.delete_table_bucket_policy(tableBucketARN="bp-bucket")


def test_table_bucket_encryption(s3tables_client):
    s3tables_client.create_table_bucket(name="enc-bucket")
    s3tables_client.put_table_bucket_encryption(
        tableBucketARN="enc-bucket",
        encryptionConfiguration={"sseAlgorithm": "AES256"},
    )
    resp = s3tables_client.get_table_bucket_encryption(tableBucketARN="enc-bucket")
    assert "encryptionConfiguration" in resp


def test_table_encryption(s3tables_client):
    s3tables_client.create_table_bucket(name="te-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="te-bucket",
        namespace=["ns"],
    )
    s3tables_client.create_table(
        tableBucketARN="te-bucket",
        namespace="ns",
        name="t1",
    )
    s3tables_client.put_table_encryption(
        tableBucketARN="te-bucket",
        namespace="ns",
        name="t1",
        encryptionConfiguration={"sseAlgorithm": "AES256"},
    )
    resp = s3tables_client.get_table_encryption(
        tableBucketARN="te-bucket",
        namespace="ns",
        name="t1",
    )
    assert "encryptionConfiguration" in resp


def test_table_maintenance(s3tables_client):
    s3tables_client.create_table_bucket(name="mt-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="mt-bucket",
        namespace=["ns"],
    )
    s3tables_client.create_table(
        tableBucketARN="mt-bucket",
        namespace="ns",
        name="t1",
    )
    s3tables_client.put_table_maintenance_configuration(
        tableBucketARN="mt-bucket",
        namespace="ns",
        name="t1",
        type="icebergCompaction",
        value={"status": "enabled"},
    )
    resp = s3tables_client.get_table_maintenance_configuration(
        tableBucketARN="mt-bucket",
        namespace="ns",
        name="t1",
    )
    assert "configuration" in resp


def test_rename_and_update_metadata(s3tables_client):
    s3tables_client.create_table_bucket(name="ren-bucket")
    s3tables_client.create_namespace(
        tableBucketARN="ren-bucket",
        namespace=["ns"],
    )
    s3tables_client.create_table(
        tableBucketARN="ren-bucket",
        namespace="ns",
        name="old-name",
    )

    s3tables_client.rename_table(
        tableBucketARN="ren-bucket",
        namespace="ns",
        name="old-name",
        newName="new-name",
    )

    s3tables_client.update_table_metadata_location(
        tableBucketARN="ren-bucket",
        namespace="ns",
        name="new-name",
        metadataLocation="s3://meta/location.json",
    )
