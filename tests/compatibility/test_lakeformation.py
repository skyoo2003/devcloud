import pytest
from botocore.exceptions import ClientError


def test_register_and_describe_resource(lakeformation_client):
    lakeformation_client.register_resource(
        ResourceArn="arn:aws:s3:::my-data-lake-bucket",
        UseServiceLinkedRole=True,
    )

    resp = lakeformation_client.describe_resource(
        ResourceArn="arn:aws:s3:::my-data-lake-bucket"
    )
    assert resp["ResourceInfo"]["ResourceArn"] == "arn:aws:s3:::my-data-lake-bucket"


def test_list_resources(lakeformation_client):
    lakeformation_client.register_resource(
        ResourceArn="arn:aws:s3:::list-lake-bucket",
        UseServiceLinkedRole=True,
    )
    resp = lakeformation_client.list_resources()
    arns = [r["ResourceArn"] for r in resp["ResourceInfoList"]]
    assert "arn:aws:s3:::list-lake-bucket" in arns


def test_deregister_resource(lakeformation_client):
    lakeformation_client.register_resource(
        ResourceArn="arn:aws:s3:::dereg-lake-bucket",
        UseServiceLinkedRole=True,
    )
    lakeformation_client.deregister_resource(
        ResourceArn="arn:aws:s3:::dereg-lake-bucket"
    )

    with pytest.raises(ClientError):
        lakeformation_client.describe_resource(
            ResourceArn="arn:aws:s3:::dereg-lake-bucket"
        )


def test_create_and_get_lf_tag(lakeformation_client):
    lakeformation_client.create_lf_tag(
        TagKey="environment",
        TagValues=["dev", "staging", "prod"],
    )
    resp = lakeformation_client.get_lf_tag(TagKey="environment")
    assert resp["TagKey"] == "environment"
    assert "dev" in resp["TagValues"]
