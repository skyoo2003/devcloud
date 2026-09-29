import pytest
from botocore.exceptions import ClientError


def test_create_and_get_environment(mwaa_client):
    resp = mwaa_client.create_environment(
        Name="compat-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    assert "Arn" in resp

    desc = mwaa_client.get_environment(Name="compat-mwaa")
    assert desc["Environment"]["Name"] == "compat-mwaa"
    assert desc["Environment"]["Status"] == "AVAILABLE"


def test_list_environments(mwaa_client):
    mwaa_client.create_environment(
        Name="list-mwaa-1",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    resp = mwaa_client.list_environments()
    assert "Environments" in resp
    assert "list-mwaa-1" in resp["Environments"]


def test_delete_environment(mwaa_client):
    mwaa_client.create_environment(
        Name="del-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    mwaa_client.delete_environment(Name="del-mwaa")

    with pytest.raises(ClientError):
        mwaa_client.get_environment(Name="del-mwaa")


def test_update_environment(mwaa_client):
    mwaa_client.create_environment(
        Name="upd-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    mwaa_client.update_environment(
        Name="upd-mwaa",
        MaxWorkers=20,
        MinWorkers=2,
    )
    desc = mwaa_client.get_environment(Name="upd-mwaa")
    assert desc["Environment"]["MaxWorkers"] == 20
    assert desc["Environment"]["MinWorkers"] == 2


def test_tag_lifecycle(mwaa_client):
    resp = mwaa_client.create_environment(
        Name="tag-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    arn = resp["Arn"]

    mwaa_client.tag_resource(
        ResourceArn=arn,
        Tags={"Env": "prod", "Team": "data"},
    )
    listed = mwaa_client.list_tags_for_resource(ResourceArn=arn)
    assert listed["Tags"]["Env"] == "prod"

    mwaa_client.untag_resource(
        ResourceArn=arn,
        tagKeys=["Env"],
    )
    listed2 = mwaa_client.list_tags_for_resource(ResourceArn=arn)
    assert "Env" not in listed2["Tags"]
    assert listed2["Tags"]["Team"] == "data"


def test_cli_token(mwaa_client):
    mwaa_client.create_environment(
        Name="tok-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    resp = mwaa_client.create_cli_token(Name="tok-mwaa")
    assert "CliToken" in resp
    assert "WebServerHostname" in resp


def test_web_login_token(mwaa_client):
    mwaa_client.create_environment(
        Name="web-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    resp = mwaa_client.create_web_login_token(Name="web-mwaa")
    assert "WebToken" in resp
    assert "WebServerHostname" in resp


def test_publish_metrics(mwaa_client):
    mwaa_client.create_environment(
        Name="metrics-mwaa",
        ExecutionRoleArn="arn:aws:iam::123456789012:role/test",
        SourceBucketArn="arn:aws:s3:::my-dags-bucket",
        DagS3Path="dags",
        NetworkConfiguration={"SubnetIds": ["subnet-12345", "subnet-67890"]},
    )
    # boto3 publish_metrics may not exist for all versions; fall back gracefully.
    if hasattr(mwaa_client, "publish_metrics"):
        mwaa_client.publish_metrics(
            EnvironmentName="metrics-mwaa",
            MetricData=[],
        )


def test_get_nonexistent_environment(mwaa_client):
    with pytest.raises(ClientError):
        mwaa_client.get_environment(Name="does-not-exist")
