import json
import pytest
import botocore.exceptions


def test_create_resource(cloudcontrol_client):
    resp = cloudcontrol_client.create_resource(
        TypeName="AWS::S3::Bucket",
        DesiredState=json.dumps({"BucketName": "my-test-bucket"}),
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    pe = resp["ProgressEvent"]
    assert pe["TypeName"] == "AWS::S3::Bucket"
    assert pe["Operation"] == "CREATE"
    assert pe["OperationStatus"] == "SUCCESS"
    assert "RequestToken" in pe
    assert "Identifier" in pe


def test_get_resource(cloudcontrol_client):
    create_resp = cloudcontrol_client.create_resource(
        TypeName="AWS::DynamoDB::Table",
        DesiredState=json.dumps({"TableName": "my-table"}),
    )
    identifier = create_resp["ProgressEvent"]["Identifier"]

    resp = cloudcontrol_client.get_resource(
        TypeName="AWS::DynamoDB::Table",
        Identifier=identifier,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    desc = resp["ResourceDescription"]
    assert desc["Identifier"] == identifier


def test_list_resources(cloudcontrol_client):
    for i in range(3):
        cloudcontrol_client.create_resource(
            TypeName="AWS::SNS::Topic",
            DesiredState=json.dumps({"TopicName": f"topic-{i}"}),
        )

    resp = cloudcontrol_client.list_resources(TypeName="AWS::SNS::Topic")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ResourceDescriptions" in resp
    assert len(resp["ResourceDescriptions"]) >= 3


def test_delete_resource(cloudcontrol_client):
    create_resp = cloudcontrol_client.create_resource(
        TypeName="AWS::Lambda::Function",
        DesiredState=json.dumps({"FunctionName": "my-fn"}),
    )
    identifier = create_resp["ProgressEvent"]["Identifier"]

    delete_resp = cloudcontrol_client.delete_resource(
        TypeName="AWS::Lambda::Function",
        Identifier=identifier,
    )
    assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    pe = delete_resp["ProgressEvent"]
    assert pe["Operation"] == "DELETE"
    assert pe["OperationStatus"] == "SUCCESS"

    with pytest.raises(botocore.exceptions.ClientError) as exc:
        cloudcontrol_client.get_resource(
            TypeName="AWS::Lambda::Function",
            Identifier=identifier,
        )
    assert exc.value.response["Error"]["Code"] in ("ResourceNotFoundException", "404")


def test_get_resource_request_status(cloudcontrol_client):
    create_resp = cloudcontrol_client.create_resource(
        TypeName="AWS::EC2::VPC",
        DesiredState=json.dumps({}),
    )
    request_token = create_resp["ProgressEvent"]["RequestToken"]

    resp = cloudcontrol_client.get_resource_request_status(
        RequestToken=request_token,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    pe = resp["ProgressEvent"]
    assert pe["RequestToken"] == request_token
    assert pe["OperationStatus"] == "SUCCESS"


def test_list_resource_requests(cloudcontrol_client):
    cloudcontrol_client.create_resource(
        TypeName="AWS::SQS::Queue",
        DesiredState=json.dumps({}),
    )
    cloudcontrol_client.create_resource(
        TypeName="AWS::SQS::Queue",
        DesiredState=json.dumps({}),
    )

    resp = cloudcontrol_client.list_resource_requests()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ResourceRequestStatusSummaries" in resp
    assert len(resp["ResourceRequestStatusSummaries"]) >= 2
