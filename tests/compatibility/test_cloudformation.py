import pytest
from botocore.exceptions import ClientError


SIMPLE_TEMPLATE = """{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Description": "Test stack",
  "Resources": {
    "Queue": {
      "Type": "AWS::SQS::Queue",
      "Properties": {"QueueName": "cfn-test-queue"}
    }
  }
}"""


def test_create_and_describe_stack(cloudformation_client):
    resp = cloudformation_client.create_stack(
        StackName="compat-stack", TemplateBody=SIMPLE_TEMPLATE
    )
    assert "StackId" in resp

    desc = cloudformation_client.describe_stacks(StackName="compat-stack")
    assert len(desc["Stacks"]) == 1
    assert desc["Stacks"][0]["StackName"] == "compat-stack"


def test_list_stacks(cloudformation_client):
    cloudformation_client.create_stack(
        StackName="list-stack", TemplateBody=SIMPLE_TEMPLATE
    )
    resp = cloudformation_client.list_stacks()
    names = [s["StackName"] for s in resp["StackSummaries"]]
    assert "list-stack" in names


def test_delete_stack(cloudformation_client):
    cloudformation_client.create_stack(
        StackName="del-stack", TemplateBody=SIMPLE_TEMPLATE
    )
    cloudformation_client.delete_stack(StackName="del-stack")


def test_describe_stack_resources(cloudformation_client):
    cloudformation_client.create_stack(
        StackName="res-stack", TemplateBody=SIMPLE_TEMPLATE
    )
    resp = cloudformation_client.describe_stack_resources(StackName="res-stack")
    assert "StackResources" in resp


def test_validate_template(cloudformation_client):
    resp = cloudformation_client.validate_template(TemplateBody=SIMPLE_TEMPLATE)
    assert "Description" in resp


def test_describe_nonexistent_stack(cloudformation_client):
    with pytest.raises(ClientError) as exc:
        cloudformation_client.describe_stacks(StackName="no-such-stack-xyz")
    assert "does not exist" in str(exc.value) or "ValidationError" in str(exc.value)


# ---------------------------------------------------------------------------
# Real-provisioning tests — verify that CreateStack actually creates the
# underlying resources in the corresponding services.
# ---------------------------------------------------------------------------


S3_TEMPLATE_YAML = """
AWSTemplateFormatVersion: '2010-09-09'
Description: Provision a real S3 bucket
Resources:
  MyBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: cfn-provisioned-bucket
"""


def test_create_stack_provisions_s3_bucket(cloudformation_client, s3_client):
    """CreateStack with AWS::S3::Bucket must actually call S3's CreateBucket."""
    cloudformation_client.create_stack(
        StackName="prov-s3-stack", TemplateBody=S3_TEMPLATE_YAML
    )
    buckets = s3_client.list_buckets()["Buckets"]
    names = [b["Name"] for b in buckets]
    assert "cfn-provisioned-bucket" in names

    # DescribeStackResources should report the new bucket.
    resp = cloudformation_client.describe_stack_resources(StackName="prov-s3-stack")
    resources = resp["StackResources"]
    assert any(
        r["LogicalResourceId"] == "MyBucket"
        and r["PhysicalResourceId"] == "cfn-provisioned-bucket"
        and r["ResourceType"] == "AWS::S3::Bucket"
        for r in resources
    )


S3_DDB_TEMPLATE = """{
  "AWSTemplateFormatVersion": "2010-09-09",
  "Resources": {
    "Bkt": {
      "Type": "AWS::S3::Bucket",
      "Properties": {"BucketName": "cfn-multi-bucket"}
    },
    "Tbl": {
      "Type": "AWS::DynamoDB::Table",
      "Properties": {
        "TableName": "cfn-multi-table",
        "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST"
      }
    }
  }
}"""


def test_create_stack_provisions_s3_and_dynamodb(
    cloudformation_client, s3_client, dynamodb_client
):
    """Multi-resource template must provision both resources."""
    cloudformation_client.create_stack(
        StackName="prov-multi-stack", TemplateBody=S3_DDB_TEMPLATE
    )

    # S3 bucket exists.
    bucket_names = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    assert "cfn-multi-bucket" in bucket_names

    # DynamoDB table exists.
    table_names = dynamodb_client.list_tables()["TableNames"]
    assert "cfn-multi-table" in table_names


SQS_SUB_TEMPLATE = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Q:
    Type: AWS::SQS::Queue
    Properties:
      QueueName:
        Fn::Sub: "${AWS::StackName}-queue"
"""


def test_create_stack_intrinsic_sub(cloudformation_client, sqs_client):
    """Fn::Sub with AWS::StackName must resolve before the queue is created."""
    cloudformation_client.create_stack(
        StackName="prov-sub-stack", TemplateBody=SQS_SUB_TEMPLATE
    )
    # The queue name must have been substituted to "prov-sub-stack-queue".
    urls = sqs_client.list_queues().get("QueueUrls", [])
    assert any(url.endswith("/prov-sub-stack-queue") for url in urls), urls


def test_delete_stack_tears_down_resources(cloudformation_client, s3_client):
    """DeleteStack must remove the provisioned bucket."""
    tmpl = """
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Bkt:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: cfn-teardown-bucket
"""
    cloudformation_client.create_stack(StackName="prov-teardown", TemplateBody=tmpl)
    names = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    assert "cfn-teardown-bucket" in names

    cloudformation_client.delete_stack(StackName="prov-teardown")
    names_after = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    assert "cfn-teardown-bucket" not in names_after
