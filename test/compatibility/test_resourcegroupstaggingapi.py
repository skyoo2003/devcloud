def test_get_resources_empty(resourcegroupstagging_client):
    resp = resourcegroupstagging_client.get_resources()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ResourceTagMappingList" in resp
    assert isinstance(resp["ResourceTagMappingList"], list)


def test_tag_and_get_resources(resourcegroupstagging_client):
    arns = [
        "arn:aws:s3:::compat-bucket-1",
        "arn:aws:s3:::compat-bucket-2",
    ]
    resourcegroupstagging_client.tag_resources(
        ResourceARNList=arns,
        Tags={"env": "compat-test", "owner": "qa"},
    )

    resp = resourcegroupstagging_client.get_resources(
        TagFilters=[{"Key": "env", "Values": ["compat-test"]}]
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    result_arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert "arn:aws:s3:::compat-bucket-1" in result_arns
    assert "arn:aws:s3:::compat-bucket-2" in result_arns


def test_get_tag_keys(resourcegroupstagging_client):
    resourcegroupstagging_client.tag_resources(
        ResourceARNList=["arn:aws:s3:::keys-bucket"],
        Tags={"key1": "v1", "key2": "v2"},
    )
    resp = resourcegroupstagging_client.get_tag_keys()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "TagKeys" in resp
    assert "key1" in resp["TagKeys"]
    assert "key2" in resp["TagKeys"]


def test_get_tag_values(resourcegroupstagging_client):
    resourcegroupstagging_client.tag_resources(
        ResourceARNList=["arn:aws:s3:::vals-bucket-1", "arn:aws:s3:::vals-bucket-2"],
        Tags={"stage": "prod"},
    )
    resourcegroupstagging_client.tag_resources(
        ResourceARNList=["arn:aws:s3:::vals-bucket-3"],
        Tags={"stage": "staging"},
    )

    resp = resourcegroupstagging_client.get_tag_values(Key="stage")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "TagValues" in resp
    assert "prod" in resp["TagValues"]
    assert "staging" in resp["TagValues"]


def test_untag_resources(resourcegroupstagging_client):
    arn = "arn:aws:lambda:us-east-1:000000000000:function:test-fn"
    resourcegroupstagging_client.tag_resources(
        ResourceARNList=[arn],
        Tags={"env": "test", "version": "1.0"},
    )

    resourcegroupstagging_client.untag_resources(
        ResourceARNList=[arn],
        TagKeys=["env"],
    )

    resp = resourcegroupstagging_client.get_resources(
        TagFilters=[{"Key": "env", "Values": ["test"]}]
    )
    result_arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
    assert arn not in result_arns


def test_start_report_creation(resourcegroupstagging_client):
    resp = resourcegroupstagging_client.start_report_creation(
        S3Bucket="my-report-bucket"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_report_creation(resourcegroupstagging_client):
    resp = resourcegroupstagging_client.describe_report_creation()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Status"] == "COMPLETE"


def test_get_compliance_summary(resourcegroupstagging_client):
    resp = resourcegroupstagging_client.get_compliance_summary()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "SummaryList" in resp
