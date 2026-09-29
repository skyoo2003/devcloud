def test_create_trail(cloudtrail_client):
    resp = cloudtrail_client.create_trail(
        Name="compat-trail",
        S3BucketName="my-trail-bucket",
    )
    assert resp["Name"] == "compat-trail"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_describe_trails(cloudtrail_client):
    cloudtrail_client.create_trail(
        Name="desc-trail",
        S3BucketName="my-trail-bucket",
    )
    resp = cloudtrail_client.describe_trails(trailNameList=["desc-trail"])
    names = [t["Name"] for t in resp["trailList"]]
    assert "desc-trail" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_trail(cloudtrail_client):
    cloudtrail_client.create_trail(
        Name="get-trail",
        S3BucketName="my-trail-bucket",
    )
    resp = cloudtrail_client.get_trail(Name="get-trail")
    assert resp["Trail"]["Name"] == "get-trail"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
