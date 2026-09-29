def test_create_and_get_resource_share(ram_client):
    resp = ram_client.create_resource_share(name="test-share")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    share = resp["resourceShare"]
    share_arn = share["resourceShareArn"]
    assert share_arn
    assert share["name"] == "test-share"
    assert share["status"] == "ACTIVE"

    get_resp = ram_client.get_resource_shares(resourceOwner="SELF")
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    arns = [s["resourceShareArn"] for s in get_resp["resourceShares"]]
    assert share_arn in arns

    ram_client.delete_resource_share(resourceShareArn=share_arn)


def test_delete_resource_share(ram_client):
    resp = ram_client.create_resource_share(name="delete-share")
    share_arn = resp["resourceShare"]["resourceShareArn"]

    del_resp = ram_client.delete_resource_share(resourceShareArn=share_arn)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_enable_sharing_with_organization(ram_client):
    resp = ram_client.enable_sharing_with_aws_organization()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
