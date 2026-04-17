def test_create_group(xray_client):
    resp = xray_client.create_group(
        GroupName="compat-group",
        FilterExpression='service("example.com")',
    )
    assert resp["Group"]["GroupName"] == "compat-group"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_group(xray_client):
    xray_client.create_group(GroupName="get-group")
    resp = xray_client.get_group(GroupName="get-group")
    assert resp["Group"]["GroupName"] == "get-group"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_group(xray_client):
    xray_client.create_group(GroupName="del-group")
    resp = xray_client.delete_group(GroupName="del-group")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = xray_client.get_groups()
    names = [g["GroupName"] for g in list_resp["Groups"]]
    assert "del-group" not in names
