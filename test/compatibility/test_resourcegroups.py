def test_create_group(resourcegroups_client):
    resp = resourcegroups_client.create_group(
        Name="compat-group",
        Description="test group",
        ResourceQuery={
            "Type": "TAG_FILTERS_1_0",
            "Query": '{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"env","Values":["test"]}]}',
        },
    )
    assert resp["Group"]["Name"] == "compat-group"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_group(resourcegroups_client):
    resourcegroups_client.create_group(
        Name="get-group",
        ResourceQuery={
            "Type": "TAG_FILTERS_1_0",
            "Query": '{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}',
        },
    )
    resp = resourcegroups_client.get_group(GroupName="get-group")
    assert resp["Group"]["Name"] == "get-group"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_groups(resourcegroups_client):
    resourcegroups_client.create_group(
        Name="list-group",
        ResourceQuery={
            "Type": "TAG_FILTERS_1_0",
            "Query": '{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}',
        },
    )
    resp = resourcegroups_client.list_groups()
    names = [g["Name"] for g in resp["Groups"]]
    assert "list-group" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_group(resourcegroups_client):
    resourcegroups_client.create_group(
        Name="del-group",
        ResourceQuery={
            "Type": "TAG_FILTERS_1_0",
            "Query": '{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[]}',
        },
    )
    resp = resourcegroups_client.delete_group(GroupName="del-group")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = resourcegroups_client.list_groups()
    names = [g["Name"] for g in list_resp["Groups"]]
    assert "del-group" not in names
