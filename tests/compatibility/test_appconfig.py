def test_create_application(appconfig_client):
    resp = appconfig_client.create_application(
        Name="compat-app",
        Description="test application",
    )
    assert resp["Name"] == "compat-app"
    assert resp["Id"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)


def test_get_application(appconfig_client):
    create_resp = appconfig_client.create_application(Name="get-app")
    app_id = create_resp["Id"]
    resp = appconfig_client.get_application(ApplicationId=app_id)
    assert resp["Name"] == "get-app"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_applications(appconfig_client):
    appconfig_client.create_application(Name="list-app")
    resp = appconfig_client.list_applications()
    names = [a["Name"] for a in resp["Items"]]
    assert "list-app" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_application(appconfig_client):
    create_resp = appconfig_client.create_application(Name="del-app")
    app_id = create_resp["Id"]
    resp = appconfig_client.delete_application(ApplicationId=app_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
