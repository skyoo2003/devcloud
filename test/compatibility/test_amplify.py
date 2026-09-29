def test_create_app(amplify_client):
    resp = amplify_client.create_app(name="compat-app")
    assert resp["app"]["name"] == "compat-app"
    assert resp["app"]["appId"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_app(amplify_client):
    create_resp = amplify_client.create_app(name="get-app")
    app_id = create_resp["app"]["appId"]
    resp = amplify_client.get_app(appId=app_id)
    assert resp["app"]["name"] == "get-app"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_apps(amplify_client):
    amplify_client.create_app(name="list-app")
    resp = amplify_client.list_apps()
    names = [a["name"] for a in resp["apps"]]
    assert "list-app" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_delete_app(amplify_client):
    create_resp = amplify_client.create_app(name="del-app")
    app_id = create_resp["app"]["appId"]
    resp = amplify_client.delete_app(appId=app_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    list_resp = amplify_client.list_apps()
    ids = [a["appId"] for a in list_resp["apps"]]
    assert app_id not in ids
