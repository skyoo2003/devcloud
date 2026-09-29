def test_create_application(codedeploy_client):
    resp = codedeploy_client.create_application(applicationName="compat-app")
    assert resp["applicationId"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_application(codedeploy_client):
    codedeploy_client.create_application(applicationName="get-app")
    resp = codedeploy_client.get_application(applicationName="get-app")
    assert resp["application"]["applicationName"] == "get-app"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_applications(codedeploy_client):
    codedeploy_client.create_application(applicationName="list-app")
    resp = codedeploy_client.list_applications()
    assert "list-app" in resp["applications"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
