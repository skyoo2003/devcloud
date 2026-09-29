def test_create_graphql_api(appsync_client):
    resp = appsync_client.create_graphql_api(
        name="compat-api",
        authenticationType="API_KEY",
    )
    assert resp["graphqlApi"]["name"] == "compat-api"
    assert resp["graphqlApi"]["apiId"]
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_graphql_api(appsync_client):
    create_resp = appsync_client.create_graphql_api(
        name="get-api",
        authenticationType="API_KEY",
    )
    api_id = create_resp["graphqlApi"]["apiId"]
    resp = appsync_client.get_graphql_api(apiId=api_id)
    assert resp["graphqlApi"]["name"] == "get-api"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_list_graphql_apis(appsync_client):
    appsync_client.create_graphql_api(
        name="list-api",
        authenticationType="API_KEY",
    )
    resp = appsync_client.list_graphql_apis()
    names = [a["name"] for a in resp["graphqlApis"]]
    assert "list-api" in names
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
