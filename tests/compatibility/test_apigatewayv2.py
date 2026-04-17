import pytest
from botocore.exceptions import ClientError


def test_create_and_get_api(apigatewayv2_client):
    resp = apigatewayv2_client.create_api(Name="compat-api", ProtocolType="HTTP")
    api_id = resp["ApiId"]
    assert resp["Name"] == "compat-api"

    get = apigatewayv2_client.get_api(ApiId=api_id)
    assert get["Name"] == "compat-api"


def test_get_apis(apigatewayv2_client):
    apigatewayv2_client.create_api(Name="list-api", ProtocolType="HTTP")
    resp = apigatewayv2_client.get_apis()
    names = [a["Name"] for a in resp["Items"]]
    assert "list-api" in names


def test_delete_api(apigatewayv2_client):
    resp = apigatewayv2_client.create_api(Name="del-api", ProtocolType="HTTP")
    apigatewayv2_client.delete_api(ApiId=resp["ApiId"])


def test_create_route(apigatewayv2_client):
    api = apigatewayv2_client.create_api(Name="route-api", ProtocolType="HTTP")
    api_id = api["ApiId"]
    resp = apigatewayv2_client.create_route(ApiId=api_id, RouteKey="GET /hello")
    assert resp["RouteKey"] == "GET /hello"

    routes = apigatewayv2_client.get_routes(ApiId=api_id)
    assert len(routes["Items"]) >= 1


def test_create_stage(apigatewayv2_client):
    api = apigatewayv2_client.create_api(Name="stage-api", ProtocolType="HTTP")
    api_id = api["ApiId"]
    resp = apigatewayv2_client.create_stage(ApiId=api_id, StageName="prod")
    assert resp["StageName"] == "prod"

    stages = apigatewayv2_client.get_stages(ApiId=api_id)
    names = [s["StageName"] for s in stages["Items"]]
    assert "prod" in names


def test_get_nonexistent_api(apigatewayv2_client):
    with pytest.raises(ClientError) as exc:
        apigatewayv2_client.get_api(ApiId="nonexistent")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
