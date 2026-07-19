import pytest
from botocore.exceptions import ClientError


def test_create_and_get_connection(codeconnections_client):
    created = codeconnections_client.create_connection(
        ConnectionName="compat-conn", ProviderType="GitHub"
    )
    arn = created["ConnectionArn"]
    assert arn
    assert created["ResponseMetadata"]["HTTPStatusCode"] == 200

    got = codeconnections_client.get_connection(ConnectionArn=arn)
    assert got["Connection"]["ConnectionName"] == "compat-conn"
    assert got["Connection"]["ProviderType"] == "GitHub"


def test_list_connections(codeconnections_client):
    codeconnections_client.create_connection(
        ConnectionName="list-conn", ProviderType="GitHub"
    )
    resp = codeconnections_client.list_connections()
    names = [c["ConnectionName"] for c in resp["Connections"]]
    assert "list-conn" in names


def test_delete_connection(codeconnections_client):
    created = codeconnections_client.create_connection(
        ConnectionName="del-conn", ProviderType="GitHub"
    )
    arn = created["ConnectionArn"]
    codeconnections_client.delete_connection(ConnectionArn=arn)
    with pytest.raises(ClientError):
        codeconnections_client.get_connection(ConnectionArn=arn)
