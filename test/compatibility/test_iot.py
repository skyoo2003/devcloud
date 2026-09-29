import pytest
from botocore.exceptions import ClientError


def test_create_and_describe_thing(iot_client):
    resp = iot_client.create_thing(thingName="compat-thing")
    assert resp["thingName"] == "compat-thing"
    assert "thingArn" in resp

    desc = iot_client.describe_thing(thingName="compat-thing")
    assert desc["thingName"] == "compat-thing"


def test_list_things(iot_client):
    iot_client.create_thing(thingName="list-thing")
    resp = iot_client.list_things()
    names = [t["thingName"] for t in resp["things"]]
    assert "list-thing" in names


def test_delete_thing(iot_client):
    iot_client.create_thing(thingName="del-thing")
    iot_client.delete_thing(thingName="del-thing")


def test_create_and_get_policy(iot_client):
    resp = iot_client.create_policy(
        policyName="compat-policy",
        policyDocument='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}',
    )
    assert resp["policyName"] == "compat-policy"

    get = iot_client.get_policy(policyName="compat-policy")
    assert get["policyName"] == "compat-policy"


def test_list_policies(iot_client):
    iot_client.create_policy(
        policyName="list-policy",
        policyDocument='{"Version":"2012-10-17","Statement":[]}',
    )
    resp = iot_client.list_policies()
    names = [p["policyName"] for p in resp["policies"]]
    assert "list-policy" in names


def test_create_keys_and_certificate(iot_client):
    resp = iot_client.create_keys_and_certificate(setAsActive=True)
    assert "certificateArn" in resp
    assert "certificateId" in resp
    assert "certificatePem" in resp
    assert "keyPair" in resp


def test_describe_nonexistent_thing(iot_client):
    with pytest.raises(ClientError) as exc:
        iot_client.describe_thing(thingName="no-such-thing-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
