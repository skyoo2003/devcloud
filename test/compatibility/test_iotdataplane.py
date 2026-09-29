import json


def test_update_and_get_thing_shadow(iotdata_client):
    payload = json.dumps({"state": {"desired": {"temperature": 25}}})
    update_resp = iotdata_client.update_thing_shadow(
        thingName="test-thing",
        payload=payload,
    )
    assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = iotdata_client.get_thing_shadow(thingName="test-thing")
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    shadow = json.loads(get_resp["payload"].read())
    assert "state" in shadow

    iotdata_client.delete_thing_shadow(thingName="test-thing")


def test_list_named_shadows(iotdata_client):
    payload = json.dumps({"state": {"desired": {"on": True}}})
    iotdata_client.update_thing_shadow(
        thingName="shadow-thing",
        payload=payload,
        shadowName="my-shadow",
    )

    resp = iotdata_client.list_named_shadows_for_thing(thingName="shadow-thing")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert (
        "results" in resp or "Results" in resp or isinstance(resp.get("results"), list)
    )

    iotdata_client.delete_thing_shadow(thingName="shadow-thing", shadowName="my-shadow")


def test_publish(iotdata_client):
    resp = iotdata_client.publish(
        topic="test/topic",
        payload=json.dumps({"msg": "hello"}),
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
