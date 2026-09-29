def test_list_wireless_devices(iotwireless_client):
    resp = iotwireless_client.list_wireless_devices()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "WirelessDeviceList" in resp


def test_list_destinations(iotwireless_client):
    resp = iotwireless_client.list_destinations()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "DestinationList" in resp


def test_create_and_get_destination(iotwireless_client):
    resp = iotwireless_client.create_destination(
        Name="test-dest",
        ExpressionType="RuleName",
        Expression="test-rule",
        RoleArn="arn:aws:iam::000000000000:role/test",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert resp["Name"] == "test-dest"

    get_resp = iotwireless_client.get_destination(Name="test-dest")
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["Name"] == "test-dest"

    iotwireless_client.delete_destination(Name="test-dest")


def test_list_wireless_gateways(iotwireless_client):
    resp = iotwireless_client.list_wireless_gateways()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "WirelessGatewayList" in resp
