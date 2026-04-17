def test_create_and_describe_protection(shield_client):
    resp = shield_client.create_protection(
        Name="test-protection",
        ResourceArn="arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-12345",
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    protection_id = resp["ProtectionId"]
    assert protection_id

    desc = shield_client.describe_protection(ProtectionId=protection_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    prot = desc["Protection"]
    assert prot["Name"] == "test-protection"
    assert prot["Id"] == protection_id

    shield_client.delete_protection(ProtectionId=protection_id)


def test_list_protections(shield_client):
    resp = shield_client.create_protection(
        Name="list-protection",
        ResourceArn="arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-list1",
    )
    protection_id = resp["ProtectionId"]

    protections = shield_client.list_protections()
    assert protections["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [p["Id"] for p in protections["Protections"]]
    assert protection_id in ids

    shield_client.delete_protection(ProtectionId=protection_id)


def test_delete_protection(shield_client):
    resp = shield_client.create_protection(
        Name="del-protection",
        ResourceArn="arn:aws:ec2:us-east-1:000000000000:eip-allocation/eipalloc-del1",
    )
    protection_id = resp["ProtectionId"]

    del_resp = shield_client.delete_protection(ProtectionId=protection_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_subscription_lifecycle(shield_client):
    create_resp = shield_client.create_subscription()
    assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    state_resp = shield_client.get_subscription_state()
    assert state_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert state_resp["SubscriptionState"] == "ACTIVE"

    desc = shield_client.describe_subscription()
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "Subscription" in desc
