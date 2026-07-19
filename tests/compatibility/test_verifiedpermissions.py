def test_list_policies(verifiedpermissions_client):
    resp = verifiedpermissions_client.list_policies(policyStoreId="store-123")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_is_authorized(verifiedpermissions_client):
    resp = verifiedpermissions_client.is_authorized(
        policyStoreId="store-123",
        principal={"entityType": "User", "entityId": "alice"},
        action={"actionType": "Action", "actionId": "read"},
        resource={"entityType": "Doc", "entityId": "doc1"},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
