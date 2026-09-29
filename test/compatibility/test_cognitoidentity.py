def test_create_and_describe_identity_pool(cognitoidentity_client):
    resp = cognitoidentity_client.create_identity_pool(
        IdentityPoolName="test-pool",
        AllowUnauthenticatedIdentities=True,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    pool_id = resp["IdentityPoolId"]
    assert pool_id
    assert resp["IdentityPoolName"] == "test-pool"

    desc = cognitoidentity_client.describe_identity_pool(IdentityPoolId=pool_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["IdentityPoolName"] == "test-pool"
    assert desc["AllowUnauthenticatedIdentities"] is True

    # cleanup
    cognitoidentity_client.delete_identity_pool(IdentityPoolId=pool_id)


def test_list_identity_pools(cognitoidentity_client):
    resp = cognitoidentity_client.create_identity_pool(
        IdentityPoolName="list-pool",
        AllowUnauthenticatedIdentities=False,
    )
    pool_id = resp["IdentityPoolId"]

    pools = cognitoidentity_client.list_identity_pools(MaxResults=10)
    assert pools["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [p["IdentityPoolId"] for p in pools["IdentityPools"]]
    assert pool_id in ids

    cognitoidentity_client.delete_identity_pool(IdentityPoolId=pool_id)


def test_update_identity_pool(cognitoidentity_client):
    resp = cognitoidentity_client.create_identity_pool(
        IdentityPoolName="update-pool",
        AllowUnauthenticatedIdentities=False,
    )
    pool_id = resp["IdentityPoolId"]

    updated = cognitoidentity_client.update_identity_pool(
        IdentityPoolId=pool_id,
        IdentityPoolName="updated-pool",
        AllowUnauthenticatedIdentities=True,
    )
    assert updated["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert updated["IdentityPoolName"] == "updated-pool"

    cognitoidentity_client.delete_identity_pool(IdentityPoolId=pool_id)


def test_delete_identity_pool(cognitoidentity_client):
    resp = cognitoidentity_client.create_identity_pool(
        IdentityPoolName="delete-pool",
        AllowUnauthenticatedIdentities=False,
    )
    pool_id = resp["IdentityPoolId"]

    delete_resp = cognitoidentity_client.delete_identity_pool(IdentityPoolId=pool_id)
    assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_id_and_list_identities(cognitoidentity_client):
    resp = cognitoidentity_client.create_identity_pool(
        IdentityPoolName="id-pool",
        AllowUnauthenticatedIdentities=True,
    )
    pool_id = resp["IdentityPoolId"]

    id_resp = cognitoidentity_client.get_id(IdentityPoolId=pool_id)
    assert id_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    identity_id = id_resp["IdentityId"]
    assert identity_id

    identities = cognitoidentity_client.list_identities(
        IdentityPoolId=pool_id, MaxResults=10
    )
    assert identities["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert any(i["IdentityId"] == identity_id for i in identities["Identities"])

    cognitoidentity_client.delete_identity_pool(IdentityPoolId=pool_id)
