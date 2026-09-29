def test_create_and_describe_user_pool(cognitoidp_client):
    resp = cognitoidp_client.create_user_pool(PoolName="test-pool")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    pool = resp["UserPool"]
    pool_id = pool["Id"]
    assert pool_id
    assert pool["Name"] == "test-pool"

    desc = cognitoidp_client.describe_user_pool(UserPoolId=pool_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["UserPool"]["Name"] == "test-pool"

    cognitoidp_client.delete_user_pool(UserPoolId=pool_id)


def test_list_user_pools(cognitoidp_client):
    resp = cognitoidp_client.create_user_pool(PoolName="list-pool")
    pool_id = resp["UserPool"]["Id"]

    pools = cognitoidp_client.list_user_pools(MaxResults=10)
    assert pools["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [p["Id"] for p in pools["UserPools"]]
    assert pool_id in ids

    cognitoidp_client.delete_user_pool(UserPoolId=pool_id)


def test_delete_user_pool(cognitoidp_client):
    resp = cognitoidp_client.create_user_pool(PoolName="delete-pool")
    pool_id = resp["UserPool"]["Id"]

    delete_resp = cognitoidp_client.delete_user_pool(UserPoolId=pool_id)
    assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_create_user_pool_client(cognitoidp_client):
    resp = cognitoidp_client.create_user_pool(PoolName="client-pool")
    pool_id = resp["UserPool"]["Id"]

    client_resp = cognitoidp_client.create_user_pool_client(
        UserPoolId=pool_id, ClientName="test-client"
    )
    assert client_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    client = client_resp["UserPoolClient"]
    assert client["ClientName"] == "test-client"
    assert client["ClientId"]

    clients = cognitoidp_client.list_user_pool_clients(
        UserPoolId=pool_id, MaxResults=10
    )
    assert clients["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert any(c["ClientId"] == client["ClientId"] for c in clients["UserPoolClients"])

    cognitoidp_client.delete_user_pool_client(
        UserPoolId=pool_id, ClientId=client["ClientId"]
    )
    cognitoidp_client.delete_user_pool(UserPoolId=pool_id)
