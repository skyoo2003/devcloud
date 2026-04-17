def test_create_and_describe_server(transfer_client):
    resp = transfer_client.create_server()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    server_id = resp["ServerId"]
    assert server_id.startswith("s-")

    desc = transfer_client.describe_server(ServerId=server_id)
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    server = desc["Server"]
    assert server["ServerId"] == server_id
    assert server["State"] in ("ONLINE", "STARTING")

    transfer_client.delete_server(ServerId=server_id)


def test_list_servers(transfer_client):
    resp = transfer_client.create_server()
    server_id = resp["ServerId"]

    servers = transfer_client.list_servers()
    assert servers["ResponseMetadata"]["HTTPStatusCode"] == 200
    ids = [s["ServerId"] for s in servers["Servers"]]
    assert server_id in ids

    transfer_client.delete_server(ServerId=server_id)


def test_delete_server(transfer_client):
    resp = transfer_client.create_server()
    server_id = resp["ServerId"]

    del_resp = transfer_client.delete_server(ServerId=server_id)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_start_and_stop_server(transfer_client):
    resp = transfer_client.create_server()
    server_id = resp["ServerId"]

    stop_resp = transfer_client.stop_server(ServerId=server_id)
    assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    start_resp = transfer_client.start_server(ServerId=server_id)
    assert start_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    transfer_client.delete_server(ServerId=server_id)
