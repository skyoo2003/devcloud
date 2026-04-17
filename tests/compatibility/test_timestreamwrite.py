def test_create_and_describe_database(timestreamwrite_client):
    resp = timestreamwrite_client.create_database(DatabaseName="test-db")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    db = resp["Database"]
    assert db["DatabaseName"] == "test-db"
    assert "Arn" in db

    desc = timestreamwrite_client.describe_database(DatabaseName="test-db")
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["Database"]["DatabaseName"] == "test-db"

    timestreamwrite_client.delete_database(DatabaseName="test-db")


def test_list_databases(timestreamwrite_client):
    timestreamwrite_client.create_database(DatabaseName="list-db")

    dbs = timestreamwrite_client.list_databases()
    assert dbs["ResponseMetadata"]["HTTPStatusCode"] == 200
    names = [d["DatabaseName"] for d in dbs["Databases"]]
    assert "list-db" in names

    timestreamwrite_client.delete_database(DatabaseName="list-db")


def test_delete_database(timestreamwrite_client):
    timestreamwrite_client.create_database(DatabaseName="del-db")

    del_resp = timestreamwrite_client.delete_database(DatabaseName="del-db")
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_create_and_describe_table(timestreamwrite_client):
    timestreamwrite_client.create_database(DatabaseName="table-db")

    resp = timestreamwrite_client.create_table(
        DatabaseName="table-db", TableName="test-table"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    table = resp["Table"]
    assert table["TableName"] == "test-table"
    assert table["DatabaseName"] == "table-db"

    desc = timestreamwrite_client.describe_table(
        DatabaseName="table-db", TableName="test-table"
    )
    assert desc["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert desc["Table"]["TableName"] == "test-table"

    timestreamwrite_client.delete_table(DatabaseName="table-db", TableName="test-table")
    timestreamwrite_client.delete_database(DatabaseName="table-db")


def test_describe_endpoints(timestreamwrite_client):
    resp = timestreamwrite_client.describe_endpoints()
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp["Endpoints"]) >= 1
    assert "Address" in resp["Endpoints"][0]
