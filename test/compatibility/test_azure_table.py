import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def table_endpoint(devcloud_server):
    return f"{DEVCLOUD_URL}/devstoreaccount1"


def test_azure_table_lifecycle_and_entities(table_endpoint):
    table_name = "DevCloudUsers"
    headers = {
        "x-ms-version": "2023-11-03",
        "Accept": "application/json;odata=nometadata",
        "Content-Type": "application/json",
        "Authorization": "SharedKey devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    }

    # 1. Create Table
    resp_create = requests.post(
        f"{table_endpoint}/Tables", headers=headers, json={"TableName": table_name}
    )
    assert resp_create.status_code in (201, 204)

    # 2. Duplicate Create returns 409
    resp_dup = requests.post(
        f"{table_endpoint}/Tables", headers=headers, json={"TableName": table_name}
    )
    assert resp_dup.status_code == 409

    # 3. Query Tables
    resp_tables = requests.get(f"{table_endpoint}/Tables", headers=headers)
    assert resp_tables.status_code == 200
    tables_list = [t["TableName"] for t in resp_tables.json().get("value", [])]
    assert table_name in tables_list

    # 4. Insert Entity
    entity = {
        "PartitionKey": "Developers",
        "RowKey": "dev-001",
        "Name": "Gopher",
        "Role": "Cloud Engineer",
        "Level": 5,
    }
    resp_insert = requests.post(
        f"{table_endpoint}/{table_name}", headers=headers, json=entity
    )
    assert resp_insert.status_code in (201, 204)

    # 5. Get Entity
    resp_get = requests.get(
        f"{table_endpoint}/{table_name}(PartitionKey='Developers',RowKey='dev-001')",
        headers=headers,
    )
    assert resp_get.status_code == 200
    retrieved = resp_get.json()
    assert retrieved["Name"] == "Gopher"
    assert retrieved["Role"] == "Cloud Engineer"

    # 6. Update Entity
    entity["Role"] = "Principal Engineer"
    resp_update = requests.put(
        f"{table_endpoint}/{table_name}(PartitionKey='Developers',RowKey='dev-001')",
        headers=headers,
        json=entity,
    )
    assert resp_update.status_code == 204

    # 7. Query Entities
    resp_query = requests.get(f"{table_endpoint}/{table_name}()", headers=headers)
    assert resp_query.status_code == 200
    entities = resp_query.json().get("value", [])
    assert len(entities) >= 1
    assert entities[0]["Role"] == "Principal Engineer"

    # 8. Delete Entity
    resp_del_ent = requests.delete(
        f"{table_endpoint}/{table_name}(PartitionKey='Developers',RowKey='dev-001')",
        headers=headers,
    )
    assert resp_del_ent.status_code == 204

    # 9. Get Deleted Entity returns 404
    resp_get_404 = requests.get(
        f"{table_endpoint}/{table_name}(PartitionKey='Developers',RowKey='dev-001')",
        headers=headers,
    )
    assert resp_get_404.status_code == 404

    # 10. Delete Table
    resp_del_tbl = requests.delete(
        f"{table_endpoint}/Tables('{table_name}')", headers=headers
    )
    assert resp_del_tbl.status_code == 204

    # 11. Delete Table again returns 404
    resp_del_tbl_404 = requests.delete(
        f"{table_endpoint}/Tables('{table_name}')", headers=headers
    )
    assert resp_del_tbl_404.status_code == 404
