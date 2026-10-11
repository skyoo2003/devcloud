import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def cosmos_endpoint(devcloud_server):
    return f"{DEVCLOUD_URL}/devstoreaccount1"


def test_azure_cosmos_lifecycle_and_documents(cosmos_endpoint):
    db_id = "test-db"
    coll_id = "users"
    headers = {
        "x-ms-version": "2018-12-31",
        "Content-Type": "application/json",
        "Authorization": "type=master&ver=1.0&sig=c29tZXNpZ25hdHVyZQ==",
    }

    # 1. Create Database
    resp_create_db = requests.post(
        f"{cosmos_endpoint}/dbs", headers=headers, json={"id": db_id}
    )
    assert resp_create_db.status_code == 201
    assert resp_create_db.json()["id"] == db_id

    # 2. Duplicate Create Database returns 409
    resp_dup_db = requests.post(
        f"{cosmos_endpoint}/dbs", headers=headers, json={"id": db_id}
    )
    assert resp_dup_db.status_code == 409

    # 3. List Databases
    resp_list_dbs = requests.get(f"{cosmos_endpoint}/dbs", headers=headers)
    assert resp_list_dbs.status_code == 200
    dbs = resp_list_dbs.json().get("Databases", [])
    assert any(d["id"] == db_id for d in dbs)

    # 4. Get Database
    resp_get_db = requests.get(f"{cosmos_endpoint}/dbs/{db_id}", headers=headers)
    assert resp_get_db.status_code == 200
    assert resp_get_db.json()["id"] == db_id

    # 5. Create Container
    resp_create_coll = requests.post(
        f"{cosmos_endpoint}/dbs/{db_id}/colls",
        headers=headers,
        json={"id": coll_id, "partitionKey": {"paths": ["/pk"]}},
    )
    assert resp_create_coll.status_code == 201
    assert resp_create_coll.json()["id"] == coll_id

    # 6. Duplicate Container returns 409
    resp_dup_coll = requests.post(
        f"{cosmos_endpoint}/dbs/{db_id}/colls",
        headers=headers,
        json={"id": coll_id, "partitionKey": {"paths": ["/pk"]}},
    )
    assert resp_dup_coll.status_code == 409

    # 7. List Containers
    resp_list_colls = requests.get(
        f"{cosmos_endpoint}/dbs/{db_id}/colls", headers=headers
    )
    assert resp_list_colls.status_code == 200
    colls = resp_list_colls.json().get("DocumentCollections", [])
    assert any(c["id"] == coll_id for c in colls)

    # 8. Get Container
    resp_get_coll = requests.get(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}", headers=headers
    )
    assert resp_get_coll.status_code == 200
    assert resp_get_coll.json()["id"] == coll_id

    # 9. Create Document
    doc = {
        "id": "doc-42",
        "pk": "tenant-1",
        "name": "Grace Hopper",
        "compiler": "A-0",
    }
    resp_create_doc = requests.post(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs",
        headers=headers,
        json=doc,
    )
    assert resp_create_doc.status_code == 201
    created_doc = resp_create_doc.json()
    assert created_doc["id"] == "doc-42"
    assert "_rid" in created_doc

    # 10. Duplicate Document returns 409
    resp_dup_doc = requests.post(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs",
        headers=headers,
        json=doc,
    )
    assert resp_dup_doc.status_code == 409

    # 11. Get Document
    resp_get_doc = requests.get(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs/doc-42",
        headers=headers,
    )
    assert resp_get_doc.status_code == 200
    assert resp_get_doc.json()["name"] == "Grace Hopper"

    # 12. Replace Document
    doc["name"] = "Rear Admiral Grace Hopper"
    resp_put_doc = requests.put(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs/doc-42",
        headers=headers,
        json=doc,
    )
    assert resp_put_doc.status_code == 200
    assert resp_put_doc.json()["name"] == "Rear Admiral Grace Hopper"

    # 13. List Documents
    resp_list_docs = requests.get(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs",
        headers=headers,
    )
    assert resp_list_docs.status_code == 200
    docs = resp_list_docs.json().get("Documents", [])
    assert len(docs) == 1
    assert docs[0]["name"] == "Rear Admiral Grace Hopper"

    # 14. Delete Document
    resp_del_doc = requests.delete(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}/docs/doc-42",
        headers=headers,
    )
    assert resp_del_doc.status_code == 204

    # 15. Delete Container
    resp_del_coll = requests.delete(
        f"{cosmos_endpoint}/dbs/{db_id}/colls/{coll_id}",
        headers=headers,
    )
    assert resp_del_coll.status_code == 204

    # 16. Delete Database
    resp_del_db = requests.delete(f"{cosmos_endpoint}/dbs/{db_id}", headers=headers)
    assert resp_del_db.status_code == 204
