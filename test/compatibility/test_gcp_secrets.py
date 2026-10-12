# SPDX-License-Identifier: Apache-2.0

import base64
import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def gcp_base_url(devcloud_server):
    return DEVCLOUD_URL


def test_gcp_secret_manager_lifecycle(gcp_base_url):
    project = "test-project-1"
    secret_id = "database-master-key"
    headers = {
        "Authorization": "Bearer ya29.devcloud-pilot-token",
        "Content-Type": "application/json",
    }

    # 1. Create Secret
    resp = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/secrets?secretId={secret_id}",
        headers=headers,
        json={"labels": {"environment": "test", "tier": "backend"}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == f"projects/{project}/secrets/{secret_id}"

    # 2. Duplicate Create -> 409 Conflict
    resp_dup = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/secrets?secretId={secret_id}",
        headers=headers,
        json={},
    )
    assert resp_dup.status_code == 409

    # 3. Get Secret
    resp_get = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["name"] == f"projects/{project}/secrets/{secret_id}"

    # 4. List Secrets
    resp_list = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets",
        headers=headers,
    )
    assert resp_list.status_code == 200
    secrets = [s["name"] for s in resp_list.json().get("secrets", [])]
    assert f"projects/{project}/secrets/{secret_id}" in secrets

    # 5. Add Secret Version
    secret_text = "SuperSecretPassword2026!"
    b64_payload = base64.b64encode(secret_text.encode("utf-8")).decode("utf-8")
    resp_v1 = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}:addVersion",
        headers=headers,
        json={"payload": {"data": b64_payload}},
    )
    assert resp_v1.status_code == 200
    v1_data = resp_v1.json()
    assert v1_data["name"] == f"projects/{project}/secrets/{secret_id}/versions/1"
    assert v1_data["state"] == "ENABLED"

    # 6. Access Secret Version (Specific version 1)
    resp_access = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}/versions/1:access",
        headers=headers,
    )
    assert resp_access.status_code == 200
    retrieved_b64 = resp_access.json()["payload"]["data"]
    assert base64.b64decode(retrieved_b64).decode("utf-8") == secret_text

    # 7. Access Secret Version (latest alias)
    resp_access_latest = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}/versions/latest:access",
        headers=headers,
    )
    assert resp_access_latest.status_code == 200
    assert resp_access_latest.json()["payload"]["data"] == b64_payload

    # 8. List Versions
    resp_v_list = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}/versions",
        headers=headers,
    )
    assert resp_v_list.status_code == 200
    versions = resp_v_list.json().get("versions", [])
    assert len(versions) == 1

    # 9. Destroy Version
    resp_destroy = requests.post(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}/versions/1:destroy",
        headers=headers,
    )
    assert resp_destroy.status_code == 200
    assert resp_destroy.json()["state"] == "DESTROYED"

    # 10. Access Destroyed Version -> 400 Bad Request
    resp_access_destroyed = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}/versions/1:access",
        headers=headers,
    )
    assert resp_access_destroyed.status_code == 400

    # 11. Delete Secret
    resp_del = requests.delete(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}",
        headers=headers,
    )
    assert resp_del.status_code == 200

    # 12. Get Deleted Secret -> 404
    resp_get_del = requests.get(
        f"{gcp_base_url}/v1/projects/{project}/secrets/{secret_id}",
        headers=headers,
    )
    assert resp_get_del.status_code == 404
