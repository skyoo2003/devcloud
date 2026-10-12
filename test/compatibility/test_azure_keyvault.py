# SPDX-License-Identifier: Apache-2.0

import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def keyvault_endpoint(devcloud_server):
    return DEVCLOUD_URL


def test_azure_keyvault_secret_lifecycle(keyvault_endpoint):
    secret_name = "test-compat-secret"
    headers = {
        "Content-Type": "application/json",
        "Host": "myvault.vault.azure.net",
    }

    # 1. Set secret (v1)
    put_body = {
        "value": "initial-secret-value",
        "contentType": "text/plain",
        "tags": {"env": "test", "owner": "devcloud"},
    }
    resp = requests.put(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        json=put_body,
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["value"] == "initial-secret-value"
    assert data["contentType"] == "text/plain"
    assert data["tags"]["env"] == "test"
    v1_id = data["id"]
    v1_version = v1_id.split("/")[-1]

    # 2. Get secret (latest)
    resp_get = requests.get(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["value"] == "initial-secret-value"

    # 3. Set new secret version (v2)
    put_body_v2 = {
        "value": "updated-secret-value",
    }
    resp_v2 = requests.put(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        json=put_body_v2,
        headers=headers,
    )
    assert resp_v2.status_code == 200
    v2_id = resp_v2.json()["id"]
    v2_version = v2_id.split("/")[-1]
    assert v2_version != v1_version

    # 4. Get specific version (v1)
    resp_v1_get = requests.get(
        f"{keyvault_endpoint}/secrets/{secret_name}/{v1_version}?api-version=7.4",
        headers=headers,
    )
    assert resp_v1_get.status_code == 200
    assert resp_v1_get.json()["value"] == "initial-secret-value"

    # 5. Get latest returns v2
    resp_latest = requests.get(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_latest.status_code == 200
    assert resp_latest.json()["value"] == "updated-secret-value"

    # 6. List secret versions
    resp_versions = requests.get(
        f"{keyvault_endpoint}/secrets/{secret_name}/versions?api-version=7.4",
        headers=headers,
    )
    assert resp_versions.status_code == 200
    versions_list = resp_versions.json().get("value", [])
    assert len(versions_list) >= 2

    # 7. List secrets in vault
    resp_list = requests.get(
        f"{keyvault_endpoint}/secrets?api-version=7.4",
        headers=headers,
    )
    assert resp_list.status_code == 200
    secrets_list = resp_list.json().get("value", [])
    assert any(
        s["id"].startswith(f"https://myvault.vault.azure.net/secrets/{secret_name}")
        for s in secrets_list
    )

    # 8. Delete secret
    resp_del = requests.delete(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_del.status_code == 200

    # 9. Get after delete returns 404
    resp_not_found = requests.get(
        f"{keyvault_endpoint}/secrets/{secret_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_not_found.status_code == 404


def test_azure_keyvault_key_lifecycle(keyvault_endpoint):
    key_name = "test-compat-key"
    headers = {
        "Content-Type": "application/json",
        "Host": "myvault.vault.azure.net",
    }

    # 1. Create Key
    create_body = {
        "kty": "RSA",
        "key_size": 2048,
        "key_ops": ["encrypt", "decrypt", "sign", "verify"],
        "tags": {"purpose": "testing"},
    }
    resp = requests.post(
        f"{keyvault_endpoint}/keys/{key_name}/create?api-version=7.4",
        json=create_body,
        headers=headers,
    )
    assert resp.status_code == 200
    key_data = resp.json()
    assert "key" in key_data
    assert key_data["key"]["kty"] == "RSA"
    assert "kid" in key_data["key"]

    # 2. Get Key
    resp_get = requests.get(
        f"{keyvault_endpoint}/keys/{key_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["key"]["kid"] == key_data["key"]["kid"]

    # 3. List Keys
    resp_list = requests.get(
        f"{keyvault_endpoint}/keys?api-version=7.4",
        headers=headers,
    )
    assert resp_list.status_code == 200
    keys_list = resp_list.json().get("value", [])
    assert any(k["kid"] == key_data["key"]["kid"] for k in keys_list)

    # 4. Delete Key
    resp_del = requests.delete(
        f"{keyvault_endpoint}/keys/{key_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_del.status_code == 200

    # 5. Get after delete returns 404
    resp_not_found = requests.get(
        f"{keyvault_endpoint}/keys/{key_name}?api-version=7.4",
        headers=headers,
    )
    assert resp_not_found.status_code == 404
