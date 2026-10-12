# SPDX-License-Identifier: Apache-2.0

import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def appconfig_endpoint(devcloud_server):
    return DEVCLOUD_URL


def test_azure_appconfig_lifecycle(appconfig_endpoint):
    key = "app:feature:setting1"
    headers = {
        "Content-Type": "application/json",
        "Host": "myconfig.azconfig.io",
    }

    # 1. Set key-value without label
    put_body = {
        "value": "initial-config-val",
        "content_type": "text/plain",
        "tags": {"tier": "backend"},
    }
    resp = requests.put(
        f"{appconfig_endpoint}/kv/{key}",
        json=put_body,
        headers=headers,
    )
    assert resp.status_code == 200
    kv = resp.json()
    assert kv["key"] == key
    assert kv["value"] == "initial-config-val"
    assert kv["content_type"] == "text/plain"
    assert kv["tags"]["tier"] == "backend"
    assert "etag" in kv
    assert "ETag" in resp.headers

    # 2. Set key-value with label
    put_staging = {
        "value": "staging-config-val",
    }
    resp_staging = requests.put(
        f"{appconfig_endpoint}/kv/{key}?label=staging",
        json=put_staging,
        headers=headers,
    )
    assert resp_staging.status_code == 200
    kv_staging = resp_staging.json()
    assert kv_staging["key"] == key
    assert kv_staging["label"] == "staging"
    assert kv_staging["value"] == "staging-config-val"

    # 3. Get key-value without label
    resp_get = requests.get(
        f"{appconfig_endpoint}/kv/{key}",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["value"] == "initial-config-val"

    # 4. Get key-value with label
    resp_get_staging = requests.get(
        f"{appconfig_endpoint}/kv/{key}?label=staging",
        headers=headers,
    )
    assert resp_get_staging.status_code == 200
    assert resp_get_staging.json()["value"] == "staging-config-val"

    # 5. List key-values
    resp_list = requests.get(
        f"{appconfig_endpoint}/kv",
        headers=headers,
    )
    assert resp_list.status_code == 200
    items = resp_list.json().get("items", [])
    keys_found = [it["key"] for it in items if it["key"] == key]
    assert len(keys_found) >= 2

    # 6. Delete key-value without label
    resp_del = requests.delete(
        f"{appconfig_endpoint}/kv/{key}",
        headers=headers,
    )
    assert resp_del.status_code == 200

    # 7. Get after delete returns 404
    resp_not_found = requests.get(
        f"{appconfig_endpoint}/kv/{key}",
        headers=headers,
    )
    assert resp_not_found.status_code == 404

    # 8. Delete again returns 204
    resp_del_again = requests.delete(
        f"{appconfig_endpoint}/kv/{key}",
        headers=headers,
    )
    assert resp_del_again.status_code == 204

    # 9. Clean up staging key
    requests.delete(
        f"{appconfig_endpoint}/kv/{key}?label=staging",
        headers=headers,
    )
