# SPDX-License-Identifier: Apache-2.0

"""Smoke test suite ensuring 100% of registered Azure services respond on port 4747."""

import pytest
import requests
from conftest import DEVCLOUD_URL

AZURE_SERVICES = [
    {
        "id": "azureblob",
        "name": "Azure Blob Storage",
        "method": "GET",
        "path": "/devstoreaccount1?comp=list",
        "headers": {
            "x-ms-version": "2023-11-03",
            "Authorization": "SharedKey devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        },
        "expected_status": 200,
    },
    {
        "id": "azurequeue",
        "name": "Azure Queue Storage",
        "method": "GET",
        "path": "/devstoreaccount1/queue?comp=list",
        "headers": {
            "x-ms-version": "2023-11-03",
            "Authorization": "SharedKey devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        },
        "expected_status": 200,
    },
    {
        "id": "azuretable",
        "name": "Azure Table Storage",
        "method": "GET",
        "path": "/devstoreaccount1/Tables",
        "headers": {
            "x-ms-version": "2023-11-03",
            "Accept": "application/json;odata=nometadata",
            "Authorization": "SharedKey devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        },
        "expected_status": 200,
    },
    {
        "id": "cosmosdb",
        "name": "Azure Cosmos DB",
        "method": "GET",
        "path": "/dbs",
        "headers": {
            "x-ms-version": "2018-12-31",
            "Authorization": "type=master&ver=1.0&sig=c21va2U=",
        },
        "expected_status": 200,
    },
    {
        "id": "azurekeyvault",
        "name": "Azure Key Vault",
        "method": "GET",
        "path": "/secrets?api-version=7.4",
        "headers": {
            "Host": "smoke-vault.vault.azure.net",
        },
        "expected_status": 200,
    },
    {
        "id": "azureappconfig",
        "name": "Azure App Configuration",
        "method": "GET",
        "path": "/kv",
        "headers": {
            "Host": "smoke-app.azconfig.io",
        },
        "expected_status": 200,
    },
]


@pytest.mark.parametrize(
    "svc",
    AZURE_SERVICES,
    ids=[s["id"] for s in AZURE_SERVICES],
)
def test_azure_registered_service_smoke(devcloud_server, svc):
    """Verify that every registered Azure service is routed and responds on port 4747."""
    url = f"{DEVCLOUD_URL}{svc['path']}"
    resp = requests.request(
        method=svc["method"],
        url=url,
        headers=svc["headers"],
    )
    assert resp.status_code == svc["expected_status"], (
        f"Service {svc['name']} ({svc['id']}) failed smoke test at {url}: "
        f"got HTTP {resp.status_code}, expected {svc['expected_status']}. Response: {resp.text[:200]}"
    )
