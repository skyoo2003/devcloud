# SPDX-License-Identifier: Apache-2.0

"""Smoke test suite ensuring 100% of registered GCP services respond on port 4747."""

import pytest
import requests
from conftest import DEVCLOUD_URL

GCP_SERVICES = [
    {
        "id": "gcs",
        "name": "Google Cloud Storage",
        "method": "GET",
        "path": "/storage/v1/b?project=test-project",
        "headers": {
            "Authorization": "Bearer devcloud-mock-token",
            "x-goog-project-id": "test-project",
        },
        "expected_status": 200,
    },
    {
        "id": "gcpsecrets",
        "name": "GCP Secret Manager",
        "method": "GET",
        "path": "/v1/projects/test-project/secrets",
        "headers": {
            "Authorization": "Bearer devcloud-mock-token",
            "x-goog-project-id": "test-project",
        },
        "expected_status": 200,
    },
    {
        "id": "gcppubsub",
        "name": "GCP Cloud Pub/Sub",
        "method": "GET",
        "path": "/v1/projects/test-project/topics",
        "headers": {
            "Authorization": "Bearer devcloud-mock-token",
            "x-goog-project-id": "test-project",
        },
        "expected_status": 200,
    },
]


@pytest.mark.parametrize(
    "svc",
    GCP_SERVICES,
    ids=[s["id"] for s in GCP_SERVICES],
)
def test_gcp_registered_service_smoke(devcloud_server, svc):
    """Verify that every registered GCP service is routed and responds on port 4747."""
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
