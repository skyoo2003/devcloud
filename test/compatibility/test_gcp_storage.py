import pytest
import requests
from conftest import DEVCLOUD_URL


@pytest.fixture
def gcs_base_url(devcloud_server):
    return DEVCLOUD_URL


def test_gcs_bucket_lifecycle(gcs_base_url):
    project = "test-proj"
    bucket_name = "test-lifecycle-bucket"
    headers = {
        "Authorization": "Bearer ya29.devcloud-pilot-token",
        "Content-Type": "application/json",
    }

    # 1. Create bucket
    resp = requests.post(
        f"{gcs_base_url}/storage/v1/b?project={project}",
        headers=headers,
        json={"name": bucket_name, "location": "US", "storageClass": "STANDARD"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == bucket_name
    assert data["kind"] == "storage#bucket"

    # 2. Duplicate create raises 409
    resp_dup = requests.post(
        f"{gcs_base_url}/storage/v1/b?project={project}",
        headers=headers,
        json={"name": bucket_name},
    )
    assert resp_dup.status_code == 409

    # 3. Get bucket
    resp_get = requests.get(
        f"{gcs_base_url}/storage/v1/b/{bucket_name}?project={project}",
        headers=headers,
    )
    assert resp_get.status_code == 200
    assert resp_get.json()["name"] == bucket_name

    # 4. List buckets
    resp_list = requests.get(
        f"{gcs_base_url}/storage/v1/b?project={project}",
        headers=headers,
    )
    assert resp_list.status_code == 200
    bucket_names = [b["name"] for b in resp_list.json().get("items", [])]
    assert bucket_name in bucket_names

    # 5. Delete bucket
    resp_del = requests.delete(
        f"{gcs_base_url}/storage/v1/b/{bucket_name}?project={project}",
        headers=headers,
    )
    assert resp_del.status_code == 204

    # 6. Accessing deleted bucket returns 404
    resp_deleted = requests.get(
        f"{gcs_base_url}/storage/v1/b/{bucket_name}?project={project}",
        headers=headers,
    )
    assert resp_deleted.status_code == 404


def test_gcs_object_crud_lifecycle(gcs_base_url):
    project = "test-proj"
    bucket_name = "test-object-crud-bucket"
    headers = {"Authorization": "Bearer ya29.devcloud-pilot-token"}

    # Setup bucket
    requests.post(
        f"{gcs_base_url}/storage/v1/b?project={project}",
        headers=headers,
        json={"name": bucket_name},
    )

    try:
        object_name = "docs/welcome.txt"
        payload = b"Hello Google Cloud Storage from local DevCloud runtime!"

        # 1. Upload object via media upload
        resp_upload = requests.post(
            f"{gcs_base_url}/upload/storage/v1/b/{bucket_name}/o?uploadType=media&name={object_name}&project={project}",
            headers={
                "Authorization": "Bearer ya29.devcloud-pilot-token",
                "Content-Type": "text/plain",
            },
            data=payload,
        )
        assert resp_upload.status_code == 200
        obj_data = resp_upload.json()
        assert obj_data["name"] == object_name
        assert obj_data["bucket"] == bucket_name
        assert obj_data["md5Hash"] != ""
        assert obj_data["crc32c"] != ""

        # 2. Get object metadata
        resp_meta = requests.get(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}/o/{object_name}?project={project}",
            headers=headers,
        )
        assert resp_meta.status_code == 200
        assert resp_meta.json()["contentType"] == "text/plain"

        # 3. Download object payload (alt=media)
        resp_down = requests.get(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}/o/{object_name}?alt=media&project={project}",
            headers=headers,
        )
        assert resp_down.status_code == 200
        assert resp_down.content == payload
        assert "x-goog-hash" in resp_down.headers

        # 4. List objects
        resp_list = requests.get(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}/o?prefix=docs/&project={project}",
            headers=headers,
        )
        assert resp_list.status_code == 200
        items = resp_list.json().get("items", [])
        assert any(item["name"] == object_name for item in items)

        # 5. Delete object
        resp_del = requests.delete(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}/o/{object_name}?project={project}",
            headers=headers,
        )
        assert resp_del.status_code == 204

        # 6. Download deleted object returns 404
        resp_down_del = requests.get(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}/o/{object_name}?alt=media&project={project}",
            headers=headers,
        )
        assert resp_down_del.status_code == 404

    finally:
        # Cleanup bucket
        requests.delete(
            f"{gcs_base_url}/storage/v1/b/{bucket_name}?project={project}",
            headers=headers,
        )
