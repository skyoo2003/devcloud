from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from conftest import DEVCLOUD_URL
import pytest


@pytest.fixture
def azure_blob_client(devcloud_server):
    endpoint = f"{DEVCLOUD_URL}/devstoreaccount1"
    conn_str = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint={endpoint};"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def test_blob_container_lifecycle(azure_blob_client):
    client = azure_blob_client
    container_name = "test-container-lifecycle"

    # 1. Create container
    container_client = client.create_container(container_name)
    assert container_client.container_name == container_name

    # 2. Duplicate create raises ResourceExistsError
    with pytest.raises(ResourceExistsError):
        client.create_container(container_name)

    # 3. Get container properties
    props = container_client.get_container_properties()
    assert props.name == container_name
    assert props.etag is not None

    # 4. List containers
    container_names = [c.name for c in client.list_containers()]
    assert container_name in container_names

    # 5. Delete container
    client.delete_container(container_name)

    # 6. Accessing deleted container raises ResourceNotFoundError
    with pytest.raises(ResourceNotFoundError):
        container_client.get_container_properties()


def test_blob_crud_lifecycle(azure_blob_client):
    client = azure_blob_client
    container_name = "test-blob-crud"
    container_client = client.create_container(container_name)

    try:
        blob_name = "docs/sample.txt"
        blob_client = container_client.get_blob_client(blob_name)
        payload = b"DevCloud Azure Blob Storage Phase 3 Test Payload"

        # 1. Upload BlockBlob
        blob_client.upload_blob(payload)

        # 2. Get Blob Properties
        props = blob_client.get_blob_properties()
        assert props.name == blob_name
        assert props.size == len(payload)
        assert props.blob_type == "BlockBlob"

        # 3. Download Blob and verify data
        download_stream = blob_client.download_blob()
        downloaded = download_stream.readall()
        assert downloaded == payload

        # 4. List Blobs in container
        blob_names = [b.name for b in container_client.list_blobs()]
        assert blob_name in blob_names

        # 5. Delete Blob
        blob_client.delete_blob()

        # 6. Accessing deleted blob raises ResourceNotFoundError
        with pytest.raises(ResourceNotFoundError):
            blob_client.get_blob_properties()
    finally:
        # Clean up container
        client.delete_container(container_name)
