from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobBlock, BlobServiceClient
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


def test_blob_conditional_upload(azure_blob_client):
    client = azure_blob_client
    container_name = "test-blob-conditional"
    container_client = client.create_container(container_name)

    try:
        blob_name = "conditional.txt"
        blob_client = container_client.get_blob_client(blob_name)

        # 1. Upload initial blob with overwrite=False
        blob_client.upload_blob(b"initial data", overwrite=False)

        # 2. Attempting upload with overwrite=False raises ResourceExistsError (If-None-Match: *)
        with pytest.raises(ResourceExistsError):
            blob_client.upload_blob(b"new data", overwrite=False)

        # 3. Uploading with overwrite=True succeeds
        blob_client.upload_blob(b"overwritten data", overwrite=True)
        assert blob_client.download_blob().readall() == b"overwritten data"
    finally:
        client.delete_container(container_name)


def test_blob_staged_block_upload(azure_blob_client):
    client = azure_blob_client
    container_name = "test-blob-staged-blocks"
    container_client = client.create_container(container_name)

    try:
        blob_name = "chunked.bin"
        blob_client = container_client.get_blob_client(blob_name)

        block1_id = "AAAAAA=="
        block2_id = "AQIDBA=="
        part1 = b"DevCloud Azure Blob "
        part2 = b"Staged Blocks Verified!"

        # 1. Stage block 1 and block 2
        blob_client.stage_block(block_id=block1_id, data=part1)
        blob_client.stage_block(block_id=block2_id, data=part2)

        # 2. Commit block list
        blob_client.commit_block_list(
            [BlobBlock(block_id=block1_id), BlobBlock(block_id=block2_id)]
        )

        # 3. Verify downloaded content is full concatenated payload
        downloaded = blob_client.download_blob().readall()
        assert downloaded == part1 + part2
    finally:
        client.delete_container(container_name)


def test_blob_custom_metadata(azure_blob_client):
    client = azure_blob_client
    container_name = "test-blob-metadata"
    container_client = client.create_container(container_name)

    try:
        blob_name = "report.pdf"
        blob_client = container_client.get_blob_client(blob_name)
        initial_meta = {"author": "devcloud", "phase": "3"}

        # 1. Upload blob with metadata
        blob_client.upload_blob(b"sample report", metadata=initial_meta)
        props = blob_client.get_blob_properties()
        assert props.metadata.get("author") == "devcloud"
        assert props.metadata.get("phase") == "3"

        # 2. Update metadata
        updated_meta = {
            "author": "devcloud",
            "phase": "3-complete",
            "status": "verified",
        }
        blob_client.set_blob_metadata(metadata=updated_meta)

        # 3. Verify updated metadata
        props2 = blob_client.get_blob_properties()
        assert props2.metadata.get("phase") == "3-complete"
        assert props2.metadata.get("status") == "verified"
    finally:
        client.delete_container(container_name)


def test_blob_byte_range_download(azure_blob_client):
    client = azure_blob_client
    container_name = "test-blob-range"
    container_client = client.create_container(container_name)

    try:
        blob_name = "stream.txt"
        blob_client = container_client.get_blob_client(blob_name)
        payload = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        blob_client.upload_blob(payload)

        # Download range (offset 10, length 5 -> "ABCDE")
        chunk = blob_client.download_blob(offset=10, length=5).readall()
        assert chunk == b"ABCDE"
    finally:
        client.delete_container(container_name)
