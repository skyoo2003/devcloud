# Azure Services

DevCloud expands its local development runtime to include Microsoft Azure services, starting with Azure Blob Storage as the Phase 3 multi-CSP pilot.

## Supported Services

| Service | Supported | Local limits |
|---|---|---|
| Blob Storage (`blob`) | Supported (Phase 3 pilot) | Local filesystem-backed FileStore under `data/blob`. Supports container CRUD, BlockBlob CRUD, listing/enumeration, and byte-range downloads. |

## Operations

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Containers | `CreateContainer` | `PUT /{container}?restype=container` | Returns `201 Created` or `409 ContainerAlreadyExists` |
| Containers | `DeleteContainer` | `DELETE /{container}?restype=container` | Returns `202 Accepted` or `404 ContainerNotFound` |
| Containers | `GetContainerProperties` | `GET /{container}?restype=container` | Returns lease state and metadata headers |
| Containers | `ListContainers` | `GET /?comp=list` | Returns XML `EnumerationResults` listing all containers |
| Blobs | `PutBlob` | `PUT /{container}/{blob}` | Creates or replaces a BlockBlob; stores payload and computes MD5 |
| Blobs | `GetBlob` | `GET /{container}/{blob}` | Downloads blob content; supports `Range` and `x-ms-range` requests |
| Blobs | `GetBlobProperties` | `HEAD /{container}/{blob}` | Returns headers (`x-ms-blob-type: BlockBlob`, size, ETag, Content-MD5) |
| Blobs | `DeleteBlob` | `DELETE /{container}/{blob}` | Permanently removes the blob |
| Blobs | `ListBlobs` | `GET /{container}?restype=container&comp=list` | Returns XML `EnumerationResults` listing blobs in the container |

## Configuration

In `devcloud.yaml`, Azure services are configured under the namespaced `providers.azure` block:

```yaml
server:
  port: 4747

providers:
  azure:
    services:
      blob:
        enabled: true
        data_dir: ./data/blob
```

## Credentials & Connection Strings

DevCloud accepts Azure Shared Key, SharedKeyLite, and Azurite emulator credentials without signature validation:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:4747/devstoreaccount1;"
```

## Python SDK Quickstart

Using `azure-storage-blob`:

```python
from azure.storage.blob import BlobServiceClient

connection_string = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:4747/devstoreaccount1;"
)

client = BlobServiceClient.from_connection_string(connection_string)

# 1. Create a container
container_client = client.create_container("my-container")

# 2. Upload a BlockBlob
blob_client = container_client.get_blob_client("notes.txt")
blob_client.upload_blob(b"Hello from local DevCloud Azure Blob Storage!")

# 3. Download the blob
content = blob_client.download_blob().readall()
print(content.decode("utf-8"))

# 4. List blobs
for blob in container_client.list_blobs():
    print(f"Blob: {blob.name}, Size: {blob.size}")
```
