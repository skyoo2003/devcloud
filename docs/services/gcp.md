# Google Cloud Platform (GCP) Services

DevCloud extends its multi-CSP runtime to support Google Cloud services, starting with Google Cloud Storage (GCS) as the first-class GCP pilot.

## Supported Services

| Service | Supported | Local limits |
|---|---|---|
| Google Cloud Storage (`gcs`) | Supported (Phase 4 pilot) | Local filesystem-backed FileStore under `data/gcs`. Supports bucket CRUD, object CRUD, simple/media multipart uploads, `alt=media` downloads, metadata inspection, and CRC32c / MD5 checksums. |

## Operations

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Buckets | `ListBuckets` | `GET /storage/v1/b?project={project}` | Returns JSON list of buckets in the project |
| Buckets | `InsertBucket` | `POST /storage/v1/b` | Creates a new bucket (`201 Created` or `409 Conflict`) |
| Buckets | `GetBucket` | `GET /storage/v1/b/{bucket}` | Returns bucket metadata and timestamp |
| Buckets | `DeleteBucket` | `DELETE /storage/v1/b/{bucket}` | Deletes bucket (`204 No Content` or `404 Not Found`) |
| Objects | `ListObjects` | `GET /storage/v1/b/{bucket}/o` | Lists objects within a bucket |
| Objects | `InsertObject` | `POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}` | Uploads raw binary object payload |
| Objects | `GetObject` | `GET /storage/v1/b/{bucket}/o/{object}?alt=media` | Downloads object binary content with `x-goog-hash` checksums |
| Objects | `GetObjectMetadata` | `GET /storage/v1/b/{bucket}/o/{object}` | Returns object JSON metadata (size, contentType, md5Hash, crc32c) |
| Objects | `DeleteObject` | `DELETE /storage/v1/b/{bucket}/o/{object}` | Removes object (`204 No Content` or `404 Not Found`) |

## Configuration

In `devcloud.yaml`, GCP services are configured under the namespaced `providers.gcp` block:

```yaml
server:
  port: 4747

providers:
  gcp:
    services:
      gcs:
        enabled: true
        data_dir: ./data/gcp/gcs
```

## Environment Variables & Credentials

DevCloud accepts Google OAuth2 Bearer tokens (`Bearer ya29...`) and mock service account credentials without rejecting on signature validation.

To point the official Google Cloud Python SDK to DevCloud:

```bash
export STORAGE_EMULATOR_HOST="http://127.0.0.1:4747"
export GOOGLE_APPLICATION_CREDENTIALS="/dev/null"
```

## Python SDK Quickstart

### Using `google-cloud-storage`

```python
import os
from google.cloud import storage

# Point SDK to local DevCloud endpoint
os.environ["STORAGE_EMULATOR_HOST"] = "http://127.0.0.1:4747"

client = storage.Client(project="local-project")

# 1. Create a Bucket
bucket = client.create_bucket("sample-bucket")
print(f"Bucket created: {bucket.name}")

# 2. Upload an Object
blob = bucket.blob("documents/hello.txt")
blob.upload_from_string(b"Hello from local DevCloud Google Cloud Storage!", content_type="text/plain")
print(f"Uploaded object: {blob.name}, size: {blob.size}")

# 3. Download the Object
downloaded = blob.download_as_bytes()
print(f"Downloaded content: {downloaded.decode('utf-8')}")

# 4. List Objects
for item in client.list_blobs("sample-bucket"):
    print(f"Object: {item.name}, MD5: {item.md5_hash}, CRC32c: {item.crc32c}")

# 5. Delete Object and Bucket
blob.delete()
bucket.delete()
```

### Using HTTP Requests / `curl`

```bash
# Create bucket
curl -X POST http://localhost:4747/storage/v1/b \
     -H "Content-Type: application/json" \
     -d '{"name": "my-gcs-bucket"}'

# Upload object
curl -X POST "http://localhost:4747/upload/storage/v1/b/my-gcs-bucket/o?uploadType=media&name=notes.txt" \
     -H "Content-Type: text/plain" \
     --data-binary "Local GCS in DevCloud"

# Download object
curl -s "http://localhost:4747/storage/v1/b/my-gcs-bucket/o/notes.txt?alt=media"
```
