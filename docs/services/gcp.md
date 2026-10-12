# Google Cloud Platform (GCP) Services

DevCloud provides local emulation for Google Cloud Platform services on port 4747, covering object storage, secret management, and messaging APIs.

## Supported Services

| Service | Identifier | Local limits and backend |
|---|---|---|
| Cloud Storage | `gcs` | Local filesystem-backed FileStore under `data/gcs`. Supports bucket CRUD, object CRUD, multipart uploads, media downloads, and CRC32c / MD5 checksums. |
| Secret Manager | `gcpsecrets` | In-memory secret store. Supports secret creation, listing, deletion, payload versioning, access (`:access`), and payload destruction (`:destroy`). |
| Cloud Pub/Sub | `gcppubsub` | In-memory messaging engine. Supports topic CRUD, message publishing (`:publish`), subscription CRUD, message pull (`:pull`), and message acknowledgement (`:acknowledge`). |

## Operations

### Cloud Storage (`gcs`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Buckets | `ListBuckets` | `GET /storage/v1/b?project={project}` | Returns JSON list of buckets in the project |
| Buckets | `InsertBucket` | `POST /storage/v1/b` | Creates a new bucket (`201 Created` or `409 Conflict`) |
| Buckets | `GetBucket` | `GET /storage/v1/b/{bucket}` | Returns bucket metadata and timestamps |
| Buckets | `DeleteBucket` | `DELETE /storage/v1/b/{bucket}` | Deletes bucket (`204 No Content` or `404 Not Found`) |
| Objects | `ListObjects` | `GET /storage/v1/b/{bucket}/o` | Lists objects within a bucket |
| Objects | `InsertObject` | `POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={name}` | Uploads raw binary object payload |
| Objects | `GetObject` | `GET /storage/v1/b/{bucket}/o/{object}?alt=media` | Downloads object binary content with checksums |
| Objects | `GetObjectMetadata` | `GET /storage/v1/b/{bucket}/o/{object}` | Returns object JSON metadata (size, contentType, md5Hash, crc32c) |
| Objects | `DeleteObject` | `DELETE /storage/v1/b/{bucket}/o/{object}` | Removes object (`204 No Content` or `404 Not Found`) |

### Secret Manager (`gcpsecrets`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Secrets | `CreateSecret` | `POST /v1/projects/{project}/secrets?secretId={id}` | Creates secret metadata resource |
| Secrets | `GetSecret` | `GET /v1/projects/{project}/secrets/{secret}` | Retrieves secret resource metadata |
| Secrets | `ListSecrets` | `GET /v1/projects/{project}/secrets` | Lists secrets within a project |
| Secrets | `DeleteSecret` | `DELETE /v1/projects/{project}/secrets/{secret}` | Deletes secret and all its payload versions |
| Versions | `AddSecretVersion` | `POST /v1/projects/{project}/secrets/{secret}:addVersion` | Adds a new base64 payload version |
| Versions | `AccessSecretVersion` | `GET /v1/projects/{project}/secrets/{secret}/versions/{version}:access` | Accesses secret payload (supports version number or `latest`) |
| Versions | `DestroySecretVersion` | `POST /v1/projects/{project}/secrets/{secret}/versions/{version}:destroy` | Destroys version payload data |

### Cloud Pub/Sub (`gcppubsub`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Topics | `CreateTopic` | `PUT /v1/projects/{project}/topics/{topic}` | Creates a Pub/Sub topic |
| Topics | `GetTopic` | `GET /v1/projects/{project}/topics/{topic}` | Retrieves topic resource details |
| Topics | `ListTopics` | `GET /v1/projects/{project}/topics` | Lists all topics in the project |
| Topics | `DeleteTopic` | `DELETE /v1/projects/{project}/topics/{topic}` | Deletes topic and detached associations |
| Topics | `Publish` | `POST /v1/projects/{project}/topics/{topic}:publish` | Publishes base64 message batch to subscribers |
| Subscriptions | `CreateSubscription` | `PUT /v1/projects/{project}/subscriptions/{sub}` | Creates subscription bound to a topic |
| Subscriptions | `GetSubscription` | `GET /v1/projects/{project}/subscriptions/{sub}` | Retrieves subscription settings |
| Subscriptions | `ListSubscriptions` | `GET /v1/projects/{project}/subscriptions` | Lists subscriptions in the project |
| Subscriptions | `DeleteSubscription` | `DELETE /v1/projects/{project}/subscriptions/{sub}` | Deletes subscription |
| Subscriptions | `Pull` | `POST /v1/projects/{project}/subscriptions/{sub}:pull` | Pulls delivered messages with ack IDs |
| Subscriptions | `Acknowledge` | `POST /v1/projects/{project}/subscriptions/{sub}:acknowledge` | Confirms message processing by ack IDs |

## Configuration

In `devcloud.yaml`, GCP services are configured under `providers.gcp`:

```yaml
server:
  port: 4747

providers:
  gcp:
    services:
      gcs:
        enabled: true
        data_dir: ./data/gcp/gcs
      gcpsecrets:
        enabled: true
      gcppubsub:
        enabled: true
```

## Credentials & Endpoint Setup

DevCloud accepts Google OAuth2 Bearer tokens (`Bearer ya29...`) or placeholder credentials without rejecting calls on signature verification.

To direct the Google Cloud Python SDK to DevCloud:

```bash
export STORAGE_EMULATOR_HOST="http://127.0.0.1:4747"
export GOOGLE_APPLICATION_CREDENTIALS="/dev/null"
```

## Python SDK Examples

### Secret Manager (`google-cloud-secret-manager`)

```python
import base64
import requests

endpoint = "http://localhost:4747"
project = "local-project"
headers = {"Authorization": "Bearer local-token"}

# 1. Create a secret
requests.post(
    f"{endpoint}/v1/projects/{project}/secrets?secretId=DatabasePassword",
    json={"replication": {"automatic": {}}},
    headers=headers,
)

# 2. Add a secret version
payload = base64.b64encode(b"ProductionDatabasePassword123").decode()
requests.post(
    f"{endpoint}/v1/projects/{project}/secrets/DatabasePassword:addVersion",
    json={"payload": {"data": payload}},
    headers=headers,
)

# 3. Access secret version (latest)
resp = requests.get(
    f"{endpoint}/v1/projects/{project}/secrets/DatabasePassword/versions/latest:access",
    headers=headers,
)
retrieved = base64.b64decode(resp.json()["payload"]["data"]).decode()
print("Retrieved secret:", retrieved)
```

### Cloud Pub/Sub (`google-cloud-pubsub`)

```python
import base64
import requests

endpoint = "http://localhost:4747"
project = "local-project"
headers = {"Authorization": "Bearer local-token"}

# 1. Create a topic
requests.put(
    f"{endpoint}/v1/projects/{project}/topics/order-events",
    headers=headers,
)

# 2. Create a subscription
requests.put(
    f"{endpoint}/v1/projects/{project}/subscriptions/order-worker",
    json={"topic": f"projects/{project}/topics/order-events"},
    headers=headers,
)

# 3. Publish a message
msg_data = base64.b64encode(b"order_id=456 status=paid").decode()
requests.post(
    f"{endpoint}/v1/projects/{project}/topics/order-events:publish",
    json={"messages": [{"data": msg_data}]},
    headers=headers,
)

# 4. Pull and acknowledge message
pull_resp = requests.post(
    f"{endpoint}/v1/projects/{project}/subscriptions/order-worker:pull",
    json={"maxMessages": 10},
    headers=headers,
)
messages = pull_resp.json().get("receivedMessages", [])
for m in messages:
    ack_id = m["ackId"]
    content = base64.b64decode(m["message"]["data"]).decode()
    print("Received:", content)
    requests.post(
        f"{endpoint}/v1/projects/{project}/subscriptions/order-worker:acknowledge",
        json={"ackIds": [ack_id]},
        headers=headers,
    )
```
