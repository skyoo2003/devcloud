# Azure Services

DevCloud provides local emulation for Microsoft Azure services on port 4747, covering storage, messaging, database, secret management, and configuration APIs.

## Supported Services

| Service | Identifier | Local limits and backend |
|---|---|---|
| Blob Storage | `blob` | Filesystem-backed FileStore under `data/blob`. Supports container CRUD, BlockBlob CRUD, staged chunk uploads (`PutBlock`, `PutBlockList`), listing, and byte-range downloads. |
| Queue Storage | `azurequeue` | In-memory message store with optional file persistence under `data/azurequeue`. Supports queue lifecycle, `PutMessage`, `GetMessages` with visibility timeouts, `PeekMessages`, and `DeleteMessage` with pop receipts. |
| Table Storage | `azuretable` | SQLite-backed OData JSON table and entity engine under `data/azuretable`. Supports table CRUD, entity insert/replace/merge/delete, PartitionKey/RowKey indexing, and queries. |
| Cosmos DB | `cosmosdb` | SQLite-backed NoSQL document store under `data/cosmos`. Supports database CRUD, collection CRUD with partition keys, document CRUD, and system attributes (`_rid`, `_etag`, `_ts`). |
| Key Vault | `azurekeyvault` | In-memory secret and cryptographic key store. Supports secret creation, versioning, retrieval, soft deletion, and RSA software-backed key generation and management. |
| App Configuration | `azureappconfig` | In-memory key-value configuration store with ETag tracking, user-defined tags, and label filtering (`?label=...`). |

## Operations

### Blob Storage (`blob`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Containers | `CreateContainer` | `PUT /{container}?restype=container` | Returns `201 Created` or `409 ContainerAlreadyExists` |
| Containers | `DeleteContainer` | `DELETE /{container}?restype=container` | Returns `202 Accepted` or `404 ContainerNotFound` |
| Containers | `GetContainerProperties` | `GET /{container}?restype=container` | Returns lease state and metadata headers |
| Containers | `ListContainers` | `GET /?comp=list` | Returns XML `EnumerationResults` listing all containers |
| Blobs | `PutBlob` | `PUT /{container}/{blob}` | Creates or replaces a BlockBlob; supports preconditions (`If-None-Match`, `If-Match`) and computes MD5 |
| Blobs | `PutBlock` | `PUT /{container}/{blob}?comp=block&blockid={id}` | Stages an uncommitted block for chunked blob uploads |
| Blobs | `PutBlockList` | `PUT /{container}/{blob}?comp=blocklist` | Commits staged blocks in sequence to materialize a BlockBlob |
| Blobs | `SetBlobMetadata` | `PUT /{container}/{blob}?comp=metadata` | Updates user-defined `x-ms-meta-*` headers |
| Blobs | `GetBlob` | `GET /{container}/{blob}` | Downloads blob content; supports `Range` and `x-ms-range` requests |
| Blobs | `GetBlobProperties` | `HEAD /{container}/{blob}` | Returns headers (`x-ms-blob-type: BlockBlob`, size, ETag, Content-MD5) |
| Blobs | `DeleteBlob` | `DELETE /{container}/{blob}` | Permanently removes the blob |
| Blobs | `ListBlobs` | `GET /{container}?restype=container&comp=list` | Returns XML `EnumerationResults` listing blobs in the container |

### Queue Storage (`azurequeue`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Queues | `CreateQueue` | `PUT /{account}/{queue}` | Returns `201 Created` or `204 No Content` |
| Queues | `DeleteQueue` | `DELETE /{account}/{queue}` | Returns `204 No Content` or `404 QueueNotFound` |
| Queues | `ListQueues` | `GET /{account}?comp=list` | Returns XML `QueueList` |
| Queues | `GetQueueMetadata` | `GET /{account}/{queue}?comp=metadata` | Returns message count and custom metadata |
| Messages | `PutMessage` | `POST /{account}/{queue}/messages` | Enqueues XML payload; returns message ID and insertion time |
| Messages | `GetMessages` | `GET /{account}/{queue}/messages` | Dequeues messages, sets visibility timeout, and returns pop receipt |
| Messages | `PeekMessages` | `GET /{account}/{queue}/messages?peekonly=true` | Reads front of queue without altering visibility timeout |
| Messages | `DeleteMessage` | `DELETE /{account}/{queue}/messages/{id}?popreceipt={rcpt}` | Deletes message matching pop receipt |
| Messages | `ClearMessages` | `DELETE /{account}/{queue}/messages` | Purges all messages from the queue |

### Table Storage (`azuretable`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Tables | `CreateTable` | `POST /{account}/Tables` | Creates table by `TableName` JSON property |
| Tables | `QueryTables` | `GET /{account}/Tables` | Returns OData JSON collection of existing tables |
| Tables | `DeleteTable` | `DELETE /{account}/Tables('{name}')` | Permanently deletes table and its entities |
| Entities | `InsertEntity` | `POST /{account}/{table}` | Inserts JSON entity with PartitionKey and RowKey |
| Entities | `GetEntity` | `GET /{account}/{table}(PartitionKey='{pk}',RowKey='{rk}')` | Retrieves entity by composite key |
| Entities | `UpdateEntity` | `PUT /{account}/{table}(PartitionKey='{pk}',RowKey='{rk}')` | Overwrites entity properties |
| Entities | `MergeEntity` | `MERGE /{account}/{table}(PartitionKey='{pk}',RowKey='{rk}')` | Merges entity properties |
| Entities | `DeleteEntity` | `DELETE /{account}/{table}(PartitionKey='{pk}',RowKey='{rk}')` | Removes entity from table |
| Entities | `QueryEntities` | `GET /{account}/{table}()` | Queries entities in the table |

### Cosmos DB (`cosmosdb`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Databases | `CreateDatabase` | `POST /dbs` | Creates a new Cosmos database (`id`) |
| Databases | `ListDatabases` | `GET /dbs` | Lists all databases in the account |
| Databases | `GetDatabase` | `GET /dbs/{db}` | Retrieves database details |
| Databases | `DeleteDatabase` | `DELETE /dbs/{db}` | Deletes database and all child collections |
| Containers | `CreateContainer` | `POST /dbs/{db}/colls` | Creates collection with partition key definition |
| Containers | `ListContainers` | `GET /dbs/{db}/colls` | Lists collections in database |
| Containers | `GetContainer` | `GET /dbs/{db}/colls/{coll}` | Retrieves container metadata |
| Containers | `DeleteContainer` | `DELETE /dbs/{db}/colls/{coll}` | Deletes container and all documents |
| Documents | `CreateDocument` | `POST /dbs/{db}/colls/{coll}/docs` | Stores JSON document, assigns `_rid`, `_ts`, `_etag` |
| Documents | `GetDocument` | `GET /dbs/{db}/colls/{coll}/docs/{doc}` | Retrieves document by ID |
| Documents | `ReplaceDocument` | `PUT /dbs/{db}/colls/{coll}/docs/{doc}` | Replaces document body |
| Documents | `DeleteDocument` | `DELETE /dbs/{db}/colls/{coll}/docs/{doc}` | Deletes document |
| Documents | `ListDocuments` | `GET /dbs/{db}/colls/{coll}/docs` | Lists documents in container |

### Key Vault (`azurekeyvault`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Secrets | `SetSecret` | `PUT /secrets/{name}?api-version=7.4` | Creates a new secret version with tags and contentType |
| Secrets | `GetSecret` | `GET /secrets/{name}[/{version}]?api-version=7.4` | Retrieves latest or specified secret version |
| Secrets | `GetSecretVersions` | `GET /secrets/{name}/versions?api-version=7.4` | Lists all versions for a secret |
| Secrets | `ListSecrets` | `GET /secrets?api-version=7.4` | Lists all secrets in the vault |
| Secrets | `DeleteSecret` | `DELETE /secrets/{name}?api-version=7.4` | Deletes secret bundle from the vault |
| Keys | `CreateKey` | `POST /keys/{name}/create?api-version=7.4` | Generates software-backed RSA cryptographic key |
| Keys | `GetKey` | `GET /keys/{name}[/{version}]?api-version=7.4` | Retrieves public key attributes and operations |
| Keys | `ListKeys` | `GET /keys?api-version=7.4` | Lists cryptographic keys in the vault |
| Keys | `DeleteKey` | `DELETE /keys/{name}?api-version=7.4` | Removes key bundle from the vault |

### App Configuration (`azureappconfig`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Key-Values | `SetKeyValue` | `PUT /kv/{key}?label={label}` | Sets or updates a key-value with ETag, content type, and tags |
| Key-Values | `GetKeyValue` | `GET /kv/{key}?label={label}` | Retrieves key-value entity by key and optional label |
| Key-Values | `DeleteKeyValue` | `DELETE /kv/{key}?label={label}` | Removes key-value entity from configuration store |
| Key-Values | `ListKeyValues` | `GET /kv?key={filter}&label={filter}` | Lists key-values with key and label filtering support |

## Configuration

In `devcloud.yaml`, Azure services are configured under `providers.azure`:

```yaml
server:
  port: 4747

providers:
  azure:
    services:
      blob:
        enabled: true
        data_dir: ./data/blob
      azurequeue:
        enabled: true
        data_dir: ./data/azurequeue
      azuretable:
        enabled: true
        data_dir: ./data/azuretable
      cosmosdb:
        enabled: true
        data_dir: ./data/cosmos
      azurekeyvault:
        enabled: true
      azureappconfig:
        enabled: true
```

## Python SDK Examples

### Key Vault (`azure-keyvault-secrets`)

```python
import os
import requests

endpoint = "http://localhost:4747"
headers = {"Host": "myvault.vault.azure.net", "Content-Type": "application/json"}

# Set a secret
resp = requests.put(
    f"{endpoint}/secrets/DatabasePassword?api-version=7.4",
    json={"value": "SuperSecretPass123", "contentType": "text/plain"},
    headers=headers,
)
print("Secret created:", resp.json()["id"])

# Read the secret back
resp = requests.get(
    f"{endpoint}/secrets/DatabasePassword?api-version=7.4",
    headers=headers,
)
print("Secret value:", resp.json()["value"])
```

### App Configuration (`azure-appconfiguration`)

```python
import requests

endpoint = "http://localhost:4747"
headers = {"Host": "myconfig.azconfig.io", "Content-Type": "application/json"}

# Store a configuration setting
requests.put(
    f"{endpoint}/kv/App:FeatureFlags:BetaUI?label=production",
    json={"value": "true", "content_type": "text/plain"},
    headers=headers,
)

# Fetch configuration setting
resp = requests.get(
    f"{endpoint}/kv/App:FeatureFlags:BetaUI?label=production",
    headers=headers,
)
print("Feature flag state:", resp.json()["value"])
```
