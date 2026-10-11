# Azure Services

DevCloud expands its local development runtime to include Microsoft Azure services, providing high-fidelity local simulation for storage, messaging, and database APIs.

## Supported Services

| Service | Supported | Local limits |
|---|---|---|
| Blob Storage (`blob`) | Supported (Phase 3) | Local filesystem-backed FileStore under `data/blob`. Supports container CRUD, BlockBlob CRUD, staged chunk uploads (`PutBlock`, `PutBlockList`), listing/enumeration, and byte-range downloads. |
| Queue Storage (`azurequeue`) | Supported (Phase 4) | Memory + SQLite/file-backed FIFO message store under `data/azurequeue`. Supports queue lifecycle, `PutMessage`, `GetMessages` with visibility timeouts, `PeekMessages`, `DeleteMessage` with pop receipts, and metadata. |
| Table Storage (`azuretable`) | Supported (Phase 4) | SQLite-backed OData JSON table and entity engine under `data/azuretable`. Supports table CRUD, entity insert/replace/merge/delete, PartitionKey/RowKey indexing, and queries. |
| Cosmos DB (`cosmosdb`) | Supported (Phase 4) | SQLite-backed NoSQL document store under `data/cosmos`. Supports database CRUD, container/collection CRUD with partition key definitions, document CRUD, and system attributes (`_rid`, `_etag`, `_ts`). |

## Operations

### Blob Storage (`blob`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Containers | `CreateContainer` | `PUT /{container}?restype=container` | Returns `201 Created` or `409 ContainerAlreadyExists` |
| Containers | `DeleteContainer` | `DELETE /{container}?restype=container` | Returns `202 Accepted` or `404 ContainerNotFound` |
| Containers | `GetContainerProperties` | `GET /{container}?restype=container` | Returns lease state and metadata headers |
| Containers | `ListContainers` | `GET /?comp=list` | Returns XML `EnumerationResults` listing all containers |
| Blobs | `PutBlob` | `PUT /{container}/{blob}` | Creates or replaces a BlockBlob; supports preconditions (`If-None-Match: *`, `If-Match`), stores payload and computes MD5 |
| Blobs | `PutBlock` | `PUT /{container}/{blob}?comp=block&blockid={id}` | Stages an uncommitted block for chunked/streaming blob uploads |
| Blobs | `PutBlockList` | `PUT /{container}/{blob}?comp=blocklist` | Commits staged blocks in sequence to materialize or update a BlockBlob |
| Blobs | `SetBlobMetadata` | `PUT /{container}/{blob}?comp=metadata` | Updates user-defined `x-ms-meta-*` headers on the blob |
| Blobs | `GetBlob` | `GET /{container}/{blob}` | Downloads blob content; supports `Range` and `x-ms-range` requests |
| Blobs | `GetBlobProperties` | `HEAD /{container}/{blob}` | Returns headers (`x-ms-blob-type: BlockBlob`, size, ETag, Content-MD5, `x-ms-meta-*`) |
| Blobs | `DeleteBlob` | `DELETE /{container}/{blob}` | Permanently removes the blob |
| Blobs | `ListBlobs` | `GET /{container}?restype=container&comp=list` | Returns XML `EnumerationResults` listing blobs in the container |

### Queue Storage (`azurequeue`)

| Area | Operation | Method & URI | Notes |
|---|---|---|---|
| Queues | `CreateQueue` | `PUT /{account}/{queue}` | Returns `201 Created` or `204 No Content` |
| Queues | `DeleteQueue` | `DELETE /{account}/{queue}` | Returns `204 No Content` or `404 QueueNotFound` |
| Queues | `ListQueues` | `GET /{account}?comp=list` | Returns XML `QueueList` |
| Queues | `GetQueueMetadata` | `GET /{account}/{queue}?comp=metadata` | Returns `x-ms-approximate-messages-count` and custom metadata |
| Messages | `PutMessage` | `POST /{account}/{queue}/messages` | Enqueues XML payload; returns message ID, insertion time, expiry |
| Messages | `GetMessages` | `GET /{account}/{queue}/messages` | Dequeues messages, calculates visibility timeout, generates pop receipt |
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
      azurequeue:
        enabled: true
        data_dir: ./data/azurequeue
      azuretable:
        enabled: true
        data_dir: ./data/azuretable
      cosmosdb:
        enabled: true
        data_dir: ./data/cosmos
```

## Credentials & Connection Strings

DevCloud accepts Azure Shared Key, SharedKeyLite, and Azurite emulator credentials:

```bash
# Unified Storage Connection String (Blob, Queue, Table)
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:4747/devstoreaccount1;QueueEndpoint=http://127.0.0.1:4747/devstoreaccount1;TableEndpoint=http://127.0.0.1:4747/devstoreaccount1;"

# Cosmos DB Endpoint & Key
export AZURE_COSMOS_ENDPOINT="http://127.0.0.1:4747"
export AZURE_COSMOS_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
```

## Python SDK Quickstarts

### Queue Storage

```python
import requests

endpoint = "http://127.0.0.1:4747/devstoreaccount1"
headers = {"x-ms-version": "2023-11-03"}

# 1. Create Queue
requests.put(f"{endpoint}/tasks", headers=headers)

# 2. Enqueue message
msg_xml = "<QueueMessage><MessageText>Process job #42</MessageText></QueueMessage>"
requests.post(f"{endpoint}/tasks/messages", headers=headers, data=msg_xml)

# 3. Dequeue message
resp = requests.get(f"{endpoint}/tasks/messages", headers=headers)
print(resp.text)
```

### Table Storage

```python
import requests

endpoint = "http://127.0.0.1:4747/devstoreaccount1"
headers = {
    "Accept": "application/json;odata=nometadata",
    "Content-Type": "application/json",
}

# 1. Create table
requests.post(f"{endpoint}/Tables", headers=headers, json={"TableName": "Customers"})

# 2. Insert entity
entity = {"PartitionKey": "Seattle", "RowKey": "cust-01", "Name": "Alice"}
requests.post(f"{endpoint}/Customers", headers=headers, json=entity)

# 3. Query entity
resp = requests.get(f"{endpoint}/Customers(PartitionKey='Seattle',RowKey='cust-01')", headers=headers)
print(resp.json())
```

### Cosmos DB

```python
import requests

endpoint = "http://127.0.0.1:4747/devstoreaccount1"
headers = {"Content-Type": "application/json", "x-ms-version": "2018-12-31"}

# 1. Create Database
requests.post(f"{endpoint}/dbs", headers=headers, json={"id": "analytics"})

# 2. Create Collection
requests.post(f"{endpoint}/dbs/analytics/colls", headers=headers, json={"id": "events", "partitionKey": {"paths": ["/tenantId"]}})

# 3. Create Document
doc = {"id": "evt-100", "tenantId": "t-1", "action": "login", "status": "ok"}
requests.post(f"{endpoint}/dbs/analytics/colls/events/docs", headers=headers, json=doc)

# 4. Read Document
resp = requests.get(f"{endpoint}/dbs/analytics/colls/events/docs/evt-100", headers=headers)
print(resp.json())
```
