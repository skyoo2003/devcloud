# DynamoDB

## Overview

DevCloud DynamoDB persists tables and items in SQLite (`dynamodb.db` under the
service's `data_dir`), with table metadata also held in an in-memory index
guarded by an RWMutex.

All ten AttributeValue types are supported: S, N, B, BOOL, NULL, L (List),
M (Map), and the SS / NS / BS sets.

## Supported APIs

These 20 operations are `hand-verified` — implemented by the provider, not by
the [CRUD engine](../crud-engine.md). Everything else DynamoDB models is served
at a lower tier or not at all; [fidelity-manifest.md](../fidelity-manifest.md)
is the per-operation answer.

| Operation | Description |
|-----------|-------------|
| CreateTable | Create table with partition key (HASH), optional sort key (RANGE), GSIs, LSIs and a StreamSpecification |
| UpdateTable / DescribeTable / DeleteTable / ListTables | Manage and inspect tables |
| PutItem | Insert or overwrite an item; honours `ConditionExpression` |
| GetItem | Retrieve an item by primary key; honours `ProjectionExpression` |
| UpdateItem | Apply an `UpdateExpression` to one item |
| DeleteItem | Delete an item by primary key; honours `ConditionExpression` |
| Query | Query by partition key, against the table or a named `IndexName`; honours `FilterExpression` |
| Scan | Full table scan; honours `FilterExpression` |
| BatchGetItem / BatchWriteItem | Multi-item reads and writes |
| TransactGetItems / TransactWriteItems | Transactional reads and writes |
| UpdateTimeToLive / DescribeTimeToLive | Store and read back a table's TTL configuration |
| TagResource / UntagResource / ListTagsOfResource | Manage table tags |

Writes to a table created with `StreamEnabled` are published to the
`dynamodbstreams` service, which is what makes DynamoDB Streams → Lambda event
source mappings fire.

## boto3 Examples

### Create a table and write items

```python
import boto3

dynamodb = boto3.client(
    "dynamodb",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Create table
dynamodb.create_table(
    TableName="users",
    KeySchema=[
        {"AttributeName": "pk", "KeyType": "HASH"},
        {"AttributeName": "sk", "KeyType": "RANGE"},
    ],
    AttributeDefinitions=[
        {"AttributeName": "pk", "AttributeType": "S"},
        {"AttributeName": "sk", "AttributeType": "S"},
    ],
    BillingMode="PAY_PER_REQUEST",
)

# Put item
dynamodb.put_item(
    TableName="users",
    Item={
        "pk": {"S": "user#123"},
        "sk": {"S": "profile"},
        "name": {"S": "Alice"},
        "age": {"N": "30"},
    },
)

# Get item
response = dynamodb.get_item(
    TableName="users",
    Key={"pk": {"S": "user#123"}, "sk": {"S": "profile"}},
)
print(response["Item"])
```

### Query and scan

```python
# Query by partition key
response = dynamodb.query(
    TableName="users",
    KeyConditionExpression="pk = :pk",
    ExpressionAttributeValues={":pk": {"S": "user#123"}},
)
for item in response["Items"]:
    print(item)

# Full table scan
response = dynamodb.scan(TableName="users")
for item in response["Items"]:
    print(item)
```

## AWS CLI Examples

```bash
# Create table
aws --endpoint-url http://localhost:4747 dynamodb create-table \
  --table-name my-table \
  --key-schema AttributeName=pk,KeyType=HASH \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --billing-mode PAY_PER_REQUEST

# Put item
aws --endpoint-url http://localhost:4747 dynamodb put-item \
  --table-name my-table \
  --item '{"pk": {"S": "key1"}, "data": {"S": "value1"}}'

# Get item
aws --endpoint-url http://localhost:4747 dynamodb get-item \
  --table-name my-table \
  --key '{"pk": {"S": "key1"}}'
```

## Known Limitations

- **TTL is configuration only.** `UpdateTimeToLive` stores the attribute name
  and echoes it back; nothing sweeps expired items, so a row past its TTL is
  still returned.
- **No pagination or capacity reporting.** Responses carry no
  `LastEvaluatedKey` and no `ConsumedCapacity`, so `Query`/`Scan` return the
  whole matching set in one page and code that loops on the cursor sees one
  iteration.
- **No PartiQL** — `ExecuteStatement`, `ExecuteTransaction` and
  `BatchExecuteStatement` are unimplemented and fail rather than answering.
- **Backups, global tables and exports answer from the CRUD engine.**
  `CreateBackup`, `CreateGlobalTable`, `DescribeContinuousBackups` and their
  neighbours return stored, plausible shapes with no behaviour behind them —
  see [crud-engine.md](../crud-engine.md).
- No Kinesis streaming destination (`EnableKinesisStreamingDestination` is
  unimplemented)
- No provisioned-throughput accounting or throttling; `BillingMode` is recorded,
  never enforced
- Single account model (account ID: `000000000000`)
