# DynamoDB

## Overview

DevCloud DynamoDB uses BadgerDB as its embedded key-value storage backend. Table metadata is kept in an in-memory index (protected by RWMutex), while items are persisted in BadgerDB with composite keys (`_item/{table}#{partitionKey}#{sortKey}`).

Supported attribute types: S (String), N (Number), B (Binary), BOOL, NULL, L (List), M (Map).

## Supported APIs

| Operation | Description |
|-----------|-------------|
| CreateTable | Create table with partition key (HASH) and optional sort key (RANGE) |
| DeleteTable | Delete table and all its items |
| ListTables | List all table names |
| PutItem | Insert or overwrite an item |
| GetItem | Retrieve an item by primary key |
| DeleteItem | Delete an item by primary key |
| Query | Query items by partition key |
| Scan | Full table scan |

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

- No UpdateItem (use PutItem to overwrite entire item)
- No batch operations (BatchGetItem, BatchWriteItem)
- No transactions (TransactGetItems, TransactWriteItems)
- No secondary indexes (GSI/LSI)
- No DynamoDB Streams
- No advanced filter expressions on Query/Scan
- No projection expressions
- No conditional writes (ConditionExpression)
- No TTL
