import pytest
from botocore.exceptions import ClientError


def test_create_table(dynamodb_client):
    response = dynamodb_client.create_table(
        TableName="test-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    assert response["TableDescription"]["TableName"] == "test-table"
    assert response["TableDescription"]["TableStatus"] == "ACTIVE"


def test_put_and_get_item(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-items",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="test-items",
        Item={"pk": {"S": "key1"}, "data": {"S": "hello"}},
    )
    response = dynamodb_client.get_item(
        TableName="test-items",
        Key={"pk": {"S": "key1"}},
    )
    assert response["Item"]["pk"]["S"] == "key1"
    assert response["Item"]["data"]["S"] == "hello"


def test_delete_item(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-del-items",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="test-del-items",
        Item={"pk": {"S": "key1"}, "data": {"S": "bye"}},
    )
    dynamodb_client.delete_item(
        TableName="test-del-items",
        Key={"pk": {"S": "key1"}},
    )
    response = dynamodb_client.get_item(
        TableName="test-del-items",
        Key={"pk": {"S": "key1"}},
    )
    assert "Item" not in response


def test_list_tables(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-list-t1",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    response = dynamodb_client.list_tables()
    assert "test-list-t1" in response["TableNames"]


def test_scan(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-scan",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(TableName="test-scan", Item={"pk": {"S": "a"}})
    dynamodb_client.put_item(TableName="test-scan", Item={"pk": {"S": "b"}})
    response = dynamodb_client.scan(TableName="test-scan")
    assert response["Count"] >= 2


def test_describe_nonexistent_table(dynamodb_client):
    with pytest.raises(ClientError) as exc:
        dynamodb_client.describe_table(TableName="no-such-table")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_update_item(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-update",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="test-update", Item={"pk": {"S": "u1"}, "count": {"N": "1"}}
    )
    dynamodb_client.update_item(
        TableName="test-update",
        Key={"pk": {"S": "u1"}},
        UpdateExpression="SET #c = #c + :inc",
        ExpressionAttributeNames={"#c": "count"},
        ExpressionAttributeValues={":inc": {"N": "5"}},
    )
    resp = dynamodb_client.get_item(TableName="test-update", Key={"pk": {"S": "u1"}})
    assert resp["Item"]["count"]["N"] == "6"


def test_query(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-query",
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
    dynamodb_client.put_item(
        TableName="test-query", Item={"pk": {"S": "user1"}, "sk": {"S": "a"}}
    )
    dynamodb_client.put_item(
        TableName="test-query", Item={"pk": {"S": "user1"}, "sk": {"S": "b"}}
    )
    dynamodb_client.put_item(
        TableName="test-query", Item={"pk": {"S": "user2"}, "sk": {"S": "c"}}
    )
    resp = dynamodb_client.query(
        TableName="test-query",
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "user1"}},
    )
    assert resp["Count"] == 2


def test_batch_write_item(dynamodb_client):
    dynamodb_client.create_table(
        TableName="test-batch",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.batch_write_item(
        RequestItems={
            "test-batch": [
                {"PutRequest": {"Item": {"pk": {"S": "b1"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "b2"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "b3"}}}},
            ]
        }
    )
    resp = dynamodb_client.scan(TableName="test-batch")
    assert resp["Count"] >= 3


def test_create_table_invalid_schema(dynamodb_client):
    with pytest.raises(ClientError) as exc:
        dynamodb_client.create_table(
            TableName="bad-schema",
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    assert exc.value.response["Error"]["Code"] == "ValidationException"


def test_filter_expression(dynamodb_client):
    dynamodb_client.create_table(
        TableName="filter-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="filter-table", Item={"pk": {"S": "1"}, "age": {"N": "25"}}
    )
    dynamodb_client.put_item(
        TableName="filter-table", Item={"pk": {"S": "2"}, "age": {"N": "35"}}
    )
    resp = dynamodb_client.scan(
        TableName="filter-table",
        FilterExpression="age > :min",
        ExpressionAttributeValues={":min": {"N": "30"}},
    )
    assert resp["Count"] == 1
    assert resp["Items"][0]["pk"]["S"] == "2"


def test_projection_expression(dynamodb_client):
    dynamodb_client.create_table(
        TableName="proj-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="proj-table",
        Item={"pk": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}},
    )
    resp = dynamodb_client.get_item(
        TableName="proj-table",
        Key={"pk": {"S": "1"}},
        ProjectionExpression="pk, #n",
        ExpressionAttributeNames={"#n": "name"},
    )
    assert "name" in resp["Item"]
    assert "age" not in resp["Item"]


def test_condition_expression(dynamodb_client):
    dynamodb_client.create_table(
        TableName="cond-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="cond-table", Item={"pk": {"S": "1"}, "v": {"N": "10"}}
    )
    with pytest.raises(ClientError) as exc:
        dynamodb_client.put_item(
            TableName="cond-table",
            Item={"pk": {"S": "1"}, "v": {"N": "20"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert "ConditionalCheckFailed" in exc.value.response["Error"]["Code"]


def test_update_item_remove(dynamodb_client):
    dynamodb_client.create_table(
        TableName="remove-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="remove-table",
        Item={"pk": {"S": "1"}, "extra": {"S": "bye"}},
    )
    dynamodb_client.update_item(
        TableName="remove-table",
        Key={"pk": {"S": "1"}},
        UpdateExpression="REMOVE extra",
    )
    resp = dynamodb_client.get_item(TableName="remove-table", Key={"pk": {"S": "1"}})
    assert "extra" not in resp["Item"]


def test_batch_get_item(dynamodb_client):
    dynamodb_client.create_table(
        TableName="bget-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.put_item(
        TableName="bget-table", Item={"pk": {"S": "a"}, "v": {"S": "1"}}
    )
    dynamodb_client.put_item(
        TableName="bget-table", Item={"pk": {"S": "b"}, "v": {"S": "2"}}
    )
    resp = dynamodb_client.batch_get_item(
        RequestItems={"bget-table": {"Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "b"}}]}},
    )
    assert len(resp["Responses"]["bget-table"]) == 2


def test_transact_write_items(dynamodb_client):
    dynamodb_client.create_table(
        TableName="tx-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb_client.transact_write_items(
        TransactItems=[
            {
                "Put": {
                    "TableName": "tx-table",
                    "Item": {"pk": {"S": "t1"}, "v": {"S": "a"}},
                }
            },
            {
                "Put": {
                    "TableName": "tx-table",
                    "Item": {"pk": {"S": "t2"}, "v": {"S": "b"}},
                }
            },
        ],
    )
    r1 = dynamodb_client.get_item(TableName="tx-table", Key={"pk": {"S": "t1"}})
    r2 = dynamodb_client.get_item(TableName="tx-table", Key={"pk": {"S": "t2"}})
    assert r1["Item"]["v"]["S"] == "a"
    assert r2["Item"]["v"]["S"] == "b"


def test_tag_resource(dynamodb_client):
    dynamodb_client.create_table(
        TableName="tag-ddb",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    desc = dynamodb_client.describe_table(TableName="tag-ddb")
    arn = desc["Table"]["TableArn"]
    dynamodb_client.tag_resource(
        ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}]
    )
    resp = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
    assert any(t["Key"] == "env" for t in resp["Tags"])
    dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["env"])
    resp2 = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
    assert not any(t["Key"] == "env" for t in resp2["Tags"])
