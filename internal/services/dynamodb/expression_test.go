// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateFilterExpression_Equal(t *testing.T) {
	item := Item{
		"name": {S: strPtr("Alice")},
		"age":  {N: strPtr("30")},
	}
	assert.True(t, EvaluateFilterExpression("name = :n", nil, map[string]*AttributeValue{":n": {S: strPtr("Alice")}}, item))
	assert.False(t, EvaluateFilterExpression("name = :n", nil, map[string]*AttributeValue{":n": {S: strPtr("Bob")}}, item))
}

func TestEvaluateFilterExpression_Comparison(t *testing.T) {
	item := Item{
		"age": {N: strPtr("25")},
	}
	assert.True(t, EvaluateFilterExpression("age > :min", nil, map[string]*AttributeValue{":min": {N: strPtr("20")}}, item))
	assert.False(t, EvaluateFilterExpression("age > :min", nil, map[string]*AttributeValue{":min": {N: strPtr("30")}}, item))
	assert.True(t, EvaluateFilterExpression("age >= :min", nil, map[string]*AttributeValue{":min": {N: strPtr("25")}}, item))
	assert.True(t, EvaluateFilterExpression("age < :max", nil, map[string]*AttributeValue{":max": {N: strPtr("30")}}, item))
	assert.True(t, EvaluateFilterExpression("age <= :max", nil, map[string]*AttributeValue{":max": {N: strPtr("25")}}, item))
	assert.True(t, EvaluateFilterExpression("age <> :other", nil, map[string]*AttributeValue{":other": {N: strPtr("99")}}, item))
}

func TestEvaluateFilterExpression_AndOr(t *testing.T) {
	item := Item{
		"age":    {N: strPtr("25")},
		"status": {S: strPtr("active")},
	}
	assert.True(t, EvaluateFilterExpression(
		"age > :min AND status = :s",
		nil,
		map[string]*AttributeValue{":min": {N: strPtr("20")}, ":s": {S: strPtr("active")}},
		item,
	))
	assert.False(t, EvaluateFilterExpression(
		"age > :min AND status = :s",
		nil,
		map[string]*AttributeValue{":min": {N: strPtr("20")}, ":s": {S: strPtr("inactive")}},
		item,
	))
	assert.True(t, EvaluateFilterExpression(
		"age > :min OR status = :s",
		nil,
		map[string]*AttributeValue{":min": {N: strPtr("20")}, ":s": {S: strPtr("inactive")}},
		item,
	))
}

func TestEvaluateFilterExpression_Not(t *testing.T) {
	item := Item{"age": {N: strPtr("25")}}
	assert.True(t, EvaluateFilterExpression("NOT age = :v", nil, map[string]*AttributeValue{":v": {N: strPtr("30")}}, item))
	assert.False(t, EvaluateFilterExpression("NOT age = :v", nil, map[string]*AttributeValue{":v": {N: strPtr("25")}}, item))
}

func TestEvaluateFilterExpression_Between(t *testing.T) {
	item := Item{"age": {N: strPtr("25")}}
	assert.True(t, EvaluateFilterExpression("age BETWEEN :lo AND :hi", nil, map[string]*AttributeValue{":lo": {N: strPtr("20")}, ":hi": {N: strPtr("30")}}, item))
	assert.False(t, EvaluateFilterExpression("age BETWEEN :lo AND :hi", nil, map[string]*AttributeValue{":lo": {N: strPtr("30")}, ":hi": {N: strPtr("40")}}, item))
}

func TestEvaluateFilterExpression_In(t *testing.T) {
	item := Item{"status": {S: strPtr("active")}}
	assert.True(t, EvaluateFilterExpression(
		"status IN (:a, :b)",
		nil,
		map[string]*AttributeValue{":a": {S: strPtr("active")}, ":b": {S: strPtr("inactive")}},
		item,
	))
	assert.False(t, EvaluateFilterExpression(
		"status IN (:a, :b)",
		nil,
		map[string]*AttributeValue{":a": {S: strPtr("pending")}, ":b": {S: strPtr("deleted")}},
		item,
	))
}

func TestEvaluateFilterExpression_Functions(t *testing.T) {
	item := Item{
		"name":   {S: strPtr("Alice")},
		"city":   {S: strPtr("New York")},
		"exists": {S: strPtr("yes")},
	}

	assert.True(t, EvaluateFilterExpression("begins_with(name, :pfx)", nil, map[string]*AttributeValue{":pfx": {S: strPtr("Ali")}}, item))
	assert.False(t, EvaluateFilterExpression("begins_with(name, :pfx)", nil, map[string]*AttributeValue{":pfx": {S: strPtr("Bob")}}, item))

	assert.True(t, EvaluateFilterExpression("contains(city, :sub)", nil, map[string]*AttributeValue{":sub": {S: strPtr("York")}}, item))
	assert.False(t, EvaluateFilterExpression("contains(city, :sub)", nil, map[string]*AttributeValue{":sub": {S: strPtr("London")}}, item))

	assert.True(t, EvaluateFilterExpression("attribute_exists(exists)", nil, nil, item))
	assert.False(t, EvaluateFilterExpression("attribute_exists(missing)", nil, nil, item))

	assert.True(t, EvaluateFilterExpression("attribute_not_exists(missing)", nil, nil, item))
	assert.False(t, EvaluateFilterExpression("attribute_not_exists(exists)", nil, nil, item))
}

func TestEvaluateFilterExpression_ExpressionAttributeNames(t *testing.T) {
	item := Item{"name": {S: strPtr("Alice")}}
	assert.True(t, EvaluateFilterExpression(
		"#n = :v",
		map[string]string{"#n": "name"},
		map[string]*AttributeValue{":v": {S: strPtr("Alice")}},
		item,
	))
}

func TestEvaluateConditionExpression(t *testing.T) {
	item := Item{"pk": {S: strPtr("1")}}

	// attribute_not_exists fails when attribute exists.
	err := EvaluateConditionExpression("attribute_not_exists(pk)", nil, nil, item)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ConditionalCheckFailed")

	// attribute_exists passes when attribute exists.
	err = EvaluateConditionExpression("attribute_exists(pk)", nil, nil, item)
	assert.NoError(t, err)

	// attribute_not_exists passes on empty item.
	err = EvaluateConditionExpression("attribute_not_exists(pk)", nil, nil, Item{})
	assert.NoError(t, err)
}

func TestApplyProjectionExpression(t *testing.T) {
	item := Item{
		"pk":   {S: strPtr("1")},
		"name": {S: strPtr("Alice")},
		"age":  {N: strPtr("30")},
	}

	projected := ApplyProjectionExpression("pk, name", nil, item)
	assert.Contains(t, projected, "pk")
	assert.Contains(t, projected, "name")
	assert.NotContains(t, projected, "age")

	// With expression attribute name.
	projected2 := ApplyProjectionExpression("pk, #n", map[string]string{"#n": "name"}, item)
	assert.Contains(t, projected2, "pk")
	assert.Contains(t, projected2, "name")
	assert.NotContains(t, projected2, "age")
}

func TestApplyUpdateExpression_Remove(t *testing.T) {
	item := Item{
		"pk":    {S: strPtr("1")},
		"extra": {S: strPtr("bye")},
	}
	err := applyUpdateExpression(item, "REMOVE extra", nil, nil)
	require.NoError(t, err)
	assert.NotContains(t, item, "extra")
	assert.Contains(t, item, "pk")
}

func TestApplyUpdateExpression_Add(t *testing.T) {
	item := Item{
		"pk":    {S: strPtr("1")},
		"count": {N: strPtr("5")},
	}
	err := applyUpdateExpression(item, "ADD count :inc", nil, map[string]*AttributeValue{":inc": {N: strPtr("3")}})
	require.NoError(t, err)
	assert.Equal(t, "8", *item["count"].N)
}

func TestApplyUpdateExpression_SetAndRemove(t *testing.T) {
	item := Item{
		"pk":   {S: strPtr("1")},
		"old":  {S: strPtr("x")},
		"keep": {S: strPtr("y")},
	}
	err := applyUpdateExpression(item, "SET new = :v REMOVE old", nil, map[string]*AttributeValue{":v": {S: strPtr("hello")}})
	require.NoError(t, err)
	assert.Contains(t, item, "new")
	assert.Equal(t, "hello", *item["new"].S)
	assert.NotContains(t, item, "old")
	assert.Contains(t, item, "keep")
}

func TestDynamoDBProvider_FilterExpression(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "filter-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{"TableName": "filter-test", "Item": {"pk": {"S": "1"}, "age": {"N": "25"}}}`)
	doRequest(t, p, "PutItem", `{"TableName": "filter-test", "Item": {"pk": {"S": "2"}, "age": {"N": "35"}}}`)

	resp := doRequest(t, p, "Scan", `{
		"TableName": "filter-test",
		"FilterExpression": "age > :min",
		"ExpressionAttributeValues": {":min": {"N": "30"}}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	assert.Equal(t, float64(1), result["Count"])
}

func TestDynamoDBProvider_ConditionExpression(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "cond-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{"TableName": "cond-test", "Item": {"pk": {"S": "1"}, "v": {"N": "10"}}}`)

	// This should fail: item already exists, attribute_not_exists(pk) is false.
	resp := doRequest(t, p, "PutItem", `{
		"TableName": "cond-test",
		"Item": {"pk": {"S": "1"}, "v": {"N": "20"}},
		"ConditionExpression": "attribute_not_exists(pk)"
	}`)
	assert.Equal(t, 400, resp.StatusCode)

	var errResult map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &errResult))
	assert.Contains(t, errResult["__type"], "ConditionalCheckFailed")
}

func TestDynamoDBProvider_UpdateItemRemove(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "remove-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{"TableName": "remove-test", "Item": {"pk": {"S": "1"}, "extra": {"S": "bye"}}}`)

	doRequest(t, p, "UpdateItem", `{
		"TableName": "remove-test",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "REMOVE extra"
	}`)

	getResp := doRequest(t, p, "GetItem", `{"TableName": "remove-test", "Key": {"pk": {"S": "1"}}}`)
	var result map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body, &result))
	item := result["Item"].(map[string]any)
	assert.NotContains(t, item, "extra")
	assert.Contains(t, item, "pk")
}

func TestDynamoDBProvider_BatchGetItem(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "bget-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{"TableName": "bget-test", "Item": {"pk": {"S": "a"}, "v": {"S": "1"}}}`)
	doRequest(t, p, "PutItem", `{"TableName": "bget-test", "Item": {"pk": {"S": "b"}, "v": {"S": "2"}}}`)

	resp := doRequest(t, p, "BatchGetItem", `{
		"RequestItems": {
			"bget-test": {"Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "b"}}]}
		}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	responses := result["Responses"].(map[string]any)
	items := responses["bget-test"].([]any)
	assert.Len(t, items, 2)
}

func TestDynamoDBProvider_TransactWriteItems(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "tx-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	resp := doRequest(t, p, "TransactWriteItems", `{
		"TransactItems": [
			{"Put": {"TableName": "tx-test", "Item": {"pk": {"S": "t1"}, "v": {"S": "a"}}}},
			{"Put": {"TableName": "tx-test", "Item": {"pk": {"S": "t2"}, "v": {"S": "b"}}}}
		]
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	r1 := doRequest(t, p, "GetItem", `{"TableName": "tx-test", "Key": {"pk": {"S": "t1"}}}`)
	var res1 map[string]any
	require.NoError(t, json.Unmarshal(r1.Body, &res1))
	assert.Equal(t, "a", res1["Item"].(map[string]any)["v"].(map[string]any)["S"])
}

func TestDynamoDBProvider_TTL(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "ttl-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	updateResp := doRequest(t, p, "UpdateTimeToLive", `{
		"TableName": "ttl-test",
		"TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
	}`)
	assert.Equal(t, 200, updateResp.StatusCode)

	descResp := doRequest(t, p, "DescribeTimeToLive", `{"TableName": "ttl-test"}`)
	assert.Equal(t, 200, descResp.StatusCode)
	var result map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body, &result))
	desc := result["TimeToLiveDescription"].(map[string]any)
	assert.Equal(t, "ENABLED", desc["TimeToLiveStatus"])
	assert.Equal(t, "ttl", desc["AttributeName"])
}

func TestDynamoDBProvider_Tags(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "tag-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	info, err := p.store.GetTable("tag-test")
	require.NoError(t, err)
	arn := info.TableArn

	tagBody := `{"ResourceArn": "` + arn + `", "Tags": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "backend"}]}`
	tagResp := doRequest(t, p, "TagResource", tagBody)
	assert.Equal(t, 200, tagResp.StatusCode)

	listBody := `{"ResourceArn": "` + arn + `"}`
	listResp := doRequest(t, p, "ListTagsOfResource", listBody)
	assert.Equal(t, 200, listResp.StatusCode)
	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body, &listResult))
	tags := listResult["Tags"].([]any)
	assert.Len(t, tags, 2)

	untagBody := `{"ResourceArn": "` + arn + `", "TagKeys": ["env"]}`
	untagResp := doRequest(t, p, "UntagResource", untagBody)
	assert.Equal(t, 200, untagResp.StatusCode)

	listResp2 := doRequest(t, p, "ListTagsOfResource", listBody)
	var listResult2 map[string]any
	require.NoError(t, json.Unmarshal(listResp2.Body, &listResult2))
	tags2 := listResult2["Tags"].([]any)
	assert.Len(t, tags2, 1)
}

func TestDynamoDBProvider_GSIQuery(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	// Create table with GSI.
	doRequest(t, p, "CreateTable", `{
		"TableName": "gsi-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "category", "AttributeType": "S"}
		],
		"GlobalSecondaryIndexes": [{
			"IndexName": "category-index",
			"KeySchema": [{"AttributeName": "category", "KeyType": "HASH"}],
			"Projection": {"ProjectionType": "ALL"}
		}],
		"BillingMode": "PAY_PER_REQUEST"
	}`)

	// Put items with category attribute.
	doRequest(t, p, "PutItem", `{"TableName": "gsi-test", "Item": {"pk": {"S": "1"}, "category": {"S": "books"}, "title": {"S": "Go Programming"}}}`)
	doRequest(t, p, "PutItem", `{"TableName": "gsi-test", "Item": {"pk": {"S": "2"}, "category": {"S": "books"}, "title": {"S": "Rust in Action"}}}`)
	doRequest(t, p, "PutItem", `{"TableName": "gsi-test", "Item": {"pk": {"S": "3"}, "category": {"S": "movies"}, "title": {"S": "Inception"}}}`)

	// Query by GSI.
	resp := doRequest(t, p, "Query", `{
		"TableName": "gsi-test",
		"IndexName": "category-index",
		"KeyConditionExpression": "category = :cat",
		"ExpressionAttributeValues": {":cat": {"S": "books"}}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	assert.Equal(t, float64(2), result["Count"])
}

func TestDynamoDBProvider_ProjectionExpression(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "proj-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)
	doRequest(t, p, "PutItem", `{"TableName": "proj-test", "Item": {"pk": {"S": "1"}, "name": {"S": "Alice"}, "age": {"N": "30"}}}`)

	resp := doRequest(t, p, "GetItem", `{
		"TableName": "proj-test",
		"Key": {"pk": {"S": "1"}},
		"ProjectionExpression": "pk, #n",
		"ExpressionAttributeNames": {"#n": "name"}
	}`)
	assert.Equal(t, 200, resp.StatusCode)

	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &result))
	item := result["Item"].(map[string]any)
	assert.Contains(t, item, "pk")
	assert.Contains(t, item, "name")
	assert.NotContains(t, item, "age")
}

func TestDynamoDBProvider_UpdateTable(t *testing.T) {
	p := newTestDynamoDBProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()

	doRequest(t, p, "CreateTable", `{
		"TableName": "update-table-test",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
	}`)

	resp := doRequest(t, p, "UpdateTable", `{
		"TableName": "update-table-test",
		"BillingMode": "PROVISIONED"
	}`)
	assert.Equal(t, 200, resp.StatusCode)
}
