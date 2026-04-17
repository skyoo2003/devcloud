// SPDX-License-Identifier: Apache-2.0

// internal/services/dynamodb/streams_integration.go
//
// This file wires DynamoDB writes to the DynamoDB Streams service. The
// integration uses a package-level global exposed by dynamodbstreams — the
// streams package never imports dynamodb, so this dependency direction avoids
// an import cycle.
//
// When no stream is active for a table, publish* functions return silently.
// Any stream errors are swallowed: streams are an auxiliary projection and
// must never break a successful write.

package dynamodb

import (
	"encoding/json"

	"github.com/skyoo2003/devcloud/internal/services/dynamodbstreams"
)

// itemToGeneric marshals a DynamoDB Item into a plain map[string]any suitable
// for embedding in a stream record payload. The stream consumer expects JSON
// with the usual DynamoDB AttributeValue shape, so the round-trip via JSON is
// the safest way to produce the canonical encoding.
func itemToGeneric(item Item) map[string]any {
	if item == nil {
		return nil
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return nil
	}
	out := make(map[string]any)
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// extractKeys returns a map containing only the partition (and optional sort)
// key attributes of an item, matching the DynamoDB stream "Keys" field.
func extractKeys(info *TableInfo, item Item) map[string]any {
	if info == nil || item == nil {
		return nil
	}
	keys := make(Item)
	if av, ok := item[info.PartitionKey.Name]; ok {
		keys[info.PartitionKey.Name] = av
	}
	if info.SortKey != nil {
		if av, ok := item[info.SortKey.Name]; ok {
			keys[info.SortKey.Name] = av
		}
	}
	return itemToGeneric(keys)
}

// publishPut emits INSERT (new row) or MODIFY (existing row replaced).
func (p *DynamoDBProvider) publishPut(tableName string, newItem Item, oldItem *Item) {
	store := dynamodbstreams.GetGlobalStore()
	if store == nil {
		return
	}
	if _, ok := store.GetLatestStreamForTable(tableName); !ok {
		return
	}
	info, err := p.store.GetTable(tableName)
	if err != nil {
		return
	}
	keys := extractKeys(info, newItem)
	newImg := itemToGeneric(newItem)
	var oldImg map[string]any
	eventName := "INSERT"
	if oldItem != nil {
		oldImg = itemToGeneric(*oldItem)
		eventName = "MODIFY"
	}
	_ = store.PublishRecord(tableName, eventName, keys, newImg, oldImg)
}

// publishDelete emits a REMOVE event for a deleted item.
func (p *DynamoDBProvider) publishDelete(tableName string, oldItem Item) {
	store := dynamodbstreams.GetGlobalStore()
	if store == nil {
		return
	}
	if _, ok := store.GetLatestStreamForTable(tableName); !ok {
		return
	}
	info, err := p.store.GetTable(tableName)
	if err != nil {
		return
	}
	keys := extractKeys(info, oldItem)
	oldImg := itemToGeneric(oldItem)
	_ = store.PublishRecord(tableName, "REMOVE", keys, nil, oldImg)
}
