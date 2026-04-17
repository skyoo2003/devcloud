// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/services/dynamodbstreams"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// DynamoDBProvider implements plugin.ServicePlugin for DynamoDB using the JSON 1.0 protocol.
type DynamoDBProvider struct {
	store *DynamoStore
}

// ServiceID returns the unique identifier for this plugin.
func (p *DynamoDBProvider) ServiceID() string { return "dynamodb" }

// ServiceName returns the human-readable name for this plugin.
func (p *DynamoDBProvider) ServiceName() string { return "Amazon DynamoDB" }

// Protocol returns the wire protocol used by this plugin.
func (p *DynamoDBProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON10 }

// Init creates a DynamoStore rooted at cfg.DataDir.
func (p *DynamoDBProvider) Init(cfg plugin.PluginConfig) error {
	store, err := NewDynamoStore(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("init dynamodb store: %w", err)
	}
	p.store = store
	return nil
}

// Shutdown closes the underlying store.
func (p *DynamoDBProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// HandleRequest routes the request to the appropriate DynamoDB operation handler.
// op is the operation name extracted from the X-Amz-Target header by the gateway.
func (p *DynamoDBProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return jsonError("SerializationException", "failed to read request body", http.StatusBadRequest), nil
	}

	switch op {
	case "CreateTable":
		return p.handleCreateTable(body)
	case "DeleteTable":
		return p.handleDeleteTable(body)
	case "DescribeTable":
		return p.handleDescribeTable(body)
	case "ListTables":
		return p.handleListTables(body)
	case "PutItem":
		return p.handlePutItem(body)
	case "GetItem":
		return p.handleGetItem(body)
	case "DeleteItem":
		return p.handleDeleteItem(body)
	case "UpdateItem":
		return p.handleUpdateItem(body)
	case "Query":
		return p.handleQuery(body)
	case "Scan":
		return p.handleScan(body)
	case "BatchWriteItem":
		return p.handleBatchWriteItem(body)
	case "BatchGetItem":
		return p.handleBatchGetItem(body)
	case "TransactWriteItems":
		return p.handleTransactWriteItems(body)
	case "TransactGetItems":
		return p.handleTransactGetItems(body)
	case "UpdateTable":
		return p.handleUpdateTable(body)
	case "DescribeTimeToLive":
		return p.handleDescribeTimeToLive(body)
	case "UpdateTimeToLive":
		return p.handleUpdateTimeToLive(body)
	case "TagResource":
		return p.handleTagResource(body)
	case "UntagResource":
		return p.handleUntagResource(body)
	case "ListTagsOfResource":
		return p.handleListTagsOfResource(body)
	default:
		return jsonError("UnknownOperationException", fmt.Sprintf("unknown operation: %s", op), http.StatusBadRequest), nil
	}
}

// ListResources returns all DynamoDB tables as plugin resources.
func (p *DynamoDBProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	names := p.store.ListTables()
	resources := make([]plugin.Resource, 0, len(names))
	for _, name := range names {
		resources = append(resources, plugin.Resource{
			Type: "table",
			ID:   name,
			Name: name,
		})
	}
	return resources, nil
}

// GetMetrics returns basic metrics for the DynamoDB provider.
func (p *DynamoDBProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	names := p.store.ListTables()
	return &plugin.ServiceMetrics{
		ResourceCount: len(names),
	}, nil
}

// --- request/response structs ---

type keySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // HASH or RANGE
}

type attributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"` // S, N, or B
}

type gsiRequest struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []keySchemaElement `json:"KeySchema"`
	Projection struct {
		ProjectionType string `json:"ProjectionType"`
	} `json:"Projection"`
}

type streamSpecRequest struct {
	StreamEnabled  bool   `json:"StreamEnabled"`
	StreamViewType string `json:"StreamViewType"`
}

type createTableRequest struct {
	TableName              string                `json:"TableName"`
	KeySchema              []keySchemaElement    `json:"KeySchema"`
	AttributeDefinitions   []attributeDefinition `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []gsiRequest          `json:"GlobalSecondaryIndexes"`
	LocalSecondaryIndexes  []gsiRequest          `json:"LocalSecondaryIndexes"`
	BillingMode            string                `json:"BillingMode"`
	StreamSpecification    *streamSpecRequest    `json:"StreamSpecification,omitempty"`
}

type streamSpecResponse struct {
	StreamEnabled  bool   `json:"StreamEnabled"`
	StreamViewType string `json:"StreamViewType,omitempty"`
}

type tableDescriptionResponse struct {
	TableName           string              `json:"TableName"`
	TableStatus         string              `json:"TableStatus"`
	TableArn            string              `json:"TableArn,omitempty"`
	KeySchema           []keySchemaElement  `json:"KeySchema"`
	CreationDateTime    float64             `json:"CreationDateTime"`
	LatestStreamArn     string              `json:"LatestStreamArn,omitempty"`
	LatestStreamLabel   string              `json:"LatestStreamLabel,omitempty"`
	StreamSpecification *streamSpecResponse `json:"StreamSpecification,omitempty"`
}

// --- operation handlers ---

func (p *DynamoDBProvider) handleCreateTable(body []byte) (*plugin.Response, error) {
	var req createTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}
	if req.TableName == "" {
		return jsonError("ValidationException", "TableName is required", http.StatusBadRequest), nil
	}
	if len(req.KeySchema) == 0 {
		return jsonError("ValidationException", "KeySchema is required", http.StatusBadRequest), nil
	}

	// Build attribute type lookup.
	attrTypes := make(map[string]string, len(req.AttributeDefinitions))
	for _, ad := range req.AttributeDefinitions {
		attrTypes[ad.AttributeName] = ad.AttributeType
	}

	// Validate that all key schema attributes are defined in AttributeDefinitions.
	for _, ks := range req.KeySchema {
		if _, ok := attrTypes[ks.AttributeName]; !ok {
			return jsonError(
				"ValidationException",
				fmt.Sprintf("One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [%s], AttributeDefinitions: [%s]",
					ks.AttributeName, strings.Join(attrDefNames(req.AttributeDefinitions), ", ")),
				http.StatusBadRequest,
			), nil
		}
	}

	// Build TableInfo from request.
	info := TableInfo{
		Name:        req.TableName,
		Status:      "ACTIVE",
		BillingMode: req.BillingMode,
	}

	for _, ks := range req.KeySchema {
		switch ks.KeyType {
		case "HASH":
			info.PartitionKey = KeyDef{
				Name: ks.AttributeName,
				Type: attrTypes[ks.AttributeName],
			}
		case "RANGE":
			sk := KeyDef{
				Name: ks.AttributeName,
				Type: attrTypes[ks.AttributeName],
			}
			info.SortKey = &sk
		}
	}

	// Parse GSI/LSI definitions.
	for _, g := range req.GlobalSecondaryIndexes {
		idx := IndexDef{
			IndexName:      g.IndexName,
			ProjectionType: g.Projection.ProjectionType,
		}
		for _, ks := range g.KeySchema {
			idx.KeySchema = append(idx.KeySchema, KeyDef{
				Name:    ks.AttributeName,
				Type:    attrTypes[ks.AttributeName],
				KeyType: ks.KeyType, // HASH or RANGE
			})
		}
		info.GlobalSecondaryIndexes = append(info.GlobalSecondaryIndexes, idx)
	}
	for _, l := range req.LocalSecondaryIndexes {
		idx := IndexDef{
			IndexName:      l.IndexName,
			ProjectionType: l.Projection.ProjectionType,
		}
		for _, ks := range l.KeySchema {
			idx.KeySchema = append(idx.KeySchema, KeyDef{
				Name:    ks.AttributeName,
				Type:    attrTypes[ks.AttributeName],
				KeyType: ks.KeyType,
			})
		}
		info.LocalSecondaryIndexes = append(info.LocalSecondaryIndexes, idx)
	}

	// Parse StreamSpecification and register with the streams service.
	if req.StreamSpecification != nil && req.StreamSpecification.StreamEnabled {
		info.StreamSpec = StreamSpec{
			Enabled:  true,
			ViewType: normalizeStreamViewType(req.StreamSpecification.StreamViewType),
		}
		label := time.Now().UTC().Format("2006-01-02T15:04:05.000")
		arn := shared.BuildARNWithAccount("dynamodb", "table",
			req.TableName+"/stream/"+label,
			shared.DefaultRegion, shared.DefaultAccountID)
		info.LatestStreamArn = arn
		info.LatestStreamLabel = label
	}

	if err := p.store.CreateTable(info); err != nil {
		if errors.Is(err, ErrTableAlreadyExists) {
			return jsonError(
				"com.amazonaws.dynamodb.v20120810#ResourceInUseException",
				fmt.Sprintf("table %q already exists", req.TableName),
				http.StatusBadRequest,
			), nil
		}
		return nil, err
	}

	// Create the stream in the streams store (best-effort).
	if info.StreamSpec.Enabled {
		if sstore := dynamodbstreams.GetGlobalStore(); sstore != nil {
			_, _ = sstore.CreateStream(info.LatestStreamArn, info.Name, info.LatestStreamLabel, info.StreamSpec.ViewType)
		}
	}

	schema := tableInfoToKeySchema(&info)
	desc := buildTableDescription(info, schema)
	return jsonResponse(http.StatusOK, map[string]any{"TableDescription": desc})
}

// normalizeStreamViewType returns a valid DynamoDB stream view type, defaulting
// to NEW_AND_OLD_IMAGES when empty or unrecognized.
func normalizeStreamViewType(v string) string {
	switch v {
	case "KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES":
		return v
	case "":
		return "NEW_AND_OLD_IMAGES"
	default:
		return "NEW_AND_OLD_IMAGES"
	}
}

func (p *DynamoDBProvider) handleDeleteTable(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	info, err := p.store.GetTable(req.TableName)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	// Build the description before deleting.
	schema := tableInfoToKeySchema(info)
	desc := buildTableDescription(*info, schema)

	if err := p.store.DeleteTable(req.TableName); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	return jsonResponse(http.StatusOK, map[string]any{"TableDescription": desc})
}

func (p *DynamoDBProvider) handleListTables(_ []byte) (*plugin.Response, error) {
	names := p.store.ListTables()
	if names == nil {
		names = []string{}
	}
	return jsonResponse(http.StatusOK, map[string]any{"TableNames": names})
}

func (p *DynamoDBProvider) handlePutItem(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                 string                     `json:"TableName"`
		Item                      Item                       `json:"Item"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	// Look up the previous item so we can (a) evaluate conditions and (b)
	// decide whether the stream event is INSERT or MODIFY.
	prior, err := p.store.GetItem(req.TableName, req.Item)
	if err != nil && !errors.Is(err, ErrItemNotFound) {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	// Evaluate ConditionExpression if present.
	if req.ConditionExpression != "" {
		var checkItem Item
		if prior != nil {
			checkItem = *prior
		} else {
			checkItem = make(Item)
		}
		if condErr := EvaluateConditionExpression(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, checkItem); condErr != nil {
			return jsonError("ConditionalCheckFailedException", condErr.Error(), http.StatusBadRequest), nil
		}
	}

	if err := p.store.PutItem(req.TableName, req.Item); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	p.publishPut(req.TableName, req.Item, prior)

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleGetItem(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                string            `json:"TableName"`
		Key                      Item              `json:"Key"`
		ProjectionExpression     string            `json:"ProjectionExpression"`
		ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	item, err := p.store.GetItem(req.TableName, req.Key)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		if errors.Is(err, ErrItemNotFound) {
			return jsonResponse(http.StatusOK, map[string]any{})
		}
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	resultItem := *item
	if req.ProjectionExpression != "" {
		resultItem = ApplyProjectionExpression(req.ProjectionExpression, req.ExpressionAttributeNames, resultItem)
	}

	return jsonResponse(http.StatusOK, map[string]any{"Item": resultItem})
}

func (p *DynamoDBProvider) handleDeleteItem(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                 string                     `json:"TableName"`
		Key                       Item                       `json:"Key"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	// Fetch the existing item for conditional evaluation and stream publish.
	prior, fetchErr := p.store.GetItem(req.TableName, req.Key)
	if fetchErr != nil {
		if errors.Is(fetchErr, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		if !errors.Is(fetchErr, ErrItemNotFound) {
			return jsonError("ValidationException", fetchErr.Error(), http.StatusBadRequest), nil
		}
	}

	// Evaluate ConditionExpression if present.
	if req.ConditionExpression != "" {
		if prior == nil {
			return jsonError("ConditionalCheckFailedException", "item not found", http.StatusBadRequest), nil
		}
		if condErr := EvaluateConditionExpression(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, *prior); condErr != nil {
			return jsonError("ConditionalCheckFailedException", condErr.Error(), http.StatusBadRequest), nil
		}
	}

	if err := p.store.DeleteItem(req.TableName, req.Key); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		if errors.Is(err, ErrItemNotFound) {
			return jsonResponse(http.StatusOK, map[string]any{})
		}
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	if prior != nil {
		p.publishDelete(req.TableName, *prior)
	}

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleDescribeTable(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	info, err := p.store.GetTable(req.TableName)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	schema := tableInfoToKeySchema(info)
	desc := buildTableDescription(*info, schema)
	return jsonResponse(http.StatusOK, map[string]any{"Table": desc})
}

func (p *DynamoDBProvider) handleUpdateItem(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                 string                     `json:"TableName"`
		Key                       Item                       `json:"Key"`
		UpdateExpression          string                     `json:"UpdateExpression"`
		ConditionExpression       string                     `json:"ConditionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	// Fetch existing item (or start with the key if it doesn't exist).
	var priorSnapshot *Item
	existing, err := p.store.GetItem(req.TableName, req.Key)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		if errors.Is(err, ErrItemNotFound) {
			// UpdateItem creates the item if it doesn't exist.
			item := make(Item)
			for k, v := range req.Key {
				item[k] = v
			}
			existing = &item
		} else {
			return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
		}
	} else {
		// Snapshot the prior state so the stream event can emit OldImage.
		snap := make(Item, len(*existing))
		for k, v := range *existing {
			snap[k] = v
		}
		priorSnapshot = &snap
	}

	// Evaluate ConditionExpression before modifying.
	if req.ConditionExpression != "" {
		if condErr := EvaluateConditionExpression(req.ConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, *existing); condErr != nil {
			return jsonError("ConditionalCheckFailedException", condErr.Error(), http.StatusBadRequest), nil
		}
	}

	// Apply UpdateExpression (supports SET, REMOVE, ADD, DELETE clauses).
	if req.UpdateExpression != "" {
		if err := applyUpdateExpression(*existing, req.UpdateExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues); err != nil {
			return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
		}
	}

	// Write back.
	if err := p.store.PutItem(req.TableName, *existing); err != nil {
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	p.publishPut(req.TableName, *existing, priorSnapshot)

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleQuery(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                 string                     `json:"TableName"`
		IndexName                 string                     `json:"IndexName"`
		KeyConditionExpression    string                     `json:"KeyConditionExpression"`
		FilterExpression          string                     `json:"FilterExpression"`
		ProjectionExpression      string                     `json:"ProjectionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	var items []Item
	var fetchErr error

	if req.IndexName != "" {
		// Query by GSI/LSI.
		pkValue := extractPartitionKeyValue(req.KeyConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		items, fetchErr = p.store.QueryGSI(req.TableName, req.IndexName, pkValue)
	} else {
		pkValue := extractPartitionKeyValue(req.KeyConditionExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		items, fetchErr = p.store.Query(req.TableName, pkValue, "")
	}

	if fetchErr != nil {
		if errors.Is(fetchErr, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, fetchErr
	}
	if items == nil {
		items = []Item{}
	}

	// Apply FilterExpression.
	if req.FilterExpression != "" {
		var filtered []Item
		for _, item := range items {
			if EvaluateFilterExpression(req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, item) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
		if items == nil {
			items = []Item{}
		}
	}

	// Apply ProjectionExpression.
	if req.ProjectionExpression != "" {
		for i, item := range items {
			items[i] = ApplyProjectionExpression(req.ProjectionExpression, req.ExpressionAttributeNames, item)
		}
	}

	return jsonResponse(http.StatusOK, map[string]any{
		"Items": items,
		"Count": len(items),
	})
}

func (p *DynamoDBProvider) handleScan(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName                 string                     `json:"TableName"`
		FilterExpression          string                     `json:"FilterExpression"`
		ProjectionExpression      string                     `json:"ProjectionExpression"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	items, err := p.store.Scan(req.TableName)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}
	if items == nil {
		items = []Item{}
	}

	// Apply FilterExpression.
	if req.FilterExpression != "" {
		var filtered []Item
		for _, item := range items {
			if EvaluateFilterExpression(req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues, item) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
		if items == nil {
			items = []Item{}
		}
	}

	// Apply ProjectionExpression.
	if req.ProjectionExpression != "" {
		for i, item := range items {
			items[i] = ApplyProjectionExpression(req.ProjectionExpression, req.ExpressionAttributeNames, item)
		}
	}

	return jsonResponse(http.StatusOK, map[string]any{
		"Items": items,
		"Count": len(items),
	})
}

func (p *DynamoDBProvider) handleBatchWriteItem(body []byte) (*plugin.Response, error) {
	var req struct {
		RequestItems map[string][]struct {
			PutRequest *struct {
				Item Item `json:"Item"`
			} `json:"PutRequest,omitempty"`
			DeleteRequest *struct {
				Key Item `json:"Key"`
			} `json:"DeleteRequest,omitempty"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	for tableName, requests := range req.RequestItems {
		for _, r := range requests {
			if r.PutRequest != nil {
				// Capture prior state for stream emission.
				prior, getErr := p.store.GetItem(tableName, r.PutRequest.Item)
				if getErr != nil && !errors.Is(getErr, ErrItemNotFound) && !errors.Is(getErr, ErrTableNotFound) {
					// Proceed; stream publish is best-effort.
					prior = nil
				}
				if err := p.store.PutItem(tableName, r.PutRequest.Item); err != nil {
					if errors.Is(err, ErrTableNotFound) {
						return tableNotFoundError(tableName), nil
					}
					return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
				}
				p.publishPut(tableName, r.PutRequest.Item, prior)
			}
			if r.DeleteRequest != nil {
				prior, getErr := p.store.GetItem(tableName, r.DeleteRequest.Key)
				if getErr != nil && !errors.Is(getErr, ErrItemNotFound) && !errors.Is(getErr, ErrTableNotFound) {
					prior = nil
				}
				if err := p.store.DeleteItem(tableName, r.DeleteRequest.Key); err != nil {
					if errors.Is(err, ErrTableNotFound) {
						return tableNotFoundError(tableName), nil
					}
					// Ignore ErrItemNotFound for batch deletes.
					if !errors.Is(err, ErrItemNotFound) {
						return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
					}
				}
				if prior != nil {
					p.publishDelete(tableName, *prior)
				}
			}
		}
	}

	return jsonResponse(http.StatusOK, map[string]any{
		"UnprocessedItems": map[string]any{},
	})
}

func (p *DynamoDBProvider) handleBatchGetItem(body []byte) (*plugin.Response, error) {
	var req struct {
		RequestItems map[string]struct {
			Keys []Item `json:"Keys"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	responses := make(map[string][]Item)
	for tableName, tableReq := range req.RequestItems {
		var tableItems []Item
		for _, key := range tableReq.Keys {
			item, err := p.store.GetItem(tableName, key)
			if err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(tableName), nil
				}
				if errors.Is(err, ErrItemNotFound) {
					continue // silently skip missing items
				}
				return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
			}
			tableItems = append(tableItems, *item)
		}
		if tableItems == nil {
			tableItems = []Item{}
		}
		responses[tableName] = tableItems
	}

	return jsonResponse(http.StatusOK, map[string]any{
		"Responses":       responses,
		"UnprocessedKeys": map[string]any{},
	})
}

func (p *DynamoDBProvider) handleTransactWriteItems(body []byte) (*plugin.Response, error) {
	var req struct {
		TransactItems []struct {
			Put *struct {
				TableName                 string                     `json:"TableName"`
				Item                      Item                       `json:"Item"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Put,omitempty"`
			Delete *struct {
				TableName                 string                     `json:"TableName"`
				Key                       Item                       `json:"Key"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Delete,omitempty"`
			Update *struct {
				TableName                 string                     `json:"TableName"`
				Key                       Item                       `json:"Key"`
				UpdateExpression          string                     `json:"UpdateExpression"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"Update,omitempty"`
			ConditionCheck *struct {
				TableName                 string                     `json:"TableName"`
				Key                       Item                       `json:"Key"`
				ConditionExpression       string                     `json:"ConditionExpression"`
				ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues"`
			} `json:"ConditionCheck,omitempty"`
		} `json:"TransactItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	// First pass: validate all conditions.
	for _, ti := range req.TransactItems {
		if ti.Put != nil && ti.Put.ConditionExpression != "" {
			existing, err := p.store.GetItem(ti.Put.TableName, ti.Put.Item)
			if err != nil && !errors.Is(err, ErrItemNotFound) {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.Put.TableName), nil
				}
			}
			checkItem := make(Item)
			if existing != nil {
				checkItem = *existing
			}
			if condErr := EvaluateConditionExpression(ti.Put.ConditionExpression, ti.Put.ExpressionAttributeNames, ti.Put.ExpressionAttributeValues, checkItem); condErr != nil {
				return jsonError("TransactionCanceledException", "Transaction cancelled, please refer cancellation reasons for specific reasons", http.StatusBadRequest), nil
			}
		}
		if ti.Delete != nil && ti.Delete.ConditionExpression != "" {
			existing, err := p.store.GetItem(ti.Delete.TableName, ti.Delete.Key)
			if err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.Delete.TableName), nil
				}
				if errors.Is(err, ErrItemNotFound) {
					return jsonError("TransactionCanceledException", "Transaction cancelled", http.StatusBadRequest), nil
				}
			}
			if condErr := EvaluateConditionExpression(ti.Delete.ConditionExpression, ti.Delete.ExpressionAttributeNames, ti.Delete.ExpressionAttributeValues, *existing); condErr != nil {
				return jsonError("TransactionCanceledException", "Transaction cancelled, please refer cancellation reasons for specific reasons", http.StatusBadRequest), nil
			}
		}
		if ti.ConditionCheck != nil {
			existing, err := p.store.GetItem(ti.ConditionCheck.TableName, ti.ConditionCheck.Key)
			if err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.ConditionCheck.TableName), nil
				}
				if errors.Is(err, ErrItemNotFound) {
					return jsonError("TransactionCanceledException", "Transaction cancelled", http.StatusBadRequest), nil
				}
			}
			if condErr := EvaluateConditionExpression(ti.ConditionCheck.ConditionExpression, ti.ConditionCheck.ExpressionAttributeNames, ti.ConditionCheck.ExpressionAttributeValues, *existing); condErr != nil {
				return jsonError("TransactionCanceledException", "Transaction cancelled, please refer cancellation reasons for specific reasons", http.StatusBadRequest), nil
			}
		}
	}

	// Second pass: apply writes.
	for _, ti := range req.TransactItems {
		if ti.Put != nil {
			if err := p.store.PutItem(ti.Put.TableName, ti.Put.Item); err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.Put.TableName), nil
				}
				return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
			}
		}
		if ti.Delete != nil {
			if err := p.store.DeleteItem(ti.Delete.TableName, ti.Delete.Key); err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.Delete.TableName), nil
				}
				if !errors.Is(err, ErrItemNotFound) {
					return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
				}
			}
		}
		if ti.Update != nil {
			existing, err := p.store.GetItem(ti.Update.TableName, ti.Update.Key)
			if err != nil {
				if errors.Is(err, ErrTableNotFound) {
					return tableNotFoundError(ti.Update.TableName), nil
				}
				if errors.Is(err, ErrItemNotFound) {
					item := make(Item)
					for k, v := range ti.Update.Key {
						item[k] = v
					}
					existing = &item
				} else {
					return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
				}
			}
			if ti.Update.UpdateExpression != "" {
				if err := applyUpdateExpression(*existing, ti.Update.UpdateExpression, ti.Update.ExpressionAttributeNames, ti.Update.ExpressionAttributeValues); err != nil {
					return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
				}
			}
			if err := p.store.PutItem(ti.Update.TableName, *existing); err != nil {
				return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
			}
		}
	}

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleTransactGetItems(body []byte) (*plugin.Response, error) {
	var req struct {
		TransactItems []struct {
			Get struct {
				TableName string `json:"TableName"`
				Key       Item   `json:"Key"`
			} `json:"Get"`
		} `json:"TransactItems"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	type getResponse struct {
		Item Item `json:"Item,omitempty"`
	}
	responses := make([]getResponse, 0, len(req.TransactItems))
	for _, ti := range req.TransactItems {
		item, err := p.store.GetItem(ti.Get.TableName, ti.Get.Key)
		if err != nil {
			if errors.Is(err, ErrTableNotFound) {
				return tableNotFoundError(ti.Get.TableName), nil
			}
			if errors.Is(err, ErrItemNotFound) {
				responses = append(responses, getResponse{})
				continue
			}
			return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
		}
		responses = append(responses, getResponse{Item: *item})
	}

	return jsonResponse(http.StatusOK, map[string]any{"Responses": responses})
}

func (p *DynamoDBProvider) handleUpdateTable(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName           string             `json:"TableName"`
		BillingMode         string             `json:"BillingMode"`
		StreamSpecification *streamSpecRequest `json:"StreamSpecification,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	// We need to know the resulting stream state after update to synchronize
	// with the streams store outside of the table mutex.
	var (
		enabledNow bool
		disabled   bool
		newARN     string
		newLabel   string
		viewType   string
		priorARN   string
	)

	err := p.store.UpdateTable(req.TableName, func(info *TableInfo) {
		if req.BillingMode != "" {
			info.BillingMode = req.BillingMode
		}
		if req.StreamSpecification == nil {
			return
		}
		priorARN = info.LatestStreamArn
		if req.StreamSpecification.StreamEnabled {
			viewType = normalizeStreamViewType(req.StreamSpecification.StreamViewType)
			if !info.StreamSpec.Enabled || info.LatestStreamArn == "" {
				label := time.Now().UTC().Format("2006-01-02T15:04:05.000")
				arn := shared.BuildARNWithAccount("dynamodb", "table",
					info.Name+"/stream/"+label,
					shared.DefaultRegion, shared.DefaultAccountID)
				info.LatestStreamArn = arn
				info.LatestStreamLabel = label
				newARN = arn
				newLabel = label
			}
			info.StreamSpec = StreamSpec{Enabled: true, ViewType: viewType}
			enabledNow = true
		} else {
			if info.StreamSpec.Enabled {
				disabled = true
			}
			info.StreamSpec = StreamSpec{Enabled: false}
		}
	})
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	if sstore := dynamodbstreams.GetGlobalStore(); sstore != nil {
		switch {
		case enabledNow && newARN != "":
			_, _ = sstore.CreateStream(newARN, req.TableName, newLabel, viewType)
		case enabledNow && newARN == "":
			// Already had a stream — leave it as-is.
		case disabled && priorARN != "":
			_ = sstore.SetStreamStatus(priorARN, "DISABLED")
		}
	}

	info, _ := p.store.GetTable(req.TableName)
	schema := tableInfoToKeySchema(info)
	desc := buildTableDescription(*info, schema)
	return jsonResponse(http.StatusOK, map[string]any{"TableDescription": desc})
}

func (p *DynamoDBProvider) handleDescribeTimeToLive(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetTable(req.TableName); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	cfg, err := p.store.GetTTLConfig(req.TableName)
	if err != nil {
		return nil, err
	}

	status := "DISABLED"
	if cfg.Enabled {
		status = "ENABLED"
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": status,
			"AttributeName":    cfg.AttributeName,
		},
	})
}

func (p *DynamoDBProvider) handleUpdateTimeToLive(body []byte) (*plugin.Response, error) {
	var req struct {
		TableName      string `json:"TableName"`
		TimeToLiveSpec struct {
			AttributeName string `json:"AttributeName"`
			Enabled       bool   `json:"Enabled"`
		} `json:"TimeToLiveSpecification"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	if _, err := p.store.GetTable(req.TableName); err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return tableNotFoundError(req.TableName), nil
		}
		return nil, err
	}

	cfg := TTLConfig{
		AttributeName: req.TimeToLiveSpec.AttributeName,
		Enabled:       req.TimeToLiveSpec.Enabled,
	}
	if err := p.store.PutTTLConfig(req.TableName, cfg); err != nil {
		return nil, err
	}

	return jsonResponse(http.StatusOK, map[string]any{
		"TimeToLiveSpecification": map[string]any{
			"AttributeName": cfg.AttributeName,
			"Enabled":       cfg.Enabled,
		},
	})
}

func (p *DynamoDBProvider) handleTagResource(body []byte) (*plugin.Response, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}
	if err := p.store.PutTags(req.ResourceArn, tags); err != nil {
		return nil, err
	}

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleUntagResource(body []byte) (*plugin.Response, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	if err := p.store.RemoveTags(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return jsonResponse(http.StatusOK, map[string]any{})
}

func (p *DynamoDBProvider) handleListTagsOfResource(body []byte) (*plugin.Response, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "invalid JSON body", http.StatusBadRequest), nil
	}

	tags, err := p.store.GetTags(req.ResourceArn)
	if err != nil {
		return nil, err
	}

	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}

	return jsonResponse(http.StatusOK, map[string]any{"Tags": tagList})
}

// --- helpers ---

// extractPartitionKeyValue parses a simple "pk = :placeholder" expression
// and returns the string value of the matched placeholder from attrs.
func extractPartitionKeyValue(expr string, nameMap map[string]string, attrs map[string]*AttributeValue) string {
	// Find placeholder token (starts with ':')
	parts := strings.Fields(expr)
	for _, part := range parts {
		if strings.HasPrefix(part, ":") {
			placeholder := part
			if av, ok := attrs[placeholder]; ok && av != nil {
				if av.S != nil {
					return *av.S
				}
				if av.N != nil {
					return *av.N
				}
				if av.B != nil {
					return string(av.B)
				}
			}
			break
		}
	}
	return ""
}

// resolveExprName resolves an expression attribute name reference (e.g. "#c" -> "count").
func resolveExprName(token string, nameMap map[string]string) string {
	if strings.HasPrefix(token, "#") {
		if resolved, ok := nameMap[token]; ok {
			return resolved
		}
	}
	return token
}

// applyUpdateExpression applies a SET/REMOVE/ADD/DELETE update expression to the item.
func applyUpdateExpression(item Item, expr string, nameMap map[string]string, valMap map[string]*AttributeValue) error {
	if nameMap == nil {
		nameMap = map[string]string{}
	}
	if valMap == nil {
		valMap = map[string]*AttributeValue{}
	}

	expr = strings.TrimSpace(expr)

	// Split expression into sections by keywords SET, REMOVE, ADD, DELETE.
	// We find positions of section keywords and split accordingly.
	sections := splitUpdateSections(expr)

	for section, content := range sections {
		content = strings.TrimSpace(content)
		if content == "" {
			continue
		}
		switch strings.ToUpper(section) {
		case "SET":
			if err := applySetClause(item, content, nameMap, valMap); err != nil {
				return err
			}
		case "REMOVE":
			applyRemoveClause(item, content, nameMap)
		case "ADD":
			if err := applyAddClause(item, content, nameMap, valMap); err != nil {
				return err
			}
		case "DELETE":
			if err := applyDeleteClause(item, content, nameMap, valMap); err != nil {
				return err
			}
		}
	}
	return nil
}

// splitUpdateSections splits an update expression into sections keyed by SET/REMOVE/ADD/DELETE.
func splitUpdateSections(expr string) map[string]string {
	keywords := []string{"SET", "REMOVE", "ADD", "DELETE"}
	sections := make(map[string]string)

	upper := strings.ToUpper(expr)
	type sectionStart struct {
		kw  string
		pos int
	}
	var found []sectionStart
	for _, kw := range keywords {
		// Find keyword at word boundary.
		search := kw
		idx := strings.Index(upper, search)
		for idx >= 0 {
			// Check it's a word boundary.
			before := idx == 0 || !isAlphaNum(rune(expr[idx-1]))
			after := idx+len(kw) >= len(upper) || !isAlphaNum(rune(upper[idx+len(kw)]))
			if before && after {
				found = append(found, sectionStart{kw: kw, pos: idx})
				break
			}
			idx = strings.Index(upper[idx+1:], search)
			if idx >= 0 {
				idx += idx + 1
			}
		}
	}

	// Sort by position.
	for i := 0; i < len(found); i++ {
		for j := i + 1; j < len(found); j++ {
			if found[j].pos < found[i].pos {
				found[i], found[j] = found[j], found[i]
			}
		}
	}

	for i, s := range found {
		start := s.pos + len(s.kw)
		end := len(expr)
		if i+1 < len(found) {
			end = found[i+1].pos
		}
		sections[s.kw] = strings.TrimSpace(expr[start:end])
	}

	// If no section keyword found, treat entire expression as SET.
	if len(sections) == 0 && expr != "" {
		sections["SET"] = expr
	}

	return sections
}

func isAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

func applySetClause(item Item, content string, nameMap map[string]string, valMap map[string]*AttributeValue) error {
	assignments := strings.Split(content, ",")
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}
		eqIdx := strings.Index(assignment, "=")
		if eqIdx < 0 {
			return fmt.Errorf("invalid SET assignment: %s", assignment)
		}
		targetToken := strings.TrimSpace(assignment[:eqIdx])
		valueExpr := strings.TrimSpace(assignment[eqIdx+1:])
		targetAttr := resolveExprName(targetToken, nameMap)

		// Check for arithmetic: "expr + expr" or "expr - expr"
		if plusIdx := strings.Index(valueExpr, "+"); plusIdx >= 0 {
			left := strings.TrimSpace(valueExpr[:plusIdx])
			right := strings.TrimSpace(valueExpr[plusIdx+1:])
			leftVal := resolveNumericValue(left, nameMap, valMap, item)
			rightVal := resolveNumericValue(right, nameMap, valMap, item)
			sum := leftVal + rightVal
			s := strconv.FormatFloat(sum, 'f', -1, 64)
			item[targetAttr] = &AttributeValue{N: &s}
		} else if minusIdx := strings.Index(valueExpr, "-"); minusIdx >= 0 && !strings.HasPrefix(valueExpr, ":") {
			left := strings.TrimSpace(valueExpr[:minusIdx])
			right := strings.TrimSpace(valueExpr[minusIdx+1:])
			leftVal := resolveNumericValue(left, nameMap, valMap, item)
			rightVal := resolveNumericValue(right, nameMap, valMap, item)
			diff := leftVal - rightVal
			s := strconv.FormatFloat(diff, 'f', -1, 64)
			item[targetAttr] = &AttributeValue{N: &s}
		} else {
			val := resolveValue(valueExpr, nameMap, valMap, item)
			if val != nil {
				item[targetAttr] = val
			}
		}
	}
	return nil
}

func applyRemoveClause(item Item, content string, nameMap map[string]string) {
	parts := strings.Split(content, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		attrName := resolveExprName(part, nameMap)
		delete(item, attrName)
	}
}

func applyAddClause(item Item, content string, nameMap map[string]string, valMap map[string]*AttributeValue) error {
	// ADD path value pairs separated by commas.
	assignments := strings.Split(content, ",")
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}
		// Split on whitespace: "path :val"
		parts := strings.Fields(assignment)
		if len(parts) < 2 {
			return fmt.Errorf("invalid ADD clause: %s", assignment)
		}
		attrName := resolveExprName(parts[0], nameMap)
		valToken := parts[1]
		addVal := resolveValue(valToken, nameMap, valMap, item)
		if addVal == nil {
			continue
		}

		existing, exists := item[attrName]
		if !exists || existing == nil {
			item[attrName] = addVal
			continue
		}

		if existing.N != nil && addVal.N != nil {
			ef, _ := strconv.ParseFloat(*existing.N, 64)
			af, _ := strconv.ParseFloat(*addVal.N, 64)
			sum := ef + af
			s := strconv.FormatFloat(sum, 'f', -1, 64)
			item[attrName] = &AttributeValue{N: &s}
		} else if existing.SS != nil && addVal.SS != nil {
			// Union of string sets.
			seen := make(map[string]bool)
			for _, s := range existing.SS {
				seen[s] = true
			}
			for _, s := range addVal.SS {
				seen[s] = true
			}
			ss := make([]string, 0, len(seen))
			for s := range seen {
				ss = append(ss, s)
			}
			item[attrName] = &AttributeValue{SS: ss}
		} else if existing.NS != nil && addVal.NS != nil {
			seen := make(map[string]bool)
			for _, n := range existing.NS {
				seen[n] = true
			}
			for _, n := range addVal.NS {
				seen[n] = true
			}
			ns := make([]string, 0, len(seen))
			for n := range seen {
				ns = append(ns, n)
			}
			item[attrName] = &AttributeValue{NS: ns}
		}
	}
	return nil
}

func applyDeleteClause(item Item, content string, nameMap map[string]string, valMap map[string]*AttributeValue) error {
	// DELETE path value (remove elements from sets).
	assignments := strings.Split(content, ",")
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}
		parts := strings.Fields(assignment)
		if len(parts) < 2 {
			return fmt.Errorf("invalid DELETE clause: %s", assignment)
		}
		attrName := resolveExprName(parts[0], nameMap)
		valToken := parts[1]
		delVal := resolveValue(valToken, nameMap, valMap, item)
		if delVal == nil {
			continue
		}

		existing, exists := item[attrName]
		if !exists || existing == nil {
			continue
		}

		if existing.SS != nil && delVal.SS != nil {
			remove := make(map[string]bool)
			for _, s := range delVal.SS {
				remove[s] = true
			}
			var ss []string
			for _, s := range existing.SS {
				if !remove[s] {
					ss = append(ss, s)
				}
			}
			item[attrName] = &AttributeValue{SS: ss}
		} else if existing.NS != nil && delVal.NS != nil {
			remove := make(map[string]bool)
			for _, n := range delVal.NS {
				remove[n] = true
			}
			var ns []string
			for _, n := range existing.NS {
				if !remove[n] {
					ns = append(ns, n)
				}
			}
			item[attrName] = &AttributeValue{NS: ns}
		}
	}
	return nil
}

// resolveNumericValue resolves a token to a float64 value.
func resolveNumericValue(token string, nameMap map[string]string, valMap map[string]*AttributeValue, item Item) float64 {
	av := resolveValue(token, nameMap, valMap, item)
	if av != nil && av.N != nil {
		f, _ := strconv.ParseFloat(*av.N, 64)
		return f
	}
	return 0
}

// resolveValue resolves a token to an AttributeValue.
func resolveValue(token string, nameMap map[string]string, valMap map[string]*AttributeValue, item Item) *AttributeValue {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(token, ":") {
		if av, ok := valMap[token]; ok {
			return av
		}
		return nil
	}
	// Resolve attribute name reference or literal attribute name.
	attrName := resolveExprName(token, nameMap)
	if av, ok := item[attrName]; ok {
		return av
	}
	return nil
}

// attrDefNames returns the attribute names from a slice of attributeDefinition.
func attrDefNames(defs []attributeDefinition) []string {
	names := make([]string, len(defs))
	for i, d := range defs {
		names[i] = d.AttributeName
	}
	return names
}

// buildTableDescription constructs the TableDescription response payload.
func buildTableDescription(info TableInfo, keySchema []keySchemaElement) tableDescriptionResponse {
	desc := tableDescriptionResponse{
		TableName:        info.Name,
		TableStatus:      info.Status,
		TableArn:         info.TableArn,
		KeySchema:        keySchema,
		CreationDateTime: float64(info.CreatedAt.UnixNano()) / float64(time.Second),
	}
	if info.StreamSpec.Enabled {
		desc.StreamSpecification = &streamSpecResponse{
			StreamEnabled:  true,
			StreamViewType: info.StreamSpec.ViewType,
		}
	}
	if info.LatestStreamArn != "" {
		desc.LatestStreamArn = info.LatestStreamArn
		desc.LatestStreamLabel = info.LatestStreamLabel
	}
	return desc
}

// tableInfoToKeySchema converts a TableInfo back into a KeySchema slice.
func tableInfoToKeySchema(info *TableInfo) []keySchemaElement {
	ks := []keySchemaElement{
		{AttributeName: info.PartitionKey.Name, KeyType: "HASH"},
	}
	if info.SortKey != nil {
		ks = append(ks, keySchemaElement{AttributeName: info.SortKey.Name, KeyType: "RANGE"})
	}
	return ks
}

// tableNotFoundError returns a DynamoDB ResourceNotFoundException response.
func tableNotFoundError(tableName string) *plugin.Response {
	return jsonError(
		"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
		fmt.Sprintf("requested resource not found: table: %s not found", tableName),
		http.StatusBadRequest,
	)
}

// jsonError builds a JSON error response compatible with DynamoDB's error format.
func jsonError(errType, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{
		"__type":  errType,
		"message": message,
	})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/x-amz-json-1.0",
		Body:        body,
	}
}

// jsonResponse marshals v and returns a 200 JSON response.
func jsonResponse(status int, v any) (*plugin.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal response: %w", err)
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/x-amz-json-1.0",
		Body:        body,
	}, nil
}
