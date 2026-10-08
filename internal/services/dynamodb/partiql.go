// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type executeStatementRequest struct {
	Statement  string            `json:"Statement"`
	Parameters []*AttributeValue `json:"Parameters,omitempty"`
}

type batchExecuteStatementRequest struct {
	Statements []executeStatementRequest `json:"Statements"`
}

type executeTransactionRequest struct {
	TransactStatements []executeStatementRequest `json:"TransactStatements"`
}

func (p *DynamoDBProvider) handleExecuteStatement(body []byte) (*plugin.Response, error) {
	var req executeStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}

	items, err := p.executePartiQL(req.Statement, req.Parameters)
	if err != nil {
		return jsonError("ValidationException", err.Error(), http.StatusBadRequest), nil
	}

	out := map[string]any{
		"Items": items,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleBatchExecuteStatement(body []byte) (*plugin.Response, error) {
	var req batchExecuteStatementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}

	var responses []map[string]any
	for _, stmt := range req.Statements {
		items, err := p.executePartiQL(stmt.Statement, stmt.Parameters)
		if err != nil {
			responses = append(responses, map[string]any{
				"Error": map[string]string{
					"Code":    "ValidationException",
					"Message": err.Error(),
				},
			})
			continue
		}
		var item Item
		if len(items) > 0 {
			item = items[0]
		}
		responses = append(responses, map[string]any{
			"Item": item,
		})
	}

	out := map[string]any{
		"Responses": responses,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleExecuteTransaction(body []byte) (*plugin.Response, error) {
	var req executeTransactionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}

	var responses []map[string]any
	for _, stmt := range req.TransactStatements {
		items, err := p.executePartiQL(stmt.Statement, stmt.Parameters)
		if err != nil {
			return jsonError("TransactionCanceledException", fmt.Sprintf("Transaction cancelled: %s", err.Error()), http.StatusBadRequest), nil
		}
		var item Item
		if len(items) > 0 {
			item = items[0]
		}
		responses = append(responses, map[string]any{
			"Item": item,
		})
	}

	out := map[string]any{
		"Responses": responses,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) executePartiQL(statement string, params []*AttributeValue) ([]Item, error) {
	stmt := strings.TrimSpace(statement)
	upper := strings.ToUpper(stmt)

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return p.executeSelect(stmt, params)
	case strings.HasPrefix(upper, "INSERT"):
		return p.executeInsert(stmt, params)
	case strings.HasPrefix(upper, "UPDATE"):
		return p.executeUpdate(stmt, params)
	case strings.HasPrefix(upper, "DELETE"):
		return p.executeDelete(stmt, params)
	default:
		return nil, fmt.Errorf("unsupported statement: %s", stmt)
	}
}

func extractTableName(fromClause string) string {
	from := strings.TrimSpace(fromClause)
	idx := strings.Index(strings.ToUpper(from), " WHERE ")
	if idx != -1 {
		from = strings.TrimSpace(from[:idx])
	}
	return strings.Trim(from, "\"'` ")
}

func (p *DynamoDBProvider) executeSelect(stmt string, params []*AttributeValue) ([]Item, error) {
	upper := strings.ToUpper(stmt)
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		return nil, fmt.Errorf("missing FROM clause")
	}

	fromPart := stmt[fromIdx+6:]
	tableName := extractTableName(fromPart)

	table, err := p.store.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Check if WHERE clause exists
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx == -1 {
		return p.store.Scan(tableName)
	}

	wherePart := strings.TrimSpace(stmt[whereIdx+7:])
	keyMap, err := parseWhereKey(wherePart, params)
	if err != nil {
		// Fallback to Scan and in-memory filter
		return p.scanAndFilter(tableName, wherePart, params)
	}

	// Try GetItem
	pkName := table.PartitionKey.Name
	if _, ok := keyMap[pkName]; ok {
		item, err := p.store.GetItem(tableName, keyMap)
		if err != nil || item == nil {
			return []Item{}, nil
		}
		return []Item{*item}, nil
	}

	return p.scanAndFilter(tableName, wherePart, params)
}

func (p *DynamoDBProvider) scanAndFilter(tableName, wherePart string, params []*AttributeValue) ([]Item, error) {
	items, err := p.store.Scan(tableName)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(wherePart, "=", 2)
	if len(parts) != 2 {
		return items, nil
	}
	k := strings.Trim(strings.TrimSpace(parts[0]), "\"'` ")
	valStr := strings.Trim(strings.TrimSpace(parts[1]), "\"'` ")
	if valStr == "?" && len(params) > 0 {
		if params[0].S != nil {
			valStr = *params[0].S
		} else if params[0].N != nil {
			valStr = *params[0].N
		}
	}

	var matched []Item
	for _, it := range items {
		if av, ok := it[k]; ok {
			var itVal string
			if av.S != nil {
				itVal = *av.S
			} else if av.N != nil {
				itVal = *av.N
			}
			if itVal == valStr {
				matched = append(matched, it)
			}
		}
	}
	return matched, nil
}

func parseWhereKey(where string, params []*AttributeValue) (Item, error) {
	parts := strings.Split(where, "AND")
	key := make(Item)
	paramIdx := 0

	for _, p := range parts {
		cond := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(cond) != 2 {
			continue
		}
		k := strings.Trim(strings.TrimSpace(cond[0]), "\"'` ")
		v := strings.TrimSpace(cond[1])
		if v == "?" {
			if paramIdx < len(params) {
				key[k] = params[paramIdx]
				paramIdx++
			}
			continue
		}
		valStr := strings.Trim(v, "\"'` ")
		key[k] = &AttributeValue{S: &valStr}
	}
	return key, nil
}

func (p *DynamoDBProvider) executeInsert(stmt string, params []*AttributeValue) ([]Item, error) {
	upper := strings.ToUpper(stmt)
	intoIdx := strings.Index(upper, " INTO ")
	if intoIdx == -1 {
		return nil, fmt.Errorf("missing INTO in INSERT")
	}

	valIdx := strings.Index(upper, " VALUE ")
	if valIdx == -1 {
		return nil, fmt.Errorf("missing VALUE in INSERT")
	}

	tableName := strings.Trim(strings.TrimSpace(stmt[intoIdx+6:valIdx]), "\"'` ")
	valStr := strings.TrimSpace(stmt[valIdx+7:])

	var item Item
	if valStr == "?" && len(params) > 0 && params[0].M != nil {
		item = params[0].M
	} else {
		// Replace single quotes with double quotes for valid JSON
		jsonStr := strings.ReplaceAll(valStr, "'", "\"")
		var raw map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
			return nil, fmt.Errorf("invalid VALUE syntax: %w", err)
		}
		item = make(Item)
		for k, v := range raw {
			sVal := fmt.Sprint(v)
			item[k] = &AttributeValue{S: &sVal}
		}
	}

	if err := p.store.PutItem(tableName, item); err != nil {
		return nil, err
	}
	return []Item{item}, nil
}

func (p *DynamoDBProvider) executeUpdate(stmt string, params []*AttributeValue) ([]Item, error) {
	upper := strings.ToUpper(stmt)
	setIdx := strings.Index(upper, " SET ")
	whereIdx := strings.Index(upper, " WHERE ")
	if setIdx == -1 || whereIdx == -1 {
		return nil, fmt.Errorf("invalid UPDATE syntax: missing SET or WHERE")
	}

	tableName := strings.Trim(strings.TrimSpace(stmt[6:setIdx]), "\"'` ")
	setClause := strings.TrimSpace(stmt[setIdx+5 : whereIdx])
	whereClause := strings.TrimSpace(stmt[whereIdx+7:])

	keyMap, err := parseWhereKey(whereClause, params)
	if err != nil {
		return nil, err
	}

	item, err := p.store.GetItem(tableName, keyMap)
	if err != nil || item == nil {
		item = &keyMap
	}

	setParts := strings.Split(setClause, ",")
	for _, sp := range setParts {
		pair := strings.SplitN(strings.TrimSpace(sp), "=", 2)
		if len(pair) == 2 {
			k := strings.Trim(strings.TrimSpace(pair[0]), "\"'` ")
			v := strings.Trim(strings.TrimSpace(pair[1]), "\"'` ")
			(*item)[k] = &AttributeValue{S: &v}
		}
	}

	if err := p.store.PutItem(tableName, *item); err != nil {
		return nil, err
	}
	return []Item{*item}, nil
}

func (p *DynamoDBProvider) executeDelete(stmt string, params []*AttributeValue) ([]Item, error) {
	upper := strings.ToUpper(stmt)
	fromIdx := strings.Index(upper, " FROM ")
	whereIdx := strings.Index(upper, " WHERE ")
	if fromIdx == -1 || whereIdx == -1 {
		return nil, fmt.Errorf("invalid DELETE syntax: missing FROM or WHERE")
	}

	tableName := strings.Trim(strings.TrimSpace(stmt[fromIdx+6:whereIdx]), "\"'` ")
	whereClause := strings.TrimSpace(stmt[whereIdx+7:])

	keyMap, err := parseWhereKey(whereClause, params)
	if err != nil {
		return nil, err
	}

	if err := p.store.DeleteItem(tableName, keyMap); err != nil {
		return nil, err
	}
	return []Item{}, nil
}
