// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
)

// ResourceSnapshot retains an identifier and detached original document.
type ResourceSnapshot struct {
	Resource string
	ID       string
	JSON     json.RawMessage
}

// Snapshot reads selected resource namespaces without changing their contents.
func Snapshot(service string, resources []string) ([]ResourceSnapshot, error) {
	result := []ResourceSnapshot{}
	if len(resources) == 0 {
		return result, nil
	}
	selected := map[string]bool{}
	for _, resource := range resources {
		selected[resource] = true
	}
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	sort.Strings(names)
	mu.RLock()
	defer mu.RUnlock()
	if db == nil {
		for _, resource := range names {
			items := store[storeKey(service, resource)]
			ids := make([]string, 0, len(items))
			for id := range items {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				encoded, err := json.Marshal(items[id])
				if err != nil {
					return nil, err
				}
				result = append(result, ResourceSnapshot{Resource: resource, ID: id, JSON: encoded})
			}
		}
		return result, nil
	}
	tx, err := db.DB().BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, resource := range names {
		rows, err := tx.Query(`SELECT resource_id, document_json FROM crud_resources WHERE service=? AND resource=? ORDER BY resource_id`, service, resource)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			result = append(result, ResourceSnapshot{Resource: resource, ID: id, JSON: append(json.RawMessage(nil), raw...)})
		}
		readErr := rows.Err()
		closeErr := rows.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
