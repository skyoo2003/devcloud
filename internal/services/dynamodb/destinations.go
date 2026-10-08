// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"encoding/json"
	"net/http"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type kinesisDestinationRequest struct {
	TableName string `json:"TableName"`
	StreamArn string `json:"StreamArn"`
}

// SetKinesisDestination updates Kinesis streaming destination status in SQLite.
func (s *DynamoStore) SetKinesisDestination(tableName, streamArn, status string) error {
	_, err := s.db.DB().Exec(
		`INSERT INTO ddb_kinesis_destinations (table_name, stream_arn, status)
		VALUES (?, ?, ?)
		ON CONFLICT(table_name, stream_arn) DO UPDATE SET status = excluded.status;`,
		tableName, streamArn, status,
	)
	return err
}

func (p *DynamoDBProvider) handleEnableKinesisStreamingDestination(body []byte) (*plugin.Response, error) {
	var req kinesisDestinationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if req.TableName == "" || req.StreamArn == "" {
		return jsonError("ValidationException", "TableName and StreamArn are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetTable(req.TableName); err != nil {
		return jsonError("ResourceNotFoundException", "Table not found", http.StatusNotFound), nil
	}

	_ = p.store.SetKinesisDestination(req.TableName, req.StreamArn, "ACTIVE")

	out := map[string]any{
		"DestinationStatus": "ACTIVE",
		"StreamArn":         req.StreamArn,
		"TableName":         req.TableName,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleDisableKinesisStreamingDestination(body []byte) (*plugin.Response, error) {
	var req kinesisDestinationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if req.TableName == "" || req.StreamArn == "" {
		return jsonError("ValidationException", "TableName and StreamArn are required", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetTable(req.TableName); err != nil {
		return jsonError("ResourceNotFoundException", "Table not found", http.StatusNotFound), nil
	}

	_ = p.store.SetKinesisDestination(req.TableName, req.StreamArn, "DISABLED")

	out := map[string]any{
		"DestinationStatus": "DISABLED",
		"StreamArn":         req.StreamArn,
		"TableName":         req.TableName,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}
