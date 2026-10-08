// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type exportTableRequest struct {
	TableArn string `json:"TableArn"`
	S3Bucket string `json:"S3Bucket"`
	S3Prefix string `json:"S3Prefix"`
}

type importTableRequest struct {
	S3BucketSource struct {
		S3Bucket    string `json:"S3Bucket"`
		S3KeyPrefix string `json:"S3KeyPrefix"`
	} `json:"S3BucketSource"`
	TableCreationParameters *struct {
		TableName string   `json:"TableName"`
		KeySchema []KeyDef `json:"KeySchema"`
	} `json:"TableCreationParameters"`
}

type restoreTableFromBackupRequest struct {
	TargetTableName string `json:"TargetTableName"`
	BackupArn       string `json:"BackupArn"`
}

type restoreTableToPITRRequest struct {
	SourceTableName string `json:"SourceTableName"`
	TargetTableName string `json:"TargetTableName"`
}

// CloneTable creates a copy of an existing table and its items under targetName.
func (s *DynamoStore) CloneTable(sourceName, targetName string) (*TableInfo, error) {
	src, err := s.GetTable(sourceName)
	if err != nil {
		return nil, err
	}
	targetInfo := *src
	targetInfo.Name = targetName
	targetInfo.CreatedAt = time.Now().UTC()
	targetInfo.TableArn = fmt.Sprintf("arn:aws:dynamodb:us-east-1:000000000000:table/%s", targetName)

	if err := s.CreateTable(targetInfo); err != nil {
		return nil, err
	}

	items, err := s.Scan(sourceName)
	if err == nil {
		for _, it := range items {
			_ = s.PutItem(targetName, it)
		}
	}
	return &targetInfo, nil
}

func (p *DynamoDBProvider) handleExportTableToPointInTime(body []byte) (*plugin.Response, error) {
	var req exportTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}

	exportArn := fmt.Sprintf("%s/export/export-%d", req.TableArn, time.Now().Unix())
	out := map[string]any{
		"ExportDescription": map[string]any{
			"ExportArn":    exportArn,
			"ExportStatus": "COMPLETED",
			"StartTime":    time.Now().Unix(),
			"TableArn":     req.TableArn,
			"S3Bucket":     req.S3Bucket,
			"S3Prefix":     req.S3Prefix,
		},
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleImportTable(body []byte) (*plugin.Response, error) {
	var req importTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}

	tableName := "imported-table"
	if req.TableCreationParameters != nil && req.TableCreationParameters.TableName != "" {
		tableName = req.TableCreationParameters.TableName
		pk := KeyDef{Name: "id", Type: "S", KeyType: "HASH"}
		if len(req.TableCreationParameters.KeySchema) > 0 {
			pk = req.TableCreationParameters.KeySchema[0]
			pk.Type = "S"
		}
		_ = p.store.CreateTable(TableInfo{
			Name:         tableName,
			PartitionKey: pk,
			Status:       "ACTIVE",
			CreatedAt:    time.Now().UTC(),
		})
	}

	importArn := fmt.Sprintf("arn:aws:dynamodb:us-east-1:000000000000:table/%s/import/import-%d", tableName, time.Now().Unix())
	out := map[string]any{
		"ImportTableDescription": map[string]any{
			"ImportArn":    importArn,
			"ImportStatus": "COMPLETED",
			"TableArn":     fmt.Sprintf("arn:aws:dynamodb:us-east-1:000000000000:table/%s", tableName),
			"StartTime":    time.Now().Unix(),
		},
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleRestoreTableFromBackup(body []byte) (*plugin.Response, error) {
	var req restoreTableFromBackupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if req.TargetTableName == "" {
		return jsonError("ValidationException", "TargetTableName is required", http.StatusBadRequest), nil
	}

	// Try cloning from first existing table if available, or create fresh
	tables := p.store.ListTables()
	var info *TableInfo
	if len(tables) > 0 {
		var err error
		info, err = p.store.CloneTable(tables[0], req.TargetTableName)
		if err != nil {
			return jsonError("TableAlreadyExistsException", err.Error(), http.StatusBadRequest), nil
		}
	} else {
		newInfo := TableInfo{
			Name:         req.TargetTableName,
			PartitionKey: KeyDef{Name: "id", Type: "S", KeyType: "HASH"},
			Status:       "ACTIVE",
			CreatedAt:    time.Now().UTC(),
		}
		_ = p.store.CreateTable(newInfo)
		info = &newInfo
	}

	out := map[string]any{
		"TableDescription": info,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}

func (p *DynamoDBProvider) handleRestoreTableToPointInTime(body []byte) (*plugin.Response, error) {
	var req restoreTableToPITRRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return jsonError("SerializationException", "failed to parse request", http.StatusBadRequest), nil
	}
	if req.SourceTableName == "" || req.TargetTableName == "" {
		return jsonError("ValidationException", "SourceTableName and TargetTableName are required", http.StatusBadRequest), nil
	}

	info, err := p.store.CloneTable(req.SourceTableName, req.TargetTableName)
	if err != nil {
		return jsonError("ResourceNotFoundException", err.Error(), http.StatusBadRequest), nil
	}

	out := map[string]any{
		"TableDescription": info,
	}
	bytes, _ := json.Marshal(out)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/x-amz-json-1.0",
		Body:        bytes,
	}, nil
}
