// SPDX-License-Identifier: Apache-2.0

// Package s3tables implements AWS S3 Tables.
package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

const accountID = plugin.DefaultAccountID

// S3TablesProvider implements plugin.ServicePlugin for S3 Tables.
type S3TablesProvider struct {
	store *Store
}

func (p *S3TablesProvider) ServiceID() string             { return "s3tables" }
func (p *S3TablesProvider) ServiceName() string           { return "S3Tables" }
func (p *S3TablesProvider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *S3TablesProvider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewStore(filepath.Join(dataDir, "s3tables"))
	return err
}

func (p *S3TablesProvider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *S3TablesProvider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, _ := io.ReadAll(req.Body)
	var params map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &params)
	}
	if params == nil {
		params = map[string]any{}
	}

	if op == "" {
		op = resolveOp(req.Method, req.URL.Path)
	}

	switch op {
	// Bucket
	case "CreateTableBucket":
		return p.createTableBucket(params)
	case "DeleteTableBucket":
		return p.deleteTableBucket(req)
	case "GetTableBucket":
		return p.getTableBucket(req)
	case "ListTableBuckets":
		return p.listTableBuckets()
	// Namespace
	case "CreateNamespace":
		return p.createNamespace(req, params)
	case "DeleteNamespace":
		return p.deleteNamespace(req)
	case "GetNamespace":
		return p.getNamespace(req)
	case "ListNamespaces":
		return p.listNamespaces(req)
	// Table
	case "CreateTable":
		return p.createTable(req, params)
	case "DeleteTable":
		return p.deleteTable(req)
	case "GetTable":
		return p.getTable(req)
	case "ListTables":
		return p.listTables(req)
	case "RenameTable":
		return p.renameTable(req, params)
	case "UpdateTableMetadataLocation":
		return p.updateTableMetadataLocation(req, params)
	case "GetTableMetadataLocation":
		return p.getTableMetadataLocation(req)
	// Policy
	case "GetTablePolicy":
		return p.getTablePolicy(req)
	case "PutTablePolicy":
		return p.putTablePolicy(req, params)
	case "DeleteTablePolicy":
		return p.deleteTablePolicy(req)
	case "GetTableBucketPolicy":
		return p.getTableBucketPolicy(req)
	case "PutTableBucketPolicy":
		return p.putTableBucketPolicy(req, params)
	case "DeleteTableBucketPolicy":
		return p.deleteTableBucketPolicy(req)
	// Encryption
	case "GetTableEncryption":
		return p.getTableEncryption(req)
	case "PutTableEncryption":
		return p.putTableEncryption(req, params)
	case "DeleteTableEncryption":
		return p.deleteTableEncryption(req)
	case "GetTableBucketEncryption":
		return p.getTableBucketEncryption(req)
	case "PutTableBucketEncryption":
		return p.putTableBucketEncryption(req, params)
	case "DeleteTableBucketEncryption":
		return p.deleteTableBucketEncryption(req)
	// Maintenance Configuration
	case "GetTableMaintenanceConfiguration":
		return p.getTableMaintenanceConfiguration(req)
	case "PutTableMaintenanceConfiguration":
		return p.putTableMaintenanceConfiguration(req, params)
	case "GetTableBucketMaintenanceConfiguration":
		return p.getTableBucketMaintenanceConfiguration(req)
	case "PutTableBucketMaintenanceConfiguration":
		return p.putTableBucketMaintenanceConfiguration(req, params)
	// Maintenance Job Status
	case "GetTableMaintenanceJobStatus":
		return p.getTableMaintenanceJobStatus(req)
	case "GetTableBucketMaintenanceJobStatus":
		return p.getTableBucketMaintenanceJobStatus(req)
	default:
		return jsonError("UnsupportedOperation", fmt.Sprintf("unknown operation: %s", op), http.StatusBadRequest), nil
	}
}

func (p *S3TablesProvider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	buckets, err := p.store.ListBuckets()
	if err != nil {
		return nil, err
	}
	res := make([]plugin.Resource, 0, len(buckets))
	for _, b := range buckets {
		res = append(res, plugin.Resource{Type: "s3tables-bucket", ID: b.ARN, Name: b.Name})
	}
	return res, nil
}

func (p *S3TablesProvider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- Bucket operations ---

func (p *S3TablesProvider) createTableBucket(params map[string]any) (*plugin.Response, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return jsonError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	b, err := p.store.CreateBucket(name, accountID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return jsonError("ConflictException", "bucket already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"arn":       b.ARN,
		"name":      b.Name,
		"createdAt": b.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

func (p *S3TablesProvider) deleteTableBucket(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	name := bucketARN
	if name == "" {
		return jsonError("ValidationException", "bucket name is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteBucket(name); err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTableBucket(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	name := bucketARN
	if name == "" {
		return jsonError("ValidationException", "bucket name is required", http.StatusBadRequest), nil
	}
	b, err := p.store.GetBucket(name)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"arn":       b.ARN,
		"name":      b.Name,
		"createdAt": b.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

func (p *S3TablesProvider) listTableBuckets() (*plugin.Response, error) {
	buckets, err := p.store.ListBuckets()
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(buckets))
	for _, b := range buckets {
		list = append(list, map[string]any{
			"arn":       b.ARN,
			"name":      b.Name,
			"createdAt": b.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"tableBuckets": list})
}

// --- Namespace operations ---

func (p *S3TablesProvider) createNamespace(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}

	ns, _ := params["namespace"].(string)
	if nsArr, ok := params["namespace"].([]any); ok && len(nsArr) > 0 {
		if s, ok := nsArr[0].(string); ok {
			ns = s
		}
	}
	if ns == "" {
		return jsonError("ValidationException", "namespace is required", http.StatusBadRequest), nil
	}

	n, err := p.store.CreateNamespace(b.ARN, ns, accountID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return jsonError("ConflictException", "namespace already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableBucketARN": n.BucketARN,
		"namespace":      []string{n.Namespace},
		"createdAt":      n.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

func (p *S3TablesProvider) deleteNamespace(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteNamespace(b.ARN, ns); err != nil {
		return jsonError("NotFoundException", "namespace not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getNamespace(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	n, err := p.store.GetNamespace(b.ARN, ns)
	if err != nil {
		return jsonError("NotFoundException", "namespace not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableBucketARN": n.BucketARN,
		"namespace":      []string{n.Namespace},
		"createdAt":      n.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}

func (p *S3TablesProvider) listNamespaces(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	nsList, err := p.store.ListNamespaces(b.ARN)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(nsList))
	for _, n := range nsList {
		list = append(list, map[string]any{
			"tableBucketARN": n.BucketARN,
			"namespace":      []string{n.Namespace},
			"createdAt":      n.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return jsonResponse(http.StatusOK, map[string]any{"namespaces": list})
}

// --- Table operations ---

func (p *S3TablesProvider) createTable(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	name, _ := params["name"].(string)
	if name == "" {
		return jsonError("ValidationException", "name is required", http.StatusBadRequest), nil
	}
	format := "ICEBERG"
	if f, ok := params["format"].(string); ok && f != "" {
		format = f
	}
	t, err := p.store.CreateTable(b.ARN, ns, name, format, accountID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return jsonError("ConflictException", "table already exists", http.StatusConflict), nil
		}
		return nil, err
	}
	return jsonResponse(http.StatusOK, tableToMap(t))
}

func (p *S3TablesProvider) deleteTable(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	if err := p.store.DeleteTable(b.ARN, ns, tableName); err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTable(req *http.Request) (*plugin.Response, error) {
	q := req.URL.Query()
	bucketName := q.Get("tableBucketARN")
	ns := q.Get("namespace")
	tableName := q.Get("name")
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, tableToMap(t))
}

func (p *S3TablesProvider) listTables(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	ns := req.URL.Query().Get("namespace")
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	tables, err := p.store.ListTables(b.ARN, ns)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]any, 0, len(tables))
	for _, t := range tables {
		tt := t
		list = append(list, tableToMap(&tt))
	}
	return jsonResponse(http.StatusOK, map[string]any{"tables": list})
}

func (p *S3TablesProvider) renameTable(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, oldName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	newName, _ := params["newName"].(string)
	newNS, _ := params["newNamespaceName"].(string)
	if newNS == "" {
		if nsArr, ok := params["newNamespaceName"].([]any); ok && len(nsArr) > 0 {
			if s, ok := nsArr[0].(string); ok {
				newNS = s
			}
		}
	}
	if err := p.store.RenameTable(b.ARN, ns, oldName, newNS, newName); err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) updateTableMetadataLocation(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	metaLoc, _ := params["metadataLocation"].(string)
	versionToken, _ := params["versionToken"].(string)
	t, err := p.store.UpdateTableMetadataLocation(b.ARN, ns, tableName, metaLoc, versionToken)
	if err != nil {
		return jsonError("NotFoundException", err.Error(), http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"name":             t.Name,
		"tableARN":         t.ARN,
		"namespace":        t.Namespace,
		"versionToken":     t.VersionToken,
		"metadataLocation": t.MetadataLocation,
	})
}

func (p *S3TablesProvider) getTableMetadataLocation(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"metadataLocation": t.MetadataLocation,
		"versionToken":     t.VersionToken,
	})
}

// --- Policy operations ---

func (p *S3TablesProvider) getTablePolicy(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	policy, err := p.store.GetPolicy(t.ARN)
	if err != nil {
		return jsonError("NotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"resourcePolicy": policy})
}

func (p *S3TablesProvider) putTablePolicy(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	policy, _ := params["resourcePolicy"].(string)
	if err := p.store.PutPolicy(t.ARN, policy); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) deleteTablePolicy(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	p.store.DeletePolicy(t.ARN) //nolint:errcheck
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTableBucketPolicy(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	policy, err := p.store.GetPolicy(b.ARN)
	if err != nil {
		return jsonError("NotFoundException", "policy not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{"resourcePolicy": policy})
}

func (p *S3TablesProvider) putTableBucketPolicy(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	policy, _ := params["resourcePolicy"].(string)
	if err := p.store.PutPolicy(b.ARN, policy); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) deleteTableBucketPolicy(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	p.store.DeletePolicy(b.ARN) //nolint:errcheck
	return jsonResponse(http.StatusNoContent, nil)
}

// --- Encryption operations ---

func (p *S3TablesProvider) getTableEncryption(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	cfg, err := p.store.GetEncryption(t.ARN)
	if err != nil {
		cfg = `{"sseAlgorithm":"AES256"}`
	}
	var m map[string]any
	_ = json.Unmarshal([]byte(cfg), &m)
	return jsonResponse(http.StatusOK, map[string]any{"encryptionConfiguration": m})
}

func (p *S3TablesProvider) putTableEncryption(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	cfg := "{}"
	if raw, ok := params["encryptionConfiguration"]; ok {
		b, _ := json.Marshal(raw)
		cfg = string(b)
	}
	if err := p.store.PutEncryption(t.ARN, cfg); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) deleteTableEncryption(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	p.store.DeleteEncryption(t.ARN) //nolint:errcheck
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTableBucketEncryption(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	cfg, err := p.store.GetEncryption(b.ARN)
	if err != nil {
		cfg = `{"sseAlgorithm":"AES256"}`
	}
	var m map[string]any
	_ = json.Unmarshal([]byte(cfg), &m)
	return jsonResponse(http.StatusOK, map[string]any{"encryptionConfiguration": m})
}

func (p *S3TablesProvider) putTableBucketEncryption(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	cfg := "{}"
	if raw, ok := params["encryptionConfiguration"]; ok {
		b, _ := json.Marshal(raw)
		cfg = string(b)
	}
	if err := p.store.PutEncryption(b.ARN, cfg); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) deleteTableBucketEncryption(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	p.store.DeleteEncryption(b.ARN) //nolint:errcheck
	return jsonResponse(http.StatusNoContent, nil)
}

// --- Maintenance operations ---

func (p *S3TablesProvider) getTableMaintenanceConfiguration(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	cfgs, err := p.store.ListMaintenance(t.ARN)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for k, v := range cfgs {
		var m map[string]any
		_ = json.Unmarshal([]byte(v), &m)
		out[k] = m
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableARN":      t.ARN,
		"configuration": out,
	})
}

func (p *S3TablesProvider) putTableMaintenanceConfiguration(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, ns, tableName, subresource := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	typeParam := subresource
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	if typeParam == "" {
		typeParam, _ = params["type"].(string)
	}
	if typeParam == "" {
		typeParam = "default"
	}
	value := "{}"
	if v, ok := params["value"]; ok {
		b, _ := json.Marshal(v)
		value = string(b)
	}
	if err := p.store.PutMaintenance(t.ARN, typeParam, value); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTableBucketMaintenanceConfiguration(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	cfgs, err := p.store.ListMaintenance(b.ARN)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for k, v := range cfgs {
		var m map[string]any
		_ = json.Unmarshal([]byte(v), &m)
		out[k] = m
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableBucketARN": b.ARN,
		"configuration":  out,
	})
}

func (p *S3TablesProvider) putTableBucketMaintenanceConfiguration(req *http.Request, params map[string]any) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	typeParam := pathSegment(req.URL.Path, "maintenance")
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	if typeParam == "" {
		typeParam, _ = params["type"].(string)
	}
	if typeParam == "" {
		typeParam = "default"
	}
	value := "{}"
	if v, ok := params["value"]; ok {
		b, _ := json.Marshal(v)
		value = string(b)
	}
	if err := p.store.PutMaintenance(b.ARN, typeParam, value); err != nil {
		return nil, err
	}
	return jsonResponse(http.StatusNoContent, nil)
}

func (p *S3TablesProvider) getTableMaintenanceJobStatus(req *http.Request) (*plugin.Response, error) {
	bucketARN, ns, tableName, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	t, err := p.store.GetTable(b.ARN, ns, tableName)
	if err != nil {
		return jsonError("NotFoundException", "table not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableARN": t.ARN,
		"status":   map[string]any{"icebergCompaction": map[string]any{"status": "Successful"}},
	})
}

func (p *S3TablesProvider) getTableBucketMaintenanceJobStatus(req *http.Request) (*plugin.Response, error) {
	bucketARN, _, _, _ := s3PathParts(req.URL.Path)
	bucketName := bucketARN
	b, err := p.store.GetBucket(bucketName)
	if err != nil {
		return jsonError("NotFoundException", "bucket not found", http.StatusNotFound), nil
	}
	return jsonResponse(http.StatusOK, map[string]any{
		"tableBucketARN": b.ARN,
		"status":         map[string]any{"icebergUnreferencedFileRemoval": map[string]any{"status": "Successful"}},
	})
}

// --- helpers ---

func tableToMap(t *Table) map[string]any {
	return map[string]any{
		"tableARN":         t.ARN,
		"tableBucketARN":   t.BucketARN,
		"namespace":        []string{t.Namespace},
		"name":             t.Name,
		"format":           t.Format,
		"type":             t.Type,
		"metadataLocation": t.MetadataLocation,
		"versionToken":     t.VersionToken,
		"createdAt":        t.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}
}

// collapseARN collapses an ARN spread across URL path segments back into a
// single segment. S3Tables uses ARNs as URI path parameters; because ARNs
// contain '/' (e.g. "arn:aws:s3tables:...:bucket/my-bucket"), naive
// strings.Split yields extra segments. This helper rejoins them.
// collapseARN rejoins path segments starting at startIdx that form a single
// ARN value (ARNs contain '/' e.g. "arn:aws:s3tables:...:bucket/my-bucket").
func collapseARN(parts []string, startIdx int) []string {
	arnEnd := startIdx + 1
	for arnEnd < len(parts) && !strings.Contains(parts[arnEnd], ":") && parts[arnEnd] != "" {
		arnEnd++
	}
	rejoined := strings.Join(parts[startIdx:arnEnd], "/")
	result := make([]string, 0, len(parts)-(arnEnd-startIdx-1))
	result = append(result, parts[:startIdx]...)
	result = append(result, rejoined)
	result = append(result, parts[arnEnd:]...)
	return result
}

func resolveOp(method, path string) string {
	path = strings.TrimPrefix(path, "/v1")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && strings.Contains(parts[1], "arn:") {
		parts = collapseARN(parts, 1)
	}
	if len(parts) == 0 {
		return ""
	}
	n := len(parts)
	switch {
	case parts[0] == "buckets":
		switch n {
		case 1:
			switch method {
			case http.MethodGet:
				return "ListTableBuckets"
			case http.MethodPut, http.MethodPost:
				return "CreateTableBucket"
			}
		case 2:
			switch method {
			case http.MethodGet:
				return "GetTableBucket"
			case http.MethodDelete:
				return "DeleteTableBucket"
			}
		case 3:
			switch parts[2] {
			case "policy":
				switch method {
				case http.MethodGet:
					return "GetTableBucketPolicy"
				case http.MethodPut, http.MethodPost:
					return "PutTableBucketPolicy"
				case http.MethodDelete:
					return "DeleteTableBucketPolicy"
				}
			case "encryption":
				switch method {
				case http.MethodGet:
					return "GetTableBucketEncryption"
				case http.MethodPut, http.MethodPost:
					return "PutTableBucketEncryption"
				case http.MethodDelete:
					return "DeleteTableBucketEncryption"
				}
			case "maintenance":
				switch method {
				case http.MethodGet:
					return "GetTableBucketMaintenanceConfiguration"
				case http.MethodPut, http.MethodPost:
					return "PutTableBucketMaintenanceConfiguration"
				}
			}
		case 4:
			if parts[2] == "maintenance" {
				switch method {
				case http.MethodGet:
					return "GetTableBucketMaintenanceConfiguration"
				case http.MethodPut, http.MethodPost:
					return "PutTableBucketMaintenanceConfiguration"
				}
			}
		}
	case parts[0] == "namespaces":
		switch n {
		case 2:
			switch method {
			case http.MethodGet:
				return "ListNamespaces"
			case http.MethodPut, http.MethodPost:
				return "CreateNamespace"
			}
		case 3:
			switch method {
			case http.MethodGet:
				return "GetNamespace"
			case http.MethodPut, http.MethodPost:
				return "CreateNamespace"
			case http.MethodDelete:
				return "DeleteNamespace"
			}
		}
	case parts[0] == "tables":
		switch n {
		case 2:
			if method == http.MethodGet {
				return "ListTables"
			}
		case 3:
			if method == http.MethodPut || method == http.MethodPost {
				return "CreateTable"
			}
		case 4:
			switch parts[3] {
			case "rename":
				if method == http.MethodPut || method == http.MethodPost {
					return "RenameTable"
				}
			case "metadata-location":
				switch method {
				case http.MethodGet:
					return "GetTableMetadataLocation"
				case http.MethodPut, http.MethodPost:
					return "UpdateTableMetadataLocation"
				}
			case "policy":
				switch method {
				case http.MethodGet:
					return "GetTablePolicy"
				case http.MethodPut, http.MethodPost:
					return "PutTablePolicy"
				case http.MethodDelete:
					return "DeleteTablePolicy"
				}
			case "encryption":
				switch method {
				case http.MethodGet:
					return "GetTableEncryption"
				case http.MethodPut, http.MethodPost:
					return "PutTableEncryption"
				case http.MethodDelete:
					return "DeleteTableEncryption"
				}
			case "maintenance":
				switch method {
				case http.MethodGet:
					return "GetTableMaintenanceConfiguration"
				case http.MethodPut, http.MethodPost:
					return "PutTableMaintenanceConfiguration"
				}
			case "maintenance-job-status":
				if method == http.MethodGet {
					return "GetTableMaintenanceJobStatus"
				}
			default:
				if method == http.MethodDelete {
					return "DeleteTable"
				}
			}
		case 5:
			switch parts[4] {
			case "rename":
				if method == http.MethodPut || method == http.MethodPost {
					return "RenameTable"
				}
			case "metadata-location":
				switch method {
				case http.MethodGet:
					return "GetTableMetadataLocation"
				case http.MethodPut, http.MethodPost:
					return "UpdateTableMetadataLocation"
				}
			case "policy":
				switch method {
				case http.MethodGet:
					return "GetTablePolicy"
				case http.MethodPut, http.MethodPost:
					return "PutTablePolicy"
				case http.MethodDelete:
					return "DeleteTablePolicy"
				}
			case "encryption":
				switch method {
				case http.MethodGet:
					return "GetTableEncryption"
				case http.MethodPut, http.MethodPost:
					return "PutTableEncryption"
				case http.MethodDelete:
					return "DeleteTableEncryption"
				}
			case "maintenance":
				switch method {
				case http.MethodGet:
					return "GetTableMaintenanceConfiguration"
				case http.MethodPut, http.MethodPost:
					return "PutTableMaintenanceConfiguration"
				}
			case "maintenance-job-status":
				if method == http.MethodGet {
					return "GetTableMaintenanceJobStatus"
				}
			}
		case 6:
			if parts[4] == "maintenance" {
				switch method {
				case http.MethodGet:
					return "GetTableMaintenanceConfiguration"
				case http.MethodPut, http.MethodPost:
					return "PutTableMaintenanceConfiguration"
				}
			}
		}
	case parts[0] == "get-table":
		if method == http.MethodGet {
			return "GetTable"
		}
	}
	return ""
}

// s3PathParts parses a boto3-style S3Tables URL and returns the extracted
// bucket ARN (or name), namespace, table name, and subresource.
func s3PathParts(path string) (bucketARN, namespace, tableName, subresource string) {
	path = strings.TrimPrefix(path, "/v1")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && strings.Contains(parts[1], "arn:") {
		parts = collapseARN(parts, 1)
	}
	// For table URLs, also collapse ARN at position 3
	if len(parts) >= 4 && parts[0] == "tables" && strings.Contains(parts[3], "arn:") {
		parts = collapseARN(parts, 3)
	}
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "buckets":
		if len(parts) >= 2 {
			bucketARN = parts[1]
		}
	case "namespaces":
		if len(parts) >= 2 {
			bucketARN = parts[1]
		}
		if len(parts) >= 3 {
			namespace = parts[2]
		}
	case "tables":
		if len(parts) >= 2 {
			bucketARN = parts[1]
		}
		if len(parts) >= 3 {
			namespace = parts[2]
		}
		if len(parts) >= 4 {
			tableName = parts[3]
		}
		if len(parts) >= 5 {
			subresource = parts[4]
		}
	}
	return
}

func pathSegment(path, key string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func jsonError(code, message string, status int) *plugin.Response {
	body, _ := json.Marshal(map[string]string{"__type": code, "message": message})
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}
}

func jsonResponse(status int, v any) (*plugin.Response, error) {
	if v == nil {
		return &plugin.Response{StatusCode: status, ContentType: "application/json", Body: []byte("{}")}, nil
	}
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		ContentType: "application/json",
		Body:        body,
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("s3tables", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &S3TablesProvider{}
	})
}

// Ensure shared is used
var _ = shared.GenerateUUID
