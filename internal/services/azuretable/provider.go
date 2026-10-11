// SPDX-License-Identifier: Apache-2.0

package azuretable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/auth"
	generated "github.com/skyoo2003/devcloud/internal/generated/azuretable"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *Store
}

func (p *Provider) ServiceID() string             { return "azuretable" }
func (p *Provider) ServiceName() string           { return "Azure Table Storage" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "azure" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/azure-table"
	}
	s, err := NewStore(p.dataDir)
	if err != nil {
		return err
	}
	p.store = s
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	tables, err := p.store.ListTables("devstoreaccount1")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, t := range tables {
		res = append(res, plugin.Resource{
			Type: "Table",
			ID:   t.TableName,
			Name: t.TableName,
		})
	}
	return res, nil
}

func randomRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (p *Provider) reply(req *http.Request, status int, headers map[string]string, body []byte) *plugin.Response {
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["x-ms-request-id"] = randomRequestID()
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version
	headers["Date"] = time.Now().UTC().Format(http.TimeFormat)

	ct := headers["Content-Type"]
	if ct == "" {
		ct = "application/json;odata=nometadata"
	}
	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        body,
		ContentType: ct,
	}
}

func (p *Provider) errorReply(req *http.Request, status int, code, message string) *plugin.Response {
	headers := map[string]string{
		"x-ms-error-code": code,
		"Content-Type":    "application/json;odata=minimalmetadata",
		"x-ms-request-id": randomRequestID(),
		"Date":            time.Now().UTC().Format(http.TimeFormat),
	}
	version := req.Header.Get("x-ms-version")
	if version == "" {
		version = "2023-11-03"
	}
	headers["x-ms-version"] = version

	type odataErr struct {
		Error struct {
			Code    string `json:"code"`
			Message struct {
				Lang  string `json:"lang"`
				Value string `json:"value"`
			} `json:"message"`
		} `json:"odata.error"`
	}
	var errObj odataErr
	errObj.Error.Code = code
	errObj.Error.Message.Lang = "en-US"
	errObj.Error.Message.Value = message
	raw, _ := json.Marshal(errObj)

	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        raw,
		ContentType: "application/json;odata=minimalmetadata",
	}
}

func (p *Provider) HandleRequest(ctx context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	account := "devstoreaccount1"
	if id, ok := auth.FromContext(ctx); ok && id.AccessKeyID != "" {
		account = id.AccessKeyID
	}

	uri := req.URL.RequestURI()
	for _, accPrefix := range []string{"/" + account, "/devstoreaccount1"} {
		if strings.HasPrefix(uri, accPrefix+"/") {
			uri = uri[len(accPrefix):]
			break
		} else if strings.HasPrefix(uri, accPrefix+"?") {
			uri = "/" + uri[len(accPrefix):]
			break
		} else if uri == accPrefix {
			uri = "/"
			break
		}
	}

	op, params := matchTableOperation(req.Method, uri)
	if op == "" {
		op, params = generated.MatchOperation(req.Method, uri)
	}
	if op == "" {
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
	}

	switch op {
	case "QueryTables":
		return p.handleQueryTables(req, account)
	case "CreateTable":
		return p.handleCreateTable(req, account)
	case "DeleteTable":
		return p.handleDeleteTable(req, account, params["table"])
	case "QueryEntities":
		return p.handleQueryEntities(req, account, params["table"])
	case "InsertEntity":
		return p.handleInsertEntity(req, account, params["table"])
	case "GetEntity":
		return p.handleGetEntity(req, account, params["table"], params["partitionKey"], params["rowKey"])
	case "UpdateEntity":
		return p.handleUpdateEntity(req, account, params["table"], params["partitionKey"], params["rowKey"], false)
	case "MergeEntity":
		return p.handleUpdateEntity(req, account, params["table"], params["partitionKey"], params["rowKey"], true)
	case "DeleteEntity":
		return p.handleDeleteEntity(req, account, params["table"], params["partitionKey"], params["rowKey"])
	default:
		return p.errorReply(req, http.StatusBadRequest, "InvalidAction", "operation not handled: "+req.Method+" "+req.URL.Path), nil
	}
}

func (p *Provider) handleQueryTables(req *http.Request, account string) (*plugin.Response, error) {
	tables, err := p.store.ListTables(account)
	if err != nil {
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	type queryTablesResponse struct {
		Value []TableInfo `json:"value"`
	}
	raw, _ := json.Marshal(queryTablesResponse{Value: tables})
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleCreateTable(req *http.Request, account string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "failed to read body"), nil
	}
	var reqBody TableInfo
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "invalid json: "+err.Error()), nil
	}

	err = p.store.CreateTable(account, reqBody.TableName)
	if err != nil {
		if errors.Is(err, ErrTableAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "TableAlreadyExists", "table already exists"), nil
		}
		if errors.Is(err, ErrInvalidTableName) {
			return p.errorReply(req, http.StatusBadRequest, "InvalidResourceName", "invalid table name"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(reqBody)
	return p.reply(req, http.StatusCreated, nil, raw), nil
}

func (p *Provider) handleDeleteTable(req *http.Request, account, tableName string) (*plugin.Response, error) {
	err := p.store.DeleteTable(account, tableName)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "TableNotFound", "table not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleQueryEntities(req *http.Request, account, tableName string) (*plugin.Response, error) {
	entities, err := p.store.QueryEntities(account, tableName)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "TableNotFound", "table not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	type queryEntitiesResponse struct {
		Value []Entity `json:"value"`
	}
	raw, _ := json.Marshal(queryEntitiesResponse{Value: entities})
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleInsertEntity(req *http.Request, account, tableName string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "failed to read body"), nil
	}
	var entity Entity
	if err := json.Unmarshal(body, &entity); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "invalid entity JSON: "+err.Error()), nil
	}

	err = p.store.InsertEntity(account, tableName, entity)
	if err != nil {
		if errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "TableNotFound", "table not found"), nil
		}
		if errors.Is(err, ErrEntityAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "EntityAlreadyExists", "entity already exists"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(entity)
	headers := map[string]string{
		"ETag": fmt.Sprintf("%v", entity["odata.etag"]),
	}
	return p.reply(req, http.StatusCreated, headers, raw), nil
}

func (p *Provider) handleGetEntity(req *http.Request, account, tableName, pk, rk string) (*plugin.Response, error) {
	entity, err := p.store.GetEntity(account, tableName, pk, rk)
	if err != nil {
		if errors.Is(err, ErrEntityNotFound) || errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ResourceNotFound", "entity not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	raw, _ := json.Marshal(entity)
	headers := map[string]string{
		"ETag": fmt.Sprintf("%v", entity["odata.etag"]),
	}
	return p.reply(req, http.StatusOK, headers, raw), nil
}

func (p *Provider) handleUpdateEntity(req *http.Request, account, tableName, pk, rk string, merge bool) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "failed to read body"), nil
	}
	var entity Entity
	if err := json.Unmarshal(body, &entity); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "InvalidInput", "invalid entity JSON: "+err.Error()), nil
	}

	err = p.store.UpdateEntity(account, tableName, pk, rk, entity, merge)
	if err != nil {
		if errors.Is(err, ErrEntityNotFound) || errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ResourceNotFound", "entity not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleDeleteEntity(req *http.Request, account, tableName, pk, rk string) (*plugin.Response, error) {
	err := p.store.DeleteEntity(account, tableName, pk, rk)
	if err != nil {
		if errors.Is(err, ErrEntityNotFound) || errors.Is(err, ErrTableNotFound) {
			return p.errorReply(req, http.StatusNotFound, "ResourceNotFound", "entity not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func matchTableOperation(method, uri string) (string, map[string]string) {
	cleanURI, _, _ := strings.Cut(uri, "?")
	cleanURI = strings.TrimPrefix(cleanURI, "/")

	if cleanURI == "Tables" {
		if method == http.MethodGet {
			return "QueryTables", nil
		}
		if method == http.MethodPost {
			return "CreateTable", nil
		}
	}

	if strings.HasPrefix(cleanURI, "Tables('") && strings.HasSuffix(cleanURI, "')") {
		tableName := strings.TrimSuffix(strings.TrimPrefix(cleanURI, "Tables('"), "')")
		if method == http.MethodDelete {
			return "DeleteTable", map[string]string{"table": tableName}
		}
	}

	if idx := strings.Index(cleanURI, "("); idx != -1 && strings.HasSuffix(cleanURI, ")") {
		tableName := cleanURI[:idx]
		inner := cleanURI[idx+1 : len(cleanURI)-1]
		if inner == "" {
			if method == http.MethodGet {
				return "QueryEntities", map[string]string{"table": tableName}
			}
		}
		pk, rk := parseTableKeys(inner)
		params := map[string]string{"table": tableName, "partitionKey": pk, "rowKey": rk}
		switch method {
		case http.MethodGet:
			return "GetEntity", params
		case http.MethodPut:
			return "UpdateEntity", params
		case "MERGE":
			return "MergeEntity", params
		case http.MethodDelete:
			return "DeleteEntity", params
		}
	}

	if !strings.Contains(cleanURI, "(") && !strings.Contains(cleanURI, ")") && cleanURI != "" && cleanURI != "Tables" {
		if method == http.MethodPost {
			return "InsertEntity", map[string]string{"table": cleanURI}
		}
		if method == http.MethodGet {
			return "QueryEntities", map[string]string{"table": cleanURI}
		}
	}

	return "", nil
}

func parseTableKeys(s string) (pk string, rk string) {
	parts := strings.Split(s, ",")
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if strings.HasPrefix(trimmed, "PartitionKey='") && strings.HasSuffix(trimmed, "'") {
			pk = strings.TrimSuffix(strings.TrimPrefix(trimmed, "PartitionKey='"), "'")
		} else if strings.HasPrefix(trimmed, "RowKey='") && strings.HasSuffix(trimmed, "'") {
			rk = strings.TrimSuffix(strings.TrimPrefix(trimmed, "RowKey='"), "'")
		}
	}
	return pk, rk
}

func init() {
	plugin.DefaultRegistry.Register("azuretable", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
