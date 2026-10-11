// SPDX-License-Identifier: Apache-2.0

package cosmosdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/auth"
	generated "github.com/skyoo2003/devcloud/internal/generated/cosmosdb"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *Store
}

func (p *Provider) ServiceID() string             { return "cosmosdb" }
func (p *Provider) ServiceName() string           { return "Azure Cosmos DB" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "azure" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/cosmos"
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
	dbs, err := p.store.ListDatabases("devstoreaccount1")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, d := range dbs {
		res = append(res, plugin.Resource{
			Type: "Database",
			ID:   d.ID,
			Name: d.ID,
		})
	}
	return res, nil
}

func randomActivityID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (p *Provider) reply(req *http.Request, status int, headers map[string]string, body []byte) *plugin.Response {
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["x-ms-activity-id"] = randomActivityID()
	headers["x-ms-request-charge"] = "1"
	headers["Date"] = time.Now().UTC().Format(http.TimeFormat)

	ct := headers["Content-Type"]
	if ct == "" {
		ct = "application/json"
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
		"x-ms-activity-id": randomActivityID(),
		"x-ms-error-code":  code,
		"Content-Type":     "application/json",
		"Date":             time.Now().UTC().Format(http.TimeFormat),
	}
	type cosmosErr struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	raw, _ := json.Marshal(cosmosErr{Code: code, Message: message})
	return &plugin.Response{
		StatusCode:  status,
		Headers:     headers,
		Body:        raw,
		ContentType: "application/json",
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

	op, params := generated.MatchOperation(req.Method, uri)
	if op == "" {
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
	}

	switch op {
	case "ListDatabases":
		return p.handleListDatabases(req, account)
	case "CreateDatabase":
		return p.handleCreateDatabase(req, account)
	case "GetDatabase":
		return p.handleGetDatabase(req, account, params["db"])
	case "DeleteDatabase":
		return p.handleDeleteDatabase(req, account, params["db"])
	case "ListContainers":
		return p.handleListContainers(req, account, params["db"])
	case "CreateContainer":
		return p.handleCreateContainer(req, account, params["db"])
	case "GetContainer":
		return p.handleGetContainer(req, account, params["db"], params["coll"])
	case "DeleteContainer":
		return p.handleDeleteContainer(req, account, params["db"], params["coll"])
	case "ListDocuments":
		return p.handleListDocuments(req, account, params["db"], params["coll"])
	case "CreateDocument":
		return p.handleCreateDocument(req, account, params["db"], params["coll"])
	case "GetDocument":
		return p.handleGetDocument(req, account, params["db"], params["coll"], params["doc"])
	case "ReplaceDocument":
		return p.handleReplaceDocument(req, account, params["db"], params["coll"], params["doc"])
	case "DeleteDocument":
		return p.handleDeleteDocument(req, account, params["db"], params["coll"], params["doc"])
	default:
		return p.errorReply(req, http.StatusBadRequest, "InvalidAction", "operation not handled: "+req.Method+" "+req.URL.Path), nil
	}
}

func (p *Provider) handleListDatabases(req *http.Request, account string) (*plugin.Response, error) {
	dbs, err := p.store.ListDatabases(account)
	if err != nil {
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	type listDbsResponse struct {
		RID       string         `json:"_rid"`
		Databases []DatabaseInfo `json:"Databases"`
		Count     int            `json:"_count"`
	}
	raw, _ := json.Marshal(listDbsResponse{
		Databases: dbs,
		Count:     len(dbs),
	})
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleCreateDatabase(req *http.Request, account string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "failed to read body"), nil
	}
	var reqBody struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "invalid json: "+err.Error()), nil
	}

	db, err := p.store.CreateDatabase(account, reqBody.ID)
	if err != nil {
		if errors.Is(err, ErrDatabaseAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "Conflict", "database already exists"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(db)
	return p.reply(req, http.StatusCreated, nil, raw), nil
}

func (p *Provider) handleGetDatabase(req *http.Request, account, dbID string) (*plugin.Response, error) {
	db, err := p.store.GetDatabase(account, dbID)
	if err != nil {
		if errors.Is(err, ErrDatabaseNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "database not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	raw, _ := json.Marshal(db)
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleDeleteDatabase(req *http.Request, account, dbID string) (*plugin.Response, error) {
	err := p.store.DeleteDatabase(account, dbID)
	if err != nil {
		if errors.Is(err, ErrDatabaseNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "database not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleListContainers(req *http.Request, account, dbID string) (*plugin.Response, error) {
	colls, err := p.store.ListContainers(account, dbID)
	if err != nil {
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	type listCollsResponse struct {
		RID        string          `json:"_rid"`
		Containers []ContainerInfo `json:"DocumentCollections"`
		Count      int             `json:"_count"`
	}
	raw, _ := json.Marshal(listCollsResponse{
		Containers: colls,
		Count:      len(colls),
	})
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleCreateContainer(req *http.Request, account, dbID string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "failed to read body"), nil
	}
	var reqBody struct {
		ID           string `json:"id"`
		PartitionKey any    `json:"partitionKey"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "invalid json: "+err.Error()), nil
	}

	coll, err := p.store.CreateContainer(account, dbID, reqBody.ID, reqBody.PartitionKey)
	if err != nil {
		if errors.Is(err, ErrDatabaseNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "database not found"), nil
		}
		if errors.Is(err, ErrContainerAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "Conflict", "container already exists"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(coll)
	return p.reply(req, http.StatusCreated, nil, raw), nil
}

func (p *Provider) handleGetContainer(req *http.Request, account, dbID, collID string) (*plugin.Response, error) {
	coll, err := p.store.GetContainer(account, dbID, collID)
	if err != nil {
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "container not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	raw, _ := json.Marshal(coll)
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleDeleteContainer(req *http.Request, account, dbID, collID string) (*plugin.Response, error) {
	err := p.store.DeleteContainer(account, dbID, collID)
	if err != nil {
		if errors.Is(err, ErrContainerNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "container not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func (p *Provider) handleListDocuments(req *http.Request, account, dbID, collID string) (*plugin.Response, error) {
	docs, err := p.store.ListDocuments(account, dbID, collID)
	if err != nil {
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	type listDocsResponse struct {
		RID       string     `json:"_rid"`
		Documents []Document `json:"Documents"`
		Count     int        `json:"_count"`
	}
	raw, _ := json.Marshal(listDocsResponse{
		Documents: docs,
		Count:     len(docs),
	})
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleCreateDocument(req *http.Request, account, dbID, collID string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "failed to read body"), nil
	}
	var doc Document
	if err := json.Unmarshal(body, &doc); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "invalid json: "+err.Error()), nil
	}

	created, err := p.store.CreateDocument(account, dbID, collID, doc)
	if err != nil {
		if errors.Is(err, ErrDocumentAlreadyExists) {
			return p.errorReply(req, http.StatusConflict, "Conflict", "document already exists"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(created)
	return p.reply(req, http.StatusCreated, nil, raw), nil
}

func (p *Provider) handleGetDocument(req *http.Request, account, dbID, collID, docID string) (*plugin.Response, error) {
	doc, err := p.store.GetDocument(account, dbID, collID, docID)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "document not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	raw, _ := json.Marshal(doc)
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleReplaceDocument(req *http.Request, account, dbID, collID, docID string) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "failed to read body"), nil
	}
	var doc Document
	if err := json.Unmarshal(body, &doc); err != nil {
		return p.errorReply(req, http.StatusBadRequest, "BadRequest", "invalid json: "+err.Error()), nil
	}

	replaced, err := p.store.ReplaceDocument(account, dbID, collID, docID, doc)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "document not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}

	raw, _ := json.Marshal(replaced)
	return p.reply(req, http.StatusOK, nil, raw), nil
}

func (p *Provider) handleDeleteDocument(req *http.Request, account, dbID, collID, docID string) (*plugin.Response, error) {
	err := p.store.DeleteDocument(account, dbID, collID, docID)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return p.errorReply(req, http.StatusNotFound, "NotFound", "document not found"), nil
		}
		return p.errorReply(req, http.StatusInternalServerError, "InternalError", err.Error()), nil
	}
	return p.reply(req, http.StatusNoContent, nil, nil), nil
}

func init() {
	plugin.DefaultRegistry.Register("cosmosdb", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
