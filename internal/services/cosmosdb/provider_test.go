// SPDX-License-Identifier: Apache-2.0

package cosmosdb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	dir, err := os.MkdirTemp("", "devcloud-cosmos-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	err = p.Init(plugin.PluginConfig{DataDir: filepath.Join(dir, "cosmos")})
	require.NoError(t, err)
	return p
}

func TestCosmosProvider_Metadata(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	assert.Equal(t, "cosmosdb", p.ServiceID())
	assert.Equal(t, "Azure Cosmos DB", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())
	assert.Equal(t, "azure", plugin.ProviderOf(p))
}

func TestCosmosProvider_DatabaseLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	defer func() { _ = p.Shutdown(ctx) }()

	// 1. Create DB
	reqCreate := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs", bytes.NewReader([]byte(`{"id": "mydb"}`)))
	reqCreate.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// 2. Duplicate DB returns 409
	reqDup := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs", bytes.NewReader([]byte(`{"id": "mydb"}`)))
	respDup, err := p.HandleRequest(ctx, "", reqDup)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respDup.StatusCode)

	// 3. Get DB
	reqGet := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs/mydb", nil)
	respGet, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGet.StatusCode)
	assert.Contains(t, string(respGet.Body), `"id":"mydb"`)

	// 4. List DBs
	reqList := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs", nil)
	respList, err := p.HandleRequest(ctx, "", reqList)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respList.StatusCode)
	assert.Contains(t, string(respList.Body), `"id":"mydb"`)

	// 5. Delete DB
	reqDel := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/dbs/mydb", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 6. Get deleted DB returns 404
	respGet404, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGet404.StatusCode)
}

func TestCosmosProvider_ContainerAndDocumentCRUD(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	defer func() { _ = p.Shutdown(ctx) }()

	// 1. Setup DB
	reqCreateDB := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs", bytes.NewReader([]byte(`{"id": "testdb"}`)))
	_, err := p.HandleRequest(ctx, "", reqCreateDB)
	require.NoError(t, err)

	// 2. Create Container
	reqCreateColl := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs/testdb/colls", bytes.NewReader([]byte(`{"id": "users", "partitionKey": {"paths": ["/tenantId"]}}`)))
	respColl, err := p.HandleRequest(ctx, "", reqCreateColl)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respColl.StatusCode)
	assert.Contains(t, string(respColl.Body), `"id":"users"`)

	// Duplicate container returns 409
	reqCollDup := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs/testdb/colls", bytes.NewReader([]byte(`{"id": "users", "partitionKey": {"paths": ["/tenantId"]}}`)))
	respCollDup, err := p.HandleRequest(ctx, "", reqCollDup)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respCollDup.StatusCode)

	// 3. Get Container
	reqGetColl := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs/testdb/colls/users", nil)
	respGetColl, err := p.HandleRequest(ctx, "", reqGetColl)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGetColl.StatusCode)

	// 4. List Containers
	reqListColls := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs/testdb/colls", nil)
	respListColls, err := p.HandleRequest(ctx, "", reqListColls)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respListColls.StatusCode)
	assert.Contains(t, string(respListColls.Body), `"users"`)

	// 5. Create Document
	docData := map[string]any{
		"id":       "doc-100",
		"name":     "Ada Lovelace",
		"tenantId": "t-1",
		"active":   true,
	}
	body, _ := json.Marshal(docData)
	reqCreateDoc := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/dbs/testdb/colls/users/docs", bytes.NewReader(body))
	respDoc, err := p.HandleRequest(ctx, "", reqCreateDoc)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respDoc.StatusCode)
	assert.Contains(t, string(respDoc.Body), `"doc-100"`)
	assert.Contains(t, string(respDoc.Body), `_rid`)

	// 6. Get Document
	reqGetDoc := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs/testdb/colls/users/docs/doc-100", nil)
	respGetDoc, err := p.HandleRequest(ctx, "", reqGetDoc)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGetDoc.StatusCode)
	assert.Contains(t, string(respGetDoc.Body), "Ada Lovelace")

	// 7. Replace Document
	docData["name"] = "Countess Ada Lovelace"
	bodyUp, _ := json.Marshal(docData)
	reqReplaceDoc := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/dbs/testdb/colls/users/docs/doc-100", bytes.NewReader(bodyUp))
	respReplaceDoc, err := p.HandleRequest(ctx, "", reqReplaceDoc)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respReplaceDoc.StatusCode)
	assert.Contains(t, string(respReplaceDoc.Body), "Countess Ada Lovelace")

	// 8. List Documents
	reqListDocs := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/dbs/testdb/colls/users/docs", nil)
	respListDocs, err := p.HandleRequest(ctx, "", reqListDocs)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respListDocs.StatusCode)
	assert.Contains(t, string(respListDocs.Body), "Countess Ada Lovelace")

	// 9. Delete Document
	reqDelDoc := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/dbs/testdb/colls/users/docs/doc-100", nil)
	respDelDoc, err := p.HandleRequest(ctx, "", reqDelDoc)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDelDoc.StatusCode)

	// 10. Delete Container
	reqDelColl := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/dbs/testdb/colls/users", nil)
	respDelColl, err := p.HandleRequest(ctx, "", reqDelColl)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDelColl.StatusCode)
}
