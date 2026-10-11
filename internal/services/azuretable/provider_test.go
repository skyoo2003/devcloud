// SPDX-License-Identifier: Apache-2.0

package azuretable

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
	dir, err := os.MkdirTemp("", "devcloud-table-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	err = p.Init(plugin.PluginConfig{DataDir: filepath.Join(dir, "table")})
	require.NoError(t, err)
	return p
}

func TestTableProvider_Metadata(t *testing.T) {
	p := newTestProvider(t)
	defer func() { _ = p.Shutdown(context.Background()) }()
	assert.Equal(t, "azuretable", p.ServiceID())
	assert.Equal(t, "Azure Table Storage", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())
	assert.Equal(t, "azure", plugin.ProviderOf(p))
}

func TestTableProvider_TableLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	defer func() { _ = p.Shutdown(ctx) }()

	// 1. Create Table
	reqCreate := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/Tables", bytes.NewReader([]byte(`{"TableName": "Users"}`)))
	reqCreate.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// 2. Duplicate Create returns 409
	reqDup := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/Tables", bytes.NewReader([]byte(`{"TableName": "Users"}`)))
	reqDup.Header.Set("Content-Type", "application/json")
	respDup, err := p.HandleRequest(ctx, "", reqDup)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, respDup.StatusCode)

	// 3. Query Tables
	reqQuery := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/Tables", nil)
	respQuery, err := p.HandleRequest(ctx, "", reqQuery)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respQuery.StatusCode)
	assert.Contains(t, string(respQuery.Body), "Users")

	// 4. Delete Table
	reqDel := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/Tables('Users')", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 5. Delete again returns 404
	respDel404, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respDel404.StatusCode)
}

func TestTableProvider_EntityCRUD(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()
	defer func() { _ = p.Shutdown(ctx) }()

	// Create table
	reqCreateT := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/Tables", bytes.NewReader([]byte(`{"TableName": "Customers"}`)))
	_, err := p.HandleRequest(ctx, "", reqCreateT)
	require.NoError(t, err)

	// 1. Insert Entity
	entity := map[string]any{
		"PartitionKey": "Seattle",
		"RowKey":       "user-001",
		"Email":        "alice@example.com",
		"Age":          30,
	}
	body, _ := json.Marshal(entity)
	reqInsert := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/Customers", bytes.NewReader(body))
	respInsert, err := p.HandleRequest(ctx, "", reqInsert)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respInsert.StatusCode)
	assert.NotEmpty(t, respInsert.Headers["ETag"])

	// 2. Get Entity
	reqGet := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/Customers(PartitionKey='Seattle',RowKey='user-001')", nil)
	respGet, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGet.StatusCode)
	assert.Contains(t, string(respGet.Body), "alice@example.com")

	// 3. Update Entity
	updateEntity := map[string]any{
		"PartitionKey": "Seattle",
		"RowKey":       "user-001",
		"Email":        "alice-updated@example.com",
	}
	upBody, _ := json.Marshal(updateEntity)
	reqUpdate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/Customers(PartitionKey='Seattle',RowKey='user-001')", bytes.NewReader(upBody))
	respUpdate, err := p.HandleRequest(ctx, "", reqUpdate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respUpdate.StatusCode)

	// Verify updated entity
	respGet2, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Contains(t, string(respGet2.Body), "alice-updated@example.com")

	// 4. Query Entities
	reqQuery := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/Customers()", nil)
	respQuery, err := p.HandleRequest(ctx, "", reqQuery)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respQuery.StatusCode)
	assert.Contains(t, string(respQuery.Body), "alice-updated@example.com")

	// 5. Delete Entity
	reqDel := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/Customers(PartitionKey='Seattle',RowKey='user-001')", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 6. Get deleted entity returns 404
	respGet404, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGet404.StatusCode)
}
