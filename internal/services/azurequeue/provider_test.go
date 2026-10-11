// SPDX-License-Identifier: Apache-2.0

package azurequeue

import (
	"bytes"
	"context"
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
	dir, err := os.MkdirTemp("", "devcloud-queue-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	p := &Provider{}
	err = p.Init(plugin.PluginConfig{DataDir: filepath.Join(dir, "queue")})
	require.NoError(t, err)
	return p
}

func TestQueueProvider_Metadata(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "azurequeue", p.ServiceID())
	assert.Equal(t, "Azure Queue Storage", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTXML, p.Protocol())
	assert.Equal(t, "azure", plugin.ProviderOf(p))
}

func TestQueueProvider_QueueLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// 1. Create Queue
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/my-test-queue", nil)
	resp, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// 2. Duplicate create returns 204 No Content
	respDup, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDup.StatusCode)

	// 3. Get Queue Metadata
	reqMeta := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/my-test-queue?comp=metadata", nil)
	respMeta, err := p.HandleRequest(ctx, "", reqMeta)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respMeta.StatusCode)
	assert.Equal(t, "0", respMeta.Headers["x-ms-approximate-messages-count"])

	// 4. List Queues
	reqList := httptest.NewRequest(http.MethodGet, "/devstoreaccount1?comp=list", nil)
	respList, err := p.HandleRequest(ctx, "", reqList)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respList.StatusCode)
	assert.Contains(t, string(respList.Body), "my-test-queue")

	// 5. Delete Queue
	reqDel := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/my-test-queue", nil)
	respDel, err := p.HandleRequest(ctx, "", reqDel)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respDel.StatusCode)

	// 6. Accessing deleted queue returns 404
	respGet404, err := p.HandleRequest(ctx, "", reqMeta)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, respGet404.StatusCode)
}

func TestQueueProvider_MessageLifecycle(t *testing.T) {
	p := newTestProvider(t)
	ctx := context.Background()

	// Create queue
	reqCreate := httptest.NewRequest(http.MethodPut, "/devstoreaccount1/msg-queue", nil)
	_, err := p.HandleRequest(ctx, "", reqCreate)
	require.NoError(t, err)

	// 1. Put Message
	msgXML := `<QueueMessage><MessageText>hello world</MessageText></QueueMessage>`
	reqPut := httptest.NewRequest(http.MethodPost, "/devstoreaccount1/msg-queue/messages", bytes.NewReader([]byte(msgXML)))
	respPut, err := p.HandleRequest(ctx, "", reqPut)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, respPut.StatusCode)
	assert.Contains(t, string(respPut.Body), "<MessageId>")
	assert.Contains(t, string(respPut.Body), "<PopReceipt>")

	// 2. Peek Message
	reqPeek := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/msg-queue/messages?peekonly=true", nil)
	respPeek, err := p.HandleRequest(ctx, "", reqPeek)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respPeek.StatusCode)
	assert.Contains(t, string(respPeek.Body), "hello world")

	// 3. Get Messages
	reqGet := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/msg-queue/messages", nil)
	respGet, err := p.HandleRequest(ctx, "", reqGet)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGet.StatusCode)
	assert.Contains(t, string(respGet.Body), "hello world")
	assert.Contains(t, string(respGet.Body), "<DequeueCount>1</DequeueCount>")

	// 4. Clear Messages
	reqClear := httptest.NewRequest(http.MethodDelete, "/devstoreaccount1/msg-queue/messages", nil)
	respClear, err := p.HandleRequest(ctx, "", reqClear)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, respClear.StatusCode)

	// 5. Peek after clear returns empty list
	respPeekEmpty, err := p.HandleRequest(ctx, "", reqPeek)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respPeekEmpty.StatusCode)
	assert.NotContains(t, string(respPeekEmpty.Body), "hello world")
}
