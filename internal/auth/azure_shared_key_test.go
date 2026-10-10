// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAzureSharedKey_Authenticate(t *testing.T) {
	adapter := AzureSharedKey{}

	t.Run("SharedKey header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mycontainer", nil)
		req.Header.Set("Authorization", "SharedKey devstoreaccount1:abcdef==")
		id, ok := adapter.Authenticate(req)
		assert.True(t, ok)
		assert.Equal(t, "azure", id.Provider)
		assert.Equal(t, "devstoreaccount1", id.AccessKeyID)
		assert.Equal(t, "blob", id.Service)
	})

	t.Run("SharedKeyLite header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mycontainer", nil)
		req.Header.Set("Authorization", "SharedKeyLite myaccount:secret123==")
		id, ok := adapter.Authenticate(req)
		assert.True(t, ok)
		assert.Equal(t, "azure", id.Provider)
		assert.Equal(t, "myaccount", id.AccessKeyID)
	})

	t.Run("SAS query parameters", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/myaccount/mycontainer?sig=sig123&se=2026-10-10", nil)
		id, ok := adapter.Authenticate(req)
		assert.True(t, ok)
		assert.Equal(t, "azure", id.Provider)
		assert.Equal(t, "myaccount", id.AccessKeyID)
	})

	t.Run("x-ms-version header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/devstoreaccount1/mycontainer", nil)
		req.Header.Set("x-ms-version", "2023-11-03")
		id, ok := adapter.Authenticate(req)
		assert.True(t, ok)
		assert.Equal(t, "azure", id.Provider)
		assert.Equal(t, "devstoreaccount1", id.AccessKeyID)
	})

	t.Run("unrecognized request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
		_, ok := adapter.Authenticate(req)
		assert.False(t, ok)
	})
}
