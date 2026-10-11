// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGCPAuth_BearerToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost:4747/storage/v1/b", nil)
	req.Header.Set("Authorization", "Bearer ya29.test-token-12345")

	id, ok := (GCPAuth{}).Authenticate(req)
	assert.True(t, ok)
	assert.Equal(t, "gcp", id.Provider)
	assert.Equal(t, "ya29.test-token-12345", id.AccessKeyID)
	assert.Equal(t, "gcs", id.Service)
}

func TestGCPAuth_HeadersWithoutToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost:4747/storage/v1/b/my-bucket", nil)
	req.Header.Set("x-goog-api-client", "gl-python/3.13.0")

	id, ok := (GCPAuth{}).Authenticate(req)
	assert.True(t, ok)
	assert.Equal(t, "gcp", id.Provider)
	assert.Equal(t, "devcloud-gcp-sa", id.AccessKeyID)
	assert.Equal(t, "gcs", id.Service)
}

func TestGCPAuth_UnrelatedRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost:4747/my-bucket", nil)

	_, ok := (GCPAuth{}).Authenticate(req)
	assert.False(t, ok)
}
