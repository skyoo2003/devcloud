// SPDX-License-Identifier: Apache-2.0

package azurekeyvault

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureKeyVault_Lifecycle(t *testing.T) {
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{})
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Set Secret
	setBody := `{"value":"p@ssword123","contentType":"text/plain","tags":{"env":"prod"}}`
	req := httptest.NewRequest("PUT", "/secrets/my-secret?api-version=7.4", bytes.NewReader([]byte(setBody)))
	req.Host = "testvault.vault.azure.net"
	resp, err := p.HandleRequest(ctx, "SetSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var bundle SecretBundle
	err = json.Unmarshal(resp.Body, &bundle)
	require.NoError(t, err)
	assert.Equal(t, "p@ssword123", bundle.Value)
	assert.Contains(t, bundle.ID, "https://testvault.vault.azure.net/secrets/my-secret/")

	// 2. Get Secret (Latest)
	req = httptest.NewRequest("GET", "/secrets/my-secret?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "GetSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	err = json.Unmarshal(resp.Body, &bundle)
	require.NoError(t, err)
	assert.Equal(t, "p@ssword123", bundle.Value)

	// 3. List Secrets
	req = httptest.NewRequest("GET", "/secrets?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "ListSecrets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var listResp struct {
		Value []SecretItem `json:"value"`
	}
	err = json.Unmarshal(resp.Body, &listResp)
	require.NoError(t, err)
	assert.Len(t, listResp.Value, 1)

	// 4. Create Key
	keyReqBody := `{"kty":"RSA","key_size":2048}`
	req = httptest.NewRequest("POST", "/keys/my-rsa-key/create?api-version=7.4", bytes.NewReader([]byte(keyReqBody)))
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "CreateKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var keyBundle KeyBundle
	err = json.Unmarshal(resp.Body, &keyBundle)
	require.NoError(t, err)
	assert.Equal(t, "RSA", keyBundle.Key.Kty)
	assert.Contains(t, keyBundle.Key.Kid, "https://testvault.vault.azure.net/keys/my-rsa-key/")

	// 5. Get Key
	req = httptest.NewRequest("GET", "/keys/my-rsa-key?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "GetKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 6. Delete Secret
	req = httptest.NewRequest("DELETE", "/secrets/my-secret?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "DeleteSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 7. Get Deleted Secret -> 404
	req = httptest.NewRequest("GET", "/secrets/my-secret?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "GetSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// 8. Delete Key
	req = httptest.NewRequest("DELETE", "/keys/my-rsa-key?api-version=7.4", nil)
	req.Host = "testvault.vault.azure.net"
	resp, err = p.HandleRequest(ctx, "DeleteKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
