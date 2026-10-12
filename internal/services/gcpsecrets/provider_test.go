// SPDX-License-Identifier: Apache-2.0

package gcpsecrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCPSecrets_Lifecycle(t *testing.T) {
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)

	ctx := context.Background()

	// 1. Create Secret
	reqBody := `{"labels":{"env":"test"}}`
	req := httptest.NewRequest("POST", "/v1/projects/my-proj/secrets?secretId=db-password", bytes.NewReader([]byte(reqBody)))
	resp, err := p.HandleRequest(ctx, "CreateSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var sec Secret
	err = json.Unmarshal(resp.Body, &sec)
	require.NoError(t, err)
	assert.Equal(t, "projects/my-proj/secrets/db-password", sec.Name)

	// 2. Duplicate Create -> 409
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/secrets?secretId=db-password", bytes.NewReader([]byte(reqBody)))
	resp, err = p.HandleRequest(ctx, "CreateSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	// 3. Get Secret
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets/db-password", nil)
	resp, err = p.HandleRequest(ctx, "GetSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 4. Add Secret Version
	secretPayload := "super-secret-pass-123"
	b64Payload := base64.StdEncoding.EncodeToString([]byte(secretPayload))
	versionReqBody := fmtPayload(b64Payload)
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/secrets/db-password:addVersion", bytes.NewReader([]byte(versionReqBody)))
	resp, err = p.HandleRequest(ctx, "AddSecretVersion", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var v SecretVersion
	err = json.Unmarshal(resp.Body, &v)
	require.NoError(t, err)
	assert.Equal(t, "projects/my-proj/secrets/db-password/versions/1", v.Name)

	// 5. Access Secret Version (by number and by "latest")
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets/db-password/versions/1:access", nil)
	resp, err = p.HandleRequest(ctx, "AccessSecretVersion", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var accessResp struct {
		Payload struct {
			Data string `json:"data"`
		} `json:"payload"`
	}
	err = json.Unmarshal(resp.Body, &accessResp)
	require.NoError(t, err)
	decoded, _ := base64.StdEncoding.DecodeString(accessResp.Payload.Data)
	assert.Equal(t, secretPayload, string(decoded))

	// latest
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets/db-password/versions/latest:access", nil)
	resp, err = p.HandleRequest(ctx, "AccessSecretVersion", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 6. List Secrets
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets", nil)
	resp, err = p.HandleRequest(ctx, "ListSecrets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 7. Destroy Version
	req = httptest.NewRequest("POST", "/v1/projects/my-proj/secrets/db-password/versions/1:destroy", nil)
	resp, err = p.HandleRequest(ctx, "DestroySecretVersion", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 8. Access Destroyed Version -> 400
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets/db-password/versions/1:access", nil)
	resp, err = p.HandleRequest(ctx, "AccessSecretVersion", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// 9. Delete Secret
	req = httptest.NewRequest("DELETE", "/v1/projects/my-proj/secrets/db-password", nil)
	resp, err = p.HandleRequest(ctx, "DeleteSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 10. Get Deleted Secret -> 404
	req = httptest.NewRequest("GET", "/v1/projects/my-proj/secrets/db-password", nil)
	resp, err = p.HandleRequest(ctx, "GetSecret", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func fmtPayload(data string) string {
	return `{"payload":{"data":"` + data + `"}}`
}
