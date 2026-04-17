// SPDX-License-Identifier: Apache-2.0

package cognitoidentity

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

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func ciReq(t *testing.T, p *Provider, op string, body any) *plugin.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AmazonCognitoIdentity."+op)
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func TestIdentityPoolCRUD(t *testing.T) {
	p := newTestProvider(t)

	// CreateIdentityPool
	resp := ciReq(t, p, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "test-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &createOut))
	poolID, ok := createOut["IdentityPoolId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, poolID)
	assert.Equal(t, "test-pool", createOut["IdentityPoolName"])

	// DescribeIdentityPool
	resp = ciReq(t, p, "DescribeIdentityPool", map[string]string{"IdentityPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	assert.Equal(t, poolID, descOut["IdentityPoolId"])
	assert.Equal(t, "test-pool", descOut["IdentityPoolName"])
	assert.Equal(t, true, descOut["AllowUnauthenticatedIdentities"])

	// UpdateIdentityPool
	resp = ciReq(t, p, "UpdateIdentityPool", map[string]any{
		"IdentityPoolId":                 poolID,
		"IdentityPoolName":               "updated-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &updateOut))
	assert.Equal(t, "updated-pool", updateOut["IdentityPoolName"])

	// ListIdentityPools
	resp = ciReq(t, p, "ListIdentityPools", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	pools := listOut["IdentityPools"].([]any)
	require.Len(t, pools, 1)

	// SetIdentityPoolRoles + GetIdentityPoolRoles
	resp = ciReq(t, p, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles": map[string]string{
			"authenticated":   "arn:aws:iam::000000000000:role/AuthRole",
			"unauthenticated": "arn:aws:iam::000000000000:role/UnauthRole",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = ciReq(t, p, "GetIdentityPoolRoles", map[string]string{"IdentityPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var rolesOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &rolesOut))
	roles := rolesOut["Roles"].(map[string]any)
	assert.Contains(t, roles, "authenticated")
	assert.Contains(t, roles, "unauthenticated")

	// DeleteIdentityPool
	resp = ciReq(t, p, "DeleteIdentityPool", map[string]string{"IdentityPoolId": poolID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DescribeIdentityPool after delete → error
	resp = ciReq(t, p, "DescribeIdentityPool", map[string]string{"IdentityPoolId": poolID})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestIdentityCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create pool first
	resp := ciReq(t, p, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "id-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["IdentityPoolId"].(string)

	// GetId
	resp = ciReq(t, p, "GetId", map[string]any{
		"IdentityPoolId": poolID,
		"AccountId":      "000000000000",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var idOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &idOut))
	identityID := idOut["IdentityId"].(string)
	require.NotEmpty(t, identityID)

	// DescribeIdentity
	resp = ciReq(t, p, "DescribeIdentity", map[string]string{"IdentityId": identityID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &descOut))
	assert.Equal(t, identityID, descOut["IdentityId"])

	// ListIdentities
	resp = ciReq(t, p, "ListIdentities", map[string]any{
		"IdentityPoolId": poolID,
		"MaxResults":     10,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	identities := listOut["Identities"].([]any)
	require.Len(t, identities, 1)

	// DeleteIdentities
	resp = ciReq(t, p, "DeleteIdentities", map[string]any{
		"IdentityIdsToDelete": []string{identityID},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DescribeIdentity after delete → error
	resp = ciReq(t, p, "DescribeIdentity", map[string]string{"IdentityId": identityID})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetCredentials(t *testing.T) {
	p := newTestProvider(t)

	// Create pool
	resp := ciReq(t, p, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "creds-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["IdentityPoolId"].(string)

	// Get identity
	resp = ciReq(t, p, "GetId", map[string]any{"IdentityPoolId": poolID})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var idOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &idOut))
	identityID := idOut["IdentityId"].(string)

	// GetCredentialsForIdentity
	resp = ciReq(t, p, "GetCredentialsForIdentity", map[string]string{"IdentityId": identityID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var credsOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &credsOut))
	assert.Equal(t, identityID, credsOut["IdentityId"])
	creds := credsOut["Credentials"].(map[string]any)
	assert.NotEmpty(t, creds["AccessKeyId"])
	assert.NotEmpty(t, creds["SecretKey"])
	assert.NotEmpty(t, creds["SessionToken"])

	// GetOpenIdToken
	resp = ciReq(t, p, "GetOpenIdToken", map[string]string{"IdentityId": identityID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var tokenOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &tokenOut))
	assert.NotEmpty(t, tokenOut["Token"])

	// GetOpenIdTokenForDeveloperIdentity
	resp = ciReq(t, p, "GetOpenIdTokenForDeveloperIdentity", map[string]any{
		"IdentityPoolId": poolID,
		"Logins":         map[string]string{"developer": "user123"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var devTokenOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &devTokenOut))
	assert.NotEmpty(t, devTokenOut["Token"])
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	// Create pool
	resp := ciReq(t, p, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "tags-pool",
		"AllowUnauthenticatedIdentities": false,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var poolOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &poolOut))
	poolID := poolOut["IdentityPoolId"].(string)

	// Get pool ARN
	resp = ciReq(t, p, "DescribeIdentityPool", map[string]string{"IdentityPoolId": poolID})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	poolARN := "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/" + poolID

	// TagResource
	resp = ciReq(t, p, "TagResource", map[string]any{
		"ResourceArn": poolARN,
		"Tags": map[string]string{
			"env":   "test",
			"owner": "alice",
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListTagsForResource
	resp = ciReq(t, p, "ListTagsForResource", map[string]string{"ResourceArn": poolARN})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags := listOut["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "alice", tags["owner"])

	// UntagResource
	resp = ciReq(t, p, "UntagResource", map[string]any{
		"ResourceArn": poolARN,
		"TagKeys":     []string{"owner"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = ciReq(t, p, "ListTagsForResource", map[string]string{"ResourceArn": poolARN})
	require.NoError(t, json.Unmarshal(resp.Body, &listOut))
	tags = listOut["Tags"].(map[string]any)
	assert.Len(t, tags, 1)
	assert.Contains(t, tags, "env")
	assert.NotContains(t, tags, "owner")
}
