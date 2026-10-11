// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

func TestProvider_Metadata(t *testing.T) {
	p := NewProvider()
	assert.Equal(t, "identity", p.ServiceID())
	assert.Equal(t, "DevCloud Federated Identity", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())
	assert.Equal(t, "devcloud", plugin.ProviderOf(p))
}

func TestProvider_OIDCDiscoveryAndJWKS(t *testing.T) {
	p := NewProvider()
	ctx := context.Background()

	// 1. Discovery
	reqDisc := httptest.NewRequest(http.MethodGet, "/devcloud/identity/.well-known/openid-configuration", nil)
	respDisc, err := p.HandleRequest(ctx, "", reqDisc)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respDisc.StatusCode)

	var disc map[string]any
	err = json.Unmarshal(respDisc.Body, &disc)
	require.NoError(t, err)
	assert.Contains(t, disc["issuer"], "/devcloud/identity")
	assert.Contains(t, disc["jwks_uri"], "/jwks.json")

	// 2. JWKS
	reqJWKS := httptest.NewRequest(http.MethodGet, "/devcloud/identity/jwks.json", nil)
	respJWKS, err := p.HandleRequest(ctx, "", reqJWKS)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respJWKS.StatusCode)

	var jwks JWKSResponse
	err = json.Unmarshal(respJWKS.Body, &jwks)
	require.NoError(t, err)
	require.Len(t, jwks.Keys, 1)
	assert.Equal(t, "RSA", jwks.Keys[0].Kty)
	assert.Equal(t, "RS256", jwks.Keys[0].Alg)
	assert.NotEmpty(t, jwks.Keys[0].N)
	assert.NotEmpty(t, jwks.Keys[0].E)
}

func TestProvider_MintToken(t *testing.T) {
	p := NewProvider()
	ctx := context.Background()

	mintPayload := map[string]any{
		"sub": "user@corp.example.com",
		"aud": "sts.amazonaws.com",
		"claims": map[string]any{
			"team": "infrastructure",
		},
	}
	body, _ := json.Marshal(mintPayload)
	reqMint := httptest.NewRequest(http.MethodPost, "/devcloud/identity/token", bytes.NewReader(body))
	reqMint.Header.Set("Content-Type", "application/json")
	respMint, err := p.HandleRequest(ctx, "", reqMint)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respMint.StatusCode)

	var tokenResp map[string]any
	err = json.Unmarshal(respMint.Body, &tokenResp)
	require.NoError(t, err)
	assert.Equal(t, "Bearer", tokenResp["token_type"])
	tokenStr, ok := tokenResp["access_token"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, tokenStr)

	// Verify token claims parsing
	parsed := ParseWebIdentityToken(tokenStr)
	assert.Equal(t, "user@corp.example.com", parsed.Subject)
	assert.Equal(t, "sts.amazonaws.com", parsed.Audience)
	assert.Equal(t, "infrastructure", parsed.Claims["team"])
}

func TestProvider_CrossCSPExchange(t *testing.T) {
	p := NewProvider()
	ctx := context.Background()

	// First mint a token to use as subject assertion
	tokenStr, err := p.issuer.MintToken(MintTokenOptions{
		Subject: "workload-gha-runner",
	})
	require.NoError(t, err)

	// 1. Azure Token Exchange
	azureReqBody := url.Values{
		"grant_type":       {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"client_assertion": {tokenStr},
		"client_id":        {"my-azure-app-client"},
	}
	reqAzure := httptest.NewRequest(http.MethodPost, "/devcloud/identity/azure/oauth2/v2.0/token", bytes.NewReader([]byte(azureReqBody.Encode())))
	reqAzure.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	respAzure, err := p.HandleRequest(ctx, "", reqAzure)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respAzure.StatusCode)

	var azResp map[string]any
	err = json.Unmarshal(respAzure.Body, &azResp)
	require.NoError(t, err)
	assert.Equal(t, "Bearer", azResp["token_type"])
	assert.NotEmpty(t, azResp["access_token"])

	// 2. GCP Token Exchange
	gcpReqBody := url.Values{
		"grant_type":    {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"subject_token": {tokenStr},
	}
	reqGCP := httptest.NewRequest(http.MethodPost, "/devcloud/identity/gcp/v1/token", bytes.NewReader([]byte(gcpReqBody.Encode())))
	reqGCP.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	respGCP, err := p.HandleRequest(ctx, "", reqGCP)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respGCP.StatusCode)

	var gcpResp map[string]any
	err = json.Unmarshal(respGCP.Body, &gcpResp)
	require.NoError(t, err)
	assert.Equal(t, "Bearer", gcpResp["token_type"])
	gcpToken, _ := gcpResp["access_token"].(string)
	assert.Contains(t, gcpToken, "ya29.")
}
