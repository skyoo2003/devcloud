// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

type CrossCSPExchange struct {
	issuer *OIDCIssuer
}

func NewCrossCSPExchange(issuer *OIDCIssuer) *CrossCSPExchange {
	return &CrossCSPExchange{issuer: issuer}
}

func parseRequestFormOrJSON(req *http.Request) (url.Values, map[string]any, error) {
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, nil, err
	}
	contentType := req.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var jsonBody map[string]any
		if err := json.Unmarshal(bodyBytes, &jsonBody); err != nil {
			return nil, nil, err
		}
		values := url.Values{}
		for k, v := range jsonBody {
			if s, ok := v.(string); ok {
				values.Set(k, s)
			}
		}
		return values, jsonBody, nil
	}

	form, err := url.ParseQuery(string(bodyBytes))
	if err != nil {
		return nil, nil, err
	}
	return form, nil, nil
}

// HandleAzureTokenExchange simulates Azure Active Directory / Microsoft Entra ID token endpoint
// (e.g. POST /devcloud/identity/azure/oauth2/v2.0/token or POST /devcloud/identity/azure/token).
// Exchanges an AWS STS token, GitHub Actions OIDC token, or local OIDC token for an Azure AD Bearer token.
func (c *CrossCSPExchange) HandleAzureTokenExchange(req *http.Request) (*plugin.Response, error) {
	form, jsonBody, err := parseRequestFormOrJSON(req)
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusBadRequest,
			ContentType: "application/json",
			Body:        []byte(`{"error":"invalid_request","error_description":"failed to parse body"}`),
		}, nil
	}

	assertion := form.Get("client_assertion")
	if assertion == "" {
		assertion = form.Get("assertion")
	}
	if assertion == "" {
		assertion = form.Get("subject_token")
	}

	parsed := ParseWebIdentityToken(assertion)
	subject := parsed.Subject
	if subject == "" {
		subject = "devcloud-azure-workload"
	}

	scope := form.Get("scope")
	if scope == "" {
		scope = "https://management.azure.com/.default"
	}
	clientID := form.Get("client_id")
	if clientID == "" {
		clientID = "devcloud-sp-app-id"
	}

	// Mint an Azure AD token via OIDC issuer
	azureToken, err := c.issuer.MintToken(MintTokenOptions{
		Subject:   subject,
		Issuer:    "https://sts.windows.net/72f988bf-86f1-41af-91ab-2d7cd011db47/",
		Audience:  "https://management.azure.com",
		ExpiresIn: time.Hour,
		Claims: map[string]any{
			"appid": clientID,
			"tid":   "72f988bf-86f1-41af-91ab-2d7cd011db47",
			"oid":   "00000000-0000-0000-0000-000000000001",
			"roles": []string{"Contributor"},
		},
	})
	if err != nil {
		// Fallback to synthetic bearer token
		azureToken = fmt.Sprintf("azure-bearer-%s", subject)
	}

	respPayload := map[string]any{
		"token_type":     "Bearer",
		"expires_in":     3600,
		"ext_expires_in": 3600,
		"access_token":   azureToken,
		"scope":          scope,
	}
	if jsonBody != nil && jsonBody["include_id_token"] == true {
		respPayload["id_token"] = azureToken
	}

	respBytes, _ := json.Marshal(respPayload)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        respBytes,
	}, nil
}

// HandleGCPTokenExchange simulates Google Cloud STS / OAuth2 token endpoint
// (e.g. POST /devcloud/identity/gcp/v1/token or POST /devcloud/identity/gcp/token).
// Exchanges an AWS STS token or OIDC token for a simulated Google OAuth2 access token (ya29...).
func (c *CrossCSPExchange) HandleGCPTokenExchange(req *http.Request) (*plugin.Response, error) {
	form, _, err := parseRequestFormOrJSON(req)
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusBadRequest,
			ContentType: "application/json",
			Body:        []byte(`{"error":"invalid_request","error_description":"failed to parse body"}`),
		}, nil
	}

	subjectToken := form.Get("subject_token")
	if subjectToken == "" {
		subjectToken = form.Get("assertion")
	}

	parsed := ParseWebIdentityToken(subjectToken)
	subject := parsed.Subject
	if subject == "" {
		subject = "gcp-federated-sa"
	}

	b := make([]byte, 16)
	_, _ = rand.Read(b)
	hexSuffix := hex.EncodeToString(b)

	// Format matching Google ya29 access tokens
	gcpToken := fmt.Sprintf("ya29.c.devcloud_%s_%s", strings.ReplaceAll(subject, ":", "_"), hexSuffix)

	respPayload := map[string]any{
		"access_token":      gcpToken,
		"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		"token_type":        "Bearer",
		"expires_in":        3600,
	}

	respBytes, _ := json.Marshal(respPayload)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        respBytes,
	}, nil
}
