// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

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

// HandleAzureTokenExchange handles Microsoft Entra / Azure AD token exchange requests
// (e.g. POST /devcloud/identity/azure/oauth2/v2.0/token or POST /devcloud/identity/azure/token).
func (c *CrossCSPExchange) HandleAzureTokenExchange(req *http.Request) (*plugin.Response, error) {
	form, _, err := parseRequestFormOrJSON(req)
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusBadRequest,
			ContentType: "application/json",
			Body:        []byte(`{"error":"invalid_request","error_description":"failed to parse body"}`),
		}, nil
	}

	grantType := form.Get("grant_type")
	clientAssertion := form.Get("client_assertion")
	assertionType := form.Get("client_assertion_type")

	sub := "azure-workload-identity"
	if clientAssertion != "" {
		claims := ParseWebIdentityToken(clientAssertion)
		if claims.Subject != "" {
			sub = claims.Subject
		}
	}

	scope := form.Get("scope")
	if scope == "" {
		scope = "https://management.azure.com/.default"
	}

	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	randSuffix := hex.EncodeToString(bytes)

	azureToken := fmt.Sprintf("azure-bearer-%s-%s", sub, randSuffix)

	resp := map[string]any{
		"token_type":     "Bearer",
		"expires_in":     3600,
		"ext_expires_in": 3600,
		"access_token":   azureToken,
		"scope":          scope,
		"grant_type":     grantType,
		"assertion_type": assertionType,
	}

	raw, _ := json.Marshal(resp)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        raw,
	}, nil
}

// HandleGCPTokenExchange handles Google Cloud STS token exchange requests
// (e.g. POST /devcloud/identity/gcp/v1/token or POST /devcloud/identity/gcp/token).
func (c *CrossCSPExchange) HandleGCPTokenExchange(req *http.Request) (*plugin.Response, error) {
	form, _, err := parseRequestFormOrJSON(req)
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusBadRequest,
			ContentType: "application/json",
			Body:        []byte(`{"error":"invalid_request","error_description":"failed to parse body"}`),
		}, nil
	}

	grantType := form.Get("grant_type")
	subjectToken := form.Get("subject_token")
	subjectTokenType := form.Get("subject_token_type")
	requestedTokenType := form.Get("requested_token_type")
	audience := form.Get("audience")

	sub := "gcp-federated-identity"
	if subjectToken != "" {
		claims := ParseWebIdentityToken(subjectToken)
		if claims.Subject != "" {
			sub = claims.Subject
		}
	}

	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	randSuffix := hex.EncodeToString(bytes)

	gcpToken := fmt.Sprintf("ya29.devcloud-%s-%s", sub, randSuffix)

	resp := map[string]any{
		"access_token":         gcpToken,
		"issued_token_type":    "urn:ietf:params:oauth:token-type:access_token",
		"token_type":           "Bearer",
		"expires_in":           3600,
		"grant_type":           grantType,
		"subject_token_type":   subjectTokenType,
		"requested_token_type": requestedTokenType,
		"audience":             audience,
	}

	raw, _ := json.Marshal(resp)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        raw,
	}, nil
}
