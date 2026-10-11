// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/identity"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Provider implements the DevCloud Federated Identity service.
type Provider struct {
	generated.BaseProvider
	issuer   *OIDCIssuer
	exchange *CrossCSPExchange
}

// NewProvider creates a new Federated Identity Provider instance.
func NewProvider() *Provider {
	iss, err := NewOIDCIssuer()
	if err != nil {
		panic(fmt.Sprintf("failed to initialize OIDC issuer: %v", err))
	}
	return &Provider{
		issuer:   iss,
		exchange: NewCrossCSPExchange(iss),
	}
}

func (p *Provider) ServiceID() string             { return "identity" }
func (p *Provider) ServiceName() string           { return "DevCloud Federated Identity" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "devcloud" }

func (p *Provider) Init(_ plugin.PluginConfig) error {
	return nil
}

func (p *Provider) Shutdown(_ context.Context) error {
	return nil
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{
		{Type: "OIDCProvider", ID: "devcloud-local-oidc", Name: "DevCloud Local OIDC Issuer"},
	}, nil
}

func (p *Provider) HandleRequest(_ context.Context, _ string, req *http.Request) (*plugin.Response, error) {
	path := req.URL.Path

	// 1. OpenID Discovery
	if req.Method == http.MethodGet && (strings.HasSuffix(path, "/.well-known/openid-configuration") || strings.Contains(path, "openid-configuration")) {
		issuerURL := fmt.Sprintf("http://%s/devcloud/identity", req.Host)
		if req.Host == "" {
			issuerURL = "http://localhost:4747/devcloud/identity"
		}
		cfg := p.issuer.OpenIDConfiguration(issuerURL)
		body, _ := json.Marshal(cfg)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Body:        body,
		}, nil
	}

	// 2. JWKS
	if req.Method == http.MethodGet && strings.HasSuffix(path, "/jwks.json") {
		jwks := p.issuer.JWKS()
		body, _ := json.Marshal(jwks)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			ContentType: "application/json",
			Body:        body,
		}, nil
	}

	// 3. Token Minting
	if req.Method == http.MethodPost && (strings.HasSuffix(path, "/identity/token") || path == "/token") {
		return p.handleMintToken(req)
	}

	// 4. Azure AD Token Exchange
	if req.Method == http.MethodPost && (strings.Contains(path, "/azure/") || strings.HasSuffix(path, "/azure/token")) {
		return p.exchange.HandleAzureTokenExchange(req)
	}

	// 5. GCP Token Exchange
	if req.Method == http.MethodPost && (strings.Contains(path, "/gcp/") || strings.HasSuffix(path, "/gcp/token")) {
		return p.exchange.HandleGCPTokenExchange(req)
	}

	return &plugin.Response{
		StatusCode:  http.StatusNotFound,
		ContentType: "application/json",
		Body:        []byte(`{"error":"not_found","error_description":"endpoint not found"}`),
	}, nil
}

func (p *Provider) handleMintToken(req *http.Request) (*plugin.Response, error) {
	form, jsonBody, err := parseRequestFormOrJSON(req)
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusBadRequest,
			ContentType: "application/json",
			Body:        []byte(`{"error":"invalid_request","error_description":"failed to parse body"}`),
		}, nil
	}

	sub := form.Get("sub")
	if sub == "" {
		sub = form.Get("subject")
	}
	if sub == "" {
		sub = "devcloud-workload-user"
	}

	iss := form.Get("iss")
	if iss == "" {
		iss = form.Get("issuer")
	}
	if iss == "" {
		host := req.Host
		if host == "" {
			host = "localhost:4747"
		}
		iss = fmt.Sprintf("http://%s/devcloud/identity", host)
	}

	aud := form.Get("aud")
	if aud == "" {
		aud = form.Get("audience")
	}
	if aud == "" {
		aud = "sts.devcloud"
	}

	expSec := 3600
	if expStr := form.Get("expires_in"); expStr != "" {
		if n, err := strconv.Atoi(expStr); err == nil && n > 0 {
			expSec = n
		}
	} else if jsonBody != nil {
		if expFloat, ok := jsonBody["expires_in"].(float64); ok && expFloat > 0 {
			expSec = int(expFloat)
		}
	}

	var customClaims map[string]any
	if jsonBody != nil {
		if c, ok := jsonBody["claims"].(map[string]any); ok {
			customClaims = c
		}
	}

	tokenStr, err := p.issuer.MintToken(MintTokenOptions{
		Subject:   sub,
		Issuer:    iss,
		Audience:  aud,
		ExpiresIn: time.Duration(expSec) * time.Second,
		Claims:    customClaims,
	})
	if err != nil {
		return &plugin.Response{
			StatusCode:  http.StatusInternalServerError,
			ContentType: "application/json",
			Body:        []byte(fmt.Sprintf(`{"error":"server_error","error_description":%q}`, err.Error())),
		}, nil
	}

	resp := map[string]any{
		"access_token": tokenStr,
		"id_token":     tokenStr,
		"token_type":   "Bearer",
		"expires_in":   expSec,
		"sub":          sub,
		"iss":          iss,
		"aud":          aud,
	}
	raw, _ := json.Marshal(resp)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Body:        raw,
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("identity", func() plugin.ServicePlugin {
		return NewProvider()
	})
}
