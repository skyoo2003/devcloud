// SPDX-License-Identifier: Apache-2.0

package azurekeyvault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/azurekeyvault"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "azurekeyvault" }
func (p *Provider) ServiceName() string           { return "Azure Key Vault" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "azure" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.store = NewStore()
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	secs, err := p.store.ListSecrets("")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, s := range secs {
		res = append(res, plugin.Resource{
			Type: "Microsoft.KeyVault/vaults/secrets",
			ID:   s.ID,
			Name: s.ID,
		})
	}
	return res, nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	vault := extractVaultName(req)
	matchedOp, params := matchKeyVaultRoute(req.Method, req.URL.RequestURI())
	if matchedOp != "" {
		op = matchedOp
	}

	switch op {
	case "ListSecrets":
		items, err := p.store.ListSecrets(vault)
		if err != nil {
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(map[string]any{"value": items})
		return jsonResponse(http.StatusOK, body)

	case "SetSecret":
		name := params["secret-name"]
		var payload struct {
			Value       *string           `json:"value"`
			ContentType string            `json:"contentType"`
			Tags        map[string]string `json:"tags"`
		}
		// A missing value is a 400, as in Key Vault, rather than a stored
		// empty secret: the SDK's auth-challenge probe sends this PUT bodyless.
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil || json.Unmarshal(bodyBytes, &payload) != nil || payload.Value == nil {
			return azureErrorResponse(http.StatusBadRequest, "BadParameter", "The request body must be JSON with a 'value' property.")
		}

		sec, err := p.store.SetSecret(vault, name, *payload.Value, payload.ContentType, payload.Tags)
		if err != nil {
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sec)
		return jsonResponse(http.StatusOK, body)

	case "GetSecret":
		name := params["secret-name"]
		version := params["secret-version"]
		sec, err := p.store.GetSecret(vault, name, version)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return azureErrorResponse(http.StatusNotFound, "SecretNotFound", fmt.Sprintf("A secret with (name/id) %s was not found in this key vault.", name))
			}
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sec)
		return jsonResponse(http.StatusOK, body)

	case "GetSecretVersions":
		name := params["secret-name"]
		versions, err := p.store.GetSecretVersions(vault, name)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return azureErrorResponse(http.StatusNotFound, "SecretNotFound", fmt.Sprintf("A secret with (name/id) %s was not found in this key vault.", name))
			}
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(map[string]any{"value": versions})
		return jsonResponse(http.StatusOK, body)

	case "DeleteSecret":
		name := params["secret-name"]
		sec, err := p.store.DeleteSecret(vault, name)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return azureErrorResponse(http.StatusNotFound, "SecretNotFound", fmt.Sprintf("A secret with (name/id) %s was not found in this key vault.", name))
			}
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sec)
		return jsonResponse(http.StatusOK, body)

	case "ListKeys":
		items, err := p.store.ListKeys(vault)
		if err != nil {
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(map[string]any{"value": items})
		return jsonResponse(http.StatusOK, body)

	case "CreateKey":
		name := params["key-name"]
		var payload struct {
			Kty     string `json:"kty"`
			KeySize int    `json:"key_size"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err == nil && len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &payload)
		}

		k, err := p.store.CreateKey(vault, name, payload.Kty, payload.KeySize)
		if err != nil {
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(k)
		return jsonResponse(http.StatusOK, body)

	case "GetKey":
		name := params["key-name"]
		k, err := p.store.GetKey(vault, name)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return azureErrorResponse(http.StatusNotFound, "KeyNotFound", fmt.Sprintf("A key with (name/id) %s was not found in this key vault.", name))
			}
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(k)
		return jsonResponse(http.StatusOK, body)

	case "DeleteKey":
		name := params["key-name"]
		k, err := p.store.DeleteKey(vault, name)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return azureErrorResponse(http.StatusNotFound, "KeyNotFound", fmt.Sprintf("A key with (name/id) %s was not found in this key vault.", name))
			}
			return azureErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(k)
		return jsonResponse(http.StatusOK, body)
	}

	return nil, plugin.ErrUnhandledOp
}

func matchKeyVaultRoute(method, uri string) (string, map[string]string) {
	uPath, _, _ := strings.Cut(uri, "?")
	parts := strings.Split(strings.Trim(uPath, "/"), "/")
	if len(parts) == 0 {
		return "", nil
	}

	// 1. /secrets...
	if parts[0] == "secrets" {
		params := map[string]string{}
		if len(parts) == 1 {
			if method == http.MethodGet {
				return "ListSecrets", params
			}
			return "", nil
		}

		params["secret-name"] = parts[1]
		if len(parts) == 2 {
			if method == http.MethodPut {
				return "SetSecret", params
			}
			if method == http.MethodGet {
				return "GetSecret", params
			}
			if method == http.MethodDelete {
				return "DeleteSecret", params
			}
			return "", nil
		}

		if len(parts) == 3 && method == http.MethodGet {
			if parts[2] == "versions" {
				return "GetSecretVersions", params
			}
			params["secret-version"] = parts[2]
			return "GetSecret", params
		}
		return "", nil
	}

	// 2. /keys...
	if parts[0] == "keys" {
		params := map[string]string{}
		if len(parts) == 1 {
			if method == http.MethodGet {
				return "ListKeys", params
			}
			return "", nil
		}

		params["key-name"] = parts[1]
		if len(parts) == 2 {
			if method == http.MethodGet {
				return "GetKey", params
			}
			if method == http.MethodDelete {
				return "DeleteKey", params
			}
			return "", nil
		}

		if len(parts) == 3 && parts[2] == "create" && method == http.MethodPost {
			return "CreateKey", params
		}
		return "", nil
	}

	return "", nil
}

func extractVaultName(req *http.Request) string {
	host := req.Host
	if strings.Contains(host, ".vault.azure.net") {
		return strings.Split(host, ".")[0]
	}
	return "devvault"
}

func jsonResponse(status int, body []byte) (*plugin.Response, error) {
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		ContentType: "application/json",
	}, nil
}

func azureErrorResponse(status int, code, message string) (*plugin.Response, error) {
	resp := map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	body, _ := json.Marshal(resp)
	return &plugin.Response{
		StatusCode:  status,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        body,
		ContentType: "application/json",
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("azurekeyvault", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
