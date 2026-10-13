// SPDX-License-Identifier: Apache-2.0

package azureappconfig

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/azureappconfig"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	store *Store
}

func (p *Provider) ServiceID() string             { return "azureappconfig" }
func (p *Provider) ServiceName() string           { return "Azure App Configuration" }
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
	items, err := p.store.ListKeyValues("", "", "")
	if err != nil {
		return nil, err
	}
	var res []plugin.Resource
	for _, item := range items {
		res = append(res, plugin.Resource{
			Type: "Microsoft.AppConfiguration/configurationStores/keyValues",
			ID:   item.Key,
			Name: item.Key,
		})
	}
	return res, nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	storeName := extractStoreName(req)

	var queryLabel *string
	q := req.URL.Query()
	if q.Has("label") {
		lbl := q.Get("label")
		if lbl == "%00" || lbl == "\x00" {
			lbl = ""
		}
		queryLabel = &lbl
	}

	// Escaped, so matchAppConfigRoute decodes the key exactly once: a key
	// holding a literal "%41" would otherwise come back as "A".
	matchedOp, params := matchAppConfigRoute(req.Method, req.URL.EscapedPath())
	if matchedOp != "" {
		op = matchedOp
	}

	switch op {
	case "ListKeyValues":
		keyFilter := q.Get("key")
		labelFilter := q.Get("label")
		items, err := p.store.ListKeyValues(storeName, keyFilter, labelFilter)
		if err != nil {
			return appConfigErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(map[string]any{"items": items})
		return &plugin.Response{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "application/vnd.microsoft.appconfig.kvset+json; charset=utf-8",
			},
			Body: body,
		}, nil

	case "SetKeyValue":
		key := params["key"]
		var payload struct {
			Value       string            `json:"value"`
			ContentType string            `json:"content_type"`
			Tags        map[string]string `json:"tags"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err == nil && len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &payload)
		}

		kv, err := p.store.SetKeyValue(storeName, key, queryLabel, payload.Value, payload.ContentType, payload.Tags)
		if err != nil {
			return appConfigErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		body, _ := json.Marshal(kv)
		return &plugin.Response{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "application/vnd.microsoft.appconfig.kv+json; charset=utf-8",
				"ETag":         kv.ETag,
			},
			Body: body,
		}, nil

	case "GetKeyValue":
		key := params["key"]
		kv, err := p.store.GetKeyValue(storeName, key, queryLabel)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return appConfigErrorResponse(http.StatusNotFound, "ResourceNotFound", "The key-value was not found.")
			}
			return appConfigErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		body, _ := json.Marshal(kv)
		return &plugin.Response{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "application/vnd.microsoft.appconfig.kv+json; charset=utf-8",
				"ETag":         kv.ETag,
			},
			Body: body,
		}, nil

	case "DeleteKeyValue":
		key := params["key"]
		kv, err := p.store.DeleteKeyValue(storeName, key, queryLabel)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return &plugin.Response{
					StatusCode: http.StatusNoContent,
					Headers:    map[string]string{},
					Body:       nil,
				}, nil
			}
			return appConfigErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		body, _ := json.Marshal(kv)
		return &plugin.Response{
			StatusCode: http.StatusOK,
			Headers: map[string]string{
				"Content-Type": "application/vnd.microsoft.appconfig.kv+json; charset=utf-8",
				"ETag":         kv.ETag,
			},
			Body: body,
		}, nil

	default:
		return appConfigErrorResponse(http.StatusNotFound, "NotFound", "The requested resource does not exist.")
	}
}

func matchAppConfigRoute(method, path string) (string, map[string]string) {
	if path == "/kv" || path == "/kv/" {
		if method == http.MethodGet {
			return "ListKeyValues", nil
		}
		return "", nil
	}
	if strings.HasPrefix(path, "/kv/") {
		rawKey := strings.TrimPrefix(path, "/kv/")
		key, err := url.PathUnescape(rawKey)
		if err != nil {
			key = rawKey
		}
		params := map[string]string{"key": key}
		switch method {
		case http.MethodPut:
			return "SetKeyValue", params
		case http.MethodGet:
			return "GetKeyValue", params
		case http.MethodDelete:
			return "DeleteKeyValue", params
		}
		return "", nil
	}
	return "", nil
}

func extractStoreName(req *http.Request) string {
	host := req.Host
	if colon := strings.Index(host, ":"); colon != -1 {
		host = host[:colon]
	}
	if strings.HasSuffix(host, ".azconfig.io") {
		return strings.TrimSuffix(host, ".azconfig.io")
	}
	return "default"
}

func appConfigErrorResponse(status int, name, detail string) (*plugin.Response, error) {
	errBody := map[string]any{
		"type":   "https://azconfig.io/errors/" + strings.ToLower(name),
		"title":  name,
		"name":   name,
		"status": status,
		"detail": detail,
	}
	b, _ := json.Marshal(errBody)
	return &plugin.Response{
		StatusCode: status,
		Headers: map[string]string{
			"Content-Type": "application/problem+json; charset=utf-8",
		},
		Body: b,
	}, nil
}

func init() {
	plugin.DefaultRegistry.Register("azureappconfig", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
