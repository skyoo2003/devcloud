// SPDX-License-Identifier: Apache-2.0

package gcpsecrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	generated "github.com/skyoo2003/devcloud/internal/generated/gcpsecrets"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

type Provider struct {
	generated.BaseProvider
	dataDir string
	store   *Store
}

func (p *Provider) ServiceID() string             { return "gcpsecrets" }
func (p *Provider) ServiceName() string           { return "Google Cloud Secret Manager" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }
func (p *Provider) Provider() string              { return "gcp" }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	if p.dataDir == "" {
		p.dataDir = "./data/gcp-secrets"
	}
	p.store = NewStore(p.dataDir)
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
			Type: "secretmanager.secret",
			ID:   s.Name,
			Name: s.Name,
		})
	}
	return res, nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	matchedOp, params := matchSecretRoute(req.Method, req.URL.RequestURI())
	if matchedOp != "" {
		op = matchedOp
	}

	switch op {
	case "ListSecrets":
		project := params["project"]
		if project == "" {
			project = extractProjectFromPath(req.URL.Path)
		}
		secs, err := p.store.ListSecrets(project)
		if err != nil {
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		if secs == nil {
			secs = []*Secret{}
		}
		body, _ := json.Marshal(map[string]any{"secrets": secs})
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "CreateSecret":
		project := params["project"]
		secretId := params["secretId"]
		if secretId == "" {
			secretId = req.URL.Query().Get("secretId")
		}
		if project == "" {
			project = extractProjectFromPath(req.URL.Path)
		}

		var payload struct {
			Labels map[string]string `json:"labels"`
		}
		if bodyBytes, err := io.ReadAll(req.Body); err == nil && len(bodyBytes) > 0 {
			_ = json.Unmarshal(bodyBytes, &payload)
		}

		sec, err := p.store.CreateSecret(project, secretId, payload.Labels)
		if err != nil {
			if errors.Is(err, ErrSecretAlreadyExists) {
				return gcpErrorResponse(http.StatusConflict, "AlreadyExists", fmt.Sprintf("Secret [%s] already exists.", secretId))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sec)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "GetSecret":
		project := params["project"]
		secret := params["secret"]
		sec, err := p.store.GetSecret(project, secret)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Secret [%s] not found.", secret))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		body, _ := json.Marshal(sec)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "DeleteSecret":
		project := params["project"]
		secret := params["secret"]
		err := p.store.DeleteSecret(project, secret)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Secret [%s] not found.", secret))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        []byte("{}"),
			ContentType: "application/json",
		}, nil

	case "AddSecretVersion":
		project := params["project"]
		secret := params["secret"]

		var reqPayload struct {
			Payload struct {
				Data string `json:"data"` // base64 encoded
			} `json:"payload"`
		}
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "failed to read request body")
		}
		if err := json.Unmarshal(bodyBytes, &reqPayload); err != nil {
			return gcpErrorResponse(http.StatusBadRequest, "InvalidArgument", "invalid JSON")
		}

		data, err := base64.StdEncoding.DecodeString(reqPayload.Payload.Data)
		if err != nil {
			// fallback: treat as raw string if not base64
			data = []byte(reqPayload.Payload.Data)
		}

		v, err := p.store.AddSecretVersion(project, secret, data)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Secret [%s] not found.", secret))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		body, _ := json.Marshal(v)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "ListSecretVersions":
		project := params["project"]
		secret := params["secret"]
		versions, err := p.store.ListSecretVersions(project, secret)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", fmt.Sprintf("Secret [%s] not found.", secret))
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}
		if versions == nil {
			versions = []*SecretVersion{}
		}
		body, _ := json.Marshal(map[string]any{"versions": versions})
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "AccessSecretVersion":
		project := params["project"]
		secret := params["secret"]
		version := params["version"]

		v, data, err := p.store.AccessSecretVersion(project, secret, version)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) || errors.Is(err, ErrVersionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", "Secret or version not found.")
			}
			if errors.Is(err, ErrVersionDestroyed) {
				return gcpErrorResponse(http.StatusBadRequest, "FailedPrecondition", "Secret version is destroyed.")
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		resp := map[string]any{
			"name": v.Name,
			"payload": map[string]string{
				"data": base64.StdEncoding.EncodeToString(data),
			},
		}
		body, _ := json.Marshal(resp)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil

	case "DestroySecretVersion":
		project := params["project"]
		secret := params["secret"]
		version := params["version"]

		v, err := p.store.DestroySecretVersion(project, secret, version)
		if err != nil {
			if errors.Is(err, ErrSecretNotFound) || errors.Is(err, ErrVersionNotFound) {
				return gcpErrorResponse(http.StatusNotFound, "NotFound", "Secret or version not found.")
			}
			return gcpErrorResponse(http.StatusInternalServerError, "InternalError", err.Error())
		}

		body, _ := json.Marshal(v)
		return &plugin.Response{
			StatusCode:  http.StatusOK,
			Headers:     map[string]string{"Content-Type": "application/json"},
			Body:        body,
			ContentType: "application/json",
		}, nil
	}

	return nil, plugin.ErrUnhandledOp
}

func extractProjectFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "projects" {
		return parts[2]
	}
	return "default"
}

func gcpErrorResponse(status int, reason, message string) (*plugin.Response, error) {
	resp := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"status":  reason,
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

func matchSecretRoute(method, uri string) (string, map[string]string) {
	uPath, _, _ := strings.Cut(uri, "?")
	parts := strings.Split(strings.Trim(uPath, "/"), "/")
	// Expected prefix: ["v1", "projects", "{project}", ...]
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "secrets" {
		return "", nil
	}

	project := parts[2]
	params := map[string]string{"project": project}

	// 1. /v1/projects/{project}/secrets
	if len(parts) == 4 {
		if method == http.MethodGet {
			return "ListSecrets", params
		}
		if method == http.MethodPost {
			return "CreateSecret", params
		}
		return "", nil
	}

	// 2. /v1/projects/{project}/secrets/{secret...}
	secretPart := parts[4]

	// Check if secretPart has custom method: e.g. "my-secret:addVersion"
	if sName, action, found := strings.Cut(secretPart, ":"); found {
		params["secret"] = sName
		if action == "addVersion" && method == http.MethodPost {
			return "AddSecretVersion", params
		}
		return "", nil
	}
	params["secret"] = secretPart

	if len(parts) == 5 {
		if method == http.MethodGet {
			return "GetSecret", params
		}
		if method == http.MethodDelete {
			return "DeleteSecret", params
		}
		return "", nil
	}

	// 3. /v1/projects/{project}/secrets/{secret}/versions...
	if parts[5] == "versions" {
		if len(parts) == 6 {
			if method == http.MethodGet {
				return "ListSecretVersions", params
			}
			return "", nil
		}

		versionPart := parts[6]
		if vNum, action, found := strings.Cut(versionPart, ":"); found {
			params["version"] = vNum
			if action == "access" && method == http.MethodGet {
				return "AccessSecretVersion", params
			}
			if action == "destroy" && method == http.MethodPost {
				return "DestroySecretVersion", params
			}
			return "", nil
		}

		params["version"] = versionPart
		if method == http.MethodGet {
			return "GetSecretVersion", params
		}
	}

	return "", nil
}

func init() {
	plugin.DefaultRegistry.Register("gcpsecrets", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
