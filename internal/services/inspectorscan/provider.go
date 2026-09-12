// SPDX-License-Identifier: Apache-2.0

package inspectorscan

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/inspectorscan"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the InspectorScan service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "inspectorscan" }
func (p *Provider) ServiceName() string           { return "InspectorScan" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op, _ = generated.MatchOperation(req.Method, req.URL.RequestURI())
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return shared.JSONError("SerializationException", "failed to read body", http.StatusBadRequest), nil
	}

	switch op {
	case "ScanSbom":
		return scanSbom(body)

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

// scanSbom echoes the submitted SBOM back unchanged. DevCloud runs no
// vulnerability scanner, so the honest answer is the document as given —
// inventing findings would make a local run disagree with a real one in the one
// direction that matters.
func scanSbom(body []byte) (*plugin.Response, error) {
	if len(body) > 0 {
		var input map[string]any
		if err := json.Unmarshal(body, &input); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
		if s, ok := input["sbom"]; ok {
			return shared.JSONResponse(http.StatusOK, map[string]any{"sbom": s})
		}
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{"sbom": map[string]any{}})
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("inspectorscan", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// Unique signing name; declared for the same reason eksauth declares its
	// own — see crud.RegisterRoutes.
	crud.RegisterRoutes("inspectorscan", generated.OperationRoutes)
}
