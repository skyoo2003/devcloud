// SPDX-License-Identifier: Apache-2.0

package sagemakerruntimehttp2

import (
	"context"
	"fmt"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/sagemakerruntimehttp2"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the SageMakerRuntimeHttp2 service.
type Provider struct {
	generated.BaseProvider
	dataDir string
}

func (p *Provider) ServiceID() string             { return "sagemakerruntimehttp2" }
func (p *Provider) ServiceName() string           { return "SageMakerRuntimeHttp2" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op, _ = generated.MatchOperation(req.Method, req.URL.RequestURI())
	}

	switch op {
	case "InvokeEndpointWithBidirectionalStream":
		return p.handleInvokeEndpointWithBidirectionalStream(ctx, req)
	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) handleInvokeEndpointWithBidirectionalStream(ctx context.Context, req *http.Request) (*plugin.Response, error) {
	_, params := generated.MatchOperation(req.Method, req.URL.RequestURI())
	endpointName := params["EndpointName"]
	if endpointName == "" {
		endpointName = "default-endpoint"
	}

	payload := fmt.Sprintf(`{"predictions":[0.0],"endpoint":"%s"}`, endpointName)
	return &plugin.Response{
		StatusCode:  http.StatusOK,
		ContentType: "application/json",
		Headers: map[string]string{
			"Content-Type":                      "application/json",
			"x-Amzn-Invoked-Production-Variant": "AllTraffic",
		},
		Body: []byte(payload),
	}, nil
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("sagemakerruntimehttp2", func() plugin.ServicePlugin {
		return &Provider{}
	})
	crud.RegisterRoutes("sagemakerruntimehttp2", generated.OperationRoutes)
}
