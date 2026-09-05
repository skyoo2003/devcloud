// SPDX-License-Identifier: Apache-2.0

package emrserverless

import (
	"context"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/emrserverless"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Provider implements the AwsToledoWebService service.
type Provider struct {
	generated.BaseProvider
	dataDir string
}

func (p *Provider) ServiceID() string             { return "emrserverless" }
func (p *Provider) ServiceName() string           { return "AwsToledoWebService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	p.dataDir = cfg.DataDir
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

// HandleRequest implements nothing by hand and says so, which is what hands the
// request to the generic CRUD engine (see docs/crud-engine.md). A scaffolded
// service therefore serves its CRUD-shaped operations from the moment it is
// generated; anything the engine cannot classify still returns an honest
// InvalidAction rather than a fabricated success.
//
// Declining any other way — including generated.ErrNotImplemented — is a plain
// refusal the gateway never routes to the engine, leaving the service
// registered, routed, and serving zero operations.
func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	return nil, plugin.ErrUnhandledOp
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("emrserverless", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
