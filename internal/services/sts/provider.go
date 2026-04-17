// SPDX-License-Identifier: Apache-2.0

package sts

import (
	"context"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/sts"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

// Provider implements the SecurityTokenServiceV20110615 service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "sts" }
func (p *Provider) ServiceName() string           { return "SecurityTokenServiceV20110615" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	return nil
}

func (p *Provider) Shutdown(ctx context.Context) error {
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	return nil, generated.ErrNotImplemented
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func (p *Provider) GetMetrics(ctx context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}
