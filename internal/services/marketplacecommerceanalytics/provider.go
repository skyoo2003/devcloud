// SPDX-License-Identifier: Apache-2.0

package marketplacecommerceanalytics

import (
	"context"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/marketplacecommerceanalytics"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the MarketplaceCommerceAnalytics20150701 service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string   { return "marketplacecommerceanalytics" }
func (p *Provider) ServiceName() string { return "MarketplaceCommerceAnalytics20150701" }

func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

// HandleRequest serves both operations by hand. Both hand work to an
// asynchronous pipeline that writes a data set to S3; the request id is the
// whole synchronous answer AWS gives, and it is the whole answer here.
func (p *Provider) HandleRequest(_ context.Context, op string, _ *http.Request) (*plugin.Response, error) {
	switch op {
	case "GenerateDataSet", "StartSupportDataExport":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"dataSetRequestId": shared.GenerateUUID(),
		})

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("marketplacecommerceanalytics", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
