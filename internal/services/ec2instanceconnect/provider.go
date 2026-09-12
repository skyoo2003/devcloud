// SPDX-License-Identifier: Apache-2.0

package ec2instanceconnect

import (
	"context"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/ec2instanceconnect"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
)

// Provider implements the AWSEC2InstanceConnectService service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "ec2instanceconnect" }
func (p *Provider) ServiceName() string           { return "AWSEC2InstanceConnectService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolJSON11 }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

// HandleRequest serves both operations by hand. Neither carries a CRUD verb
// prefix, so the generic engine holds no metadata for this service and would
// decline every call.
func (p *Provider) HandleRequest(_ context.Context, op string, _ *http.Request) (*plugin.Response, error) {
	switch op {
	// AWS answers both with a request id and a bare success flag; there is no
	// instance to reach and no key to install, so the flag is the whole answer.
	case "SendSSHPublicKey", "SendSerialConsoleSSHPublicKey":
		return shared.JSONResponse(http.StatusOK, map[string]any{
			"RequestId": shared.GenerateUUID(),
			"Success":   true,
		})

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("ec2instanceconnect", func() plugin.ServicePlugin {
		return &Provider{}
	})
}
