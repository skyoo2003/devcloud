// SPDX-License-Identifier: Apache-2.0

package kinesisvideowebrtcstorage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/kinesisvideowebrtcstorage"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the AWSAcuityRoutingServiceLambda service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "kinesisvideowebrtcstorage" }
func (p *Provider) ServiceName() string           { return "AWSAcuityRoutingServiceLambda" }
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
	// Both operations model a Smithy Unit output, so an empty object is the
	// complete response. The only thing worth checking is the one input AWS
	// insists on.
	case "JoinStorageSession", "JoinStorageSessionAsViewer":
		return joinStorageSession(body)

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

func joinStorageSession(body []byte) (*plugin.Response, error) {
	var input map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return shared.JSONError("SerializationException", "invalid JSON", http.StatusBadRequest), nil
		}
	}
	if arn, _ := input["channelArn"].(string); arn == "" {
		return shared.JSONError("InvalidArgumentException", "channelArn is required", http.StatusBadRequest), nil
	}
	return shared.JSONResponse(http.StatusOK, map[string]any{})
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("kinesisvideowebrtcstorage", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// This service signs as "kinesisvideo", which kinesisvideo itself claims.
	// Neither its operations nor the parent's model the join-session paths in
	// the CRUD registry, so without this the gateway's shared-signing-name split
	// has no candidate and the parent keeps the request. See
	// crud.RegisterRoutes.
	crud.RegisterRoutes("kinesisvideowebrtcstorage", generated.OperationRoutes)
}
