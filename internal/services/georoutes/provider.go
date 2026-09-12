// SPDX-License-Identifier: Apache-2.0

package georoutes

import (
	"context"
	"net/http"

	generated "github.com/skyoo2003/devcloud/internal/generated/georoutes"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// Provider implements the RoutesService service.
type Provider struct {
	generated.BaseProvider
}

func (p *Provider) ServiceID() string             { return "georoutes" }
func (p *Provider) ServiceName() string           { return "RoutesService" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTJSON }

func (p *Provider) Init(_ plugin.PluginConfig) error { return nil }

func (p *Provider) Shutdown(_ context.Context) error { return nil }

// HandleRequest answers every operation with the smallest well-formed shape the
// model declares, all members PascalCase on the wire.
//
// ponytail: empty result sets; a real router needs a road graph DevCloud does
// not ship. A caller reading 200 with "Routes": [] is being told there is no
// route engine here, not that no route exists.
func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	if op == "" {
		op, _ = generated.MatchOperation(req.Method, req.URL.RequestURI())
	}

	switch op {
	case "CalculateRoutes":
		return priced("RoutesRequest", map[string]any{
			"LegGeometryFormat": "Simple",
			"Notices":           []any{},
			"Routes":            []any{},
		})

	case "CalculateIsolines":
		return priced("IsolinesRequest", map[string]any{
			"ArrivalTime":           "",
			"DepartureTime":         "",
			"IsolineGeometryFormat": "Simple",
			"Isolines":              []any{},
			"SnappedDestination":    []any{},
			"SnappedOrigin":         []any{},
		})

	case "CalculateRouteMatrix":
		return priced("RouteMatrixRequest", map[string]any{
			"ErrorCount":      0,
			"RouteMatrix":     []any{},
			"RoutingBoundary": map[string]any{},
		})

	case "OptimizeWaypoints":
		return priced("OptimizeWaypointsRequest", map[string]any{
			"Connections":        []any{},
			"Distance":           0,
			"Duration":           0,
			"ImpedingWaypoints":  []any{},
			"OptimizedWaypoints": []any{},
			"TimeBreakdown":      map[string]any{},
		})

	case "SnapToRoads":
		return priced("SnapToRoadsRequest", map[string]any{
			"Notices":               []any{},
			"SnappedGeometry":       map[string]any{},
			"SnappedGeometryFormat": "Simple",
			"SnappedTracePoints":    []any{},
		})

	default:
		return nil, plugin.ErrUnhandledOp
	}
}

// pricingBucketHeader is where every operation in this service reports its
// pricing bucket. The model binds the member to a header rather than the body,
// and botocore reads it from there — a body member by that name is discarded
// without an error, so the caller would simply never see it.
const pricingBucketHeader = "x-amz-geo-pricing-bucket"

func priced(bucket string, body map[string]any) (*plugin.Response, error) {
	resp, err := shared.JSONResponse(http.StatusOK, body)
	if err != nil {
		return nil, err
	}
	resp.Headers = map[string]string{pricingBucketHeader: bucket}
	return resp, nil
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	return []plugin.Resource{}, nil
}

func init() {
	plugin.DefaultRegistry.Register("georoutes", func() plugin.ServicePlugin {
		return &Provider{}
	})
	// Unique signing name; declared for the same reason eksauth declares its
	// own — see crud.RegisterRoutes.
	crud.RegisterRoutes("georoutes", generated.OperationRoutes)
}
