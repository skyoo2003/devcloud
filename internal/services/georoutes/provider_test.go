// SPDX-License-Identifier: Apache-2.0

// internal/services/georoutes/provider_test.go
package georoutes

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callREST(t *testing.T, p *Provider, method, path, op, body string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.HandleRequest(context.Background(), op, req)
	require.NoError(t, err)
	return resp
}

func parseBody(t *testing.T, resp *plugin.Response) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(resp.Body, &m))
	return m
}

// TestEveryOperation calls all five by path, so the generated route table is
// exercised as well as the dispatch. Members are PascalCase on the wire.
//
// PricingBucket is absent from these lists on purpose: the model binds it to a
// header, and TestPricingBucketIsAHeader asserts it there.
func TestEveryOperation(t *testing.T) {
	p := newTestProvider(t)

	cases := []struct {
		op      string
		path    string
		members []string
	}{
		{"CalculateRoutes", "/v2/routes",
			[]string{"LegGeometryFormat", "Notices", "Routes"}},
		{"CalculateIsolines", "/v2/isolines",
			[]string{"ArrivalTime", "DepartureTime", "IsolineGeometryFormat", "Isolines",
				"SnappedDestination", "SnappedOrigin"}},
		{"CalculateRouteMatrix", "/v2/route-matrix",
			[]string{"ErrorCount", "RouteMatrix", "RoutingBoundary"}},
		{"OptimizeWaypoints", "/v2/optimize-waypoints",
			[]string{"Connections", "Distance", "Duration", "ImpedingWaypoints",
				"OptimizedWaypoints", "TimeBreakdown"}},
		{"SnapToRoads", "/v2/snap-to-roads",
			[]string{"Notices", "SnappedGeometry", "SnappedGeometryFormat",
				"SnappedTracePoints"}},
	}
	require.Len(t, cases, 5, "every operation the model declares must be called")

	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			resp := callREST(t, p, "POST", c.path, "", `{"Origin":[0,0],"Destination":[1,1]}`)
			assert.Equal(t, 200, resp.StatusCode)

			rb := parseBody(t, resp)
			for _, m := range c.members {
				assert.Contains(t, rb, m)
			}
		})
	}
}

// TestPricingBucketIsAHeader covers the one response member this service does
// not bind to the body. The model puts PricingBucket in the
// x-amz-geo-pricing-bucket header, and botocore reads it from there — a member
// by that name in the body is discarded without an error, so the caller simply
// never sees it.
func TestPricingBucketIsAHeader(t *testing.T) {
	p := newTestProvider(t)

	cases := []struct{ op, path, bucket string }{
		{"CalculateRoutes", "/v2/routes", "RoutesRequest"},
		{"CalculateIsolines", "/v2/isolines", "IsolinesRequest"},
		{"CalculateRouteMatrix", "/v2/route-matrix", "RouteMatrixRequest"},
		{"OptimizeWaypoints", "/v2/optimize-waypoints", "OptimizeWaypointsRequest"},
		{"SnapToRoads", "/v2/snap-to-roads", "SnapToRoadsRequest"},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			resp := callREST(t, p, "POST", c.path, "", "{}")
			require.Equal(t, 200, resp.StatusCode)

			assert.Equal(t, c.bucket, resp.Headers["x-amz-geo-pricing-bucket"])
			assert.NotContains(t, parseBody(t, resp), "PricingBucket",
				"the model binds this to a header; a body member by that name is dropped")
		})
	}
}

// An empty body is what the smoke suite sends; no operation here reads one.
func TestEmptyBodyStillAnswers(t *testing.T) {
	p := newTestProvider(t)

	resp := callREST(t, p, "POST", "/v2/routes", "", "")
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, []any{}, parseBody(t, resp)["Routes"])
}

// TestProviderIdentity pins what the registry and the fidelity manifest read.
// A typo in ServiceName is invisible everywhere else.
func TestProviderIdentity(t *testing.T) {
	p := newTestProvider(t)
	assert.Equal(t, "georoutes", p.ServiceID())
	assert.Equal(t, "RoutesService", p.ServiceName())
	assert.Equal(t, plugin.ProtocolRESTJSON, p.Protocol())

	resources, err := p.ListResources(context.Background())
	require.NoError(t, err)
	assert.Empty(t, resources)
}

func TestUnknownOperationIsUnhandled(t *testing.T) {
	p := newTestProvider(t)
	req := httptest.NewRequest("POST", "/v2/nope", strings.NewReader("{}"))
	_, err := p.HandleRequest(context.Background(), "NoSuchThing", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)

	// An unmodelled path resolves to no operation at all, which must decline
	// the same way rather than answering for some other route.
	req = httptest.NewRequest("POST", "/v2/nope", strings.NewReader("{}"))
	_, err = p.HandleRequest(context.Background(), "", req)
	assert.ErrorIs(t, err, plugin.ErrUnhandledOp)
}
