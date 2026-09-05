// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_crud_meta_test.go
package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// crudModel builds a minimal model with one list-returning and one
// item-returning output shape, so classifyOps has both output keys to find.
func crudModel(protocol string, ops ...ir.Operation) *ir.Model {
	return &ir.Model{
		ServiceID:  "testsvc",
		Protocol:   protocol,
		Operations: ops,
		Shapes: map[string]*ir.Shape{
			"GraphList": {Name: "GraphList", Type: ir.ShapeList},
			"Graph":     {Name: "Graph", Type: ir.ShapeStructure},
			"ListGraphsOutput": {Name: "ListGraphsOutput", Type: ir.ShapeStructure, Members: []ir.Member{
				{Name: "Graphs", TargetName: "GraphList"},
			}},
			"GetGraphOutput": {Name: "GetGraphOutput", Type: ir.ShapeStructure, Members: []ir.Member{
				{Name: "Graph", TargetName: "Graph"},
			}},
		},
	}
}

func opsOf(data CRUDServiceData) map[string]crudOpData {
	out := make(map[string]crudOpData, len(data.Ops))
	for _, op := range data.Ops {
		out[op.Op] = op
	}
	return out
}

// TestServiceCRUDDataAcceptsRESTJSON is the change that makes Milestone 4
// possible: 33 of the 57 demand-set services are restJson1, and refusing the
// protocol here is what left them registered but serving nothing.
func TestServiceCRUDDataAcceptsRESTJSON(t *testing.T) {
	model := crudModel("rest-json",
		ir.Operation{Name: "ListGraphs", OutputName: "ListGraphsOutput",
			HTTPMethod: "GET", HTTPUri: "/v1/graphs"},
		ir.Operation{Name: "GetGraph", OutputName: "GetGraphOutput",
			HTTPMethod: "GET", HTTPUri: "/v1/graphs/{GraphName}"},
	)

	data, ok := ServiceCRUDData(model)
	require.True(t, ok, "rest-json service must be engine-servable")

	ops := opsOf(data)
	require.Contains(t, ops, "ListGraphs")
	require.Contains(t, ops, "GetGraph")

	// Without the REST binding the engine has no way back from a request to an
	// operation name, so carrying it is the whole point.
	assert.Equal(t, "GET", ops["ListGraphs"].Method)
	assert.Equal(t, "/v1/graphs", ops["ListGraphs"].URI)
	assert.Equal(t, "/v1/graphs/{GraphName}", ops["GetGraph"].URI)

	// Classification itself is unchanged: the verb still comes from the name.
	assert.Equal(t, "List", ops["ListGraphs"].Verb)
	assert.Equal(t, "Graphs", ops["ListGraphs"].ListKey)
	assert.Equal(t, "Get", ops["GetGraph"].Verb)
	assert.Equal(t, "Graph", ops["GetGraph"].ItemKey)
}

// TestServiceCRUDDataJSONUnchanged pins that admitting rest-json did not alter
// what the JSON protocols produce. A JSON model has no HTTP binding, and
// emitting an empty URI must not create a route that matches everything.
func TestServiceCRUDDataJSONUnchanged(t *testing.T) {
	for _, protocol := range []string{"json-1.0", "json-1.1"} {
		t.Run(protocol, func(t *testing.T) {
			model := crudModel(protocol,
				ir.Operation{Name: "ListGraphs", OutputName: "ListGraphsOutput"},
			)

			data, ok := ServiceCRUDData(model)
			require.True(t, ok)

			ops := opsOf(data)
			assert.Equal(t, "List", ops["ListGraphs"].Verb)
			assert.Empty(t, ops["ListGraphs"].Method)
			assert.Empty(t, ops["ListGraphs"].URI)
		})
	}
}

// TestServiceCRUDDataAdmitsRESTXML is the codegen half of the rest-xml change.
// This gate and crud.Servable answer the same question and must agree: a
// protocol admitted here but refused there registers operations nothing can
// reach, and the fidelity manifest would publish them as auto-crud.
func TestServiceCRUDDataAdmitsRESTXML(t *testing.T) {
	model := crudModel("rest-xml",
		ir.Operation{Name: "ListAccessPoints", OutputName: "ListAccessPointsOutput",
			HTTPMethod: "GET", HTTPUri: "/v20180820/accesspoint"},
	)

	data, ok := ServiceCRUDData(model)
	require.True(t, ok, "rest-xml service must be engine-servable")

	ops := map[string]crudOpData{}
	for _, op := range data.Ops {
		ops[op.Op] = op
	}
	// The route is the whole classification story for rest-xml, so it has to
	// survive into the registry the same way rest-json's does.
	assert.Equal(t, "GET", ops["ListAccessPoints"].Method)
	assert.Equal(t, "/v20180820/accesspoint", ops["ListAccessPoints"].URI)
}

// TestServiceCRUDDataAdmitsQuery closes the last protocol gap. query has no
// modelled path at all — its operations carry no http trait — so unlike the
// REST protocols it registers no route, and the engine matches it by the
// Action field of the form body instead.
func TestServiceCRUDDataAdmitsQuery(t *testing.T) {
	model := crudModel("query",
		ir.Operation{Name: "DescribeLoadBalancers", OutputName: "DescribeLoadBalancersOutput"},
	)

	data, ok := ServiceCRUDData(model)
	require.True(t, ok, "query service must be engine-servable")

	ops := map[string]crudOpData{}
	for _, op := range data.Ops {
		ops[op.Op] = op
	}
	// No route, and that is correct rather than a gap: a query operation has
	// no method or URI to register, and Register skips an empty URI so the
	// service contributes nothing to the route table.
	assert.Empty(t, ops["DescribeLoadBalancers"].Method)
	assert.Empty(t, ops["DescribeLoadBalancers"].URI)
}

// TestServiceCRUDDataRejectsEC2Query holds the remaining boundary. ec2Query is
// form-encoded like query but not interchangeable with it, and the only service
// that speaks it has a hand-written provider that never reaches the engine.
func TestServiceCRUDDataRejectsEC2Query(t *testing.T) {
	model := crudModel("ec2-query",
		ir.Operation{Name: "DescribeInstances", OutputName: "DescribeInstancesOutput"},
	)

	_, ok := ServiceCRUDData(model)
	assert.False(t, ok, "ec2-query must not be engine-servable")
}

// TestServiceCRUDDataSkipsUnclassifiableService is the rds-data case: a
// rest-json service whose entire API is ExecuteStatement-shaped classifies
// nothing, so it registers nothing and routes nothing, and every call to it
// gets a clean error instead of an invented success.
func TestServiceCRUDDataSkipsUnclassifiableService(t *testing.T) {
	model := crudModel("rest-json",
		ir.Operation{Name: "ExecuteStatement", OutputName: "GetGraphOutput",
			HTTPMethod: "POST", HTTPUri: "/Execute"},
		ir.Operation{Name: "BeginTransaction", OutputName: "GetGraphOutput",
			HTTPMethod: "POST", HTTPUri: "/BeginTransaction"},
	)

	_, ok := ServiceCRUDData(model)
	assert.False(t, ok, "a service with no CRUD-shaped operation must register nothing")
}
