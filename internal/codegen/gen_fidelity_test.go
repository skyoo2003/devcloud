// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_fidelity_test.go
package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tierOf finds one operation's tier in the built data.
func tierOf(t *testing.T, data FidelityData, serviceID, op string) string {
	t.Helper()
	for _, svc := range data.Services {
		if svc.ServiceID != serviceID {
			continue
		}
		for _, o := range svc.Ops {
			if o.Name == op {
				return o.TierConst
			}
		}
		require.Failf(t, "operation missing", "%s/%s is absent from the manifest", serviceID, op)
	}
	require.Failf(t, "service missing", "%s is absent from the manifest", serviceID)
	return ""
}

// TestBuildFidelityDataRequiresEngineWiring is the guard against the manifest
// overstating coverage.
//
// Being in the CRUD registry only means an operation is *classifiable* — the
// engine is reached at runtime only for providers that return
// plugin.ErrUnhandledOp. Labelling an operation auto-crud on registry
// membership alone publishes "this is served" for a provider that refuses it,
// which is exactly what a freshly scaffolded service would look like.
func TestBuildFidelityDataRequiresEngineWiring(t *testing.T) {
	modelOps := map[string][]string{
		"wired":   {"CreateThing"},
		"unwired": {"CreateThing"},
	}
	// Neither service hand-implements anything; the only difference is whether
	// its dispatch reaches the engine.
	providers := map[string]ProviderScan{
		"wired":   {EngineWired: true},
		"unwired": {EngineWired: false},
	}
	autoCRUD := []CRUDServiceData{
		{ServiceID: "wired", Ops: []crudOpData{{Op: "CreateThing", Verb: "Create"}}},
		{ServiceID: "unwired", Ops: []crudOpData{{Op: "CreateThing", Verb: "Create"}}},
	}

	data := BuildFidelityData(modelOps, map[string]string{"demo": "json-1.1"}, providers, autoCRUD)

	assert.Equal(t, "AutoCRUD", tierOf(t, data, "wired", "CreateThing"),
		"an engine-wired provider's CRUD-shaped operation is served, so auto-crud is truthful")
	assert.Equal(t, "Unimplemented", tierOf(t, data, "unwired", "CreateThing"),
		"a provider that never reaches the engine serves nothing, whatever the CRUD registry says")
}

// TestBuildFidelityDataHandVerifiedBeatsWiring keeps the documented precedence:
// a hand-written implementation always wins, and it does not depend on engine
// wiring because such an operation never falls through to the engine at all.
func TestBuildFidelityDataHandVerifiedBeatsWiring(t *testing.T) {
	modelOps := map[string][]string{"svc": {"CreateThing"}}
	providers := map[string]ProviderScan{
		"svc": {Operations: []string{"CreateThing"}, EngineWired: false},
	}
	autoCRUD := []CRUDServiceData{
		{ServiceID: "svc", Ops: []crudOpData{{Op: "CreateThing", Verb: "Create"}}},
	}

	data := BuildFidelityData(modelOps, map[string]string{"demo": "json-1.1"}, providers, autoCRUD)

	assert.Equal(t, "HandVerified", tierOf(t, data, "svc", "CreateThing"),
		"hand-verified outranks both auto-crud and unimplemented")
}
