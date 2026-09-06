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

// TestBuildFidelityDataPromotesShortDeclaredOperations covers the operation name
// too short for the scanner's shape test.
//
// opNamePattern requires four characters, so `Untag` is scanned and `Tag` is
// not, even though resourcegroups implements both in the same switch. The
// manifest called Tag unimplemented, a probe asked for it, got the 200 the
// provider has always returned, and reported a fabricated success that was
// never fabricated — the manifest was simply wrong about served code.
//
// Loosening the pattern alone is not the fix: at two characters it admits the
// HTTP-verb literals that path-resolver switches are full of. The model is what
// separates them. `Tag` is an operation because resourcegroups.json says so;
// `GET` is not an operation in any model AWS publishes.
func TestBuildFidelityDataPromotesShortDeclaredOperations(t *testing.T) {
	modelOps := map[string][]string{"svc": {"Tag", "Untag", "ListGroups"}}
	providers := map[string]ProviderScan{
		"svc": {
			Operations: []string{"Untag", "ListGroups"},
			// Below the strict shape test. Only the model decides which of
			// these is an operation.
			ShortOperations: []string{"Tag", "GET", "PUT"},
			EngineWired:     false,
		},
	}

	data := BuildFidelityData(modelOps, map[string]string{"svc": "rest-json"}, providers, nil)

	assert.Equal(t, "HandVerified", tierOf(t, data, "svc", "Tag"),
		"the model declares Tag and the provider dispatches it, so it is served")
	assert.Equal(t, "HandVerified", tierOf(t, data, "svc", "Untag"),
		"the long name was never in doubt and must not regress")

	for _, svc := range data.Services {
		if svc.ServiceID != "svc" {
			continue
		}
		for _, op := range svc.Ops {
			assert.NotContains(t, []string{"GET", "PUT"}, op.Name,
				"a short literal no model declares must not become an operation")
		}
	}
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
