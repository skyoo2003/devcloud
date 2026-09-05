// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_compat_manifest_test.go
package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fidelityFixture builds the manifest input for a two-service fleet: one that
// serves something, one that serves nothing at all.
func fidelityFixture() FidelityData {
	modelOps := map[string][]string{
		"served":   {"CreateThing", "ListThings", "RunSomething"},
		"declines": {"ExecuteStatement"},
	}
	providers := map[string]ProviderScan{
		"served":   {Operations: []string{"CreateThing"}, EngineWired: true},
		"declines": {EngineWired: true},
	}
	autoCRUD := []CRUDServiceData{
		{ServiceID: "served", Ops: []crudOpData{{Op: "ListThings", Verb: "List"}}},
	}
	protocols := map[string]string{"served": "json-1.1", "declines": "rest-json"}

	return BuildFidelityData(modelOps, protocols, providers, autoCRUD)
}

// TestBuildCompatManifestListsEveryRegisteredService is the property the
// coverage gate rests on: the compatibility suite parametrizes over this file,
// so a service missing from it is a service nothing tests. A service that serves
// nothing must still appear — it is the one whose "declines cleanly" guarantee
// most needs a test.
func TestBuildCompatManifestListsEveryRegisteredService(t *testing.T) {
	manifest := BuildCompatManifest(fidelityFixture())

	require.Len(t, manifest, 2)
	assert.Contains(t, manifest, "served")
	assert.Contains(t, manifest, "declines",
		"a service that serves nothing is still tested, for the decline guarantee")
}

// TestBuildCompatManifestServedOpsExcludeUnimplemented keeps the suite from
// asking a service for an operation the manifest already says is unimplemented.
// Such a call fails, correctly, and would read as the service being broken.
func TestBuildCompatManifestServedOpsExcludeUnimplemented(t *testing.T) {
	manifest := BuildCompatManifest(fidelityFixture())

	assert.Equal(t, []string{"CreateThing", "ListThings"}, manifest["served"].ServedOps,
		"hand-verified and auto-crud are served; RunSomething is unimplemented and must not appear")
	assert.Empty(t, manifest["declines"].ServedOps,
		"a service with no served operation reports none, which is what makes it a decline case")
}

// TestBuildCompatManifestCarriesProtocol publishes why a service serves what it
// does. docs/coverage.md splits the fleet by protocol, and the compat suite
// reports it on failure so a broken service names its own protocol.
func TestBuildCompatManifestCarriesProtocol(t *testing.T) {
	manifest := BuildCompatManifest(fidelityFixture())

	assert.Equal(t, "json-1.1", manifest["served"].Protocol)
	assert.Equal(t, "rest-json", manifest["declines"].Protocol)
}

// TestRenderCompatManifestIsStable keeps `make codegen` idempotent. The
// codegen-drift CI job diffs the regenerated tree, so unstable ordering here
// would fail every unrelated pull request.
func TestRenderCompatManifestIsStable(t *testing.T) {
	first, err := RenderCompatManifest(BuildCompatManifest(fidelityFixture()))
	require.NoError(t, err)
	second, err := RenderCompatManifest(BuildCompatManifest(fidelityFixture()))
	require.NoError(t, err)

	assert.Equal(t, string(first), string(second))
	assert.Contains(t, string(first), `"protocol": "json-1.1"`,
		"the file is read by Python, so it is indented JSON rather than a Go literal")
}
