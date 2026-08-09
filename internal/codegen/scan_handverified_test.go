// SPDX-License-Identifier: Apache-2.0

// internal/codegen/scan_handverified_test.go
package codegen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// modelOperationsForTest reads the real Smithy models so the scan is checked
// against the same input the generator uses.
func modelOperationsForTest(t *testing.T) map[string][]string {
	t.Helper()
	entries, err := os.ReadDir("../../smithy-models")
	require.NoError(t, err)

	ops := make(map[string][]string)
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join("../../smithy-models", e.Name()))
		require.NoError(t, err)
		model, err := ParseSmithyJSON(data)
		if err != nil {
			continue
		}
		names := make([]string, 0, len(model.Operations))
		for _, op := range model.Operations {
			names = append(names, op.Name)
		}
		ops[model.ServiceID] = names
	}
	return ops
}

func TestScanProviders(t *testing.T) {
	scans, err := ScanProviders("../services")
	require.NoError(t, err)
	got := make(map[string][]string, len(scans))
	for id, scan := range scans {
		got[id] = scan.Operations
	}

	// Every registered service resolves; the count tracks internal/services.
	assert.GreaterOrEqual(t, len(got), 100, "expected the full service fleet")

	// Query and JSON providers resolve from their dispatch literals.
	assert.Contains(t, got["acm"], "DescribeCertificate")
	assert.Contains(t, got["sqs"], "SendMessage")
	assert.Contains(t, got["dynamodb"], "PutItem")

	// A package registering two services splits by receiver type: sts lives in
	// internal/services/iam, and neither service claims the other's operations.
	assert.Contains(t, got["sts"], "AssumeRole")
	assert.NotContains(t, got["iam"], "AssumeRole")
	assert.Contains(t, got["iam"], "CreateUser")

	// Path-routed providers come from the override.
	assert.Contains(t, got["s3"], "PutObject")
	assert.Contains(t, got["bedrock"], "InvokeModel")

	// No service is silently empty — an empty set means the scan lost a provider.
	for id, ops := range got {
		assert.NotEmpty(t, ops, "%s resolved no hand-verified operations", id)
	}
}

// TestScanProvidersKeepsOperationsTheModelOmits is the regression that made the
// scan drop real coverage: it used to intersect with the Smithy model, so every
// operation a provider serves beyond its in-tree model vanished from the
// manifest. dynamodbstreams dispatches 22 operations against a 4-operation
// model; acm serves UpdateCertificate, which its model does not declare.
func TestScanProvidersKeepsOperationsTheModelOmits(t *testing.T) {
	scans, err := ScanProviders("../services")
	require.NoError(t, err)
	models := modelOperationsForTest(t)

	assert.Greater(t, len(scans["dynamodbstreams"].Operations), len(models["dynamodbstreams"]),
		"the provider serves more than the model declares")
	assert.Contains(t, scans["dynamodbstreams"].Operations, "ListStreamArns")
	assert.Contains(t, scans["acm"].Operations, "UpdateCertificate")
}

// TestScanProvidersIgnoresLiteralsOutsideDispatch is the other half of dropping
// the model intersection: without it, the only thing keeping non-operation
// strings out of the manifest is the scan's scope. Attribute-path switches and
// HTTP-verb switches live in helpers, not HandleRequest, and must not leak —
// identitystore matched on "DisplayName", pipes on "POST".
func TestScanProvidersIgnoresLiteralsOutsideDispatch(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "demo")
	require.NoError(t, os.MkdirAll(pkg, 0o755))

	src := `package demo

func init() {
	plugin.DefaultRegistry.Register("demo", func() plugin.ServicePlugin { return &Provider{} })
}

func (p *Provider) HandleRequest(op string) string {
	switch op {
	case "GetWidget":
		return "get"
	case "ListWidgets":
		return "list"
	case "GET", "id":
		return "not an operation name"
	}
	return ""
}

func applyPatch(path string) string {
	switch path {
	case "DisplayName", "Description":
		return "attribute, not an operation"
	}
	return ""
}
`
	require.NoError(t, os.WriteFile(filepath.Join(pkg, "provider.go"), []byte(src), 0o600))

	got, err := ScanProviders(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"GetWidget", "ListWidgets"}, got["demo"].Operations)
}

func TestScanProvidersRealServicesCarryNoStrays(t *testing.T) {
	scans, err := ScanProviders("../services")
	require.NoError(t, err)

	for _, stray := range []string{"DisplayName", "Description", "Emails"} {
		assert.NotContains(t, scans["identitystore"].Operations, stray)
	}
	for _, stray := range []string{"POST", "DELETE"} {
		assert.NotContains(t, scans["pipes"].Operations, stray)
	}
}

// TestPathRoutedOverridesAreReal guards the hand-maintained override: every
// declared operation must exist in the service's model, except the documented
// bedrock-runtime borrows.
func TestPathRoutedOverridesAreReal(t *testing.T) {
	models := modelOperationsForTest(t)
	// bedrock-runtime operations, served by the bedrock provider. See pathRoutedOps.
	known := map[string]bool{"InvokeModel": true, "InvokeModelWithResponseStream": true}

	for id, declared := range pathRoutedOps {
		modelSet := make(map[string]bool, len(models[id]))
		for _, op := range models[id] {
			modelSet[op] = true
		}
		for _, op := range declared {
			if known[op] {
				continue
			}
			assert.True(t, modelSet[op], "%s: %q is not an operation in the model", id, op)
		}
	}
}

// TestScanProvidersIsDeterministic keeps generated output stable across runs.
func TestScanProvidersIsDeterministic(t *testing.T) {
	first, err := ScanProviders("../services")
	require.NoError(t, err)
	second, err := ScanProviders("../services")
	require.NoError(t, err)

	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	assert.JSONEq(t, string(a), string(b))
}
