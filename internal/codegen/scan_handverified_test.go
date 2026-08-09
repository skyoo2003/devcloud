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

// modelOperationsForTest reads the real Smithy models so the scan is exercised
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
	scans, err := ScanProviders("../services", modelOperationsForTest(t))
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

	// A directory registering two services splits by model: sts lives in
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

func TestScanProvidersIgnoresNonOperationLiterals(t *testing.T) {
	dir := t.TempDir()
	pkg := filepath.Join(dir, "demo")
	require.NoError(t, os.MkdirAll(pkg, 0o755))

	src := `package demo

func init() { plugin.DefaultRegistry.Register("demo", nil) }

func (p *Provider) HandleRequest(op string) string {
	switch op {
	case "GetWidget":
		return "get"
	case "ListWidgets":
		return "list"
	}
	switch op {
	case "attribute_exists", "GET", "NotAnOperation":
		return "noise"
	}
	return ""
}
`
	require.NoError(t, os.WriteFile(filepath.Join(pkg, "provider.go"), []byte(src), 0o600))

	got, err := ScanProviders(dir, map[string][]string{"demo": {"GetWidget", "ListWidgets"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"GetWidget", "ListWidgets"}, got["demo"].Operations,
		"literals the model does not declare must be dropped")
}

// TestPathRoutedOverridesAreReal guards the hand-maintained override: every
// declared operation must exist in the service's model, except the documented
// bedrock-runtime borrow.
func TestPathRoutedOverridesAreReal(t *testing.T) {
	models := modelOperationsForTest(t)
	known := map[string]bool{"InvokeModel": true} // bedrock-runtime, see pathRoutedOps

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
	models := modelOperationsForTest(t)
	first, err := ScanProviders("../services", models)
	require.NoError(t, err)
	second, err := ScanProviders("../services", models)
	require.NoError(t, err)

	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	assert.JSONEq(t, string(a), string(b))
}

// TestScanProvidersReadsProtocol pins the other half of the scan: the manifest
// must know the protocol a provider actually serves, which can differ from the
// Smithy model's. cloudwatch is the case that matters — its model declares JSON
// while the provider answers Query, and the CRUD engine serves only JSON.
func TestScanProvidersReadsProtocol(t *testing.T) {
	scans, err := ScanProviders("../services", modelOperationsForTest(t))
	require.NoError(t, err)

	assert.Equal(t, "Query", scans["cloudwatch"].Protocol)
	assert.Equal(t, "Query", scans["sqs"].Protocol)
	assert.Equal(t, "JSON11", scans["kms"].Protocol)
	assert.Equal(t, "JSON10", scans["dynamodb"].Protocol)
	assert.Equal(t, "RESTXML", scans["s3"].Protocol)

	assert.False(t, JSONProtocol("Query"))
	assert.False(t, JSONProtocol("RESTJSON"), "rest-json carries no X-Amz-Target, so the engine cannot serve it")
	assert.True(t, JSONProtocol("JSON11"))

	for id, scan := range scans {
		assert.NotEmpty(t, scan.Protocol, "%s: provider declares no protocol", id)
	}
}
