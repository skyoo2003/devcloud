// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_fidelity.go
package codegen

import (
	"fmt"
	"go/format"
	"sort"
)

// FidelityData drives fidelity_manifest.go.tmpl: every service DevCloud
// registers, with each of its operations labelled by tier.
type FidelityData struct {
	Services []FidelityServiceData
}

// FidelityServiceData is one service's classified operations.
type FidelityServiceData struct {
	ServiceID   string
	ModelBacked bool
	Ops         []FidelityOpData
}

// FidelityOpData is one operation and the template constant naming its tier.
type FidelityOpData struct {
	Name string
	// TierConst is the suffix of the generated Tier constant: HandVerified,
	// AutoCRUD or Unimplemented.
	TierConst string
}

// BuildFidelityData joins the three inputs that decide an operation's tier.
//
// The universe of a service is its model's operations *union* what the provider
// actually serves: a provider may implement an operation the in-tree model does
// not declare (bedrock serves InvokeModel, which AWS models under
// bedrock-runtime), and dropping it would hide a served operation.
//
// Precedence is hand-verified > auto-crud > unimplemented, mirroring the runtime:
// the CRUD engine is reached only when a provider's dispatch falls through, so a
// hand-written implementation always wins. See docs/crud-engine.md.
//
// Registry membership alone does not earn the auto-crud label. It says an
// operation is *classifiable*, while the engine is reached at runtime only for
// providers that return plugin.ErrUnhandledOp — so an unwired provider's
// CRUD-shaped operations are refused, and calling them auto-crud would publish
// coverage the binary does not serve.
func BuildFidelityData(
	modelOps map[string][]string,
	providers map[string]ProviderScan,
	autoCRUD []CRUDServiceData,
) FidelityData {
	crudOps := make(map[string]map[string]bool, len(autoCRUD))
	for _, svc := range autoCRUD {
		ops := make(map[string]bool, len(svc.Ops))
		for _, op := range svc.Ops {
			ops[op.Op] = true
		}
		crudOps[svc.ServiceID] = ops
	}

	data := FidelityData{}
	for _, serviceID := range sortedKeys(providers) {
		provider := providers[serviceID]

		// Reachable, not merely classifiable: an unwired provider serves none of
		// the registry's operations, so its served set is empty and its
		// CRUD-shaped operations fall through to unimplemented.
		var served map[string]bool
		if provider.EngineWired {
			served = crudOps[serviceID]
		}

		hand := make(map[string]bool, len(provider.Operations))
		universe := make(map[string]bool)
		for _, op := range provider.Operations {
			hand[op] = true
			universe[op] = true
		}
		for _, op := range modelOps[serviceID] {
			universe[op] = true
		}
		for op := range crudOps[serviceID] {
			universe[op] = true
		}

		names := make([]string, 0, len(universe))
		for op := range universe {
			names = append(names, op)
		}
		sort.Strings(names)

		ops := make([]FidelityOpData, 0, len(names))
		for _, name := range names {
			ops = append(ops, FidelityOpData{
				Name:      name,
				TierConst: tierFor(name, hand, served),
			})
		}

		data.Services = append(data.Services, FidelityServiceData{
			ServiceID:   serviceID,
			ModelBacked: len(modelOps[serviceID]) > 0,
			Ops:         ops,
		})
	}
	return data
}

// tierFor labels one operation. served holds the engine-servable operations of
// an engine-wired provider, and is empty for a provider the engine never
// reaches — so an unwired service can only produce hand-verified or
// unimplemented, never auto-crud.
func tierFor(op string, hand, served map[string]bool) string {
	switch {
	case hand[op]:
		return "HandVerified"
	case served[op]:
		return "AutoCRUD"
	default:
		return "Unimplemented"
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// GenerateFidelityManifest renders the single aggregate manifest file.
func (g *Generator) GenerateFidelityManifest(data FidelityData) (string, error) {
	rendered, err := g.renderTemplate("fidelity_manifest.go.tmpl", data)
	if err != nil {
		return "", err
	}
	formatted, err := format.Source([]byte(rendered))
	if err != nil {
		return "", fmt.Errorf("gofmt fidelity_manifest.go: %w", err)
	}
	return string(formatted), nil
}
