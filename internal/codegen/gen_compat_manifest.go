// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_compat_manifest.go
package codegen

import "encoding/json"

// CompatService is one service's entry in the compatibility suite's manifest.
type CompatService struct {
	// Protocol is the service's wire protocol, reported so a failing case names
	// it rather than making a reader look it up.
	Protocol string `json:"protocol"`
	// ServedOps are the operations the fidelity manifest says actually answer —
	// hand-verified or auto-crud, never unimplemented. The suite picks its probe
	// from this set, so it never asks a service for an operation the manifest
	// has already declared unserved and then reports the honest failure as a
	// broken service.
	//
	// Empty is meaningful: it is what makes a service a decline case, and the
	// "never a fabricated success" guarantee applies to exactly those.
	ServedOps []string `json:"servedOps"`
}

// BuildCompatManifest projects the fidelity data down to what the boto3
// compatibility suite needs to parametrize itself.
//
// The suite used to carry two hand-written service lists, which is why a service
// could be registered, counted in the published coverage figure, and exercised
// by no test at all: adding it to internal/services was one step and adding it
// to the list was another, and nothing failed when only the first happened.
// Deriving the list here means the second step no longer exists.
//
// Registered, not served, is the membership rule. A service that answers
// nothing still appears, with an empty ServedOps — it is the one whose "declines
// with a clean AWS error rather than a fabricated success" guarantee most needs
// a test, and it is the case docs/coverage.md calls absolute.
func BuildCompatManifest(data FidelityData) map[string]CompatService {
	manifest := make(map[string]CompatService, len(data.Services))
	for _, svc := range data.Services {
		// Non-nil so the JSON is [] rather than null: Python distinguishes them,
		// and "no served operations" is a real state rather than a missing field.
		served := make([]string, 0, len(svc.Ops))
		for _, op := range svc.Ops {
			if op.TierConst == "Unimplemented" {
				continue
			}
			served = append(served, op.Name)
		}
		manifest[svc.ServiceID] = CompatService{
			Protocol:  svc.Protocol,
			ServedOps: served,
		}
	}
	return manifest
}

// RenderCompatManifest marshals the manifest for the compatibility suite.
//
// encoding/json sorts map keys and FidelityData arrives with its operations
// already sorted, so the output is byte-stable across runs — which is what the
// codegen-drift CI job requires of everything under internal/generated.
func RenderCompatManifest(manifest map[string]CompatService) ([]byte, error) {
	out, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}
