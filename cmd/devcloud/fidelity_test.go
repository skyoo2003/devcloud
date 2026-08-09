// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/skyoo2003/devcloud/internal/generated/fidelity"
	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/skyoo2003/devcloud/internal/shared/crud"
)

// validTiers is the closed set of fidelity tiers. See docs/fidelity-manifest.md.
var validTiers = map[fidelity.Tier]bool{
	fidelity.TierHandVerified:  true,
	fidelity.TierAutoCRUD:      true,
	fidelity.TierUnimplemented: true,
}

// TestFidelityManifestCoverage is the guarantee behind v1.0's fidelity claim:
// every operation DevCloud can serve carries a declared tier. It runs after
// package init, so both the plugin registry and the CRUD registry hold their
// full surface. Regenerate with `make codegen` when it fails.
func TestFidelityManifestCoverage(t *testing.T) {
	// Conservative floors, in the spirit of TestServicePluginConformance: a
	// mangled generator or a dropped scan collapses these far below the real
	// numbers without the fragility of asserting an exact count.
	const (
		minServices   = 100
		minOperations = 6000
	)

	if len(fidelity.Services) < minServices {
		t.Fatalf("manifest covers %d services, want >= %d", len(fidelity.Services), minServices)
	}

	operations := 0
	for id, svc := range fidelity.Services {
		if len(svc.Operations) == 0 {
			t.Errorf("%s: manifest lists no operations", id)
			continue
		}
		operations += len(svc.Operations)

		handVerified := 0
		for op, tier := range svc.Operations {
			if !validTiers[tier] {
				t.Errorf("%s/%s: tier %q is not a declared tier", id, op, tier)
			}
			if tier == fidelity.TierHandVerified {
				handVerified++
			}
		}
		// A service with nothing hand-verified means the scan lost its provider —
		// most likely a new path-routing provider that needs a pathRoutedOps
		// entry in internal/codegen/scan_handverified.go.
		if handVerified == 0 {
			t.Errorf("%s: no hand-verified operations; the dispatch scan probably missed this provider", id)
		}
	}
	if operations < minOperations {
		t.Errorf("manifest covers %d operations, want >= %d", operations, minOperations)
	}
}

// TestFidelityManifestCoversRegisteredServices keeps the manifest honest about
// the live surface: anything the gateway routes to must be classified.
func TestFidelityManifestCoversRegisteredServices(t *testing.T) {
	for _, id := range plugin.DefaultRegistry.RegisteredServices() {
		if _, ok := fidelity.Services[id]; !ok {
			t.Errorf("%s: registered service is absent from the fidelity manifest", id)
		}
	}
}

// TestFidelityManifestCoversCRUDRegistry checks the other direction: every
// operation the CRUD engine would serve is declared, and never as unimplemented
// — that would promise an InvalidAction the runtime does not return.
func TestFidelityManifestCoversCRUDRegistry(t *testing.T) {
	for _, id := range plugin.DefaultRegistry.RegisteredServices() {
		for op := range crud.RegisteredOps(id) {
			tier, ok := fidelity.Lookup(id, op)
			if !ok {
				t.Errorf("%s/%s: engine-registered operation is absent from the manifest", id, op)
				continue
			}
			if tier == fidelity.TierUnimplemented {
				t.Errorf("%s/%s: declared unimplemented but the CRUD engine serves it", id, op)
			}
		}
	}
}

// TestAutoCRUDIsReachable guards against the manifest promising a tier the
// runtime cannot deliver. The CRUD engine refuses any non-JSON protocol
// (crud.Handle → JSONProtocol), so an operation declared auto-crud on a
// Query or REST provider would in fact return InvalidAction.
func TestAutoCRUDIsReachable(t *testing.T) {
	for id, svc := range fidelity.Services {
		autoCRUD := 0
		for _, tier := range svc.Operations {
			if tier == fidelity.TierAutoCRUD {
				autoCRUD++
			}
		}
		if autoCRUD == 0 {
			continue
		}

		p, ok := plugin.DefaultRegistry.Construct(id)
		if !ok {
			t.Errorf("%s: declares %d auto-crud operations but is not registered", id, autoCRUD)
			continue
		}
		if !crud.JSONProtocol(string(p.Protocol())) {
			t.Errorf("%s: declares %d auto-crud operations but serves %q; the CRUD engine only answers JSON protocols, so those operations really return InvalidAction",
				id, autoCRUD, p.Protocol())
		}
	}
}

// TestFidelityLookup pins the accessor's contract: an unknown service and an
// unknown operation are both misses, not a zero-value tier.
func TestFidelityLookup(t *testing.T) {
	if tier, ok := fidelity.Lookup("s3", "PutObject"); !ok || tier != fidelity.TierHandVerified {
		t.Errorf("Lookup(s3, PutObject) = %q, %v; want hand-verified, true", tier, ok)
	}
	if _, ok := fidelity.Lookup("nosuchservice", "PutObject"); ok {
		t.Error("Lookup on an unknown service reported a hit")
	}
	if _, ok := fidelity.Lookup("s3", "NoSuchOperation"); ok {
		t.Error("Lookup on an unknown operation reported a hit")
	}
}
