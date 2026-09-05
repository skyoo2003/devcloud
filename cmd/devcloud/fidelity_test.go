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

		served := 0
		for op, tier := range svc.Operations {
			if !validTiers[tier] {
				t.Errorf("%s/%s: tier %q is not a declared tier", id, op, tier)
			}
			if tier == fidelity.TierHandVerified || tier == fidelity.TierAutoCRUD {
				served++
			}
		}
		// Every routed service must serve something — unless its protocol puts
		// it out of the CRUD engine's reach and nobody has written it by hand.
		//
		// The engine classifies from the operation name, which only the
		// X-Amz-Target JSON protocols carry (crud.JSONProtocol). A restJson1,
		// query or restXml service therefore serves nothing until it gets a
		// hand-written provider. Registering it anyway is deliberate: it routes,
		// and it declines with a clean AWS error instead of the caller falling
		// through to real AWS. What it must never do is count as covered — see
		// docs/coverage.md, which publishes the registered / engine-served split
		// rather than the registered number alone.
		//
		// The defect this leaves is the sharp one: the engine holds classified
		// operations for the service, and the manifest still says none is
		// served. That is wiring, not protocol — the dispatch scan lost a
		// provider (a new path-routing provider needs a pathRoutedOps entry in
		// internal/codegen/scan_handverified.go), or a provider declines without
		// returning plugin.ErrUnhandledOp and so never reaches the engine.
		//
		// An engine-servable service with nothing in the registry is a third,
		// legitimate case: forecastquery is json-1.1 but its whole API is
		// QueryForecast / QueryWhatIfForecast, and neither is CRUD-shaped. There
		// is nothing to serve generically, so zero served is the truth.
		if served == 0 && len(crud.RegisteredOps(id)) > 0 {
			t.Errorf("%s: the CRUD engine holds %d operations for it, but the manifest serves none",
				id, len(crud.RegisteredOps(id)))
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

// TestFidelityManifestCoversCRUDRegistry checks the other direction, against a
// source the manifest was not generated from: every operation the CRUD engine
// would serve at runtime is declared, and never as unimplemented — that would
// promise a failure the runtime does not deliver. This is also the only guard
// that can notice an engine-served operation going *missing* from the manifest;
// TestFidelityManifestCoverage can only inspect what the manifest already
// lists.
func TestFidelityManifestCoversCRUDRegistry(t *testing.T) {
	for _, id := range plugin.DefaultRegistry.RegisteredServices() {
		// Registry membership is classifiability, not reachability. The CRUD
		// registry is built from the model, so it also holds operations for
		// services whose hand-written provider refuses unknown operations
		// itself (apigatewayv2, xray) — the engine is never routed to for
		// those, so "unimplemented" is the truth and this check would be
		// asserting the opposite. TestFidelityManifestCoverage's `served == 0
		// && RegisteredOps > 0` guard is what catches wiring that goes missing.
		if !fidelity.Services[id].EngineWired {
			continue
		}
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

// TestAutoCRUDIsServedOverJSON pins what makes an auto-crud tier true. The
// gateway derives the protocol from the *request* (gateway.DetectProtocol), not
// from the provider, so a service whose provider declares Query still reaches
// the engine when a client speaks JSON. cloudwatch is the case that matters: its
// provider answers Query for boto3 and falls through to the engine for
// X-Amz-Target callers. Filtering the registry by the provider's declared
// protocol once removed that coverage outright.
func TestAutoCRUDIsServedOverJSON(t *testing.T) {
	for id, svc := range fidelity.Services {
		for op, tier := range svc.Operations {
			if tier != fidelity.TierAutoCRUD {
				continue
			}
			// Asked through the JSON path regardless of the service's real
			// protocol: what "auto-crud" claims is that the operation is
			// registered and CRUD-classified, which is exactly what this
			// exercises. Whether a rest-json request also resolves to it is
			// route matching, covered in internal/shared/crud and the compat
			// suite.
			if _, err := crud.Handle(crud.Call{Service: id, Protocol: "json-1.1", Op: op, Body: []byte("{}")}); err != nil {
				t.Errorf("%s/%s: declared auto-crud but the engine refused it: %v", id, op, err)
			}
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
