// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// validProtocols is the closed set of wire protocols a ServicePlugin may declare.
var validProtocols = map[plugin.ProtocolType]bool{
	plugin.ProtocolRESTXML:  true,
	plugin.ProtocolRESTJSON: true,
	plugin.ProtocolJSON10:   true,
	plugin.ProtocolJSON11:   true,
	plugin.ProtocolQuery:    true,
}

// TestServicePluginConformance locks the stable ServicePlugin contract against
// every service registered via the blank imports in imports.go. It runs after
// package init, so DefaultRegistry holds the full plugin surface. See
// docs/plugin-api.md for the contract this enforces.
func TestServicePluginConformance(t *testing.T) {
	ids := plugin.DefaultRegistry.RegisteredServices()

	// Conservative floor. Broken registration wiring (a mangled imports.go,
	// init-order regression, a dropped Register) collapses the live surface far
	// below this, so a constant catches it without the fragility of parsing
	// imports.go at runtime. Raise it if the real count ever nears it.
	const minServices = 50
	if len(ids) < minServices {
		t.Fatalf("registered %d services, want >= %d", len(ids), minServices)
	}

	var prev string
	for i, id := range ids {
		if id == "" {
			t.Error("RegisteredServices() contains an empty service ID")
			continue
		}
		// RegisteredServices() is documented to return a sorted slice.
		if i > 0 && id < prev {
			t.Errorf("RegisteredServices() not sorted: %q came before %q", prev, id)
		}
		prev = id

		p, ok := plugin.DefaultRegistry.Construct(id)
		if !ok {
			t.Errorf("%s: registered but Construct returned not-found", id)
			continue
		}
		if got := p.ServiceID(); got != id {
			t.Errorf("%s: ServiceID() = %q, want the registry key %q", id, got, id)
		}
		if p.ServiceName() == "" {
			t.Errorf("%s: ServiceName() is empty", id)
		}
		if proto := p.Protocol(); !validProtocols[proto] {
			t.Errorf("%s: Protocol() = %q is not a valid ProtocolType", id, proto)
		}
	}
}
