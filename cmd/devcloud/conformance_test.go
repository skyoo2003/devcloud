// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"strings"
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

	// Every blank import in imports.go registers at least one service, so the
	// live surface must be at least that large. Deriving the floor from the
	// import list keeps this honest as services are added or removed, instead
	// of drifting against a hardcoded count.
	if want := countServiceImports(t); len(ids) < want {
		t.Fatalf("registered %d services, want >= %d from imports.go blank imports", len(ids), want)
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

// countServiceImports counts the blank service imports in imports.go so the
// conformance floor tracks the generated import list rather than a magic number.
func countServiceImports(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("imports.go")
	if err != nil {
		t.Fatalf("read imports.go: %v", err)
	}
	return strings.Count(string(b), `_ "github.com/skyoo2003/devcloud/internal/services/`)
}
