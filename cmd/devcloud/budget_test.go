// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// fleetBudget is an order-of-magnitude ceiling, deliberately far above the
// figure docs/coverage.md publishes. The published number is a measurement on a
// quiet machine; this runs on a shared CI runner, where a 2x reading is
// indistinguishable from a noisy neighbour. A budget tight enough to catch a 2x
// regression would therefore fail for reasons that have nothing to do with
// DevCloud, so this one catches the regression that is real — a provider that
// starts doing per-call work at startup, which shows up as 10x, not 2x.
//
// Measured at 135-155 ms locally for all 431 services. Injecting 8 ms of work
// per service takes it to 4.0 s.
const fleetBudget = 2 * time.Second

// TestRegisteredFleetComesUpWithinItsBudget gates the startup half of the
// runtime cost docs/coverage.md publishes.
//
// It brings every registered service up the way main.go does — factory, then
// Init against a per-service data directory — because that is where the cost
// is. Registration itself is a map insert and does not get slower. What gets
// slower is Init: s3 alone does an os.MkdirAll and opens a SQLite database
// there, and 431 of those is how a 42 ms startup becomes a second.
//
// An earlier version of this test timed Construct instead, which only calls the
// factory. Every factory in the tree is a struct literal, so it measured 84 µs
// against a 150 ms budget and would have passed unchanged while startup
// regressed arbitrarily — verified by injecting 8 ms per service, which that
// version did not notice and this one fails on.
//
// The wall-clock figures in docs/coverage.md stay a measurement, re-taken per
// release. This is the gate that notices between measurements.
func TestRegisteredFleetComesUpWithinItsBudget(t *testing.T) {
	ids := plugin.DefaultRegistry.RegisteredServices()
	if len(ids) == 0 {
		t.Fatal("no services are registered; imports.go is not linking the service packages")
	}

	// A fresh registry rather than DefaultRegistry: Init records the instance as
	// active, and leaving 431 initialized services behind would leak into every
	// other test in this package.
	fresh := plugin.NewRegistry()
	for _, id := range ids {
		fresh.Register(id, func() plugin.ServicePlugin {
			p, ok := plugin.DefaultRegistry.Construct(id)
			if !ok {
				t.Errorf("%s is registered but its factory would not construct", id)
				return nil
			}
			return p
		})
	}
	t.Cleanup(func() { _ = fresh.ShutdownAll(context.Background()) })

	root := t.TempDir()

	start := time.Now()
	for _, id := range ids {
		if _, err := fresh.Init(id, plugin.PluginConfig{DataDir: filepath.Join(root, id)}); err != nil {
			// Not a timing failure but a real one: main.go treats a failed Init
			// outside initOrder as a warning, so a service that stops coming up
			// would otherwise only be noticed as a fast run.
			t.Errorf("%s is registered but failed to initialize: %v", id, err)
		}
	}
	elapsed := time.Since(start)

	t.Logf("brought %d services up in %s", len(ids), elapsed.Round(time.Millisecond))
	if elapsed > fleetBudget {
		t.Errorf("bringing %d services up took %s, over the %s budget. Something in a "+
			"provider's Init is doing work it did not do before. Find it rather than "+
			"raising the budget; the startup figure is published in docs/coverage.md.",
			len(ids), elapsed.Round(time.Millisecond), fleetBudget)
	}
}
