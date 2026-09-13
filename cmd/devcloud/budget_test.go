// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

// startupBudgetEnv opts the timing half of TestRegisteredFleetComesUpWithinItsBudget
// into being an assertion. Unset — which is every CI run — the elapsed time is
// logged and nothing is asserted about it. See the test's comment for why.
//
//	DEVCLOUD_STARTUP_BUDGET=300ms go test ./cmd/devcloud/
const startupBudgetEnv = "DEVCLOUD_STARTUP_BUDGET"

// TestRegisteredFleetComesUpWithinItsBudget asserts that every registered
// service actually comes up, and measures what that costs.
//
// The correctness half is the part that holds unconditionally. main.go brings
// the long tail up with fatal=false, so a service that is registered but can no
// longer initialize degrades to a warning in a log nobody reads. Here it fails.
//
// The timing half is a measurement, not a gate, and the reason is a number. The
// fleet comes up in ~141 ms locally and took 2.048 s on a GitHub arm64 runner:
// the shared runner is 14x slower, and that is before accounting for variance
// between runs on it. The regression actually worth catching — a provider that
// starts opening a file or a database per service at startup — is 1.2x to 2x,
// because that is what 431 extra file opens cost. There is no absolute ceiling
// that clears a 14x machine difference and still fails on a 2x regression, so an
// absolute ceiling in CI would only ever have been decoration that flakes. It is
// better to say startup is measured than to claim a gate that cannot fire.
//
// Set DEVCLOUD_STARTUP_BUDGET to assert on a machine whose speed you know —
// that is how the published figure in docs/coverage.md is re-taken per release.
//
// An earlier version of this test timed Construct instead of Init, which only
// calls the factory. Every factory in the tree is a struct literal, so it
// measured 84 µs and could not have failed for any reason. What costs is Init:
// S3Provider.Init does an os.MkdirAll and opens a SQLite database.
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
			t.Errorf("%s is registered but failed to initialize: %v", id, err)
		}
	}
	elapsed := time.Since(start)

	t.Logf("brought %d services up in %s", len(ids), elapsed.Round(time.Millisecond))

	raw, ok := os.LookupEnv(startupBudgetEnv)
	if !ok {
		return
	}
	budget, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("%s=%q is not a duration: %v", startupBudgetEnv, raw, err)
	}
	if elapsed > budget {
		t.Errorf("bringing %d services up took %s, over the %s asked for. Something in a "+
			"provider's Init is doing work it did not do before — that is what scales with "+
			"service count. If this is a slower machine than the one the budget was set on, "+
			"it is the budget that is wrong.", len(ids), elapsed.Round(time.Millisecond), budget)
	}
}
