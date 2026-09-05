// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnroutedCollector_Accumulates is the property that makes this a different
// structure from LogCollector: repeated asks add up rather than scrolling away.
func TestUnroutedCollector_Accumulates(t *testing.T) {
	c := NewUnroutedCollector(100)

	for i := 0; i < 3; i++ {
		c.Add("amp")
	}
	c.Add("appflow")

	report := c.Snapshot()
	require.Len(t, report.Services, 2)

	// Most-requested first.
	assert.Equal(t, "amp", report.Services[0].ServiceID)
	assert.Equal(t, 3, report.Services[0].Count)
	assert.Equal(t, "appflow", report.Services[1].ServiceID)
	assert.Equal(t, 1, report.Services[1].Count)

	// FirstSeen is pinned at the first ask; LastSeen tracks the latest.
	assert.False(t, report.Services[0].FirstSeen.IsZero())
	assert.False(t, report.Services[0].LastSeen.Before(report.Services[0].FirstSeen))
}

// TestUnroutedCollector_SnapshotIsStable checks the tie-break. Without it the
// map iteration order reorders equal-count services on every call, and a
// reviewer diffing two snapshots sees churn that is not traffic.
func TestUnroutedCollector_SnapshotIsStable(t *testing.T) {
	c := NewUnroutedCollector(100)
	for _, id := range []string{"zeta", "alpha", "mu"} {
		c.Add(id)
	}

	first := c.Snapshot()
	for i := 0; i < 5; i++ {
		assert.Equal(t, first, c.Snapshot())
	}
	assert.Equal(t, []string{"alpha", "mu", "zeta"},
		[]string{first.Services[0].ServiceID, first.Services[1].ServiceID, first.Services[2].ServiceID})
}

// TestUnroutedCollector_CapsKeysAndReportsIt covers the trust boundary: service
// IDs come from caller-controlled headers, so an unbounded map is a memory
// budget handed to whoever is calling. Dropping is acceptable; dropping
// silently is not, because the report would then read as complete.
func TestUnroutedCollector_CapsKeysAndReportsIt(t *testing.T) {
	c := NewUnroutedCollector(2)

	c.Add("first")
	c.Add("second")
	c.Add("third") // over the cap
	c.Add("first") // already tracked — still counted

	report := c.Snapshot()
	require.Len(t, report.Services, 2)
	assert.Equal(t, 2, report.MaxServiceIDs)
	assert.Equal(t, 1, report.DroppedServiceIDs)
	assert.Equal(t, "first", report.Services[0].ServiceID)
	assert.Equal(t, 2, report.Services[0].Count)
}

// TestUnroutedCollector_TruncatesLongServiceID keeps one oversized header from
// retaining an oversized map key.
func TestUnroutedCollector_TruncatesLongServiceID(t *testing.T) {
	c := NewUnroutedCollector(10)
	c.Add(strings.Repeat("a", 500))

	report := c.Snapshot()
	require.Len(t, report.Services, 1)
	assert.Len(t, report.Services[0].ServiceID, maxUnroutedServiceIDLen)
}

// TestUnroutedCollector_NilIsUsable is the gateway's admin-disabled case. The
// router calls Add unconditionally, so a nil collector must not panic.
func TestUnroutedCollector_NilIsUsable(t *testing.T) {
	var c *UnroutedCollector

	assert.NotPanics(t, func() { c.Add("amp") })
	assert.Empty(t, c.Snapshot().Services)
}

func TestUnroutedCollector_IgnoresEmptyServiceID(t *testing.T) {
	c := NewUnroutedCollector(10)
	c.Add("")
	assert.Empty(t, c.Snapshot().Services)
}

// TestUnroutedCollector_Concurrent runs under -race; the collector is written
// from the request path and read from the admin API at the same time.
func TestUnroutedCollector_Concurrent(t *testing.T) {
	c := NewUnroutedCollector(100)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Add("amp")
		}()
		go func() {
			defer wg.Done()
			_ = c.Snapshot()
		}()
	}
	wg.Wait()

	report := c.Snapshot()
	require.Len(t, report.Services, 1)
	assert.Equal(t, 50, report.Services[0].Count)
}

// TestNewUnroutedCollector_FloorsMaxKeys guards the configuration mistake that
// would make the collector record nothing while reporting a clean report.
func TestNewUnroutedCollector_FloorsMaxKeys(t *testing.T) {
	c := NewUnroutedCollector(0)
	c.Add("amp")

	report := c.Snapshot()
	require.Len(t, report.Services, 1)
	assert.Equal(t, 1, report.MaxServiceIDs)
}
