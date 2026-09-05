// SPDX-License-Identifier: Apache-2.0

package admin

import (
	"sort"
	"sync"
	"time"
)

// maxUnroutedServiceIDLen bounds a recorded service ID. The ID is derived from
// caller-controlled headers, and Go's default MaxHeaderBytes is 1 MiB, so an
// untruncated key lets one client trade a header for a megabyte of retained
// memory. No real AWS service ID comes close to this.
const maxUnroutedServiceIDLen = 64

// UnroutedService is one service DevCloud was asked for and does not register.
type UnroutedService struct {
	ServiceID string    `json:"serviceId"`
	Count     int       `json:"count"`
	FirstSeen time.Time `json:"firstSeen"`
	LastSeen  time.Time `json:"lastSeen"`
}

// UnroutedReport is the full answer to "what did callers ask for that we do not
// serve?". It carries the collector's limits alongside the data because an
// evidence structure that silently stopped recording is worse than no evidence.
type UnroutedReport struct {
	Services []UnroutedService `json:"services"`
	// MaxServiceIDs is the cap on distinct service IDs held at once.
	MaxServiceIDs int `json:"maxServiceIds"`
	// DroppedServiceIDs counts requests for a service ID that was not already
	// tracked and arrived after the cap was reached. Non-zero means this report
	// is incomplete.
	DroppedServiceIDs int `json:"droppedServiceIds"`
}

// UnroutedCollector counts requests for services the gateway does not register.
//
// Unlike LogCollector, which answers "what happened recently" from a ring
// buffer, this answers "how often, over weeks" — a ring would discard exactly
// the accumulation that makes it evidence. A map is safe here because the key
// space is bounded by what a caller can name, and the cap below bounds it again
// for callers who are not naming real services.
//
// A nil *UnroutedCollector is usable: Add is a no-op and Snapshot returns an
// empty report. That is what the gateway holds when the admin API is off — with
// nobody able to read the counts, nothing is paid to keep them.
type UnroutedCollector struct {
	mu      sync.RWMutex
	entries map[string]*UnroutedService
	maxKeys int
	dropped int
}

// NewUnroutedCollector creates a collector holding at most maxKeys distinct
// service IDs. A non-positive maxKeys would make the collector record nothing
// while reporting no drops, so it is floored at 1.
func NewUnroutedCollector(maxKeys int) *UnroutedCollector {
	if maxKeys < 1 {
		maxKeys = 1
	}
	return &UnroutedCollector{
		entries: make(map[string]*UnroutedService),
		maxKeys: maxKeys,
	}
}

// Add records one request for an unregistered service.
func (c *UnroutedCollector) Add(serviceID string) {
	if c == nil || serviceID == "" {
		return
	}
	if len(serviceID) > maxUnroutedServiceIDLen {
		serviceID = serviceID[:maxUnroutedServiceIDLen]
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.entries[serviceID]; ok {
		e.Count++
		e.LastSeen = now
		return
	}
	if len(c.entries) >= c.maxKeys {
		c.dropped++
		return
	}
	c.entries[serviceID] = &UnroutedService{
		ServiceID: serviceID,
		Count:     1,
		FirstSeen: now,
		LastSeen:  now,
	}
}

// Snapshot returns the current counts, most-requested first, ties broken by
// service ID so the output is stable between calls with no new traffic.
func (c *UnroutedCollector) Snapshot() UnroutedReport {
	if c == nil {
		return UnroutedReport{Services: []UnroutedService{}}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]UnroutedService, 0, len(c.entries))
	for _, e := range c.entries {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].ServiceID < out[j].ServiceID
	})

	return UnroutedReport{
		Services:          out,
		MaxServiceIDs:     c.maxKeys,
		DroppedServiceIDs: c.dropped,
	}
}
