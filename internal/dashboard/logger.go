// SPDX-License-Identifier: Apache-2.0

package dashboard

import (
	"sync"
	"time"
)

const (
	defaultLogCollectorSize = 1000
	maxLogCollectorSize     = 10000
)

// RequestLog holds details about a single API request.
type RequestLog struct {
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	Status    int       `json:"status"`
	Duration  string    `json:"duration"` // e.g. "1.234ms"
	Timestamp time.Time `json:"timestamp"`
	Service   string    `json:"service"` // detected service ID
}

// LogCollector is a thread-safe ring buffer for recent request logs.
type LogCollector struct {
	mu      sync.RWMutex
	entries []RequestLog
	maxSize int
	cursor  int // points to the slot where the next entry will be written
	count   int // total number of valid entries (capped at maxSize)
}

// NewLogCollector creates a LogCollector that holds at most maxSize entries.
func NewLogCollector(maxSize int) *LogCollector {
	if maxSize <= 0 {
		maxSize = defaultLogCollectorSize
	}
	if maxSize > maxLogCollectorSize {
		maxSize = maxLogCollectorSize
	}

	return &LogCollector{
		entries: make([]RequestLog, maxSize),
		maxSize: maxSize,
	}
}

// Add inserts a new log entry. When the buffer is full the oldest entry is
// overwritten (ring-buffer semantics).
func (c *LogCollector) Add(log RequestLog) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[c.cursor] = log
	c.cursor = (c.cursor + 1) % c.maxSize
	if c.count < c.maxSize {
		c.count++
	}
}

// Recent returns up to n most-recent log entries, newest first.
func (c *LogCollector) Recent(n int) []RequestLog {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if n <= 0 || c.count == 0 {
		return []RequestLog{}
	}
	if n > maxLogCollectorSize {
		n = maxLogCollectorSize
	}
	if n > c.count {
		n = c.count
	}

	result := make([]RequestLog, n)
	// cursor points to the slot that will be written next, so cursor-1 is the
	// most-recently written entry (with wrap-around).
	for i := 0; i < n; i++ {
		idx := (c.cursor - 1 - i + c.maxSize) % c.maxSize
		result[i] = c.entries[idx]
	}
	return result
}
