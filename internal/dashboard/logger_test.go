// SPDX-License-Identifier: Apache-2.0

package dashboard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeLog(method, path string, status int) RequestLog {
	return RequestLog{
		Method:    method,
		Path:      path,
		Status:    status,
		Duration:  "1.000ms",
		Timestamp: time.Now(),
		Service:   "s3",
	}
}

// TestLogCollector_AddAndRecent adds 3 entries and verifies Recent(10) returns
// all 3, newest first.
func TestLogCollector_AddAndRecent(t *testing.T) {
	c := NewLogCollector(10)

	log1 := makeLog("GET", "/a", 200)
	log2 := makeLog("POST", "/b", 201)
	log3 := makeLog("DELETE", "/c", 204)

	c.Add(log1)
	c.Add(log2)
	c.Add(log3)

	result := c.Recent(10)
	require.Len(t, result, 3)
	assert.Equal(t, log3.Path, result[0].Path)
	assert.Equal(t, log2.Path, result[1].Path)
	assert.Equal(t, log1.Path, result[2].Path)
}

// TestLogCollector_Overflow adds 5 entries with maxSize=3 and verifies Recent
// returns the latest 3, newest first.
func TestLogCollector_Overflow(t *testing.T) {
	c := NewLogCollector(3)

	for i := 0; i < 5; i++ {
		c.Add(makeLog("GET", "/"+string(rune('a'+i)), 200))
	}

	result := c.Recent(10)
	require.Len(t, result, 3)
	// Entries added were /a, /b, /c, /d, /e — newest first: /e, /d, /c
	assert.Equal(t, "/e", result[0].Path)
	assert.Equal(t, "/d", result[1].Path)
	assert.Equal(t, "/c", result[2].Path)
}

// TestLogCollector_RecentLimit adds 5 entries and verifies Recent(2) returns
// only the 2 most recent.
func TestLogCollector_RecentLimit(t *testing.T) {
	c := NewLogCollector(10)

	for i := 0; i < 5; i++ {
		c.Add(makeLog("GET", "/"+string(rune('a'+i)), 200))
	}

	result := c.Recent(2)
	require.Len(t, result, 2)
	assert.Equal(t, "/e", result[0].Path)
	assert.Equal(t, "/d", result[1].Path)
}
