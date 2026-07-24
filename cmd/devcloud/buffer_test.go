// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// TestBufferHandler_FlushHonorsFormatAndLevel verifies that records buffered
// before the real handler is installed are replayed as JSON and filtered by
// the destination handler's level — the two acceptance criteria from #113.
func TestBufferHandler_FlushHonorsFormatAndLevel(t *testing.T) {
	buf := &bufferHandler{}
	log := slog.New(buf)
	log.Info("config file not found")
	log.Warn("dashboard key deprecated", "token", "tierX")

	var out bytes.Buffer
	dst := slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn})
	buf.flushTo(dst)

	got := out.String()
	if strings.Contains(got, "config file not found") {
		t.Errorf("info record should be dropped at level=warn, got: %s", got)
	}
	if !strings.Contains(got, `"dashboard key deprecated"`) || !strings.Contains(got, `"token":"tierX"`) {
		t.Errorf("warn record (with attrs) should be replayed as JSON, got: %s", got)
	}

	// flush clears the buffer so a second flush is a no-op.
	out.Reset()
	buf.flushTo(dst)
	if out.Len() != 0 {
		t.Errorf("second flush should emit nothing, got: %s", out.String())
	}
}

// TestBufferHandler_EnabledBuffersAllLevels confirms buffering does not drop
// records below the eventual threshold; the level filter applies only on flush.
func TestBufferHandler_EnabledBuffersAllLevels(t *testing.T) {
	buf := &bufferHandler{}
	if !buf.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("bufferHandler must buffer all levels, including debug")
	}
}
