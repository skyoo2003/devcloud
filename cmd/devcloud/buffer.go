// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
)

// bufferHandler is a slog.Handler that captures every record instead of
// emitting it, so log calls made before the operator-configured handler is
// installed (notably config.parse() warnings) can be replayed afterwards and
// honor logging.format / logging.level. See flushTo.
//
// ponytail: single-threaded startup, add a mutex if buffered logging ever
// goes concurrent.
type bufferHandler struct {
	records []slog.Record
}

// Enabled buffers all levels; the real handler filters on replay in flushTo.
func (h *bufferHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *bufferHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Clone())
	return nil
}

// WithAttrs/WithGroup return the handler unchanged: config-time logging uses
// plain slog.Warn/Info without attr or group chaining, so nothing is lost.
func (h *bufferHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *bufferHandler) WithGroup(string) slog.Handler      { return h }

// flushTo replays buffered records through dst, honoring dst's level filter,
// then clears the buffer.
func (h *bufferHandler) flushTo(dst slog.Handler) {
	for _, r := range h.records {
		if dst.Enabled(context.Background(), r.Level) {
			_ = dst.Handle(context.Background(), r)
		}
	}
	h.records = nil
}
