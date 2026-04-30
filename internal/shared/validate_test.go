// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"path/filepath"
	"testing"
)

func TestIsWithinDir(t *testing.T) {
	tmp := t.TempDir()

	tests := []struct {
		name  string
		child string
		want  bool
	}{
		{"direct child", filepath.Join(tmp, "a"), true},
		{"nested child", filepath.Join(tmp, "a", "b", "c"), true},
		{"cleaned dot", tmp + string(filepath.Separator) + "." + string(filepath.Separator) + "a", true},
		{"parent itself", tmp, true},
		{"sibling", filepath.Join(filepath.Dir(tmp), "other"), false},
		{"traversal", filepath.Join(tmp, "..", "other"), false},
		{"double traversal", filepath.Join(tmp, "..", "..", "etc"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsWithinDir(tt.child, tmp); got != tt.want {
				t.Errorf("IsWithinDir(%q, %q) = %v, want %v", tt.child, tmp, got, tt.want)
			}
		})
	}
}
