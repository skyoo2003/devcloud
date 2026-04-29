// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"path/filepath"
	"strings"
)

// ValidateUploadID checks that id is a 32-character lowercase hex string,
// matching the format produced by InitiateLayerUpload in S3 and ECR.
func ValidateUploadID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !isHexLower(c) {
			return false
		}
	}
	return true
}

func isHexLower(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// IsWithinDir returns true if child resolves to a path within parent.
// It uses filepath.Abs and filepath.Rel for robust cross-platform containment.
func IsWithinDir(child, parent string) bool {
	absChild, err := filepath.Abs(filepath.Clean(child))
	if err != nil {
		return false
	}
	absParent, err := filepath.Abs(filepath.Clean(parent))
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absParent, absChild)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
