// SPDX-License-Identifier: Apache-2.0

package shared

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
