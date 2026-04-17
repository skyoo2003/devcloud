// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func GenerateID(prefix string, totalLen int) string {
	hexLen := totalLen - len(prefix)
	if hexLen <= 0 {
		return prefix
	}
	b := make([]byte, (hexLen+1)/2)
	rand.Read(b)
	return prefix + hex.EncodeToString(b)[:hexLen]
}

func GenerateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
