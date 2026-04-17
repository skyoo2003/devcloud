// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateID_PrefixAndLength(t *testing.T) {
	id := GenerateID("i-", 19)
	assert.Equal(t, 19, len(id))
	assert.True(t, id[:2] == "i-")
}

func TestGenerateID_HexOnly(t *testing.T) {
	id := GenerateID("vpc-", 21)
	assert.Equal(t, 21, len(id))
	suffix := id[4:]
	for _, c := range suffix {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'), "non-hex char: %c", c)
	}
}

func TestGenerateID_Unique(t *testing.T) {
	a := GenerateID("i-", 19)
	b := GenerateID("i-", 19)
	assert.NotEqual(t, a, b)
}

func TestGenerateUUID(t *testing.T) {
	u := GenerateUUID()
	assert.Equal(t, 36, len(u))
	assert.Equal(t, byte('-'), u[8])
	assert.Equal(t, byte('-'), u[13])
	assert.Equal(t, byte('-'), u[18])
	assert.Equal(t, byte('-'), u[23])
}

func TestGenerateUUID_Unique(t *testing.T) {
	a := GenerateUUID()
	b := GenerateUUID()
	assert.NotEqual(t, a, b)
}
