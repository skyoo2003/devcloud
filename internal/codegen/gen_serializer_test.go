// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSerializer(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateSerializer("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "func DeserializeCreateBucketRequest")
	assert.Contains(t, output, "func DeserializePutObjectRequest")
	assert.Contains(t, output, "pathParams")
	assert.Contains(t, output, "r.Header.Get")
	assert.Contains(t, output, "io.ReadAll")
}
