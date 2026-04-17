// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateDeserializer(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateDeserializer("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "func SerializeCreateBucketOutput")
	assert.Contains(t, output, "func SerializePutObjectOutput")
	assert.Contains(t, output, "Location")
	assert.Contains(t, output, "ETag")
}
