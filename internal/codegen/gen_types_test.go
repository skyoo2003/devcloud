// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadTestModel(t *testing.T) *SmithyModel {
	t.Helper()
	data, err := os.ReadFile("../../cmd/codegen/testdata/s3-minimal.json")
	require.NoError(t, err)
	model, err := ParseSmithyJSON(data)
	require.NoError(t, err)
	return model
}

func TestGenerateTypes(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateTypes("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "type CreateBucketRequest struct")
	assert.Contains(t, output, "type CreateBucketOutput struct")
	assert.Contains(t, output, "type Bucket struct")
	assert.Contains(t, output, "Bucket string")
	assert.Contains(t, output, "Name string")
	assert.Contains(t, output, "BucketList")
	assert.NotContains(t, output, "type AmazonS3 struct")
	assert.NotContains(t, output, "type CreateBucket struct")
}
