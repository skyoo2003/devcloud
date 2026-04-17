// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateInterface(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateInterface("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "type S3Service interface")
	assert.Contains(t, output, "CreateBucket(ctx context.Context, input *CreateBucketRequest) (*CreateBucketOutput, error)")
	assert.Contains(t, output, "ListBuckets(ctx context.Context, input *ListBucketsInput) (*ListBucketsOutput, error)")
	assert.Contains(t, output, "PutObject(ctx context.Context, input *PutObjectRequest) (*PutObjectOutput, error)")
}

func TestGenerateBaseProvider(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateBaseProvider("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "type BaseProvider struct")
	assert.Contains(t, output, "func (b *BaseProvider) CreateBucket")
	assert.Contains(t, output, "ErrNotImplemented")
}
