// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateRouter(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateRouter("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "OperationRoutes")
	assert.Contains(t, output, `"CreateBucket"`)
	assert.Contains(t, output, `"PUT"`)
	assert.Contains(t, output, `"/{Bucket}"`)
	// The route table is generated per service; the matching is not. Asserting
	// the delegation is what stops a future edit from re-inlining the algorithm
	// into 148 packages, which is the drift internal/shared/httproute exists to
	// prevent.
	assert.Contains(t, output, "httproute.Match(OperationRoutes, method, uri)")
}

func TestGenerateErrors(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")
	output, err := gen.GenerateErrors("s3", model)
	require.NoError(t, err)
	assert.Contains(t, output, "package s3")
	assert.Contains(t, output, "type BucketAlreadyExists struct")
	assert.Contains(t, output, "func (e *BucketAlreadyExists) Error()")
	assert.Contains(t, output, "409")
}
