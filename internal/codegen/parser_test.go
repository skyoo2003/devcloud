// SPDX-License-Identifier: Apache-2.0

// internal/codegen/parser_test.go
package codegen

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSmithyJSON(t *testing.T) {
	data, err := os.ReadFile("../../cmd/codegen/testdata/s3-minimal.json")
	require.NoError(t, err)

	model, err := ParseSmithyJSON(data)
	require.NoError(t, err)

	assert.Equal(t, "AmazonS3", model.ServiceName)
	assert.Equal(t, "s3", model.ServiceID)
	assert.Equal(t, "rest-xml", model.Protocol)
	assert.Len(t, model.Operations, 3)

	// Check CreateBucket operation
	var createBucket *Operation
	for i := range model.Operations {
		if model.Operations[i].Name == "CreateBucket" {
			createBucket = &model.Operations[i]
			break
		}
	}
	require.NotNil(t, createBucket)
	assert.Equal(t, "CreateBucketRequest", createBucket.InputName)
	assert.Equal(t, "CreateBucketOutput", createBucket.OutputName)
	assert.Equal(t, "PUT", createBucket.HTTPMethod)
	assert.Equal(t, "/{Bucket}", createBucket.HTTPUri)
	assert.Contains(t, createBucket.Errors, "BucketAlreadyExists")

	// Check CreateBucketRequest shape
	reqShape, ok := model.Shapes["CreateBucketRequest"]
	require.True(t, ok)
	assert.Len(t, reqShape.Members, 1)
	assert.Equal(t, "Bucket", reqShape.Members[0].Name)
	assert.True(t, reqShape.Members[0].Required)
	assert.True(t, reqShape.Members[0].HTTPLabel)

	// Check PutObjectRequest has payload
	putReq, ok := model.Shapes["PutObjectRequest"]
	require.True(t, ok)
	var bodyMember *Member
	for i := range putReq.Members {
		if putReq.Members[i].Name == "Body" {
			bodyMember = &putReq.Members[i]
			break
		}
	}
	require.NotNil(t, bodyMember)
	assert.True(t, bodyMember.HTTPPayload)
	assert.Equal(t, "[]byte", bodyMember.GoType)

	// Check BucketAlreadyExists error
	errShape, ok := model.Shapes["BucketAlreadyExists"]
	require.True(t, ok)
	require.NotNil(t, errShape.ErrorTrait)
	assert.Equal(t, "client", errShape.ErrorTrait.Type)
	assert.Equal(t, 409, errShape.ErrorTrait.HTTPStatus)

	// Check list shape
	bucketList, ok := model.Shapes["BucketList"]
	require.True(t, ok)
	assert.Equal(t, ShapeList, bucketList.Type)
}
