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

// resourceModel is a minimal model whose operations hang off resource shapes
// rather than the service's own operations list — the shape real AWS models like
// lambda, ecs and bedrock use.
const resourceModel = `{
  "smithy": "2.0",
  "shapes": {
    "com.amazonaws.demo#Demo": {
      "type": "service",
      "version": "2020-01-01",
      "operations": [{"target": "com.amazonaws.demo#DirectOp"}],
      "resources": [{"target": "com.amazonaws.demo#Widget"}],
      "traits": {"aws.protocols#awsJson1_1": {}}
    },
    "com.amazonaws.demo#Widget": {
      "type": "resource",
      "read": {"target": "com.amazonaws.demo#GetWidget"},
      "delete": {"target": "com.amazonaws.demo#DeleteWidget"},
      "list": {"target": "com.amazonaws.demo#ListWidgets"},
      "operations": [{"target": "com.amazonaws.demo#TagWidget"}],
      "collectionOperations": [{"target": "com.amazonaws.demo#PurgeWidgets"}],
      "resources": [{"target": "com.amazonaws.demo#Gadget"}]
    },
    "com.amazonaws.demo#Gadget": {
      "type": "resource",
      "read": {"target": "com.amazonaws.demo#GetGadget"}
    },
    "com.amazonaws.demo#DirectOp": {"type": "operation"},
    "com.amazonaws.demo#GetWidget": {"type": "operation", "output": {"target": "com.amazonaws.demo#GetWidgetOutput"}},
    "com.amazonaws.demo#DeleteWidget": {"type": "operation"},
    "com.amazonaws.demo#ListWidgets": {"type": "operation"},
    "com.amazonaws.demo#TagWidget": {"type": "operation"},
    "com.amazonaws.demo#PurgeWidgets": {"type": "operation"},
    "com.amazonaws.demo#GetGadget": {"type": "operation"},
    "com.amazonaws.demo#GetWidgetOutput": {"type": "structure", "members": {}}
  }
}`

func TestParseSmithyJSON_ResourceAttachedOperations(t *testing.T) {
	model, err := ParseSmithyJSON([]byte(resourceModel))
	require.NoError(t, err)

	names := make([]string, 0, len(model.Operations))
	for _, op := range model.Operations {
		names = append(names, op.Name)
	}

	// Lifecycle bindings, operations, collectionOperations and nested resources
	// all contribute; the service's own list still counts.
	assert.Equal(t, []string{
		"DeleteWidget", "DirectOp", "GetGadget", "GetWidget",
		"ListWidgets", "PurgeWidgets", "TagWidget",
	}, names, "operations must be collected from resources and sorted")

	// Output wiring survives the resource walk.
	var getWidget *Operation
	for i := range model.Operations {
		if model.Operations[i].Name == "GetWidget" {
			getWidget = &model.Operations[i]
		}
	}
	require.NotNil(t, getWidget)
	assert.Equal(t, "GetWidgetOutput", getWidget.OutputName)
}

func TestParseSmithyJSON_ResourceCycleTerminates(t *testing.T) {
	const cyclic = `{
      "smithy": "2.0",
      "shapes": {
        "com.amazonaws.demo#Demo": {
          "type": "service",
          "version": "2020-01-01",
          "resources": [{"target": "com.amazonaws.demo#A"}],
          "traits": {"aws.protocols#awsJson1_1": {}}
        },
        "com.amazonaws.demo#A": {
          "type": "resource",
          "read": {"target": "com.amazonaws.demo#GetA"},
          "resources": [{"target": "com.amazonaws.demo#B"}]
        },
        "com.amazonaws.demo#B": {
          "type": "resource",
          "resources": [{"target": "com.amazonaws.demo#A"}]
        },
        "com.amazonaws.demo#GetA": {"type": "operation"}
      }
    }`

	model, err := ParseSmithyJSON([]byte(cyclic))
	require.NoError(t, err)
	require.Len(t, model.Operations, 1)
	assert.Equal(t, "GetA", model.Operations[0].Name)
}
