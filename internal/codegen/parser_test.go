// SPDX-License-Identifier: Apache-2.0

// internal/codegen/parser_test.go
package codegen

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
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
	var createBucket *ir.Operation
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
	var bodyMember *ir.Member
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
	assert.Equal(t, ir.ShapeList, bucketList.Type)
}

// twoNamespaceModel is the shape healthlake.json actually has: the service's own
// namespace plus a second one bundled into the same file, with short names that
// collide. Shapes are keyed by short name, so one of the two wins — and which one
// used to depend on Go's map iteration order, making `make codegen` emit a
// different file on every run.
const twoNamespaceModel = `{
  "smithy": "2.0",
  "shapes": {
    "com.amazonaws.demo#Demo": {
      "type": "service",
      "version": "2020-01-01",
      "operations": [{"target": "com.amazonaws.demo#GetWidget"}],
      "traits": {"aws.protocols#awsJson1_1": {}}
    },
    "com.amazonaws.demo#GetWidget": {"type": "operation", "output": {"target": "com.amazonaws.demo#Widget"}},
    "com.amazonaws.demo#Widget": {
      "type": "structure",
      "members": {"Mine": {"target": "smithy.api#String"}}
    },
    "com.amazon.demolegacyfrontend#Widget": {
      "type": "structure",
      "members": {"Theirs": {"target": "smithy.api#String"}}
    }
  }
}`

// TestParseSmithyJSON_ServiceNamespaceWinsShapeCollision pins both halves: the
// winner is the service's own shape, and it is the winner every time.
//
// Determinism alone would not be enough — sorting the FQNs picks
// "com.amazon.demolegacyfrontend#Widget", the foreign one, because it sorts
// first. The model's service defines its own API; a bundled second namespace is
// a dependency, not the subject.
func TestParseSmithyJSON_ServiceNamespaceWinsShapeCollision(t *testing.T) {
	for i := 0; i < 50; i++ {
		model, err := ParseSmithyJSON([]byte(twoNamespaceModel))
		require.NoError(t, err)

		widget, ok := model.Shapes["Widget"]
		require.True(t, ok)
		require.Len(t, widget.Members, 1)
		assert.Equal(t, "Mine", widget.Members[0].Name,
			"the service's own namespace must win, on every run")
	}
}

// TestSmithyToGoType_Primitives covers Smithy's non-boxed primitives. They are
// ordinary prelude shapes, so nothing defines them locally, and a member
// targeting one used to emit a bare "PrimitiveLong" that does not compile —
// which is how onboarding the AI/ML category broke the build on bedrockagent
// and omics. Every prelude scalar the parser can meet belongs here, not only
// the two that happened to appear.
func TestSmithyToGoType_Primitives(t *testing.T) {
	cases := map[string]string{
		"smithy.api#PrimitiveLong":    "int64",
		"smithy.api#PrimitiveInteger": "int32",
		"smithy.api#PrimitiveShort":   "int16",
		"smithy.api#PrimitiveByte":    "int8",
		"smithy.api#PrimitiveBoolean": "bool",
		"smithy.api#PrimitiveFloat":   "float32",
		"smithy.api#PrimitiveDouble":  "float64",
	}
	for target, want := range cases {
		assert.Equal(t, want, smithyToGoType(target), target)
	}
}

// TestParseSmithyJSON_ServiceIdentifiers covers the names a caller can use to
// address a service on the wire. They are the input to the derived alias table,
// and rekognition is the model that proves why the shape name has to be carried
// separately: its X-Amz-Target prefix is RekognitionService while every other
// identifier it publishes is plain "rekognition".
func TestParseSmithyJSON_ServiceIdentifiers(t *testing.T) {
	data, err := os.ReadFile("../../smithy-models/rekognition.json")
	require.NoError(t, err)

	model, err := ParseSmithyJSON(data)
	require.NoError(t, err)

	assert.Equal(t, "rekognition", model.ServiceID)
	assert.Equal(t, "RekognitionService", model.ShapeName, "the X-Amz-Target prefix")
	assert.Equal(t, "rekognition", model.SigningName)
	assert.Equal(t, "rekognition", model.EndpointPrefix)
	assert.Equal(t, "rekognition", model.ARNNamespace)
	assert.Equal(t, "Rekognition", model.CloudFormationName)
	assert.Equal(t, "Rekognition", model.SDKID)
}

// TestParseSmithyJSON_ServiceIdentifiersAbsent — a model without the AWS service
// traits must parse, leaving the identifiers empty rather than failing. The IR is
// provider-neutral; a non-AWS source will not set them at all.
func TestParseSmithyJSON_ServiceIdentifiersAbsent(t *testing.T) {
	model, err := ParseSmithyJSON([]byte(resourceModel))
	require.NoError(t, err)

	assert.Equal(t, "Demo", model.ShapeName)
	assert.Empty(t, model.SigningName)
	assert.Empty(t, model.EndpointPrefix)
	assert.Empty(t, model.SDKID)
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
	var getWidget *ir.Operation
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
