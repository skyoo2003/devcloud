// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOpenAPIJSON(t *testing.T) {
	data, err := os.ReadFile("../../api/models/azure-blob-storage.json")
	require.NoError(t, err)

	model, err := ParseOpenAPIJSON(data)
	require.NoError(t, err)

	assert.Equal(t, "azure", model.Provider)
	assert.Equal(t, "blob", model.ServiceID)
	assert.Equal(t, "Azure Blob Storage", model.ServiceName)
	assert.Equal(t, "rest-xml", model.Protocol)

	// Ensure all 9 operations are parsed
	opMap := make(map[string]string)
	for _, op := range model.Operations {
		opMap[op.Name] = op.HTTPMethod + " " + op.HTTPUri
	}

	expectedOps := map[string]string{
		"ListContainers":         "GET /?comp=list",
		"CreateContainer":        "PUT /{container}?restype=container",
		"DeleteContainer":        "DELETE /{container}?restype=container",
		"GetContainerProperties": "GET /{container}?restype=container",
		"ListBlobs":              "GET /{container}?restype=container&comp=list",
		"PutBlock":               "PUT /{container}/{blob+}?comp=block",
		"PutBlockList":           "PUT /{container}/{blob+}?comp=blocklist",
		"SetBlobMetadata":        "PUT /{container}/{blob+}?comp=metadata",
		"PutBlob":                "PUT /{container}/{blob+}",
		"GetBlob":                "GET /{container}/{blob+}",
		"GetBlobProperties":      "HEAD /{container}/{blob+}",
		"DeleteBlob":             "DELETE /{container}/{blob+}",
	}

	for opName, expectedRoute := range expectedOps {
		route, ok := opMap[opName]
		assert.True(t, ok, "operation %s must exist", opName)
		assert.Equal(t, expectedRoute, route, "operation %s route mismatch", opName)
	}

	// Verify route ordering: ListBlobs (2 query params) must precede GetContainerProperties (1 query param)
	listBlobsIdx := -1
	getContainerPropsIdx := -1
	for i, op := range model.Operations {
		if op.Name == "ListBlobs" {
			listBlobsIdx = i
		}
		if op.Name == "GetContainerProperties" {
			getContainerPropsIdx = i
		}
	}
	assert.True(t, listBlobsIdx != -1 && getContainerPropsIdx != -1)
	assert.True(t, listBlobsIdx < getContainerPropsIdx, "ListBlobs must precede GetContainerProperties for route matching")

	// Verify error shapes
	errShape, ok := model.Shapes["ContainerNotFound"]
	require.True(t, ok)
	require.NotNil(t, errShape.ErrorTrait)
	assert.Equal(t, 404, errShape.ErrorTrait.HTTPStatus)

	existsShape, ok := model.Shapes["BlobAlreadyExists"]
	require.True(t, ok)
	require.NotNil(t, existsShape.ErrorTrait)
	assert.Equal(t, 409, existsShape.ErrorTrait.HTTPStatus)

	condShape, ok := model.Shapes["ConditionNotMet"]
	require.True(t, ok)
	require.NotNil(t, condShape.ErrorTrait)
	assert.Equal(t, 412, condShape.ErrorTrait.HTTPStatus)

	// Verify determinism: parsing multiple times produces identical member ordering
	model2, err := ParseOpenAPIJSON(data)
	require.NoError(t, err)
	for shapeName, s1 := range model.Shapes {
		s2 := model2.Shapes[shapeName]
		require.NotNil(t, s2)
		require.Equal(t, len(s1.Members), len(s2.Members))
		for idx := range s1.Members {
			assert.Equal(t, s1.Members[idx].Name, s2.Members[idx].Name)
		}
	}
}
