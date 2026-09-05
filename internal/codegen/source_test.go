// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// smithyDoc is a minimal but complete Smithy 2.0 AST: one service binding one
// operation, with a protocol trait so the parser can classify it.
const smithyDoc = `{
  "smithy": "2.0",
  "shapes": {
    "com.example#Widgets": {
      "type": "service",
      "version": "2024-01-01",
      "operations": [{"target": "com.example#GetWidget"}],
      "traits": {"aws.protocols#awsJson1_0": {}}
    },
    "com.example#GetWidget": {"type": "operation"}
  }
}`

func TestSmithySourceDetect(t *testing.T) {
	s := SmithySource{}

	assert.True(t, s.Detect("widgets.json", []byte(smithyDoc)))
	// An OpenAPI document is also JSON. Detection keys off the "smithy" version
	// field precisely so the two do not fight over the same file extension.
	assert.False(t, s.Detect("widgets.json", []byte(`{"openapi":"3.0.0","paths":{}}`)))
	assert.False(t, s.Detect("widgets.yaml", []byte(smithyDoc)))
	assert.False(t, s.Detect("widgets.json", []byte("not json at all")))
}

func TestSourceForRoutesToSmithy(t *testing.T) {
	src, err := SourceFor("widgets.json", []byte(smithyDoc))
	require.NoError(t, err)
	assert.Equal(t, "smithy", src.Name())

	model, err := src.Parse([]byte(smithyDoc))
	require.NoError(t, err)
	assert.Equal(t, "aws", model.ProviderID())
	assert.Equal(t, "example", model.ServiceID)
	assert.Equal(t, "json-1.0", model.Protocol)
}

func TestSourceForRejectsUnknownFormat(t *testing.T) {
	_, err := SourceFor("widgets.yaml", []byte(smithyDoc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "widgets.yaml")
}
