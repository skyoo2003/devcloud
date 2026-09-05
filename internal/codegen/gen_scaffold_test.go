// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_scaffold_test.go
package codegen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGenerateScaffoldOptsIntoCRUDEngine pins the one line that decides whether
// a newly scaffolded service serves anything at all.
//
// The gateway routes to the generic CRUD engine only when a provider returns
// plugin.ErrUnhandledOp (internal/gateway/router.go). Any other error is a plain
// refusal, so a scaffold that declines differently is registered, routed, and
// serves zero operations — while the fidelity manifest still advertises its
// CRUD-shaped operations as auto-crud.
func TestGenerateScaffoldOptsIntoCRUDEngine(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")

	output, err := gen.GenerateScaffold("s3", model)
	require.NoError(t, err)

	// Assert on the return statement, not on the string anywhere in the file:
	// the scaffold's doc comment names the wrong-way error on purpose, to say
	// why declining that way serves nothing.
	assert.Contains(t, output, "return nil, plugin.ErrUnhandledOp",
		"a scaffolded provider must opt into the CRUD engine, or it serves nothing")
	assert.NotContains(t, output, "return nil, generated.ErrNotImplemented",
		"ErrNotImplemented is a refusal the gateway never routes to the CRUD engine")
}

// TestGenerateScaffoldKeepsDataDir checks the scaffold carries the data
// directory it is handed, matching what hand-written providers do in Init. A
// scaffold that drops it forces the first contributor who needs persistence to
// rewrite Init before writing any behaviour.
func TestGenerateScaffoldKeepsDataDir(t *testing.T) {
	model := loadTestModel(t)
	gen := NewGenerator("templates")

	output, err := gen.GenerateScaffold("s3", model)
	require.NoError(t, err)

	assert.Contains(t, output, "cfg.DataDir",
		"Init must keep the configured data directory, as hand-written providers do")
}
