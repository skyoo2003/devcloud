// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// ModelSource reads one API description format and produces the provider-neutral
// IR the generators consume. Smithy is the first implementation; OpenAPI and
// Protobuf are the reason the interface exists.
//
// A source owns its own detection so the codegen driver does not have to know
// which formats exist: it hands each file to SourceFor and generates from
// whatever comes back.
type ModelSource interface {
	// Name identifies the format, e.g. "smithy". Used in diagnostics.
	Name() string
	// Detect reports whether this source can parse data, which was read from
	// filename. It must not be expensive — it runs against every candidate file
	// for every registered source.
	Detect(filename string, data []byte) bool
	// Parse converts one model document into the IR. Callers should only call
	// Parse on data that Detect accepted.
	Parse(data []byte) (*ir.Model, error)
}

// SmithySource reads the Smithy 2.0 JSON AST that AWS publishes for its
// services. See ParseSmithyJSON for the conversion itself.
type SmithySource struct{}

func (SmithySource) Name() string { return "smithy" }

// Detect accepts a .json file whose top-level object carries a "smithy" version
// key. That key is what distinguishes a Smithy AST from any other JSON model
// document — an OpenAPI document in the same directory carries "openapi"
// instead, and is left for the source that claims it.
func (SmithySource) Detect(filename string, data []byte) bool {
	if !strings.EqualFold(filepath.Ext(filename), ".json") {
		return false
	}
	var probe struct {
		Smithy string `json:"smithy"`
	}
	return json.Unmarshal(data, &probe) == nil && probe.Smithy != ""
}

func (SmithySource) Parse(data []byte) (*ir.Model, error) { return ParseSmithyJSON(data) }

// DefaultSources is the set of formats the codegen driver understands, in
// detection order. Adding a format means appending an implementation here — no
// generator, template or driver change.
var DefaultSources = []ModelSource{SmithySource{}}

// SourceFor returns the first source in DefaultSources that claims the file.
// A file no source claims is an error at the call site rather than a silent
// skip: an unreadable model used to leave codegen output quietly incomplete.
func SourceFor(filename string, data []byte) (ModelSource, error) {
	for _, s := range DefaultSources {
		if s.Detect(filename, data) {
			return s, nil
		}
	}
	return nil, fmt.Errorf("no model source recognizes %s", filepath.Base(filename))
}
