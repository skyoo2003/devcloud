// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"fmt"
	"go/format"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// CRUDRegistryData drives crud_registry.go.tmpl: one file registering every
// service's CRUD-shaped operations with the fallback engine.
type CRUDRegistryData struct {
	Services []CRUDServiceData
}

// CRUDServiceData is one service's classified operations.
type CRUDServiceData struct {
	ServiceID string
	Ops       []crudOpData
}

type crudOpData struct {
	Op       string
	Verb     string
	Resource string
	ListKey  string
	ItemKey  string
}

// verbPrefixes maps operation-name prefixes to canonical CRUD verbs, longest and
// most specific first. Only high-confidence CRUD shapes are classified; anything
// else is left unclassified so the engine returns an honest "unknown action".
var verbPrefixes = []struct{ prefix, verb string }{
	{"Untag", "Untag"},
	{"Tag", "Tag"},
	{"Describe", "Get"},
	{"BatchGet", "List"},
	{"Get", "Get"},
	{"List", "List"},
	{"Create", "Create"},
	{"Register", "Create"},
	{"Deregister", "Delete"},
	{"Delete", "Delete"},
	{"Update", "Update"},
	{"Modify", "Update"},
	{"Put", "Update"},
}

func canonicalVerb(op string) (verb, resource string) {
	for _, vp := range verbPrefixes {
		if strings.HasPrefix(op, vp.prefix) && len(op) > len(vp.prefix) {
			return vp.verb, singularize(strings.TrimPrefix(op, vp.prefix))
		}
	}
	return "", ""
}

// singularize converts a resource noun to a stable singular key so that, e.g.,
// CreateDatabase and ListDatabases share the same store bucket.
func singularize(s string) string {
	switch {
	case strings.HasSuffix(s, "ies"):
		return s[:len(s)-3] + "y"
	case strings.HasSuffix(s, "sses"), strings.HasSuffix(s, "xes"), strings.HasSuffix(s, "zes"):
		return s[:len(s)-2]
	case strings.HasSuffix(s, "ss"):
		return s
	case strings.HasSuffix(s, "s"):
		return s[:len(s)-1]
	default:
		return s
	}
}

// outputKeys finds the output member holding a collection (list key) and the one
// wrapping a single resource structure (item key).
func outputKeys(model *ir.Model, op ir.Operation) (listKey, itemKey string) {
	out := model.Shapes[op.OutputName]
	if out == nil {
		return "", ""
	}
	for _, mem := range out.Members {
		target := model.Shapes[mem.TargetName]
		if target == nil {
			continue
		}
		switch target.Type {
		case ir.ShapeList:
			if listKey == "" {
				listKey = mem.Name
			}
		case ir.ShapeStructure:
			if itemKey == "" {
				itemKey = mem.Name
			}
		}
	}
	return listKey, itemKey
}

// classifyOps returns the CRUD metadata for every classifiable operation, in the
// model's (already sorted) order for reproducible output.
func classifyOps(model *ir.Model) []crudOpData {
	var ops []crudOpData
	for _, op := range model.Operations {
		verb, resource := canonicalVerb(op.Name)
		if verb == "" || resource == "" {
			continue
		}
		listKey, itemKey := outputKeys(model, op)
		ops = append(ops, crudOpData{
			Op:       op.Name,
			Verb:     verb,
			Resource: resource,
			ListKey:  listKey,
			ItemKey:  itemKey,
		})
	}
	return ops
}

// isJSONProtocol reports whether the engine can serve a service's protocol.
// Only X-Amz-Target JSON protocols carry an operation name at the router.
func isJSONProtocol(protocol string) bool {
	return strings.HasPrefix(protocol, "json")
}

// ServiceCRUDData classifies a JSON-protocol model's CRUD operations. It returns
// (data, false) when the service is not engine-servable or has no CRUD ops.
func ServiceCRUDData(model *ir.Model) (CRUDServiceData, bool) {
	if !isJSONProtocol(model.Protocol) {
		return CRUDServiceData{}, false
	}
	ops := classifyOps(model)
	if len(ops) == 0 {
		return CRUDServiceData{}, false
	}
	return CRUDServiceData{ServiceID: model.ServiceID, Ops: ops}, true
}

// GenerateCRUDRegistry renders the single aggregate registry file. Services are
// registered from one init() so registration does not depend on any individual
// generated package being imported.
func (g *Generator) GenerateCRUDRegistry(services []CRUDServiceData) (string, error) {
	rendered, err := g.renderTemplate("crud_registry.go.tmpl", CRUDRegistryData{Services: services})
	if err != nil {
		return "", err
	}
	formatted, err := format.Source([]byte(rendered))
	if err != nil {
		return "", fmt.Errorf("gofmt crud_registry.go: %w", err)
	}
	return string(formatted), nil
}
