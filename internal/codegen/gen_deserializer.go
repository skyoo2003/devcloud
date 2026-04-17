// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_deserializer.go
package codegen

import "strings"

// deserializerOperationData holds per-operation template data for the
// deserializer (serialize-to-HTTP-response) generator.
type deserializerOperationData struct {
	OutputName    string
	HeaderMembers []Member
	BodyMembers   []Member
}

// deserializerTemplateData is the top-level data passed to deserializer.go.tmpl.
type deserializerTemplateData struct {
	PkgName    string
	Operations []deserializerOperationData
}

// GenerateDeserializer produces a Go source file containing one
// Serialize<OutputType> function per operation. Each function writes the
// output struct to an http.ResponseWriter, setting HTTP headers for members
// that carry the httpHeader trait and marshalling the remaining body members
// to XML or JSON based on the protocol argument.
func (g *Generator) GenerateDeserializer(pkgName string, model *SmithyModel) (string, error) {
	data := deserializerTemplateData{PkgName: pkgName}
	seen := make(map[string]bool)

	for _, op := range model.Operations {
		if seen[op.OutputName] {
			continue
		}
		seen[op.OutputName] = true
		shape, ok := model.Shapes[op.OutputName]
		if !ok {
			continue
		}

		opData := deserializerOperationData{OutputName: op.OutputName}
		for _, m := range shape.Members {
			m.Name = exportName(m.Name)
			if m.HTTPHeader != "" {
				opData.HeaderMembers = append(opData.HeaderMembers, m)
			} else if !m.HTTPLabel && m.HTTPQuery == "" {
				opData.BodyMembers = append(opData.BodyMembers, m)
			}
		}
		data.Operations = append(data.Operations, opData)
	}

	rendered, err := g.renderTemplate("deserializer.go.tmpl", data)
	if err != nil {
		return "", err
	}

	// Remove unused imports.
	if !strings.Contains(rendered, "strconv.") {
		rendered = strings.Replace(rendered, "\t\"strconv\"\n", "", 1)
	}
	if !strings.Contains(rendered, "time.") {
		rendered = strings.Replace(rendered, "\t\"time\"\n", "", 1)
	}
	if !strings.Contains(rendered, "json.") {
		rendered = strings.Replace(rendered, "\t\"encoding/json\"\n", "", 1)
	}
	if !strings.Contains(rendered, "xml.") {
		rendered = strings.Replace(rendered, "\t\"encoding/xml\"\n", "", 1)
	}
	if !strings.Contains(rendered, "http.") {
		rendered = strings.Replace(rendered, "\t\"net/http\"\n", "", 1)
	}

	// If the import block is now empty, remove it entirely.
	rendered = strings.Replace(rendered, "\nimport (\n)\n", "\n", 1)

	return rendered, nil
}
