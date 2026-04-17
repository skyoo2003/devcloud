// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_serializer.go
package codegen

import "strings"

// serializerMemberData holds per-member HTTP binding info for the template.
type serializerMemberData struct {
	Name          string
	GoType        string
	IsHTTPLabel   bool
	IsHTTPPayload bool
	IsHTTPHeader  bool
	IsHTTPQuery   bool
	HeaderName    string
	QueryName     string
}

// serializerOperationData holds per-operation data for the serializer template.
type serializerOperationData struct {
	FuncName  string
	InputType string
	Members   []serializerMemberData
}

// serializerTemplateData is the top-level data passed to serializer.go.tmpl.
type serializerTemplateData struct {
	PkgName    string
	Operations []serializerOperationData
}

// GenerateSerializer produces a Go source file containing Deserialize*
// functions that parse HTTP requests into the input types of all operations
// in model.
func (g *Generator) GenerateSerializer(pkgName string, model *SmithyModel) (string, error) {
	data := serializerTemplateData{PkgName: pkgName}
	seen := make(map[string]bool)

	for _, op := range model.Operations {
		if op.InputName == "" {
			continue
		}
		if seen[op.InputName] {
			continue
		}
		seen[op.InputName] = true
		inputShape, ok := model.Shapes[op.InputName]
		if !ok {
			continue
		}

		opData := serializerOperationData{
			FuncName:  op.InputName,
			InputType: op.InputName,
		}

		for _, m := range inputShape.Members {
			md := serializerMemberData{
				Name:          exportName(m.Name),
				GoType:        m.GoType,
				IsHTTPLabel:   m.HTTPLabel,
				IsHTTPPayload: m.HTTPPayload,
				IsHTTPHeader:  m.HTTPHeader != "",
				IsHTTPQuery:   m.HTTPQuery != "",
				HeaderName:    m.HTTPHeader,
				QueryName:     m.HTTPQuery,
			}
			opData.Members = append(opData.Members, md)
		}

		data.Operations = append(data.Operations, opData)
	}

	rendered, err := g.renderTemplate("serializer.go.tmpl", data)
	if err != nil {
		return "", err
	}

	// Remove unused imports.
	if !strings.Contains(rendered, "fmt.") {
		rendered = strings.Replace(rendered, "\t\"fmt\"\n", "", 1)
	}
	if !strings.Contains(rendered, "io.") {
		rendered = strings.Replace(rendered, "\t\"io\"\n", "", 1)
	}
	if !strings.Contains(rendered, "json.") {
		rendered = strings.Replace(rendered, "\t\"encoding/json\"\n", "", 1)
	}
	if !strings.Contains(rendered, "xml.") {
		rendered = strings.Replace(rendered, "\t\"encoding/xml\"\n", "", 1)
	}
	if !strings.Contains(rendered, "strconv.") {
		rendered = strings.Replace(rendered, "\t\"strconv\"\n", "", 1)
	}
	if !strings.Contains(rendered, "time.") {
		rendered = strings.Replace(rendered, "\t\"time\"\n", "", 1)
	}
	// When there are no operations, *http.Request is never referenced.
	if !strings.Contains(rendered, "http.") {
		rendered = strings.Replace(rendered, "\t\"net/http\"\n", "", 1)
	}

	// If the import block is now empty, remove it entirely.
	rendered = strings.Replace(rendered, "\nimport (\n)\n", "\n", 1)

	return rendered, nil
}
