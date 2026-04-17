// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_errors.go
package codegen

import (
	"sort"
	"strings"
)

// errorData is the template data for a single error shape.
type errorData struct {
	Name       string
	HTTPStatus int
}

// errorsTemplateData is the data model for errors.go.tmpl.
type errorsTemplateData struct {
	PkgName string
	Errors  []errorData
}

// GenerateErrors renders the errors template for the given package name and
// Smithy model. It collects all shapes that have an ErrorTrait and passes
// their names and HTTP status codes to the template.
func (g *Generator) GenerateErrors(pkgName string, model *SmithyModel) (string, error) {
	// Collect shape names in sorted order for deterministic output.
	names := make([]string, 0, len(model.Shapes))
	for name := range model.Shapes {
		names = append(names, name)
	}
	sort.Strings(names)

	data := errorsTemplateData{PkgName: pkgName}
	for _, name := range names {
		shape := model.Shapes[name]
		if shape.ErrorTrait == nil {
			continue
		}
		data.Errors = append(data.Errors, errorData{
			Name:       name,
			HTTPStatus: shape.ErrorTrait.HTTPStatus,
		})
	}

	rendered, err := g.renderTemplate("errors.go.tmpl", data)
	if err != nil {
		return "", err
	}

	// Remove unused "fmt" import when no errors are generated.
	if !strings.Contains(rendered, "fmt.") {
		rendered = strings.Replace(rendered, "\nimport \"fmt\"\n", "\n", 1)
	}

	return rendered, nil
}
