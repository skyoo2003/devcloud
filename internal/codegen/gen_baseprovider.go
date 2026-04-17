// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_baseprovider.go
package codegen

import "strings"

// baseProviderTemplateData is the data model for base_provider.go.tmpl.
type baseProviderTemplateData struct {
	PackageName string
	Operations  []Operation
}

// GenerateBaseProvider renders the base provider template for the given package
// name and Smithy model, returning a Go source string with NotImplemented stubs.
func (g *Generator) GenerateBaseProvider(pkgName string, model *SmithyModel) (string, error) {
	data := baseProviderTemplateData{
		PackageName: pkgName,
		Operations:  model.Operations,
	}
	rendered, err := g.renderTemplate("base_provider.go.tmpl", data)
	if err != nil {
		return "", err
	}

	// Remove unused "context" import when there are no operations.
	if len(model.Operations) == 0 {
		rendered = strings.Replace(rendered, "\t\"context\"\n", "", 1)
	}

	return rendered, nil
}
