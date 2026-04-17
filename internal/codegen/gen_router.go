// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_router.go
package codegen

// routerTemplateData is the data model for router.go.tmpl.
type routerTemplateData struct {
	PkgName    string
	Operations []Operation
}

// GenerateRouter renders the router template for the given package name and
// Smithy model, returning a Go source string with route definitions and a URI
// matcher.
func (g *Generator) GenerateRouter(pkgName string, model *SmithyModel) (string, error) {
	data := routerTemplateData{
		PkgName:    pkgName,
		Operations: model.Operations,
	}
	return g.renderTemplate("router.go.tmpl", data)
}
