// SPDX-License-Identifier: Apache-2.0

package codegen

type registerTemplateData struct {
	PkgName   string
	ServiceID string
}

func (g *Generator) GenerateRegister(pkgName string, model *SmithyModel) (string, error) {
	data := registerTemplateData{
		PkgName:   pkgName,
		ServiceID: model.ServiceID,
	}
	return g.renderTemplate("register.go.tmpl", data)
}
