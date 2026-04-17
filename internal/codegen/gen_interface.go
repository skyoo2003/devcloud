// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_interface.go
package codegen

import "strings"

// interfaceTemplateData is the data model for interface.go.tmpl.
type interfaceTemplateData struct {
	PackageName   string
	InterfaceName string
	Operations    []Operation
}

// toInterfaceName converts a service name like "AmazonS3" or "AWSS3" into
// an interface name like "S3Service".
func toInterfaceName(serviceName string) string {
	name := serviceName
	if strings.HasPrefix(name, "Amazon") {
		name = strings.TrimPrefix(name, "Amazon")
	} else if strings.HasPrefix(name, "AWS") {
		name = strings.TrimPrefix(name, "AWS")
	}
	return name + "Service"
}

// GenerateInterface renders the interface template for the given package name
// and Smithy model, returning a Go source string.
func (g *Generator) GenerateInterface(pkgName string, model *SmithyModel) (string, error) {
	data := interfaceTemplateData{
		PackageName:   pkgName,
		InterfaceName: toInterfaceName(model.ServiceName),
		Operations:    model.Operations,
	}
	rendered, err := g.renderTemplate("interface.go.tmpl", data)
	if err != nil {
		return "", err
	}

	// Remove unused "context" import when there are no operations.
	if len(model.Operations) == 0 {
		rendered = strings.Replace(rendered, "\nimport \"context\"\n", "\n", 1)
	}

	return rendered, nil
}
