// SPDX-License-Identifier: Apache-2.0

package codegen

import (
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

type scaffoldTemplateData struct {
	PkgName       string
	ServiceID     string
	ServiceName   string
	ProtocolConst string
}

func protocolToConst(protocol string) string {
	switch protocol {
	case "rest-xml":
		return "ProtocolRESTXML"
	case "json-1.0":
		return "ProtocolJSON10"
	case "json-1.1":
		return "ProtocolJSON11"
	case "query":
		return "ProtocolQuery"
	case "rest-json":
		return "ProtocolRESTJSON"
	default:
		return "ProtocolJSON10"
	}
}

func cleanServiceName(name string) string {
	name = strings.TrimPrefix(name, "Amazon")
	name = strings.TrimPrefix(name, "AWS")
	return strings.TrimSpace(name)
}

func (g *Generator) GenerateScaffold(pkgName string, model *ir.Model) (string, error) {
	data := scaffoldTemplateData{
		PkgName:       pkgName,
		ServiceID:     model.ServiceID,
		ServiceName:   cleanServiceName(model.ServiceName),
		ProtocolConst: protocolToConst(model.Protocol),
	}
	return g.renderTemplate("scaffold_provider.go.tmpl", data)
}
