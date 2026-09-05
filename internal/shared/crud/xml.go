// SPDX-License-Identifier: Apache-2.0

package crud

const xmlHeader = `<?xml version="1.0" encoding="UTF-8"?>`

const (
	protocolQuery   = "query"
	protocolRESTXML = "rest-xml"
)

// encodeXML renders an engine response as an AWS XML body.
//
// Not implemented: this stub exists so the specification in xml_test.go
// compiles and fails at runtime against real assertions rather than as a build
// error. See that file for the dialects and value rules it must satisfy.
func encodeXML(protocol, op string, v map[string]any) []byte {
	return nil
}
