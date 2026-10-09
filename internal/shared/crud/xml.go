// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
)

const xmlHeader = `<?xml version="1.0" encoding="UTF-8"?>`

const (
	protocolQuery    = "query"
	protocolRESTXML  = "rest-xml"
	protocolEC2Query = "ec2-query"
)

// encodeXML renders an engine response as an AWS XML body.
//
// The dialects differ in the envelope:
// - query: <OperationResponse><OperationResult>...</OperationResult><ResponseMetadata><RequestId>...</RequestId></ResponseMetadata></OperationResponse>
// - rest-xml: <OperationResult>...</OperationResult> (botocore maps root children directly)
// - ec2-query: <OperationResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>...</requestId>...</OperationResponse>
//
// This is the same "plausible, not faithful" contract as the JSON path: the
// body echoes what the caller stored, in the shape an SDK can parse. It is not
// derived from the model's output shape, so a member the model flattens or
// renames is emitted in its default form.
func encodeXML(protocol, op string, v map[string]any) []byte {
	var b strings.Builder
	b.WriteString(xmlHeader)

	switch protocol {
	case protocolEC2Query:
		b.WriteString("<" + op + "Response xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">")
		b.WriteString("<requestId>" + randHex(8) + "</requestId>")
		writeValue(&b, protocol, v)
		b.WriteString("</" + op + "Response>")
	case protocolQuery:
		b.WriteString("<" + op + "Response>")
		writeElement(&b, protocol, op+"Result", v)
		b.WriteString("<ResponseMetadata><RequestId>" + randHex(8) + "</RequestId></ResponseMetadata>")
		b.WriteString("</" + op + "Response>")
	default:
		writeElement(&b, protocol, op+"Result", v)
	}
	return []byte(b.String())
}

// writeElement writes one <name>…</name> element whose content is value.
func writeElement(b *strings.Builder, protocol, name string, value any) {
	b.WriteString("<" + name + ">")
	writeValue(b, protocol, value)
	b.WriteString("</" + name + ">")
}

// writeValue writes the content of an element: nested elements for a map,
// <item> (for ec2-query) or <member> (for query/rest-xml) entries for a list, escaped text for anything else.
func writeValue(b *strings.Builder, protocol string, value any) {
	switch val := value.(type) {
	case nil:

	case map[string]any:
		for _, k := range sortedKeys(val) {
			writeElement(b, protocol, k, val[k])
		}

	case []map[string]any:
		listElem := "member"
		if protocol == protocolEC2Query {
			listElem = "item"
		}
		for _, item := range val {
			writeElement(b, protocol, listElem, item)
		}

	case []any:
		listElem := "member"
		if protocol == protocolEC2Query {
			listElem = "item"
		}
		for _, item := range val {
			writeElement(b, protocol, listElem, item)
		}

	case string:
		// The error is the Writer's, and a strings.Builder never fails a write.
		_ = xml.EscapeText(b, []byte(val))

	default:
		// Numbers and bools. fmt renders these the way AWS does, and escaping
		// is applied anyway because the branch is reachable from a caller's
		// arbitrary JSON body, not only from the two types named.
		_ = xml.EscapeText(b, []byte(fmt.Sprint(val)))
	}
}

// sortedKeys returns the emittable keys of a response map in a stable order.
//
// Sorted because Go map iteration is randomised and an unsorted body would make
// the same stored resource serialize differently between runs of the same
// binary — the same reason Register sorts the route table.
//
// A key containing "." is dropped. Query input is flat form-encoding, so a
// structured member arrives as "Listeners.member.1.Protocol"; there is nowhere
// to put it in a generic store, and no SDK expects an element by that name, so
// echoing it back would be noise in a response rather than information.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if strings.Contains(k, ".") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
