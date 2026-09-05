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
	protocolQuery   = "query"
	protocolRESTXML = "rest-xml"
)

// encodeXML renders an engine response as an AWS XML body.
//
// The two dialects differ only in the envelope. botocore's query parser looks
// for <OperationResult> nested inside <OperationResponse> and returns a result
// with nothing in it when the nesting is absent; its rest-xml parser maps the
// root element's children straight onto the output shape and ignores the root's
// name. So query gets both wrappers and rest-xml gets one.
//
// No xmlns is emitted. botocore strips the namespace from every element before
// matching it, so declaring one would be bytes on the wire that nothing reads.
// If that ever stops being true the namespace belongs on OpMeta, from the
// model's xmlNamespace trait, rather than guessed at here.
//
// This is the same "plausible, not faithful" contract as the JSON path: the
// body echoes what the caller stored, in the shape an SDK can parse. It is not
// derived from the model's output shape, so a member the model flattens or
// renames is emitted in its default form.
func encodeXML(protocol, op string, v map[string]any) []byte {
	var b strings.Builder
	b.WriteString(xmlHeader)

	if protocol == protocolQuery {
		b.WriteString("<" + op + "Response>")
	}
	writeElement(&b, op+"Result", v)
	if protocol == protocolQuery {
		// RequestId is not optional: botocore's query parser reads
		// ResponseMetadata into every response's metadata, and an SDK that logs
		// or retries on it gets an empty string otherwise.
		b.WriteString("<ResponseMetadata><RequestId>" + randHex(8) + "</RequestId></ResponseMetadata>")
		b.WriteString("</" + op + "Response>")
	}
	return []byte(b.String())
}

// writeElement writes one <name>…</name> element whose content is value.
func writeElement(b *strings.Builder, name string, value any) {
	b.WriteString("<" + name + ">")
	writeValue(b, value)
	b.WriteString("</" + name + ">")
}

// writeValue writes the content of an element: nested elements for a map,
// <member> entries for a list, escaped text for anything else.
func writeValue(b *strings.Builder, value any) {
	switch val := value.(type) {
	case nil:

	case map[string]any:
		for _, k := range sortedKeys(val) {
			writeElement(b, k, val[k])
		}

	case []map[string]any:
		// AWS wraps list entries in <member> for both dialects unless the model
		// flattens the list. The engine has no flattening information, so it
		// emits the default rather than guessing per service.
		for _, item := range val {
			writeElement(b, "member", item)
		}

	case []any:
		// The engine's own list() produces []map[string]any, but a list echoed
		// back from a caller's JSON body arrives as []any.
		for _, item := range val {
			writeElement(b, "member", item)
		}

	case string:
		xml.EscapeText(b, []byte(val))

	default:
		// Numbers and bools. fmt renders these the way AWS does, and escaping
		// is applied anyway because the branch is reachable from a caller's
		// arbitrary JSON body, not only from the two types named.
		xml.EscapeText(b, []byte(fmt.Sprint(val)))
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
