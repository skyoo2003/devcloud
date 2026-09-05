// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"regexp"
	"strings"
	"testing"
)

// requestIDPattern strips the synthesized RequestId so envelope assertions can
// compare a fixed string.
var requestIDPattern = regexp.MustCompile(`<RequestId>[0-9a-f]+</RequestId>`)

func normalize(b []byte) string {
	s := requestIDPattern.ReplaceAllString(string(b), "<RequestId>ID</RequestId>")
	return strings.TrimPrefix(s, xmlHeader)
}

// TestEncodeXMLEnvelopes pins the two dialects apart. botocore's query parser
// looks for <OperationResult> inside <OperationResponse>; its rest-xml parser
// maps the root element's children straight onto the output shape and ignores
// the root's name.
func TestEncodeXMLEnvelopes(t *testing.T) {
	body := map[string]any{"DNSName": "lb-1.example.com"}

	got := normalize(encodeXML(protocolQuery, "CreateLoadBalancer", body))
	want := "<CreateLoadBalancerResponse>" +
		"<CreateLoadBalancerResult><DNSName>lb-1.example.com</DNSName></CreateLoadBalancerResult>" +
		"<ResponseMetadata><RequestId>ID</RequestId></ResponseMetadata>" +
		"</CreateLoadBalancerResponse>"
	if got != want {
		t.Errorf("query envelope:\n got %s\nwant %s", got, want)
	}

	got = normalize(encodeXML(protocolRESTXML, "CreateAccessPoint", body))
	want = "<CreateAccessPointResult><DNSName>lb-1.example.com</DNSName></CreateAccessPointResult>"
	if got != want {
		t.Errorf("rest-xml envelope:\n got %s\nwant %s", got, want)
	}
}

func TestEncodeXMLValues(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]any
		want string
	}{
		{
			// Keys are sorted so the same store produces the same bytes across
			// runs — Go map iteration is randomised, same reason Register sorts
			// the route table.
			name: "keys_are_sorted",
			in:   map[string]any{"Zeta": "z", "Alpha": "a", "Mu": "m"},
			want: "<Alpha>a</Alpha><Mu>m</Mu><Zeta>z</Zeta>",
		},
		{
			name: "scalars",
			in:   map[string]any{"B": true, "N": 42, "F": 1.5, "S": "x"},
			want: "<B>true</B><F>1.5</F><N>42</N><S>x</S>",
		},
		{
			// AWS wraps list entries in <member> for both dialects unless the
			// model flattens them. The engine has no flattening information, so
			// it emits the default.
			name: "list_wraps_members",
			in: map[string]any{"Things": []map[string]any{
				{"Name": "a"}, {"Name": "b"},
			}},
			want: "<Things><member><Name>a</Name></member><member><Name>b</Name></member></Things>",
		},
		{
			// The engine's own list() returns []map[string]any, but a value
			// echoed back from a caller's JSON body arrives as []any.
			name: "any_slice_wraps_members",
			in:   map[string]any{"Names": []any{"a", "b"}},
			want: "<Names><member>a</member><member>b</member></Names>",
		},
		{
			// An empty collection must be an empty element, not an omitted one:
			// botocore reads a missing key as absent rather than as empty.
			name: "empty_list_is_an_empty_element",
			in:   map[string]any{"Things": []map[string]any{}},
			want: "<Things></Things>",
		},
		{
			name: "nested_map",
			in:   map[string]any{"Outer": map[string]any{"Inner": "v"}},
			want: "<Outer><Inner>v</Inner></Outer>",
		},
		{
			// Query input is flat form-encoding, so a structured member arrives
			// as "Listeners.member.1.Protocol". There is nowhere to put it in a
			// generic store and it is not a name any SDK expects back, so it is
			// dropped rather than emitted as a bogus element.
			name: "dotted_keys_are_dropped",
			in: map[string]any{
				"LoadBalancerName":            "lb",
				"Listeners.member.1.Protocol": "HTTP",
			},
			want: "<LoadBalancerName>lb</LoadBalancerName>",
		},
		{
			// A caller controls these values. Emitting them raw produces a body
			// botocore cannot parse at all, which reads as a transport failure
			// rather than as the echo it is.
			name: "values_are_escaped",
			in:   map[string]any{"S": `a<b&c>"d"`},
			want: "<S>a&lt;b&amp;c&gt;&#34;d&#34;</S>",
		},
		{
			// Nothing in the engine produces nil, but a caller's JSON body can.
			name: "nil_is_an_empty_element",
			in:   map[string]any{"S": nil},
			want: "<S></S>",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := normalize(encodeXML(protocolRESTXML, "Op", c.in))
			want := "<OpResult>" + c.want + "</OpResult>"
			if got != want {
				t.Errorf("\n got %s\nwant %s", got, want)
			}
		})
	}
}

// TestEncodeXMLIsDeterministic guards the sorting above against a regression
// that only shows up as a flaky diff between runs.
func TestEncodeXMLIsDeterministic(t *testing.T) {
	in := map[string]any{"A": "1", "B": "2", "C": "3", "D": "4", "E": "5", "F": "6"}
	first := normalize(encodeXML(protocolRESTXML, "Op", in))
	for i := 0; i < 50; i++ {
		if got := normalize(encodeXML(protocolRESTXML, "Op", in)); got != first {
			t.Fatalf("run %d differs:\n got %s\nfirst %s", i, got, first)
		}
	}
}

// TestEncodeXMLDeclaresItsHeader keeps the XML declaration on the body: botocore
// tolerates its absence, but the gateway's own error envelopes emit it and a
// response that differs from an error only in that respect is a needless
// difference.
func TestEncodeXMLDeclaresItsHeader(t *testing.T) {
	if !strings.HasPrefix(string(encodeXML(protocolQuery, "Op", nil)), xmlHeader) {
		t.Error("body does not start with the XML declaration")
	}
}
