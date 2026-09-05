// SPDX-License-Identifier: Apache-2.0

package httproute

import "testing"

func TestMatchURILabels(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		uri     string
		want    Params
		ok      bool
	}{
		{
			name:    "literal path",
			pattern: "/2020-05-31/distribution",
			uri:     "/2020-05-31/distribution",
			want:    Params{},
			ok:      true,
		},
		{
			name:    "single-segment label",
			pattern: "/2020-05-31/distribution/{Id}",
			uri:     "/2020-05-31/distribution/E123",
			want:    Params{"Id": "E123"},
			ok:      true,
		},
		{
			name:    "two labels",
			pattern: "/v1/{Bucket}/object/{Key}",
			uri:     "/v1/mybucket/object/mykey",
			want:    Params{"Bucket": "mybucket", "Key": "mykey"},
			ok:      true,
		},
		{
			name:    "greedy label consumes the remaining segments",
			pattern: "/v1/objects/{Key+}",
			uri:     "/v1/objects/a/b/c.txt",
			want:    Params{"Key": "a/b/c.txt"},
			ok:      true,
		},
		{
			name:    "greedy label needs at least one segment",
			pattern: "/v1/objects/{Key+}",
			uri:     "/v1/objects",
			ok:      false,
		},
		{
			name:    "trailing slash is not an extra segment",
			pattern: "/2020-05-31/distribution",
			uri:     "/2020-05-31/distribution/",
			want:    Params{},
			ok:      true,
		},
		{
			name:    "literal segment mismatch",
			pattern: "/2020-05-31/distribution",
			uri:     "/2020-05-31/streaming-distribution",
			ok:      false,
		},
		{
			name:    "request has too few segments",
			pattern: "/2020-05-31/distribution/{Id}",
			uri:     "/2020-05-31/distribution",
			ok:      false,
		},
		{
			name:    "request has too many segments",
			pattern: "/2020-05-31/distribution/{Id}",
			uri:     "/2020-05-31/distribution/E123/config",
			ok:      false,
		},
		{
			name:    "query string is ignored when the pattern does not constrain it",
			pattern: "/2020-05-31/distribution",
			uri:     "/2020-05-31/distribution?Marker=abc&MaxItems=10",
			want:    Params{},
			ok:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := MatchURI(tc.pattern, tc.uri)
			if ok != tc.ok {
				t.Fatalf("MatchURI(%q, %q) ok = %v, want %v", tc.pattern, tc.uri, ok, tc.ok)
			}
			if !ok {
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("params = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("params[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestMatchURIQueryConstraints(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		uri     string
		ok      bool
	}{
		{
			name:    "bare key must be present",
			pattern: "/2020-05-31/distribution?WithTags",
			uri:     "/2020-05-31/distribution?WithTags",
			ok:      true,
		},
		{
			name:    "bare key absent does not match",
			pattern: "/2020-05-31/distribution?WithTags",
			uri:     "/2020-05-31/distribution",
			ok:      false,
		},
		{
			name:    "key=value must match the value",
			pattern: "/2020-05-31/tagging?Operation=Tag",
			uri:     "/2020-05-31/tagging?Operation=Tag&Resource=arn",
			ok:      true,
		},
		{
			name:    "key present with the wrong value does not match",
			pattern: "/2020-05-31/tagging?Operation=Tag",
			uri:     "/2020-05-31/tagging?Operation=Untag",
			ok:      false,
		},
		{
			name:    "every term of a multi-term constraint must hold",
			pattern: "/v1/thing?a=1&b=2",
			uri:     "/v1/thing?a=1",
			ok:      false,
		},
		{
			name:    "a malformed tail cannot hide a valid earlier term",
			pattern: "/v1/thing?a=1",
			uri:     "/v1/thing?a=1&%zz",
			ok:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := MatchURI(tc.pattern, tc.uri); ok != tc.ok {
				t.Errorf("MatchURI(%q, %q) ok = %v, want %v", tc.pattern, tc.uri, ok, tc.ok)
			}
		})
	}
}

// TestMatchPrefersConstrainedRoutes locks the two-pass ordering the generated
// routers depend on. OperationRoutes is sorted by operation name, not by
// specificity, so an unconstrained "/distribution" route appears before
// "/distribution?WithTags" and would shadow it in a single pass.
func TestMatchPrefersConstrainedRoutes(t *testing.T) {
	routes := []Route{
		// Deliberately in the order codegen emits: alphabetical by operation.
		{Method: "POST", Pattern: "/2020-05-31/distribution", Operation: "CreateDistribution"},
		{Method: "POST", Pattern: "/2020-05-31/distribution?WithTags", Operation: "CreateDistributionWithTags"},
	}

	if op, _ := Match(routes, "POST", "/2020-05-31/distribution?WithTags"); op != "CreateDistributionWithTags" {
		t.Errorf("constrained route lost to the unconstrained one: got %q", op)
	}
	if op, _ := Match(routes, "POST", "/2020-05-31/distribution"); op != "CreateDistribution" {
		t.Errorf("unconstrained route did not match: got %q", op)
	}
}

func TestMatchMethodAndMiss(t *testing.T) {
	routes := []Route{
		{Method: "GET", Pattern: "/2020-05-31/distribution/{Id}", Operation: "GetDistribution"},
		{Method: "DELETE", Pattern: "/2020-05-31/distribution/{Id}", Operation: "DeleteDistribution"},
	}

	op, params := Match(routes, "GET", "/2020-05-31/distribution/E123")
	if op != "GetDistribution" {
		t.Fatalf("op = %q, want GetDistribution", op)
	}
	if params["Id"] != "E123" {
		t.Errorf("params[Id] = %q, want E123", params["Id"])
	}

	if op, _ := Match(routes, "PUT", "/2020-05-31/distribution/E123"); op != "" {
		t.Errorf("method mismatch matched %q, want no match", op)
	}

	// A miss must be an empty operation and nil params, never a zero-value
	// route: the CRUD engine treats "" as unclassified, and a non-nil empty
	// params map would read as a successful match with no labels.
	op, params = Match(routes, "GET", "/some/other/path")
	if op != "" || params != nil {
		t.Errorf("miss returned (%q, %v), want (\"\", nil)", op, params)
	}
}
