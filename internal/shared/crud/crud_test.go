// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func handleJSON(t *testing.T, service, op string, params map[string]any) (*Result, error) {
	t.Helper()
	body, _ := json.Marshal(params)
	return Handle(Call{Service: service, Protocol: "json-1.1", Op: op, Body: body})
}

func decode(t *testing.T, r *Result) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Body, &m); err != nil {
		t.Fatalf("bad response body: %v", err)
	}
	return m
}

func TestEngineCRUDRoundTrip(t *testing.T) {
	const svc = "testsvc"
	Register(svc, map[string]OpMeta{
		"CreateThing": {Verb: "Create", Resource: "Thing", OutputItemKey: "Thing"},
		"GetThing":    {Verb: "Get", Resource: "Thing", OutputItemKey: "Thing"},
		"ListThings":  {Verb: "List", Resource: "Thing", OutputListKey: "Things"},
		"UpdateThing": {Verb: "Update", Resource: "Thing", OutputItemKey: "Thing"},
		"DeleteThing": {Verb: "Delete", Resource: "Thing"},
	})

	// Create echoes input and synthesizes id/arn under the item wrapper.
	r, err := handleJSON(t, svc, "CreateThing", map[string]any{"ThingName": "a", "Color": "red"})
	if err != nil || r.Status != 200 {
		t.Fatalf("create: status=%d err=%v", statusOf(r), err)
	}
	thing := decode(t, r)["Thing"].(map[string]any)
	if thing["ThingName"] != "a" || thing["Color"] != "red" {
		t.Fatalf("create echo wrong: %v", thing)
	}
	if thing["ThingArn"] == nil || thing["ThingId"] == nil {
		t.Fatalf("create did not synthesize id/arn: %v", thing)
	}

	// Get returns the stored item.
	r, _ = handleJSON(t, svc, "GetThing", map[string]any{"ThingName": "a"})
	if r.Status != 200 {
		t.Fatalf("get status=%d", r.Status)
	}
	if decode(t, r)["Thing"].(map[string]any)["Color"] != "red" {
		t.Fatalf("get did not return stored color")
	}

	// List wraps items under the output list key.
	r, _ = handleJSON(t, svc, "ListThings", map[string]any{})
	things := decode(t, r)["Things"].([]any)
	if len(things) != 1 {
		t.Fatalf("list len = %d, want 1", len(things))
	}

	// Update merges.
	r, _ = handleJSON(t, svc, "UpdateThing", map[string]any{"ThingName": "a", "Color": "blue"})
	if decode(t, r)["Thing"].(map[string]any)["Color"] != "blue" {
		t.Fatalf("update did not merge")
	}

	// Delete then Get -> not found.
	r, _ = handleJSON(t, svc, "DeleteThing", map[string]any{"ThingName": "a"})
	if r.Status != 200 {
		t.Fatalf("delete status=%d", r.Status)
	}
	r, _ = handleJSON(t, svc, "GetThing", map[string]any{"ThingName": "missing"})
	if r.Status != 400 || decode(t, r)["__type"] != "ResourceNotFoundException" {
		t.Fatalf("expected not-found, got %d %v", r.Status, decode(t, r))
	}
}

func TestEngineUnclassified(t *testing.T) {
	const svc = "othersvc"
	Register(svc, map[string]OpMeta{"CreateWidget": {Verb: "Create", Resource: "Widget"}})

	if _, err := handleJSON(t, svc, "SomeCustomOp", nil); err != ErrUnclassified {
		t.Fatalf("unknown op: want ErrUnclassified, got %v", err)
	}
	if _, err := Handle(Call{Service: svc, Protocol: "query", Op: "CreateWidget", Body: []byte("{}")}); err != ErrUnclassified {
		t.Fatalf("non-json protocol: want ErrUnclassified, got %v", err)
	}
	if _, err := handleJSON(t, "unregistered", "CreateThing", nil); err != ErrUnclassified {
		t.Fatalf("unregistered service: want ErrUnclassified, got %v", err)
	}
}

// TestEngineConcurrentAccess exercises the store under concurrent Update (which
// mutates a resource) against Get/List (which marshal it). Before get()/list()
// returned copies, this tripped `fatal error: concurrent map read and map write`
// under -race and crashed the gateway.
func TestEngineConcurrentAccess(t *testing.T) {
	const svc = "racesvc"
	Register(svc, map[string]OpMeta{
		"CreateThing": {Verb: "Create", Resource: "RThing", OutputItemKey: "Thing"},
		"GetThing":    {Verb: "Get", Resource: "RThing", OutputItemKey: "Thing"},
		"ListThings":  {Verb: "List", Resource: "RThing", OutputListKey: "Things"},
		"UpdateThing": {Verb: "Update", Resource: "RThing", OutputItemKey: "Thing"},
	})
	_, _ = handleJSON(t, svc, "CreateThing", map[string]any{"RThingName": "x", "N": float64(0)})

	do := func(op string, params map[string]any) {
		body, _ := json.Marshal(params)
		if _, err := Handle(Call{Service: svc, Protocol: "json-1.1", Op: op, Body: body}); err != nil {
			t.Errorf("%s: %v", op, err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func(n int) { defer wg.Done(); do("UpdateThing", map[string]any{"RThingName": "x", "N": float64(n)}) }(i)
		go func() { defer wg.Done(); do("GetThing", map[string]any{"RThingName": "x"}) }()
		go func() { defer wg.Done(); do("ListThings", map[string]any{}) }()
	}
	wg.Wait()
}

// --- rest-json ---

func handleREST(t *testing.T, service, method, uri string, params map[string]any) (*Result, error) {
	t.Helper()
	var body []byte
	if params != nil {
		body, _ = json.Marshal(params)
	}
	return Handle(Call{Service: service, Protocol: "rest-json", Method: method, URI: uri, Body: body})
}

// TestEngineRESTJSONRoundTrip is the whole point of teaching the engine
// rest-json: the operation name is not in a header, so it has to come from the
// method and path, and the resource identifier has to come from the path label
// rather than the body.
func TestEngineRESTJSONRoundTrip(t *testing.T) {
	const svc = "restsvc"
	Register(svc, map[string]OpMeta{
		"CreateGraph": {Verb: "Create", Resource: "Graph", OutputItemKey: "Graph",
			Method: "POST", URI: "/v1/graphs"},
		"GetGraph": {Verb: "Get", Resource: "Graph", OutputItemKey: "Graph",
			Method: "GET", URI: "/v1/graphs/{GraphName}"},
		"ListGraphs": {Verb: "List", Resource: "Graph", OutputListKey: "Graphs",
			Method: "GET", URI: "/v1/graphs"},
		"DeleteGraph": {Verb: "Delete", Resource: "Graph",
			Method: "DELETE", URI: "/v1/graphs/{GraphName}"},
	})

	// An empty store lists cleanly rather than erroring — the parameterless
	// read every compat smoke test performs.
	r, err := handleREST(t, svc, "GET", "/v1/graphs", nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("list on empty store: status=%d err=%v", statusOf(r), err)
	}
	if got := decode(t, r)["Graphs"].([]any); len(got) != 0 {
		t.Fatalf("empty store listed %d graphs", len(got))
	}

	// Create takes its parameters from the body.
	r, err = handleREST(t, svc, "POST", "/v1/graphs", map[string]any{"GraphName": "g1", "Tier": "small"})
	if err != nil || r.Status != 200 {
		t.Fatalf("create: status=%d err=%v", statusOf(r), err)
	}
	graph := decode(t, r)["Graph"].(map[string]any)
	if graph["GraphName"] != "g1" || graph["Tier"] != "small" {
		t.Fatalf("create echo wrong: %v", graph)
	}

	// Get takes its identifier from the path label, with no body at all. This
	// is the case a JSON-only engine could never serve.
	r, err = handleREST(t, svc, "GET", "/v1/graphs/g1", nil)
	if err != nil || r.Status != 200 {
		t.Fatalf("get by path label: status=%d err=%v", statusOf(r), err)
	}
	if got := decode(t, r)["Graph"].(map[string]any); got["Tier"] != "small" {
		t.Fatalf("get returned the wrong resource: %v", got)
	}

	// Delete, also addressed by path label, and the list goes back to empty.
	if _, err := handleREST(t, svc, "DELETE", "/v1/graphs/g1", nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	r, _ = handleREST(t, svc, "GET", "/v1/graphs", nil)
	if got := decode(t, r)["Graphs"].([]any); len(got) != 0 {
		t.Fatalf("after delete, list returned %d graphs", len(got))
	}
}

// TestEngineRESTJSONContentType guards against serving a rest-json caller the
// X-Amz-Target content type. botocore parses the body by the modelled protocol,
// but an amz-json content type on a REST response is wrong on the wire and
// misleads anyone reading a capture.
func TestEngineRESTJSONContentType(t *testing.T) {
	const svc = "ctsvc"
	Register(svc, map[string]OpMeta{
		"ListThings": {Verb: "List", Resource: "CtThing", OutputListKey: "Things",
			Method: "GET", URI: "/things"},
	})

	r, err := handleREST(t, svc, "GET", "/things", nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if r.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want application/json", r.ContentType)
	}
}

// TestEngineRESTJSONPathLabelWinsOverBody pins which source of a parameter is
// authoritative. A restJson1 model never binds one member to both the URI and
// the body, so real SDK traffic cannot hit this — but the URI is what addresses
// the resource, so a request that does carry both must not be able to redirect
// the lookup from the body.
func TestEngineRESTJSONPathLabelWinsOverBody(t *testing.T) {
	const svc = "authsvc"
	Register(svc, map[string]OpMeta{
		"CreateItem": {Verb: "Create", Resource: "AItem", OutputItemKey: "Item",
			Method: "POST", URI: "/items"},
		"UpdateItem": {Verb: "Update", Resource: "AItem", OutputItemKey: "Item",
			Method: "PUT", URI: "/items/{AItemName}"},
	})

	if _, err := handleREST(t, svc, "POST", "/items", map[string]any{"AItemName": "real"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	r, err := handleREST(t, svc, "PUT", "/items/real",
		map[string]any{"AItemName": "spoofed", "Colour": "blue"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := decode(t, r)["Item"].(map[string]any); got["AItemName"] != "real" {
		t.Errorf("body overrode the path label: AItemName = %v, want real", got["AItemName"])
	}
}

// TestEngineRESTJSONQueryParams covers the third parameter source: values the
// model binds with httpQuery rather than into the body.
func TestEngineRESTJSONQueryParams(t *testing.T) {
	const svc = "qsvc"
	Register(svc, map[string]OpMeta{
		"CreateQThing": {Verb: "Create", Resource: "QThing", OutputItemKey: "Thing",
			Method: "POST", URI: "/qthings"},
	})

	r, err := handleREST(t, svc, "POST", "/qthings?QThingName=fromquery", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := decode(t, r)["Thing"].(map[string]any); got["QThingName"] != "fromquery" {
		t.Errorf("query parameter ignored: %v", got)
	}
}

// TestEngineRESTJSONUnmatchedPathIsUnclassified is the guarantee the whole
// coverage claim rests on. A path the route table does not know must decline,
// never fabricate an empty success — see docs/coverage.md.
func TestEngineRESTJSONUnmatchedPathIsUnclassified(t *testing.T) {
	const svc = "misssvc"
	Register(svc, map[string]OpMeta{
		"ListThings": {Verb: "List", Resource: "MThing", OutputListKey: "Things",
			Method: "GET", URI: "/things"},
	})

	if _, err := handleREST(t, svc, "GET", "/not/a/known/path", nil); err != ErrUnclassified {
		t.Errorf("unknown path: want ErrUnclassified, got %v", err)
	}
	if _, err := handleREST(t, svc, "POST", "/things", nil); err != ErrUnclassified {
		t.Errorf("known path, wrong method: want ErrUnclassified, got %v", err)
	}
	if _, err := handleREST(t, "unregistered", "GET", "/things", nil); err != ErrUnclassified {
		t.Errorf("unregistered service: want ErrUnclassified, got %v", err)
	}
}

// TestEngineRESTJSONUnroutedOperationIsUnclassified covers the operation that is
// reachable by path but not CRUD-shaped. codegen omits it from the registry, so
// it has no route, and the caller must get a clean error.
func TestEngineRESTJSONUnroutedOperationIsUnclassified(t *testing.T) {
	const svc = "execsvc"
	// Modelled on rds-data: every operation is a verb the classifier refuses,
	// so nothing registers and nothing routes.
	Register(svc, map[string]OpMeta{})

	if _, err := handleREST(t, svc, "POST", "/Execute", nil); err != ErrUnclassified {
		t.Errorf("want ErrUnclassified, got %v", err)
	}
}

// TestEngineQueryStillDeclines pins what is left of the boundary. query names
// its operation in the form body rather than in the path, so until the engine
// learns to read that, a query call must decline.
//
// This replaces TestEngineRESTXMLStillDeclines: rest-xml is no longer on the
// wrong side of the line. Every one of its operations is bound to a method and
// a URI template, exactly like rest-json, so the same route table classifies it.
func TestEngineQueryStillDeclines(t *testing.T) {
	const svc = "querysvc"
	Register(svc, map[string]OpMeta{
		"ListThings": {Verb: "List", Resource: "QYThing", OutputListKey: "Things",
			Method: "GET", URI: "/things"},
	})

	_, err := Handle(Call{Service: svc, Protocol: "query", Method: "GET", URI: "/things"})
	if err != ErrUnclassified {
		t.Errorf("query: want ErrUnclassified, got %v", err)
	}
}

// TestServableAndNeedsBody separates two questions the engine used to answer
// with one predicate.
//
// "Can the engine serve this protocol" and "must the gateway buffer the body
// before calling it" are not the same question, and rest-xml is where they come
// apart: the engine can classify it from the route table, but S3 speaks it and
// S3 bodies are large binary uploads. Buffering those to serve a service that
// never reaches the engine would turn a streaming path into a memory one.
func TestServableAndNeedsBody(t *testing.T) {
	cases := []struct {
		protocol            string
		servable, needsBody bool
	}{
		{"json-1.0", true, true},
		{"json-1.1", true, true},
		{"json", true, true},
		{"rest-json", true, true},
		// The whole reason the predicates are separate.
		{"rest-xml", true, false},
		// Admitted in Task 4; its operation name is in the body, so it must be
		// buffered before it can be read.
		{"query", false, true},
		{"ec2-query", false, false},
		{"nonsense", false, false},
	}
	for _, c := range cases {
		t.Run(c.protocol, func(t *testing.T) {
			if got := Servable(c.protocol); got != c.servable {
				t.Errorf("Servable(%q) = %v, want %v", c.protocol, got, c.servable)
			}
			if got := NeedsBody(c.protocol); got != c.needsBody {
				t.Errorf("NeedsBody(%q) = %v, want %v", c.protocol, got, c.needsBody)
			}
		})
	}
}

// --- rest-xml ---

func handleXML(t *testing.T, service, method, uri string) (*Result, error) {
	t.Helper()
	// Deliberately no body: rest-xml is not buffered by the gateway, so the
	// engine must serve it from the path and query alone.
	return Handle(Call{Service: service, Protocol: "rest-xml", Method: method, URI: uri})
}

// TestEngineRESTXMLRoundTrip uses S3 Control's own routes, because it is the
// service this unlocks and its identifiers all arrive as path labels.
func TestEngineRESTXMLRoundTrip(t *testing.T) {
	const svc = "s3controltest"
	Register(svc, map[string]OpMeta{
		"CreateAccessPoint": {Verb: "Create", Resource: "AccessPoint", OutputItemKey: "AccessPoint",
			Method: "PUT", URI: "/v20180820/accesspoint/{Name}"},
		"GetAccessPoint": {Verb: "Get", Resource: "AccessPoint", OutputItemKey: "AccessPoint",
			Method: "GET", URI: "/v20180820/accesspoint/{Name}"},
		"ListAccessPoints": {Verb: "List", Resource: "AccessPoint", OutputListKey: "AccessPointList",
			Method: "GET", URI: "/v20180820/accesspoint"},
		"DeleteAccessPoint": {Verb: "Delete", Resource: "AccessPoint",
			Method: "DELETE", URI: "/v20180820/accesspoint/{Name}"},
	})

	// The parameterless read on an empty store, which is what the compat smoke
	// test performs for every engine-served service.
	r, err := handleXML(t, svc, "GET", "/v20180820/accesspoint")
	if err != nil || r.Status != 200 {
		t.Fatalf("list on empty store: status=%d err=%v", statusOf(r), err)
	}
	if got := string(r.Body); !strings.Contains(got, "<AccessPointList></AccessPointList>") {
		t.Fatalf("empty list not rendered as an empty element: %s", got)
	}

	// Create: the only parameter is the path label, and there is no body at all.
	r, err = handleXML(t, svc, "PUT", "/v20180820/accesspoint/ap1")
	if err != nil || r.Status != 200 {
		t.Fatalf("create: status=%d err=%v", statusOf(r), err)
	}
	if got := string(r.Body); !strings.Contains(got, "<Name>ap1</Name>") {
		t.Fatalf("create did not echo the path label: %s", got)
	}

	// Get addresses the same resource by the same label.
	r, err = handleXML(t, svc, "GET", "/v20180820/accesspoint/ap1")
	if err != nil || r.Status != 200 {
		t.Fatalf("get: status=%d err=%v", statusOf(r), err)
	}
	if got := string(r.Body); !strings.Contains(got, "<Name>ap1</Name>") {
		t.Fatalf("get returned the wrong resource: %s", got)
	}

	// It is in the list, wrapped as a member.
	r, _ = handleXML(t, svc, "GET", "/v20180820/accesspoint")
	if got := string(r.Body); !strings.Contains(got, "<member>") {
		t.Fatalf("created resource is not in the list: %s", got)
	}

	// Delete, then the list is empty again.
	if _, err := handleXML(t, svc, "DELETE", "/v20180820/accesspoint/ap1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	r, _ = handleXML(t, svc, "GET", "/v20180820/accesspoint")
	if got := string(r.Body); strings.Contains(got, "<member>") {
		t.Fatalf("resource survived delete: %s", got)
	}
}

func TestEngineRESTXMLContentType(t *testing.T) {
	const svc = "xmlctsvc"
	Register(svc, map[string]OpMeta{
		"ListThings": {Verb: "List", Resource: "XCThing", OutputListKey: "Things",
			Method: "GET", URI: "/things"},
	})

	r, err := handleXML(t, svc, "GET", "/things")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if r.ContentType != "application/xml" {
		t.Errorf("ContentType = %q, want application/xml", r.ContentType)
	}
	if !strings.HasPrefix(string(r.Body), xmlHeader) {
		t.Errorf("body is not XML: %s", r.Body)
	}
}

// TestEngineRESTXMLUnmatchedPathIsUnclassified is the guarantee that matters
// most for this protocol, because S3 speaks it. A path the route table does not
// know must decline rather than answer from the generic store.
func TestEngineRESTXMLUnmatchedPathIsUnclassified(t *testing.T) {
	const svc = "xmlmisssvc"
	Register(svc, map[string]OpMeta{
		"ListThings": {Verb: "List", Resource: "XMThing", OutputListKey: "Things",
			Method: "GET", URI: "/v20180820/things"},
	})

	if _, err := handleXML(t, svc, "GET", "/my-bucket/my-key"); err != ErrUnclassified {
		t.Errorf("an S3-shaped path: want ErrUnclassified, got %v", err)
	}
	if _, err := handleXML(t, svc, "POST", "/v20180820/things"); err != ErrUnclassified {
		t.Errorf("known path, wrong method: want ErrUnclassified, got %v", err)
	}
	if _, err := handleXML(t, "unregistered", "GET", "/v20180820/things"); err != ErrUnclassified {
		t.Errorf("unregistered service: want ErrUnclassified, got %v", err)
	}
}

func statusOf(r *Result) int {
	if r == nil {
		return 0
	}
	return r.Status
}
