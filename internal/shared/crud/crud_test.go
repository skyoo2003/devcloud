// SPDX-License-Identifier: Apache-2.0

package crud

import (
	"encoding/json"
	"testing"
)

func handleJSON(t *testing.T, service, op string, params map[string]any) (*Result, error) {
	t.Helper()
	body, _ := json.Marshal(params)
	return Handle(service, op, "json-1.1", body)
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
	if _, err := Handle(svc, "CreateWidget", "query", []byte("{}")); err != ErrUnclassified {
		t.Fatalf("non-json protocol: want ErrUnclassified, got %v", err)
	}
	if _, err := handleJSON(t, "unregistered", "CreateThing", nil); err != ErrUnclassified {
		t.Fatalf("unregistered service: want ErrUnclassified, got %v", err)
	}
}

func statusOf(r *Result) int {
	if r == nil {
		return 0
	}
	return r.Status
}
