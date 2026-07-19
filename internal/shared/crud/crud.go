// SPDX-License-Identifier: Apache-2.0

// Package crud is a generic, Smithy-model-driven fallback engine that serves
// standard CRUD-shaped operations (Create/Get/List/Delete/Update/Tag/…) which a
// service's hand-written provider has not implemented.
//
// Fidelity is deliberately "plausible, not faithful": responses are store-backed
// and echo the caller's input plus synthesized ids/ARNs, so SDKs round-trip, but
// there is no validation, cross-resource integrity, or business logic. Operations
// the engine cannot classify return ErrUnclassified so the caller falls back to a
// normal AWS error. See docs/crud-engine.md for fidelity tiers and scope.
package crud

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

// ErrUnclassified means the engine has no CRUD handling for this operation; the
// caller should fall back to its usual "unknown action" error.
var ErrUnclassified = errors.New("crud: operation not classifiable")

// OpMeta describes how one operation maps onto generic CRUD behaviour. It is
// produced by codegen from the Smithy model and registered per service.
type OpMeta struct {
	Verb          string // canonical action: Create, Get, List, Delete, Update, Tag, Untag, Relate, Toggle
	Resource      string // singular resource key, e.g. "Database"
	OutputListKey string // output member holding the collection (List ops)
	OutputItemKey string // output member wrapping a single resource ("" = flat echo)
}

// Result is a protocol-agnostic response the caller maps onto its transport.
type Result struct {
	Status      int
	Body        []byte
	ContentType string
}

const jsonContentType = "application/x-amz-json-1.1"

var (
	mu       sync.RWMutex
	registry = map[string]map[string]OpMeta{}         // service -> op -> meta
	store    = map[string]map[string]map[string]any{} // "service|resource" -> id -> doc
)

// Register records a service's operation metadata. The generated crudregistry
// package calls this from a single init().
func Register(service string, ops map[string]OpMeta) {
	mu.Lock()
	defer mu.Unlock()
	registry[service] = ops
}

// RegisteredOps returns the operation metadata registered for a service.
func RegisteredOps(service string) map[string]OpMeta {
	mu.RLock()
	defer mu.RUnlock()
	return registry[service]
}

// JSONProtocol reports whether the engine can serve requests for a protocol.
// Only the X-Amz-Target JSON protocols carry an operation name at the router,
// which the engine needs to classify.
func JSONProtocol(protocol string) bool {
	return strings.HasPrefix(protocol, "json")
}

// Handle attempts to serve op for service from its request body. It returns
// ErrUnclassified when the operation is unknown or not CRUD-shaped.
func Handle(service, op, protocol string, body []byte) (*Result, error) {
	if !JSONProtocol(protocol) {
		return nil, ErrUnclassified
	}
	mu.RLock()
	m, ok := registry[service][op]
	mu.RUnlock()
	if !ok || m.Verb == "" {
		return nil, ErrUnclassified
	}

	var params map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &params); err != nil {
			// Malformed body: real services reject with SerializationException
			// rather than fabricating a success from empty params.
			return withContentType(serializationError(), protocol), nil
		}
	}
	if params == nil {
		params = map[string]any{}
	}
	res, err := dispatch(service, m, params)
	return withContentType(res, protocol), err
}

// withContentType stamps the protocol-appropriate amz-json version so a json-1.0
// service is not served a 1.1 content type. dispatch/okJSON default to 1.1.
func withContentType(res *Result, protocol string) *Result {
	if res != nil && protocol == "json-1.0" {
		res.ContentType = "application/x-amz-json-1.0"
	}
	return res
}

func dispatch(service string, m OpMeta, params map[string]any) (*Result, error) {
	switch m.Verb {
	case "Create":
		id := resourceID(m.Resource, params)
		item := cloneMap(params)
		stamp(item, service, m.Resource, id)
		put(service, m.Resource, id, item)
		return okJSON(wrapItem(m, item))

	case "Get":
		// A Describe*/Get* whose output is a collection (list key, no single-item
		// wrapper) returns the stored list, not a single-resource lookup — a fresh
		// store yields an empty collection (200), not ResourceNotFoundException.
		if m.OutputItemKey == "" && m.OutputListKey != "" {
			return okJSON(map[string]any{m.OutputListKey: list(service, m.Resource)})
		}
		id := resourceID(m.Resource, params)
		item, found := get(service, m.Resource, id)
		if !found {
			return notFound(m.Resource), nil
		}
		return okJSON(wrapItem(m, item))

	case "List":
		key := m.OutputListKey
		if key == "" {
			key = m.Resource + "s"
		}
		return okJSON(map[string]any{key: list(service, m.Resource)})

	case "Delete":
		id := resourceID(m.Resource, params)
		del(service, m.Resource, id)
		return okJSON(map[string]any{})

	case "Update":
		id := resourceID(m.Resource, params)
		item, found := get(service, m.Resource, id)
		if !found {
			item = cloneMap(params)
			stamp(item, service, m.Resource, id)
		} else {
			for k, v := range params {
				item[k] = v
			}
		}
		put(service, m.Resource, id, item)
		return okJSON(wrapItem(m, item))

	case "Tag", "Untag", "Relate", "Toggle":
		// Relationship / tag / status flips: acknowledge without modelling state.
		return okJSON(map[string]any{})

	default:
		return nil, ErrUnclassified
	}
}

// resourceID picks the caller-supplied identifier for a resource, or generates
// one when none is present (e.g. Create with a server-assigned id).
func resourceID(resource string, params map[string]any) string {
	for _, k := range []string{
		resource + "Name", resource + "Id", resource + "Arn", resource + "Identifier",
		"Name", "Id", "Arn", "Identifier", "ResourceName", "ResourceArn",
	} {
		if v, ok := params[k].(string); ok && v != "" {
			return v
		}
	}
	return "res-" + randHex(10)
}

// stamp fills in plausible server-assigned fields so single-resource responses
// look populated. It never overwrites caller-supplied values.
func stamp(item map[string]any, service, resource, id string) {
	setIfAbsent(item, resource+"Id", id)
	setIfAbsent(item, resource+"Arn",
		"arn:aws:"+service+":us-east-1:000000000000:"+strings.ToLower(resource)+"/"+id)
	setIfAbsent(item, "CreationTime", time.Now().UTC().Format(time.RFC3339))
}

func wrapItem(m OpMeta, item map[string]any) map[string]any {
	if m.OutputItemKey != "" {
		return map[string]any{m.OutputItemKey: item}
	}
	return item
}

// --- in-memory store ---

func storeKey(service, resource string) string { return service + "|" + resource }

func put(service, resource, id string, doc map[string]any) {
	mu.Lock()
	defer mu.Unlock()
	k := storeKey(service, resource)
	if store[k] == nil {
		store[k] = map[string]map[string]any{}
	}
	store[k][id] = doc
}

func get(service, resource, id string) (map[string]any, bool) {
	mu.RLock()
	defer mu.RUnlock()
	doc, ok := store[storeKey(service, resource)][id]
	if !ok {
		return nil, false
	}
	// Copy: the caller marshals/mutates this after the lock is released, so it
	// must not alias the stored map (else a concurrent Get/List marshalling it
	// while an Update writes to it triggers a fatal concurrent map read/write).
	return cloneMap(doc), true
}

func del(service, resource, id string) {
	mu.Lock()
	defer mu.Unlock()
	delete(store[storeKey(service, resource)], id)
}

func list(service, resource string) []map[string]any {
	mu.RLock()
	defer mu.RUnlock()
	items := store[storeKey(service, resource)]
	out := make([]map[string]any, 0, len(items))
	for _, doc := range items {
		out = append(out, cloneMap(doc)) // copy: marshalled unlocked, see get()
	}
	return out
}

// --- helpers ---

func okJSON(v any) (*Result, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &Result{Status: 200, Body: b, ContentType: jsonContentType}, nil
}

func notFound(resource string) *Result {
	b, _ := json.Marshal(map[string]string{
		"__type":  "ResourceNotFoundException",
		"message": resource + " not found",
	})
	return &Result{Status: 400, Body: b, ContentType: jsonContentType}
}

func serializationError() *Result {
	b, _ := json.Marshal(map[string]string{
		"__type":  "SerializationException",
		"message": "failed to deserialize request body",
	})
	return &Result{Status: 400, Body: b, ContentType: jsonContentType}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func setIfAbsent(m map[string]any, k string, v any) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "0000000000"
	}
	return hex.EncodeToString(b)
}
