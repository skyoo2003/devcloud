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
	"maps"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/skyoo2003/devcloud/internal/shared/httproute"
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

	// Method and URI are the operation's REST binding, empty for a service
	// whose protocol has none. They are what lets the engine serve rest-json:
	// that protocol puts the operation in the method and path instead of in an
	// X-Amz-Target header, so without them there is nothing to classify.
	Method string // e.g. "GET"
	URI    string // e.g. "/v1/graphs/{GraphName}"
}

// Call is one request the engine may be asked to serve, in protocol-neutral
// terms. The JSON protocols name the operation in a header, so they set Op; the
// REST protocols put it in the method and path, so they set Method and URI and
// leave Op empty.
type Call struct {
	Service  string
	Protocol string
	Op       string
	Method   string
	URI      string
	Body     []byte
}

// Result is a protocol-agnostic response the caller maps onto its transport.
type Result struct {
	Status      int
	Body        []byte
	ContentType string
}

const (
	jsonContentType = "application/x-amz-json-1.1"
	restJSONType    = "application/json"

	protocolJSON10   = "json-1.0"
	protocolRESTJSON = "rest-json"
)

var (
	mu       sync.RWMutex
	registry = map[string]map[string]OpMeta{}         // service -> op -> meta
	routes   = map[string][]httproute.Route{}         // service -> REST routes, sorted by operation
	store    = map[string]map[string]map[string]any{} // "service|resource" -> id -> doc
)

// Register records a service's operation metadata. The generated crudregistry
// package calls this from a single init().
func Register(service string, ops map[string]OpMeta) {
	mu.Lock()
	defer mu.Unlock()
	registry[service] = ops

	// Build the REST route table alongside the op map. Sorted by operation
	// name because Go map iteration is randomised, and httproute.Match takes
	// the first route that fits within a specificity pass — an unsorted table
	// would make routing differ between runs of the same binary. The order
	// mirrors what codegen emits for the generated per-service routers.
	rs := make([]httproute.Route, 0, len(ops))
	for op, m := range ops {
		if m.URI == "" {
			continue
		}
		rs = append(rs, httproute.Route{Method: m.Method, Pattern: m.URI, Operation: op})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Operation < rs[j].Operation })
	routes[service] = rs
}

// RegisteredOps returns the operation metadata registered for a service.
func RegisteredOps(service string) map[string]OpMeta {
	mu.RLock()
	defer mu.RUnlock()
	return registry[service]
}

// JSONProtocol reports whether a protocol carries the operation name in an
// X-Amz-Target header. It says nothing about whether the engine can serve the
// protocol — use Servable for that.
func JSONProtocol(protocol string) bool {
	return strings.HasPrefix(protocol, "json")
}

// Servable reports whether the engine can serve requests for a protocol.
//
// The JSON protocols carry the operation name in a header. rest-json does not,
// but its model binds every operation to a method and URI template, which the
// route table turns back into an operation name. query and rest-xml have
// neither, so a request for one of them cannot be classified at all and is
// declined rather than guessed at.
func Servable(protocol string) bool {
	return JSONProtocol(protocol) || protocol == protocolRESTJSON
}

// Handle attempts to serve a call from the service's registered operations. It
// returns ErrUnclassified when the operation is unknown, not CRUD-shaped, or
// carried by a protocol the engine cannot read — never a fabricated success.
func Handle(c Call) (*Result, error) {
	op := c.Op
	var labels httproute.Params
	rest := false

	switch {
	case JSONProtocol(c.Protocol):
		// The operation name arrived in X-Amz-Target; nothing to resolve.
	case c.Protocol == protocolRESTJSON:
		rest = true
		mu.RLock()
		rs := routes[c.Service]
		mu.RUnlock()
		// A miss here is any of: unregistered service, an operation codegen
		// refused to classify, or a path this service does not model. All
		// three must decline — see docs/coverage.md on why a registered
		// service must never fabricate a success.
		if op, labels = httproute.Match(rs, c.Method, c.URI); op == "" {
			return nil, ErrUnclassified
		}
	default:
		return nil, ErrUnclassified
	}

	mu.RLock()
	m, ok := registry[c.Service][op]
	mu.RUnlock()
	if !ok || m.Verb == "" {
		return nil, ErrUnclassified
	}

	params := map[string]any{}
	// Three sources, least authoritative first: httpQuery values, then the
	// JSON body, then the path labels — the URI is what addresses the
	// resource, so it wins. A restJson1 model never binds one member to two of
	// these, so real SDK traffic never exercises the precedence; it is here so
	// a hand-rolled request cannot redirect a lookup from the body.
	if rest {
		maps.Copy(params, queryParams(c.URI))
	}
	if len(c.Body) > 0 {
		var body map[string]any
		if err := json.Unmarshal(c.Body, &body); err != nil {
			// Malformed body: real services reject with SerializationException
			// rather than fabricating a success from empty params.
			return withContentType(serializationError(), c.Protocol), nil
		}
		maps.Copy(params, body)
	}
	for k, v := range labels {
		params[k] = v
	}

	res, err := dispatch(c.Service, m, params)
	return withContentType(res, c.Protocol), err
}

// queryParams reads the request URI's query string as engine parameters. A
// repeated key keeps its first value: the engine has no list-valued parameter,
// and dropping the key entirely would lose a resource identifier.
func queryParams(uri string) map[string]any {
	_, query, found := strings.Cut(uri, "?")
	if !found || query == "" {
		return nil
	}
	// ParseQuery returns what it could parse alongside any error, so a
	// malformed tail cannot discard the terms before it.
	values, _ := url.ParseQuery(query)
	out := make(map[string]any, len(values))
	for k, vs := range values {
		if len(vs) > 0 {
			out[k] = vs[0]
		}
	}
	return out
}

// withContentType stamps the protocol-appropriate content type so a json-1.0
// service is not served a 1.1 one, and a rest-json service is not served an
// X-Amz-Target type it never uses. dispatch/okJSON default to amz-json 1.1.
func withContentType(res *Result, protocol string) *Result {
	if res == nil {
		return res
	}
	switch protocol {
	case protocolJSON10:
		res.ContentType = "application/x-amz-json-1.0"
	case protocolRESTJSON:
		res.ContentType = restJSONType
	}
	return res
}

func dispatch(service string, m OpMeta, params map[string]any) (*Result, error) {
	switch m.Verb {
	case "Create":
		id := resourceID(m.Resource, params)
		item := maps.Clone(params)
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
			item = maps.Clone(params)
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
	return maps.Clone(doc), true
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
		out = append(out, maps.Clone(doc)) // copy: marshalled unlocked, see get()
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

func setIfAbsent(m map[string]any, k string, v any) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	// On the near-impossible rand failure, b stays zeroed: still a valid,
	// consistent-length hex string rather than a differently-sized literal.
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
