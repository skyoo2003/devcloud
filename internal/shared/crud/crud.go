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
	xmlContentType  = "application/xml"

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

// HasRoute reports whether a service models a REST operation at this method and
// URI. It answers "is this request one this service could serve?", which is how
// the gateway tells apart services that share a SigV4 signing name and so cannot
// be separated by the credential scope alone.
func HasRoute(service, method, uri string) bool {
	mu.RLock()
	rs := routes[service]
	mu.RUnlock()
	op, _ := httproute.Match(rs, method, uri)
	return op != ""
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
// Each protocol says which operation it means in a different place, and the
// engine now reads all three: the JSON protocols put it in an X-Amz-Target
// header; rest-json and rest-xml bind every operation to a method and URI
// template, which the route table turns back into a name; query puts it in the
// Action field of a form body.
//
// ec2-query is the one left out. It is form-encoded like query but not
// interchangeable with it, and no registered service speaks it except EC2,
// whose provider is hand-written and never reaches the engine.
//
// This is deliberately the same question codegen's engineServable answers. The
// two must agree: a protocol admitted there but refused here registers
// operations nothing can reach, and the fidelity manifest would call them
// auto-crud.
func Servable(protocol string) bool {
	return JSONProtocol(protocol) || protocol == protocolRESTJSON ||
		protocol == protocolRESTXML || protocol == protocolQuery
}

// NeedsBody reports whether the gateway must buffer the request body before
// calling Handle.
//
// It is not the same question as Servable, and rest-xml is where the two come
// apart. The engine can classify a rest-xml request from its route table, but
// S3 speaks rest-xml and S3 bodies are large binary uploads; buffering those
// would turn a streaming path into a memory one, to serve a provider that never
// returns ErrUnhandledOp in the first place. Nothing is lost by it: every
// CRUD-shaped S3 Control operation addresses its resource with a path label or
// a query term, so the engine has what it needs without the body.
//
// query is the mirror image: servable, and unservable without the body, because
// the operation name itself is a field in it.
func NeedsBody(protocol string) bool {
	return JSONProtocol(protocol) || protocol == protocolRESTJSON || protocol == protocolQuery
}

// Handle attempts to serve a call from the service's registered operations. It
// returns ErrUnclassified when the operation is unknown, not CRUD-shaped, or
// carried by a protocol the engine cannot read — never a fabricated success.
func Handle(c Call) (*Result, error) {
	op := c.Op
	var labels httproute.Params
	var form map[string]any
	rest := false

	switch {
	case JSONProtocol(c.Protocol):
		// The operation name arrived in X-Amz-Target; nothing to resolve.
	case c.Protocol == protocolQuery:
		// query names its operation in neither a header nor a path: Action is
		// a field of the form body, which is why it outlived the two REST
		// protocols as a gap. An absent or empty Action is not a request this
		// engine can classify — the other fields describe a resource, not an
		// operation, and picking one from them would be a guess.
		op, form = parseQueryForm(c.Body)
		if op == "" {
			return nil, ErrUnclassified
		}
	case c.Protocol == protocolRESTJSON, c.Protocol == protocolRESTXML:
		rest = true
		mu.RLock()
		rs := routes[c.Service]
		mu.RUnlock()
		// A miss here is any of: unregistered service, an operation codegen
		// refused to classify, or a path this service does not model. All
		// three must decline — see docs/coverage.md on why a registered
		// service must never fabricate a success.
		//
		// It matters most for rest-xml, because S3 speaks it: an S3-shaped
		// path reaching this engine must come back unclassified rather than be
		// answered out of the generic store.
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
	switch {
	case c.Protocol == protocolQuery:
		// Already decoded above. A query body is form-encoded, not JSON, so it
		// must not reach the branch below — parsing it as JSON fails, and
		// reporting that as a SerializationException would decline every query
		// request with an error about a format it never claimed to send.
		maps.Copy(params, form)
	case len(c.Body) > 0:
		var body map[string]any
		if err := json.Unmarshal(c.Body, &body); err != nil {
			// Malformed body: real services reject with SerializationException
			// rather than fabricating a success from empty params.
			return withContentType(serializationError(c.Protocol), c.Protocol), nil
		}
		maps.Copy(params, body)
	}
	for k, v := range labels {
		params[k] = v
	}

	res, err := dispatch(c.Service, c.Protocol, op, m, params)
	return withContentType(res, c.Protocol), err
}

// parseQueryForm reads a Query-protocol form body, returning the operation name
// from Action and the remaining fields as engine parameters.
//
// Action and Version are dropped from the parameters. They describe the
// request, not the resource, and the engine echoes what it stores — leaving
// them in would put <Action>CreateLoadBalancer</Action> inside a result element
// where no SDK expects a member by that name, and would make Version part of
// the stored document forever.
//
// Structured members arrive flattened, as "Listeners.member.1.Protocol". They
// are kept here and dropped at the serializer (see sortedKeys), so that a
// dotted key can still be a resource identifier if some model ever binds one,
// without being emitted as a bogus element.
func parseQueryForm(body []byte) (action string, params map[string]any) {
	// ParseQuery returns what it could parse alongside any error, so a
	// malformed tail cannot discard the terms before it.
	values, _ := url.ParseQuery(string(body))
	params = make(map[string]any, len(values))
	for k, vs := range values {
		if len(vs) == 0 {
			continue
		}
		switch k {
		case "Action":
			action = vs[0]
		case "Version":
		default:
			params[k] = vs[0]
		}
	}
	return action, params
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

// xmlProtocol reports whether a protocol's bodies are XML. It is the
// serialization question, not the routing one: query and rest-xml resolve their
// operation in completely different ways but write the same kind of body.
func xmlProtocol(protocol string) bool {
	return protocol == protocolRESTXML || protocol == protocolQuery
}

// withContentType stamps the protocol-appropriate content type so a json-1.0
// service is not served a 1.1 one, and a rest-json service is not served an
// X-Amz-Target type it never uses. dispatch/okBody default to amz-json 1.1.
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

// dispatch runs the generic CRUD behaviour for one classified operation. It
// takes the protocol and operation name only to serialize the result: the two
// XML dialects need the operation to build their envelope, and the behaviour
// above the serializer is identical for every protocol.
func dispatch(service, protocol, op string, m OpMeta, params map[string]any) (*Result, error) {
	switch m.Verb {
	case "Create":
		id := resourceID(m.Resource, params)
		item := maps.Clone(params)
		stamp(item, service, m.Resource, id)
		put(service, m.Resource, id, item)
		return okBody(protocol, op, wrapItem(m, item))

	case "Get":
		// A Describe*/Get* whose output is a collection (list key, no single-item
		// wrapper) returns the stored list, not a single-resource lookup — a fresh
		// store yields an empty collection (200), not ResourceNotFoundException.
		if m.OutputItemKey == "" && m.OutputListKey != "" {
			return okBody(protocol, op, map[string]any{m.OutputListKey: list(service, m.Resource)})
		}
		id := resourceID(m.Resource, params)
		item, found := get(service, m.Resource, id)
		if !found {
			return notFound(protocol, m.Resource), nil
		}
		return okBody(protocol, op, wrapItem(m, item))

	case "List":
		key := m.OutputListKey
		if key == "" {
			key = m.Resource + "s"
		}
		return okBody(protocol, op, map[string]any{key: list(service, m.Resource)})

	case "Delete":
		id := resourceID(m.Resource, params)
		del(service, m.Resource, id)
		return okBody(protocol, op, map[string]any{})

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
		return okBody(protocol, op, wrapItem(m, item))

	case "Tag", "Untag", "Relate", "Toggle":
		// Relationship / tag / status flips: acknowledge without modelling state.
		return okBody(protocol, op, map[string]any{})

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

// okBody serializes a successful response in the protocol's own encoding. The
// JSON protocols marshal the map directly; the XML dialects go through
// encodeXML, which needs the operation name to build its envelope.
func okBody(protocol, op string, v map[string]any) (*Result, error) {
	if xmlProtocol(protocol) {
		return &Result{Status: 200, Body: encodeXML(protocol, op, v), ContentType: xmlContentType}, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &Result{Status: 200, Body: b, ContentType: jsonContentType}, nil
}

func notFound(protocol, resource string) *Result {
	return errorResult(protocol, "ResourceNotFoundException", resource+" not found")
}

func serializationError(protocol string) *Result {
	return errorResult(protocol, "SerializationException", "failed to deserialize request body")
}

// errorResult renders one of the engine's own errors in the protocol's error
// vocabulary. The XML dialects do not share an envelope: botocore's query
// parser looks for Error *inside* ErrorResponse and returns a code-less
// ClientError given a bare Error, while rest-xml expects the bare one. The
// gateway's writeAWSError makes the same distinction for the errors it writes;
// these are the ones the engine produces after it has already taken the request.
func errorResult(protocol, code, message string) *Result {
	if xmlProtocol(protocol) {
		body := xmlHeader
		if protocol == protocolQuery {
			body += "<ErrorResponse><Error><Type>Sender</Type><Code>" + code +
				"</Code><Message>" + message + "</Message></Error></ErrorResponse>"
		} else {
			body += "<Error><Code>" + code + "</Code><Message>" + message + "</Message></Error>"
		}
		return &Result{Status: 400, Body: []byte(body), ContentType: xmlContentType}
	}
	b, _ := json.Marshal(map[string]string{"__type": code, "message": message})
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
