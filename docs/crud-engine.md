# Generic CRUD Fallback Engine

DevCloud auto-serves standard CRUD-shaped operations that a service's hand-written
provider has not implemented, using a generic engine driven by the Smithy models.
This is what lets the long tail respond to SDK calls without hand-coding each one.

**Fidelity is deliberately "plausible, not faithful."** Responses are store-backed
and echo the caller's input plus synthesized ids and ARNs, so SDKs round-trip
create → get → list → delete. There is **no** validation, cross-resource
integrity, pagination correctness, or business logic. Treat engine-served
operations as scaffolding for local wiring, not as behavioural parity.

Operation counts are published in [coverage.md](coverage.md); which tier any given
operation carries is in the [fidelity manifest](fidelity-manifest.md).

## How it works

| Piece | Where | What it does |
|---|---|---|
| Engine | [`internal/shared/crud`](../internal/shared/crud/crud.go) | In-memory resource store plus verb dispatch (Create/Get/List/Delete/Update/…) |
| Classification | `internal/codegen/gen_crud_meta.go` | Inspects each operation's name and output shape, emits `internal/generated/crudregistry/registry_gen.go` whose `init()` registers every classifiable operation |
| Integration | [`internal/gateway/router.go`](../internal/gateway/router.go) | Calls the engine when a provider returns `plugin.ErrUnhandledOp`; returns `InvalidAction` if the engine cannot classify the operation |

A provider opts in by returning `plugin.ErrUnhandledOp` from its dispatch
`default:` case. Hand-written operations always win — the engine is reached only
on fall-through, so it never shadows a real implementation. **Never a fabricated
success for an unclassifiable op.**

## Which protocols it can serve

The engine has to know which operation a request is for before it can classify it.

| Protocol | Operation name comes from | Served |
|---|---|---|
| `json-1.0`, `json-1.1` | the `X-Amz-Target` header | yes |
| `rest-json` | method + path, matched against the model's URI templates (`internal/shared/httproute`) | yes |
| `rest-xml` | the same — every `restXml` operation binds to a method and URI | yes |
| `query` | the `Action` field of the form body | yes |
| `ec2-query` | — | no |

Every protocol DevCloud registers is readable, so a service that serves nothing
does so because none of its operations is CRUD-shaped, not because of how it
talks. `ec2-query` is the one exception and not a gap in practice: only EC2 speaks
it, and EC2's provider is hand-written and never reaches the engine.

### Where parameters come from

For the two REST protocols, three places — least authoritative first: values the
model binds with `httpQuery`, then the request body, then the path labels. The URI
addresses the resource, so a path label wins. A REST model never binds one member
to two of these, so real SDK traffic never exercises the precedence; it is defined
so a hand-rolled request cannot redirect a lookup.

For `query`, the form body, flat keys only. `Action` and `Version` are dropped —
they describe the request, not the resource, and storing them would echo
`<Action>CreateLoadBalancer</Action>` back inside a result element. A structured
member arrives flattened as `Listeners.member.1.Protocol`; the engine has no
nested shape to put it in, so it is not emitted in the response.

**`rest-xml` request bodies are never read.** The gateway does not buffer them —
S3 speaks `rest-xml` and its bodies are multi-gigabyte uploads that must keep
streaming — so a `rest-xml` operation is served from its path and query alone.
Every CRUD-shaped S3 Control operation addresses its resource that way, so nothing
is lost; an operation carrying its identifier only in a body would get a generated
id.

The engine does **not** read `httpHeader` members, `httpPayload` blobs, or
streaming bodies. An operation whose identifier arrives only in a header is served
with a generated id rather than the caller's — plausible, not faithful, which is
the stated contract.

### Responses

JSON protocols get a JSON body. `query` and `rest-xml` get XML, and they do not
share an envelope: botocore's query parser looks for `<OperationResult>` nested
inside `<OperationResponse>` and, given anything else, returns an empty result
rather than an error, while its `rest-xml` parser maps the root element's children
straight onto the output shape. List entries are wrapped in `<member>`, AWS's
default for both dialects; a model that flattens a list gets the unflattened form,
because the engine has no flattening information. No `xmlns` is emitted — botocore
strips namespaces before matching element names.

### Declining

A request whose method and path match no route in the service's table, or whose
`Action` names an operation the service did not register, is **declined** with
`InvalidAction` and never served from the store. That is what keeps a registered
service from answering for operations it does not model — and it matters most for
`rest-xml`, because `DetectProtocol` routes anything it cannot classify to S3.

## Known limits

- `List*` responses return stored objects; when the real AWS output member is a
  list of *names* rather than structures, an SDK may not populate it.
- No required-parameter validation, so calls succeed with minimal input.
- The store is in-memory and per-process — not persisted across restarts.

To promote an operation from `auto-crud` to `hand-verified`, implement it as an
explicit `case` in the service provider, following existing patterns. See
[fidelity-manifest.md](fidelity-manifest.md#getting-an-operation-promoted).
