# ServicePlugin API

The stable contract every DevCloud service implements. As of **v1.0** this is a
public API — see [API stability](#api-stability).

The interface and registry live in [`internal/plugin/`](../internal/plugin/).
Plugins are compiled in-tree and register themselves at startup; there is no
dynamic loading. (That is why the package is under `internal/` — it is imported
by DevCloud's own binaries, not by third-party modules.)

## The interface

```go
type ServicePlugin interface {
    ServiceID() string
    ServiceName() string
    Protocol() ProtocolType
    Init(config PluginConfig) error
    Shutdown(ctx context.Context) error
    HandleRequest(ctx context.Context, op string, req *http.Request) (*Response, error)
    ListResources(ctx context.Context) ([]Resource, error)
}
```

| Method | Contract |
|--------|----------|
| `ServiceID()` | Stable lowercase identifier, e.g. `"s3"`. **Must equal the key the plugin is registered under** — the gateway routes by it. Constant; safe before `Init`. |
| `ServiceName()` | Human-readable name for logs and the admin API. Non-empty, constant, safe before `Init`. |
| `Protocol()` | One of the [`ProtocolType`](#protocols) constants. Constant; safe before `Init`. |
| `Init(config)` | Called once at startup before any request. Open stores, read `config.Options`. A non-nil error aborts startup (services in the fixed init order) or skips the service (others). |
| `Shutdown(ctx)` | Called once at graceful shutdown, 15s budget. Flush and close. Idempotent-friendly. |
| `HandleRequest(ctx, op, req)` | Handle one API call. See [operation names](#operation-names) — `op` may be `""` for REST protocols, in which case derive it yourself. Return a `*Response`; return a Go `error` only for unexpected internal failures (the gateway wraps those as `500 InternalError`). **Model AWS errors as a normal `*Response`.** |
| `ListResources(ctx)` | The resources this service currently holds, for the admin API. May be empty. |

These invariants are enforced for every registered service by
[`TestServicePluginConformance`](../cmd/devcloud/conformance_test.go).

## Protocols

`Protocol()` returns the wire protocol the gateway uses to detect and serialize
for the service:

| Constant | Value | Example services |
|----------|-------|------------------|
| `ProtocolRESTXML` | `rest-xml` | S3, Route53, CloudFront |
| `ProtocolRESTJSON` | `rest-json` | Lambda, ACM, API Gateway |
| `ProtocolJSON10` | `json-1.0` | DynamoDB, SQS, Kinesis |
| `ProtocolJSON11` | `json-1.1` | ECS, DMS, and many others |
| `ProtocolQuery` | `query` | IAM, STS, SNS, RDS, EC2 |

## Operation names

The gateway derives `op` in
[`extractOperationName`](../internal/gateway/router.go):

- **JSON protocols** — the suffix of `X-Amz-Target` after the last `.`
  (`DynamoDB_20120810.GetItem` → `GetItem`).
- **Query protocol** — the `Action` parameter.
- **REST protocols** — `""`; the operation is implicit in method + URL, so the
  plugin resolves it itself.

Service routing uses the target prefix or signing name — see
[`serviceFromTarget` and `normalizeServiceID`](../internal/gateway/protocol.go).
Target prefixes may be dotted
(`com.amazonaws.codeconnections.CodeConnections_20231201`); the last dotted
segment is treated as the `ServiceName_Date` token.

## Configuration

```go
type PluginConfig struct {
    DataDir string
    Options map[string]any
}
```

- `DataDir` — the per-service directory from config (`data_dir:`).
- `Options` — cross-cutting values injected by
  [`buildOptions`](../cmd/devcloud/main.go). Keys a plugin may rely on:
  - `server_port` (`int`) — the HTTP port, for building resource URLs such as SQS
    queue URLs. Passed to every service.
  - `iam_store` — set for `sts` so it shares the IAM store. A service needing a
    peer's store receives it here.

## Caller identity

The gateway parses whatever credentials a request carried and attaches the result
to the request context:

```go
if id, ok := auth.FromContext(ctx); ok {
    region := id.Region // what the SDK signed for; "" if unsigned
}
```

`auth.Identity` carries `Provider`, `AccessKeyID`, `Region`, `Service` (the
signing name as presented) and `SessionToken`. **None of it is verified** —
DevCloud accepts any credentials, so treat these as a claim, never as an
authorization decision. Adapters live in [`internal/auth`](../internal/auth/);
`SigV4` is AWS's, and Azure (AAD/SAS) and GCP (OAuth2) join by implementing
`auth.Adapter`.

## Error convention

Return AWS-shaped errors as a `*Response`, using the helpers in
[`internal/shared/response.go`](../internal/shared/response.go):

- JSON protocols → `shared.JSONError(code, message, status)` (`{"__type", "message"}`)
- Query/XML protocols → `shared.QueryXMLError(...)` or the service's XML helper

**Unknown or unimplemented operations must return an error, never a false
success.** The convention is `InvalidAction` with HTTP 400:

```go
default:
    return shared.JSONError("InvalidAction", "unknown action: "+op, http.StatusBadRequest), nil
```

A silent `200 OK {}` is a bug: it makes an SDK believe the call succeeded. To opt
into the [generic CRUD engine](crud-engine.md) instead, return
`plugin.ErrUnhandledOp` from the `default:` case — the gateway falls back to the
engine and emits `InvalidAction` itself if the engine cannot classify the
operation.

## Registering a service

```go
func init() {
    plugin.DefaultRegistry.Register("myservice", func() plugin.ServicePlugin {
        return &MyProvider{}
    })
}
```

Blank-import the package in
[`cmd/devcloud/imports.go`](../cmd/devcloud/imports.go) so its `init()` runs, and
enable it in [`internal/config/default.yaml`](../internal/config/default.yaml) so
it is initialized at startup. Full checklist:
[contributing.md](contributing.md#adding-a-new-aws-service).

The registry also exposes `RegisteredServices()` (all registered IDs) and
`Construct(id)` (build an instance without `Init`, for introspection and tests).

## Providers and CSP neutrality

Phase 2 of the [roadmap](roadmap.md) reviewed this interface for CSP neutrality
and concluded that **`ServicePlugin` needs no change**: every method is about
serving an HTTP API, and none assumes AWS. What is AWS-specific is the vocabulary
around it, and each piece is open rather than closed:

| Surface | Why it is already neutral |
|---|---|
| `ProtocolType` | An open `string` type, not an enum. Another CSP declares its own values. |
| `ServiceID()` | An opaque registry key. A non-AWS service registers under whatever id it wants. |
| `DefaultAccountID` | An AWS concept used only by AWS services. Nothing in the interface refers to it. |

The one thing missing was a way to ask *which CSP a plugin serves*. That is the
optional `ProviderScoped` interface:

```go
type ProviderScoped interface {
    Provider() string
}

func ProviderOf(p ServicePlugin) string // defaults to plugin.DefaultProvider ("aws")
```

It is **not** a `ServicePlugin` method on purpose: adding one would force an edit
to every service in the tree just to state the value it already defaults to, and
would break the [v1.x contract](#api-stability) for no gain. Always read the
provider through `ProviderOf`, never by type-asserting.

Startup uses it to resolve configuration —
`cfg.ProviderService(plugin.ProviderOf(p), id)` — so a plugin lands in its own
provider's config namespace without the config package needing to know which
services exist. See [configuration.md](configuration.md#provider-namespacing).

## API stability

This section is the **in-tree** contract: it constrains contributors writing
service plugins inside this repository. `internal/plugin` cannot be imported from
another Go module, so it is not the promise a *user* of DevCloud depends on —
that is [compatibility-policy.md](compatibility-policy.md).

Starting at **v1.0**, `ServicePlugin`, `PluginConfig`, `Response`, `Resource` and
the `ProtocolType` constants are stable within `v1.x`:

- No method is **removed** from `ServicePlugin`, and no existing **signature**
  changes, in a `v1.x` release.
- Struct fields are only **added**, never removed or repurposed.
- Any breaking change must be called out in the release notes with every in-tree
  plugin updated in the same change. It is not on its own a major-version event:
  nothing outside this module can import `internal/plugin`, so no user depends on
  it. Release versioning is governed by
  [compatibility-policy.md](compatibility-policy.md).

New optional capability is introduced additively — new `Options` keys, new
`ProtocolType` values, optional side interfaces such as `ProviderScoped` — so
existing plugins keep compiling and behaving.
