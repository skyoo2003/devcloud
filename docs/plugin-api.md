# ServicePlugin API

This document specifies the **`ServicePlugin`** interface — the stable contract
every DevCloud service implements. As of **v1.0** this interface is a public API:
see [API stability](#api-stability) for the compatibility guarantee.

The interface and registry live in [`internal/plugin/`](../internal/plugin/).
Plugins are compiled in-tree and register themselves at startup; there is no
dynamic/external plugin loading. (The package lives under `internal/` for that
reason — it is imported by DevCloud's own binaries, not by third-party modules.)

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

### Method contracts

| Method | Contract |
|--------|----------|
| `ServiceID()` | Stable lowercase identifier, e.g. `"s3"`. **Must equal the key the plugin is registered under** (the gateway routes by this key). Constant; safe to call before `Init`. |
| `ServiceName()` | Human-readable name for logs/admin API. Must be non-empty. Constant; safe before `Init`. |
| `Protocol()` | One of the [`ProtocolType`](#protocols) constants. Constant; safe before `Init`. |
| `Init(config)` | Called once at startup before any request. Open stores, read `config.Options`. Return a non-nil error to abort startup (services in the fixed init order) or skip the service (others). |
| `Shutdown(ctx)` | Called once at graceful shutdown (15s budget). Flush/close resources. Idempotent-friendly. |
| `HandleRequest(ctx, op, req)` | Handle one API call. `op` is the operation name pre-extracted by the gateway (see [operation names](#operation-names)); it may be `""` for REST protocols, in which case derive it from method+path or `X-Amz-Target`. Return a `*Response`; return an `error` only for unexpected internal failures (the gateway wraps those as a `500 InternalError`). **Model AWS errors as a normal `*Response`** with the right status and error body, not as a Go `error`. |
| `ListResources(ctx)` | Return the resources this service currently holds (admin API). May be empty. |

These invariants are enforced for every registered service by
[`TestServicePluginConformance`](../cmd/devcloud/conformance_test.go).

## Providers and CSP neutrality

Phase 2 of the [roadmap](roadmap.md) reviewed this interface for CSP neutrality.
The conclusion was that **`ServicePlugin` needs no change**: every method is
about serving an HTTP API, and none of them assumes AWS.

What is AWS-specific is the vocabulary around it, and each piece is open rather
than closed:

| Surface | Why it is already neutral |
|---|---|
| `ProtocolType` | An open `string` type, not an enum. The constants are AWS's wire protocols; another CSP declares its own values. |
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

It is **not** a `ServicePlugin` method on purpose. Adding one would force an
edit to every service in the tree so it could state the value it already
defaults to, and would break the [v1.x contract](#api-stability) below for no
gain. Always read the provider through `ProviderOf`, never by type-asserting.

Startup uses it to resolve configuration: `cmd/devcloud` looks up each service
with `cfg.ProviderService(plugin.ProviderOf(p), id)`, so a plugin lands in its
own provider's config namespace without the config package needing to know which
services exist. See [configuration.md](configuration.md#provider-namespacing).

## Caller identity

The gateway parses whatever credentials a request carried and attaches the
result to the request context. Inside `HandleRequest`:

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

## Protocols

`Protocol()` returns the wire protocol the gateway uses to detect and serialize
for the service:

| Constant | Value | Example services |
|----------|-------|------------------|
| `ProtocolRESTXML` | `rest-xml` | S3, Route53, CloudFront |
| `ProtocolRESTJSON` | `rest-json` | ACM, API Gateway, Lambda REST |
| `ProtocolJSON10` | `json-1.0` | DynamoDB, Kinesis, CodeConnections |
| `ProtocolJSON11` | `json-1.1` | ECS, Lambda, DMS, many others |
| `ProtocolQuery` | `query` | IAM, STS, SQS, SNS, RDS, EC2 |

## Operation names

The gateway derives `op` in
[`extractOperationName`](../internal/gateway/router.go):

- **JSON protocols** — the suffix of the `X-Amz-Target` header after the last
  `.` (e.g. `DynamoDB_20120810.GetItem` → `GetItem`).
- **Query protocol** — the `Action` parameter.
- **REST protocols** — `""`; the operation is implicit in the URL + method, so
  the plugin resolves it itself.

Service routing uses the target prefix / signing name; see
[`serviceFromTarget` and `normalizeServiceID`](../internal/gateway/protocol.go).
Target prefixes may be dotted (e.g.
`com.amazonaws.codeconnections.CodeConnections_20231201`); the last dotted
segment is treated as the `ServiceName_Date` token.

## Configuration

```go
type PluginConfig struct {
    DataDir string
    Options map[string]any
}
```

- `DataDir` — per-service data directory from config (`data_dir:`).
- `Options` — cross-cutting values injected by
  [`buildOptions`](../cmd/devcloud/main.go). Keys a plugin may rely on:
  - `server_port` (`int`) — the HTTP port, for building resource URLs (e.g. SQS
    queue URLs). Passed to every service.
  - `iam_store` — set for `sts` so it shares the IAM store; services needing a
    peer's store receive it here.

## Error convention

Return AWS-shaped errors as a `*Response`, using the shared helpers in
[`internal/shared/response.go`](../internal/shared/response.go):

- JSON protocols → `shared.JSONError(code, message, status)` (`{"__type", "message"}`).
- Query/XML protocols → `shared.QueryXMLError(...)` or the service's XML error helper.

**Unknown / unimplemented operations must return an error, never a false
success.** The convention is `InvalidAction` with HTTP 400:

```go
default:
    return shared.JSONError("InvalidAction", "unknown action: "+op, http.StatusBadRequest), nil
```

A silent `200 OK {}` for an unimplemented op is a bug: it makes an SDK believe
the call succeeded.

## Registering a service

Each service registers a factory in its `init()`:

```go
func init() {
    plugin.DefaultRegistry.Register("myservice", func() plugin.ServicePlugin {
        return &MyProvider{}
    })
}
```

The service package is blank-imported in
[`cmd/devcloud/imports.go`](../cmd/devcloud/imports.go) so its `init()` runs, and
enabled in [`internal/config/default.yaml`](../internal/config/default.yaml) so
it is initialized at startup. See [contributing.md](contributing.md) for the
full "add a service" checklist.

The registry also exposes `RegisteredServices()` (all registered IDs) and
`Construct(id)` (build an instance without `Init`, for introspection/tests).

## API stability

This section is the **in-tree** contract — it constrains contributors writing
service plugins inside this repository. `internal/plugin` cannot be imported
from another Go module, so it is not the promise a *user* of DevCloud depends
on. That is [compatibility-policy.md](compatibility-policy.md), which covers the
config file, environment variables, CLI, admin API and wire behaviour.

Starting at **v1.0**, `ServicePlugin`, `PluginConfig`, `Response`, `Resource`,
and the `ProtocolType` constants are stable within the `v1.x` series:

- No method will be **removed** from `ServicePlugin` and no existing method
  **signature** will change in a `v1.x` release.
- Struct fields will only be **added**, never removed or repurposed.
- Any breaking change to this contract must be called out in the release notes,
  and every in-tree plugin updated in the same change. It is not on its own a
  major-version event: nothing outside this module can import
  `internal/plugin`, so no user can be depending on it. Release versioning is
  governed by [compatibility-policy.md](compatibility-policy.md).

New optional capability is introduced additively (new `Options` keys, new
`ProtocolType` values, optional side interfaces such as `ProviderScoped`) so
existing plugins keep compiling and behaving.
