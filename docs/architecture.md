# Architecture

DevCloud is a single Go binary that serves cloud provider APIs locally. It targets
**AWS today**; the seams that let a second CSP join are described at the bottom.

## Request Flow

```
Client (boto3 / AWS CLI / Terraform / CDK)
  │
  ▼
API Gateway (port 4747)
  │
  ├─ Middleware Chain
  │   ├─ ErrorRecovery (panic recovery)
  │   ├─ BodyLimit (request size limiting)
  │   ├─ CORS (cross-origin handling)
  │   ├─ RequestID (X-Amz-Request-Id)
  │   ├─ RequestLogger (structured logging)
  │   ├─ Identity (auth.Adapter → claimed identity on the request context)
  │   └─ LogCollector (admin API live logs)
  │
  ├─ Route: /devcloud/api/* → Admin API
  │
  ▼
Protocol Detector
  │
  ├─ X-Amz-Target header present      → JSON protocol
  ├─ x-www-form-urlencoded + Action=  → Query protocol
  ├─ SigV4 credential scope           → REST-JSON (signing name picks the service)
  └─ Default                          → REST-XML (S3)
  │
  ▼
Service Router → Plugin Registry → ServicePlugin.HandleRequest()
  │
  ├─ returns plugin.ErrUnhandledOp → generic CRUD engine (docs/crud-engine.md)
  │
  ▼
Storage Backend → Response Serializer → HTTP Response
```

## Plugin System

Every service implements `ServicePlugin`:

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

Plugins register themselves in the `Registry` at startup; the gateway routes to
one by protocol detection and service identification. The per-method contract,
config keys, error convention and v1.x stability guarantee are in
[plugin-api.md](plugin-api.md).

### Protocols

| Protocol | Request format | Response | Example services |
|----------|----------------|----------|------------------|
| REST-XML | HTTP path/headers | XML | S3, Route53, CloudFront |
| REST-JSON | JSON body, REST path | JSON | Lambda, ACM, API Gateway |
| JSON 1.0 | JSON body, `X-Amz-Target` | JSON | DynamoDB, SQS, Kinesis |
| JSON 1.1 | JSON body, `X-Amz-Target` | JSON | ECS, Batch, CW Logs, SFN |
| Query | Form-encoded body with `Action=` | XML | IAM, STS, SNS, RDS, EC2 |

SQS speaks both Query and JSON; the protocol is detected per request.

## Authentication

DevCloud reads credentials and never verifies them. `internal/auth` holds one
`Adapter` per provider — `SigV4` for AWS — which parses the credential scope off
the `Authorization` header or a presigned URL and reports the access key, region,
signing name and session token the caller *claimed*. `IdentityMiddleware` puts the
result on the request context for `auth.FromContext`, and the protocol detector
uses the same parse to route by signing name, so routing and identity cannot
disagree.

Signature validation is deliberately absent: a local server that rejected calls
whose signature did not match would break every SDK pointed at it with
placeholder credentials. See
[compatibility-policy.md](compatibility-policy.md#not-guaranteed).

## Code Generation

Go code is generated from API models, so tracking upstream API changes costs
little manual work. AWS Smithy is the only format read today; a second one is an
added file, not a rewrite.

```
smithy-models/*.json  (AWS Smithy model files)
       │
       ▼
  ModelSource (internal/codegen/source.go)
  Each source detects and parses its own format. SmithySource is the
  first; OpenAPI and Protobuf join by implementing the interface and
  appending to DefaultSources. cmd/codegen names no format.
       │
       ▼
  IR — *ir.Model (internal/codegen/ir)
  Provider-neutral: service id, protocol, operations, shapes.
  Everything downstream reads only this.
       │
       ▼
  Generator (internal/codegen/generator.go) — Go templates produce:
       ├─ types.go          — request/response structs
       ├─ router.go         — method+URI → operation routing (REST services)
       ├─ errors.go         — service-specific error types
       └─ base_provider.go  — stub implementation (NotImplementedError)
       │
       ▼
  internal/generated/{service}/  (DO NOT EDIT)
```

There is no generated serializer: providers receive the raw `*http.Request` and
parse it themselves, so only `router.go` is consumed today — by the REST services
that need `MatchOperation`.

```bash
make codegen      # all services
make codegen-s3   # one service — fast loop while editing templates
```

The models under `smithy-models/` are committed on purpose. The download URL
tracks `aws-sdk-go-v2` *main*, so the vendored copies are the pin that makes
`make codegen` reproducible and offline. A [weekly workflow](../.github/workflows/smithy-sync.yml)
passes `--refresh`, regenerates, and opens a PR — which is what makes an upstream
API change show up as a reviewable model diff next to the regenerated code. How to
review one: [contributing.md](contributing.md#reviewing-the-weekly-model-sync).

## Admin API

`internal/admin/` serves a REST API at `/devcloud/api/` — service status, resource
listing, fidelity tiers, unrouted calls, and recent request logs (a circular
buffer of the last 1000). Disabled by default (`admin.enabled: false`). The web
dashboard UI that consumes it is a separate repository; this server serves no UI.

## Startup Flow

1. Load config from `devcloud.yaml` (or `--config`), applying `DEVCLOUD_PORT`, `DEVCLOUD_SERVICES`, `DEVCLOUD_DATA_DIR`
2. Initialize the structured logger (slog) and the plugin registry
3. Register service factories, then initialize services in dependency order (IAM before STS — STS receives the IAM store via plugin config options)
4. Set up the event bus, log collector and admin API
5. Build the gateway with its middleware chain and service router, and listen
6. On SIGINT/SIGTERM, shut down gracefully with a 15s budget

## Directory Structure

```
devcloud/
├── cmd/
│   ├── devcloud/           # Server entry point
│   └── codegen/            # Smithy code generator CLI
├── internal/
│   ├── gateway/            # HTTP server, middleware, protocol detection, routing
│   ├── plugin/             # ServicePlugin interface, Registry, ProviderScoped
│   ├── auth/               # Per-provider credential adapters (SigV4 today)
│   ├── codegen/            # ModelSource implementations, generators, templates
│   │   └── ir/             # Provider-neutral intermediate representation
│   ├── config/             # YAML loading, provider namespacing, env overrides
│   ├── generated/          # Auto-generated code (DO NOT EDIT)
│   ├── services/           # Service implementations
│   ├── shared/             # CRUD engine, HTTP routing, response helpers
│   ├── admin/              # Admin REST API
│   └── storage/            # Shared storage abstractions
├── docker/                 # Dockerfile, docker-compose.yml
├── smithy-models/          # AWS Smithy JSON model files
├── tests/compatibility/    # Python/boto3 compatibility tests
└── docs/
```

## Multi-CSP seams

The long-term direction is to serve multiple CSPs behind one local runtime. Phase
2 of the [roadmap](roadmap.md) made each place a second provider would have
touched an *addition* rather than an edit:

| Seam | Package | How a provider joins |
|---|---|---|
| Model → code | [`internal/codegen/ir`](../internal/codegen/ir/ir.go), [`source.go`](../internal/codegen/source.go) | Implement `ModelSource`, append it to `DefaultSources` |
| Configuration | [`internal/config`](../internal/config/config.go) | `providers.<name>.services.*`; add the name to `knownProviders` |
| Service contract | [`internal/plugin`](../internal/plugin/plugin.go) | Implement the optional `ProviderScoped` on the plugin |
| Credentials | [`internal/auth`](../internal/auth/auth.go) | Implement `Adapter`, append it to `Adapters` |

The plugin system, protocol detector and storage abstractions stay CSP-agnostic
by convention rather than enforcement — `ProtocolType` is an open string type,
not an enum, for exactly this reason.
