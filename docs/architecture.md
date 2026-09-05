# Architecture

## Multi-CSP Vision

DevCloud's long-term direction is to support multiple Cloud Service Providers (AWS, Azure, GCP, and others) behind a single local runtime. Today's implementation targets **AWS only** — the sections below describe the current AWS-specific architecture.

A phased refactor is planned (see [roadmap.md](roadmap.md)):

- **Phase 1 (complete, shipped as v1.0)** — AWS services via Smithy codegen, single-port gateway.
- **Phase 2 (complete)** — Intermediate Representation (IR) between API models and codegen; `ModelSource` so OpenAPI (Azure) and Protocol Buffers / Discovery Documents (GCP) can feed the same pipeline; provider namespacing in config; per-provider auth adapters.
- **Phase 3 (pilot)** — First non-AWS service (candidate: Azure Blob Storage) validates the multi-CSP architecture.
- **Phase 4 (breadth)** — Additional services across CSPs; community-owned providers.

After Phase 2, the four places a second CSP would have touched are each an
addition rather than an edit:

| Seam | Package | How a provider joins |
|---|---|---|
| Model → code | [`internal/codegen/ir`](../internal/codegen/ir/ir.go), [`source.go`](../internal/codegen/source.go) | Implement `ModelSource`, append it to `DefaultSources` |
| Configuration | [`internal/config`](../internal/config/config.go) | `providers.<name>.services.*`; add the name to `knownProviders` |
| Service contract | [`internal/plugin`](../internal/plugin/plugin.go) | Implement the optional `ProviderScoped` on the plugin |
| Credentials | [`internal/auth`](../internal/auth/auth.go) | Implement `Adapter`, append it to `Adapters` |

The plugin system, protocol detector, and storage abstractions stay CSP-agnostic
by convention rather than by enforcement — `ProtocolType` is an open string type,
not an enum, for exactly this reason.

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
  ├─ X-Amz-Target header present       → JSON protocol (DynamoDB, SQS JSON)
  ├─ Content-Type: x-www-form-urlencoded + Action= → Query protocol (IAM, STS, SQS Query)
  ├─ SigV4 credential scope            → REST-JSON (Lambda, and the signing name decides the service)
  └─ Default                           → REST-XML (S3)
  │
  ▼
Service Router → Plugin Registry → ServicePlugin.HandleRequest()
  │
  ▼
Service Implementation (S3, SQS, DynamoDB, Lambda, IAM, STS)
  │
  ▼
Storage Backend → Response Serializer → HTTP Response
```

## Plugin System

All services implement the `ServicePlugin` interface:

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

Plugins are registered in the `Registry` at startup. The gateway routes requests to the correct plugin based on protocol detection and service identification.

The full contract for each method, the configuration keys, the error convention, and the v1.x stability guarantee are documented in [plugin-api.md](plugin-api.md).

### Supported Protocols

| Protocol | Services | Request Format | Response Format |
|----------|----------|----------------|-----------------|
| REST-XML | S3 | HTTP path/headers | XML |
| JSON 1.0 | DynamoDB, SQS | JSON body, `X-Amz-Target` header | JSON |
| JSON 1.1 | Lambda | JSON body, REST path | JSON |
| Query | IAM, STS, SQS | Form-encoded body with `Action=` | XML |

SQS supports both Query and JSON protocols. The protocol is auto-detected per request based on Content-Type and headers.

## Authentication

DevCloud reads credentials and never verifies them. `internal/auth` holds one
`Adapter` per provider — `SigV4` for AWS today — which parses the credential
scope off the `Authorization` header or a presigned URL's query string and
reports the access key, region, signing name and session token the caller
claimed. `IdentityMiddleware` puts the result on the request context, where any
plugin can read it with `auth.FromContext`, and the protocol detector uses the
same parse to route by signing name so routing and identity cannot disagree.

Signature validation is deliberately absent: a local server that rejected calls
whose signature did not match would break every SDK pointed at it with
placeholder credentials. See
[compatibility-policy.md](compatibility-policy.md#not-guaranteed).

## Code Generation

DevCloud auto-generates Go code from API models. This enables rapid tracking of
API changes with minimal manual work. AWS Smithy is the only format read today;
the pipeline is built so a second one is an added file rather than a rewrite.

### Pipeline

```
smithy-models/*.json  (AWS Smithy model files)
       │
       ▼
  ModelSource (internal/codegen/source.go)
  Each source detects its own format and parses it. SmithySource is
  the first; OpenAPI and Protobuf join by implementing the interface
  and appending to DefaultSources. cmd/codegen names no format.
       │
       ▼
  IR — *ir.Model (internal/codegen/ir)
  Provider-neutral: service id, protocol, operations, shapes.
  Everything downstream reads only this.
       │
       ▼
  Generator (internal/codegen/generator.go)
  Uses Go templates to produce:
       │
       ├─ types.go          — Request/response structs
       ├─ router.go          — Method+URI → operation routing (REST services)
       ├─ errors.go          — Service-specific error types
       └─ base_provider.go   — Stub implementation (NotImplementedError)

  There is no generated serializer: providers receive the raw
  *http.Request and parse it themselves (map[string]any for JSON
  protocols), so types.go/base_provider.go have no wire glue and only
  router.go is consumed today — by the REST services that need
  MatchOperation. See docs/crud-engine.md for how the long tail of
  unimplemented operations is actually served.
       │
       ▼
  internal/generated/{service}/  (DO NOT EDIT)
```

### Running Codegen

```bash
# Generate code for all services
make codegen

# Generate for a specific service
make codegen-s3
```

### Weekly Auto-Sync

A GitHub Actions workflow runs weekly to:
1. Re-download the Smithy models (`scripts/download-smithy-models.sh --refresh`)
2. Run codegen
3. Open a PR if the models or the generated code changed

The models under `smithy-models/` are committed on purpose: the download URL
tracks `aws-sdk-go-v2` *main*, so they are the pin that makes `make codegen`
reproducible and offline. Only the weekly job passes `--refresh`, which is what
makes an upstream API change show up as a reviewable model diff next to the
regenerated code.

## Admin API

The admin API (`internal/admin/`) provides a **REST API** at `/devcloud/api/` —
service status, resource listing, and recent request logs.

It is disabled by default (`admin.enabled: false`). The web dashboard UI is a
separate project (its own repository) that consumes this API; the Go server
serves no UI itself.

Log collector maintains a circular buffer of the last 1000 API requests for the admin log endpoint.

## Directory Structure

```
devcloud/
├── cmd/
│   ├── devcloud/           # Server entry point (main.go)
│   └── codegen/            # Smithy code generator CLI (main.go)
├── internal/
│   ├── gateway/            # HTTP server, middleware, protocol detection, routing
│   ├── plugin/             # ServicePlugin interface, Registry, ProviderScoped
│   ├── auth/               # Per-provider credential adapters (SigV4 today)
│   ├── codegen/            # ModelSource implementations, generators, templates
│   │   └── ir/             # Provider-neutral intermediate representation
│   ├── config/             # YAML config loading, provider namespacing, env overrides
│   ├── generated/          # Auto-generated code (DO NOT EDIT; run `make stats` for count)
│   ├── services/           # Service implementations (run `make stats` for count)
│   ├── admin/              # Admin REST API
│   └── storage/            # Shared storage abstractions
├── docker/                 # Dockerfile, docker-compose.yml
├── smithy-models/          # AWS Smithy JSON model files
├── tests/compatibility/    # Python/boto3 compatibility tests
├── docs/                   # Documentation
├── Makefile
└── go.mod
```

## Startup Flow

1. Load config from `devcloud.yaml` (or specified path), applying environment variable overrides (`DEVCLOUD_PORT`, `DEVCLOUD_SERVICES`, `DEVCLOUD_DATA_DIR`)
2. Initialize structured logger (slog)
3. Create plugin registry
4. Register service factories (run `make stats` for count)
5. Initialize services in dependency order (IAM before STS, etc.)
6. IAM store is shared with STS via plugin config options
7. Set up event bus, log collector, admin API
8. Create gateway with middleware chain and service router
9. Start HTTP server on configured port
10. Wait for shutdown signal (SIGINT/SIGTERM), graceful shutdown with 15s timeout
