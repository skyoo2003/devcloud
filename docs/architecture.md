# Architecture

DevCloud is a single Go binary that serves cloud provider APIs locally. It supports AWS, Azure, and Google Cloud Platform behind a single unified endpoint on port 4747.

## Request Flow

```
Client (AWS SDK, Azure SDK, Google Cloud SDK, Terraform, CDK, CLI)
  │
  ▼
API Gateway (port 4747)
  │
  ├─ Middleware Chain
  │   ├─ ErrorRecovery (panic recovery)
  │   ├─ BodyLimit (request size limiting)
  │   ├─ CORS (cross-origin handling)
  │   ├─ RequestID (request correlation IDs)
  │   ├─ RequestLogger (structured logging)
  │   ├─ Identity (auth.Adapter -> claimed identity on request context)
  │   └─ LogCollector (admin API live logs)
  │
  ├─ Route: /devcloud/api/* -> Admin API
  ├─ Route: /devcloud/identity/* -> Federated Identity Issuer (OIDC / JWKS / STS)
  │
  ▼
Protocol & Provider Detector
  │
  ├─ GCP Detector (/storage/v1/, /v1/projects/*, Google headers)
  │   ├─ /storage/v1/*, /upload/storage/v1/*   -> Google Cloud Storage (gcs)
  │   ├─ /v1/projects/*/secrets*               -> GCP Secret Manager (gcpsecrets)
  │   └─ /v1/projects/*/topics*, subscriptions -> GCP Cloud Pub/Sub (gcppubsub)
  │
  ├─ Azure Detector (x-ms-* headers, SharedKey auth, /devstoreaccount1, /secrets, /kv)
  │   ├─ /devstoreaccount1, blob headers       -> Azure Blob Storage (blob)
  │   ├─ /devstoreaccount1/queue, comp=messages-> Azure Queue Storage (azurequeue)
  │   ├─ /devstoreaccount1/Tables, OData JSON  -> Azure Table Storage (azuretable)
  │   ├─ /dbs, x-ms-documentdb-* headers       -> Azure Cosmos DB (cosmosdb)
  │   ├─ /secrets, /keys, vault.azure.net      -> Azure Key Vault (azurekeyvault)
  │   └─ /kv, azconfig.io                      -> Azure App Configuration (azureappconfig)
  │
  └─ AWS Detector
      ├─ X-Amz-Target header present           -> JSON protocol (DynamoDB, ECS, Kinesis)
      ├─ x-www-form-urlencoded + Action=       -> Query protocol (IAM, STS, SNS, RDS, EC2)
      ├─ SigV4 credential scope                -> REST-JSON (Lambda, ACM, API Gateway)
      └─ Default                               -> REST-XML (S3)
  │
  ▼
Service Router -> Plugin Registry -> ServicePlugin.HandleRequest()
  │
  ├─ returns plugin.ErrUnhandledOp -> generic CRUD engine (docs/crud-engine.md)
  │
  ▼
Storage Backend & Workload Engine -> Response Serializer -> HTTP Response
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

Plugins register themselves in the `Registry` at startup. The gateway routes requests by protocol detection and service identification. The per-method contract, configuration keys, error conventions, and stability guarantees are documented in [plugin-api.md](plugin-api.md).

### Protocols

| Protocol | Request format | Response | Example services |
|----------|----------------|----------|------------------|
| REST-XML | HTTP path/headers | XML | S3, Route53, CloudFront, Azure Blob, Azure Queue |
| REST-JSON | JSON body, REST path | JSON | Lambda, GCS, GCP Secrets, GCP Pub/Sub, Azure Cosmos, Key Vault, App Config |
| JSON 1.0 | JSON body, `X-Amz-Target` | JSON | DynamoDB, SQS, Kinesis |
| JSON 1.1 | JSON body, `X-Amz-Target` | JSON | ECS, Batch, CloudWatch Logs, Step Functions |
| Query | Form-encoded body with `Action=` | XML | IAM, STS, SNS, RDS, EC2 |
| EC2 Query | Form-encoded body with `Action=` | XML (`<requestId>`) | EC2 |
| RPC-v2 CBOR | Binary CBOR payload | CBOR | Partner Central Revenue Measurement |

## Universal Workload Runtime

Serverless function invocations (such as AWS Lambda) execute through the workload abstraction in `internal/shared/workload`:

- **ContainerRunner (OCI)**: Discovers and invokes standard OCI container runtimes (`podman`, `docker`, or `nerdctl`). The runtime engine can be set explicitly via `CONTAINER_RUNTIME`.
- **WASMRunner (WebAssembly)**: Executes `.wasm` binaries directly using standalone or embedded WebAssembly runtimes (`wasmtime`, `wasmer`, `wazero`), enabling zero-daemon serverless execution.
- **ProcessRunner (Host Process Sandbox)**: Executes handlers directly in isolated temporary directories for lightweight local environments without container engines.

## Authentication

DevCloud inspects incoming credentials without rejecting calls for signature mismatches:

- **AWS**: `internal/auth/SigV4` extracts access keys, regions, signing names, and session tokens from the `Authorization` header or presigned URLs.
- **Azure**: `internal/auth/AzureSharedKey` parses account names, SharedKey signatures, and Bearer tokens for Azure services.
- **GCP**: `internal/auth/GCPAuth` accepts Google OAuth2 Bearer tokens (`Bearer ya29...`) and mock service account headers.
- **Federated Identity**: `internal/services/iam` provides a built-in OIDC discovery issuer, JWKS endpoint, and STS token exchange (`AssumeRoleWithWebIdentity`) for cross-cloud credential minting.

Signature validation is omitted so local test suites can run with placeholder credentials without failures.

## Code Generation

Go code is generated from API models to keep pace with upstream cloud changes:

```
api/smithy/*.json (AWS Smithy)  api/models/*.json (OpenAPI)
       │                              │
       └──────────────┬───────────────┘
                      ▼
        ModelSource (internal/codegen/source.go)
                      │
                      ▼
        IR (internal/codegen/ir)
        Provider-neutral representation: service ID, protocol, operations, shapes.
                      │
                      ▼
        Generator (internal/codegen/generator.go)
             ├─ types.go         (request/response structs)
             ├─ router.go        (method + URI routing)
             ├─ errors.go        (service error catalogues)
             └─ base_provider.go (stub interfaces)
                      │
                      ▼
        internal/generated/{service}/ (DO NOT EDIT)
```

Generated code is updated with:

```bash
make codegen      # all registered services
make codegen-s3   # single service during template development
```

## Admin API

`internal/admin/` serves an opt-in REST API at `/devcloud/api/` for service health, resource inventories, fidelity tiers, unrouted calls, and recent request logs. It is disabled by default (`admin.enabled: false`).

## Startup Flow

1. Load configuration from `devcloud.yaml` (or `--config`), applying `DEVCLOUD_PORT`, `DEVCLOUD_SERVICES`, and `DEVCLOUD_DATA_DIR`.
2. Initialize structured logging (slog) and the plugin registry.
3. Register service factories and initialize services in dependency order (IAM precedes STS; STS receives the IAM store).
4. Set up the event bus, log collector, and admin API.
5. Initialize the gateway with middleware chains and the multi-cloud service router.
6. On SIGINT or SIGTERM, shut down gracefully within a 15-second budget.

## Directory Structure

```
devcloud/
├── cmd/
│   └── devcloud/           # Server entry point and imports
├── tools/
│   └── codegen/            # Smithy and OpenAPI code generator CLI
├── internal/
│   ├── gateway/            # Multi-cloud HTTP router, protocol detection, middleware
│   ├── plugin/             # ServicePlugin interfaces and Registry
│   ├── auth/               # Provider credential adapters (AWS SigV4, Azure, GCP)
│   ├── codegen/            # Model sources, generators, and templates
│   ├── config/             # YAML loading, provider namespaces, environment overrides
│   ├── generated/          # Auto-generated code (DO NOT EDIT)
│   ├── services/           # Service implementations (AWS, Azure, GCP, Identity)
│   ├── shared/             # CRUD engine, HTTP router, and workload execution engine
│   │   └── workload/       # Pluggable OCI, WASM, and Process workload drivers
│   ├── admin/              # Admin REST API
│   └── storage/            # Shared storage backends
├── api/
│   ├── smithy/             # AWS Smithy JSON models
│   └── models/             # OpenAPI models for Azure and GCP services
├── build/package/          # Packaging and container builds
├── test/compatibility/     # Automated Python SDK and CLI compatibility tests
└── docs/                   # Documentation
```

## Multi-CSP Architecture Seams

The runtime supports multiple cloud providers behind a single engine:

| Seam | Package | How a provider joins |
|---|---|---|
| Model to code | `internal/codegen/ir`, `source.go` | Implement `ModelSource`, register in `DefaultSources` |
| Configuration | `internal/config` | Namespace under `providers.<csp>.services.*` |
| Service contract | `internal/plugin` | Implement `ServicePlugin` and optional `ProviderScoped` |
| Credentials | `internal/auth` | Implement `Adapter` and register in credential adapters |
| Routing | `internal/gateway/protocol.go` | Register protocol and prefix rules on port 4747 |
| Workload | `internal/shared/workload` | Select OCI container, WebAssembly, or process driver |
