# Architecture

## Multi-CSP Vision

DevCloud's long-term direction is to support multiple Cloud Service Providers (AWS, Azure, GCP, and others) behind a single local runtime. Today's implementation targets **AWS only** — the sections below describe the current AWS-specific architecture.

A phased refactor is planned (see [roadmap.md](roadmap.md)):

- **Phase 1 (current)** — AWS services via Smithy codegen, single-port gateway, AWS SigV4 auth.
- **Phase 2 (preparation)** — Introduce an Intermediate Representation (IR) between API models and codegen. Abstract `ModelSource` so OpenAPI (Azure) and Protocol Buffers / Discovery Documents (GCP) can feed the same pipeline. Extract a per-provider auth adapter interface.
- **Phase 3 (pilot)** — First non-AWS service (candidate: Azure Blob Storage) validates the multi-CSP architecture.
- **Phase 4 (breadth)** — Additional services across CSPs; community-owned providers.

The existing plugin system, protocol detector, and storage abstractions are intentionally CSP-agnostic where possible, to minimize rework when providers are added.

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
  │   └─ LogCollector (dashboard live logs)
  │
  ├─ Route: /devcloud/api/* → Dashboard API
  │
  ▼
Protocol Detector
  │
  ├─ X-Amz-Target header present       → JSON protocol (DynamoDB, SQS JSON)
  ├─ Content-Type: x-www-form-urlencoded + Action= → Query protocol (IAM, STS, SQS Query)
  ├─ SigV4 with Lambda path            → REST-JSON (Lambda)
  └─ Default                           → REST-XML (S3)
  │
  ▼
Auth Validator (SigV4 format check, account ID extraction)
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
    GetMetrics(ctx context.Context) (*ServiceMetrics, error)
}
```

Plugins are registered in the `Registry` at startup. The gateway routes requests to the correct plugin based on protocol detection and service identification.

### Supported Protocols

| Protocol | Services | Request Format | Response Format |
|----------|----------|----------------|-----------------|
| REST-XML | S3 | HTTP path/headers | XML |
| JSON 1.0 | DynamoDB, SQS | JSON body, `X-Amz-Target` header | JSON |
| JSON 1.1 | Lambda | JSON body, REST path | JSON |
| Query | IAM, STS, SQS | Form-encoded body with `Action=` | XML |

SQS supports both Query and JSON protocols. The protocol is auto-detected per request based on Content-Type and headers.

## Smithy Code Generation

DevCloud auto-generates Go code from AWS Smithy JSON models. This enables rapid tracking of AWS API changes with minimal manual work.

### Pipeline

```
smithy-models/*.json  (AWS Smithy model files)
       │
       ▼
  Parser (internal/codegen/parser.go)
  Reads service definitions, operations, input/output shapes
       │
       ▼
  Generator (internal/codegen/generator.go)
  Uses Go templates to produce:
       │
       ├─ types.go          — Request/response structs
       ├─ interface.go       — Service interface (all operations)
       ├─ serializer.go      — Request marshaling
       ├─ deserializer.go    — Response unmarshaling
       ├─ router.go          — Operation routing
       ├─ errors.go          — Service-specific error types
       └─ base_provider.go   — Stub implementation (NotImplementedError)
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
1. Fetch the latest Smithy models from AWS
2. Run codegen
3. Open a PR if any generated code changed

## Event Bus

The event bus (`internal/eventbus/`) provides in-memory pub/sub for internal communication:

- Services publish events (resource created, deleted, etc.)
- Dashboard subscribes to stream real-time updates via WebSocket
- Loose coupling between services and dashboard

## Dashboard

The dashboard (`internal/dashboard/`) provides:

- **REST API** at `/devcloud/api/` — service status, resource listing, request logs
- **WebSocket** at `/devcloud/api/ws` — real-time event streaming
- **Web UI** — Next.js static export served by the Go server

Log collector maintains a circular buffer of the last 1000 API requests for the dashboard log viewer.

## Directory Structure

```
devcloud/
├── cmd/
│   ├── devcloud/           # Server entry point (main.go)
│   └── codegen/            # Smithy code generator CLI (main.go)
├── internal/
│   ├── gateway/            # HTTP server, middleware, protocol detection, routing
│   ├── plugin/             # ServicePlugin interface, Registry
│   ├── codegen/            # Smithy parser, code generators, templates
│   ├── config/             # YAML config loading, env overrides
│   ├── generated/          # Auto-generated code (DO NOT EDIT)
│   │   ├── s3/
│   │   ├── sqs/
│   │   ├── dynamodb/
│   │   ├── lambda/
│   │   ├── iam/
│   │   └── sts/
│   ├── services/           # Service implementations
│   │   ├── s3/             # FileSystem + SQLite
│   │   ├── sqs/            # In-memory queues
│   │   ├── dynamodb/       # BadgerDB
│   │   ├── lambda/         # SQLite + filesystem (stub runtime)
│   │   └── iam/            # SQLite (IAM + STS)
│   ├── dashboard/          # Dashboard REST API + WebSocket
│   ├── eventbus/           # In-memory event pub/sub
│   └── storage/            # Shared storage abstractions
├── web/                    # Next.js dashboard (React, TypeScript, Tailwind)
├── docker/                 # Dockerfile, docker-compose.yml
├── smithy-models/          # AWS Smithy JSON model files
├── tests/compatibility/    # Python/boto3 compatibility tests
├── docs/                   # Documentation
├── Makefile
├── devcloud.yaml           # Default configuration
└── go.mod
```

## Startup Flow

1. Load config from `devcloud.yaml` (or specified path)
2. Initialize structured logger (slog)
3. Create plugin registry
4. Register service factories (S3, SQS, DynamoDB, IAM, Lambda)
5. Initialize services in order: S3 → SQS → DynamoDB → IAM → STS → Lambda
6. IAM store is shared with STS via plugin config options
7. Set up event bus, log collector, dashboard API
8. Create gateway with middleware chain and service router
9. Start HTTP server on configured port
10. Wait for shutdown signal (SIGINT/SIGTERM), graceful shutdown with 15s timeout
