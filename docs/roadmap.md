# Roadmap

DevCloud serves as a local development companion for cloud-native applications across major cloud providers: an on-ramp to iterate without cloud bills and deploy to production cloud providers with confidence.

## Guiding Principles

1. **Local-first, cost-free**: zero cloud charges for inner-loop development.
2. **On-ramp, not replacement**: DevCloud helps teams land cleanly on target cloud providers, not avoid them.
3. **API-compatible, not behavior-perfect**: SDK and CLI compatibility prioritized over obscure provider-internal edge cases.
4. **Extensible architecture**: clean plugin architecture and code generation pipelines for broad community contributions.
5. **Trademark-respectful**: see [TRADEMARKS.md](../TRADEMARKS.md).

## Phase 1: AWS Depth and Stabilization (Complete, shipped as v1.0)

Matured the core AWS surface into a stable v1.0 release:

- [x] AWS services scaffolded from official Smithy models via in-tree codegen.
- [x] Deep hand-written coverage on core and integration services.
- [x] Cross-service integrations: CloudFormation, DynamoDB Streams to Lambda, SQS to Lambda, S3 to Lambda, EventBridge, SNS to SQS.
- [x] Automated compatibility test suites validated in CI.
- [x] Clean error handling: unimplemented operations decline with authentic cloud error formats.
- [x] Stable `ServicePlugin` API ([plugin-api.md](plugin-api.md)).
- [x] Generic [CRUD fallback engine](crud-engine.md) serving the long tail with plausible, store-backed responses.

## Phase 2: Architectural Preparation (Complete, v1.x)

Refactored internally so adding new cloud providers is an addition rather than a rewrite:

- [x] Intermediate Representation (IR) decoupling models from generators ([`internal/codegen/ir`](../internal/codegen/ir/ir.go)).
- [x] Extensible `ModelSource` interface ([`internal/codegen/source.go`](../internal/codegen/source.go)).
- [x] Namespaced provider configuration: `providers.<name>.services.*`.
- [x] Provider-scoped plugin contracts via `ProviderScoped` interface.
- [x] Pluggable authentication adapters ([`internal/auth`](../internal/auth/auth.go)).

## Phase 3: First Non-AWS Service (Complete, v2.0 groundwork)

Validated multi-cloud architecture with an initial Azure pilot:

- [x] Azure Blob Storage (`blob`) pilot implemented with container and block blob lifecycle.
- [x] OpenAPI to IR to codegen pipeline established.
- [x] Azure Shared Key credential adapter and emulator signature support.
- [x] End-to-end integration tests with official Azure SDK for Python.

## Phase 4: Multi-CSP Breadth and Workload Runtime (Complete, v2.1)

Expanded multi-cloud services, unified routing, and universal execution:

- [x] **Unified Single Port (4747) Routing**: Eliminated separate ports (`4566`, `4577`, `4588`) by routing AWS, Azure, and GCP transparently through port 4747.
- [x] **Azure Core Expansion**: Added Queue Storage (`azurequeue`), Table Storage (`azuretable`), Cosmos DB (`cosmosdb`), Key Vault (`azurekeyvault`), and App Configuration (`azureappconfig`).
- [x] **Google Cloud Platform Services**: Added Cloud Storage (`gcs`), Secret Manager (`gcpsecrets`), and Cloud Pub/Sub (`gcppubsub`) with complete lifecycle operations.
- [x] **Universal Workload Runtime**: Implemented `internal/shared/workload` supporting OCI container engines (Docker, Podman, nerdctl), WebAssembly, and local process sandboxing (Zero-Daemon).
- [x] **Federated Identity Playground**: Built-in OIDC discovery issuer, JWKS endpoint, and cross-cloud STS token exchange.
- [x] **100% Client Compatibility Verification**: Automated smoke tests across all registered Azure and GCP services on port 4747.

## Out of Scope

- Production multi-tenant hosting or high-availability clustering
- Real billing, metering, or cloud quota enforcement
- Exact replication of proprietary cloud backend internals
- Redistribution of provider-owned branding assets, logos, or documentation

## Feedback and Contributions

- **Feature or service requests**: open an issue or pull request on GitHub.
- **Contributions**: see [contributing.md](contributing.md).

## Version Mapping

| Version | Focus |
|---------|-------|
| 0.x | Initial AWS services, exploratory API |
| 1.x | AWS depth, stable plugin API, multi-CSP groundwork |
| 2.x (Current) | Multi-cloud architecture (AWS, Azure, GCP), universal workload runtime, unified port 4747 |
| 3.x+ | Extended community providers, distributed orchestration |
