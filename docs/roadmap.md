# Roadmap

DevCloud's long-term vision is to be a **local development companion for cloud-native apps across every major Cloud Service Provider (CSP)** — not a production replacement, but an on-ramp that lets developers iterate fast without cloud bills and deploy to their target CSP with confidence.

We pursue this vision through a **phased rollout** to manage scope, architectural complexity, and community expectations.

## Guiding Principles

1. **Local-first, cost-free** — developers should not incur cloud charges for inner-loop development.
2. **On-ramp, not replacement** — DevCloud helps you *land on* a CSP, not avoid it.
3. **API-compatible, not behavior-perfect** — prioritize SDK compatibility (boto3, azure-sdk, google-cloud-python) over edge-case parity.
4. **Community-owned** — scope is too large for a single maintainer; plugin architecture and contributor experience are first-class concerns.
5. **Trademark-respectful** — see [TRADEMARKS.md](../TRADEMARKS.md).

## Phases

### Phase 1 — AWS Depth & Stabilization (Complete, shipped as v1.0)

**Goal**: mature the already-broad AWS surface into a stable, well-tested v1.0.

- [x] 100+ AWS services scaffolded from official Smithy models via in-tree codegen (run `make stats` for current counts)
- [x] Deep hand-written coverage on core, integration, and major extended services (see [services-matrix.md](services-matrix.md) for per-tier depth)
- [x] Cross-service integration (CloudFormation, DynamoDB Streams → Lambda, SQS → Lambda, S3 → Lambda, EventBridge, SNS → SQS)
- [x] boto3 compatibility suite (`make test-compat`); core services — S3, SQS, DynamoDB, Lambda, IAM, STS, SNS, CloudWatch, KMS, Secrets Manager, EventBridge, CloudFormation — pass 100%
- [x] boto3 compatibility suite green in CI (`make test-compat`) — a failing test fails the build
- [x] Unimplemented operations return a consistent AWS error (`InvalidAction`, HTTP 400) instead of a false `200` success; dead scaffold code removed
- [x] boto3 compatibility coverage added for previously-untested services (CodeConnections, DMS, Verified Permissions)
- [x] Stable `ServicePlugin` API finalized and documented ([plugin-api.md](plugin-api.md)), enforced by a conformance test over every registered service
- [x] Generic CRUD fallback engine ([crud-engine.md](crud-engine.md)) auto-serves ~2,200 CRUD-shaped operations across all 46 JSON-protocol services with plausible, store-backed responses; every registered JSON service is wired. **Follow-up**: promote high-value auto-crud ops to hand-verified fidelity.
- [x] v1.0 release — see [compatibility-policy.md](compatibility-policy.md) for what 1.x guarantees

### Phase 2 — Architectural Preparation (Current, v1.x)

**Goal**: internal refactor so adding a new CSP doesn't require forking the project.

- [ ] Introduce Intermediate Representation (IR) between API models and codegen
- [ ] Refactor `internal/codegen/parser.go` behind a `ModelSource` interface (Smithy being the first implementation; OpenAPI/Protobuf to follow)
- [ ] Provider namespacing in config (`providers.aws.*`, forward-compatible with `providers.azure.*`)
- [ ] Plugin interface review — ensure `ServicePlugin` is CSP-agnostic
- [ ] Per-provider auth adapter interface (SigV4, AAD/SAS, OAuth2)

### Phase 3 — First Non-AWS Service (v2.0, exploratory)

**Goal**: validate the multi-CSP architecture with a single, well-scoped pilot.

- [ ] Pick one Azure service as pilot (candidate: **Azure Blob Storage** — closest to S3 semantically)
- [ ] OpenAPI → IR → codegen proof of concept
- [ ] Azure authentication adapter (Shared Key for starters)
- [ ] Compatibility tests against `azure-sdk-for-python`
- [ ] Documentation pattern for multi-CSP service docs

### Phase 4 — Breadth Expansion (v2.x+)

**Goal**: community-driven growth across CSPs.

- [ ] Additional Azure services (Queue Storage, Table Storage, Cosmos DB)
- [ ] Google Cloud pilot (candidate: **Google Cloud Storage**)
- [ ] Other providers as community interest justifies (OCI, Alibaba, Tencent)
- [ ] Federated identity playground (simulate cross-CSP IAM)

## Out of Scope

- Production hosting or high-availability guarantees
- Billing/quota simulation matching real CSP pricing
- Exact replication of CSP-internal behavior (eventual consistency timing, rate limits, etc.)
- Redistribution of CSP-owned branding assets, logos, or documentation

## How to Influence the Roadmap

- Open a [Feature Request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml) describing the service or capability you need
- Upvote existing requests with reactions — we look at vote counts when prioritizing
- Contribute a service implementation following [docs/contributing.md](contributing.md)

## Version Mapping

| Version | Focus |
|---------|-------|
| 0.x | AWS services, unstable API |
| 1.x ← current | AWS depth, stable plugin API |
| 2.x | Multi-CSP architecture, Azure pilot |
| 3.x+ | Broad CSP coverage, community-owned providers |
