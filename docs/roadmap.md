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

### Phase 1 — AWS Depth (Current, targeting v1.0)

**Goal**: prove the core thesis on AWS before expanding surface area.

- [x] Core AWS services: S3, SQS, DynamoDB, Lambda, IAM, STS
- [x] Smithy-based codegen pipeline (96 services, 4,438 operations scaffolded)
- [x] Cross-service integration (CFN, DDB Streams → Lambda, EventBridge, S3 → Lambda)
- [x] boto3 compatibility suite (currently 671/699 passing)
- [ ] Lift boto3 compatibility to ≥ 95%
- [ ] Deepen coverage on existing 6 implemented services (fewer `NotImplementedError` returns)
- [ ] v1.0 release with stable plugin API

### Phase 2 — Architectural Preparation (v1.x)

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
| 1.x | AWS depth, stable plugin API |
| 2.x | Multi-CSP architecture, Azure pilot |
| 3.x+ | Broad CSP coverage, community-owned providers |
