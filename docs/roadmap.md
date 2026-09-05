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

### Phase 2 — Architectural Preparation (Complete, v1.x)

**Goal**: internal refactor so adding a new CSP doesn't require forking the project.

- [x] Intermediate Representation (IR) between API models and codegen — [`internal/codegen/ir`](../internal/codegen/ir/ir.go). The generators read `*ir.Model` and nothing in the IR names Smithy; `ir.Model.Provider` carries the owning CSP.
- [x] `internal/codegen/parser.go` refactored behind a [`ModelSource`](../internal/codegen/source.go) interface. `SmithySource` is the first implementation and owns its own format detection, so `cmd/codegen` hands every file to `SourceFor` and never names a format. OpenAPI/Protobuf are an added file.
- [x] Provider namespacing in config — `providers.aws.services.*`, forward-compatible with `providers.azure.*`. The top-level `services` block is the same AWS block under its historical name and keeps working ([configuration.md](configuration.md#provider-namespacing)).
- [x] Plugin interface review — `ServicePlugin` needs no change; the CSP is carried by the optional [`ProviderScoped`](plugin-api.md#providers-and-csp-neutrality) interface, read through `plugin.ProviderOf`. Adding a method would have broken every in-tree plugin to make it state a value it already defaults to.
- [x] Per-provider auth adapter interface — [`internal/auth`](../internal/auth/auth.go). `Adapter` reads one provider's credential form; `SigV4` is the AWS implementation, and AAD/SAS and OAuth2 slot in beside it without touching the gateway. The caller's claimed identity reaches plugins via `auth.FromContext`.

What Phase 2 deliberately did **not** do: no non-AWS service, no second `ModelSource`, and no signature verification. Those are Phase 3 and beyond — Phase 2's job was to make each of them an addition rather than a fork.

### Phase 3 — First Non-AWS Service (Next, v2.0, exploratory)

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
| 1.x ← current | AWS depth, stable plugin API, multi-CSP groundwork (IR, `ModelSource`, provider namespacing, auth adapters) |
| 2.x | Multi-CSP architecture, Azure pilot |
| 3.x+ | Broad CSP coverage, community-owned providers |
