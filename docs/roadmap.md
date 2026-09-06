# Roadmap

DevCloud aims to be a **local development companion for cloud-native apps across
every major CSP** — not a production replacement, but an on-ramp that lets you
iterate without cloud bills and deploy to your target CSP with confidence. It gets
there in phases, to keep scope, architectural complexity and community
expectations manageable.

## Guiding principles

1. **Local-first, cost-free** — no cloud charges for inner-loop development.
2. **On-ramp, not replacement** — DevCloud helps you *land on* a CSP, not avoid it.
3. **API-compatible, not behaviour-perfect** — SDK compatibility over edge-case parity.
4. **Community-owned** — the scope is too large for one maintainer, so the plugin architecture and contributor experience are first-class.
5. **Trademark-respectful** — see [TRADEMARKS.md](../TRADEMARKS.md).

## Phase 1 — AWS depth and stabilization (complete, shipped as v1.0)

Mature the already-broad AWS surface into a stable, well-tested v1.0.

- [x] AWS services scaffolded from official Smithy models via in-tree codegen — counts in [coverage.md](coverage.md)
- [x] Deep hand-written coverage on core and integration services
- [x] Cross-service integration — CloudFormation, DynamoDB Streams → Lambda, SQS → Lambda, S3 → Lambda, EventBridge, SNS → SQS
- [x] boto3 compatibility suite green in CI; a failing test fails the build
- [x] Unimplemented operations return an AWS-shaped error instead of a false `200`
- [x] Stable `ServicePlugin` API ([plugin-api.md](plugin-api.md)), enforced by a conformance test over every registered service
- [x] Generic [CRUD fallback engine](crud-engine.md) serving the long tail with plausible, store-backed responses. **Follow-up:** promote high-value `auto-crud` operations to hand-verified fidelity.
- [x] v1.0 release — see [compatibility-policy.md](compatibility-policy.md)

## Phase 2 — Architectural preparation (complete, v1.x)

Refactor internally so adding a CSP does not require forking the project. Each
item made a future provider an *addition* rather than an edit — the seams are
tabulated in [architecture.md](architecture.md#multi-csp-seams).

- [x] Intermediate Representation between models and codegen ([`internal/codegen/ir`](../internal/codegen/ir/ir.go)). Generators read `*ir.Model`; nothing in the IR names Smithy.
- [x] Parser refactored behind [`ModelSource`](../internal/codegen/source.go). `SmithySource` owns its own format detection, so `cmd/codegen` never names a format.
- [x] Provider namespacing in config — `providers.aws.services.*`, forward-compatible with `providers.azure.*` ([configuration.md](configuration.md#provider-namespacing)).
- [x] Plugin interface review — `ServicePlugin` needed no change; the CSP is carried by the optional [`ProviderScoped`](plugin-api.md#providers-and-csp-neutrality).
- [x] Per-provider auth adapters ([`internal/auth`](../internal/auth/auth.go)). `SigV4` is the AWS implementation; AAD/SAS and OAuth2 slot in beside it without touching the gateway.

Phase 2 deliberately shipped **no** non-AWS service, no second `ModelSource`, and
no signature verification. Those are Phase 3 and beyond; Phase 2's job was to make
each of them an addition rather than a fork.

## Phase 3 — First non-AWS service (next, v2.0, exploratory)

Validate the multi-CSP architecture with one well-scoped pilot.

- [ ] Pick an Azure pilot service — candidate: **Azure Blob Storage**, closest to S3 semantically
- [ ] OpenAPI → IR → codegen proof of concept
- [ ] Azure authentication adapter (Shared Key to start)
- [ ] Compatibility tests against `azure-sdk-for-python`
- [ ] Documentation pattern for multi-CSP service docs

## Phase 4 — Breadth expansion (v2.x+)

- [ ] More Azure services — Queue Storage, Table Storage, Cosmos DB
- [ ] Google Cloud pilot — candidate: **Google Cloud Storage**
- [ ] Other providers as community interest justifies
- [ ] Federated identity playground (cross-CSP IAM simulation)

## Out of scope

- Production hosting or high-availability guarantees
- Billing and quota simulation matching real CSP pricing
- Exact replication of CSP-internal behaviour (consistency timing, rate limits)
- Redistribution of CSP-owned branding assets, logos or documentation

## Influencing the roadmap

- **Missing service?** File a [service request](https://github.com/skyoo2003/devcloud/issues/new?template=service_request.yml) with `GET /devcloud/api/unrouted` output — that is what moves a service into the target ([coverage.md](coverage.md#service-not-supported)).
- **Missing operation or capability?** File a [feature request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml).
- Upvote existing requests with reactions; vote counts inform prioritization.
- Or contribute it — see [contributing.md](contributing.md).

## Version mapping

| Version | Focus |
|---------|-------|
| 0.x | AWS services, unstable API |
| 1.x ← current | AWS depth, stable plugin API, multi-CSP groundwork |
| 2.x | Multi-CSP architecture, Azure pilot |
| 3.x+ | Broad CSP coverage, community-owned providers |
