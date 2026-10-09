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

Phase 1 execution follow-up: Lambda Python 3.12/Node 22 handlers execute in
Docker, published code is immutable, failed SQS/Streams batches preserve source
state, and SNS ARN subscriptions preserve message contents. A required
`lambda-runtime` job verifies actual handler results and S3/SQS/Streams/EventBridge
delivery; the ordinary suite verifies Docker-free contracts. See
[Lambda](services/lambda.md) for runtime requirements and local limits.

AWS operation expansion follow-up: EventBridge's ActivateEventSource,
DeactivateEventSource, DeauthorizeConnection and RemovePermission now share
durable state with their companion APIs. SDK tests verify source-controlled local
delivery, connection reuse and bus policy removal. SNS's six missing operations
now support actual SQS batch/FIFO delivery, mobile/SMS attribute readback, opt-in
and verification with a local outbox OTP. S3 UploadPartCopy now copies actual
source bytes/ranges with condition checks, error rollback and restart persistence.
Directory RenameObject now preserves bytes, metadata and tags with conditional checks,
persistent token replay and scoped CreateSession credentials.
Phase 1A Core Integrations completed native support for all remaining operations
across S3 (RestoreObject, SelectObjectContent, WriteGetObjectResponse),
DynamoDB (PartiQL engine, Kinesis destinations, PITR and table restores),
Lambda (Layer versioning, response streaming, async and durable callbacks), and
CloudFormation (CancelUpdateStack, drift detection, resource scans and orgs access).
Phase 1B Core Identity & Access completed native support for all remaining operations
across IAM (MFA devices, password changes, service credentials, SSH/server/signing certificates,
OIDC provider client IDs, delegation requests, access/credential reports, policy simulation,
and org root credentials/sessions) and STS (AssumeRoleWithWebIdentity, AssumeRoleWithSAML,
AssumeRoot, DecodeAuthorizationMessage, GetDelegatedAccessToken, GetFederationToken, GetWebIdentityToken).
Phase 1C Compute & Network added `aws.protocols#ec2Query` protocol detection,
CRUD engine XML response support (`<requestId>`, item list tags), gateway error routing,
and wired EC2 unhandled operations into the fallback CRUD engine, promoting 498 EC2
operations into `auto-crud`.
Phase 1D Generic CRUD Engine Semantic Verb Expansion added comprehensive
classification for lifecycle and association verbs (Relate, Toggle, Lifecycle, Batch)
across the entire fleet, promoting 2,014 operations into `auto-crud`.
Phase 1E Non-CRUD Data Planes implemented SQLite-backed query execution for `rdsdata`
(`ExecuteStatement`, `BatchExecuteStatement`, `BeginTransaction`, `CommitTransaction`,
`RollbackTransaction`, `ExecuteSql`) and dedicated mock inference backends for
`sagemakerruntime` and `sagemakerruntimehttp2` (`InvokeEndpoint`, `InvokeEndpointAsync`,
`InvokeEndpointWithResponseStream`, `InvokeEndpointWithBidirectionalStream`), promoting
10 operations to `hand-verified` and leaving only 1 registered-only service in the fleet.
Phase 1F 100% Service Fleet Activation implemented native CBOR encoding/decoding
(`internal/shared/cbor`), `smithy.protocols#rpcv2Cbor` protocol detection, and gateway
RPC-v2 routing, bringing `partnercentralrevenuemeasurement` into `auto-crud`. Every
single registered service in the AWS fleet (431 of 431, 100.0%) now serves $\ge 1$ operation,
leaving **zero** registered-only services.
The original 3,085-operation unimplemented baseline has **477 remaining** across all registered services (267 within the serving target);
complete AWS operation support is still in progress across subsequent bundles. See
[EventBridge](services/eventbridge.md), [SNS](services/sns.md), [S3](services/s3.md) and
[IAM/STS](services/iam-sts.md) for local limits.

## Phase 2 — Architectural preparation (complete, v1.x)

Refactor internally so adding a CSP does not require forking the project. Each
item made a future provider an *addition* rather than an edit — the seams are
tabulated in [architecture.md](architecture.md#multi-csp-seams).

- [x] Intermediate Representation between models and codegen ([`internal/codegen/ir`](../internal/codegen/ir/ir.go)). Generators read `*ir.Model`; nothing in the IR names Smithy.
- [x] Parser refactored behind [`ModelSource`](../internal/codegen/source.go). `SmithySource` owns its own format detection, so `tools/codegen` never names a format.
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
