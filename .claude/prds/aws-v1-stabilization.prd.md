# AWS Surface Stabilization — DevCloud v1.0

> Roadmap: [Phase 1 — AWS Depth & Stabilization](../../docs/roadmap.md#phase-1--aws-depth--stabilization-complete-shipped-as-v10)

## Problem

DevCloud's Phase 1 engineering checklist is 9 of 10 complete — 104 services, 729 green boto3 compatibility tests, a finalized `ServicePlugin` API. The single open box is `v1.0 release (pending maintainer tag)`. The blocker is not missing code; it is that **no agreed, published bar exists for what v1.0 promises**, and ~2,200 operations are served by a generic CRUD engine at deliberately "plausible, not faithful" fidelity with no per-operation signal telling a user which is which.

Leaving this unsolved means the project stays at 0.x indefinitely: the maintainer cannot tag with confidence, and Phase 2's internal refactor (IR, `ModelSource`) will churn the codebase with no stable line for users to sit on.

## Evidence

Measured from the repository (2026-08-09):

- 104 services registered, 4,484 dispatch cases — `make stats`
- 729 boto3 compatibility tests across 111 files, green in CI; a failing test fails the build — `docs/services-matrix.md:3,64`
- ~2,200 operations across 46 JSON-protocol services served by the CRUD fallback engine, documented as **"plausible, not faithful"** with no validation, cross-resource integrity, or pagination correctness — `docs/crud-engine.md:8,48`
- Fidelity tiers (`hand-verified` / `auto-crud` / `unimplemented`) are *defined in prose* but not emitted as a per-operation artifact a user can query — `docs/crud-engine.md:29-35`
- Latest tag v0.2.0 (2026-04-21); `[Unreleased]` section is empty — `git tag`, `CHANGELOG.md:9`

**Assumption — needs validation via user research or issue triage**: that end users (boto3 developers, CI teams) are blocked by the 0.x version or by fidelity opacity. No user quotes, tickets, or telemetry support this today. This PRD is maintainer-driven; user demand is unmeasured.

## Users

- **Primary**: the DevCloud maintainer, at the moment of deciding whether to tag v1.0 — needs an explicit, testable definition of "v1.0-ready" and a compatibility contract they can stand behind across the 1.x line.
- **Secondary (unvalidated)**: boto3 application developers and CI teams who would consume the fidelity manifest to know which operations they can trust.
- **Not for**: contributors adding *new* services (the surface is assumed frozen at its current breadth for this release), and anyone needing production-grade or behavior-faithful AWS emulation.

## Hypothesis

We believe **an explicit, machine-verified compatibility contract — a per-operation fidelity manifest, a published stability policy, and a repeatable release process** will **turn "is this ready?" from a judgment call into a passing test** for **the maintainer**.

We'll know we're right when **100% of reachable operations carry a declared fidelity tier, enforced by a test that fails the build if any operation is unclassified — and v1.0 is tagged.**

## Success Metrics

| Metric | Target | How measured |
|---|---|---|
| **Primary** — operations with a declared fidelity tier | 100% | Generated manifest + a conformance test that fails if any reachable operation is unclassified |
| boto3 compatibility suite | stays 100% green | `make test-compat` in CI (existing guardrail) |
| High-value auto-crud ops promoted to hand-verified | **8**, selected from the manifest rather than guessed — the tag operations (`TagResource`, `UntagResource`, `ListTagsForResource`) still `auto-crud` in cloudwatch, eventbridge and kms while their siblings were hand-written | Manifest tier diff between v0.2.0 and v1.0 |
| Breaking `ServicePlugin` changes after tag | 0 across the 1.x line | Existing plugin conformance test + release review |

## Scope

**MVP** — all three of:

1. **Fidelity manifest** — a generated, published artifact declaring every reachable operation's tier (`hand-verified` / `auto-crud` / `unimplemented`), with a test that fails on any unclassified operation.
2. **Stability policy** — a written v1.0 compatibility contract stating what is guaranteed across 1.x (plugin API surface, and the extent of any wire-behavior promise) and what explicitly is not.
3. **Promotion pass + release hardening** — an agreed shortlist of high-value auto-crud operations promoted to hand-verified with compatibility tests, plus a repeatable release checklist, upgrade/deprecation policy, and versioned docs.

**Out of scope**

- **All Phase 2 items** (IR layer, `ModelSource` interface, provider namespacing, per-provider auth adapters) — deferred to 1.x/2.0; v1.0 exists precisely to give users a stable line *before* that refactor.
- **Behavioral parity with real AWS** — no input validation, business logic, eventual-consistency timing, or rate limits on engine-served operations. "Plausible, not faithful" stays the documented stance; the manifest makes it legible rather than eliminating it.

## Delivery Milestones

<!-- Business outcomes, not engineering tasks. /plan turns each into a plan. -->
<!-- Status: pending | in-progress | complete -->

| # | Milestone | Outcome | Status | Plan |
|---|---|---|---|---|
| 1 | Fidelity manifest | Any user can look up any operation and see exactly how much to trust it; no operation is silently unclassified | complete | [fidelity-manifest.plan.md](../plans/fidelity-manifest.plan.md) |
| 2 | Stability policy published | The v1.0 promise is written down and bounded — maintainer and users agree on what 1.x will not break | complete | [stability-policy.plan.md](../plans/stability-policy.plan.md) |
| 3 | High-value promotion pass | The operations users hit most return hand-verified responses instead of plausible ones | complete | [promotion-pass.plan.md](../plans/promotion-pass.plan.md) |
| 4 | Release process hardened | Tagging a release is a checklist, not a judgment call; deprecations follow a stated policy | complete | [release-hardening.plan.md](../plans/release-hardening.plan.md) |
| 5 | v1.0 tagged | DevCloud ships a version users can depend on, ahead of the Phase 2 refactor | complete | [v1-tag.plan.md](../plans/v1-tag.plan.md) |

## Open Questions

- [x] Which operations make the promotion shortlist, and how many? **8 operations across 3 services, and the shortlist was read off the manifest instead of guessed.** The premise was wrong: the operations a local inner loop actually calls are already hand-verified — s3, sqs, lambda, iam and sts have zero `auto-crud` operations. Of the 957 remaining, the bulk is operational and replication surface (sagemaker 280, glue 169) that no inner loop touches. The one real gap was tag operations left at `auto-crud` in cloudwatch, eventbridge and kms. Further promotion is **on request with a stated use case**, per the policy in `docs/fidelity-manifest.md` — not maintainer guesswork. See `.claude/plans/promotion-pass.plan.md`.
- [x] Does the stability guarantee cover only the `ServicePlugin` API, or also wire behavior of hand-verified operations? **Both, but the wire promise is compat-suite-scoped** — 1.x guarantees the response shape of hand-verified operations covered by `tests/compatibility/` (775 tests); uncovered hand-verified operations get best-effort language, not a promise. Keeps the Phase 2 refactor unblocked. See `.claude/plans/stability-policy.plan.md`.
- [x] Is adding new service coverage in or out of scope for v1.0? **Not blocking the tag, and additive when it happens** — new services and `unimplemented` → served transitions are minor-version changes, never breaking.
- [x] Does the fidelity manifest ship as documentation, a queryable runtime endpoint, or both? **Both, already shipped** in #126 — `docs/fidelity-manifest.md` and `GET /devcloud/api/fidelity`.

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Shipping 1.0 with ~2,200 "plausible, not faithful" operations damages trust despite the manifest | Medium | High | Manifest is prominent and machine-generated, not buried prose; stability policy states explicitly that `auto-crud` carries no behavioral promise |
| The v1.0 stability promise over-commits and blocks the Phase 2 refactor | Medium | High | Scope the guarantee narrowly and in writing (open question above) before tagging |
| Promotion shortlist is chosen on guesswork and misses what users actually call | High | Medium | Treat the shortlist as explicitly provisional; derive from issue triage, and revisit in 1.x rather than blocking the tag |
| Manifest drifts from reality as services change | Medium | Medium | Generate it from source, never hand-maintain; enforce with a build-failing test |
| Primary user need is unvalidated (maintainer-driven, no user evidence) | High | Low | Cost of being wrong is low — the work is honesty and process, useful regardless of demand |

---
*Status: DELIVERED — v1.0.0 tagged 2026-08-11 on `2109922`. All five milestones complete.*

*Post-release note: the first v1.0.0 tag published binaries that registered no services —
`.goreleaser.yaml` built `cmd/devcloud/main.go` instead of the package, dropping `imports.go`.
Caught by the plan's own artifact-verification step, fixed in #131, and v1.0.0 was withdrawn and
re-tagged. The compat gate now builds with GoReleaser, so the tested binary is the shipped one.*
