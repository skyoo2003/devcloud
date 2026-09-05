# Plan: High-Value Promotion Pass

**Source PRD**: `.claude/prds/aws-v1-stabilization.prd.md`
**Selected Milestone**: 3 — High-value promotion pass
**Complexity**: Small (half a day) — *after* cutting the milestone to what the data supports

## Summary

The milestone assumed a long list of frequently-called operations sitting at `auto-crud`
that users would want promoted. Now that the manifest exists, that list can be read instead
of guessed — and it is almost empty. What remains worth promoting is **8 tag operations
across 3 services**, where DevCloud is simply inconsistent with itself.

The rest of this plan argues for *not* doing the bulk of the milestone, and for replacing
it with a written promotion policy.

## The finding that reshapes this milestone

957 `auto-crud` operations sit in 19 services; the top four hold 67% of them:

| Service | auto-crud | hand-verified | Tier |
|---|---:|---:|---|
| sagemaker | 280 | 65 | 3 |
| glue | 169 | 65 | 3 |
| ssm | 112 | 19 | 2 |
| cloudwatchlogs | 79 | 23 | 2 |
| ecr | 31 | 25 | 2 |
| eventbridge | 29 | 24 | 2 |
| dynamodb | 28 | 20 | **1** |
| cloudwatch | 20 | 20 | 2 |
| kms | 14 | 28 | 2 |

Reading what those operations actually *are* is what changes the plan. The Tier 1 service's
28 are: `CreateBackup`, `CreateGlobalTable`, `DescribeContributorInsights`,
`DescribeKinesisStreamingDestination`, `ListExports`, `UpdateTableReplicaAutoScaling` …
— backup, global replication, export/import and operational insight. A developer running
`put_item` and `query` against a local DynamoDB never calls one of them. The same holds
elsewhere: kms's are CloudHSM custom key stores and key rotation; cloudwatch's are Insight
Rules and Metric Streams; ecr's are pull-through cache, registry policy and signing
configuration.

**The operations that matter in a local inner loop are already hand-verified.** s3, sqs,
lambda, iam and sts have zero `auto-crud` operations — nothing left to promote.

So "promote the top-N high-value operations" describes work that mostly does not exist. The
one genuine gap is narrower and sharper.

### The real gap: tag operations

| Operation | Still `auto-crud` in |
|---|---|
| `TagResource` | cloudwatch, eventbridge, kms |
| `UntagResource` | cloudwatch, eventbridge, kms |
| `ListTagsForResource` | cloudwatch, eventbridge |

**64 services already implement these by hand** on the shared `TagStore`
(`internal/shared/tags.go`). These three are the stragglers — not a judgement call about
value, just an inconsistency. Tagging is also something local code genuinely does, and
`auto-crud` handles it badly: the engine echoes input rather than persisting tags against
an ARN, so `TagResource` followed by `ListTagsForResource` does not round-trip correctly.

## Patterns to Mirror

| Category | Source | Pattern |
|---|---|---|
| Tag dispatch | `internal/services/ecr/provider.go:118-123` | `case "TagResource": return p.handleTagResource(params)` — three cases delegating to handlers |
| Tag storage | `internal/shared/tags.go:26-71` | `NewTagStore(db)` with `AddTags` / `RemoveTags` / `ListTags` / `DeleteAllTags`, keyed by ARN |
| ARN building | `internal/shared/arn.go` | shared ARN construction rather than per-service string building |
| Compat test | `tests/compatibility/test_ecr.py` | boto3 round-trip: tag → list → untag → list |
| Tier movement | `internal/generated/fidelity/manifest_gen.go` | regenerating reclassifies automatically; no manual bookkeeping |

## Files to Change

| File | Action | Why |
|---|---|---|
| `internal/services/kms/provider.go` (+ store) | UPDATE | Add the tag cases on `TagStore` |
| `internal/services/cloudwatch/provider.go` (+ store) | UPDATE | Same |
| `internal/services/eventbridge/provider.go` (+ store) | UPDATE | Same |
| `tests/compatibility/test_{kms,cloudwatch,eventbridge}.py` | UPDATE | boto3 tag round-trip per service |
| `internal/generated/fidelity/manifest_gen.go` | REGENERATE | 8 operations move `auto-crud` → `hand-verified` |
| `docs/fidelity-manifest.md` | UPDATE | Add the promotion policy section (Task 3) |
| `changes/unreleased/Added-*.yaml` | CREATE | Changie fragment |

## Tasks

### Task 1: Promote the tag operations
- **Action**: For kms, cloudwatch and eventbridge, wire `TagResource` / `UntagResource` /
  `ListTagsForResource` onto the shared `TagStore`, keyed by the resource ARN the service
  already builds. kms names its lister `ListResourceTags` — also `auto-crud`, so include it.
- **Mirror**: `internal/services/ecr/provider.go:118-123` plus its handlers.
- **Validate**: `go test ./internal/services/{kms,cloudwatch,eventbridge}/`.

### Task 2: Prove the round-trip in boto3
- **Action**: Per service, a compat test that tags a real resource, lists tags, untags, and
  lists again — the round-trip `auto-crud` cannot do.
- **Mirror**: `tests/compatibility/test_ecr.py`.
- **Validate**: `make test-compat` stays green and covers the new paths.

### Task 3: Write the promotion policy instead of promoting more
- **Action**: A section in `docs/fidelity-manifest.md` stating how an operation moves from
  `auto-crud` to `hand-verified`: **on request, with the requester's use case**, via a
  GitHub issue — not by maintainer guesswork. Point at the manifest as the way to check a
  tier before filing. Record explicitly that the remaining ~949 `auto-crud` operations are
  overwhelmingly operational/replication surface with no inner-loop role, and that v1.0
  ships them declared rather than implemented.
- **Validate**: the claim matches the manifest; no promise the repo cannot keep.

### Task 4: Regenerate and confirm the move
- **Action**: `make codegen`; verify the 8 operations changed tier and nothing else did.
- **Validate**: `git diff internal/generated/fidelity/` shows exactly the expected lines.

## Validation

```bash
go test ./...
make test-compat
make codegen && git diff --stat internal/generated/fidelity/
```

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Promoting tags changes behaviour for callers relying on the engine's echo | Low | The engine never persisted tags; a real round-trip is strictly more correct. Compat tests pin the new behaviour |
| "We promoted only 8 operations" reads as a thin milestone | High | It is the honest read of the data. The deliverable is the policy plus the manifest, not a promotion count |
| A service's ARN construction differs from what TagStore expects | Medium | Reuse `internal/shared/arn.go`; assert the ARN in a unit test before wiring the handler |

## Acceptance

- [ ] Tag operations serve real, persisted tags across kms, cloudwatch, eventbridge
- [ ] boto3 round-trip (tag → list → untag → list) passes per service
- [ ] Manifest regenerated; exactly those operations moved tier
- [ ] Promotion policy documented, with the remaining auto-crud surface characterised honestly
- [ ] `make test-compat` green

## Open Decision

**Is cutting the milestone to 8 operations acceptable?** The alternative is promoting all
313 `auto-crud` operations in Tier 1/2 services — weeks of work on backup, global-table,
Insight Rule and pull-through-cache surface that a local development companion has no
inner-loop use for. My recommendation is the cut: ship the manifest's honesty plus the tag
fix, and let real requests drive promotion in 1.x.
