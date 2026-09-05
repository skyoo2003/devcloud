# Plan: Release Process Hardened

**Source PRD**: `.claude/prds/aws-v1-stabilization.prd.md`
**Selected Milestone**: 4 — Release process hardened
**Complexity**: Small (half a day)

## Summary

`docs/release.md` already documents SemVer, Changie and GoReleaser end to end, so the
*procedure* exists. What is missing is enforcement: four defects let a bad v1.0 ship today,
and every one of them is caught by a check the repo does not run. This plan adds the checks
and turns the prose into a pre-flight checklist.

## The four defects that block a trustworthy v1.0 tag

Each was reproduced against the current tree.

### 1. The fidelity manifest can drift silently — the headline v1.0 artifact

`internal/generated/fidelity/manifest_gen.go` is committed (one of 370 tracked files under
`internal/generated`), generated from provider dispatch by `make codegen`. **No CI job
verifies the committed output still matches its sources.**

`cmd/devcloud/fidelity_test.go` guards the manifest in three directions — floors
(`minServices 100`, `minOperations 6000`, line 28-31), every registered service present
(line 68), every CRUD-engine operation classified (line 79). None of them fire when a
contributor adds a `case "NewOp":` to a provider's dispatch switch and forgets to
regenerate: the operation is simply absent, and absence from a 7,475-entry map is invisible.

The PRD's primary metric is *"100% of reachable operations carry a declared fidelity tier,
enforced by a test that fails the build if any operation is unclassified"*. It is enforced
for two of the three ways an operation becomes reachable.

### 2. Eight changelog fragments render broken links

`.changie.yaml` declares `Issue` as `type: int, minInt: 1` and `changeFormat` interpolates
it twice. Eight fragments carry `Issue: ""`. Verified with `changie batch v9.9.9 --dry-run`:

```
* Per-operation fidelity manifest ... ([#](https://github.com/skyoo2003/devcloud/issues/))
```

`changie batch` exits 0 — it does not validate custom fields on hand-written fragments. Those
eight lines would ship in `CHANGELOG.md` and in the v1.0 GitHub Release notes verbatim. All
eight are from this session's two PRs and are trivially fixable; the durable fix is the check.

### 3. One fragment is in a directory nothing reads

`.changes/unreleased/Fixed-113.yaml` sits beside the real `changes/unreleased/` (25
fragments). `.changie.yaml` sets `changesDir: changes`, so this fragment is silently
excluded from every release.

### 4. Pushing a tag publishes without running the test suite

`.github/workflows/release.yml` verifies `changes/<tag>.md` exists (line 49-55) and then
runs GoReleaser. It has no `needs:` on CI. `ci.yml` does trigger on `tags: ["v*"]`, but the
two workflows race — a red tag still publishes binaries, container images and a Homebrew
formula. `compat.yml` does not run on tags at all.

## Patterns to Mirror

| Category | Source | Pattern |
|---|---|---|
| Release-blocking guard | `.github/workflows/release.yml:49-55` | Shell check + `::error::` annotation naming the fix command, then `exit 1` |
| Conservative test floor | `cmd/devcloud/fidelity_test.go:28-31` | Assert a floor, not an exact count; `t.Errorf` per violation so all failures surface |
| Docs cross-link | `docs/release.md:15-16` | Table row per pipeline, linking the workflow file |
| Deprecation in practice | `internal/config/config.go:138-145` | Honour the old key one release, warn, then remove |

## Files to Change

| File | Action | Why |
|---|---|---|
| `.github/workflows/ci.yml` | UPDATE | New `codegen-drift` job: regenerate, fail on diff |
| `.github/workflows/release.yml` | UPDATE | Gate publishing on the test suite; validate fragment issue numbers |
| `changes/unreleased/*.yaml` (8) | UPDATE | Fill the empty `Issue` fields (#126) |
| `.changes/unreleased/Fixed-113.yaml` | MOVE | Into `changes/unreleased/` so it reaches a release |
| `docs/release.md` | UPDATE | Pre-flight checklist; deprecation step; link the policy |
| `.goreleaser.yaml` | UPDATE | Ship `docs/` in the archive — "versioned docs", by tag |
| `changes/unreleased/Changed-*.yaml` | CREATE | Changie fragment for this work |

## Tasks

### Task 1: Close the codegen-drift hole
- **Action**: add a `codegen-drift` job to `ci.yml` — `make codegen`, then
  `git diff --exit-code internal/generated`. On failure, `::error::` telling the contributor
  to run `make codegen` and commit.
- **Mirror**: the error-annotation style of `release.yml:52-53`.
- **Validate**: add `case "Bogus":` to any provider, push → job fails. Revert → green.
- **Note**: this is the check that makes the PRD's primary metric true rather than
  approximately true. It belongs in CI, not in a Go test, because it needs the generator to
  run against the working tree.

### Task 2: Validate changelog fragments
- **Action**: extend the existing "Verify changie release notes exist" step in
  `release.yml` to also reject a batched version file containing `issues/)` — the signature
  of an empty issue number. One `grep -q`, no new tooling.
- **Validate**: `grep -c 'issues/)' changes/v9.9.9.md` on a dry batch returns 8 before the
  fragment fix, 0 after.

### Task 3: Fix the fragments and the stray directory
- **Action**: set `Issue: "126"` on the eight empty fragments; `git mv` the stray
  `.changes/unreleased/Fixed-113.yaml` into `changes/unreleased/` and remove the empty
  `.changes/` tree.
- **Validate**: `changie batch v9.9.9 --dry-run | grep -c 'issues/)'` → 0, and the batch
  includes 26 entries.

### Task 4: Gate the release on green tests
- **Action**: add a `test` job to `release.yml` that runs `CGO_ENABLED=1 go test ./...`, and
  `needs: test` on the release job. Skip on `dry_run` only if the dry run is meant to be
  fast — default is to run it there too.
- **Decision**: duplicate the job rather than `workflow_run` on CI. A `workflow_run` trigger
  cannot block a tag push, and reusable-workflow plumbing costs more than a 12-line job.
- **Validate**: dispatch the workflow with `dry_run: true` on a commit with a deliberately
  failing test → no artifacts published.

### Task 5: Turn release.md into a checklist
- **Action**: add a **Pre-flight** section ahead of "Cutting a release" — main green,
  `make codegen` clean, `make test-compat` green, every fragment has an issue number,
  `changes/unreleased/` non-empty, and *"if this release removes a deprecated key, confirm it
  shipped with a warning in the previous release"* (the `dashboard` → `admin` precedent at
  `internal/config/config.go:138-145`). Cross-link `docs/compatibility-policy.md`.
- **Dependency**: the deprecation *policy text* is Milestone 2's Task 1. This plan links to
  it; if M2 lands after this, leave the link and land M2's file to resolve it.

### Task 6: Versioned docs, the lazy version
- **Action**: add `docs/` to `.goreleaser.yaml:31-34`'s archive `files:` list. Docs are then
  versioned by tag — reading docs at `v1.0.0` gives v1.0 docs, and the downloaded archive
  carries the fidelity manifest and compatibility policy matching the binary.
- **Skipped**: a versioned docs site (Docusaurus/mkdocs + per-version builds). Add when docs
  need to be browsable without a checkout — not before v1.0.

## Validation

```bash
# defect reproduction, before the fix
changie batch v9.9.9 --dry-run | grep -c 'issues/)'   # 8 -> 0 after Task 3

# the new guard
make codegen && git diff --exit-code internal/generated

# unchanged guardrails
CGO_ENABLED=1 go test ./... && make test-compat
golangci-lint run ./...
```

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `make codegen` is not byte-reproducible (map iteration order, timestamps) — the drift job flakes | Medium | Verify by running it twice on a clean tree before wiring CI; if unstable, sort at generation time rather than relaxing the check |
| Codegen needs `smithy-models/` present in CI | Medium | Already checked in; confirm the job does not need `smithy-sync.yml`'s network fetch |
| Duplicating the test job slows tag pushes by ~1 min | Low | Accepted — a wrong publish costs more than a minute |
| `docs/` in the archive bloats it | Low | Docs are markdown; measured impact is tens of KB |

## Acceptance

- [ ] A provider dispatch case added without `make codegen` fails CI
- [ ] `changie batch --dry-run` produces zero empty issue links
- [ ] `Fixed-113.yaml` reaches the changelog; no stray `.changes/` tree
- [ ] A red commit cannot publish a release
- [ ] `docs/release.md` opens with a pre-flight checklist, including the deprecation check
- [ ] `make test-compat` green

## Open Decision

**Does the drift job run `make codegen` (all 104 services, from `smithy-models/`) or only the
fidelity scan?** Full codegen is the honest check but is the slower and more fragile of the
two. Default: full codegen, on the grounds that stale generated code is a release defect
whether or not it is the manifest. Fall back to a fidelity-only job if the full run proves
non-reproducible.
