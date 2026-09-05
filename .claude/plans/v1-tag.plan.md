# Plan: v1.0 Tagged

**Source PRD**: `.claude/prds/aws-v1-stabilization.prd.md`
**Selected Milestone**: 5 — v1.0 tagged
**Complexity**: Small (~2 hours, most of it waiting on the release workflow)

## Summary

Milestones 1–4 built everything the tag depends on: the fidelity manifest, the compatibility
policy, the promotion pass and policy, and a release workflow that refuses to publish off a red
commit. The pre-flight in `docs/release.md` passes locally today, verified below.

What is left is not engineering. It is three documents that still declare the project pre-1.0
and would ship *inside* the v1.0 archive saying so, PRD bookkeeping that contradicts what M3
delivered, one unexercised step in the release path, and the execution itself.

## Pre-flight, run against the current tree (2026-08-11, `4ffafe0`)

| Check | Command | Result |
|---|---|---|
| Go tests | `CGO_ENABLED=0 go test ./...` | pass |
| boto3 compatibility | `make test-compat` | **775 passed** in 10.45s |
| Fragments carry issue numbers | `grep -L 'Issue: "[0-9]' changes/unreleased/*.yaml` | no output |
| Release notes render | `changie batch v1.0.0 --dry-run` | 33 entries, **0** empty issue links |
| `changes/unreleased/` non-empty | `ls` | 33 fragments |
| Codegen drift | `rm -rf internal/generated && make codegen` | **not run** — mutates the tree; Task 1 |

Only the drift check is outstanding, and the release workflow re-runs it on the tag
(`release.yml:85-112`) before anything publishes.

## What actually blocks the tag

### 1. Three files ship inside the v1.0 archive announcing that v1.0 has not happened

`.goreleaser.yaml:31-49` puts the whole `docs/` tree and `GOVERNANCE.md` in every archive —
added in #127 and **not yet in any release**, so v1.0 is the first tag where these are user-facing
artifacts rather than repo files.

- `docs/roadmap.md:30` — `- [ ] v1.0 release (pending maintainer tag)`
- `docs/roadmap.md:78` — `| 0.x ← current | AWS services, unstable API |`
- `GOVERNANCE.md:5,11` — `## Current state (0.x)` … *"while the project is pre-1.0"*

Unpacking `devcloud_v1.0.0_linux_amd64.tar.gz` would hand a user a roadmap saying the release
they just downloaded is pending, and a governance doc calling the API unstable — directly
contradicting `docs/compatibility-policy.md`, which ships beside it.

### 2. GoReleaser is the one step in the release path with no rehearsal

The `dry_run` dispatch cannot rehearse an unreleased version: `resolve` checks out
`github.event.inputs.tag` (`release.yml:41-44`), so the tag must already exist — and any `v*`
tag push publishes for real (`release.yml:3-6`). The three gate jobs cover tests, drift and
compat; GoReleaser itself has not run since `.goreleaser.yaml` gained `docs`, `dockers_v2` and
the archive file list.

Fix is local, not more CI: `goreleaser release --snapshot --clean --skip=publish,docker`.

### 3. No release candidate — and the config would mis-publish one anyway

`.goreleaser.yaml` sets no `release.prerelease`, so it defaults to `false`. A `v1.0.0-rc1` tag
would publish as a **full latest release**, push the Homebrew formula (`brews:` has no
`skip_upload: auto`), and move `latest-alpine`, `v1-alpine` and `v1.0-alpine`
(`.goreleaser.yaml:88-92`).

**Decision: no rc.** The defect class that made v0.2.0's binaries unusable (#128 — every
published binary exited at startup) is now caught before publishing, because the `compat` gate
builds with `CGO_ENABLED=0` and runs all 775 boto3 tests against that exact binary
(`release.yml:130-134`). An rc would cost a `prerelease: auto` change plus brew/tag guards for
coverage the gates already provide. If the maintainer wants one anyway, that config change lands
first — it is a prerequisite, not an option.

### 4. The PRD contradicts its own delivered milestone

Success Metrics row 3 still reads *"TBD — count and selection criteria to be set during /plan"*
and Open Question 1 is unchecked, although M3 answered both: **8 tag operations promoted**, and
promotion is on request with a stated use case
(`.claude/plans/promotion-pass.plan.md:11,99-101`). Tagging v1.0 against a PRD whose primary
artifact is honesty, while the PRD itself is stale, is the one inconsistency worth closing.

## Patterns to Mirror

| Category | Source | Pattern |
|---|---|---|
| Release procedure | `docs/release.md:70-104` | batch → merge → commit `chore(release): vX.Y.Z` → tag → push; tag must match `changes/<tag>.md` |
| Changelog fragment | `changes/unreleased/*.yaml` | `kind` + one-line `body` + `Issue: "<n>"`, written by `changie new` |
| Doc cross-link | `docs/release.md:64-65` | link the policy file that owns the rule rather than restating it |
| Milestone bookkeeping | `.claude/prds/aws-v1-stabilization.prd.md:64-67` | status cell `complete` + relative link to the plan |

## Files to Change

| File | Action | Why |
|---|---|---|
| `docs/roadmap.md` | UPDATE | Tick the v1.0 box; move the current marker to `1.x` |
| `GOVERNANCE.md` | UPDATE | Drop the `0.x` / pre-1.0 framing |
| `.claude/prds/aws-v1-stabilization.prd.md` | UPDATE | Fill metric row 3 and Open Question 1 from M3; M5 status |
| `changes/unreleased/Documentation-*.yaml` | CREATE | Fragment for the doc corrections |
| `changes/v1.0.0.md` | CREATE | `changie batch v1.0.0` output |
| `CHANGELOG.md` | UPDATE | `changie merge` output |

Nothing under `internal/` changes. If this plan touches Go code, something was missed in M1–M4.

## Tasks

### Task 1: Finish the pre-flight
- **Action**: `rm -rf internal/generated && make codegen && git status --porcelain internal/generated`
  → prints nothing. Clear the tree first, as CI does (`release.yml:100-104`) — regenerating in
  place leaves a retired output looking current.
- **Validate**: empty output. Any diff means commit the regeneration before going further.

### Task 2: Retire the pre-1.0 framing from what ships
- **Action**: `docs/roadmap.md:30` → `- [x] v1.0 release`; `:78` → `| 1.x ← current | AWS depth,
  stable plugin API |` with the `0.x` row kept as history (`AWS services, unstable API`).
  `GOVERNANCE.md:5` → `## Current state`; drop the "pre-1.0" sentence at `:11` — the maintainer
  model is not what changes at 1.0.
- **Mirror**: `docs/compatibility-policy.md:1-12`, which already speaks in the present tense
  about what 1.x guarantees.
- **Validate**: `grep -rn '0\.x\|pre-1\.0\|pending maintainer tag' docs/ *.md` returns only the
  historical roadmap row.

### Task 3: Reconcile the PRD with what shipped
- **Action**: Success Metrics row 3 → `8 operations` promoted, criteria = *the manifest's own
  data: services with remaining `auto-crud` operations a local inner loop actually reaches*;
  measured by the manifest tier diff. Tick Open Question 1 with the M3 answer and link
  `.claude/plans/promotion-pass.plan.md`.
- **Validate**: no `TBD` remains in the PRD.

### Task 4: Rehearse GoReleaser locally
- **Action**:
  ```sh
  HOMEBREW_TAP_OWNER=skyoo2003 HOMEBREW_TAP_REPO=homebrew-tap HOMEBREW_TAP_TOKEN=dummy \
    goreleaser release --snapshot --clean --skip=publish,docker
  ```
- **Validate**: 6 archives in `dist/`, and
  `tar tzf dist/devcloud_*_linux_amd64.tar.gz | grep -c '^docs/'` is non-zero — the #127 change
  proven before it reaches a user, not after.
- **Note**: `--skip=docker` only because the image build needs a running daemon; the release
  workflow builds it, and a broken Dockerfile fails the job before publishing.

### Task 5: Cut the release
- **Action**: follow `docs/release.md:70-104` verbatim, `VERSION=v1.0.0`:
  `changie batch v1.0.0` → `changie merge` → commit `chore(release): v1.0.0` → push `main` →
  `git tag v1.0.0 && git push origin v1.0.0`.
- **Validate**: `changes/v1.0.0.md` holds exactly one `## [v1.0.0](…)` heading and 33 `* ` entries
  — the two properties `release.yml:184-200` enforces.
- **Note**: Tasks 2 and 3 must be committed *before* batching, so their fragment is in the notes
  and the corrected docs are in the archive.

### Task 6: Verify what was published
- **Action**: after the workflow goes green —
  1. `curl -sL https://github.com/skyoo2003/devcloud/releases/download/v1.0.0/devcloud_v1.0.0_darwin_arm64.tar.gz | tar tz | grep docs/compatibility-policy.md`
  2. unpack, run `./devcloud &`, then
     `curl -s localhost:4747/devcloud/api/fidelity | head` — the manifest endpoint the release
     promises, answering from the published binary
  3. `docker run --rm ghcr.io/skyoo2003/devcloud:v1.0.0-alpine` starts
  4. `brew install skyoo2003/tap/devcloud && devcloud -h`
- **Why**: v0.2.0 passed CI and shipped binaries that could not start (#128). The gates now catch
  that class, but the Homebrew formula and the container image are still only exercised at publish
  time. Ten minutes here is the difference between finding out and being told.

### Task 7: Close the milestone
- **Action**: PRD milestone 5 → `complete`; add a `changes/unreleased/` fragment only if anything
  in Task 6 required a fix.
- **Validate**: `git tag --list 'v1.0.0'` and the GitHub Release both exist.

## Validation

```bash
# pre-flight (Task 1)
rm -rf internal/generated && make codegen && git status --porcelain internal/generated
CGO_ENABLED=0 go test ./... && make test-compat

# notes (Task 5)
changie batch v1.0.0 --dry-run | grep -c '^\* '        # 34 after the Task 2 fragment
changie batch v1.0.0 --dry-run | grep -c 'issues/)'    # 0

# archive (Task 4)
tar tzf dist/devcloud_*_linux_amd64.tar.gz | grep -c '^docs/'
```

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| GoReleaser fails mid-publish, leaving a partial release under a burned tag | Low | Task 4 rehearses it locally first; recovery is delete the release + tag and re-push the same tag, before anything consumes it |
| Homebrew tap push fails on token/permissions — the one step no gate covers | Medium | Task 6 checks it; a tap fix is a tap-repo commit, it does not invalidate the release |
| `make codegen` is not byte-reproducible and Task 1 shows spurious drift | Low | The same job has been green in CI since M4; a diff here is real |
| v1.0 goes out with ~2,200 `auto-crud` operations and reads as overclaiming | Medium | Already mitigated by design — the manifest is generated and served at `/devcloud/api/fidelity`, and the compatibility policy states plainly that `auto-crud` carries no behavioural promise |
| 1.x then blocks the Phase 2 refactor | Medium | The wire promise is scoped to the 775 compat-suite tests; `internal/` is explicitly outside the user contract (`plugin-api.md:128-135`) |

## Acceptance

- [ ] No shipped file describes DevCloud as pre-1.0
- [ ] PRD carries no `TBD`; Open Question 1 answered
- [ ] Local GoReleaser snapshot produces archives containing `docs/`
- [ ] `v1.0.0` tagged, workflow green, GitHub Release notes = `changes/v1.0.0.md`
- [ ] Published binary, container image and Homebrew formula each verified to start
- [ ] PRD milestone 5 marked `complete`

## Open Decision

**Straight to `v1.0.0`, or `v1.0.0-rc1` first?** This plan assumes straight to v1.0.0 — the gates
cover the failure mode an rc would catch, and `.goreleaser.yaml` would currently mis-publish an rc
as the latest stable release. Choosing an rc means landing `release.prerelease: auto` plus
`skip_upload: auto` on `brews:` and prerelease-aware docker tags first, which is a change to the
release config on the eve of the release it is meant to de-risk.
