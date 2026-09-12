# TDD evidence — Phase 3 code review follow-up

Date: 2026-09-13
Branch: `feat/phase-3-scaffold-the-218`

## Source plan

No `*.plan.md`. The work items are the three MEDIUM findings from the local
code review of the uncommitted Phase 3 changes (scaffold the 218 services, plus
the registry fix that stops an unclassifiable operation borrowing a sibling's
200).

**No production code changed in this cycle.** Two findings were missing tests
for behaviour that already worked, and one was a documentation contradiction.
The RED evidence below is therefore produced by reverting the Phase 3 fix out of
the working tree, not by writing a fix afterwards.

## User journeys

1. As an SDK caller, I want an operation DevCloud cannot classify to return an
   honest AWS error rather than a broader sibling's `200`, so I never build on a
   success that did not happen.
2. As a maintainer, I want every probe the compatibility suite claims to run to
   either run or be named, so the suite cannot quietly shrink.
3. As a reader of `docs/coverage.md`, I want the registered count and the stated
   target to be reconcilable on one page, so the coverage claim stays legible.

## Task report

### Finding 2 — the fixed defect had no regression test

`_unserved_probe` picks one operation per service and prefers `Describe`/`List`/
`Get`, so chime's `AssociatePhoneNumberWithUser` is never selected; `ImportRestApi`
is excluded outright by `_MUTATING_PREFIXES`. Neither case the changelog names
was reachable by any test.

Two tests added, at the two layers the fix spans:

- `internal/codegen/gen_crud_meta_test.go::TestClassifyOpsRecordsUnclassifiableRESTRoutes`
  — codegen emits the route-only entry (empty `Verb`, REST binding kept), and
  omits an unclassifiable operation that has no REST binding.
- `cmd/devcloud/routing_test.go::TestUnclassifiableRouteDeclinesInsteadOfAnsweringAsASibling`
  — end-to-end against the registry the binary ships, both named cases, plus the
  opposite direction so declining cannot become the cheap way to pass.

RED was produced by reverting the fix with
`git stash push -- internal/codegen/gen_crud_meta.go internal/codegen/gen_fidelity.go internal/shared/crud/crud.go internal/generated/crudregistry/registry_gen.go internal/generated/fidelity/manifest_gen.go`,
then restored with `git stash pop`.

```
$ go test ./internal/codegen/ -run TestClassifyOpsRecordsUnclassifiableRESTRoutes   # RED
--- FAIL: TestClassifyOpsRecordsUnclassifiableRESTRoutes (0.00s)
    Error: map[string]codegen.crudOpData{"UpdateUser":...} does not contain "AssociatePhoneNumberWithUser"
    Messages: an unclassifiable REST operation must still carry its route, or a
              broader sibling's route answers for its path

$ go test ./cmd/devcloud/ -run TestUnclassifiableRouteDeclines   # RED
--- FAIL: .../chime_associate_phone_number
    Error: Expected error with "crud: operation not classifiable" in chain but got nil.
--- FAIL: .../apigateway_import_rest_api
    Error: Expected error with "crud: operation not classifiable" in chain but got nil.
```

The `got nil` is the defect itself: the engine answered, and answered `200`.

```
$ go test ./internal/codegen/ ./cmd/devcloud/ ./internal/shared/crud/   # GREEN
ok  github.com/skyoo2003/devcloud/internal/codegen  6.481s
ok  github.com/skyoo2003/devcloud/cmd/devcloud      1.765s
ok  github.com/skyoo2003/devcloud/internal/shared/crud
```

### Finding 1 — the `sends() == 0` skip was unbounded

`test_service_smoke.py` treats "nothing reached DevCloud" as a failure demanding
an `UNREACHABLE_FROM_BOTO3` entry. `test_no_fabricated_success.py` skipped on the
same condition with nothing pinned, so a change that broke request building for
many services would shrink the suite with no assertion moving.

Added `_coverage.UNSENDABLE_PROBES` and routed both skip sites (the
`ParamValidationError` branch and the `sends() == 0` branch) through
`_skip_if_pinned_unsendable`, which fails with the exact pin key unless the probe
is named. A stale pin — one that does reach the wire — also fails.

RED was produced by adding the mechanism with an empty pin dict:

```
$ pytest test_no_fabricated_success.py -q   # RED, empty UNSENDABLE_PROBES
FAILED test_unserved_operation_declines[machinelearning.Predict]
FAILED test_unserved_operation_declines[rbin.LockRule]
2 failed, 303 passed, 1 xfailed
```

Both turned out to be **this suite's own stub builder**, not a botocore policy —
which is the thing the pin exists to surface. Recorded as such:

- `machinelearning.Predict` — no `PredictEndpoint` in the stub; botocore resolves
  the endpoint from that member and calls `.split()` on the `None`.
- `rbin.LockRule` — the stub pads top-level strings to their minimum length but
  does not descend into nested members; `UnlockDelayValue` has a minimum of 7 and
  the stub sends 1.

```
$ pytest test_no_fabricated_success.py test_service_smoke.py -q   # GREEN
733 passed, 2 skipped, 1 xfailed
```

### Finding 3 — `docs/coverage.md` contradicted itself

The page reported **431 registered** while "The target" stated the target is
"205 services, not 431", listed `Missing services (M) | 283`, and said the 226
"are no longer work DevCloud has promised" — after this change registered all of
them.

Rewritten to separate the two claims the page was conflating. The 2026-09-05
decision refused the *cost* of hand-building 283 services on an untested
assumption; the codegen scaffold removed that cost, so breadth was taken because
it became nearly free. The target stays at 205 because it was never a count of
registrations — it is where depth is promised. Table updated: registered 205 →
431, "Explicitly not targeted" → "Registered, scaffold-served, outside the
target".

No test asserts this section, so there is no RED/GREEN for it. `go test
./cmd/devcloud/` does assert other figures parsed out of this file and stays
green after the edit.

## Test specification

| # | What is guaranteed | Test | Type | Result | Evidence |
|---|---|---|---|---|---|
| 1 | codegen records an unclassifiable REST operation as a route-only entry with an empty `Verb` | `internal/codegen/gen_crud_meta_test.go::TestClassifyOpsRecordsUnclassifiableRESTRoutes` | unit | PASS | `go test ./internal/codegen/` |
| 2 | An unclassifiable operation with no REST binding is not recorded at all | same test | unit | PASS | `go test ./internal/codegen/` |
| 3 | chime `AssociatePhoneNumberWithUser` declines instead of returning `UpdateUser`'s 200 | `cmd/devcloud/routing_test.go::TestUnclassifiableRouteDeclines...` | integration | PASS | `go test ./cmd/devcloud/` |
| 4 | apigateway `ImportRestApi` declines instead of returning `CreateRestApi`'s 200 | same test | integration | PASS | `go test ./cmd/devcloud/` |
| 5 | The classified sibling still serves its own path (no over-declining) | same test, `sibling_still_serves_its_own_path` | integration | PASS | `go test ./cmd/devcloud/` |
| 6 | A probe botocore will not send fails unless pinned by name | `tests/compatibility/test_no_fabricated_success.py::_skip_if_pinned_unsendable` | integration | PASS | `pytest test_no_fabricated_success.py` |
| 7 | A pin that does reach the wire fails as stale | same helper, `assert sent == 0` | integration | PASS (not independently exercised — see gaps) | `pytest test_no_fabricated_success.py` |

## Coverage and known gaps

```
$ go test -cover ./internal/codegen/ ./internal/shared/crud/ ./cmd/devcloud/
internal/codegen        coverage: 84.3% of statements
internal/shared/crud    coverage: 86.8% of statements
cmd/devcloud            coverage: 0.0% of statements
```

`cmd/devcloud` reports 0.0% because its tests assert the registries other
packages build rather than executing `main`; statement coverage is not a
meaningful figure for it.

Full suites, after all edits:

```
$ go build ./... && go vet ./...        # both clean
$ go test ./...                         # no FAIL
$ pytest (tests/compatibility)          # 1527 passed, 2 skipped, 1 xfailed
```

1527 + 2 + 1 = 1,530, the figure `README.md` and `docs/faq.md` publish.

Known gaps, deliberate:

- **Guarantee 7 is not independently exercised.** No pinned probe currently
  reaches the wire, so the stale-pin assertion has never fired. Provoking it
  would mean pinning a healthy probe to watch it fail, which leaves a trap in
  the suite; the assertion is cheap and left unproven.
- **The two pinned probes are fixable, and not fixed here.** Teaching
  `stub_params` to fill `PredictEndpoint` and to descend into nested members
  would put both back on the wire and delete both pins. That is stub-builder
  work, outside this review's scope; the pin text says so, so the next reader
  sees a to-do rather than a botocore limit.
- **`docs/coverage.md` "The target" is still unasserted.** Its table can drift
  from the binary the way it just did. Pinning those three numbers in
  `cmd/devcloud/coverage_test.go` alongside the figures it already parses would
  close it.

## Merge evidence

If these commits are squashed, this section is the surviving record.

- RED: Phase 3 fix reverted from the working tree; the two new Go tests failed
  for the intended reason — the registry held no route-only entry, and
  `crud.Handle` returned a `nil` error (a fabricated `200`) for chime and
  apigateway. Separately, the pinning mechanism with an empty dict failed
  exactly the two probes that had been skipping silently.
- GREEN: fix restored; `go build`, `go vet`, `go test ./...` clean, and the full
  compatibility suite at 1527 passed / 2 skipped (both named) / 1 xfailed.
- Refactor: none. No production code was changed in this cycle.
