# TDD evidence — Phase 4 code review follow-up

Date: 2026-09-13
Branch: `feat/phase-4-two-axis-docs-and-gates`

## Source plan

No `*.plan.md`. The work items are the findings from the local code review of the
uncommitted Phase 4 changes (the two-axis coverage docs, the target-table gate,
the binary-size CI check, and the startup budget test).

One finding raised in that review — that the runtime table's delimiter row had
fallen out of sync with its header — **was wrong**. The delimiter row has five
cells and matches. It is recorded here so the correction is not lost:

```
$ awk 'NR==251||NR==252 {n=gsub(/\|/,"|"); print NR": "n-1" cells: "$0}' docs/coverage.md
251: 5 cells: | | 105 services | 147 services | 205 services | 431 services |
252: 5 cells: |---|---|---|---|---|
```

## User journeys

1. As a maintainer, I want the published startup figure to be gated by something
   that would actually fail if startup regressed, so a green CI is evidence and
   not decoration.
2. As a maintainer hitting the binary-size failure in CI, I want the error to
   name a number I can find in the docs, so the message is actionable.
3. As a reader of `docs/coverage.md`, I want the text describing what is gated to
   describe what is in fact gated.

## Task report

### Finding (HIGH) — the startup gate measured the wrong phase

`cmd/devcloud/budget_test.go` timed `Registry.Construct`, which only calls the
factory. Every factory in the tree is a struct literal — `internal/services/s3/provider.go:1415`
is `return &S3Provider{}` — while the real startup cost is in `Init`, where
`S3Provider.Init` does an `os.MkdirAll` and opens a SQLite database
(`internal/services/s3/provider.go:48`). The gate measured 84 µs against a 150 ms
budget: roughly 1,800x slack over work that could not regress.

The test now brings every service up the way `main.go:86` does — factory, then
`Init` against a per-service `DataDir` under `t.TempDir()` — into a fresh
registry, so `DefaultRegistry.active` is not polluted for the rest of the
package. A `t.Cleanup` runs `ShutdownAll` to close the SQLite handles.

**RED** was produced by injecting a regression of the exact shape the gate claims
to catch: `time.Sleep(8 * time.Millisecond)` inside `Registry.Init`
(`internal/plugin/registry.go:47`), simulating every provider gaining per-call
work at startup. Both the old and the new gate were run against it.

```
$ go test ./cmd/devcloud/ -run 'TestOldGateConstructOnly|TestRegisteredFleetComesUpWithinItsBudget' -v -count=1
=== RUN   TestOldGateConstructOnly
    budget_old_test.go:30: OLD GATE: constructed 431 services in 79µs
--- PASS: TestOldGateConstructOnly (0.00s)          <-- the blind gate, unmoved
=== RUN   TestRegisteredFleetComesUpWithinItsBudget
    budget_test.go:47: initialized 431 services (0 declined) in 4.018s
    budget_test.go:52: bringing 431 services up took 4.018s, over the 2s budget
--- FAIL: TestRegisteredFleetComesUpWithinItsBudget (4.07s)
```

The injection was then reverted (`git diff --quiet internal/plugin/registry.go`
reports clean) and the temporary `budget_old_test.go` deleted.

**GREEN**:

```
$ go test ./cmd/devcloud/ -run TestRegisteredFleetComesUpWithinItsBudget -v -count=1
    budget_test.go:78: brought 431 services up in 141ms
--- PASS: TestRegisteredFleetComesUpWithinItsBudget (0.17s)
```

Baseline across three runs before the budget was chosen: 135 ms, 155 ms, 142 ms,
with 0 of 431 services declining to initialize. The 141 ms path reproduces the
cold-path figure `docs/coverage.md` publishes (118–148 ms) rather than
approximating it.

**Why the budget is 2 s and not 150 ms.** The published number is a measurement
on a quiet machine; the gate runs on a shared CI runner where a 2x reading is
indistinguishable from a noisy neighbour. A budget tight enough to catch 2x would
fail for reasons unrelated to DevCloud — the objection `docs/coverage.md` itself
raises. 2 s is ~14x the local reading and catches the regression that is real: a
provider doing per-call work at startup, which the injection above shows arrives
as 10x, not 2x. The looseness is stated in the test comment so it is not mistaken
for a tight gate.

### Finding (MEDIUM) — the CI size error named a figure the docs did not carry

`45 MiB` appeared nowhere under `docs/`, yet the failure text read "exceeds the
size budget docs/coverage.md publishes". `coverage.md` publishes 36.8 MiB on
Apple Silicon while CI builds for linux, so they are not the same number and
never will be.

Fixed on both sides rather than either: `docs/coverage.md` now names the 45 MiB
ceiling next to the 36.8 MiB measurement, and the CI step is renamed to
"regression ceiling" with an error that quotes both numbers. No test — the check
is a shell comparison in `.github/workflows/ci.yml` and its own failure is the
evidence.

### Finding (MEDIUM/LOW) — documentation follow-through

- `docs/coverage.md` runtime section rewritten: it described gating "constructing
  all 431 providers", which is what the test no longer does. It now states what
  is asserted (an order of magnitude) and why that differs from what is measured.
- The "published ceiling is 150 ms" sentence was removed; that ceiling no longer
  exists.
- Duplicate row in the upstream-sync table collapsed — "Models vendored when
  sampled | 194 (of 420 today)" and "Vendored models refreshed | 194" said the
  same thing twice.
- `docs/contributing.md:95` rewrapped from 144 characters to the ~80 used
  throughout. No words changed.

## Test specification

| # | What is guaranteed | Test file or command | Test type | Result | Evidence |
|---|---|---|---|---|---|
| 1 | Every registered service initializes; none is registered-but-broken | `cmd/devcloud/budget_test.go:TestRegisteredFleetComesUpWithinItsBudget` | integration | PASS | `go test ./cmd/devcloud/ -run TestRegisteredFleetComesUpWithinItsBudget` — 431 up, 0 declined |
| 2 | Bringing the whole fleet up stays within an order of magnitude of the published figure | same | integration | PASS | 141 ms against a 2 s ceiling; fails at 4.0 s under an 8 ms/service injection |
| 3 | The routing target on the coverage page equals the count the binary registers | `cmd/devcloud/coverage_test.go:TestPublishedTargetTableMatchesTheBinary` | unit | PASS | `go test ./cmd/devcloud/` |
| 4 | The three numbers in the two-axis table are arithmetically consistent | same | unit | PASS | registered − serving target = outside-target row |
| 5 | The shipped linux binary stays under 45 MiB | `.github/workflows/ci.yml` "Check the binary stays within its regression ceiling" | CI check | not run locally | darwin host cannot produce the linux figure; runs on both ubuntu matrix arches |

## Coverage and known gaps

```
$ go vet ./...                      # clean
$ gofmt -l ./cmd ./internal         # clean
$ go test ./... -count=1            # all packages ok
$ go test ./cmd/devcloud/ -cover    # coverage: 0.0% of statements
```

The 0.0% figure on `cmd/devcloud` is expected and is not a gap this cycle
introduced. That package's tests assert the binary's *published claims* — the
coverage page, the fidelity manifest, the demand set, the startup budget —
against the registry the binary ships. They exercise `internal/...` through the
registry, not `main.go`'s own statements, so a statement-coverage reading of the
package is not a meaningful number. The 80% target applies to the service and
codegen packages, which `go test ./...` covers.

Known gaps, deliberate:

- **The 45 MiB check is unverified locally.** It needs a linux build; the host is
  darwin. It will first run on this branch's CI.
- **The gate cannot catch a 2x startup regression**, only ~10x. See the reasoning
  above; a tighter budget would be flaky rather than strict.
- **`Init` is exercised with default options** (no `db_path`, no `server_port`),
  which is the first-run configuration. A service whose cost only appears under a
  non-default option is not covered.

## Merge evidence

If these commits are squashed, the RED/GREEN summary is: the startup gate timed
factory calls (84 µs / 150 ms budget) and was proven blind by an 8 ms-per-service
injection it did not notice; it now times `Init` the way `main.go` does (141 ms /
2 s ceiling) and fails at 4.0 s under the same injection. The reasoning is also
carried in the doc comment on `TestRegisteredFleetComesUpWithinItsBudget`.
