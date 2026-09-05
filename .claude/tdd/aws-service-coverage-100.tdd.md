# TDD Evidence: Repeatable AWS Service Onboarding

**Source plan**: `.claude/plans/aws-service-coverage-100.plan.md`
**Source PRD**: `.claude/prds/aws-service-coverage-100.prd.md` — Milestone 1, "Onboarding cost is known"
**Branch**: `feat/repeatable-service-onboarding`
**Commits**: `56e1867` (RED) → `9a53664` (GREEN) → `4881772` → `113ac22`

## User journeys

Extracted from the plan's tasks; none invented here.

| # | Journey | Plan task |
|---|---|---|
| J1 | As a maintainer, regenerating `imports.go` returns exactly what is committed, so the weekly sync cannot erase service registration | Task 1 |
| J2 | If `imports.go` drifts from the service directories, the build fails, so a dropped service is never silently unreachable | Task 2 |
| J3 | As a contributor, a scaffolded JSON-protocol service serves its CRUD operations immediately, with no hand-written dispatch | Task 3 |
| J4 | `auto-crud` in the manifest means the operation is actually served, so published coverage is not inflated | Task 4 |
| J5 | Adding a service needs a name in a list, not a hand-written boto3 fixture | Task 5 |
| J6 | The per-service onboarding cost is a measured number, not an estimate | Task 6 |

## Task report

### Task 1 — Repair the import generator

Ran `scripts/generate-imports.sh` against a scratch tree and compared with the committed file.

**RED** (`CGO_ENABLED=0 go test ./cmd/devcloud/ -run TestGenerateImportsReproducesCommittedFile`):

```
--- FAIL: TestGenerateImportsReproducesCommittedFile
    expected: "// Code generated ...\n// SPDX-License-Identifier: Apache-2.0\npackage main\n\nimport (\n\t_ \"...services/account\"\n ... 103 entries ...
    actual  : "// Code generated ...\npackage main\n\nimport ()\n"
```

Two defects in one output: the import block is empty, and the SPDX line is missing. The empty block is the load-bearing one — an unimported package never runs its `init()`, so every service was unregistered, and only in the built binary.

**GREEN**: gate on the package calling `DefaultRegistry.Register` instead of on a `register.go` filename no service has ever had. Supporting changes: build into a temp file and `mv` on success; refuse below `MIN_SERVICES` (default 50); delegate SPDX to `scripts/add-spdx-header.sh`.

```
$ bash scripts/generate-imports.sh && git diff --exit-code cmd/devcloud/imports.go
Generated ./cmd/devcloud/imports.go with 103 services
IDEMPOTENT: committed file reproduced exactly

$ MIN_SERVICES=200 bash scripts/generate-imports.sh; git diff --exit-code cmd/devcloud/imports.go
ERROR: found 103 registering service package(s), expected at least 200.
Refusing to write a gutted ./cmd/devcloud/imports.go; it is unchanged.
exit=1
GUARD OK: imports.go untouched after refusal
```

**Guaranteed**: the generator reproduces the committed file byte for byte, and a run that would gut the file exits non-zero without touching it.

### Task 2 — Lock `imports.go` against drift

`TestImportsCoverEveryRegisteredService` compares the registering service directories with the blank imports, both directions, behind a floor of 50.

**No RED phase** — it passes on the current tree because no drift exists today. It is a regression lock, not a reproducer. Recorded as such rather than presented as a RED→GREEN cycle.

### Task 3 — Make the scaffold meet the quality floor

**RED** (executed, `-run TestGenerateScaffold`):

```
--- FAIL: TestGenerateScaffoldOptsIntoCRUDEngine
    rendered scaffold does not contain "plugin.ErrUnhandledOp"
    (it contains: return nil, generated.ErrNotImplemented)
--- FAIL: TestGenerateScaffoldKeepsDataDir
    rendered scaffold does not contain "cfg.DataDir"
```

**GREEN**: template returns `plugin.ErrUnhandledOp`; `Init` stores `cfg.DataDir`.

One test assertion was corrected during the GREEN step: `NotContains(output, "ErrNotImplemented")` was too broad — it forbade the string anywhere, including the scaffold's own doc comment explaining why declining that way serves nothing. Tightened to the return statement (`return nil, generated.ErrNotImplemented`), which keeps the regression value and stops the assertion colliding with documentation.

**Guaranteed**: a scaffolded provider reaches the CRUD engine and keeps its data directory.

### Task 4 — Stop the manifest overstating coverage

**RED** (compile-time, the failure mode the skill permits):

```
internal/codegen/gen_fidelity_test.go:47: unknown field EngineWired in struct literal of type ProviderScan
internal/codegen/gen_fidelity_test.go:55: cannot use providers (map[string]ProviderScan) as map[string][]string
internal/codegen/scan_handverified_test.go:178: scans[id].EngineWired undefined
```

The failure is the missing contract, not unrelated breakage: `tierFor` labelled an operation `auto-crud` from CRUD-registry membership alone, and membership means *classifiable*, never *reachable*.

**GREEN**: `ProviderScan.EngineWired`, detected by a receiver-scoped AST walk for `plugin.ErrUnhandledOp` reusing the same method traversal as the operation scan; `BuildFidelityData` takes the `ProviderScan` map directly.

**Key validation — the manifest did not change.**

```
$ make codegen && git status --porcelain
(no entries under internal/generated/)
```

A byte-identical manifest proves the wiring detection agrees with all 46 engine-wired JSON services and demoted no live operation. Idempotency confirmed by hash across two consecutive runs.

### Task 5 — Remove the per-service fixture cost

Added `service_client`, one parameterized boto3 factory fixture, and `test_service_smoke.py`, which picks a parameterless `List*`/`Describe*` operation from the service model and asserts HTTP 200.

Verified in **both** directions:

```
$ pytest test_service_smoke.py
test_service_serves_at_least_one_operation[comprehend] PASSED

# after reverting the provider to generated.ErrNotImplemented:
E  Failed: comprehend.ListDatasets is routed but not served:
   {'Message': 'operation not implemented', 'Code': 'InternalError'}
FAILED test_service_serves_at_least_one_operation[comprehend]
```

The negative case exposed a second consequence of the old scaffold: it returned **HTTP 500 `InternalError`**, so a pre-fix scaffolded service also broke the project's "unimplemented operations return a clean AWS error, never a false success" guarantee.

The 104 existing named fixtures were deliberately left alone — they back real tests, not scaffolding.

### Task 6 — Measured baseline

`comprehend` (`awsJson1_1`), end to end on this machine:

| Stage | Time |
|---|---|
| model download | 0s |
| codegen + scaffold | 2s |
| import regeneration | 0s |
| build | 1s |
| **total** | **3s** |

- Hand-written lines: **0**
- Generated: 53 lines in `internal/services/comprehend/provider.go`, plus the generated package
- Manifest: 49 `auto-crud`, 36 `unimplemented`, 0 `hand-verified` — matching what the smoke test confirms is served

**This number does not hold for every service.** See "Known gaps" below.

Two existing invariants needed updating, both encoding "every service is hand-written":
`TestScanProviders` (`scan_handverified_test.go:71`) and `TestFidelityManifestCoverage` (`fidelity_test.go:58`) required at least one hand-verified operation per service. A scaffolded service legitimately has none. The invariant is now "every service serves something — hand-verified **or** engine-served", which is stronger: zero of both is the real defect, and that is what these checks now catch.

## Test specification

| # | What is guaranteed | Test | Type | Result | Evidence |
|---|---|---|---|---|---|
| 1 | Regenerating `imports.go` reproduces the committed file byte for byte | `cmd/devcloud/imports_test.go:TestGenerateImportsReproducesCommittedFile` | integration | PASS | `go test ./cmd/devcloud/` |
| 2 | Every registering service package is blank-imported, and nothing else | `cmd/devcloud/imports_test.go:TestImportsCoverEveryRegisteredService` | unit | PASS | `go test ./cmd/devcloud/` |
| 3 | A run that would gut `imports.go` exits non-zero and leaves it unchanged | manual, `MIN_SERVICES=200` | integration | PASS | output quoted above |
| 4 | A scaffolded provider opts into the CRUD engine | `internal/codegen/gen_scaffold_test.go:TestGenerateScaffoldOptsIntoCRUDEngine` | unit | PASS | `go test ./internal/codegen/` |
| 5 | A scaffolded provider keeps `cfg.DataDir` | `internal/codegen/gen_scaffold_test.go:TestGenerateScaffoldKeepsDataDir` | unit | PASS | `go test ./internal/codegen/` |
| 6 | The scan reports which providers reach the CRUD engine | `internal/codegen/scan_handverified_test.go:TestScanProvidersDetectsEngineWiring` | unit | PASS | `go test ./internal/codegen/` |
| 7 | An unwired provider's CRUD-shaped operations are `unimplemented`, not `auto-crud` | `internal/codegen/gen_fidelity_test.go:TestBuildFidelityDataRequiresEngineWiring` | unit | PASS | `go test ./internal/codegen/` |
| 8 | Hand-verified outranks both other tiers regardless of wiring | `internal/codegen/gen_fidelity_test.go:TestBuildFidelityDataHandVerifiedBeatsWiring` | unit | PASS | `go test ./internal/codegen/` |
| 9 | Every routed service serves at least one operation | `cmd/devcloud/fidelity_test.go:TestFidelityManifestCoverage` | unit | PASS | `go test ./cmd/devcloud/` |
| 10 | A scaffolded service answers a real boto3 call with 200 | `tests/compatibility/test_service_smoke.py` | e2e | PASS | `make test-compat` |
| 11 | A service that is routed but serves nothing fails the smoke test | same test, provider temporarily reverted | e2e | PASS (fails as designed) | output quoted above |

## Coverage

```
$ go test -cover ./internal/codegen/
ok  internal/codegen  coverage: 77.7% of statements

$ go tool cover -func
BuildFidelityData   100.0%
tierFor             100.0%
sortedKeys          100.0%
GenerateScaffold    100.0%
wiresCRUDEngine     100.0%
ScanProviders        76.9%
```

Every function changed in this run is at 100% except `ScanProviders` (76.9%; the gap is filesystem error paths). `cmd/devcloud` reports 0.0% because it is `package main` whose tests exercise generated data rather than `main`'s statements — pre-existing, not introduced here.

Full suite:

```
$ CGO_ENABLED=0 go test ./...        → 110 packages ok, 0 failures
$ make test-compat                   → 776 passed in 9.47s  (775 baseline + 1 smoke)
$ make codegen (twice, hashed)       → IDEMPOTENT
```

## Known gaps

1. **The 3-second figure is the clean case, not the average.** `rekognition` was attempted as the plan's second measurement subject and **failed**: `UnknownService: rekognitionservice`. Its `X-Amz-Target` prefix is `RekognitionService`, and `normalizeServiceID` (`internal/gateway/protocol.go:124`) is a hand-maintained alias map — **63 case clauses covering ~150 alias strings for 103 services**. Roughly half of all services needed a hand-written entry in a gateway switch. `comprehend` only worked because its prefix is `Comprehend_20171127`, which the existing date-suffix logic already strips. The alias is derivable from the model (the service shape name plus `aws.api#service.arnNamespace`), but a blanket "strip `service` suffix" rule is unsafe — `elasticsearchservice` is a real service ID. Resolving this is a scope decision, recorded and awaiting the user.
2. **`rekognition` artifacts were removed**, not left half-added: shipping a registered service that answers nothing would violate the PRD's own quality floor. Reproducible in ~5s with the rollback command in the commit history.
3. **The CRUD engine is narrower than "JSON-family".** `crud.JSONProtocol` (`internal/shared/crud/crud.go:71`) matches only protocols starting with `json`, so `rest-json` services **cannot** be engine-served. The plan overstated this as "non-JSON protocols (Query / EC2 Query / REST-XML) are out of scope". Locally only 39 of 93 models (42%) are `awsJson1_0`/`awsJson1_1`; `rest-json` adds another 27 that are equally unreachable. This narrows Milestone 4's addressable set and should be corrected in the plan and PRD.
4. **No changelog entries.** `.changie.yaml` requires a `Custom.Issue` integer used to build the CHANGELOG link, and the convention in `changes/unreleased/` is to use the PR number, which does not exist yet. Deliberately not guessed.
5. **No separate RED checkpoint commit for Tasks 3–4.** The repo's `go-vet` pre-commit hook refuses a tree that does not compile, and the Task 4 RED *was* a compile failure. Committing it would have required `--no-verify` and left a commit that breaks CI for anyone checking it out. The RED output is preserved verbatim in commit `4881772` and above.

## Merge evidence

If these four commits are squashed, this file is the surviving record. Summary for the PR body:

- **RED**: `imports.go` regenerated as `import ()` (empty); scaffold returned `generated.ErrNotImplemented` and dropped `cfg.DataDir`; `ProviderScan`/`BuildFidelityData` had no notion of engine wiring, so `tierFor` labelled `auto-crud` from registry membership alone.
- **GREEN**: 11 guarantees above; `go test ./...` 110 packages; `make test-compat` 776 passed; `make codegen` produces **no manifest diff**, proving the wiring detection demoted no live operation.
- **Refactor**: none beyond the assertion correction noted in Task 3; the changes were small enough that no separate refactor pass was warranted.
