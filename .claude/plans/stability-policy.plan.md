# Plan: v1.0 Stability Policy

**Source PRD**: `.claude/prds/aws-v1-stabilization.prd.md`
**Selected Milestone**: 2 — Stability policy published
**Complexity**: Small (half a day)

> Refreshed 2026-08-09 against the tree at `6d5e66b`, after #126/#127/#128 landed.
> The earlier draft of this plan predated them and cited stale counts and a stray
> `.changes/` tree that #127 removed.

## Summary

Write down what v1.0 promises across the **surfaces users actually touch**, and lock the
promise with tests that fail on drift. The repo already documents a v1.x guarantee, but it
covers the wrong thing.

## The finding that shapes this plan

`docs/plugin-api.md:128-139` is the only stability policy in the repo, and it guarantees
`ServicePlugin`, `PluginConfig`, `Response`, `Resource` and the `ProtocolType` constants
across v1.x. Every one of those lives in `internal/plugin` — and the same document says so
at `docs/plugin-api.md:7-9`: *"The package lives under `internal/` for that reason."*

Go forbids importing `internal/` from another module. **The only documented v1.0 guarantee
therefore applies to nobody.** It is worth keeping — it constrains in-tree contributors —
but it is not the promise a user of v1.0 needs.

The surfaces a user is actually exposed to, none of which carry a policy today:

| Surface | Inventory (verified in tree) |
|---|---|
| HTTP wire behaviour | AWS API on port 4747; 775 boto3 compat tests — `docs/services-matrix.md:3` |
| Config file | `server.port`, `services.<id>.{enabled,data_dir}`, `admin.enabled`, `logging.{level,format}` — `internal/config/config.go:27-73` |
| Config, deprecated | `dashboard.enabled` (warns, still honoured) — `config.go:33-37,140-145` |
| Config, removed | `auth.enabled` (warns, ignored) — `config.go:39-43,153-160` |
| Environment | `DEVCLOUD_PORT`, `DEVCLOUD_SERVICES`, `DEVCLOUD_DATA_DIR` — `config.go:227-237` |
| CLI | `-config` — `cmd/devcloud/main.go:33` |
| Admin API | `/devcloud/api/{services,services/,logs,fidelity}` — `internal/admin/api.go:34-37` |
| Fidelity tiers | `hand-verified` / `auto-crud` / `unimplemented` as names with meaning — `docs/fidelity-manifest.md` |

### The second finding: the admin snapshot tests do not snapshot anything

`internal/admin/api_test.go:75` decodes the response into the internal `serviceInfo` struct.
Renaming a JSON tag renames it on both sides of the assertion, so the test stays green while
every consumer breaks. Same for `TestAPI_Fidelity`. Task 3 exists because of this.

## Resolved decisions

**Wire-behaviour guarantee is compat-suite-scoped** (PRD open question #2, and this plan's
prior open decision). 1.x guarantees the response shape of hand-verified operations *that
`tests/compatibility/` covers*. The suite is the enforcement; prose is not. Hand-verified
operations with no test get best-effort language, not a promise. This keeps the Phase 2
IR/`ModelSource` refactor unblocked, which is the risk the PRD flags as Medium/High.

**Service coverage additions are minor-version, not breaking** (PRD open question #3).
Adding a service or promoting `unimplemented` → served only adds surface.

**Fidelity manifest ships as both docs and a runtime endpoint** (PRD open question #4).
Already true: `docs/fidelity-manifest.md` + `/devcloud/api/fidelity`. The policy documents
it rather than deciding it.

## Patterns to Mirror

| Category | Source | Pattern |
|---|---|---|
| Policy prose | `docs/plugin-api.md:128-139` | Bulleted guarantee, then "any breaking change requires a major bump" |
| Deprecation in practice | `internal/config/config.go:140-145` | Honour the old key one release, warn, document the migration |
| Drift test | `cmd/devcloud/fidelity_test.go:24-50` | Closed set + conservative floor, `t.Errorf` per violation so all failures surface |
| Docs index | `docs/README.md:7-12` | One line per doc with a "what it is" clause |

## Files to Change

| File | Action | Why |
|---|---|---|
| `docs/compatibility-policy.md` | CREATE | The v1.0 promise: guaranteed, not guaranteed, deprecation procedure |
| `internal/config/config_test.go` | UPDATE | Snapshot guaranteed config keys + env vars — removing one fails the build |
| `internal/admin/api_test.go` | UPDATE | Snapshot route set and response keys via `map[string]any`, not the internal struct |
| `docs/plugin-api.md` | UPDATE | Scope its guarantee to in-tree contributors; link the user-facing policy |
| `docs/README.md` | UPDATE | Index the new doc; also fix "101 services" → 104 and add the missing `plugin-api` / `crud-engine` / `fidelity-manifest` rows |
| `README.md` | UPDATE | Index the new doc in the `docs/` quick-links block (lines 60-67) |
| `docs/release.md` | UPDATE | Resolve the dangling cross-link the pre-flight checklist expects — closes M4's residual |
| `changes/unreleased/Added-*.yaml` | CREATE | Changie fragment with a non-empty `Issue` (`release.yml:225` rejects otherwise) |

## Tasks

### Task 1: Write the policy
- **Action**: `docs/compatibility-policy.md` with three sections.
  - **Guaranteed across 1.x** — config keys and env vars keep their meaning; admin API
    responses only gain fields, never remove or repurpose one; the three tier names keep
    their meaning; hand-verified operations covered by `tests/compatibility/` keep their
    response shape; `-config` keeps its meaning.
  - **Not guaranteed** — `auto-crud` response *content* (948 operations, "plausible, not
    faithful", per `docs/crud-engine.md`); hand-verified operations with no compat test;
    in-memory/on-disk store durability across restarts and versions; `unimplemented` →
    served transitions; new service coverage; error message wording; anything under
    `internal/`, explicitly including the Phase 2 IR and `ModelSource` work.
  - **Deprecation procedure** — one-release overlap with a runtime warning before removal,
    mirroring `dashboard` → `admin` (`config.go:140-145`) and the removed `auth` key
    (`config.go:153-160`); removal without that overlap is a major bump.
- **Mirror**: the bulleted-guarantee shape of `docs/plugin-api.md:128-139`.
- **Validate**: every claim traceable to a file:line in the inventory table above. No promise
  the repo has no mechanism to keep.

### Task 2: Lock the config surface
- **Action**: table-driven test asserting each guaranteed key parses onto the expected field,
  each of the three env vars overrides as documented, and the deprecated/removed keys still
  warn. One case per guaranteed surface element.
- **Mirror**: `TestLoadConfig_DefaultFile` (`config_test.go:116`), `TestParse_DeprecatedDashboardKey`
  (`:200`), `TestParse_RemovedAuthKeyWarns` (`:213`).
- **Validate**: delete a field from `Config` → the test fails to compile or asserts false.

### Task 3: Lock the admin surface
- **Action**: assert the exact route set (4 paths), and decode each response into
  `map[string]any` to assert the documented **keys are present** — not into the internal
  struct, which is what makes the current tests blind to renames.
- **Mirror**: the request/recorder setup of `TestAPI_Services` (`api_test.go:54`); the
  per-violation `t.Errorf` style of `fidelity_test.go`.
- **Validate**: rename a JSON tag in `internal/admin/api.go` → test fails. It does not today.

### Task 4: Cross-link and publish
- **Action**: scope `docs/plugin-api.md:128` to in-tree contributors and link the new policy;
  add the policy to both index files, correcting `docs/README.md`'s stale service count and
  missing rows while there; cross-link from `docs/release.md`'s pre-flight checklist, which
  M4 wrote expecting this file; add the Changie fragment.

## Validation

```bash
CGO_ENABLED=0 go test ./internal/config/ ./internal/admin/ ./cmd/devcloud/
CGO_ENABLED=0 go test ./... && make test-compat
golangci-lint run ./...

# the fragment must satisfy the release gate
changie batch v9.9.9 --dry-run | grep -c 'issues/)'   # 0

# no dead links in the new doc
grep -o '](\.\./[^)]*' docs/compatibility-policy.md | cut -d'(' -f2 | while read -r p; do
  [ -e "docs/$p" ] || echo "dead: $p"
done
```

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Policy over-promises and blocks the Phase 2 refactor | Medium | Guarantee user-facing surfaces only; state in writing that `internal/`, the IR layer and `ModelSource` are unconstrained across 1.x |
| "hand-verified keeps its response shape" is stronger than the code can hold | Medium | Resolved above — scoped to what the 775-test compat suite covers; the suite is the enforcement |
| Snapshot tests become churn on every additive change | Low | Assert key *presence*, never full payload equality — additive change stays green by construction |
| `docs/README.md` fixes balloon the diff | Low | Limit to the service count and the three missing rows; anything else is a separate issue |

## Acceptance

- [x] `docs/compatibility-policy.md` states guaranteed / not-guaranteed / deprecation per surface
- [x] Wire guarantee is compat-suite-scoped and says so
- [x] `plugin-api.md`'s guarantee is scoped to in-tree contributors
- [x] Renaming an admin JSON tag or dropping a config key fails the build — reproduced both:
      `json:"resourceCount"` → `resource_count` and `yaml:"format"` → `fmt`, each failing with a
      message naming the policy, then reverted
- [x] `docs/release.md`'s pre-flight cross-link resolves — M4 residual closed
- [x] `CGO_ENABLED=0 go test ./...` green; boto3 suite 775 passed
- [x] Changie fragment present with a non-empty issue number (`#129`); dry batch renders 29
      entries, 0 dead issue links

## Follow-on (not this plan)

M4 is otherwise complete (#127). After this lands, the only remaining milestone is **M5 —
tag v1.0**, which is the pre-flight checklist at `docs/release.md:38-65`, then
`make changelog VERSION=v1.0.0` and a tag push.
