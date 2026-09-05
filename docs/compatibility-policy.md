# Compatibility Policy

What DevCloud **v1.0** promises, and what it deliberately does not.

This document covers the surfaces you touch as a *user* of DevCloud — the config file, the
environment, the CLI, the admin API, and the AWS wire protocol. For the in-tree Go contract
that service implementations are written against, see
[plugin-api.md](plugin-api.md#api-stability); that surface lives under `internal/` and is not
importable from another module.

Versions follow [Semantic Versioning](https://semver.org). "Across 1.x" below means every
release from v1.0.0 up to but not including v2.0.0.

## Guaranteed across 1.x

### Configuration file

These keys keep their name, type and meaning. New keys may be added; existing ones are not
removed or repurposed. Defined in [`internal/config/config.go`](../internal/config/config.go).

| Key | Type | Meaning |
|---|---|---|
| `server.port` | int | Listen port. Default `4747` when absent or `0`. |
| `services` | map | Presence of the block is authoritative — only the services it lists run, and an empty block runs nothing. Absent means every registered service runs. |
| `services.<id>.enabled` | bool | Whether that service starts. |
| `services.<id>.data_dir` | string | Where that service stores data. |
| `providers.aws.services` | map | The same block as `services`, namespaced by provider. Identical semantics; wins over `services` when both are written, with a warning. |
| `admin.enabled` | bool | Whether the admin API is served. Default `false`. |
| `logging.level` | string | Log level. |
| `logging.format` | string | Log format. |

### Environment variables

| Variable | Meaning |
|---|---|
| `DEVCLOUD_PORT` | Overrides `server.port`. |
| `DEVCLOUD_SERVICES` | Names the running service set. `all`, a comma-separated list of service ids, or the `tier1` / `tier2` / `tier3` shortcuts. Unknown tokens are treated as literal service names. |
| `DEVCLOUD_DATA_DIR` | Base directory; each service stores under `<base>/<id>`. Overrides `data_dir`. |

Environment overrides config file, and that precedence is guaranteed.

When `DEVCLOUD_SERVICES` names services, it decides membership on its own: it
starts a service the `services` block omits and stops one the block enables. It
replaces the block's selection rather than intersecting with it. The literal
`all` is the exception — it switches the filter off and hands the decision back
to the `services` block, which is what "all" has always meant here
([configuration.md](configuration.md#devcloud_services)).

### Command line

`-config <path>` keeps its meaning. With no flag, DevCloud uses `./devcloud.yaml` if present
and the embedded defaults otherwise — zero-config startup keeps working.

### Admin API

Served at `/devcloud/api/` when `admin.enabled: true`. These routes keep responding, and their
JSON responses **only gain fields** — no documented key is removed or repurposed.

| Route | Guaranteed response keys |
|---|---|
| `GET /devcloud/api/services` | array of `id`, `name`, `status`, `resourceCount` |
| `GET /devcloud/api/services/{id}/resources` | array of `type`, `id`, `name` |
| `GET /devcloud/api/logs` | array of `method`, `path`, `status`, `duration`, `timestamp`, `service`; newest first; `?limit=` honoured |
| `GET /devcloud/api/fidelity` | object keyed by service id, each with `modelBacked` and `counts`; `?service=<id>` adds `operations` |

### Fidelity tier names

`hand-verified`, `auto-crud` and `unimplemented` keep the meanings given in
[fidelity-manifest.md](fidelity-manifest.md). The set does not shrink, and a name is never
reused for a different meaning.

Every operation the manifest lists carries a tier from that set, every registered service
appears, and every operation the CRUD engine serves is present and not filed as
`unimplemented` — all three fail the build, in
[`cmd/devcloud/fidelity_test.go`](../cmd/devcloud/fidelity_test.go).

What no test can catch is an operation that never reaches the manifest at all, and that is
bounded rather than eliminated: for the 93 services with an in-tree Smithy model the operation
universe comes from the model, so an operation losing its implementation reclassifies to
`unimplemented` instead of disappearing. For the 11 without one, the universe *is* what the
providers serve, so the manifest lists no unimplemented tail for them — `modelBacked` on
`GET /devcloud/api/fidelity` reports which is which.

### Wire behaviour — scoped to the compatibility suite

**Whatever a test in [`tests/compatibility/`](../tests/compatibility/) asserts about a response
keeps holding across 1.x — that property, and nothing wider.**

The promise is as wide as each individual assertion: not as wide as the field, and not as wide as
the operation. `CreateFunction` is covered by `test_lambda.py`, and its two assertions are worth
reading closely:

- `FunctionName` is asserted **equal** to the name that was sent, so its key and its value are
  both promised.
- `FunctionArn` is asserted only to be **present**, so its presence is promised and its type,
  format and meaning are not. If it stopped being ARN-shaped the suite would stay green — so this
  policy does not promise it stays ARN-shaped.
- `Runtime`, `Handler` and `MemorySize` are not asserted at all, so they carry no promise even
  though today's response includes them.

That narrowness is the point: it is the promise the repo can actually keep. The suite — 775 tests
driving real boto3 clients — runs in CI on every push and again against the tagged commit before
a release publishes, so breaking an assertion fails the build rather than depending on review
discipline. Anything the suite does not assert rests on nothing but intent. Widening the promise
means adding or strengthening assertions, and such contributions are welcome.

## Not guaranteed

Depending on any of the following will break, and breaking it is **not** a major-version event.

- **`auto-crud` response content.** 948 operations are served by the
  [generic CRUD engine](crud-engine.md) at fidelity that is deliberately *plausible, not
  faithful*: store-backed responses echoing your input plus synthesized ids and ARNs, with no
  validation, no cross-resource integrity, no pagination correctness and no business logic.
  Their shape and content may change in any release. Use them to wire an SDK up, nothing more.
- **Hand-verified operations with no compatibility test.** Of 4,496 hand-verified operations,
  only what the suite covers is promised. The rest are best-effort.
- **Data durability.** Stores are local development stores. Several are in-memory and
  per-process; on-disk layouts under `data_dir` may change format between releases without a
  migration. Do not treat DevCloud as a database.
- **`unimplemented` → served transitions.** An operation that returns an error today may start
  returning a response. This is additive, and ships in a minor release.
- **Service coverage.** New services may be added in a minor release. The 104 services present
  at v1.0 are a floor, not a ceiling.
- **Error codes, HTTP status and message wording.** What *is* guaranteed for an `unimplemented`
  operation is that it **fails** — an AWS-shaped error, never a fabricated success. Which error
  is not: it comes from whichever provider handles the request, so it is `InvalidAction` (400)
  for services that fall through to the CRUD engine, `NotImplemented` (501) for the 33 providers
  with their own dispatch default, and each path-routed provider's own vocabulary otherwise
  (`s3` `MethodNotAllowed` 405, `bedrock` `UnsupportedOperation` 400). `sqs` even differs by
  protocol. [fidelity-manifest.md](fidelity-manifest.md) records the current behaviour;
  normalizing it is a minor release, not a major one.
- **Log output.** Format, levels and wording of server logs are operational, not an API.
- **Everything under `internal/`.** Go forbids importing it from another module, and DevCloud
  reserves the right to restructure it freely across 1.x. The Phase 2 refactor on the
  [roadmap](roadmap.md) is the precedent: the intermediate representation, `ModelSource`,
  `ProviderScoped` and the `auth` adapters all landed inside a 1.x minor without a
  compatibility event, because none of them is reachable from outside this module. Internal
  churn is not a compatibility event. The in-tree `ServicePlugin` contract in
  [plugin-api.md](plugin-api.md#api-stability) is not an exception to this: it is a convention
  that keeps in-tree plugins compiling, and it does not gate release versioning.
- **Behavioural parity with AWS.** No release of DevCloud promises AWS's validation, business
  logic, eventual-consistency timing, rate limits, or IAM enforcement. Credentials are accepted
  without signature verification.

## Deprecation procedure

Removing anything from the guaranteed list is a **major** version bump. Before that can happen:

1. **Deprecate in a minor release.** The old form keeps working and emits a runtime warning
   naming its replacement. The precedent is the `dashboard` → `admin` config rename: the old key
   still enables the admin API, warns, and yields to an explicit `admin` block
   ([`config.go`](../internal/config/config.go)).
2. **Document it** — in the release notes for that version, and here.
3. **Remove no earlier than the next major.** At least one released version must have shipped
   the warning.

Silence is not deprecation. A removed key that YAML would otherwise drop without comment is
kept in the parser purely to warn — that is why `auth` still produces a message telling you
SigV4 is not enforced rather than being ignored.

The pre-flight checklist in [release.md](release.md#pre-flight-checklist) makes this a step in
cutting a release, not a thing to remember.

## Reporting a break

If a 1.x release breaks something on the guaranteed list, that is a bug — please
[open an issue](https://github.com/skyoo2003/devcloud/issues) with the DevCloud version and a
reproducing snippet. If it breaks something on the not-guaranteed list, an issue is still
useful: it is evidence for tightening the policy in a future major.
