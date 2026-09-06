# Coverage

Three numbers describe DevCloud's AWS surface, and they mean different things.
The service count alone is not a capability claim, so this page never states it
alone.

| Number | What it means | Today |
|---|---|---|
| **Registered** | The gateway routes the service, so the call reaches DevCloud instead of real AWS. | **205** |
| **Serving ≥1 operation** | At least one operation returns a real, store-backed answer. | **201** |
| **Registered-only** | Routed, but every operation declines with a clean AWS error. | **4** |
| **Compatibility-tested** | A boto3 test exercises the service in CI and passes. | **203** |

Per operation, from the [fidelity manifest](fidelity-manifest.md):

| Tier | Operations |
|---|---|
| `hand-verified` | 4,497 |
| `auto-crud` | 5,193 |
| `unimplemented` | 2,717 |
| **total known** | **12,407** |

> **The coverage target is 205 services, not 431.** It was 431, and the evidence
> did not support it — see [The target](#the-target).

Every figure on this page is asserted against the binary by
`go test ./cmd/devcloud/`. Editing one here without the code moving fails CI, and
so does the reverse. See [Reproducing these numbers](#reproducing-these-numbers).

## Why a registered service can serve nothing

The generic [CRUD engine](crud-engine.md) needs two things: to know which
operation a request is for, and to recognise that operation as CRUD-shaped. Only
the second still stops it.

**The protocol always says which operation**, and the engine reads every form:

| Protocol | Services | Operation name comes from |
|---|---|---|
| `rest-json` | 93 | HTTP method + path (`internal/shared/httproute`) |
| `json-1.1` | 64 | the `X-Amz-Target` header |
| `json-1.0` | 17 | the `X-Amz-Target` header |
| `query` | 15 | the `Action` form field |
| `rest-xml` | 4 | HTTP method + path |
| no in-tree model | 12 | n/a — hand-written providers |

**The operation is not CRUD-shaped.** `GetThing`, `ListThings` and `CreateThing`
map onto a generic store. `ExecuteStatement`, `InvokeEndpoint` and
`QueryForecast` do not, and the engine refuses them rather than inventing an
answer. This is now the *only* reason a service serves nothing, and it applies to
exactly four: `forecastquery`, the two SageMaker Runtime variants
`sagemaker-runtime` and `sagemakerruntimehttp2`, and `rds-data`. No protocol
change reaches them.

Registering a service the engine cannot serve is deliberate. The alternative is
worse: an *unregistered* service is not routed, so the SDK call leaves the
machine and bills a real AWS account. A registered-only service answers locally,
in AWS's own error vocabulary. What it must never do is fabricate a success —
`tests/compatibility/test_service_smoke.py::test_registered_only_service_declines_cleanly`
is the check that keeps that true.

Engine-*servable* is not the same as engine-*served*. The engine is entered only
when a provider returns `plugin.ErrUnhandledOp`, so a hand-written provider that
refuses unknown operations itself (`apigatewayv2`, `xray`) never reaches it. The
manifest records this per service as `EngineWired`.

## Why compatibility-tested is 203, not 205

`tests/compatibility/test_service_smoke.py` parametrises over the generated
service list rather than a hand-written one, so a service cannot be registered
and quietly go untested — which is what 31 of them were until this was measured.

Two are excluded, and they are not a backlog: botocore publishes no client for
`sagemakerruntimehttp2` or `transcribestreaming` (`sagemaker-runtime` and
`transcribe` are different clients with different APIs). No boto3 test can exist
for a client that does not exist. That is a property of the AWS SDK, and it is
the ceiling on this number.

## Contested signing names

A runtime or control-plane split-out signs with its parent's name, so `sagemaker`
is claimed by eight services at once. Where exactly one claimant carries the name
as its own service ID, that service wins and the rest are borrowers — see
`codegen.BuildAliases`. Where none does, the request is handed to the group and
answered by the sibling whose route table models its method and path.

All four Lex clients sign as `lex` and none is named `lex`, so: `GET /bots` is
`lex-models`, `POST /bots` is `lexv2-models`, `/bot/…/session` is `lex-runtime`,
`/bots/…/botAliases/…/sessions/…` is `lexv2-runtime`. One route is claimed by two
siblings — `DeleteBot` at `DELETE /bots/{id}` — and it is refused rather than
guessed: deleting the wrong bot is worse than an honest error.

## What counts as a service

Upstream publishes 431 model files, and DevCloud counts **model files**, one
registered service each — because that is what an SDK client selects.
`boto3.client("sagemaker-runtime")` is a different client with a different API
from `boto3.client("sagemaker")`. The split-outs are real choices a caller makes,
not packaging artefacts.

## The target

**Decided 2026-09-05. The target is 205 services, not 431 — and it is met.**

The old target was every service AWS publishes. It rested on an assumption nobody
had tested: that the services DevCloud does not register are services anyone
wants. Before committing to building ~283 of them, the assumption was tested
against three independent projects that each only add a service when someone
asks. It did not hold.

**The rule was fixed before the numbers were seen** — four outcomes written down
in advance, including one for "the method itself failed", specifically so the
result could not be argued into whichever answer was most convenient. Full
evidence in [demand.md](demand.md); re-derive with
`python3 scripts/demand_rank.py`.

| Reading | Value |
|---|---|
| Missing services (`M`) | 283 |
| `M` built by all three projects | 8 |
| `M` built by at least two | **57 (20.1%)** |
| `M` built by exactly one | 111 |
| `M` built by none | 115 |
| DevCloud's own service requests, all time | **0** |

The rule kept the 100% target only if ≥60% of `M` had support ≥2, and narrowed to
a demand set if ≥100 did. 57 cleared neither bar, so the pre-registered
consequence applied: **the 100% claim is dropped and the published target becomes
the demand set.** All 57 are registered — 56 serve at least one operation, and
`rds-data` is the exception named above. It is supported by all three projects,
the strongest signal in the set, and still cannot be served generically. Breadth
does not reach every service, and saying so is cheaper than a fabricated success.

| | Services |
|---|---|
| Registered today | **205** |
| Target: registered + demonstrated demand | **205 — met** |
| Explicitly not targeted | 226 |

Four fifths of the AWS surface DevCloud does not cover is surface that three
projects with far more history and staffing have collectively declined to build.
That is what a long tail looks like. The 226 are not refused — any of them can be
onboarded when someone asks. What changed is that they are no longer work
DevCloud has promised.

**What this verdict is not.** The three sources measure *emulator and provider
effort*, not user demand. They are a proxy, chosen because DevCloud had no
measurement of its own and the alternative was an unbounded wait. The instrument
below accrues the real thing.

## Service not supported?

DevCloud counts what it was asked for and could not route. Enable the admin API
(`admin.enabled: true`) and read:

```bash
curl -s localhost:4747/devcloud/api/unrouted | jq .
```

```json
{
  "services": [
    { "serviceId": "appflow", "count": 2,
      "firstSeen": "2026-09-05T17:54:25+09:00",
      "lastSeen": "2026-09-05T17:54:40+09:00" }
  ],
  "maxServiceIds": 1000,
  "droppedServiceIds": 0
}
```

Paste that into a [service request](https://github.com/skyoo2003/devcloud/issues/new?template=service_request.yml).
One report outranks all three proxies, because it is demand rather than a
stand-in for it — and it is what moves a service from the 226 into the target.

Two ceilings, so the number is not read as more than it is:

1. **It is a floor, not a census.** `gateway.DetectProtocol` classifies a request
   it cannot identify as `("rest-xml", "s3")`, and S3 is registered — so an
   unrecognisable request is routed to S3 rather than counted as a miss. The case
   that matters is caught: a real SDK or CLI call to an unregistered service
   signs with that service's own name and misses the registry. Verified with
   boto3 — `boto3.client("appflow")` is recorded as `appflow`.
2. **A non-zero `droppedServiceIds` means the list is incomplete.** Service IDs
   come from caller-controlled headers, so the collector caps how many distinct
   ones it holds and reports both the cap and what it dropped, rather than
   growing without bound or truncating in silence.

The counts live in memory and reset when the process does. Nothing is sent
anywhere.

## Runtime cost

Apple Silicon, `CGO_ENABLED=0`, measured at each step of the roadmap:

| | 105 services | 147 services | 205 services |
|---|---|---|---|
| Binary | 30.8 MiB | 31.3 MiB | **33.1 MiB** |
| Peak RSS (`/usr/bin/time -l`) | 57.3 MiB | 57.7 MiB | **43.7 MiB** |
| Service registration | — | — | **49 ms for all 205** |

The single-binary, zero-config property holds at the target with room to spare.
Startup does not scale meaningfully with service count: registration is a map
insert per service in `init()`, and the generated type definitions are mostly
dead-code-eliminated by the linker, which is why 58 more services cost 1.8 MiB.

The RSS readings were taken on different days and are not a controlled
comparison. Read them as "memory is not the constraint at 205" rather than as a
saving — what they agree on is the shape: memory is dominated by the runtime and
the store, not by how many services are registered.

## Keeping up with upstream

The 205 services are vendored from 194 Smithy models, and AWS keeps changing
them. A [weekly workflow](../.github/workflows/smithy-sync.yml) refreshes all of
them and opens a pull request. What that review costs was measured once, on
**2026-09-06**:

| Reading | Value |
|---|---|
| Vendored models refreshed | 194 |
| Models that changed | 93 |
| Of those, models that added or removed an operation | 32 |
| Of those, models that changed only documentation | 0 |
| Net change in known operations | 12,407 to 12,660 |
| Generated files that moved | 134 |
| Wall-clock to download 194 models | 1 min 53 s |

**This is one sample, and it is not one week of churn.** The 93 models that
changed were all vendored 141 days earlier; the other 101 were vendored the day
before, and not one of them changed. So the reading is an accumulated backlog,
and the weekly rate is still unknown.

What the sample does settle is the *shape* of the work. None of the 93 was
documentation-only, so no sync can be waved through on the assumption that AWS
only reworded things. Thirty-two services gained operations — `ec2` alone gained
46 — which moves the manifest and makes the published-figure gate fail on
purpose. That failure *is* the review: the numbers here have to be re-derived, by
a person, before the sync can merge. The PR body states which operations moved;
re-derive it with `python3 scripts/model_churn.py --upstream`.

## Reproducing these numbers

```bash
make codegen             # regenerate the manifest from the models
make stats               # registered services and hand-written operations
go test ./cmd/devcloud/  # asserts every number on this page against the binary
make test-compat         # the compatibility-tested number, over all 205 services
```

Every figure comes from `internal/generated/fidelity/manifest_gen.go` and nothing
else, so it cannot drift from what the binary serves. It cannot drift from *this
page* either — five tests compare the two, in both directions:

| Test | Gates |
|---|---|
| `TestPublishedCoverageMatchesTheBinary` | the three service counts |
| `TestPublishedOperationTiersMatchTheManifest` | the four operation tiers |
| `TestRegisteredOnlyServicesAreNamedInTheDocs` | that a service serving nothing is named here |
| `TestOtherDocsQuoteTheSameFigure` | that `README.md` and `docs/README.md` agree |
| `TestDemandSetIsRegistered` | that all 57 demand-set services are still registered |
