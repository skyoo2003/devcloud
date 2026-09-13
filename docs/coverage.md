# Coverage

Three numbers describe DevCloud's AWS surface, and they mean different things.
The service count alone is not a capability claim, so this page never states it
alone.

| Number | What it means | Today |
|---|---|---|
| **Registered** | The gateway routes the service, so the call reaches DevCloud instead of real AWS. | **431** |
| **Serving ≥1 operation** | At least one operation returns a real, store-backed answer. | **426** |
| **Registered-only** | Routed, but every operation declines with a clean AWS error. | **5** |
| **Compatibility-tested** | A boto3 test exercises the service in CI and passes. | **426** |

Per operation, from the [fidelity manifest](fidelity-manifest.md):

| Tier | Operations |
|---|---|
| `hand-verified` | 4,528 |
| `auto-crud` | 10,871 |
| `unimplemented` | 3,802 |
| **total known** | **19,201** |

> **Two targets, not one: routing is 431 of 431, depth is 205.** Every service
> AWS publishes is registered, so no call can leave for a billable account — that
> is a safety property and it admits no smaller number. Depth is the separate,
> smaller promise, and the evidence still puts it at 205. See
> [The target](#the-target).

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
| `rest-json` | 251 | HTTP method + path (`internal/shared/httproute`) |
| `json-1.1` | 100 | the `X-Amz-Target` header |
| `json-1.0` | 48 | the `X-Amz-Target` header |
| `query` | 15 | the `Action` form field |
| `rest-xml` | 4 | HTTP method + path |
| no in-tree model | 12 | n/a — hand-written providers |
| unrecognised protocol | 1 | n/a — `partnercentralrevenuemeasurement` is `rpcv2Cbor` |

**The operation is not CRUD-shaped.** `GetThing`, `ListThings` and `CreateThing`
map onto a generic store. `ExecuteStatement`, `InvokeEndpoint` and
`QueryForecast` do not, and the engine refuses them rather than inventing an
answer. This applies to four services: `forecastquery`, the two SageMaker Runtime
variants `sagemaker-runtime` and `sagemakerruntimehttp2`, and `rds-data`. No
protocol change reaches them.

**The protocol is one the parser does not read.** This applies to exactly one
service, and it is a different failure from the four above.
`partnercentralrevenuemeasurement` speaks `smithy.protocols#rpcv2Cbor`, which
`internal/codegen/parser.go` does not recognise, so *none* of its operations is
classified — not because their names are unshaped, but because the model never
reached the classifier. Teaching the parser a sixth protocol would reach it; no
amount of CRUD-shaping would.

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

## Why compatibility-tested is 426, not 431

`tests/compatibility/test_service_smoke.py` parametrises over the generated
service list rather than a hand-written one, so a service cannot be registered
and quietly go untested — which is what 31 of them were until this was measured.

Five are excluded, and they are not a backlog. In every one it is botocore, not
DevCloud, that stops the request, so no answer DevCloud could give would change
the outcome. Each stays registered: the call is still answered locally instead
of reaching a billed AWS account.

**No client exists** (2). botocore publishes none for `sagemakerruntimehttp2` or
`transcribestreaming` (`sagemaker-runtime` and `transcribe` are different clients
with different APIs). No boto3 test can exist for a client that does not exist.

**The client exists but cannot be pointed at localhost** (3). `codecatalyst`
authenticates with a bearer token rather than SigV4, so botocore raises
`NoAuthTokenError` before the request is built. `cloudfront-keyvaluestore`
resolves its endpoint from a KVS ARN and so never honours `endpoint_url`.
`partnercentralrevenuemeasurement` decodes every reply with botocore's CBOR
parser, and DevCloud has no CBOR encoder, so even a clean decline reads as a
corrupt frame — see the protocol table above.

Both sets are pinned in `tests/compatibility/_coverage.py` and asserted, so
adding a sixth is a deliberate edit that moves this figure with it.

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

Three data planes are separated the same way without holding a single
CRUD-classifiable operation: `payment-cryptography-data`, `cloudsearch-domain`
and `kinesis-video-webrtc-storage` declare their own route tables through
`crud.RegisterRoutes`, so route matching tells them apart from the neighbour
whose signing name they borrow. No override names a winner — the model does.

## What counts as a service

Upstream publishes 431 model files, and DevCloud counts **model files**, one
registered service each — because that is what an SDK client selects.
`boto3.client("sagemaker-runtime")` is a different client with a different API
from `boto3.client("sagemaker")`. The split-outs are real choices a caller makes,
not packaging artefacts.

## The target

DevCloud publishes **two** targets. They answer different questions, and reading
one as the other is the mistake this page exists to prevent.

- **Routing — every service AWS publishes, and it is met.** A registered service
  is answered at `localhost:4747`; an unregistered one is not routed, so the SDK
  call leaves the machine and bills a real AWS account. That is a safety
  property, not a capability claim, and the only number that satisfies it is all
  of them.
- **Serving depth — 205 services, decided 2026-09-05, and it is met.** Depth is
  what costs, so it follows evidence of demand rather than the shape of AWS's
  catalogue. The study below is that evidence, and it is unchanged.

| Axis | Services | Governed by |
|---|---|---|
| **Routing target** | **431 / 431 — met** | leak-zero; every published model is registered |
| **Serving target** | **205 — met** | the demand study below, sampled 2026-09-05 |
| Registered and engine-served, outside the serving target | 226 | no depth promise — see the [CRUD engine](crud-engine.md) |

### How the depth target was set

The old target was every service AWS publishes. It rested on an assumption nobody
had tested: that the services DevCloud does not register are services anyone
wants. Before committing to building ~283 of them, the assumption was tested
against three independent projects that each only add a service when someone
asks. It did not hold.

**Registering all 431 later did not overturn that decision.** What the study
refused was the *cost* — hand-building 283 services on the assumption someone
wanted them. The codegen scaffold removed that cost: registering the remaining
226 became a flag on `make codegen`, not a programme of work, and the services it
reached are served by the generic [CRUD engine](crud-engine.md) at engine
fidelity. So routing was taken because it turned out to be nearly free, and the
depth target stayed where the evidence put it, because it was never a count of
registrations — it is where DevCloud promises to be worth trusting.

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

The rule kept the 100% depth target only if ≥60% of `M` had support ≥2, and
narrowed to a demand set if ≥100 did. 57 cleared neither bar, so the
pre-registered consequence applied: **the 100% depth claim is dropped and the
published depth target becomes the demand set.** All 57 are registered — 56 serve
at least one operation, and `rds-data` is the exception named above. It is
supported by all three projects, the strongest signal in the set, and still
cannot be served generically. The engine does not reach every service it routes,
and saying so is cheaper than a fabricated success.

Four fifths of the AWS surface is surface that three projects with far more
history and staffing have collectively declined to build. That is what a long
tail looks like, and it is why the 226 carry no promise of depth: each is
registered and engine-served, so a call to one is answered locally in AWS's error
vocabulary instead of reaching a billed account, but nothing here commits to
making any of them faithful. That commitment follows demand, and the instrument
below is what measures it.

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

| | 105 services | 147 services | 205 services | 431 services |
|---|---|---|---|---|
| Binary | 30.8 MiB | 31.3 MiB | 33.1 MiB | **36.8 MiB** |
| Peak RSS (`/usr/bin/time -l`) | 57.3 MiB | 57.7 MiB | 43.7 MiB | **51.2 MiB** |
| Service startup | — | — | 49 ms for all 205 | **42 ms for all 431** |

The single-binary, zero-config property holds at 431 with room to spare. Startup
does not scale meaningfully with service count: registration is a map insert per
service in `init()`, and the generated type definitions are mostly
dead-code-eliminated by the linker, which is why 58 more services cost 1.8 MiB —
and why 226 more, nearly quadrupling the fleet, cost 3.7 MiB rather than the
7 MiB a linear reading of that figure predicts.

Two ceilings on the startup reading:

1. **It is timed from the first `service initialized` line to `DevCloud ready`**,
   so it covers bringing every service up, not the `init()` registration alone.
   That is the number an operator waits for.
2. **A first run costs about three times as much** — 118 to 148 ms across four
   measurements, against 42 ms once the data directories exist, because each of
   the 431 services creates its own on the way up. The cold path is the one to
   watch, and it is the one `cmd/devcloud/budget_test.go` reproduces.

**Startup is measured, not gated, and that is a deliberate retreat.** The gate
was written as a wall-clock ceiling and CI disproved it: the same path that takes
141 ms locally took 2.048 s on a GitHub arm64 runner. The regression worth
catching — a provider that starts opening a file or a database per service at
startup — costs 1.2x to 2x, because that is what 431 extra file opens are worth.
No absolute ceiling clears a 14x difference between machines and still fails on a
2x regression, so a ceiling in CI would have been decoration that flakes. Saying
the figure is measured is cheaper than claiming a gate that cannot fire.

What `budget_test.go` *does* assert on every run is that all 431 services come
up at all. `main.go` initializes the long tail non-fatally, so a service that is
registered but can no longer initialize would otherwise degrade to a warning in a
log nobody reads. The timing is logged beside it, and becomes an assertion when
you name a budget on a machine whose speed you know:

```
DEVCLOUD_STARTUP_BUDGET=300ms go test ./cmd/devcloud/
```

That is how the figures above are re-taken per release.

The binary size *is* gated, because size does not vary with how busy a runner is:
CI fails the build past **45 MiB**, against the 36.8 MiB above. The gap is the
platform difference — that figure is Apple Silicon, CI builds for linux — not
slack. It is a decision; if it starts failing, find what grew before raising it.

The RSS readings were taken on different days and are not a controlled
comparison. Read them as "memory is not the constraint" rather than as a saving —
what they agree on is the shape: memory is dominated by the runtime and the
store, not by how many services are registered.

## Keeping up with upstream

The 431 services are vendored from 420 Smithy models, and AWS keeps changing
them. A [weekly workflow](../.github/workflows/smithy-sync.yml) refreshes all of
them and opens a pull request. What that review costs was measured once, on
**2026-09-06**:

| Reading | Value |
|---|---|
| Vendored models refreshed | 194 (of 420 vendored today) |
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

**And it was taken at 194 models, not 420.** The vendored set has since more than
doubled, so the diff a reviewer faces is larger than anything measured here by
roughly the same factor, and the operation counts in the table predate the 6,763
operations the long tail brought with it. What does not change is the shape of
the review: it is driven by which operations moved, not by how many files did.
Reducing that cost rather than restating it is tracked separately.

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
make test-compat         # the compatibility-tested number, over all 431 services
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
