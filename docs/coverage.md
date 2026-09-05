# Coverage

DevCloud's service count is not a capability claim on its own, so this page never
states it alone. Three numbers describe the surface, and they mean different
things:

> **The coverage target is not 100% of AWS.** It was, and the evidence did not
> support it — see [The target, and the rule that set it](#the-target-and-the-rule-that-set-it).

| Number | What it means | Today |
|---|---|---|
| **Registered** | The gateway routes the service. A call reaches DevCloud instead of falling through to real AWS. | **205** |
| **Serving ≥1 operation** | At least one operation returns a real, store-backed answer — hand-written or served by the generic CRUD engine. | **201** |
| **Registered-only** | Routed, but every operation declines with a clean AWS error. Nothing is served. | **4** |
| **Compatibility-tested** | A boto3 test exercises the service in CI and passes — either serving an operation or declining cleanly. | **199** |

Every number on this page is asserted against the binary by
`go test ./cmd/devcloud/`. Editing one here without the code moving fails CI, and
so does the reverse. See [Reproducing these numbers](#reproducing-these-numbers).

Per operation, from the [fidelity manifest](fidelity-manifest.md):

| Tier | Operations |
|---|---|
| `hand-verified` | 4,496 |
| `auto-crud` | 5,193 |
| `unimplemented` | 2,718 |
| **total known** | **12,407** |

## Why a registered service can serve nothing

The generic CRUD engine has to know which operation a request is for, and it has
to recognise that operation as CRUD-shaped. Only the second one still stops it.

**The protocol always says which operation.** Each one says it somewhere
different, and the engine reads all of them: the `X-Amz-Target` JSON protocols
put it in a header; `rest-json` and `rest-xml` bind every operation to an HTTP
method and a URI template, which `internal/shared/httproute` matches a request
back to; `query` puts it in the `Action` field of the form body. This used to be
the main reason a service served nothing. It no longer is.

**The operation is not CRUD-shaped.** `GetThing`, `ListThings`, `CreateThing`
and their siblings map onto a generic store. `ExecuteStatement`, `InvokeEndpoint`
and `QueryForecast` do not, and the engine refuses them rather than inventing an
answer. A service whose entire API is that shape serves nothing whatever its
protocol. This is now the *only* reason.

Registered services by protocol:

| Protocol | Services | Engine-servable |
|---|---|---|
| `json-1.1` | 64 | yes — operation from `X-Amz-Target` |
| `json-1.0` | 17 | yes — operation from `X-Amz-Target` |
| `rest-json` | 93 | yes — operation from method + path |
| `query` | 15 | yes — operation from the `Action` form field |
| `rest-xml` | 4 | yes — operation from method + path |
| no in-tree model | 12 | n/a — hand-written providers |

Four services are registered and serve nothing, and all four are the same case —
no CRUD-shaped operation anywhere in their API: `forecastquery` (`QueryForecast`,
`QueryWhatIfForecast`), the two SageMaker Runtime variants `sagemaker-runtime`
and `sagemakerruntimehttp2` (`InvokeEndpoint*`), and `rds-data`
(`ExecuteStatement`, `BeginTransaction`, `CommitTransaction`,
`RollbackTransaction`). No protocol change reaches them.

## Why the compatibility-tested number is 199, not 205

Every registered service is exercised by
`tests/compatibility/test_service_smoke.py`, which parametrises over the
generated service list rather than a hand-written one — so a service cannot be
registered and quietly go untested, which is what 31 of them were until this was
measured. Six are excluded, in two groups, and neither group is a backlog.

**Two have no boto3 client at all.** botocore publishes 431 clients and neither
`sagemakerruntimehttp2` nor `transcribestreaming` is among them —
`sagemaker-runtime` and `transcribe` are different clients with different APIs.
No boto3 test can exist for a client that does not exist. This is a property of
the AWS SDK, not of DevCloud, and it is the ceiling on this number.

**Four are registered and unreachable.** All four Lex clients — `lex-models`,
`lex-runtime`, `lexv2-models`, `lexv2-runtime` — sign as `lex`, and no service
is named `lex`, so the alias stays contested and routes nowhere rather than
sending one service's traffic to another. The consequence, which this page did
not state until it was measured: the "reached by its own unambiguous name"
escape below does not exist for a boto3 caller, because boto3 signs with the
contested name and offers nothing else. The four are counted in *serving ≥1
operation*, because their operations are classified and would be served; they
are not counted here, because nothing can ask for them.

Splitting them on the URL is what resolved `opensearch` / `elasticsearchservice`
and API Gateway v1 / v2, and it does not work here: `/bots` is the first path
segment for three of the four. A correct split needs full path matching across
all four, which is routing work rather than coverage work.
`test_lex_services_are_unreachable_from_boto3` pins the current state, so
fixing the routing fails that test and moves this number up with it.

Being engine-servable is not the same as being served. Thirteen of the fifteen
`query` services and three of the four `rest-xml` ones have hand-written
providers that answer their own unknown operations, so they never enter the
engine and their manifest did not move when the protocols were admitted — the
`EngineWired` flag records that per service.

A registered operation is not automatically a reachable one. The engine is
entered only when a provider returns `plugin.ErrUnhandledOp`, so a hand-written
provider that refuses unknown operations itself — `apigatewayv2`, `xray` — never
reaches it, and the manifest records that per service as `EngineWired`.

Registering a service the engine cannot serve is deliberate, not an oversight.
The alternative is worse: an unregistered service is not routed, so the SDK call
leaves the machine and bills a real AWS account. A registered-only service
answers locally, in AWS's own error vocabulary, and the developer finds out
immediately. What it must never do is fabricate a success —
`tests/compatibility/test_service_smoke.py::test_registered_only_service_declines_cleanly`
is the check that keeps that true.

## AI / Machine Learning category

The first category taken to completion. All 48 upstream models are registered.

| | Count |
|---|---|
| Registered | 48 |
| Engine-served or hand-written | 45 |
| Registered-only | 3 |

When this category was completed, only 17 of its 48 members served anything: 30
of the other 31 were `rest-json`, which the engine could not read at all. Teaching
it that protocol moved 28 of those 30 into coverage without touching a single one
of their providers.

The three that remain are the ones no protocol change can help — `forecastquery`
and the two SageMaker Runtime variants, none of which has a CRUD-shaped
operation. See [Why a registered service can serve nothing](#why-a-registered-service-can-serve-nothing).

## The demand set

All 57 are registered, and the target of 205 is met. They were worked in the
support-rank order of [demand.md](demand.md), so a stop at any point would have
stopped on the best surface available.

| | Count |
|---|---|
| Registered | 57 |
| Serving ≥1 operation | 56 |
| Registered-only | 1 |

The 57 split by protocol as 34 `rest-json`, 15 `json-1.1`, 6 `json-1.0`, 1
`query`, 1 `rest-xml`. Two thirds of the set is `rest-json`, which is why the
engine gaining that protocol is what made this milestone possible rather than a
matter of arithmetic: without it, 34 of the 57 would have registered and served
nothing.

The one that does not meet the floor is `rds-data`, and it is the one worth
calling out: it is supported by all three projects in the demand survey — the
strongest signal in the whole set — and it still cannot be served generically,
because not one of its six operations is CRUD-shaped. Breadth does not reach
every service, and saying so is cheaper than a fabricated success.

`elastic-load-balancing` and `s3-control` were the other two. They were the last
of the demand set because of their protocols, not their APIs, and both are
served now: 18 of ELB's 29 operations and 94 of S3 Control's 97. The eleven and
the three that remain are the not-CRUD-shaped case again — `ConfigureHealthCheck`
and `ApplySecurityGroupsToLoadBalancer` among them, which a Terraform `aws_elb`
resource does call. Breadth is what this milestone bought; depth is still not
claimed.

## What counts as a service

Upstream `aws-sdk-go-v2/aws-models` publishes 431 model files. That is not 431
services from a caller's point of view: 12 of the AI/ML category's 48 files are
runtime or control-plane split-outs of another service (`sagemaker-runtime`,
`bedrock-runtime`, `forecastquery`, `personalize-runtime`, the four Lex
services, `bedrock-agentcore-control`, and the SageMaker `*-runtime` variants).

DevCloud counts **model files**, one registered service each, because that is
what an SDK client selects: `boto3.client("sagemaker-runtime")` is a different
client with a different API from `boto3.client("sagemaker")`. The split-outs are
real choices a caller makes, not packaging artefacts.

The consequence is visible in routing. A split-out signs with its parent's name,
so `sagemaker` is claimed by eight services at once. Where exactly one claimant
carries the name as its own service ID, that service wins and the rest are
treated as borrowers — see `codegen.BuildAliases`. Where none does, the alias is
left unrouted rather than guessed: all four Lex services sign as `lex` and none
is called `lex`, so `lex` routes nowhere and each Lex service is reached by its
own unambiguous name.

## The target, and the rule that set it

**Decided 2026-09-05. The target is 205 services, not 431.**

DevCloud's roadmap previously aimed at every AWS service AWS publishes — 431
model files. That target rested on an assumption nobody had tested: that the
services DevCloud does not register are services anyone wants. Before committing
to building ~283 of them, the assumption was tested, and it did not hold.

**The rule was fixed before the numbers were seen.** Four outcomes were written
down in advance, including one for "the method itself failed", specifically so
the result could not be argued into whichever answer was most convenient once it
arrived. The full evidence is in [demand.md](demand.md), re-derivable with
`python3 scripts/demand_rank.py`.

**What was measured.** For each of the 283 unregistered services, how many of
three independent projects have already built it: `moto` (163 services),
LocalStack (119), `terraform-provider-aws` (273). Each serves a population
DevCloud names as its user, and each only adds a service when someone asks.

| Reading | Value |
|---|---|
| Missing services (`M`) | 283 |
| `M` built by all three projects | 8 |
| `M` built by at least two | **57 (20.1%)** |
| `M` built by exactly one | 111 |
| `M` built by none | 115 |
| DevCloud's own service requests, all time | **0** |

**The outcome that fired.** The rule kept the 100% target only if ≥60% of `M`
had support ≥ 2, and narrowed to a demand set if ≥100 did. 57 cleared neither
bar, so the pre-registered consequence applies: **the 100% claim is dropped from
the docs before any bulk work, and the published target becomes the demand set.**

Four fifths of the AWS surface DevCloud does not cover is surface that three
projects — with far more history and staffing than DevCloud — have collectively
declined to build. That is not an accident of their roadmaps. It is what a long
tail looks like.

| | Services |
|---|---|
| Registered today | **205** |
| Target: registered + demonstrated demand | **205 — met** |
| Explicitly not targeted | 226 |

The 226 are not refused. Any of them can be onboarded when someone asks — the
["Service not supported"](https://github.com/skyoo2003/devcloud/issues/new?template=service_request.yml)
form is that channel, and one report outranks all three proxies, because it is
demand rather than a stand-in for it. What changed is that they are no longer
work DevCloud has promised.

**What this verdict is not.** The three sources measure *emulator and provider
effort*, not user demand. They are a proxy, chosen because DevCloud had no
measurement of its own and the alternative was an unbounded wait. The instrument
below now accrues the real thing.

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

Then open a [service request](https://github.com/skyoo2003/devcloud/issues/new?template=service_request.yml)
and paste it. That output is what moves a service from the 226 into the target.

**Two ceilings, so the number is not read as more than it is.**

1. **It is a floor, not a census.** `gateway.DetectProtocol` classifies a request
   it cannot identify as `("rest-xml", "s3")`, and S3 is registered — so an
   unrecognisable request is routed to S3 rather than counted as a miss. What
   this does catch is the case that matters: a real SDK or CLI call to an
   unregistered service signs with that service's own name and misses the
   registry. Verified with boto3 — `boto3.client("appflow")` is recorded as
   `appflow`.
2. **A non-zero `droppedServiceIds` means the list is incomplete.** Service IDs
   come from caller-controlled headers, so the collector caps how many distinct
   ones it holds and reports both the cap and what it dropped, rather than
   growing without bound or truncating in silence.

The counts live in memory and reset when the process does. This is a local
development tool; nothing is sent anywhere.

## Runtime cost

Measured on an Apple Silicon machine, `CGO_ENABLED=0`, at each step of the
roadmap:

| | 105 services | 147 services | 205 services |
|---|---|---|---|
| Binary | 30.8 MiB | 31.3 MiB | **33.1 MiB** |
| Peak RSS (`/usr/bin/time -l`) | 57.3 MiB | 57.7 MiB | **43.7 MiB** |
| Service registration | — | — | **49 ms for all 205** |

The single-binary, zero-config property holds at the target with room to spare.
Startup does not scale meaningfully with service count because registration is a
map insert per service in `init()`, and the generated type definitions are mostly
dead-code-eliminated by the linker — which is why 58 more services cost 1.8 MiB
of binary.

Peak RSS did not rise with the service count; it fell. The earlier figures were
taken on a different day and are not a controlled comparison, so read this as
"memory is not the constraint at 205" rather than as a saving. What the two
readings agree on is the shape: memory is dominated by the runtime and the
store, not by how many services are registered.

## Reproducing these numbers

```bash
make codegen        # regenerate the manifest from the models
make stats          # registered services and hand-written operations
go test ./cmd/devcloud/    # asserts every number on this page against the binary
make test-compat           # the compatibility-tested number, over all 205 services
```

The service and operation numbers come from
`internal/generated/fidelity/manifest_gen.go` and nothing else, so they cannot
drift from what the binary actually serves — `TestFidelityManifestCoverage` and
`TestFidelityManifestCoversCRUDRegistry` fail if they do.

They cannot drift from *this page* either.
`TestPublishedCoverageMatchesTheBinary` and
`TestPublishedOperationTiersMatchTheManifest` read the tables above and compare
each figure to the live registry and manifest, in both directions: a service
removed without editing the page fails, and a figure edited here without the
code moving fails identically. `TestRegisteredOnlyServicesAreNamedInTheDocs`
does the same for the depth claim, so a service that starts serving nothing
cannot join that set without being named. `TestDemandSetIsRegistered` checks the
target itself — all 57 services in [demand.md](demand.md) are still registered,
so the count cannot be held steady by swapping one out for something else.
