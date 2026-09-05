# Coverage

DevCloud's service count is not a capability claim on its own, so this page never
states it alone. Three numbers describe the surface, and they mean different
things:

> **The coverage target is not 100% of AWS.** It was, and the evidence did not
> support it — see [The target, and the rule that set it](#the-target-and-the-rule-that-set-it).

| Number | What it means | Today |
|---|---|---|
| **Registered** | The gateway routes the service. A call reaches DevCloud instead of falling through to real AWS. | **148** |
| **Serving ≥1 operation** | At least one operation returns a real, store-backed answer — hand-written or served by the generic CRUD engine. | **117** |
| **Registered-only** | Routed, but every operation declines with a clean AWS error. Nothing is served. | **31** |

Per operation, from the [fidelity manifest](fidelity-manifest.md):

| Tier | Operations |
|---|---|
| `hand-verified` | 4,496 |
| `auto-crud` | 1,415 |
| `unimplemented` | 3,119 |
| **total known** | **9,030** |

## Why a registered service can serve nothing

The generic CRUD engine classifies an operation by its name, and only the
`X-Amz-Target` JSON protocols put the operation name where the router can see it
(`internal/shared/crud/crud.go`, `JSONProtocol`). A `rest-json`, `query` or
`rest-xml` service is therefore unreachable by the engine and serves nothing
until somebody writes its provider by hand.

Registered services by protocol:

| Protocol | Services | Engine-servable |
|---|---|---|
| `json-1.1` | 49 | yes |
| `json-1.0` | 11 | yes |
| `rest-json` | 59 | no |
| `query` | 14 | no |
| `rest-xml` | 3 | no |
| no in-tree model | 12 | n/a — hand-written providers |

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
| Engine-served or hand-written | 17 |
| Registered-only | 31 |
| Operations served across the category | 935 |

Engine-served: `bedrock`, `bedrock-data-automation-runtime`, `comprehend`,
`comprehendmedical`, `forecast`, `frauddetector`, `healthlake`, `kendra`,
`kendra-ranking`, `lookoutequipment`, `personalize`, `rekognition`, `sagemaker`,
`textract`, `transcribe`, `translate`, `voice-id`.

Of the 31 registered-only, 30 are `rest-json`. The exception is `forecastquery`,
which is `json-1.1` but whose entire API is `QueryForecast` and
`QueryWhatIfForecast` — neither is CRUD-shaped, so there is nothing for the
engine to serve generically.

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
| Registered today | 148 |
| Target: registered + demonstrated demand | **205** |
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

Measured on an Apple Silicon machine, `CGO_ENABLED=0`, going from 105 to 147
registered services (+42):

| | 105 services | 147 services | Per service |
|---|---|---|---|
| Binary | 30.8 MiB | 31.3 MiB | ~12 KiB |
| Cold start | ~460 ms | ~460 ms | no measurable change |
| Idle RSS | 57.3 MiB | 57.7 MiB | ~10 KiB |

Extrapolated to all 431 upstream models: roughly **35 MiB binary** and **61 MiB
idle RSS**, with startup flat. The single-binary, zero-config property survives
the full target with room to spare. Startup does not scale with service count
because registration is a map insert per service in `init()`; the generated type
definitions are mostly dead-code-eliminated by the linker, which is why 322k
lines of generated Go cost half a megabyte of binary.

## Reproducing these numbers

```bash
make codegen        # regenerate the manifest from the models
make stats          # registered services and hand-written operations
go test ./cmd/devcloud/    # asserts the manifest against the live registry
```

The three service numbers come from `internal/generated/fidelity/manifest_gen.go`
and nothing else, so they cannot drift from what the binary actually serves —
`TestFidelityManifestCoverage` and `TestFidelityManifestCoversCRUDRegistry` fail
if they do.
