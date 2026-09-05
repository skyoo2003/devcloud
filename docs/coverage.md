# Coverage

DevCloud's service count is not a capability claim on its own, so this page never
states it alone. Three numbers describe the surface, and they mean different
things:

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
