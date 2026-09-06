# Fidelity Manifest

Not every operation is served the same way. The **fidelity manifest** answers, per
operation, *how much you can trust this call* — so a green response never leaves
you guessing whether DevCloud implemented the operation or made something
plausible up.

It is generated, never hand-written: `make codegen` derives it from the in-tree
Smithy models, the CRUD registry, and each provider's dispatch code. Current
totals are published in [coverage.md](coverage.md).

## Tiers

| Tier | Meaning | Trust it for |
|------|---------|--------------|
| `hand-verified` | The service's provider implements the operation explicitly. | Behaviour, not just shape. Covered by the boto3 suite where tests exist. |
| `auto-crud` | Served by the [generic CRUD engine](crud-engine.md): store-backed, plausible responses with no validation, business logic, or cross-resource integrity. | Wiring your SDK up and round-tripping create → get → list → delete. Nothing else. |
| `unimplemented` | Not served — the call fails instead of inventing a success. | Knowing early that DevCloud will not serve this call. |

`hand-verified` always wins: the CRUD engine is reached only when a provider's
dispatch falls through, so a hand-written implementation is never shadowed.

## What an `unimplemented` call returns

There is no single error. It depends on how the owning provider declines:

| How the provider declines | Error | Status | Providers |
|---|---|---|---|
| Returns `ErrUnhandledOp`, and the CRUD engine cannot classify the operation either — [`gateway/router.go`](../internal/gateway/router.go) emits the fallback | `InvalidAction` | 400 | 155 |
| Its dispatch `default:` answers directly | `NotImplemented` | 501 | 32 |
| Its dispatch `default:` answers in its own vocabulary | `UnsupportedOperation` / `MethodNotAllowed` | 400 / 405 | `iot`, `iotwireless`, `apigatewayv2`, `backup`, `bedrock`, `s3` |

A service can even differ by protocol: `sqs` returns `NotImplemented` (501) on
Query and `InvalidAction` (400) on JSON.

Only the *failure* is stable, and that is all
[compatibility-policy.md](compatibility-policy.md) promises — an `unimplemented`
operation never fabricates a success. The specific code and status are not
guaranteed across 1.x; normalizing them would be a minor release.

## Reading it

**At runtime** — via the admin API, which is off by default. Enable it in
`devcloud.yaml` ([configuration](configuration.md)):

```yaml
admin:
  enabled: true
```

```bash
curl -s localhost:4747/devcloud/api/fidelity              # tier counts, every service
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3' # per-operation, one service
```

```json
{
  "s3": {
    "modelBacked": true,
    "counts": {"hand-verified": 37, "unimplemented": 70},
    "operations": {"PutObject": "hand-verified", "SelectObjectContent": "unimplemented"}
  }
}
```

The unfiltered response omits `operations` — the full manifest runs to five
figures.

**In Go** — [`internal/generated/fidelity`](../internal/generated/fidelity/manifest_gen.go):

```go
tier, ok := fidelity.Lookup("s3", "PutObject") // TierHandVerified, true
```

`ok` is false for a service or operation DevCloud does not know at all, which is
not the same as `unimplemented`: an unknown service is not routed, whereas an
unimplemented operation reaches its provider and is refused.

## How it is derived

| Input | Source | Contributes |
|-------|--------|-------------|
| Operation universe | `smithy-models/*.json`, including resource-attached operations | every known operation |
| `auto-crud` | the generated [CRUD registry](crud-engine.md) | engine-servable operations |
| `hand-verified` | the case literals of each provider's `HandleRequest` dispatch | implemented operations |

Three providers (`s3`, `lambda`, `bedrock`) route on method and path rather than
an operation name, so they declare their operations explicitly in `pathRoutedOps`
(`internal/codegen/scan_handverified.go`).

The universe is *model ∪ hand-verified*. A provider may serve an operation its
model does not declare — `bedrock` serves `InvokeModel`, which AWS models under
bedrock-runtime, and `dynamodbstreams` dispatches 22 operations against a
4-operation model — and hiding those would understate what DevCloud does. So a
`hand-verified` entry is a statement about **DevCloud**, not about AWS: it can
name an operation your SDK has never heard of.

The scan is scoped to `HandleRequest` and whatever it delegates dispatch to,
because that is the only place an operation name means "operation". Elsewhere in
the same package `identitystore` switches on `"DisplayName"` to apply an
attribute patch and `pipes` switches on `"POST"` to resolve a path; reading the
whole package would file both as operations.

## Limits

- **11 services have no in-tree Smithy model** — `account`, `cloudcontrol`,
  `codeconnections`, `dms`, `identitystore`, `mediaconvert`, `pipes`, `s3tables`,
  `scheduler`, `serverlessrepo`, `verifiedpermissions`. Their `modelBacked` flag
  is `false` and the manifest lists only what DevCloud serves; the unimplemented
  tail is not enumerable without a model.
- **`hand-verified` means "the provider dispatches this operation"**, not "this
  matches AWS byte for byte". Depth varies; `make test-compat` is the stronger
  signal for the services it covers.
- The manifest describes the **whole registered surface**, not the services
  enabled in your config.

## Getting an operation promoted

Promotion to `hand-verified` happens **on request, with a use case** —
[open a feature request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml)
naming the service, the operation, and what you are trying to run locally. Check
the tier first with the endpoint above so the request is concrete.

Requests beat guesswork. The unpromoted operations are overwhelmingly operational
surface — DynamoDB backups and global tables, KMS custom key stores and rotation,
CloudWatch Insight Rules and Metric Streams, ECR pull-through cache. A local
inner loop does not call them, while the ones it does call (S3, SQS, Lambda, IAM,
STS in full) are already `hand-verified`. So v1.0 ships the long tail *declared
rather than implemented*, and lets real use decide what gets promoted in 1.x.

## Guarantees

`TestFidelityManifestCoverage` (`cmd/devcloud/fidelity_test.go`) fails the build
when:

- any operation carries a tier outside the closed set,
- any registered service is missing from the manifest,
- any CRUD-registered operation is missing or marked `unimplemented`,
- any service resolves zero `hand-verified` operations — the signal that a new
  path-routing provider needs a `pathRoutedOps` entry,
- any `auto-crud` operation is one the engine will not actually serve
  (`TestAutoCRUDIsServedOverJSON`).

The engine reads the protocol from the **request**, not the provider, so
`auto-crud` survives a Query-speaking provider: `cloudwatch` answers boto3 over
Query and falls through to the engine for `X-Amz-Target` callers. Filtering the
registry by the provider's declared protocol would delete that coverage.
