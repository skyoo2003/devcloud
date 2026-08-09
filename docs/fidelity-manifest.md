# Fidelity Manifest

DevCloud serves 104 AWS services, but not every operation is served the same
way. The **fidelity manifest** answers, per operation, *how much you can trust
this call* — so you never have to guess whether a green response means DevCloud
implemented the operation or merely made something plausible up.

It is generated, never hand-written: `make codegen` derives it from the in-tree
Smithy models, the CRUD registry, and each provider's dispatch code.

## Tiers

| Tier | Meaning | Trust it for |
|------|---------|--------------|
| `hand-verified` | The service's provider implements the operation explicitly. | Behaviour, not just shape. Covered by the boto3 compatibility suite where tests exist. |
| `auto-crud` | Served by the [generic CRUD engine](crud-engine.md) with plausible, store-backed responses. No validation, no business logic, no cross-resource integrity. | Wiring your SDK calls and round-tripping create → get → list → delete. Nothing else. |
| `unimplemented` | Not served — the call fails instead of inventing a success. The error is the provider's own: JSON and Query services return `InvalidAction` (HTTP 400), while the path-routed providers answer in their own vocabulary (`s3` `MethodNotAllowed` 405, `lambda` `ResourceNotFoundException` 404, `bedrock` `UnsupportedOperation` 400). | Knowing early that DevCloud will not serve this call. |

`hand-verified` always wins: the CRUD engine is reached only when a provider's
dispatch falls through, so a hand-written implementation is never shadowed.

## Current coverage

| Tier | Operations |
|------|-----------:|
| `hand-verified` | 4,496 |
| `auto-crud` | 948 |
| `unimplemented` | 2,031 |
| **Total** | **7,475** across 104 services |

Examples:

| Service | hand-verified | auto-crud | unimplemented |
|---------|--------------:|----------:|--------------:|
| sqs | 23 | 0 | 0 |
| ecs | 57 | 22 | 3 |
| s3 | 37 | 0 | 70 |
| lambda | 25 | 0 | 60 |
| dynamodb | 20 | 28 | 9 |
| cloudwatch | 23 | 17 | 6 |
| bedrock | 19 | 0 | 84 |

_Regenerate with `make codegen`; these numbers change with the surface._

## Reading it

**At runtime** — the admin API, which is off by default. Enable it in your
`devcloud.yaml` ([configuration](configuration.md)):

```yaml
admin:
  enabled: true
```

```bash
# Tier counts for every service
curl -s localhost:4747/devcloud/api/fidelity

# Per-operation tiers for one service
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3'
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

The unfiltered response omits `operations` — the full manifest is ~7,000 entries.

**In Go** — [`internal/generated/fidelity`](../internal/generated/fidelity/manifest_gen.go):

```go
tier, ok := fidelity.Lookup("s3", "PutObject") // TierHandVerified, true
```

`ok` is false for a service or operation DevCloud does not know at all, which is
not the same as `unimplemented`: an unknown service is not routed at all,
whereas an unimplemented operation reaches its provider and is refused.

## Limits

- **11 services have no in-tree Smithy model** (`account`, `cloudcontrol`,
  `codeconnections`, `dms`, `identitystore`, `mediaconvert`, `pipes`, `s3tables`,
  `scheduler`, `serverlessrepo`, `verifiedpermissions`). Their `modelBacked` flag
  is `false` and the manifest lists only what DevCloud serves — the
  unimplemented tail is not enumerable without a model.
- **`hand-verified` means "the provider dispatches this operation"**, not "this
  operation matches AWS byte for byte". Depth varies; the boto3 compatibility
  suite (`make test-compat`) is the stronger signal for the services it covers.
- The manifest describes the **whole registered surface**, not the services
  enabled in your config.

## How it is derived

| Input | Source | Contributes |
|-------|--------|-------------|
| Operation universe | `smithy-models/*.json`, parsed including resource-attached operations | every known operation |
| `auto-crud` | the generated [CRUD registry](crud-engine.md) | engine-servable operations |
| `hand-verified` | the case literals of the dispatch switches inside each provider's `HandleRequest` | implemented operations |

Three providers (`s3`, `lambda`, `bedrock`) route on HTTP method and path rather
than an operation name, so they declare their operations explicitly in
`pathRoutedOps` (`internal/codegen/scan_handverified.go`).

The universe is *model ∪ hand-verified*: a provider may serve an operation the
model does not declare — `bedrock` serves `InvokeModel`, which AWS models under
bedrock-runtime, and `dynamodbstreams` dispatches 22 operations against a
4-operation model — and hiding them would understate what DevCloud does. So a
`hand-verified` entry is a statement about **DevCloud**, not about AWS: it can
name an operation your SDK has never heard of.

The scan is scoped to `HandleRequest` (and whatever it delegates dispatch to)
for a reason. That is the only place an operation name means "operation":
elsewhere in the same package, `identitystore` switches on `"DisplayName"` to
apply an attribute patch and `pipes` switches on `"POST"` to resolve a path.
Reading the whole package would file both as operations.

## Getting an operation promoted

Promotion from `auto-crud` (or `unimplemented`) to `hand-verified` happens **on request,
with a use case** — [open a feature request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml)
naming the service, the operation, and what you are trying to run locally. Check the tier
first with the endpoint above so the request is concrete.

Requests beat guesswork here. Reading what the unpromoted operations *are* shows why: they
are overwhelmingly operational surface — DynamoDB backups, global tables and Contributor
Insights; KMS custom key stores and key rotation; CloudWatch Insight Rules and Metric
Streams; ECR pull-through cache and registry policy. A local inner loop does not call them,
and the operations it does call — S3, SQS, Lambda, IAM and STS in full — are already
`hand-verified` with **zero** `auto-crud` operations between them.

So v1.0 ships the long tail **declared rather than implemented**, and lets real use decide
what gets promoted in 1.x.

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

The engine reads the protocol from the **request**, not from the provider, so
`auto-crud` survives a Query-speaking provider: `cloudwatch` answers boto3 over
Query and falls through to the engine for `X-Amz-Target` callers. Filtering the
registry by the provider's declared protocol would delete that coverage.
