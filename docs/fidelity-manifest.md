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
| `unimplemented` | Returns an honest `InvalidAction` (HTTP 400). | Knowing early that DevCloud will not serve this call. |

`hand-verified` always wins: the CRUD engine is reached only when a provider's
dispatch falls through, so a hand-written implementation is never shadowed.

## Current coverage

| Tier | Operations |
|------|-----------:|
| `hand-verified` | 4,261 |
| `auto-crud` | 957 |
| `unimplemented` | 2,031 |
| **Total** | **7,249** across 104 services |

Examples:

| Service | hand-verified | auto-crud | unimplemented |
|---------|--------------:|----------:|--------------:|
| sqs | 23 | 0 | 0 |
| ecs | 51 | 22 | 3 |
| s3 | 37 | 0 | 70 |
| lambda | 25 | 0 | 60 |
| dynamodb | 20 | 28 | 9 |
| bedrock | 18 | 0 | 84 |

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
not the same as `unimplemented`: an unknown service is not served, whereas an
unimplemented operation is served an `InvalidAction` error.

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
| `hand-verified` | dispatch-case literals scanned from `internal/services/*`, intersected with the model | implemented operations |

Three providers (`s3`, `lambda`, `bedrock`) route on HTTP method and path rather
than an operation name, so they declare their operations explicitly in
`pathRoutedOps` (`internal/codegen/scan_handverified.go`).

The universe is *model ∪ hand-verified*: a provider may serve an operation the
model does not declare — `bedrock` serves `InvokeModel`, which AWS models under
bedrock-runtime — and hiding it would understate what DevCloud does.

## Guarantees

`TestFidelityManifestCoverage` (`cmd/devcloud/fidelity_test.go`) fails the build
when:

- any operation carries a tier outside the closed set,
- any registered service is missing from the manifest,
- any CRUD-registered operation is missing or marked `unimplemented`,
- any service resolves zero `hand-verified` operations — the signal that a new
  path-routing provider needs a `pathRoutedOps` entry.
