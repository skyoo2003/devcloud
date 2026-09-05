# DevCloud Services Matrix

**Total**: 148 services registered, 117 serving at least one operation (run `make stats`). The boto3 compatibility suite in `tests/compatibility/` passes in CI. A registered service that serves nothing still declines with a clean AWS error rather than letting the call reach real AWS — see [coverage.md](coverage.md).

> To refresh these numbers, run `make stats` (service/handler counts) and `make test-compat` (compatibility suite), then update the line above. Note: `make stats`' "Operations" counts dispatch cases (a service carrying both Query and JSON protocols counts each), not distinct AWS operations.

_Last updated with each release. For unreleased changes, see [CHANGELOG.md](../CHANGELOG.md)._

DevCloud is a Go-based local cloud environment with AWS API compatibility. This matrix tracks implemented operations per service.

## Summary by tier

| Tier | Services | Ops | Description |
|------|----------|-----|-------------|
| Tier 1 (Big 6) | S3, SQS, DynamoDB, Lambda, IAM, STS | 128+ | Core services (SQS at full 23/23 op coverage) |
| Tier 2 (Integration) | EventBridge, SNS, CW Logs, CloudWatch, KMS, Secrets Manager, SSM, ECR | 157+ | Integration services |
| Tier 3 (Extended) | EFS, EBS, EC2, Route53, ACM, ECS, Bedrock, Account, Pipes, CloudControl, RGTAPI, AppAutoScaling, Firehose, S3Tables, MWAA, Scheduler, Support, IdentityStore, MediaConvert, Textract, ServerlessRepo, DDB Streams, SFN, Kinesis, CloudFormation | 900+ | Extended platform services, networking, and services requiring custom integration logic |
| Category Expansion | remaining services | varies | Smithy-scaffolded services with working dispatch — common operations implemented; less-common ones return a clean `InvalidAction` error rather than a false success |

## Top 25 services (by ops count)

| # | Service | Ops | Category |
|---|---------|-----|----------|
| 1 | sesv2 | 155 | Business Apps |
| 2 | appconfig | 97 | Management |
| 3 | pinpoint | 93 | Business Apps |
| 4 | opensearch | 87 | Analytics |
| 5 | iot | 82 | IoT |
| 6 | backup | 82 | Storage |
| 7 | apigatewayv2 | 79 | Networking |
| 8 | waf | 77 | Security |
| 9 | neptune | 71 | Databases |
| 10 | elasticsearchservice | 67 | Analytics |
| 11 | sagemaker | 65 | ML |
| 12 | glue | 65 | Analytics |
| 13 | route53resolver | 64 | Networking |
| 14 | ssoadmin | 62 | Security |
| 15 | athena | 62 | Analytics |
| 16 | rds | 61 | Databases |
| 17 | lakeformation | 61 | Analytics |
| 18 | cloudformation | 61 | Management |
| 19 | emr | 60 | Analytics |
| 20 | kafka | 59 | Analytics |
| 21 | cognitoidentityprovider | 59 | Security |
| 22 | ecs | 57 | Containers |
| 23 | eks | 56 | Containers |
| 24 | docdb | 55 | Databases |
| 25 | codecommit | 53 | DevTools |

## Cross-service integrations

| Integration | Status | Implementation |
|-------------|--------|----------------|
| CloudFormation → 6 resource types | ✅ | `cloudformation/engine.go` with topological sort, intrinsic functions |
| DynamoDB Streams → Lambda | ✅ | `lambda/eventsource.go` polls DDB stream shards |
| SQS → Lambda | ✅ | Event source poller (pre-existing) |
| S3 → Lambda | ✅ | `s3/notifications.go` on PUT events |
| EventBridge → SQS/SNS/Lambda | ✅ | Rule matching + `dispatchToTarget` |
| SNS → SQS subscription | ✅ | Topic publish triggers queue delivery |
| DynamoDB → DynamoDB Streams | ✅ | Write-path publishes records |

## boto3 compatibility

- Tests: `tests/compatibility/` (775 tests)
- Status: the full suite passes in CI (`.github/workflows/compat.yml`); any failing test fails the build
- Run: `make test-compat`

Every service the suite covers passes — including S3Tables (ARN path parsing),
ServerlessRepo (restJson1 `jsonName`), Textract, and Support, which were once
rough edges. Core services (S3, SQS, DynamoDB, Lambda, IAM, STS, SNS, CloudWatch,
KMS, Secrets Manager, EventBridge, CloudFormation) have the deepest coverage.
Services implement their common operations; less-common operations return a clean
AWS error rather than a false success, so an SDK always gets a truthful response.

## Operation coverage & the CRUD fallback engine

Hand-written providers implement each service's common operations. For the long
tail of standard CRUD-shaped operations across 46 JSON-protocol services, a
generic engine can serve plausible, store-backed responses so SDK calls
round-trip. This coverage is **plausible, not faithful** — no validation or
business logic. See [crud-engine.md](crud-engine.md) for the wired services and
limits. Operations that are neither hand-written nor CRUD-classifiable return an
honest `InvalidAction` error, never a fabricated success.

Every operation's tier is declared in the generated
[fidelity manifest](fidelity-manifest.md) — 4,496 `hand-verified`, 948
`auto-crud`, 2,031 `unimplemented` across 7,475 operations. Query it per service
(requires `admin.enabled: true`):

```bash
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3'
```

## Supported protocols

- **JSON 1.0** (`application/x-amz-json-1.0`): DynamoDB, DynamoDB Streams, Kinesis
- **JSON 1.1** (`application/x-amz-json-1.1`): ECS, Lambda, Batch, CloudWatch Logs, SFN, many others
- **REST-JSON** (`application/json`): ACM, APIGW, Lambda REST, S3Tables, ServerlessRepo, MWAA, IdentityStore
- **REST-XML** (`application/xml`): S3, Route53, CloudFront
- **Query** (`application/x-www-form-urlencoded`): IAM, STS, SQS, SNS, RDS, CloudFormation, EC2, AutoScaling

## Architecture

For system design and plugin architecture, see [architecture.md](architecture.md).
For the phased multi-CSP vision, see [roadmap.md](roadmap.md).

## How to verify

```bash
# Build and run unit tests
make build
make test

# Run boto3 compatibility tests
make test-compat

# Print service and operation counts
make stats
```

See [Getting Started](getting-started.md) for installation and [contributing.md](contributing.md) for development setup.
