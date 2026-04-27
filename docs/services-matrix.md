# DevCloud Services Matrix

**Total**: 101 services, 4,451 operations, 96% boto3 compatibility

> To refresh these numbers, run `make stats` and update the line above.

_Last updated with each release. For unreleased changes, see [CHANGELOG.md](../CHANGELOG.md)._

DevCloud is a Go-based local cloud environment with AWS API compatibility. This matrix tracks implemented operations per service.

## Summary by tier

| Tier | Services | Ops | Description |
|------|----------|-----|-------------|
| Tier 1 (Big 6) | S3, SQS, DynamoDB, Lambda, IAM, STS | 124+ | Core services |
| Tier 2 (Integration) | EventBridge, SNS, CW Logs, CloudWatch, KMS, Secrets Manager, SSM, ECR | 157+ | Integration services |
| Tier 3 (Extended) | EFS, EBS, EC2, Route53, ACM, ECS, Bedrock, Account, Pipes, CloudControl, RGTAPI, AppAutoScaling, Firehose, S3Tables, MWAA, Scheduler, Support, IdentityStore, MediaConvert, Textract, ServerlessRepo, DDB Streams, SFN, Kinesis, CloudFormation | 900+ | Extended platform services, networking, and services requiring custom integration logic |
| Category Expansion | 40+ additional services | 2,000+ | All remaining AWS services scaffolded from Smithy models |

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

- Tests: `tests/compatibility/`
- Pass rate: **671/699 (96%)**
- Run: `make test-compat`

Remaining 28 failures are concentrated in edge cases of newly-added services
(ARN path parsing in S3Tables, restJson1 jsonName in ServerlessRepo, specialized
Textract/Support endpoints). Core services (S3, SQS, DynamoDB, Lambda, IAM, STS,
SNS, CloudWatch, KMS, SecretsManager, EventBridge, CloudFormation) all pass
100% of their tests.

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

# Count ops per service (quick matrix refresh)
for svc in $(ls internal/services/); do
  count=$(grep -c 'case "' internal/services/$svc/provider.go 2>/dev/null || echo 0)
  [ "$count" -gt "0" ] && echo "$count $svc"
done | sort -rn

# Count total scaffolded services
awk '/^services:/,/^auth:/' internal/config/default.yaml | grep -cE '^\s+[a-z][a-z0-9_]*:$'
```

See [Getting Started](getting-started.md) for installation and [contributing.md](contributing.md) for development setup.
