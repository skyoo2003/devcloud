# Services Matrix

Which AWS services DevCloud serves, and how deeply.

**Counts live in [coverage.md](coverage.md)** — it is the only page that publishes
them, and CI fails if its figures and the binary disagree. This page describes the
*shape* of the surface instead.

## How to look up one service

Per-operation depth is declared by the generated
[fidelity manifest](fidelity-manifest.md), not by a hand-maintained table. Ask it
directly (requires `admin.enabled: true`):

```bash
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3'
```

| Tier | What it means |
|------|---------------|
| `hand-verified` | The service's provider implements the operation explicitly. |
| `auto-crud` | Served by the [CRUD engine](crud-engine.md) — store-backed and plausible, not faithful. |
| `unimplemented` | Refused with an AWS-shaped error. Never a fabricated success. |

## Depth by group

| Group | Services | Depth |
|-------|----------|-------|
| Core | S3, SQS, DynamoDB, Lambda, IAM, STS | Hand-written throughout; the deepest boto3 coverage |
| Integration | SNS, EventBridge, CloudWatch, CW Logs, KMS, Secrets Manager, SSM, ECR, CloudFormation | Hand-written common operations, plus the cross-service wiring below |
| Extended | EC2, ECS, EKS, Route53, ACM, RDS, Kinesis, Firehose, SFN, Bedrock, and the rest of the registered set | Common operations hand-written; the long tail is engine-served or declines cleanly |

The `tier1` / `tier2` / `tier3` tokens accepted by `DEVCLOUD_SERVICES` are a
*startup* grouping, not a depth claim — see
[configuration.md](configuration.md#devcloud_services) for their exact contents.

## Cross-service integrations

These are wired end to end, not stubbed:

| Integration | Implementation |
|-------------|----------------|
| CloudFormation → 6 resource types | `cloudformation/engine.go` — topological sort, intrinsic functions |
| DynamoDB → DynamoDB Streams | Write path publishes records |
| DynamoDB Streams → Lambda | `lambda/eventsource.go` polls stream shards |
| SQS → Lambda | Event source poller |
| S3 → Lambda | `s3/notifications.go` on PUT events |
| EventBridge → SQS / SNS / Lambda | Rule matching + `dispatchToTarget` |
| SNS → SQS | Topic publish triggers queue delivery |

## Protocols

| Protocol | Content type | Example services |
|----------|--------------|------------------|
| JSON 1.0 | `application/x-amz-json-1.0` | DynamoDB, DynamoDB Streams, Kinesis |
| JSON 1.1 | `application/x-amz-json-1.1` | ECS, Lambda, Batch, CW Logs, SFN |
| REST-JSON | `application/json` | ACM, API Gateway, S3Tables, MWAA, IdentityStore |
| REST-XML | `application/xml` | S3, Route53, CloudFront |
| Query | `application/x-www-form-urlencoded` | IAM, STS, SQS, SNS, RDS, EC2, AutoScaling |

SQS speaks both Query and JSON; the protocol is detected per request.

## Verifying

```bash
make test          # Go unit tests
make test-compat   # boto3 compatibility suite
make stats         # registered services and hand-written operations
```

The compatibility suite runs in CI on every push — a failing test fails the
build. What it does and does not promise is
[compatibility-policy.md](compatibility-policy.md#wire-behaviour--scoped-to-the-compatibility-suite).
