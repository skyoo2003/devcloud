# Services Matrix

Which cloud services DevCloud serves, and how deeply.

**Counts live in [coverage.md](coverage.md)**: it is the primary page that publishes official counts, and CI validates that its figures match the binary. This page describes the architectural shape of supported services.

## Looking Up a Service

Per-operation depth is declared by the generated [fidelity manifest](fidelity-manifest.md). You can query it directly using the Admin API (requires `admin.enabled: true`):

```bash
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3'
```

| Tier | What it means |
|------|---------------|
| `hand-verified` | The service provider explicitly implements and dispatches the operation. |
| `auto-crud` | Served by the generic [CRUD engine](crud-engine.md): store-backed and plausible responses. |
| `unimplemented` | Refused with an authentic cloud error format. Never returns a fabricated success. |

## Depth by Group

### AWS
| Group | Services | Depth |
|-------|----------|-------|
| Core | S3, SQS, DynamoDB, Lambda, IAM, STS | Hand-written throughout; deepest SDK coverage |
| Integration | SNS, EventBridge, CloudWatch, CloudWatch Logs, KMS, Secrets Manager, SSM, ECR, CloudFormation | Hand-written common operations plus cross-service event routing |
| Extended | EC2, ECS, EKS, Route53, ACM, RDS, Kinesis, Firehose, Step Functions, Bedrock, and long-tail services | Common operations hand-written; long tail is engine-served or declines cleanly |

The `tier1` / `tier2` / `tier3` tokens accepted by `DEVCLOUD_SERVICES` represent startup convenience sets, not depth tiers. See [configuration.md](configuration.md#devcloud_services) for details.

### Azure
| Service | Identifier | Protocol | Depth |
|---------|------------|----------|-------|
| Blob Storage | `blob` | REST-XML | Hand-written container and block blob lifecycle, chunked blocks, conditional requests |
| Queue Storage | `azurequeue` | REST-XML | Hand-written queue management, FIFO messages, visibility timeouts, pop receipts |
| Table Storage | `azuretable` | REST-JSON (OData) | SQLite-backed OData table and entity operations, composite key lookups |
| Cosmos DB | `cosmosdb` | REST-JSON | SQLite-backed NoSQL document store, collections, partition keys, system attributes |
| Key Vault | `azurekeyvault` | REST-JSON | Hand-written secret versioning, payload retrieval, RSA software-backed keys |
| App Configuration | `azureappconfig` | REST-JSON | Hand-written key-values, ETag tracking, tag management, label filtering |

### GCP
| Service | Identifier | Protocol | Depth |
|---------|------------|----------|-------|
| Cloud Storage | `gcs` | REST-JSON | Hand-written bucket/object CRUD, media/resumable uploads, downloads, CRC32c checksums |
| Secret Manager | `gcpsecrets` | REST-JSON | Hand-written secret lifecycle, version management, payload access, and destruction |
| Cloud Pub/Sub | `gcppubsub` | REST-JSON | Hand-written topic/subscription lifecycle, batch publishing, message pull/ack delivery |

## Cross-Service Integrations

These integrations execute real workflows locally:

| Integration | Implementation |
|-------------|----------------|
| CloudFormation -> Resource Types | `cloudformation/engine.go`: topological sort and intrinsic functions |
| DynamoDB -> DynamoDB Streams | Write paths publish stream change records |
| DynamoDB Streams -> Lambda | `lambda/eventsource.go` polls stream shards |
| SQS -> Lambda | Event source polling worker |
| S3 -> Lambda | `s3/notifications.go` on PUT events |
| EventBridge -> SQS / SNS / Lambda | Rule matching and target dispatch |
| SNS -> SQS | Topic publish triggers queue delivery |

## Protocols

| Protocol | Content type | Example services |
|----------|--------------|------------------|
| JSON 1.0 | `application/x-amz-json-1.0` | DynamoDB, DynamoDB Streams, Kinesis |
| JSON 1.1 | `application/x-amz-json-1.1` | ECS, Lambda, Batch, CloudWatch Logs, Step Functions |
| REST-JSON | `application/json` | ACM, API Gateway, GCS, GCP Secrets, Azure Key Vault, App Config |
| REST-XML | `application/xml` | S3, Route53, CloudFront, Azure Blob, Azure Queue |
| Query | `application/x-www-form-urlencoded` | IAM, STS, SQS, SNS, RDS, EC2, AutoScaling |
| EC2 Query | `application/x-www-form-urlencoded` | EC2 (EC2-compliant XML with `<requestId>`) |
| RPC-v2 CBOR | `application/cbor` | Partner Central Revenue Measurement |

SQS supports both Query and JSON protocols, auto-detected per request.

## Verification

```bash
make test          # Go unit tests
make test-compat   # Multi-cloud compatibility test suites
make stats         # Registered services and hand-written operations inventory
```

The compatibility test suite runs in CI on every commit. A failing test fails the build.
