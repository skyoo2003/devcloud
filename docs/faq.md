# Frequently Asked Questions

## General

### What is DevCloud?

A local, API-compatible cloud environment for inner-loop development. You run it on your laptop or in CI, and your AWS SDKs / CLI / Terraform talk to it instead of real AWS. See [README.md](../README.md).

### Is DevCloud a production service?

No. DevCloud is a local development tool. It is **not** designed or tested for production use, and you should not expose it to untrusted networks. See [SECURITY.md](../SECURITY.md).

### How is this different from other local cloud emulators?

DevCloud positions itself as an **on-ramp** rather than a replacement. Key differentiators:

- **Go-based single binary** — starts in under a second, no JVM or Python runtime.
- **Smithy-driven codegen** — 96 AWS services scaffolded from official models, weekly auto-sync.
- **Multi-CSP vision** — AWS today, Azure and GCP planned; see [roadmap.md](roadmap.md).
- **Apache 2.0 licensed** with explicit patent grant.

DevCloud does not aim to displace any existing tool. Use whatever works for your workflow.

### Why "on-ramp, not replacement"?

Developers should be able to iterate locally without cloud bills and deploy to their target CSP with confidence. DevCloud's compatibility targets the SDK surface, not the full behavioral model of real cloud providers. Code that works against DevCloud should be "close enough" that the remaining gap is caught by a staging environment.

## Running DevCloud

### Which services are supported?

96 AWS services are scaffolded and routed; coverage depth varies. See [services-matrix.md](services-matrix.md) for the canonical list and per-service operation counts. Core services (S3, SQS, DynamoDB, Lambda, IAM, STS, SNS, CloudWatch, KMS, SecretsManager, EventBridge, CloudFormation) pass 100% of their boto3 compatibility tests.

### Does DevCloud work on Windows?

DevCloud is developed and tested on Linux and macOS. Windows support via WSL2 is expected to work but is not part of the CI matrix. Native Windows is currently unsupported. Contributions welcome.

### Can I run DevCloud in CI?

Yes. A typical pattern is to start DevCloud as a service container in your CI job and point your tests at `http://localhost:4747`. The Docker image starts in well under a second and needs no external dependencies.

### Does DevCloud persist data across restarts?

Yes, for services with persistent backends (S3, DynamoDB, Lambda, IAM/STS). Data is written under each service's `data_dir`. SQS is in-memory only and loses state on restart.

See [configuration.md](configuration.md) for data directory options.

## Compatibility

### Will my existing boto3 code work?

96% of boto3's official SDK test suite passes against DevCloud. Most apps that use the Big 6 (S3, SQS, DynamoDB, Lambda, IAM, STS) and common integration services (SNS, CloudWatch, KMS, SecretsManager, EventBridge, CloudFormation) will work with only an `endpoint_url` change.

### What about Terraform / CDK?

Point the AWS provider / CDK at `http://localhost:4747` and set dummy credentials. Most `resource "aws_s3_bucket"`, `aws_dynamodb_table`, `aws_lambda_function` resources work out of the box. Complex IAM policies and deeply CSP-coupled resources (e.g., ACM certificates for real domains) are out of scope.

### Does DevCloud enforce IAM policies?

Not by default. IAM accepts policy documents for roundtrip compatibility but does not evaluate them when handling requests. You can enable experimental enforcement with `services.iam.enforce_policies: true`. See [configuration.md](configuration.md).

## Contributing

### I want to add a service. Where do I start?

See the "Adding a New AWS Service" section in [contributing.md](contributing.md). Short version: add the Smithy model, run `make codegen`, implement the provider and store, register the plugin.

### Will you accept my PR for service X?

We prioritize services that (a) appear in the Big 6 / Tier 1-3 lists, (b) are requested by multiple users, or (c) come with both implementation and boto3 tests. Early-stage services that fail compatibility tests will be accepted as scaffolds with `NotImplementedError` stubs.

### What's the licensing policy for contributions?

All contributions are accepted under Apache 2.0. See [CONTRIBUTING.md](../CONTRIBUTING.md).
