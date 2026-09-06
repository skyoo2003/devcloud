# FAQ

## General

**What is DevCloud?**
A local, API-compatible cloud environment for inner-loop development. Run it on your laptop or in CI and point your AWS SDKs, CLI or Terraform at it instead of real AWS.

**Is it a production service?**
No. It is a local development tool, not designed or tested for production, and should not be exposed to untrusted networks. See [SECURITY.md](../SECURITY.md).

**How is it different from other local cloud emulators?**
DevCloud positions itself as an *on-ramp* rather than a replacement:

- Go single binary — starts in under a second, no JVM or Python runtime
- Smithy-driven codegen from official AWS models, with a weekly sync
- Multi-CSP vision — AWS today, Azure and GCP planned ([roadmap](roadmap.md))
- Apache 2.0 with an explicit patent grant

It does not aim to displace any existing tool. Use whatever works for your workflow.

**Why "on-ramp, not replacement"?**
Compatibility targets the SDK surface, not the full behavioural model of a real cloud. Code that works against DevCloud should be close enough that a staging environment catches the rest.

## Running DevCloud

**Which services are supported?**
See [coverage.md](coverage.md) for the counts and what they promise, and [services-matrix.md](services-matrix.md) for depth by group. Per-operation depth is queryable at runtime via the [fidelity manifest](fidelity-manifest.md).

**Does it work on Windows?**
Developed and tested on Linux and macOS. WSL2 is expected to work but is not in the CI matrix; native Windows is unsupported. Contributions welcome.

**Can I run it in CI?**
Yes — start DevCloud as a service container and point your tests at `http://localhost:4747`. The image starts in well under a second and needs no external dependencies.

**Does data persist across restarts?**
For services with a persistent backend (S3, DynamoDB, Lambda, IAM/STS), yes — under each service's `data_dir`. SQS is in-memory and loses state. See [configuration.md](configuration.md#data-directories).

## Compatibility

**Will my existing boto3 code work?**
Most apps using the core services (S3, SQS, DynamoDB, Lambda, IAM, STS) and common integration services (SNS, CloudWatch, KMS, Secrets Manager, EventBridge, CloudFormation) work with only an `endpoint_url` change. The 1,144-test boto3 suite runs green in CI on every push; what that does and does not promise is spelled out in [compatibility-policy.md](compatibility-policy.md#wire-behaviour--scoped-to-the-compatibility-suite).

**What about Terraform / CDK?**
Point the AWS provider or CDK at `http://localhost:4747` with dummy credentials. Common resources (`aws_s3_bucket`, `aws_dynamodb_table`, `aws_lambda_function`) work out of the box. Complex IAM policies and deeply CSP-coupled resources are out of scope.

**Does it enforce IAM policies?**
Not by default. IAM accepts policy documents for round-trip compatibility but does not evaluate them. Experimental enforcement: `services.iam.enforce_policies: true`.

## Contributing

**I want to add a service. Where do I start?**
[contributing.md](contributing.md#adding-a-new-aws-service). Short version: add the Smithy model, run `make codegen`, implement the provider and store, register the plugin.

**Will you accept my PR for service X?**
Priority goes to services that are in the core/integration set, have [demonstrated demand](demand.md), or arrive with both an implementation and boto3 tests. Early-stage services are accepted as scaffolds that decline cleanly.

**What is the licensing policy for contributions?**
Apache 2.0, same as the project. See [CONTRIBUTING.md](../CONTRIBUTING.md).
