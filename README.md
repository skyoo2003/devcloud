# DevCloud

A **local development companion for cloud-native apps**. Iterate fast without cloud bills, then land cleanly on your target CSP.

![CI](https://github.com/skyoo2003/devcloud/actions/workflows/ci.yml/badge.svg)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![License](https://img.shields.io/badge/License-Apache_2.0-blue)

## Why DevCloud?

Modern cloud development is expensive and slow to iterate on: every test hits a billed service, every feature branch needs its own sandbox, and every new joiner waits for cloud credentials. DevCloud runs a **local, API-compatible cloud environment** so you can:

- **Develop offline** — no VPN, no credentials, no internet
- **Iterate without a bill** — run integration tests at CI speed, not cloud speed
- **Onboard in minutes** — `docker run` and your team is productive
- **Ship with confidence** — compatibility tests against real SDKs mean your code that works locally works in production

DevCloud is an **on-ramp to the cloud**, not a replacement for it. The goal is to make the local → CSP transition boring.

## Vision: one local environment for every CSP

Today DevCloud targets **AWS**. Our long-term goal is to support the full range of Cloud Service Providers (Azure, GCP, and beyond) behind the same local runtime and plugin architecture. We are rolling this out in phases — see [docs/roadmap.md](docs/roadmap.md) for the phased plan.

## Features

- **101 AWS services scaffolded** — 4,451 operations across all major categories (see [services-matrix.md](docs/services-matrix.md))
- **96% boto3 compatibility** — 671/699 SDK tests pass; works with most boto3 apps
- **Cross-service integration** — CFN provisioning, DDB Streams → Lambda, EventBridge targets, S3 → Lambda
- **Smithy-driven codegen** — auto-generate Go interfaces, types, and serializers from Smithy models
- **Weekly auto-sync** — GitHub Actions keeps generated code up to date with upstream AWS API changes
- **Single binary, zero-config** — one Docker image, one port (4747), no config file required (embedded defaults)
- **Environment variable overrides** — `DEVCLOUD_SERVICES`, `DEVCLOUD_DATA_DIR`, `DEVCLOUD_PORT` for quick configuration without YAML
- **SDK/CLI compatible** — works with the AWS SDK, CLI, Terraform, CDK out of the box
- **Web dashboard** — real-time monitoring and resource browser

## Quick Start

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

Then point any AWS SDK at `http://localhost:4747`. See [Getting Started](docs/getting-started.md) for boto3 / AWS CLI / Terraform examples and installation options.

## Supported Services (AWS)

| Service | Protocol | Storage | Docs |
|---------|----------|---------|------|
| S3 | REST-XML | Filesystem + SQLite | [docs/services/s3.md](docs/services/s3.md) |
| SQS | Query + JSON | In-memory | [docs/services/sqs.md](docs/services/sqs.md) |
| DynamoDB | JSON 1.0 | BadgerDB | [docs/services/dynamodb.md](docs/services/dynamodb.md) |
| Lambda | REST-JSON | SQLite + Filesystem | [docs/services/lambda.md](docs/services/lambda.md) |
| IAM/STS | Query | SQLite | [docs/services/iam-sts.md](docs/services/iam-sts.md) |

Azure and GCP support is on the [roadmap](docs/roadmap.md).

## Documentation

Start at the [docs index](docs/) for the full map. Quick links:

- [Getting Started](docs/getting-started.md) — Installation, first use, boto3 / AWS CLI / Terraform examples
- [Configuration](docs/configuration.md) — Config options, env-var overrides, tier shortcuts
- [Architecture](docs/architecture.md) — System design, codegen pipeline, plugin model, multi-CSP vision
- [Services Matrix](docs/services-matrix.md) — 96 services, coverage status, boto3 pass rate
- [Roadmap](docs/roadmap.md) — Phased plan toward multi-CSP support
- [FAQ](docs/faq.md) / [Troubleshooting](docs/troubleshooting.md) — Common questions and errors
- [Contributing](docs/contributing.md) — Development setup, adding new services
- [Support](SUPPORT.md) / [Governance](GOVERNANCE.md) — Where to ask, how decisions are made
- [Changelog](CHANGELOG.md) — Release history

## Contributing

We welcome contributions — especially service implementations, compatibility fixes, and documentation. See the [Contributing Guide](docs/contributing.md) for development setup and the [Code of Conduct](CODE_OF_CONDUCT.md) for community standards.

For security issues, please follow the [Security Policy](SECURITY.md) and do not file a public issue.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for attribution requirements.

## Trademark Notice

DevCloud is an independent open-source project. References to cloud service providers describe **API compatibility** only. All trademarks (AWS, Azure, Google Cloud, etc.) are the property of their respective owners. See [TRADEMARKS.md](TRADEMARKS.md) for details.
