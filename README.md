# DevCloud

A **local development companion for cloud-native apps**. Iterate fast without cloud bills, then land cleanly on your target CSP.

![CI](https://github.com/skyoo2003/devcloud/actions/workflows/ci.yml/badge.svg)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![License](https://img.shields.io/badge/License-Apache_2.0-blue)

## Quick Start

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

Point any AWS SDK at `http://localhost:4747`. See [Getting Started](docs/getting-started.md) for boto3 / AWS CLI / Terraform examples and other install options.

## Why DevCloud?

Every test against real AWS is billed, every feature branch wants its own sandbox, and every new joiner waits for credentials. DevCloud runs a local, API-compatible cloud environment so you can:

- **Develop offline** — no VPN, no credentials, no internet
- **Iterate without a bill** — integration tests at CI speed, not cloud speed
- **Onboard in minutes** — `docker run` and your team is productive
- **Ship with confidence** — compatibility tests run against real SDKs

DevCloud is an **on-ramp to the cloud**, not a replacement for it. The goal is to make the local → CSP transition boring. It targets AWS today; Azure, GCP and beyond are on the [roadmap](docs/roadmap.md).

## Features

- **205 AWS services registered, 201 serving at least one operation** — the remaining 4 are routed and decline with a clean AWS error rather than letting the call bill a real account. See [coverage.md](docs/coverage.md) for what the numbers do and do not promise.
- **boto3-compatible** — a 1,144-test suite runs in CI (`make test-compat`) across every registered service. Unsupported operations return a clean AWS error, never a false success.
- **Cross-service integration** — CloudFormation provisioning, DynamoDB Streams → Lambda, EventBridge targets, S3 → Lambda
- **Smithy-driven codegen** — Go types, routers and error catalogues generated from AWS models, with a weekly sync workflow that keeps them current
- **Single binary, zero-config** — one Docker image, one port (4747), no config file required; override with `DEVCLOUD_SERVICES`, `DEVCLOUD_DATA_DIR`, `DEVCLOUD_PORT`
- **SDK/CLI compatible** — AWS SDK, CLI, Terraform and CDK work out of the box
- **Admin API** — opt-in REST at `/devcloud/api/*` for service status, resources and request logs (`admin.enabled: true`)

## Core Services (AWS)

| Service | Protocol | Storage | Docs |
|---------|----------|---------|------|
| S3 | REST-XML | Filesystem + SQLite | [docs/services/s3.md](docs/services/s3.md) |
| SQS | Query + JSON | In-memory | [docs/services/sqs.md](docs/services/sqs.md) |
| DynamoDB | JSON 1.0 | BadgerDB | [docs/services/dynamodb.md](docs/services/dynamodb.md) |
| Lambda | REST-JSON | SQLite + Filesystem | [docs/services/lambda.md](docs/services/lambda.md) |
| IAM/STS | Query | SQLite | [docs/services/iam-sts.md](docs/services/iam-sts.md) |

The rest of the registered surface is described in the [services matrix](docs/services-matrix.md).

## Documentation

Start at the [docs index](docs/). Most-read pages:

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Install, first run, SDK examples |
| [Configuration](docs/configuration.md) | Config file, env-var overrides, tier shortcuts |
| [Coverage](docs/coverage.md) | What the service counts mean, and the target |
| [Compatibility Policy](docs/compatibility-policy.md) | What v1.0 guarantees across 1.x — and what it does not |
| [Architecture](docs/architecture.md) | System design, codegen pipeline, plugin model |
| [Contributing](docs/contributing.md) | Dev setup, adding a service |

Project files: [Support](SUPPORT.md) · [Governance](GOVERNANCE.md) · [Releasing](RELEASE.md) · [Changelog](CHANGELOG.md)

## Contributing

Contributions are welcome — especially service implementations, compatibility fixes, and documentation. See the [Contributing Guide](docs/contributing.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).

For security issues, follow the [Security Policy](SECURITY.md) and do not file a public issue.

## License

Apache License, Version 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

DevCloud is an independent open-source project. References to cloud service providers describe **API compatibility** only; all trademarks are the property of their respective owners. See [TRADEMARKS.md](TRADEMARKS.md).
