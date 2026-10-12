# DevCloud

A local development companion for cloud-native applications. Iterate fast without cloud bills, then deploy cleanly to your target cloud provider.

![CI](https://github.com/skyoo2003/devcloud/actions/workflows/ci.yml/badge.svg)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![License](https://img.shields.io/badge/License-Apache_2.0-blue)

## Quick Start

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

Point your AWS, Azure, or GCP SDK and CLI tools at `http://localhost:4747`. See [Getting Started](docs/getting-started.md) for SDK examples, CLI configurations, and Terraform setups.

## Why DevCloud?

Every integration test against a live cloud account costs money, every feature branch needs an isolated sandbox, and every new team member waits for IAM credentials. DevCloud provides an API-compatible local cloud environment on a single port:

- **Develop offline**: no VPN, no cloud credentials, and no internet connection required.
- **Iterate without bills**: run integration tests at local speeds instead of waiting on remote provisioning.
- **Onboard in minutes**: start a single container or binary, and your team is ready to build.
- **Unified multi-cloud on port 4747**: route AWS, Azure, and GCP workloads transparently through one endpoint.
- **Pluggable workload runtimes**: execute serverless functions with standard OCI container runtimes (Docker, Podman, nerdctl), WebAssembly, or isolated host processes without mandatory daemons.
- **Ship with confidence**: verified against official SDKs and CLIs with automated compatibility suites.

DevCloud serves as a local on-ramp to cloud providers, not a production replacement. The goal is to make the transition from local development to cloud deployment straightforward and predictable.

## Features

- **441 AWS services registered, 441 serving at least one operation**: every service AWS publishes is routed locally to prevent billable leaks. See [coverage.md](docs/coverage.md) for depth and routing targets.
- **Multi-cloud API emulation**: Azure (Blob Storage, Queue Storage, Table Storage, Cosmos DB, Key Vault, App Configuration) and GCP (Cloud Storage, Secret Manager, Cloud Pub/Sub) served transparently alongside AWS on port 4747.
- **Universal workload runtime**: pluggable OCI (Docker, Podman, nerdctl), WebAssembly, and host process drivers for serverless workloads, enabling zero-daemon local execution.
- **SDK and CLI compatibility**: verified in CI against official AWS (boto3, aws CLI), Azure (Python SDK, az CLI), and GCP (Python SDK, gcloud CLI) client tooling.
- **Cross-service and federated identity**: CloudFormation orchestration, DynamoDB Streams to Lambda, EventBridge targets, S3 to Lambda, and cross-cloud OIDC and STS federation.
- **Smithy and OpenAPI codegen**: Go types, routers, and error catalogues generated from official API models, kept current with automated sync workflows.
- **Single binary, zero-config**: one port (4747), no mandatory configuration file required; customizable via environment variables and devcloud.yaml.
- **Admin API**: opt-in REST endpoints at `/devcloud/api/*` for service status, resource inventory, and request logs (`admin.enabled: true`).

## Core Services

### AWS
| Service | Protocol | Storage | Docs |
|---------|----------|---------|------|
| S3 | REST-XML | Filesystem + SQLite | [docs/services/s3.md](docs/services/s3.md) |
| SQS | Query + JSON | In-memory | [docs/services/sqs.md](docs/services/sqs.md) |
| DynamoDB | JSON 1.0 | SQLite | [docs/services/dynamodb.md](docs/services/dynamodb.md) |
| Lambda | REST-JSON | SQLite + Workload Driver | [docs/services/lambda.md](docs/services/lambda.md) |
| IAM / STS | Query | SQLite | [docs/services/iam-sts.md](docs/services/iam-sts.md) |

### Azure
| Service | Protocol | Storage | Docs |
|---------|----------|---------|------|
| Blob Storage | REST-XML | Filesystem + SQLite | [docs/services/azure.md](docs/services/azure.md) |
| Queue Storage | REST-XML | In-memory | [docs/services/azure.md](docs/services/azure.md) |
| Table Storage | REST-JSON (OData) | In-memory / SQLite | [docs/services/azure.md](docs/services/azure.md) |
| Cosmos DB | REST-JSON | In-memory / SQLite | [docs/services/azure.md](docs/services/azure.md) |
| Key Vault | REST-JSON | In-memory | [docs/services/azure.md](docs/services/azure.md) |
| App Configuration | REST-JSON | In-memory | [docs/services/azure.md](docs/services/azure.md) |

### GCP
| Service | Protocol | Storage | Docs |
|---------|----------|---------|------|
| Cloud Storage (GCS) | REST-JSON | Filesystem + SQLite | [docs/services/gcp.md](docs/services/gcp.md) |
| Secret Manager | REST-JSON | In-memory | [docs/services/gcp.md](docs/services/gcp.md) |
| Cloud Pub/Sub | REST-JSON | In-memory | [docs/services/gcp.md](docs/services/gcp.md) |

The complete list of supported services is documented in the [services matrix](docs/services-matrix.md).

## Documentation

Start at the [docs index](docs/). Key references:

| Page | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, first run, and multi-cloud SDK examples |
| [Configuration](docs/configuration.md) | Configuration file options, environment overrides, and runtime controls |
| [Coverage](docs/coverage.md) | Service counts, routing targets, and depth guarantees |
| [Compatibility Policy](docs/compatibility-policy.md) | Version guarantees across releases and boundary definitions |
| [Architecture](docs/architecture.md) | System architecture, workload execution engine, and plugin design |
| [Contributing](docs/contributing.md) | Local development setup and service contribution guidelines |

Project links: [Support](SUPPORT.md) | [Governance](GOVERNANCE.md) | [Releasing](RELEASE.md) | [Changelog](CHANGELOG.md)

## Contributing

Contributions are welcome, including new service implementations, compatibility improvements, and documentation fixes. See the [Contributing Guide](docs/contributing.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).

For security reports, please refer to the [Security Policy](SECURITY.md) instead of creating public issues.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

DevCloud is an independent open-source project. References to cloud service providers describe API compatibility only. All trademarks belong to their respective owners. See [TRADEMARKS.md](TRADEMARKS.md).
