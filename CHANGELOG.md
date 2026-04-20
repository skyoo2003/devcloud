# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

_Nothing yet._

## [v0.2.0](https://github.com/skyoo2003/devcloud/releases/tag/v0.2.0) - 2026-04-21

### Added
* Add GoReleaser + Changie release pipeline ([#26](https://github.com/skyoo2003/devcloud/issues/26))

### Changed
* Bump dependencies: golangci-lint-action, go-sqlite3, GitHub Actions (setup-python, stale, checkout, release-drafter), web packages (lucide-react, shadcn, react, typescript, base-ui) ([#27](https://github.com/skyoo2003/devcloud/issues/27))

### Fixed
* Fix slice memory allocation with excessive size value (code scanning alerts #19-#22) ([#6](https://github.com/skyoo2003/devcloud/issues/6))
* Remove reference to non-existent devcloud.yaml in Dockerfile ([#25](https://github.com/skyoo2003/devcloud/issues/25))

### Security
* Fix uncontrolled data used in path expression (code scanning alerts #1-#15) ([#23](https://github.com/skyoo2003/devcloud/issues/23))

## [0.1.0] - 2026-04-18

First public release of DevCloud — a local development companion for cloud-native apps. Positioned as an on-ramp to the cloud, not a replacement: iterate locally without cloud bills, then land cleanly on your target CSP.

### Added

**Core runtime**
- Single-binary HTTP gateway on port **4747** with multi-protocol support (REST-XML, JSON 1.0, JSON 1.1, REST-JSON, Query) and middleware chain (error recovery, body limit, CORS, request ID, structured logging).
- Plugin registry with deterministic service initialization order and graceful shutdown.
- Zero-config startup: server runs with embedded default configuration if `devcloud.yaml` is absent.
- Config loader with `Load()` (strict) and `LoadOrDefault()` (graceful fallback to embedded).
- Environment-variable overrides: `DEVCLOUD_SERVICES` (with `tier1` / `tier2` / `tier3` / `all` shortcuts) and `DEVCLOUD_DATA_DIR`.

**AWS service coverage**
- 96 AWS services scaffolded from official Smithy models via an in-tree code generator.
- Big 6 fully implemented: **S3, SQS, DynamoDB, Lambda, IAM, STS**.
- Integration services: SNS, CloudWatch, CloudWatch Logs, KMS, SecretsManager, SSM, EventBridge, ECR, EC2, ECS, Route53, ACM, CloudFormation, and more.
- Cross-service integrations: CloudFormation provisioning, DynamoDB Streams → Lambda, SQS → Lambda, S3 → Lambda, EventBridge → SQS/SNS/Lambda, SNS → SQS subscriptions.
- **96% boto3 compatibility** (671/699 tests passing against the official AWS SDK test surface).
- Port-aware URL construction in SQS, ECR, and CloudFormation response paths (opts-based, not hardcoded).

**Code generation pipeline**
- Smithy JSON model parser and template-driven Go generator.
- Weekly auto-sync workflow keeps models current with AWS upstream.
- Generated files (`internal/generated/**`) include SPDX license headers and are marked `DO NOT EDIT`.

**Web dashboard** (optional, gated on `dashboard.enabled`)
- Next.js 15 / React 19 / Tailwind UI served statically by the Go server.
- Service status, resource browser, WebSocket-based live API log stream.

**Docker packaging**
- Multi-stage production Dockerfile (Alpine runtime).
- Dockerfile.dev for hot-reload frontend development.
- `docker-compose.yml` wiring backend (port 4747) and Next.js dev server (port 3000).

**Testing**
- Go unit tests for every service package (108 packages, all green).
- Table-driven port-propagation regression tests in SQS, ECR, CloudFormation.
- Python/boto3 compatibility suite under `tests/compatibility/`.

### Known Issues

- `auth.enabled: true` is accepted but SigV4 enforcement is not yet implemented; the server emits a warning at startup to make this visible.
- Lambda function invocation is a stub (accepts registration but does not execute handler code).
- Windows is not in the CI matrix; WSL2 is expected to work.

[Unreleased]: https://github.com/skyoo2003/devcloud/compare/v0.2.0...HEAD
[v0.2.0]: https://github.com/skyoo2003/devcloud/releases/tag/v0.2.0
[0.1.0]: https://github.com/skyoo2003/devcloud/releases/tag/v0.1.0
