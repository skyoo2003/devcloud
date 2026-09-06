# DevCloud Documentation

## Start here

| Page | What it covers |
|---|---|
| [Getting Started](getting-started.md) | Install, first run, boto3 / AWS CLI / Terraform examples |
| [Configuration](configuration.md) | YAML options, env-var overrides, tier shortcuts |
| [Architecture](architecture.md) | System design, plugin model, codegen pipeline, multi-CSP vision |
| [Roadmap](roadmap.md) | Phased plan toward multi-CSP support |

## What you can rely on

| Page | What it covers |
|---|---|
| [Coverage](coverage.md) | 205 registered / 201 serving — what the counts promise, and the target |
| [Compatibility Policy](compatibility-policy.md) | What v1.0 guarantees across 1.x, what it does not, and how deprecation works |
| [Fidelity Manifest](fidelity-manifest.md) | Per-operation tiers: how much to trust any given call |
| [CRUD Engine](crud-engine.md) | How engine-served operations behave, and where they stop |
| [Services Matrix](services-matrix.md) | Depth by group, cross-service integrations, protocols |
| [Demand](demand.md) | Which unregistered services have demonstrated demand, and how it was measured |

## Per-service references

[S3](services/s3.md) · [SQS](services/sqs.md) · [DynamoDB](services/dynamodb.md) · [Lambda](services/lambda.md) · [IAM / STS](services/iam-sts.md)

## Problem solving

- [FAQ](faq.md) — scope, compatibility, CI use
- [Troubleshooting](troubleshooting.md) — common errors and fixes
- [Support](../SUPPORT.md) — where to ask

## Contributing

- [Contributing Guide](contributing.md) — dev setup, testing, codegen, adding a service
- [Plugin API](plugin-api.md) — the in-tree `ServicePlugin` contract
- [Releasing](../RELEASE.md) — Changie + GoReleaser, versioning, Homebrew tap, dry runs
- Root pointers: [CONTRIBUTING.md](../CONTRIBUTING.md) · [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) · [SECURITY.md](../SECURITY.md) · [GOVERNANCE.md](../GOVERNANCE.md)

## Meta

[Changelog](../CHANGELOG.md) · [License (Apache 2.0)](../LICENSE) · [Trademarks](../TRADEMARKS.md) · [NOTICE](../NOTICE)

---

If something is missing from this index, that's a bug — please file an [issue](https://github.com/skyoo2003/devcloud/issues) or open a PR.
