# DevCloud Documentation

## Start here

| Page | What it covers |
|---|---|
| [Getting Started](getting-started.md) | Installation, first run, and multi-cloud SDK examples |
| [Configuration](configuration.md) | YAML options, environment variable overrides, and runtime settings |
| [Architecture](architecture.md) | System design, universal workload runtime, codegen pipeline, and multi-CSP routing |
| [Roadmap](roadmap.md) | Multi-cloud milestones, delivered capabilities, and upcoming priorities |

## Reliability and coverage

| Page | What it covers |
|---|---|
| [Coverage](coverage.md) | 441 registered / 441 serving, routing and depth targets and their guarantees |
| [Compatibility Policy](compatibility-policy.md) | Stability guarantees across 1.x releases and boundary definitions |
| [Fidelity Manifest](fidelity-manifest.md) | Per-operation tiers and reliability classifications |
| [CRUD Engine](crud-engine.md) | Generic fallback engine behavior and operational limits |
| [Services Matrix](services-matrix.md) | Depth by service group, cross-service wiring, and wire protocols |
| [Demand](demand.md) | Evaluated cloud services and demand measurement methodology |

## Service references

[S3](services/s3.md) | [SQS](services/sqs.md) | [SNS](services/sns.md) | [DynamoDB](services/dynamodb.md) | [Lambda](services/lambda.md) | [IAM / STS](services/iam-sts.md) | [EventBridge](services/eventbridge.md) | [Azure](services/azure.md) | [GCP](services/gcp.md) | [Federated Identity](services/federated-identity.md)

## Problem solving

- [FAQ](faq.md): scope, compatibility expectations, and CI usage
- [Troubleshooting](troubleshooting.md): common errors and solutions
- [Support](../SUPPORT.md): community support and help channels

## Contributing

- [Contributing Guide](contributing.md): local environment setup, testing, codegen, and adding new services
- [Plugin API](plugin-api.md): in-tree ServicePlugin interfaces and lifecycle contracts
- [Releasing](../RELEASE.md): release workflows, versioning conventions, and distribution packaging
- Core guidelines: [CONTRIBUTING.md](../CONTRIBUTING.md) | [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) | [SECURITY.md](../SECURITY.md) | [GOVERNANCE.md](../GOVERNANCE.md)

## Project information

[Changelog](../CHANGELOG.md) | [License (Apache 2.0)](../LICENSE) | [Trademarks](../TRADEMARKS.md) | [NOTICE](../NOTICE)

---

If something is missing from this index, please file an [issue](https://github.com/skyoo2003/devcloud/issues) or open a pull request.
