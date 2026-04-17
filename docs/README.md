# DevCloud Documentation

This directory holds DevCloud's technical documentation. Use the map below to jump to what you need.

## Start here

- **[Getting Started](getting-started.md)** — install, first run, boto3 / AWS CLI / Terraform examples
- **[Configuration](configuration.md)** — YAML options, env-var overrides, tier shortcuts
- **[Architecture](architecture.md)** — system design, plugin model, codegen pipeline, multi-CSP vision
- **[Roadmap](roadmap.md)** — phased plan toward multi-CSP support
- **[Services Matrix](services-matrix.md)** — 96 services, coverage status, boto3 pass rate

## Per-service references

Located under [`services/`](services/):

- [S3](services/s3.md) — object storage (REST-XML)
- [SQS](services/sqs.md) — message queue (Query + JSON)
- [DynamoDB](services/dynamodb.md) — NoSQL KV + document (JSON 1.0)
- [Lambda](services/lambda.md) — function runtime (REST-JSON)
- [IAM / STS](services/iam-sts.md) — identity & tokens (Query)

## Problem solving

- **[FAQ](faq.md)** — common questions about scope, compatibility, CI use
- **[Troubleshooting](troubleshooting.md)** — common errors and fixes

## Contributing

- **[Contributing Guide](contributing.md)** — dev setup, testing, codegen, adding new services
- Root-level pointers: [CONTRIBUTING.md](../CONTRIBUTING.md), [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md), [SECURITY.md](../SECURITY.md), [SUPPORT.md](../SUPPORT.md)

## Meta

- [Changelog](../CHANGELOG.md)
- [License (Apache 2.0)](../LICENSE)
- [Trademarks](../TRADEMARKS.md)
- [NOTICE](../NOTICE)

---

If something is missing from this index, that's a bug. Please file an [issue](https://github.com/skyoo2003/devcloud/issues) or open a PR to fix the link.
