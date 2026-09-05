# DevCloud Documentation

This directory holds DevCloud's technical documentation. Use the map below to jump to what you need.

## Start here

- **[Getting Started](getting-started.md)** — install, first run, boto3 / AWS CLI / Terraform examples
- **[Configuration](configuration.md)** — YAML options, env-var overrides, tier shortcuts
- **[Architecture](architecture.md)** — system design, plugin model, codegen pipeline, multi-CSP vision
- **[Roadmap](roadmap.md)** — phased plan toward multi-CSP support
- **[Coverage](coverage.md)** — 205 registered / 201 serving, what the count promises, and the target
- **[Demand](demand.md)** — which unregistered services have demonstrated demand, and how that was measured
- **[Services Matrix](services-matrix.md)** — per-service coverage status and boto3 pass rate

## What you can rely on

- **[Compatibility Policy](compatibility-policy.md)** — what v1.0 guarantees across 1.x, what it explicitly does not, and how deprecation works
- **[Fidelity Manifest](fidelity-manifest.md)** — per-operation tiers: how much to trust any given call
- **[CRUD Engine](crud-engine.md)** — how engine-served operations behave, and where they stop
- **[Plugin API](plugin-api.md)** — the in-tree `ServicePlugin` contract for contributors

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
- **[Releasing](release.md)** — Changie + GoReleaser release process, versioning, dry runs
- Root-level pointers: [CONTRIBUTING.md](../CONTRIBUTING.md), [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md), [SECURITY.md](../SECURITY.md), [SUPPORT.md](../SUPPORT.md)

## Meta

- [Changelog](../CHANGELOG.md)
- [License (Apache 2.0)](../LICENSE)
- [Trademarks](../TRADEMARKS.md)
- [NOTICE](../NOTICE)

---

If something is missing from this index, that's a bug. Please file an [issue](https://github.com/skyoo2003/devcloud/issues) or open a PR to fix the link.
