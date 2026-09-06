# Configuration

**Configuration is optional.** With no flags, `devcloud` enables every registered
service on port 4747 and stores data under `./data/<service>/`. The defaults are
compiled into the binary from
[`internal/config/default.yaml`](https://github.com/skyoo2003/devcloud/blob/main/internal/config/default.yaml).

Config is resolved in this order, and **environment beats YAML beats embedded
default**:

1. `--config <path>` — explicit; the file must exist
2. `./devcloud.yaml` in the working directory — auto-detected
3. Embedded defaults

## Environment variables

| Variable | Overrides | Description |
|----------|-----------|-------------|
| `DEVCLOUD_PORT` | `server.port` | HTTP server port |
| `DEVCLOUD_SERVICES` | `services.*.enabled` | Comma-separated service list, or a tier shortcut |
| `DEVCLOUD_DATA_DIR` | `services.*.data_dir` | Base data directory for all services |

### `DEVCLOUD_SERVICES`

When set, it names the running set outright: **only** the listed services start,
regardless of their `enabled` setting, and a service the `services` block omits
entirely still starts if you name it here (the block still supplies its
`data_dir`). When unset, each service uses its YAML `enabled` value. An unknown
`tierN` token is treated as a literal service name and logged as a warning.

| Token | Expands to |
|-------|------------|
| `tier1` | Big 6 + core integration: s3, sqs, dynamodb, iam, sts, lambda, sns, kms, secretsmanager, ssm, cloudwatchlogs, cloudwatch, eventbridge, ec2, ecs, ecr, route53, acm |
| `tier2` | cognito, elasticloadbalancingv2, ebs, efs, states, apigateway, apigatewayv2, kinesis, firehose, ses, sesv2, rds, cloudformation |
| `tier3` | elasticache, cloudfront, wafv2, glue, athena, organizations, cloudtrail, eks, autoscaling, appsync, emr, batch |
| `all` | Disables the env-var filter — the `services` block decides |

The exact lists live in [`internal/config/config.go`](https://github.com/skyoo2003/devcloud/blob/main/internal/config/config.go).

```bash
DEVCLOUD_SERVICES=s3,sqs ./dist/devcloud              # only S3 and SQS
DEVCLOUD_SERVICES=tier1 ./dist/devcloud               # all Tier 1
DEVCLOUD_SERVICES=tier1,kinesis,firehose ./dist/devcloud
docker run -p 4747:4747 -e DEVCLOUD_SERVICES=tier1 ghcr.io/skyoo2003/devcloud:latest
```

### `DEVCLOUD_DATA_DIR`

Every service uses `<DEVCLOUD_DATA_DIR>/<service>`, and per-service `data_dir`
values in YAML are ignored. Useful for CI jobs needing ephemeral per-run
directories, or for relocating state without editing `devcloud.yaml`.

```bash
DEVCLOUD_DATA_DIR=/tmp/devcloud-local ./dist/devcloud

docker run -p 4747:4747 \
  -e DEVCLOUD_DATA_DIR=/app/data \
  -v $(pwd)/devcloud-data:/app/data \
  ghcr.io/skyoo2003/devcloud:latest
```

### `DEVCLOUD_PORT`

```bash
DEVCLOUD_PORT=8080 ./dist/devcloud
docker run -p 8080:8080 -e DEVCLOUD_PORT=8080 ghcr.io/skyoo2003/devcloud:latest
```

## Configuration file

```yaml
server:
  port: 4747

# Optional: listing services restricts startup to exactly this set.
# Omit the whole block to run every registered service.
services:
  s3:
    enabled: true
    data_dir: ./data/s3
  sqs:
    enabled: true
  dynamodb:
    enabled: true

admin:
  enabled: false

logging:
  level: info
  format: text
```

| Key | Default | Description |
|-----|---------|-------------|
| `server.port` | `4747` | HTTP server port |
| `services.<name>.enabled` | `false` | **Required per entry.** Listing a service is not enough — `enabled: true` still has to be set. |
| `services.<name>.data_dir` | `./data/<name>` | Data directory for persistent storage |
| `services.lambda.runtime` | `""` | Lambda runtime configuration |
| `services.lambda.warm_containers` | `0` | Warm containers to keep |
| `services.iam.enforce_policies` | `false` | Enforce IAM policies (experimental) |
| `admin.enabled` | `false` | Serve the admin REST API at `/devcloud/api/*` |
| `logging.level` | `info` | `debug`, `info`, `warn`, `error` |
| `logging.format` | `text` | `text` or `json` |

The `services` block is **optional and authoritative**: omit it and every
registered service starts; list any service and *only* the services you list
start. `services: {}` lists nothing, so nothing starts — a block is a block even
when empty.

> The `dashboard` key was renamed to `admin`. `dashboard.enabled` is still
> honoured for one release with a deprecation warning; migrate to `admin.enabled`.

> There is no `auth` key: SigV4 signature validation is not implemented and any
> credentials are accepted.

## Provider namespacing

DevCloud serves AWS today and is prepared to serve more ([roadmap](roadmap.md)),
so service configuration can be written under an explicit provider:

```yaml
providers:
  aws:
    services:
      s3:
        enabled: true
        data_dir: ./data/s3
```

`providers.aws.services` and the top-level `services` block are **the same
block** — one under its forward-compatible name, one under its historical one.

- Write either. The top-level block is not deprecated.
- If both are present, `providers.aws.services` wins and a warning names the
  ignored block. Do not write both.
- A block for a provider this build does not serve (`providers.azure`) parses
  without error and logs a warning — so a config written for a later DevCloud
  still loads, and a typo like `providers.awss` announces itself.
- `DEVCLOUD_SERVICES` and `DEVCLOUD_DATA_DIR` are AWS-scoped. There is no syntax
  for naming another provider's services, so they cannot silently disable one.
- AWS data directories stay flat (`<base>/<id>`). A future provider nests under
  its own name (`<base>/<provider>/<id>`) so two CSPs offering a same-named
  service cannot collide.

## Data directories

| Service | Default `data_dir` | Backend | Contents |
|---------|-------------------|---------|----------|
| S3 | `./data/s3` | Filesystem + SQLite | Object files, `metadata.db` |
| DynamoDB | `./data/dynamodb` | BadgerDB | BadgerDB data files |
| IAM | `./data/iam` | SQLite | `iam.db` (users, roles, keys) |
| STS | `./data/sts` | Shared with IAM | Uses IAM's database |
| Lambda | `./data/lambda` | SQLite + Filesystem | `lambda.db`, `code/` |
| SQS | — | In-memory | No persistence |
