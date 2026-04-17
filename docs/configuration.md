# Configuration

**Configuration is optional.** DevCloud ships with built-in defaults: running `devcloud` with no flags enables all 96 services on port 4747, storing data under `./data/<service>/`. The embedded defaults are compiled into the binary from [`internal/config/default.yaml`](https://github.com/skyoo2003/devcloud/blob/main/internal/config/default.yaml).

To override defaults, provide a YAML file. DevCloud looks for config in this order:

1. `--config <path>` flag (explicit; the file must exist)
2. `./devcloud.yaml` in the current working directory (auto-detected)
3. Embedded defaults (used when neither of the above is present)

Environment variables override YAML values for selected keys (see [Environment Variable Overrides](#environment-variable-overrides)).

## Configuration File

### Server

| Key | Default | Description |
|-----|---------|-------------|
| `server.port` | `4747` | HTTP server port |

### Services

Each service has the following options:

| Key | Default | Description |
|-----|---------|-------------|
| `services.<name>.enabled` | `true` | Enable or disable the service |
| `services.<name>.data_dir` | varies | Data directory for persistent storage |

Service-specific options:

| Key | Default | Description |
|-----|---------|-------------|
| `services.lambda.runtime` | `""` | Lambda runtime configuration |
| `services.lambda.warm_containers` | `0` | Number of warm containers to keep |
| `services.iam.enforce_policies` | `false` | Enforce IAM policies (experimental) |

### Auth

| Key | Default | Description |
|-----|---------|-------------|
| `auth.enabled` | `false` | Enable SigV4 signature validation |

### Dashboard

| Key | Default | Description |
|-----|---------|-------------|
| `dashboard.enabled` | `false` | Enable the web dashboard |

### Logging

| Key | Default | Description |
|-----|---------|-------------|
| `logging.level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `logging.format` | `text` | Log format: `text` or `json` |

## Full Example

```yaml
server:
  port: 4747

services:
  s3:
    enabled: true
    data_dir: ./data/s3
  sqs:
    enabled: true
  dynamodb:
    enabled: true
    data_dir: ./data/dynamodb
  iam:
    enabled: true
    data_dir: ./data/iam
  sts:
    enabled: true
    data_dir: ./data/sts
  lambda:
    enabled: true
    data_dir: ./data/lambda

auth:
  enabled: false

dashboard:
  enabled: false

logging:
  level: info
  format: text
```

## Environment Variable Overrides

### `DEVCLOUD_SERVICES`

Comma-separated list of services to enable. All other services listed in the config file are disabled. Accepts individual service names and tier shortcuts.

**Tier shortcuts** (expand to predefined service groups — see [`internal/config/config.go`](https://github.com/skyoo2003/devcloud/blob/main/internal/config/config.go) for the exact list):

| Token | Expands to |
|-------|------------|
| `tier1` | Big 6 + core integration: s3, sqs, dynamodb, iam, sts, lambda, sns, kms, secretsmanager, ssm, cloudwatchlogs, cloudwatch, eventbridge, ec2, ecs, ecr, route53, acm |
| `tier2` | Extended services: cognito, elasticloadbalancingv2, ebs, efs, states, apigateway, apigatewayv2, kinesis, firehose, ses, sesv2, rds, cloudformation |
| `tier3` | Analytics & platform: elasticache, cloudfront, wafv2, glue, athena, organizations, cloudtrail, eks, autoscaling, appsync, emr, batch |
| `all` | Disables the env-var filter (all enabled services in the config stay enabled) |

Examples:

```bash
# Enable only S3 and SQS
DEVCLOUD_SERVICES=s3,sqs ./dist/devcloud

# Enable all Tier 1 services
DEVCLOUD_SERVICES=tier1 ./dist/devcloud

# Enable Tier 1 + a few extras
DEVCLOUD_SERVICES=tier1,kinesis,firehose ./dist/devcloud

# With Docker
docker run -p 4747:4747 -e DEVCLOUD_SERVICES=tier1 ghcr.io/skyoo2003/devcloud:latest
```

### `DEVCLOUD_DATA_DIR`

Overrides the base data directory for **all** services. Each service's `data_dir` becomes `<DEVCLOUD_DATA_DIR>/<service_name>` regardless of what's in the YAML file.

```bash
# Put all service data under /tmp/devcloud-local
DEVCLOUD_DATA_DIR=/tmp/devcloud-local ./dist/devcloud

# With Docker (mount the host directory)
docker run -p 4747:4747 \
  -e DEVCLOUD_DATA_DIR=/app/data \
  -v $(pwd)/devcloud-data:/app/data \
  ghcr.io/skyoo2003/devcloud:latest
```

Useful for:
- CI jobs that need ephemeral per-run data dirs
- Quickly relocating state without editing `devcloud.yaml`

## Data Directories

Services with persistent storage create data under their configured `data_dir`:

| Service | Default `data_dir` | Storage Backend | Contents |
|---------|-------------------|-----------------|----------|
| S3 | `./data/s3` | Filesystem + SQLite | Object files, `metadata.db` |
| DynamoDB | `./data/dynamodb` | BadgerDB | BadgerDB data files |
| IAM | `./data/iam` | SQLite | `iam.db` (users, roles, keys) |
| STS | `./data/sts` | Shared with IAM | Uses IAM's database |
| Lambda | `./data/lambda` | SQLite + Filesystem | `lambda.db`, `code/` directory |
| SQS | — | In-memory | No persistence |
