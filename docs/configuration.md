# Configuration

Configuration is optional. With no flags, DevCloud enables every registered service on port 4747 and stores state under `./data/<service>/`. Defaults are compiled into the binary from `internal/config/default.yaml`.

Configuration precedence follows this order: **environment variables override YAML, which overrides embedded defaults**:

1. `--config <path>`: explicit configuration file path
2. `./devcloud.yaml` in the working directory: auto-detected if present
3. Embedded defaults

## Environment Variables

| Variable | Overrides | Description |
|----------|-----------|-------------|
| `DEVCLOUD_PORT` | `server.port` | HTTP server listening port (default: 4747) |
| `DEVCLOUD_SERVICES` | `services.*.enabled` | Comma-separated service list, or a tier shortcut |
| `DEVCLOUD_DATA_DIR` | `services.*.data_dir` | Base data directory for persistent services |
| `CONTAINER_RUNTIME` | workload engine | OCI container executable (`podman`, `docker`, `nerdctl`) |
| `WASM_RUNTIME` | workload engine | WebAssembly runner executable (`wasmtime`, `wasmer`, `wazero`) |
| `DEVCLOUD_LAMBDA_NETWORK` | lambda runtime | Container network for invoked Lambda handlers |

### `DEVCLOUD_SERVICES`

When set, it specifies the running set: only the listed services start, regardless of YAML `enabled` settings. If unset, each service uses its YAML `enabled` value.

| Token | Expands to |
|-------|------------|
| `tier1` | Core services: s3, sqs, dynamodb, iam, sts, lambda, sns, kms, secretsmanager, ssm, cloudwatchlogs, cloudwatch, eventbridge, ec2, ecs, ecr, route53, acm |
| `tier2` | Extended services: cognito, elasticloadbalancingv2, ebs, efs, states, apigateway, apigatewayv2, kinesis, firehose, ses, sesv2, rds, cloudformation |
| `tier3` | Analytics and platform: elasticache, cloudfront, wafv2, glue, athena, organizations, cloudtrail, eks, autoscaling, appsync, emr, batch |
| `all` | Disables the env-var filter: the `services` block decides |

Examples:

```bash
DEVCLOUD_SERVICES=s3,sqs ./dist/devcloud
DEVCLOUD_SERVICES=tier1 ./dist/devcloud
DEVCLOUD_SERVICES=tier1,kinesis,firehose ./dist/devcloud
docker run -p 4747:4747 -e DEVCLOUD_SERVICES=tier1 ghcr.io/skyoo2003/devcloud:latest
```

### `DEVCLOUD_DATA_DIR`

Every persistent service places its database and assets under `<DEVCLOUD_DATA_DIR>/<service>`. This is useful in CI pipelines requiring isolated ephemeral storage directories.

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

## Configuration File

```yaml
server:
  port: 4747

# Optional: Top-level services block (AWS)
services:
  s3:
    enabled: true
    data_dir: ./data/s3
  sqs:
    enabled: true
  dynamodb:
    enabled: true

# Multi-provider namespaces. providers.aws.services is the same block as the
# top-level services above; set only one, or the top-level one is ignored.
providers:
  azure:
    services:
      blob:
        enabled: true
        data_dir: ./data/blob
      azurequeue:
        enabled: true
        data_dir: ./data/azurequeue
      azuretable:
        enabled: true
        data_dir: ./data/azuretable
      cosmosdb:
        enabled: true
        data_dir: ./data/cosmos
      azurekeyvault:
        enabled: true
      azureappconfig:
        enabled: true
  gcp:
    services:
      gcs:
        enabled: true
        data_dir: ./data/gcp/gcs
      gcpsecrets:
        enabled: true
      gcppubsub:
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
| `services.<name>.enabled` | `false` | Required per entry when the `services` block is explicitly present. |
| `services.<name>.data_dir` | `./data/<name>` | Data directory for persistent storage |
| `storage.data_dir` | `./data` | Directory holding shared DevCloud state and generic CRUD database |
| `admin.enabled` | `false` | Serves the admin REST API at `/devcloud/api/*` |
| `logging.level` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `logging.format` | `text` | Log output formatting: `text` or `json` |

The `services` block is optional: omit it to start all registered services automatically.

## Provider Namespacing

DevCloud supports multi-provider configurations under the `providers` map:

- `providers.aws.services`: AWS services. If top-level `services` is also specified, `providers.aws.services` takes precedence.
- `providers.azure.services`: Azure services (`blob`, `azurequeue`, `azuretable`, `cosmosdb`, `azurekeyvault`, `azureappconfig`).
- `providers.gcp.services`: GCP services (`gcs`, `gcpsecrets`, `gcppubsub`).

Data directories for providers are isolated under their respective paths to prevent naming collisions.

## Data Directories

Generic resource fallback persists state in SQLite at `<storage.data_dir>/devcloud.db` (default: `./data/devcloud.db`).

To perform a full reset of all local state, run `devcloud --reset-data` before starting the server, or send `DELETE /devcloud/api/data` while the admin API is enabled.

| Service | Default `data_dir` | Backend | Contents |
|---------|-------------------|---------|----------|
| S3 | `./data/s3` | Filesystem + SQLite | Object payloads, `metadata.db` |
| DynamoDB | `./data/dynamodb` | SQLite | `dynamodb.db` (tables, items, TTL, tags) |
| IAM | `./data/iam` | SQLite | `iam.db` (users, roles, access keys) |
| STS | `./data/sts` | Shared with IAM | Stored in IAM database |
| Lambda | `./data/lambda` | SQLite + Filesystem | `lambda.db`, code archives |
| SQS | none | In-memory | Queue state and message buffers |
| Azure Blob | `./data/blob` | Filesystem + SQLite | Container metadata, block blobs |
| Azure Table | `./data/azuretable` | SQLite | OData tables and entity records |
| Azure Cosmos | `./data/cosmos` | SQLite | Databases, collections, documents |
| GCS | `./data/gcp/gcs` | Filesystem + SQLite | Bucket metadata, object payloads |

## Workload and Container Networking

`DEVCLOUD_LAMBDA_NETWORK` selects the container network for Lambda functions. Pass the DevCloud endpoint to handlers via function environment variables. See [Lambda execution setup](services/lambda.md#workload-execution-engine) for networking options.
