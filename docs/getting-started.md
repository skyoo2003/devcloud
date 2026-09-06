# Getting Started

## Run with Docker (recommended)

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

To persist data across restarts, mount a volume:

```bash
docker run -p 4747:4747 -v $(pwd)/data:/app/data ghcr.io/skyoo2003/devcloud:latest
```

### GHCR Authentication

If pulling from a private registry:

```bash
docker login ghcr.io
# Username: <your GitHub username>
# Password: <your GitHub personal access token>
```

## Build from Source

Building from source is recommended only for contributors. For full prerequisites, development setup, and workflow, see [contributing.md](contributing.md).

Quick version:

```bash
git clone https://github.com/skyoo2003/devcloud.git
cd devcloud
make build   # builds the Go binaries (devcloud + codegen)
make run     # starts server on port 4747
```

## Docker Compose (Development)

```bash
docker compose -f docker/docker-compose.yml up
```

This builds the image from source and starts one service: the Go server on port
4747, with `./data` mounted for persistence and the host Docker socket mounted
so the Lambda runtime can start containers.

It runs a subset, not everything —
[`docker-compose.yml`](../docker/docker-compose.yml) sets
`DEVCLOUD_SERVICES=s3,sqs,dynamodb,iam,sts,lambda`. Edit that line, or see
[Configuration](configuration.md#devcloud_services), to run more.

## Verify Installation

### Using boto3

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

s3.create_bucket(Bucket="test-bucket")
print(s3.list_buckets()["Buckets"])
# [{'Name': 'test-bucket', 'CreationDate': ...}]
```

### Using AWS CLI

```bash
# Configure a profile (one-time)
aws configure set aws_access_key_id test --profile devcloud
aws configure set aws_secret_access_key test --profile devcloud
aws configure set region us-east-1 --profile devcloud

# Use it
aws --endpoint-url http://localhost:4747 --profile devcloud s3 mb s3://test-bucket
aws --endpoint-url http://localhost:4747 --profile devcloud s3 ls
```

You can also set an alias for convenience:

```bash
alias awslocal='aws --endpoint-url http://localhost:4747'
awslocal s3 ls
```

## Admin API

When the server runs with `admin.enabled: true` in config, an opt-in admin API
is exposed under `/devcloud/api/*`:

- `GET /devcloud/api/services` — service status overview
- `GET /devcloud/api/services/{id}/resources` — resource browser (buckets, queues, tables, functions)
- `GET /devcloud/api/logs` — recent API call logs (`?limit=`)
- `GET /devcloud/api/fidelity` — per-operation fidelity tiers (`?service=` to filter); see [fidelity-manifest.md](fidelity-manifest.md)
- `GET /devcloud/api/unrouted` — calls made to services this build does not register; see [coverage.md](coverage.md)

The web dashboard UI that consumes this API lives in a separate repository.

## Next Steps

- [Configuration](configuration.md) — All configuration options
- [Services](services/) — Per-service API reference and examples
- [Architecture](architecture.md) — System design overview
