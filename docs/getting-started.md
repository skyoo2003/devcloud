# Getting Started

## Run with Docker (recommended)

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

To persist data across restarts, mount a volume:

```bash
docker run -p 4747:4747 -v $(pwd)/data:/app/data ghcr.io/skyoo2003/devcloud:latest
```

## Docker Compose (development)

```bash
docker compose -f docker/docker-compose.yml up
```

Builds the image from source and starts the Go server on port 4747 with `./data`
mounted. It runs a subset, not everything —
[`docker-compose.yml`](../docker/docker-compose.yml) sets
`DEVCLOUD_SERVICES=s3,sqs,dynamodb,iam,sts,lambda`. Edit that line, or see
[Configuration](configuration.md#devcloud_services), to run more.

## Build from source

Recommended only for contributors — see [contributing.md](contributing.md) for
full prerequisites and workflow.

```bash
git clone https://github.com/skyoo2003/devcloud.git
cd devcloud
make build   # builds the Go binaries (devcloud + codegen)
make run     # starts the server on port 4747
```

## Verify it works

### boto3

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

Any dummy credentials work — DevCloud reads them but never verifies them. They
must be non-empty.

### AWS CLI

```bash
# Configure a profile (one-time)
aws configure set aws_access_key_id test --profile devcloud
aws configure set aws_secret_access_key test --profile devcloud
aws configure set region us-east-1 --profile devcloud

aws --endpoint-url http://localhost:4747 --profile devcloud s3 mb s3://test-bucket
aws --endpoint-url http://localhost:4747 --profile devcloud s3 ls
```

Or alias it: `alias awslocal='aws --endpoint-url http://localhost:4747'`.

### Terraform / CDK

Point the AWS provider at `http://localhost:4747` with dummy credentials. See the
[FAQ](faq.md#compatibility) for what works and what does not.

## Admin API

Opt-in — set `admin.enabled: true` in config, then:

| Route | Returns |
|---|---|
| `GET /devcloud/api/services` | Service status overview |
| `GET /devcloud/api/services/{id}/resources` | Buckets, queues, tables, functions |
| `GET /devcloud/api/logs` | Recent API call logs (`?limit=`) |
| `GET /devcloud/api/fidelity` | Per-operation tiers (`?service=`) — [fidelity-manifest.md](fidelity-manifest.md) |
| `GET /devcloud/api/unrouted` | Calls to services this build does not register — [coverage.md](coverage.md) |

The web dashboard UI that consumes this API lives in a separate repository.

## Next steps

- [Configuration](configuration.md) — all options and env-var overrides
- [Services](services/) — per-service API reference and examples
- [Troubleshooting](troubleshooting.md) — when something does not work
