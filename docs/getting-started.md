# Getting Started

## Run with Docker

```bash
docker run -p 4747:4747 ghcr.io/skyoo2003/devcloud:latest
```

To persist data across restarts, mount a local volume:

```bash
docker run -p 4747:4747 -v $(pwd)/data:/app/data ghcr.io/skyoo2003/devcloud:latest
```

Generic resources persist by default alongside service data. To reset local environment data, run `devcloud --reset-data`. When the admin API is enabled, `DELETE /devcloud/api/data` performs the same reset via HTTP.

## Docker Compose

```bash
docker compose -f deployments/docker-compose.yml up
```

Builds from source and starts the server on port 4747 with `./data` mounted. To select specific services, set `DEVCLOUD_SERVICES` (for example, `DEVCLOUD_SERVICES=s3,sqs,dynamodb,iam,sts,lambda`). See [Configuration](configuration.md#devcloud_services) for details.

## Build from Source

```bash
git clone https://github.com/skyoo2003/devcloud.git
cd devcloud
make build   # builds devcloud and codegen binaries
make run     # starts the server on port 4747
```

## Verify Your Setup

All AWS, Azure, and GCP services listen on port 4747.

### AWS (boto3)

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
```

### AWS CLI

```bash
# Configure a local profile
aws configure set aws_access_key_id test --profile devcloud
aws configure set aws_secret_access_key test --profile devcloud
aws configure set region us-east-1 --profile devcloud

# Use the endpoint
aws --endpoint-url http://localhost:4747 --profile devcloud s3 mb s3://test-bucket
aws --endpoint-url http://localhost:4747 --profile devcloud s3 ls
```

### Azure (Python SDK)

```python
from azure.storage.blob import BlobServiceClient

client = BlobServiceClient(
    account_url="http://localhost:4747/devstoreaccount1",
    credential="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
)

container = client.create_container("test-container")
print("Container created:", container.container_name)
```

### GCP (Python SDK)

```python
import os
from google.cloud import storage

os.environ["STORAGE_EMULATOR_HOST"] = "http://127.0.0.1:4747"
client = storage.Client(project="test-project")

bucket = client.create_bucket("test-bucket")
print("GCS bucket created:", bucket.name)
```

## Admin API

Opt-in by setting `admin.enabled: true` in your configuration:

| Route | Returns |
|---|---|
| `GET /devcloud/api/services` | Service status overview |
| `GET /devcloud/api/services/{id}/resources` | Inventory of created resources (buckets, queues, tables, functions) |
| `GET /devcloud/api/logs` | Recent API request logs (`?limit=100`) |
| `GET /devcloud/api/fidelity` | Per-operation fidelity tiers (`?service=s3`) |
| `GET /devcloud/api/unrouted` | Log of requests sent to unhandled or unmapped services |

## Next Steps

- Explore the [Architecture](architecture.md) and workload execution runtime.
- Check [Configuration](configuration.md) options for multi-provider deployments.
- Review [Coverage](coverage.md) for registered service details.
