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
make build-all   # builds Go binary + Next.js dashboard
make run         # starts server on port 4747
```

## Docker Compose (Development)

For development with hot-reload on the frontend:

```bash
docker compose -f docker/docker-compose.yml up
```

This starts:
- **Backend** on port 4747 — Go server with all services
- **Frontend** on port 3000 — Next.js dev server with hot-reload

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

## Dashboard

When the server is running with `dashboard.enabled: true` in config, open http://localhost:4747 in your browser to access the web dashboard:

- Service status overview
- Resource browser (buckets, queues, tables, functions)
- Real-time API call logs
- WebSocket live updates

## Next Steps

- [Configuration](configuration.md) — All configuration options
- [Services](services/) — Per-service API reference and examples
- [Architecture](architecture.md) — System design overview
