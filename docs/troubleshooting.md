# Troubleshooting

Common issues and how to resolve them. If your problem isn't listed here, check [SUPPORT.md](../SUPPORT.md) for where to ask.

## Installation & Build

### `sqlite3.h: No such file or directory` during `make build`

DevCloud requires SQLite development headers because the server uses CGO for SQLite access.

- **macOS**: `brew install sqlite3`
- **Ubuntu / Debian**: `sudo apt-get install libsqlite3-dev`
- **Fedora / RHEL**: `sudo dnf install sqlite-devel`
- **Alpine**: `apk add sqlite-dev build-base`

Then rebuild with `CGO_ENABLED=1 make build`.

### `go: module github.com/skyoo2003/devcloud: Go 1.26 required`

Upgrade Go to 1.26 or later. Check with `go version`. See [go.dev/dl](https://go.dev/dl/).

### `make build-all` fails at the web step

The dashboard requires Node.js 20+ and a fresh `npm install`.

```bash
cd web
rm -rf node_modules
npm ci
cd ..
make build-all
```

If you only need the Go server (no dashboard), use `make build` instead.

## Running the Server

### `bind: address already in use` on port 4747

Another process is already on port 4747. Either stop it or change DevCloud's port:

```yaml
# devcloud.yaml
server:
  port: 4100
```

Or for Docker, map to a different host port:

```bash
docker run -p 4100:4747 ghcr.io/skyoo2003/devcloud:latest
```

### Server starts but clients get `connection refused`

Check that the server is actually listening:

```bash
curl http://localhost:4747/devcloud/api/health
```

If that fails:
- On Docker for Mac/Windows, make sure you used `-p 4747:4747` (not just `-p 4747`)
- If you're inside another container, `localhost` refers to *that* container. Use the host bridge (e.g., `host.docker.internal` on Docker Desktop) or a shared Docker network.

### Permission denied on `./data/`

The Docker container may write as root while your host user owns the mount. Either run the container with your UID:

```bash
docker run --user $(id -u):$(id -g) -p 4747:4747 -v $(pwd)/data:/app/data ghcr.io/skyoo2003/devcloud:latest
```

Or pre-create the data directory with permissive mode (`chmod 777 data`) if you're OK with that for local development.

## SDK / Client Errors

### `SignatureDoesNotMatch` or signature-related errors

By default, DevCloud accepts any signature — it checks the SigV4 *format* but does not verify the secret. If you see signature errors, make sure:

- Your client is configured with dummy but **non-empty** credentials (`aws_access_key_id="test"`, `aws_secret_access_key="test"`).
- The `endpoint_url` points to DevCloud, not real AWS.
- If you enabled `auth.enabled: true`, either disable it or provide the credentials you registered.

### `NoSuchBucket` / `ResourceNotFoundException` after restart

Check whether the service has a persistent backend (see [configuration.md](configuration.md#data-directories)). SQS is in-memory and loses state; S3 / DynamoDB / Lambda / IAM persist only if `data_dir` points to a mounted volume (when using Docker).

### `Operation X is not implemented` errors

Not every operation of every service is implemented. Check [services-matrix.md](services-matrix.md) for the current coverage, and file a [Feature Request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml) if the operation you need is missing.

### Terraform apply succeeds but `terraform plan` shows drift next time

Some services return default values that real AWS does not echo back, and vice versa. This is a known compatibility gap. Workarounds:

- Use `ignore_changes` on the specific attributes
- Pin DevCloud and Terraform AWS-provider versions together
- File a bug with the exact resource and attributes involved

## Lambda

### Lambda function returns `runtime not available`

DevCloud's Lambda implementation is a stub — it accepts function registration but does not execute your handler code locally. Real function execution requires configuring `services.lambda.runtime`. This is an area under active development; see [roadmap.md](roadmap.md).

### Lambda invocations hang

Ensure Docker is running if you have configured Lambda to use a Docker-based runtime. Check DevCloud's logs for container startup errors.

## Dashboard

### `http://localhost:4747` shows a blank page

The dashboard is disabled by default. Enable it:

```yaml
# devcloud.yaml
dashboard:
  enabled: true
```

Then restart the server. If still blank, the Next.js static build may be missing — run `make build-web` from source or use the full Docker image (which includes the dashboard).

## When to open an issue

If your problem isn't listed here **and** you have checked [SUPPORT.md](../SUPPORT.md), open a [bug report](https://github.com/skyoo2003/devcloud/issues/new?template=bug_report.yml) with the reproduction template filled in.
