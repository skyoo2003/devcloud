# Troubleshooting

If your problem isn't here, see [SUPPORT.md](../SUPPORT.md) for where to ask.

## Build and startup

**`go: module github.com/skyoo2003/devcloud: Go 1.26 required`**
Upgrade Go to 1.26+ (`go version`, then [go.dev/dl](https://go.dev/dl/)).

**`bind: address already in use` on port 4747**
Something else holds the port. Change DevCloud's:

```yaml
# devcloud.yaml
server:
  port: 4100
```

Or map a different host port: `docker run -p 4100:4747 …`.

**Server starts but clients get `connection refused`**
Check it is actually listening — any request works, DevCloud has no health
endpoint:

```bash
curl -i http://localhost:4747/
```

If that fails:
- On Docker Desktop, make sure you used `-p 4747:4747`, not just `-p 4747`.
- From inside another container, `localhost` means *that* container. Use
  `host.docker.internal` or a shared Docker network.

**Permission denied on `./data/`**
The container may write as root while your host user owns the mount. Run it as
your user:

```bash
docker run --user $(id -u):$(id -g) -p 4747:4747 -v $(pwd)/data:/app/data ghcr.io/skyoo2003/devcloud:latest
```

## SDK and client errors

**`SignatureDoesNotMatch` or other signature errors**
DevCloud checks the SigV4 *format* but never verifies the secret. Make sure your
client has dummy but **non-empty** credentials (`aws_access_key_id="test"`,
`aws_secret_access_key="test"`) and that `endpoint_url` points at DevCloud.

**`NoSuchBucket` / `ResourceNotFoundException` after restart**
SQS is in-memory and loses state. S3, DynamoDB, Lambda and IAM persist only if
`data_dir` points somewhere durable — under Docker, a mounted volume. See
[configuration.md](configuration.md#data-directories).

**`InvalidAction`, `NotImplemented` or `UnsupportedOperation`**
That operation is not served. Check its tier — enable `admin.enabled: true`, then:

```bash
curl -s 'localhost:4747/devcloud/api/fidelity?service=s3'
```

An `unimplemented` operation is a deliberate honest failure, never a fabricated
success ([fidelity-manifest.md](fidelity-manifest.md)). To ask for it, open a
[feature request](https://github.com/skyoo2003/devcloud/issues/new?template=feature_request.yml).

**The call reached real AWS instead of DevCloud**
The service is not registered, so nothing routed it. Confirm with
`GET /devcloud/api/unrouted` and file a
[service request](https://github.com/skyoo2003/devcloud/issues/new?template=service_request.yml) —
see [coverage.md](coverage.md#service-not-supported).

**Terraform apply succeeds but the next plan shows drift**
Some services return defaults real AWS does not echo back, and vice versa. This is
a known gap. Use `ignore_changes` on the affected attributes, pin DevCloud and the
AWS provider together, or file a bug with the exact resource and attributes.

## Lambda

**Invoking a function returns `Lambda invoke requires Docker runtime`**
Expected. `internal/services/lambda/runtime.go` is a stub: DevCloud registers
functions, stores their code, and drives event source mappings (SQS, DynamoDB
Streams, S3 notifications), but it does not execute your handler. Real local
execution is not implemented — see [roadmap.md](roadmap.md).

## Admin API

**`GET /devcloud/api/*` returns 404**
It is disabled by default:

```yaml
# devcloud.yaml
admin:
  enabled: true
```

Restart afterwards. The routes are `services`, `services/{id}/resources`, `logs`,
`fidelity` and `unrouted` — there is no `health` route, and no bundled UI (the web
dashboard is a separate repository).

## Still stuck?

Open a [bug report](https://github.com/skyoo2003/devcloud/issues/new?template=bug_report.yml)
with the reproduction template filled in.
