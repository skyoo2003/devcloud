# Lambda

DevCloud stores function metadata in SQLite and function archives on disk. Function invocations execute through the universal workload runtime (`internal/shared/workload`), supporting OCI container engines (Docker, Podman, nerdctl), WebAssembly, and host process sandboxing. Management APIs and control-plane calls run without requiring container engines.

## Workload Execution Engine

By default, DevCloud executes Python 3.12 (`python3.12`) and Node.js 22 (`nodejs22.x`) handlers inside OCI containers.

### Container Images

Pull the official AWS runtime images before invoking containerized handlers:

```bash
docker pull public.ecr.aws/lambda/python:3.12
docker pull public.ecr.aws/lambda/nodejs:22
```

### Runtime Selection

The workload engine automatically detects available execution environments:

- **OCI Container Runtime**: Automatically discovers `podman`, `docker`, or `nerdctl` from `PATH`. You can override the engine by setting `CONTAINER_RUNTIME`:
  ```bash
  export CONTAINER_RUNTIME=podman
  ```
- **WebAssembly (WASM)**: If handlers are compiled to `.wasm` binaries or configured for WebAssembly, DevCloud executes them via `wasmtime`, `wasmer`, or embedded runtimes, eliminating container daemon overhead.
- **Process Sandbox**: For environments without container runtimes installed, the process runner executes handler code within temporary working directories.

### Network Configuration

`DEVCLOUD_LAMBDA_NETWORK` optionally selects a container network. Handlers can reach DevCloud using an endpoint passed in `Environment.Variables`:

- With a native Linux host: use `http://host.docker.internal:4747` (containers receive a `host-gateway` entry).
- When DevCloud runs inside a container: attach both containers to the same network and use the service name, such as `http://devcloud:4747`.
- For Colima on macOS: use `http://host.lima.internal:4747`.

Host AWS credentials are never inherited; defaults are `test`/`test` and region `us-east-1`. Explicit function environment variables override these defaults.

## Supported APIs

These 35 operations are hand-verified at the scope exercised by the compatibility test suite:

| Operation | Behaviour |
|---|---|
| Invoke | Actual handler result; RequestResponse, Event, DryRun and Tail logs |
| CreateFunction | ZIP archive and configuration including environment variables |
| ListFunctions / GetFunction / DeleteFunction | Function management lifecycle |
| UpdateFunctionCode / UpdateFunctionConfiguration | Code and configuration updates |
| PublishVersion / ListVersionsByFunction | Separate immutable ZIP and configuration snapshots |
| CreateAlias / GetAlias / UpdateAlias / DeleteAlias / ListAliases | Version references |
| CreateEventSourceMapping / GetEventSourceMapping / UpdateEventSourceMapping | SQS and DynamoDB Streams mappings and Enabled state |
| DeleteEventSourceMapping / ListEventSourceMappings | Mapping management lifecycle |
| AddPermission / GetPolicy / RemovePermission | Stored resource policies |
| TagResource / UntagResource / ListTags | Function tags |

## boto3 Example

```python
import io
import zipfile
import boto3

client = boto3.client(
    "lambda",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

code = io.BytesIO()
with zipfile.ZipFile(code, "w") as archive:
    archive.writestr(
        "index.py",
        "def handler(event, context): return {'executed': True, 'input': event}",
    )

client.create_function(
    FunctionName="my-function",
    Runtime="python3.12",
    Handler="index.handler",
    Role="arn:aws:iam::000000000000:role/test",
    Code={"ZipFile": code.getvalue()},
)

response = client.invoke(
    FunctionName="my-function",
    Payload=b'{"key": "value"}',
)

print("Invoke response:", response["Payload"].read().decode())
```
