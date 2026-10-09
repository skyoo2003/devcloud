# Lambda

DevCloud stores function metadata in SQLite and ZIP code on disk. Invoke executes
Python 3.12 (`python3.12`) and Node.js 22 (`nodejs22.x`) handlers in fresh Docker
containers. Management APIs and server startup work without Docker.

## Execution setup

```sh
docker pull public.ecr.aws/lambda/python:3.12
docker pull public.ecr.aws/lambda/nodejs:22
```

The DevCloud process needs Docker CLI access to a running daemon. The packaged
Docker images include the CLI; mount the daemon socket and grant its access
explicitly when running DevCloud in a container. The default container user is
unchanged. No host directory bind mount or exposed Lambda port is required:
DevCloud copies code with `docker cp` and invokes inside the container.

`DEVCLOUD_LAMBDA_NETWORK` optionally selects a Docker network. Handlers can call
DevCloud using an endpoint passed in `Environment.Variables`. With a native
Linux server, use `http://host.docker.internal:4747`; containers receive a
`host-gateway` entry. For DevCloud running in a container, attach both containers
to the same named network and use its service name, such as
`http://devcloud:4747`. For Colima on macOS, `http://host.lima.internal:4747`
addresses the native host. Docker daemon/host networking must be configured by
the caller. Host AWS credentials are never inherited; defaults are `test`/`test`
and `us-east-1`. Explicit function environment values may override these defaults.

## Supported APIs

These 35 operations are hand verified at the scope exercised by the compatibility
suite. Unlisted operations use the documented fidelity tiers.

| Operation | Behaviour |
|---|---|
| Invoke | Actual handler result; RequestResponse, Event, DryRun and Tail logs |
| CreateFunction | ZIP and configuration including environment variables |
| ListFunctions / GetFunction / DeleteFunction | Function management |
| UpdateFunctionCode / UpdateFunctionConfiguration | Code and configuration updates |
| PublishVersion / ListVersionsByFunction | Separate immutable ZIP/config snapshots |
| CreateAlias / GetAlias / UpdateAlias / DeleteAlias / ListAliases | Version references |
| CreateEventSourceMapping / GetEventSourceMapping / UpdateEventSourceMapping | SQS/Streams mappings and Enabled state |
| DeleteEventSourceMapping / ListEventSourceMappings | Mapping management |
| AddPermission / GetPolicy / RemovePermission | Stored resource policies |
| TagResource / UntagResource / ListTags | Function tags |

## boto3 example

```python
import boto3, io, json, zipfile

client = boto3.client("lambda", endpoint_url="http://localhost:4747",
                      aws_access_key_id="test", aws_secret_access_key="test",
                      region_name="us-east-1")
code = io.BytesIO()
with zipfile.ZipFile(code, "w") as archive:
    archive.writestr("index.py", "def handler(event, context): return {'executed': True, 'input': event}")
client.create_function(FunctionName="my-function", Runtime="python3.12",
                       Handler="index.handler", Role="arn:aws:iam::000000000000:role/test",
                       Code={"ZipFile": code.getvalue()}, Environment={"Variables": {"MODE": "local"}})
response = client.invoke(FunctionName="my-function", Payload=json.dumps({"key": "value"}))
print(json.loads(response["Payload"].read()))
# {"executed": True, "input": {"key": "value"}}
```

Node supports async CommonJS (`exports.handler = async (...) => ...`) and ES
module exports. Python imports the configured handler module. Both receive the
runtime-provided context. Callback-style Node handlers are outside this scope.
Environment omitted in an update preserves existing variables; `Variables: {}`
clears them. `AWS_LAMBDA_RUNTIME_API`, `_HANDLER` and `DEVCLOUD_LAMBDA_HANDLER`
are reserved. Function names, full ARNs, version/alias suffixes and `Qualifier`
resolve the requested code and configuration; conflicting qualifiers return 400.
Legacy version rows sharing the latest ZIP are copied on startup. Already lost
historical code cannot be reconstructed.

## Responses and event delivery

Handler/import/serialization failures and timeouts return HTTP 200 with
`FunctionError: Unhandled` and an error payload. Inspect `FunctionError`, not
only `StatusCode`. A user result containing `errorType` remains ordinary data.
Docker/daemon/image failures return 503 `ServiceException`; invalid ZIP/JSON,
handler syntax or unsupported runtimes return 400 `InvalidParameterValueException`.
Concurrency saturation returns 429 `TooManyRequestsException`. `DryRun` returns
204 after resolving the function and requires no Docker execution.

`Event` accepts into a 64-item in-memory queue and returns 202; one worker runs
accepted jobs using the provider lifetime and waits when execution capacity is
full. Shutdown cancels outstanding jobs. This queue has no persistence, handler
failure retry or DLQ. Runtime execution permits four
concurrent calls, uses each function's Timeout, and removes containers on
completion, cancellation and shutdown. Each call uses a fresh container; image
preparation and initialization have separate 120s/30s deadlines. Tail contains
up to the last 4,096 bytes of collected container stdout and stderr.

SQS mappings delete received messages only after a successful handler response;
failed batches remain available after visibility timeout. Streams mappings save
successful shard sequences, resume after them and retain failed batches for
retry. `TRIM_HORIZON` and registration-time `LATEST` are supported. Stream records
remain in memory: a server restart loses old records and creates a new source
generation, so old checkpoints do not hide new records. S3 and EventBridge targets
preserve version/alias references and log delivery failures.

## Limits and verification

No runtime layer mounting (layers can be managed, but not attached to function execution), function URLs, reserved/provisioned concurrency, policy enforcement,
SQS partial batch responses, FIFO/DLQ parity or exactly-once delivery are promised.
ZIP extraction rejects traversal, symlinks, special files and reserved adapter
names; limits are 10,000 entries and 250 MiB uncompressed.

The ordinary suite verifies Docker-free management and failure contracts.
Actual handler and cross-service tests are a separate required CI job:

```sh
DEVCLOUD_LAMBDA_RUNTIME_TESTS=1 pytest test/compatibility/ -m lambda_runtime
```

Opt-in requires both images and a reachable daemon; missing dependencies fail
instead of skipping. Native Colima test runs can set
`DEVCLOUD_LAMBDA_CALLBACK_HOST=host.lima.internal`. Skipped runtime tests are not
execution evidence.

- **Streaming invocations** do not use binary EventStream frames; they run synchronously and return the full JSON payload at once with chunked headers.
- **Durable executions** track state in SQLite but there is no workflow orchestrator or automatic resume functionality.
