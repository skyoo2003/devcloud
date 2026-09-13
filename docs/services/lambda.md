# Lambda

## Overview

DevCloud Lambda stores function metadata in SQLite and function code (ZIP files) on the filesystem. Function registration and management are fully supported, but **code execution is currently a stub** — invoking a function returns a placeholder response instead of running the actual code.

## Supported APIs

These 25 operations are `hand-verified` — implemented by the provider, not by
the [CRUD engine](../crud-engine.md). Read the first row with the limitations
below: the control plane is real, the data plane is not.

| Operation | Description |
|-----------|-------------|
| Invoke | Accepts the call and returns a placeholder — **your code never runs** |
| CreateFunction | Create function with base64-encoded ZIP code |
| ListFunctions / GetFunction / DeleteFunction | Manage functions |
| UpdateFunctionCode / UpdateFunctionConfiguration | Update code ZIP or configuration |
| PublishVersion / ListVersionsByFunction | Immutable published versions |
| CreateAlias / GetAlias / UpdateAlias / DeleteAlias / ListAliases | Aliases onto versions |
| CreateEventSourceMapping / GetEventSourceMapping / UpdateEventSourceMapping | Wire an SQS queue or DynamoDB stream to a function |
| DeleteEventSourceMapping / ListEventSourceMappings | Manage those mappings |
| AddPermission / GetPolicy / RemovePermission | Resource-based policy (stored, not evaluated) |
| TagResource / UntagResource / ListTags | Function tags |

Event source mappings are polled for real: the poller reads from the SQS queue
or DynamoDB stream, builds the AWS-shaped event, and POSTs it to the function's
invoke endpoint. That endpoint is the stub, so the wiring is observable end to
end while the handler body is not.

## boto3 Examples

### Create and manage functions

```python
import boto3
import zipfile
import io
import base64

lambda_client = boto3.client(
    "lambda",
    endpoint_url="http://localhost:4747",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# Create a ZIP file with handler code
zip_buffer = io.BytesIO()
with zipfile.ZipFile(zip_buffer, "w") as zf:
    zf.writestr("index.py", 'def handler(event, context): return {"statusCode": 200}')
zip_buffer.seek(0)

# Create function
lambda_client.create_function(
    FunctionName="my-func",
    Runtime="python3.12",
    Handler="index.handler",
    Role="arn:aws:iam::000000000000:role/lambda-role",
    Code={"ZipFile": zip_buffer.read()},
)

# List functions
response = lambda_client.list_functions()
for fn in response["Functions"]:
    print(fn["FunctionName"], fn["Runtime"])

# Get function details
response = lambda_client.get_function(FunctionName="my-func")
print(response["Configuration"]["FunctionArn"])
```

### Invoke function (stub)

```python
import json

response = lambda_client.invoke(FunctionName="my-func", Payload=json.dumps({"key": "value"}))
print(json.loads(response["Payload"].read()))
# {"statusCode": 200, "body": "Lambda invoke requires Docker runtime"}
```

## AWS CLI Examples

```bash
# Create function
aws --endpoint-url http://localhost:4747 lambda create-function \
  --function-name my-func \
  --runtime python3.12 \
  --handler index.handler \
  --role arn:aws:iam::000000000000:role/role \
  --zip-file fileb://code.zip

# List functions
aws --endpoint-url http://localhost:4747 lambda list-functions

# Invoke (returns stub response)
aws --endpoint-url http://localhost:4747 lambda invoke \
  --function-name my-func output.json
```

## Known Limitations

- **No code execution.** `Invoke` returns
  `{"statusCode": 200, "body": "Lambda invoke requires Docker runtime"}` whatever
  the function or payload. Nothing in the response distinguishes it from a real
  result, so a test asserting only on the status code passes against a handler
  that never ran. Docker runtime integration is planned, not implemented.
- **Event source mappings deliver to that stub.** Messages are read from the
  source and the invoke is issued, so the plumbing is testable — but no handler
  logic runs, and a delivery failure is logged rather than retried or sent to a
  DLQ.
- **Resource-based policies are stored, never evaluated.** `AddPermission`
  succeeds and `GetPolicy` reads it back; no invoke is ever denied by one.
- No layers
- No concurrency controls (reserved or provisioned)
- No function URLs
- No environment variables — `Environment` is not part of the parsed
  `CreateFunction` request, so it is dropped without a warning and `GetFunction`
  will not return it
