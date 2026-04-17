# Lambda

## Overview

DevCloud Lambda stores function metadata in SQLite and function code (ZIP files) on the filesystem. Function registration and management are fully supported, but **code execution is currently a stub** — invoking a function returns a placeholder response instead of running the actual code.

## Supported APIs

| Operation | Description |
|-----------|-------------|
| CreateFunction | Create function with base64-encoded ZIP code |
| ListFunctions | List all functions in the account |
| GetFunction | Get function metadata and code location |
| DeleteFunction | Delete a function |
| UpdateFunctionCode | Update function code ZIP |
| Invoke | Invoke function (stub — returns placeholder response) |

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

- **No code execution** — Invoke returns a placeholder response. Docker runtime integration is planned but not yet implemented.
- No layers
- No aliases or versions
- No event source mappings
- No concurrency controls
- No function URLs
- No resource-based policies
- No environment variables support
