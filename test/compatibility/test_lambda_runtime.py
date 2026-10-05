import base64
import io
import json
import os
import subprocess
import time
import uuid
import zipfile
from urllib.parse import urlparse

import pytest
from conftest import DEVCLOUD_URL

pytestmark = pytest.mark.lambda_runtime


def code_zip(source, filename="index.py"):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(filename, source)
    return buf.getvalue()


@pytest.fixture
def function_factory(runtime_lambda_client):
    client = runtime_lambda_client
    functions = []

    def create(
        source, runtime="python3.12", environment=None, timeout=3, filename=None
    ):
        name = "runtime-" + uuid.uuid4().hex[:16]
        functions.append(name)
        client.create_function(
            FunctionName=name,
            Runtime=runtime,
            Handler="index.handler",
            Role="arn:aws:iam::000000000000:role/test",
            Code={
                "ZipFile": code_zip(
                    source,
                    filename
                    or ("index.js" if runtime.startswith("node") else "index.py"),
                )
            },
            Environment={"Variables": environment or {}},
            Timeout=timeout,
        )
        return name

    yield create
    for name in functions:
        client.delete_function(FunctionName=name)


def invoke(client, name, event=None, **kwargs):
    response = client.invoke(
        FunctionName=name, Payload=json.dumps(event or {}), **kwargs
    )
    return response, json.loads(response["Payload"].read())


def test_python_handler_executes(runtime_lambda_client, function_factory):
    name = function_factory(
        "import sys\ndef handler(event, context):\n    print('phase1-handler-stderr', file=sys.stderr, flush=True)\n    return {'executed': True, 'input': event}"
    )
    response, payload = invoke(
        runtime_lambda_client, name, {"key": "한글 + & %"}, LogType="Tail"
    )
    assert payload == {"executed": True, "input": {"key": "한글 + & %"}}
    assert "FunctionError" not in response
    logs = base64.b64decode(response["LogResult"])
    assert len(logs) <= 4096
    assert b"phase1-handler-stderr" in logs


@pytest.mark.parametrize(
    "filename,source",
    [
        (
            "index.js",
            "exports.handler = async (event, context) => ({executed: true, input: event});",
        ),
        (
            "index.mjs",
            "export const handler = async (event, context) => ({executed: true, input: event});",
        ),
    ],
)
def test_node_handler_executes(
    runtime_lambda_client, function_factory, filename, source
):
    name = function_factory(source, runtime="nodejs22.x", filename=filename)
    response, payload = invoke(runtime_lambda_client, name, {"key": "value"})
    assert payload == {"executed": True, "input": {"key": "value"}}
    assert "FunctionError" not in response


def test_function_error_is_not_success(runtime_lambda_client, function_factory):
    name = function_factory(
        "def handler(event, context): raise ValueError('phase1 failure')"
    )
    response, payload = invoke(runtime_lambda_client, name)
    assert response["StatusCode"] == 200
    assert response["FunctionError"] == "Unhandled"
    assert payload == {"errorType": "ValueError", "errorMessage": "phase1 failure"}
    name = function_factory(
        "def handler(event, context): return {'errorType': 'user data'}"
    )
    response, payload = invoke(runtime_lambda_client, name)
    assert "FunctionError" not in response
    assert payload == {"errorType": "user data"}


def test_environment_reaches_handler(runtime_lambda_client, function_factory):
    name = function_factory(
        "import os\ndef handler(event, context): return {'mode': os.environ['MODE']}",
        environment={"MODE": "phase1"},
    )
    _, payload = invoke(runtime_lambda_client, name)
    assert payload == {"mode": "phase1"}


def test_timeout_cleans_runtime(runtime_lambda_client, function_factory):
    before = set(
        subprocess.check_output(
            ["docker", "ps", "-aq", "--filter", "name=devcloud-lambda-"]
        ).split()
    )
    name = function_factory(
        "import time\ndef handler(event, context): time.sleep(10)", timeout=1
    )
    response, payload = invoke(runtime_lambda_client, name)
    assert response["FunctionError"] == "Unhandled"
    assert payload["errorType"] == "TimeoutError"
    after = set(
        subprocess.check_output(
            ["docker", "ps", "-aq", "--filter", "name=devcloud-lambda-"]
        ).split()
    )
    assert after == before


def test_alias_keeps_published_code(runtime_lambda_client, function_factory):
    name = function_factory("def handler(event, context): return {'version': 1}")
    version = runtime_lambda_client.publish_version(FunctionName=name)["Version"]
    runtime_lambda_client.create_alias(
        FunctionName=name, Name="published", FunctionVersion=version
    )
    runtime_lambda_client.update_function_code(
        FunctionName=name,
        ZipFile=code_zip("def handler(event, context): return {'version': 2}"),
    )
    _, payload = invoke(runtime_lambda_client, name + ":published")
    assert payload == {"version": 1}
    _, payload = invoke(runtime_lambda_client, name)
    assert payload == {"version": 2}


@pytest.fixture
def sink(sqs_client):
    queue = sqs_client.create_queue(QueueName="sink-" + uuid.uuid4().hex[:12])[
        "QueueUrl"
    ]
    host = os.environ.get("DEVCLOUD_LAMBDA_CALLBACK_HOST", "host.docker.internal")
    endpoint = f"http://{host}:{urlparse(DEVCLOUD_URL).port}"
    yield {"ENDPOINT": endpoint, "SINK": queue}
    sqs_client.delete_queue(QueueUrl=queue)


SINK_HANDLER = """import json, os, urllib.request
def handler(event, context):
    body = json.dumps({'QueueUrl': os.environ['SINK'], 'MessageBody': json.dumps(event)}).encode()
    req = urllib.request.Request(os.environ['ENDPOINT'], data=body, headers={'Content-Type': 'application/x-amz-json-1.0', 'X-Amz-Target': 'AmazonSQS.SendMessage'})
    with urllib.request.urlopen(req, timeout=5) as response:
        response.read()
    return {'delivered': True}
"""


def receive_events(sqs_client, queue, count=1, timeout=30):
    events = []
    deadline = time.monotonic() + timeout
    while len(events) < count and time.monotonic() < deadline:
        response = sqs_client.receive_message(QueueUrl=queue, MaxNumberOfMessages=10)
        for message in response.get("Messages", []):
            events.append(json.loads(message["Body"]))
            sqs_client.delete_message(
                QueueUrl=queue, ReceiptHandle=message["ReceiptHandle"]
            )
        if len(events) < count:
            time.sleep(0.2)
    assert len(events) == count, (
        f"Expected {count} executed handler events, got {events}"
    )
    return events


def test_async_invocation_executes(
    runtime_lambda_client, function_factory, sqs_client, sink
):
    name = function_factory(SINK_HANDLER, environment=sink)
    response = runtime_lambda_client.invoke(
        FunctionName=name, InvocationType="Event", Payload=json.dumps({"async": True})
    )
    assert response["StatusCode"] == 202
    assert receive_events(sqs_client, sink["SINK"]) == [{"async": True}]


def test_s3_executes_qualified_handler(
    runtime_lambda_client, function_factory, sqs_client, s3_client, sink
):
    name = function_factory(SINK_HANDLER, environment=sink)
    version = runtime_lambda_client.publish_version(FunctionName=name)["Version"]
    arn = runtime_lambda_client.create_alias(
        FunctionName=name, Name="published", FunctionVersion=version
    )["AliasArn"]
    bucket = "runtime-" + uuid.uuid4().hex[:12]
    s3_client.create_bucket(Bucket=bucket)
    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "LambdaFunctionConfigurations": [
                    {"LambdaFunctionArn": arn, "Events": ["s3:ObjectCreated:*"]}
                ]
            },
        )
        s3_client.put_object(Bucket=bucket, Key="test.txt", Body=b"hello")
        event = receive_events(sqs_client, sink["SINK"])[0]["Records"][0]
        assert event["eventSource"] == "aws:s3"
        assert event["s3"]["bucket"]["name"] == bucket
        assert event["s3"]["object"]["key"] == "test.txt"
    finally:
        s3_client.delete_object(Bucket=bucket, Key="test.txt")
        s3_client.delete_bucket(Bucket=bucket)


def test_sqs_executes_handler(
    runtime_lambda_client, function_factory, sqs_client, sink
):
    name = function_factory(SINK_HANDLER, environment=sink)
    queue = sqs_client.create_queue(QueueName="source-" + uuid.uuid4().hex[:12])[
        "QueueUrl"
    ]
    arn = sqs_client.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    mapping = runtime_lambda_client.create_event_source_mapping(
        FunctionName=name, EventSourceArn=arn
    )["UUID"]
    try:
        sqs_client.send_message(
            QueueUrl=queue, MessageBody="한글 + payload & key=value"
        )
        event = receive_events(sqs_client, sink["SINK"])[0]["Records"][0]
        assert event["body"] == "한글 + payload & key=value"
        time.sleep(0.2)
        assert (
            sqs_client.get_queue_attributes(
                QueueUrl=queue, AttributeNames=["ApproximateNumberOfMessagesNotVisible"]
            )["Attributes"]["ApproximateNumberOfMessagesNotVisible"]
            == "0"
        )
    finally:
        runtime_lambda_client.delete_event_source_mapping(UUID=mapping)
        sqs_client.delete_queue(QueueUrl=queue)


def test_streams_executes_without_replay(
    runtime_lambda_client, function_factory, sqs_client, dynamodb_client, sink
):
    name = function_factory(SINK_HANDLER, environment=sink)
    table = "runtime-" + uuid.uuid4().hex[:12]
    created = dynamodb_client.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    arn = created["TableDescription"]["LatestStreamArn"]
    mapping = runtime_lambda_client.create_event_source_mapping(
        FunctionName=name,
        EventSourceArn=arn,
        StartingPosition="TRIM_HORIZON",
        BatchSize=1,
    )["UUID"]
    try:
        for value in ("first", "second"):
            dynamodb_client.put_item(TableName=table, Item={"id": {"S": value}})
            event = receive_events(sqs_client, sink["SINK"])[0]["Records"][0]
            assert event["dynamodb"]["Keys"]["id"]["S"] == value
        time.sleep(2.2)
        assert (
            sqs_client.receive_message(QueueUrl=sink["SINK"]).get("Messages", []) == []
        )
    finally:
        runtime_lambda_client.delete_event_source_mapping(UUID=mapping)
        dynamodb_client.delete_table(TableName=table)


def test_eventbridge_executes_qualified_handler(
    runtime_lambda_client, function_factory, sqs_client, events_client, sink
):
    name = function_factory(SINK_HANDLER, environment=sink)
    version = runtime_lambda_client.publish_version(FunctionName=name)["Version"]
    arn = runtime_lambda_client.create_alias(
        FunctionName=name, Name="published", FunctionVersion=version
    )["AliasArn"]
    rule = "runtime-" + uuid.uuid4().hex[:12]
    events_client.put_rule(
        Name=rule, EventPattern=json.dumps({"source": ["phase1.runtime"]})
    )
    events_client.put_targets(Rule=rule, Targets=[{"Id": "lambda", "Arn": arn}])
    try:
        events_client.put_events(
            Entries=[
                {
                    "Source": "phase1.runtime",
                    "DetailType": "runtime",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )
        event = receive_events(sqs_client, sink["SINK"])[0]
        assert event["Source"] == "phase1.runtime"
        assert json.loads(event["Detail"]) == {"key": "value"}
    finally:
        events_client.remove_targets(Rule=rule, Ids=["lambda"])
        events_client.delete_rule(Name=rule)


def test_sqs_function_error_keeps_message(
    runtime_lambda_client, function_factory, sqs_client
):
    name = function_factory("def handler(event, context): raise ValueError('retry me')")
    queue = sqs_client.create_queue(
        QueueName="failure-" + uuid.uuid4().hex[:12],
        Attributes={"VisibilityTimeout": "1"},
    )["QueueUrl"]
    arn = sqs_client.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    assert (
        sqs_client.get_queue_attributes(
            QueueUrl=queue, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]["VisibilityTimeout"]
        == "1"
    )
    mapping = runtime_lambda_client.create_event_source_mapping(
        FunctionName=name, EventSourceArn=arn
    )["UUID"]
    try:
        sqs_client.send_message(QueueUrl=queue, MessageBody="retry me")
        time.sleep(3)
        runtime_lambda_client.update_event_source_mapping(UUID=mapping, Enabled=False)
        deadline = time.monotonic() + 5
        messages = []
        while time.monotonic() < deadline and not messages:
            messages = sqs_client.receive_message(QueueUrl=queue).get("Messages", [])
            if not messages:
                time.sleep(0.2)
        attributes = sqs_client.get_queue_attributes(
            QueueUrl=queue, AttributeNames=["All"]
        )["Attributes"]
        assert [m["Body"] for m in messages] == ["retry me"], attributes
    finally:
        runtime_lambda_client.delete_event_source_mapping(UUID=mapping)
        sqs_client.delete_queue(QueueUrl=queue)
