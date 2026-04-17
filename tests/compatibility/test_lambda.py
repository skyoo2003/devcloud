import base64
import io
import json
import zipfile

import pytest
from botocore.exceptions import ClientError


def _create_test_function(client, name):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", "def handler(event, context): return {}")
    buf.seek(0)
    try:
        client.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test",
            Handler="handler.handler",
            Code={"ZipFile": buf.read()},
        )
    except client.exceptions.ResourceConflictException:
        pass


def test_create_function(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")  # minimal zip
    response = lambda_client.create_function(
        FunctionName="test-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/lambda-role",
        Code={"ZipFile": code_zip},
    )
    assert response["FunctionName"] == "test-func"
    assert "FunctionArn" in response


def test_list_functions(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="list-func-1",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    lambda_client.create_function(
        FunctionName="list-func-2",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    response = lambda_client.list_functions()
    names = [f["FunctionName"] for f in response["Functions"]]
    assert "list-func-1" in names
    assert "list-func-2" in names


def test_get_function(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="get-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    response = lambda_client.get_function(FunctionName="get-func")
    assert response["Configuration"]["FunctionName"] == "get-func"


def test_delete_function(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="del-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    lambda_client.delete_function(FunctionName="del-func")
    try:
        lambda_client.get_function(FunctionName="del-func")
        assert False, "Expected ResourceNotFoundException"
    except lambda_client.exceptions.ClientError as e:
        assert "ResourceNotFoundException" in str(e) or "404" in str(e)


def test_invoke(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="invoke-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    response = lambda_client.invoke(
        FunctionName="invoke-func",
        Payload=json.dumps({"key": "value"}),
    )
    assert response["StatusCode"] == 200
    payload = json.loads(response["Payload"].read())
    assert payload is not None


def test_invoke_nonexistent_function(lambda_client):
    with pytest.raises(ClientError) as exc:
        lambda_client.invoke(FunctionName="no-such-func")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_create_duplicate_function(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="dup-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    with pytest.raises(ClientError) as exc:
        lambda_client.create_function(
            FunctionName="dup-func",
            Runtime="python3.12",
            Handler="index.handler",
            Role="arn:aws:iam::000000000000:role/role",
            Code={"ZipFile": code_zip},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceConflictException"


def test_update_function_configuration(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="update-config-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    resp = lambda_client.update_function_configuration(
        FunctionName="update-config-func",
        Handler="index.new_handler",
        MemorySize=256,
    )
    assert resp["Handler"] == "index.new_handler"
    assert resp["MemorySize"] == 256


def test_update_function_code(lambda_client):
    code_zip = base64.b64decode("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==")
    lambda_client.create_function(
        FunctionName="update-code-func",
        Runtime="python3.12",
        Handler="index.handler",
        Role="arn:aws:iam::000000000000:role/role",
        Code={"ZipFile": code_zip},
    )
    resp = lambda_client.update_function_code(
        FunctionName="update-code-func",
        ZipFile=code_zip,
    )
    assert resp["FunctionName"] == "update-code-func"


def test_publish_version(lambda_client):
    _create_test_function(lambda_client, "ver-func")
    resp = lambda_client.publish_version(FunctionName="ver-func")
    assert resp["Version"] == "1"
    versions = lambda_client.list_versions_by_function(FunctionName="ver-func")
    assert any(v["Version"] == "1" for v in versions["Versions"])


def test_aliases(lambda_client):
    _create_test_function(lambda_client, "alias-func")
    lambda_client.publish_version(FunctionName="alias-func")
    resp = lambda_client.create_alias(
        FunctionName="alias-func",
        Name="prod",
        FunctionVersion="1",
    )
    assert resp["Name"] == "prod"
    got = lambda_client.get_alias(FunctionName="alias-func", Name="prod")
    assert got["FunctionVersion"] == "1"
    lambda_client.update_alias(
        FunctionName="alias-func",
        Name="prod",
        FunctionVersion="1",
    )
    aliases = lambda_client.list_aliases(FunctionName="alias-func")
    assert any(a["Name"] == "prod" for a in aliases["Aliases"])
    lambda_client.delete_alias(FunctionName="alias-func", Name="prod")


def test_function_tags(lambda_client):
    _create_test_function(lambda_client, "tag-func")
    arn = "arn:aws:lambda:us-east-1:000000000000:function:tag-func"
    lambda_client.tag_resource(Resource=arn, Tags={"env": "test"})
    resp = lambda_client.list_tags(Resource=arn)
    assert resp["Tags"]["env"] == "test"
    lambda_client.untag_resource(Resource=arn, TagKeys=["env"])


def test_event_source_mapping(lambda_client):
    _create_test_function(lambda_client, "esm-func")
    resp = lambda_client.create_event_source_mapping(
        FunctionName="esm-func",
        EventSourceArn="arn:aws:sqs:us-east-1:000000000000:my-queue",
        BatchSize=5,
    )
    uuid = resp["UUID"]
    assert resp["FunctionArn"].endswith("esm-func")
    got = lambda_client.get_event_source_mapping(UUID=uuid)
    assert got["BatchSize"] == 5
    lambda_client.update_event_source_mapping(UUID=uuid, BatchSize=10)
    updated = lambda_client.get_event_source_mapping(UUID=uuid)
    assert updated["BatchSize"] == 10
    mappings = lambda_client.list_event_source_mappings(FunctionName="esm-func")
    assert any(m["UUID"] == uuid for m in mappings["EventSourceMappings"])
    lambda_client.delete_event_source_mapping(UUID=uuid)
