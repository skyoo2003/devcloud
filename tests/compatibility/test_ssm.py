import pytest


def test_put_and_get_parameter(ssm_client):
    ssm_client.put_parameter(Name="/compat/param", Value="hello", Type="String")
    resp = ssm_client.get_parameter(Name="/compat/param")
    assert resp["Parameter"]["Value"] == "hello"


def test_put_parameter_overwrite(ssm_client):
    ssm_client.put_parameter(Name="/compat/ow", Value="v1", Type="String")
    ssm_client.put_parameter(
        Name="/compat/ow", Value="v2", Type="String", Overwrite=True
    )
    resp = ssm_client.get_parameter(Name="/compat/ow")
    assert resp["Parameter"]["Value"] == "v2"
    assert resp["Parameter"]["Version"] == 2


def test_get_parameters(ssm_client):
    ssm_client.put_parameter(Name="/compat/multi/a", Value="1", Type="String")
    ssm_client.put_parameter(Name="/compat/multi/b", Value="2", Type="String")
    resp = ssm_client.get_parameters(Names=["/compat/multi/a", "/compat/multi/b"])
    assert len(resp["Parameters"]) == 2


def test_get_parameters_by_path(ssm_client):
    ssm_client.put_parameter(Name="/compat/path/x", Value="x", Type="String")
    ssm_client.put_parameter(Name="/compat/path/y", Value="y", Type="String")
    resp = ssm_client.get_parameters_by_path(Path="/compat/path/")
    assert len(resp["Parameters"]) >= 2


def test_delete_parameter(ssm_client):
    ssm_client.put_parameter(Name="/compat/del", Value="gone", Type="String")
    ssm_client.delete_parameter(Name="/compat/del")
    try:
        ssm_client.get_parameter(Name="/compat/del")
        assert False, "should have raised"
    except ssm_client.exceptions.ParameterNotFound:
        pass


def test_get_nonexistent_parameter(ssm_client):
    with pytest.raises(ssm_client.exceptions.ParameterNotFound):
        ssm_client.get_parameter(Name="/no/such/param")


def test_describe_parameters(ssm_client):
    ssm_client.put_parameter(Name="/compat/desc/p1", Value="v", Type="String")
    resp = ssm_client.describe_parameters(
        ParameterFilters=[{"Key": "Name", "Values": ["/compat/desc/p1"]}]
    )
    assert len(resp["Parameters"]) >= 1
    assert resp["Parameters"][0]["Name"] == "/compat/desc/p1"


def test_put_parameter_secure_string(ssm_client):
    ssm_client.put_parameter(
        Name="/compat/secure", Value="secret123", Type="SecureString"
    )
    resp = ssm_client.get_parameter(Name="/compat/secure", WithDecryption=True)
    assert resp["Parameter"]["Value"] == "secret123"
    assert resp["Parameter"]["Type"] == "SecureString"


def test_put_parameter_with_tags(ssm_client):
    ssm_client.put_parameter(
        Name="/compat/tagged",
        Value="tagged-val",
        Type="String",
        Tags=[{"Key": "env", "Value": "staging"}],
    )
    resp = ssm_client.list_tags_for_resource(
        ResourceType="Parameter",
        ResourceId="/compat/tagged",
    )
    tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
    assert tags.get("env") == "staging"


def test_parameter_tags(ssm_client):
    ssm_client.put_parameter(
        Name="/tagged/param",
        Value="v1",
        Type="String",
    )
    ssm_client.add_tags_to_resource(
        ResourceType="Parameter",
        ResourceId="/tagged/param",
        Tags=[{"Key": "env", "Value": "test"}],
    )
    tags = ssm_client.list_tags_for_resource(
        ResourceType="Parameter",
        ResourceId="/tagged/param",
    )
    assert any(t["Key"] == "env" for t in tags["TagList"])
    ssm_client.remove_tags_from_resource(
        ResourceType="Parameter",
        ResourceId="/tagged/param",
        TagKeys=["env"],
    )


def test_get_parameter_history(ssm_client):
    ssm_client.put_parameter(Name="/hist/p", Value="v1", Type="String")
    ssm_client.put_parameter(Name="/hist/p", Value="v2", Type="String", Overwrite=True)
    ssm_client.put_parameter(Name="/hist/p", Value="v3", Type="String", Overwrite=True)
    resp = ssm_client.get_parameter_history(Name="/hist/p")
    assert len(resp["Parameters"]) >= 3


def test_label_parameter_version(ssm_client):
    ssm_client.put_parameter(Name="/lbl/p", Value="v1", Type="String")
    resp = ssm_client.label_parameter_version(
        Name="/lbl/p",
        Labels=["stable"],
    )
    assert "InvalidLabels" in resp or "ParameterVersion" in resp


def test_document_lifecycle(ssm_client):
    content = '{"schemaVersion":"2.2","description":"test","mainSteps":[]}'
    ssm_client.create_document(
        Name="MyDoc",
        Content=content,
        DocumentType="Command",
    )
    desc = ssm_client.describe_document(Name="MyDoc")
    assert desc["Document"]["Name"] == "MyDoc"
    got = ssm_client.get_document(Name="MyDoc")
    assert "Content" in got
    listing = ssm_client.list_documents()
    assert any(d["Name"] == "MyDoc" for d in listing["DocumentIdentifiers"])
    ssm_client.delete_document(Name="MyDoc")


def test_update_document(ssm_client):
    content1 = '{"schemaVersion":"2.2","mainSteps":[]}'
    content2 = '{"schemaVersion":"2.2","mainSteps":[{"action":"aws:runShellScript"}]}'
    ssm_client.create_document(Name="UpdDoc", Content=content1, DocumentType="Command")
    ssm_client.update_document(
        Name="UpdDoc",
        Content=content2,
        DocumentVersion="$LATEST",
    )


def test_session_stubs(ssm_client):
    resp = ssm_client.start_session(Target="i-1234567890abcdef0")
    assert "SessionId" in resp
    assert "TokenValue" in resp
    ssm_client.terminate_session(SessionId=resp["SessionId"])
