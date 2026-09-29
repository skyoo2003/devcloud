import pytest
from botocore.exceptions import ClientError


def test_create_secret(secretsmanager_client):
    resp = secretsmanager_client.create_secret(
        Name="compat-secret", SecretString="hello"
    )
    assert "ARN" in resp
    assert "compat-secret" in resp["ARN"]


def test_get_secret_value(secretsmanager_client):
    secretsmanager_client.create_secret(Name="get-compat-secret", SecretString="world")
    resp = secretsmanager_client.get_secret_value(SecretId="get-compat-secret")
    assert resp["SecretString"] == "world"


def test_put_secret_value(secretsmanager_client):
    secretsmanager_client.create_secret(Name="put-compat-secret", SecretString="v1")
    secretsmanager_client.put_secret_value(
        SecretId="put-compat-secret", SecretString="v2"
    )
    resp = secretsmanager_client.get_secret_value(SecretId="put-compat-secret")
    assert resp["SecretString"] == "v2"


def test_list_secrets(secretsmanager_client):
    secretsmanager_client.create_secret(Name="list-sm-1", SecretString="a")
    resp = secretsmanager_client.list_secrets()
    names = [s["Name"] for s in resp["SecretList"]]
    assert "list-sm-1" in names


def test_delete_secret(secretsmanager_client):
    secretsmanager_client.create_secret(Name="del-sm-secret", SecretString="bye")
    secretsmanager_client.delete_secret(
        SecretId="del-sm-secret", ForceDeleteWithoutRecovery=True
    )
    resp = secretsmanager_client.list_secrets()
    names = [s["Name"] for s in resp["SecretList"]]
    assert "del-sm-secret" not in names


def test_get_nonexistent_secret(secretsmanager_client):
    with pytest.raises(ClientError) as exc:
        secretsmanager_client.get_secret_value(SecretId="no-such-secret-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_create_duplicate_secret(secretsmanager_client):
    secretsmanager_client.create_secret(Name="dup-secret", SecretString="v1")
    with pytest.raises(ClientError) as exc:
        secretsmanager_client.create_secret(Name="dup-secret", SecretString="v2")
    assert exc.value.response["Error"]["Code"] == "ResourceExistsException"


def test_update_secret(secretsmanager_client):
    secretsmanager_client.create_secret(Name="upd-secret", SecretString="old")
    secretsmanager_client.update_secret(
        SecretId="upd-secret", Description="updated desc"
    )
    resp = secretsmanager_client.describe_secret(SecretId="upd-secret")
    assert resp["Description"] == "updated desc"


def test_tag_resource(secretsmanager_client):
    resp = secretsmanager_client.create_secret(Name="tag-secret", SecretString="v1")
    arn = resp["ARN"]
    secretsmanager_client.tag_resource(
        SecretId=arn, Tags=[{"Key": "env", "Value": "test"}]
    )
    desc = secretsmanager_client.describe_secret(SecretId=arn)
    tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
    assert tags["env"] == "test"


def test_restore_secret(secretsmanager_client):
    secretsmanager_client.create_secret(Name="restore-secret", SecretString="v1")
    secretsmanager_client.delete_secret(
        SecretId="restore-secret", ForceDeleteWithoutRecovery=False
    )
    secretsmanager_client.restore_secret(SecretId="restore-secret")
    resp = secretsmanager_client.get_secret_value(SecretId="restore-secret")
    assert resp["SecretString"] == "v1"


def test_get_random_password(secretsmanager_client):
    resp = secretsmanager_client.get_random_password(PasswordLength=16)
    assert len(resp["RandomPassword"]) == 16


def test_random_password_exclude_chars(secretsmanager_client):
    resp = secretsmanager_client.get_random_password(
        PasswordLength=20,
        ExcludeCharacters="!@#$%^&*()",
        ExcludeNumbers=False,
    )
    pw = resp["RandomPassword"]
    assert not any(c in pw for c in "!@#$%^&*()")


def test_rotate_secret(secretsmanager_client):
    secretsmanager_client.create_secret(Name="rot-secret", SecretString="initial")
    resp = secretsmanager_client.rotate_secret(
        SecretId="rot-secret",
        RotationLambdaARN="arn:aws:lambda:us-east-1:000000000000:function:rotator",
        RotationRules={"AutomaticallyAfterDays": 30},
    )
    assert "VersionId" in resp
    desc = secretsmanager_client.describe_secret(SecretId="rot-secret")
    assert desc["RotationEnabled"] is True
    secretsmanager_client.cancel_rotate_secret(SecretId="rot-secret")


def test_batch_get_secret_value(secretsmanager_client):
    secretsmanager_client.create_secret(Name="batch-1", SecretString="v1")
    secretsmanager_client.create_secret(Name="batch-2", SecretString="v2")
    resp = secretsmanager_client.batch_get_secret_value(
        SecretIdList=["batch-1", "batch-2"],
    )
    values = {s["Name"]: s["SecretString"] for s in resp["SecretValues"]}
    assert values.get("batch-1") == "v1"
    assert values.get("batch-2") == "v2"


def test_list_secret_version_ids(secretsmanager_client):
    secretsmanager_client.create_secret(Name="ver-secret", SecretString="v1")
    secretsmanager_client.put_secret_value(SecretId="ver-secret", SecretString="v2")
    resp = secretsmanager_client.list_secret_version_ids(SecretId="ver-secret")
    assert len(resp["Versions"]) >= 2


def test_resource_policy(secretsmanager_client):
    secretsmanager_client.create_secret(Name="rp-secret", SecretString="s")
    policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"secretsmanager:*","Resource":"*"}]}'
    secretsmanager_client.put_resource_policy(
        SecretId="rp-secret",
        ResourcePolicy=policy,
    )
    resp = secretsmanager_client.get_resource_policy(SecretId="rp-secret")
    assert "Statement" in resp["ResourcePolicy"]
    secretsmanager_client.delete_resource_policy(SecretId="rp-secret")


def test_validate_resource_policy(secretsmanager_client):
    policy = '{"Version":"2012-10-17","Statement":[]}'
    resp = secretsmanager_client.validate_resource_policy(ResourcePolicy=policy)
    assert resp["PolicyValidationPassed"] is True


def test_update_secret_version_stage(secretsmanager_client):
    secretsmanager_client.create_secret(Name="stage-secret", SecretString="v1")
    resp1 = secretsmanager_client.put_secret_value(
        SecretId="stage-secret",
        SecretString="v2",
    )
    v2_id = resp1["VersionId"]
    secretsmanager_client.update_secret_version_stage(
        SecretId="stage-secret",
        VersionStage="AWSPREVIOUS",
        MoveToVersionId=v2_id,
    )
