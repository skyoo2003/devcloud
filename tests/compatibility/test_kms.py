import pytest
from botocore.exceptions import ClientError


def test_create_key(kms_client):
    resp = kms_client.create_key(Description="compat-key")
    assert resp["KeyMetadata"]["KeyState"] == "Enabled"
    assert "KeyId" in resp["KeyMetadata"]


def test_describe_key(kms_client):
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    resp = kms_client.describe_key(KeyId=key_id)
    assert resp["KeyMetadata"]["KeyId"] == key_id


def test_encrypt_decrypt(kms_client):
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    plaintext = b"hello kms"
    enc = kms_client.encrypt(KeyId=key_id, Plaintext=plaintext)
    assert "CiphertextBlob" in enc
    dec = kms_client.decrypt(KeyId=key_id, CiphertextBlob=enc["CiphertextBlob"])
    assert dec["Plaintext"] == plaintext


def test_generate_data_key(kms_client):
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    resp = kms_client.generate_data_key(KeyId=key_id, KeySpec="AES_256")
    assert "Plaintext" in resp
    assert "CiphertextBlob" in resp
    assert len(resp["Plaintext"]) == 32


def test_list_keys(kms_client):
    kms_client.create_key(Description="list-key")
    resp = kms_client.list_keys()
    assert len(resp["Keys"]) >= 1


def test_encrypt_invalid_key(kms_client):
    with pytest.raises(ClientError) as exc:
        kms_client.encrypt(KeyId="invalid-key-id", Plaintext=b"data")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_create_and_list_alias(kms_client):
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName="alias/test-alias", TargetKeyId=key_id)
    resp = kms_client.list_aliases()
    alias_names = [a["AliasName"] for a in resp["Aliases"]]
    assert "alias/test-alias" in alias_names


def test_disable_and_enable_key(kms_client):
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    kms_client.disable_key(KeyId=key_id)
    resp = kms_client.describe_key(KeyId=key_id)
    assert resp["KeyMetadata"]["KeyState"] == "Disabled"
    kms_client.enable_key(KeyId=key_id)
    resp = kms_client.describe_key(KeyId=key_id)
    assert resp["KeyMetadata"]["KeyState"] == "Enabled"


def test_grant_lifecycle(kms_client):
    key = kms_client.create_key(Description="grant test")
    kid = key["KeyMetadata"]["KeyId"]
    grant = kms_client.create_grant(
        KeyId=kid,
        GranteePrincipal="arn:aws:iam::000000000000:user/alice",
        Operations=["Encrypt", "Decrypt"],
        Name="my-grant",
    )
    assert "GrantId" in grant
    assert "GrantToken" in grant
    listing = kms_client.list_grants(KeyId=kid)
    assert any(g["GrantId"] == grant["GrantId"] for g in listing["Grants"])
    kms_client.revoke_grant(KeyId=kid, GrantId=grant["GrantId"])


def test_retire_grant(kms_client):
    key = kms_client.create_key(Description="retire test")
    kid = key["KeyMetadata"]["KeyId"]
    grant = kms_client.create_grant(
        KeyId=kid,
        GranteePrincipal="arn:aws:iam::000000000000:user/bob",
        RetiringPrincipal="arn:aws:iam::000000000000:user/admin",
        Operations=["Encrypt"],
    )
    kms_client.retire_grant(GrantToken=grant["GrantToken"])


def test_key_policy(kms_client):
    key = kms_client.create_key(Description="policy test")
    kid = key["KeyMetadata"]["KeyId"]
    policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"kms:*","Resource":"*"}]}'
    kms_client.put_key_policy(KeyId=kid, PolicyName="default", Policy=policy)
    resp = kms_client.get_key_policy(KeyId=kid, PolicyName="default")
    assert "Statement" in resp["Policy"]
    names = kms_client.list_key_policies(KeyId=kid)
    assert "default" in names["PolicyNames"]


def test_generate_data_key_variants(kms_client):
    key = kms_client.create_key(Description="dk test")
    kid = key["KeyMetadata"]["KeyId"]
    resp = kms_client.generate_data_key_without_plaintext(KeyId=kid, KeySpec="AES_256")
    assert "CiphertextBlob" in resp


def test_reencrypt(kms_client):
    k1 = kms_client.create_key(Description="src")
    k2 = kms_client.create_key(Description="dst")
    enc = kms_client.encrypt(KeyId=k1["KeyMetadata"]["KeyId"], Plaintext=b"hello")
    resp = kms_client.re_encrypt(
        CiphertextBlob=enc["CiphertextBlob"],
        DestinationKeyId=k2["KeyMetadata"]["KeyId"],
    )
    assert "CiphertextBlob" in resp


def test_sign_verify(kms_client):
    key = kms_client.create_key(Description="sign test")
    kid = key["KeyMetadata"]["KeyId"]
    msg = b"hello world"
    sig = kms_client.sign(
        KeyId=kid,
        Message=msg,
        MessageType="RAW",
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert "Signature" in sig
    ver = kms_client.verify(
        KeyId=kid,
        Message=msg,
        Signature=sig["Signature"],
        SigningAlgorithm="RSASSA_PKCS1_V1_5_SHA_256",
    )
    assert ver["SignatureValid"] is True


def test_schedule_key_deletion(kms_client):
    key = kms_client.create_key(Description="del test")
    kid = key["KeyMetadata"]["KeyId"]
    resp = kms_client.schedule_key_deletion(KeyId=kid, PendingWindowInDays=7)
    assert "DeletionDate" in resp
    kms_client.cancel_key_deletion(KeyId=kid)


def test_update_key_description(kms_client):
    key = kms_client.create_key(Description="original")
    kid = key["KeyMetadata"]["KeyId"]
    kms_client.update_key_description(KeyId=kid, Description="updated")
    desc = kms_client.describe_key(KeyId=kid)
    assert desc["KeyMetadata"]["Description"] == "updated"
